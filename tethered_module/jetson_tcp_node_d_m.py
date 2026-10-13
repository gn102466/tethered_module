#!/usr/bin/env python3
"""
ROS2 node that acts as TCP *client* for tethered ROV/AUV communication.
Maintains several independent, auto-reconnecting TCP channels to the laptop/GCS
side (each channel is a separate RS-485<->Ethernet bridge pair on the tether),
handles bidirectional JSON and binary file transfers, and publishes telemetry
and state to ROS2 topics.

Subscribes to real sensor topics:
  - /imu (sensor_msgs/Imu)
  - /depth (geometry_msgs/Vector3Stamped)
  - /depth/pressure (sensor_msgs/FluidPressure)
  - /temperature (sensor_msgs/Temperature)
  - /humidity (std_msgs/Float32)
  - /battery_state (sensor_msgs/BatteryState)
  - /hardware/trigger_playload (std_msgs/Bool)
  - /leak_markers (visualization_msgs/MarkerArray) – we parse leak states from markers

New features:
  - ROSbag recording control via /ros2_bag_control topic (start / stop / save)
  - Reports recording status in telemetry
  - File listing (lists jetson_files directory tree)
  - Telemetry arm/disarm: when telemetry is disabled, sensor data is NOT sent
    over TCP to reduce bandwidth for high-speed joystick use.

Multi-channel link redundancy:
  - There are 3 independent RS-485<->Ethernet bridge pairs on the tether
    (CH-A/CH-B/CH-C below). Each is dialled and kept alive completely
    independently, with its own reconnect loop, keepalive and heartbeat -
    losing one never blocks or slows the others.
  - The highest-priority currently-connected channel is "active" (drives
    outbound commands/telemetry authority on the GCS side and is where this
    node expects most GCS command traffic to land). The next-highest
    currently-connected channel is "secondary" - telemetry/state are
    mirrored to it too so the GCS can show a live second feed. Any further
    connected channel is just tracked as a warm "backup".
  - Role assignment is STICKY: once a lower-priority channel takes over as
    active/secondary after a drop, it stays in that role even if a
    higher-priority channel reconnects later. This avoids "flapping" back
    and forth on a marginal link - see TcpClientNode._update_roles().
  - Incoming JSON commands are handled regardless of which channel they
    arrive on (defense in depth against transient active/secondary
    disagreement between the two ends), and any reply/ack/file-transfer
    traffic they trigger is sent back out on that same channel.
"""

import rclpy
from rclpy.node import Node
from rclpy.action import ActionClient
from rclpy.qos import DurabilityPolicy, QoSProfile
from std_msgs.msg import String, Float32, Bool
from sensor_msgs.msg import Imu, FluidPressure, Temperature, BatteryState
from geometry_msgs.msg import Vector3Stamped, Point
from nav_msgs.msg import Odometry
from visualization_msgs.msg import MarkerArray
from mission_interfaces.action import RunMissionTree
from action_msgs.srv import CancelGoal
import socket
import select
import subprocess
import threading
import json
import struct
import time
import os
import re
import shutil
import tempfile
import zlib
import math
import hashlib
import base64
from pathlib import Path
from queue import Queue, Empty, Full
from sensor_msgs.msg import NavSatFix, NavSatStatus
from sensor_msgs.msg import Joy


from dotenv import load_dotenv
load_dotenv()
JETSON_FILES_DIRS = os.getenv("JETSON_FILES_DIR", os.path.join(os.path.dirname(__file__), "jetson_files"))



# ----------------------------------------------------------------------
# Constants - redundant tether channels, each a separate RS-485<->Ethernet
# bridge pair. Priority is index order: 0 = highest priority (preferred
# "active" channel whenever it's connected), last = lowest (pure backup,
# only used if every higher-priority channel is down).
# ----------------------------------------------------------------------
CHANNELS_CONFIG = [
    {"name": "CH-A", "host": "192.168.1.203", "port": 6000},
    {"name": "CH-B", "host": "192.168.1.201", "port": 5005},
    {"name": "CH-C", "host": "192.168.1.202", "port": 6000},
    # Local loopback test channel - stands in for a real bridge pair when
    # testing against scripts/test_bridge_server.py instead of real RS-485
    # hardware (run that script, then this node + laptop_server_d_m.py,
    # both on the same machine - see that script's docstring). Lowest
    # priority so it never preempts a real channel; on the actual deployed
    # Jetson with nothing listening on 127.0.0.1:7000 it just stays "down"
    # and is harmless. Comment out / remove for a production-only build.
    {"name": "CH-test", "host": "127.0.0.1", "port": 7000},
]
# Must match the GCS's own FILE_CHUNK_SIZE in laptop_server_d_m_light.py.
# A large chunk here means one blocking sendall() call per chunk in
# TcpChannel._send_loop that can't be interrupted or interleaved with
# anything else outbound (heartbeats, telemetry, JSON replies) until the OS
# socket buffer accepts the whole thing - on a ~10kbps tether the old 65536
# value meant each chunk could block the send thread for the better part of
# a minute. Smaller chunks keep every send_queue item short enough that
# other outbound traffic never waits long behind one.
FILE_CHUNK_SIZE = 4096
RECV_BUFFER = 65536

# ----------------------------------------------------------------------
# Manual/ad-hoc connections - separate from the always-on, default
# CHANNELS_CONFIG channels above. These exist purely as operator-initiated
# BACKUP links (GCS "Manual Connection" tab): a raw TCP link over a spare
# RS-485 bridge, a direct Ethernet cable, WiFi, or localhost (for testing)
# all land here, on the SAME listening port, speaking the exact same ROVC
# packet protocol as the primary channels - see ManualConnectionServer.
# An SSH-tunneled manual connection ALSO ends up here: the GCS's SSH client
# tunnels a `direct-tcpip` channel to 127.0.0.1:MANUAL_LISTEN_PORT on this
# same machine, so from this server's point of view it's just another
# accepted socket - no SSH-specific code needed on this side at all.
MANUAL_LISTEN_PORT = 6100
# WebSocket manual connections need actual WS framing (not just a raw
# socket), so they get their own listening port with a minimal hand-rolled
# RFC 6455 server in front of the same packet protocol - see
# WebSocketManualServer.
MANUAL_WS_LISTEN_PORT = 6101

# ----------------------------------------------------------------------
# Link liveness / reconnect tuning
#
# Everything here is deliberately self-contained on the Jetson side - none
# of it requires any cooperation, ACK, or code change on the topside/GCS
# process. Two independent layers detect a dead link, per channel:
#
#   1. OS-level TCP keepalive (primary, ~9s: KEEPIDLE + KEEPINTVL*KEEPCNT).
#      Once armed, the *kernel* on this end sends keepalive probes and
#      expects the peer kernel to ACK them - that ACK is generated by the
#      remote TCP/IP stack itself (RFC 1122), not by the GCS application,
#      so this works even if the topside app is frozen/misbehaving, and
#      fails fast if the physical tether is actually gone.
#   2. App-level silence backstop (LINK_DEAD_TIMEOUT_SEC below) - a purely
#      defensive fallback in case keepalive doesn't fire for some reason
#      (platform without keepalive support, some NAT/proxy in the path).
#      It's deliberately much longer than the keepalive timeout so a GCS
#      that's simply idle (operator not touching anything, no reason for
#      it to send us bytes) never gets misdiagnosed as a dead link.
# ----------------------------------------------------------------------
# How often the recv loop wakes up (via select()) to re-check the above,
# independent of the socket's actual blocking/timeout mode - sendall() on
# the same socket must stay fully blocking, see TcpChannel._connect_loop().
RECV_POLL_INTERVAL_SEC = 2.0
# Backstop only - see comment block above. Deliberately generous.
LINK_DEAD_TIMEOUT_SEC = 20.0
# We proactively send our own heartbeat rather than only ACK'ing the
# server's, so a one-sided-silent link (server up, not talking) still
# generates outbound traffic (helps flush a broken send path fast, and
# keeps any NAT/switch state on the path from expiring). We do NOT require
# or wait for a reply to it - the GCS is not required to understand it.
HEARTBEAT_INTERVAL_SEC = 2.0
RECONNECT_BACKOFF_SEC = 5.0
# How often channel roles (active/secondary/backup) are recomputed.
ROLE_UPDATE_INTERVAL_SEC = 0.5

# jetson_files base directory.
# Resolution order:
#   1. JETSON_FILES_DIR environment variable (set this in your launch file or
#      ~/.bashrc for a permanent override, e.g.
#      export JETSON_FILES_DIR=/home/gofloat/pralay2_ros2_isaacsim/jetson_files)
#   2. <directory containing this .py file>/jetson_files  (works when run
#      directly with python3 jetson_tcp_node.py)
#   3. ~/pralay2_ros2_isaacsim/jetson_files  (hard-coded fallback matching your
#      actual workspace layout on the Jetson)
def _resolve_jetson_files_dir() -> Path:
    env = JETSON_FILES_DIRS #os.environ.get("JETSON_FILES_DIR", "").strip()
    if env:
        p = Path(env)
        p.mkdir(parents=True, exist_ok=True)
        return p
    # Sibling directory next to this source file
    sibling = Path(__file__).resolve().parent / "jetson_files"
    if sibling.exists():
        return sibling
    # Hard-coded workspace fallback
    fallback = Path.home() / "pralay2_ros2_isaacsim" / "jetson_files"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback

JETSON_FILES_DIR: Path = _resolve_jetson_files_dir()

def _resolve_within(base: Path, rel_path: str):
    """
    Resolve `rel_path` under `base` and verify the result is actually
    inside `base` - returns the resolved Path if valid, else None.

    NOT the same as `str(target).startswith(str(base))` (the pattern this
    replaces at every call site below) - that's a naive string-prefix
    check that a sibling directory sharing a prefix would incorrectly
    pass, e.g. base=".../jetson_files" would wrongly accept a resolved
    target of ".../jetson_files_evil/...". `Path.relative_to()` raises
    ValueError unless `target` really is `base` itself or a descendant of
    it, which is the actual check every caller here wants.
    """
    target = (base / rel_path).resolve()
    try:
        target.relative_to(base)
    except ValueError:
        return None
    return target

PKT_HEARTBEAT      = 0x01
PKT_HEARTBEAT_ACK  = 0x02
PKT_JSON           = 0x10
PKT_FILE_REQ       = 0x11
PKT_FILE_CHUNK     = 0x12
PKT_FILE_COMPLETE  = 0x13
MAGIC = b"ROVC"

# core/thrustmaster.py's joy_callback (the node that actually drives the
# fins/thrusters) indexes msg.axes[0..3] and msg.buttons[0..2] directly. A
# short/empty teleop payload (the virtual joystick used to send buttons:[])
# used to reach it as-is and throw an uncaught IndexError there, which
# crashes that whole node until it's manually restarted. Pad every teleop
# message up to this minimum length here so no client can ever send a Joy
# message short enough to do that again.
MIN_JOY_AXES = 4
MIN_JOY_BUTTONS = 3

# core/depth_hold.py publishes /depth_hold/status at 1 Hz. Older than this and
# the state message reports it as missing (null), so the GCS shows "node not
# running" instead of the last thing a dead node said.
DEPTH_HOLD_STATUS_TIMEOUT_SEC = 3.0

# data_type values published on /hardware/black_box that should NOT be
# relayed to the GCS blackbox console - these carry raw host:port
# connection strings and are just link-management noise, not operator-
# relevant events.
BLACKBOX_FILTERED_TYPES = {"gcs_connection"}

# ----------------------------------------------------------------------
# Remote Terminal ("run_command") tuning.
#
# NOTE ON TRUST MODEL: this runs whatever shell command the GCS sends with
# no additional authentication - same trust boundary as every other command
# on this link (arm/disarm/delete_file/etc already have no auth either; the
# tether itself, not a credential, is what's assumed to gate who can talk
# to this node). Each command runs as its own OS subprocess (never inline
# in this node's process/thread), so a hung or crashing command can never
# take down the TCP link or the rest of this node - it just times out.
# ----------------------------------------------------------------------
DEFAULT_COMMAND_MAX_LINES = 50         # cap on lines sent back, GCS-configurable per request
MAX_COMMAND_MAX_LINES = 1000           # hard ceiling regardless of what GCS asks for (bandwidth)
DEFAULT_COMMAND_TIMEOUT_SEC = 30.0
MAX_COMMAND_TIMEOUT_SEC = 300.0        # hard ceiling regardless of what GCS asks for

# ----------------------------------------------------------------------
# ROS2 parameter introspection ("ros2 param list/dump/set" via subprocess -
# same trust/isolation model as run_command: no rclpy service-client/executor
# wiring needed, and a hung `ros2` CLI invocation can't block the node).
# ----------------------------------------------------------------------
PARAM_DUMP_TIMEOUT_SEC = 5.0    # per-node timeout while gathering list_params
PARAM_SET_TIMEOUT_SEC = 5.0
# Overall soft cap across ALL nodes combined for one list_params gather - a
# graph with many nodes can otherwise take longer than any reasonable GCS
# wait, especially with `ros2` CLI's own per-invocation DDS-discovery
# startup overhead (commonly 1-3s each). Past this, return whatever was
# gathered so far rather than the GCS side timing out with nothing at all.
LIST_PARAMS_TOTAL_BUDGET_SEC = 45.0

# ----------------------------------------------------------------------
# Packet helpers  -  wire format (all big-endian):
#   MAGIC(4) | type(1) | payload_len(4) | crc32(4) | payload(N)
#   Header = 13 bytes total before payload.
#
# The CRC-32 covers the payload only.  On the receiver side, if the CRC
# does not match we know the "ROVC" we found was a false sync (the magic
# bytes appeared by chance inside a previous payload on the RS-485 link)
# and we continue scanning for the real next frame.  This eliminates the
# "Bad JSON packet" UTF-8 errors caused by framing desync on the tether.
# ----------------------------------------------------------------------
HEADER_SIZE = 13   # 4 MAGIC + 1 type + 4 len + 4 crc32

def pack_message(msg_type: int, payload: bytes) -> bytes:
    crc = zlib.crc32(payload) & 0xFFFFFFFF
    return MAGIC + struct.pack("!BII", msg_type, len(payload), crc) + payload

def pack_json(msg_type: int, obj: dict) -> bytes:
    data = json.dumps(obj).encode("utf-8")
    return pack_message(msg_type, data)


# ----------------------------------------------------------------------
# TcpChannel - one independently-reconnecting tether link
# ----------------------------------------------------------------------
class TcpChannel:
    """
    One of the 3 redundant tether channels. Owns its own socket, its own
    reconnect loop (with OS keepalive tuning identical to the original
    single-channel implementation), and its own send/recv queues.

    `send_queue` is None whenever the channel is disconnected (so callers
    can tell "no session to send into" apart from "session exists but
    empty") and is replaced with a fresh Queue() on every successful
    connect, which also means anything queued right before a drop is
    discarded rather than being flushed stale into the next session.

    `recv_queue` is a single long-lived Queue for the channel's lifetime -
    it's drained continuously by `_dispatch_loop` regardless of connection
    state, so there's no staleness concern there.

    `telemetry_queue` is a SEPARATE, maxsize=1 "latest wins" queue for
    best-effort periodic data (telemetry/compass - see send_bulk()).
    `_send_loop` always drains `send_queue` first and only sends from
    `telemetry_queue` once `send_queue` is empty. On a ~10kbps tether,
    telemetry published at 5Hz is, by itself, already faster than the link
    can carry - without this split it piles up in a single FIFO ahead of
    file-transfer/command/ack traffic, which is what previously made a
    mission map upload's "file_ready" ACK (queued behind minutes of
    backlogged telemetry) blow straight through the GCS's short ack-wait
    timeout. Bounding it to maxsize=1 (replace-oldest) means at most one
    stale telemetry frame is ever in flight, so it can no longer starve
    higher-priority traffic, and the GCS still always sees the latest data
    once bandwidth is available for it.
    """
    def __init__(self, node: "TcpClientNode", name: str, host: str, port: int, priority: int):
        self.node = node
        self.name = name
        self.host = host
        self.port = port
        self.priority = priority

        self.sock = None
        self.send_queue: Queue | None = None
        self.telemetry_queue: "Queue" = Queue(maxsize=1)
        self.recv_queue: "Queue" = Queue()

        self.connected = False
        self.last_rx = 0.0
        self.reconnects = 0
        self.role = "down"   # "active" | "secondary" | "backup" | "down"
        # Set once by _manual_session() when a manual/ad-hoc connection's
        # one-shot session ends, so the paired _dispatch_loop thread (also
        # spawned by start_manual) knows to exit too. Never set for
        # config-based channels (start(), not start_manual()) - those only
        # ever stop via node._stop_event at full shutdown. Without this,
        # every manual connect/disconnect cycle leaked one daemon thread
        # polling a dead channel's recv_queue forever - found via code
        # review after the fact, not caught before this shipped.
        self._manual_done = threading.Event()

    def start(self):
        threading.Thread(target=self._connect_loop, daemon=True, name=f"{self.name}-conn").start()
        threading.Thread(target=self._dispatch_loop, daemon=True, name=f"{self.name}-disp").start()

    def start_manual(self, sock):
        """
        Alternate entry point for a connection handed to us already-
        connected (e.g. accepted by ManualConnectionServer, or a WebSocket
        connection after its handshake completes) instead of one we dial
        out to ourselves. Runs _run_session() exactly ONCE - no redial loop
        - since there's no fixed remote address to retry for an ephemeral
        manual/ad-hoc peer; once it disconnects, the node removes this
        channel from its list (see TcpClientNode._remove_channel) rather
        than leaving a permanently-"down" entry cluttering the GCS UI.
        """
        threading.Thread(target=self._manual_session, args=(sock,), daemon=True, name=f"{self.name}-manual").start()
        threading.Thread(target=self._dispatch_loop, daemon=True, name=f"{self.name}-disp").start()

    def _manual_session(self, sock):
        self._run_session(sock)
        self._manual_done.set()
        self.node._remove_channel(self)

    def send(self, data: bytes) -> bool:
        """Queue bytes for send on this channel. No-op (returns False) if down."""
        q = self.send_queue
        if q is not None:
            q.put(data)
            return True
        return False

    def send_bulk(self, data: bytes) -> bool:
        """Queue bytes as low-priority "latest wins" bulk data (telemetry/
        compass) - see the telemetry_queue docstring above. Discards
        whatever's currently pending in favor of this newer item rather
        than blocking or growing the queue - an old telemetry frame is
        worthless once a fresher one exists, so there's nothing worth
        preserving. No-op (returns False) if the channel is down."""
        if self.send_queue is None:
            return False
        try:
            self.telemetry_queue.put_nowait(data)
        except Full:
            try:
                self.telemetry_queue.get_nowait()
            except Empty:
                pass
            try:
                self.telemetry_queue.put_nowait(data)
            except Full:
                pass
        return True

    def status(self) -> dict:
        age = None if self.last_rx == 0.0 else max(0.0, time.monotonic() - self.last_rx)
        return {
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "connected": self.connected,
            "role": self.role,
            "last_rx_age": age,
            "reconnects": self.reconnects,
        }

    def close(self):
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass

    # ---------- connection lifecycle ----------
    def _run_session(self, sock):
        """
        Shared post-connect session body: OS keepalive tuning, spawn send/
        recv threads, block until disconnected, clean up. Extracted from
        _connect_loop unchanged (byte-for-byte the same steps it always
        did) so it can also be reused by _manual_session() for a socket
        that arrived via accept() instead of connect() - the dial-out
        redial LOOP stays in _connect_loop, which is the only thing that
        actually differs between "we called out" and "someone connected to
        us."
        """
        node = self.node
        # OS-level keepalive: makes the kernel itself probe a link that's
        # gone silent and error the socket out if the peer is truly
        # unreachable, instead of relying solely on the application-level
        # staleness check below.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 3)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 2)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        except (AttributeError, OSError):
            pass  # keepalive tuning isn't available on this platform

        self.sock = sock
        self.send_queue = Queue()
        # Drain any bulk item left over from a previous session rather than
        # carrying stale telemetry into the fresh connection.
        try:
            while True:
                self.telemetry_queue.get_nowait()
        except Empty:
            pass
        self.last_rx = time.monotonic()
        self.connected = True
        node.get_logger().info(f"[{self.name}] Connected to {self.host}:{self.port}")
        node.save_black_box("gcs_connection", f"{self.name} connected to {self.host}:{self.port}")

        active = threading.Event()
        active.set()
        send_thread = threading.Thread(target=self._send_loop, args=(sock, active), daemon=True)
        recv_thread = threading.Thread(target=self._recv_loop, args=(sock, active), daemon=True)
        send_thread.start()
        recv_thread.start()

        while active.is_set() and not node._stop_event.is_set():
            time.sleep(0.2)

        active.clear()
        self.connected = False
        self.send_queue = None
        # If this channel dropped mid-upload (link loss, not a clean
        # PKT_FILE_COMPLETE), clear that upload's state so the transfer-
        # active telemetry pause (see _set_transfer_active) can't get stuck
        # on forever - without this, a tether drop during a mission map
        # upload would silently disable telemetry for the rest of the
        # session.
        node.file_handler.cancel_for_channel(self)
        send_thread.join(timeout=1)
        recv_thread.join(timeout=1)
        sock.close()
        self.sock = None
        self.reconnects += 1
        node.save_black_box("gcs_connection", f"{self.name} disconnected")
        node.get_logger().warn(f"[{self.name}] Disconnected.")

    def _connect_loop(self):
        node = self.node
        while not node._stop_event.is_set():
            try:
                node.get_logger().info(f"[{self.name}] Attempting to connect to {self.host}:{self.port}...")
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                sock.connect((self.host, self.port))
                # Back to fully blocking. We poll for readability with
                # select() in _recv_loop instead of using socket-level
                # settimeout() - settimeout() applies to the whole socket
                # object (shared with the send thread), so using it here
                # would also cap sendall() and misfire on a slow-but-healthy
                # send (e.g. a large file chunk) as if the link had died.
                sock.settimeout(None)

                self._run_session(sock)

                if not node._stop_event.is_set():
                    node.get_logger().warn(f"[{self.name}] Reconnecting in {RECONNECT_BACKOFF_SEC}s...")
                    time.sleep(RECONNECT_BACKOFF_SEC)

            except socket.error as e:
                self.connected = False
                self.send_queue = None
                node.get_logger().error(f"[{self.name}] Connection error: {e}")
                node.save_black_box("gcs_connection", f"{self.name} connection error: {e}")
                if not node._stop_event.is_set():
                    time.sleep(RECONNECT_BACKOFF_SEC)

            except Exception as e:
                self.connected = False
                self.send_queue = None
                node.get_logger().error(f"[{self.name}] Unexpected error in channel loop: {e}")
                if not node._stop_event.is_set():
                    time.sleep(RECONNECT_BACKOFF_SEC)

    def _send_loop(self, sock, active):
        while active.is_set() and not self.node._stop_event.is_set():
            # Control/file-transfer/ack traffic (send_queue) always goes out
            # ahead of best-effort telemetry (telemetry_queue) - see the
            # telemetry_queue docstring in __init__. Only fall through to
            # the bulk queue once send_queue is genuinely empty right now.
            try:
                data = self.send_queue.get_nowait()
            except Empty:
                try:
                    data = self.telemetry_queue.get(timeout=0.5)
                except Empty:
                    continue
            try:
                sock.sendall(data)
            except (BrokenPipeError, ConnectionResetError, OSError):
                active.clear()
                break

    def _recv_loop(self, sock, active):
        buffer = bytearray()
        while active.is_set() and not self.node._stop_event.is_set():
            try:
                ready, _, _ = select.select([sock], [], [], RECV_POLL_INTERVAL_SEC)
                if not ready:
                    # Nothing to read yet - expected, this is just our poll
                    # interval. Only a problem if we've heard NOTHING for
                    # LINK_DEAD_TIMEOUT_SEC (backstop behind OS keepalive -
                    # see the tuning comment near the top of this file).
                    if time.monotonic() - self.last_rx > LINK_DEAD_TIMEOUT_SEC:
                        self.node.get_logger().warn(
                            f"[{self.name}] No data in {LINK_DEAD_TIMEOUT_SEC}s - link considered dead."
                        )
                        active.clear()
                        break
                    continue

                chunk = sock.recv(RECV_BUFFER)
                if not chunk:
                    active.clear()
                    break
                self.last_rx = time.monotonic()
                buffer.extend(chunk)
                while len(buffer) >= HEADER_SIZE:
                    if buffer[:4] != MAGIC:
                        buffer.pop(0)
                        continue
                    msg_type, payload_len, expected_crc = struct.unpack("!BII", buffer[4:13])
                    total = HEADER_SIZE + payload_len
                    if len(buffer) < total:
                        break
                    payload = bytes(buffer[HEADER_SIZE:total])
                    actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
                    if actual_crc != expected_crc:
                        # False sync - ROVC bytes appeared inside a previous payload.
                        # Skip just the first byte of this candidate and keep scanning.
                        buffer.pop(0)
                        continue
                    del buffer[:total]
                    self.recv_queue.put((msg_type, payload))
            except (ConnectionResetError, BrokenPipeError, OSError):
                active.clear()
                break

    def _dispatch_loop(self):
        """Single consumer of recv_queue for the life of the node - see the
        FileTransferHandler docstring for why a second consumer is a bug.
        Also exits once _manual_done is set (manual channels only - see
        __init__) rather than running forever after the channel it serves
        has already been removed."""
        while not self.node._stop_event.is_set() and not self._manual_done.is_set():
            try:
                msg_type, payload = self.recv_queue.get(timeout=0.5)
            except Empty:
                continue
            self.node._dispatch_packet(self, msg_type, payload)


# ----------------------------------------------------------------------
# Manual connection server - accepts raw TCP connections for the GCS's
# "Manual Connection" tab (Tethered/RS-485, Ethernet, Localhost, and the
# far end of an SSH tunnel - see MANUAL_LISTEN_PORT's comment above). Each
# accepted socket becomes an ordinary TcpChannel via start_manual(), joining
# TcpClientNode.channels as a lowest-priority backup with the full ROVC
# protocol - no special-casing needed anywhere else in the dispatch code.
# ----------------------------------------------------------------------
class ManualConnectionServer:
    def __init__(self, node: "TcpClientNode", port: int):
        self.node = node
        self.port = port
        self._counter = 0

    def start(self):
        threading.Thread(target=self._accept_loop, daemon=True, name="ManualListen").start()

    def _accept_loop(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            srv.bind(("0.0.0.0", self.port))
            srv.listen(4)
        except OSError as e:
            self.node.get_logger().error(f"Manual connection listener failed to bind :{self.port}: {e}")
            return
        self.node.get_logger().info(f"Manual connection listener up on 0.0.0.0:{self.port}")

        srv.settimeout(1.0)   # so we can notice _stop_event without blocking forever in accept()
        while not self.node._stop_event.is_set():
            try:
                sock, addr = srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            self._counter += 1
            name = f"MANUAL-{addr[0]}:{addr[1]}-{self._counter}"
            self.node.get_logger().info(f"[{name}] Manual connection accepted from {addr[0]}:{addr[1]}")
            ch = self.node._add_channel(name, addr[0], addr[1])
            ch.start_manual(sock)
        srv.close()


# ----------------------------------------------------------------------
# Minimal hand-rolled WebSocket (RFC 6455) support - binary frames only, no
# fragmentation, no extensions, no auto ping/pong replies (the existing
# app-level heartbeat already covers liveness). A real library
# (websockets/aiohttp) is asyncio-based and would need its own event loop
# woven into this otherwise synchronous/threaded node, so this is
# deliberately self-contained instead. Shared by both the Jetson's WS
# SERVER below and the GCS's WS CLIENT, since decode handles both masked
# (client->server) and unmasked (server->client) frames.
#
# This is the LEAST field-tested part of the whole manual-connection
# feature - verified via unit tests on the encode/decode round-trip in
# isolation (pure functions, no network needed), not against a real
# browser or WS client library.
# ----------------------------------------------------------------------
_WS_OPCODE_CONTINUATION = 0x0
_WS_OPCODE_TEXT = 0x1
_WS_OPCODE_BINARY = 0x2
_WS_OPCODE_CLOSE = 0x8
_WS_OPCODE_PING = 0x9
_WS_OPCODE_PONG = 0xA
_WS_MAGIC_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def _ws_accept_key(client_key: str) -> str:
    digest = hashlib.sha1((client_key + _WS_MAGIC_GUID).encode("ascii")).digest()
    return base64.b64encode(digest).decode("ascii")


def _ws_encode_frame(payload: bytes, opcode: int = _WS_OPCODE_BINARY, mask: bool = False) -> bytes:
    """RFC 6455 frame encoder, single frame (FIN=1), no fragmentation.
    Server->client frames must be unmasked (mask=False, the default here);
    a WS CLIENT sending frames must set mask=True (required by spec)."""
    fin_opcode = 0x80 | (opcode & 0x0F)
    length = len(payload)
    mask_bit = 0x80 if mask else 0x00
    if length <= 125:
        header = bytes([fin_opcode, mask_bit | length])
    elif length <= 0xFFFF:
        header = bytes([fin_opcode, mask_bit | 126]) + struct.pack("!H", length)
    else:
        header = bytes([fin_opcode, mask_bit | 127]) + struct.pack("!Q", length)
    if not mask:
        return header + payload
    mask_key = os.urandom(4)
    masked = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
    return header + mask_key + masked


def _ws_try_decode_frame(buf):
    """Attempt to decode ONE complete frame from the front of `buf`
    (a bytearray). Returns (opcode, payload_bytes, consumed_length) if a
    complete frame is present, else None (caller should read more bytes
    off the wire and retry)."""
    if len(buf) < 2:
        return None
    b0, b1 = buf[0], buf[1]
    opcode = b0 & 0x0F
    masked = bool(b1 & 0x80)
    length = b1 & 0x7F
    offset = 2
    if length == 126:
        if len(buf) < offset + 2:
            return None
        length = struct.unpack("!H", bytes(buf[offset:offset + 2]))[0]
        offset += 2
    elif length == 127:
        if len(buf) < offset + 8:
            return None
        length = struct.unpack("!Q", bytes(buf[offset:offset + 8]))[0]
        offset += 8
    mask_key = None
    if masked:
        if len(buf) < offset + 4:
            return None
        mask_key = bytes(buf[offset:offset + 4])
        offset += 4
    if len(buf) < offset + length:
        return None
    payload = bytes(buf[offset:offset + length])
    if masked:
        payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
    return opcode, payload, offset + length


class _WsFramedSocket:
    """
    Wraps an already-handshaken WS socket so the REST of TcpChannel
    (_send_loop/_recv_loop, which call .sendall()/.recv()/select() on
    whatever "sock" _run_session() was given - completely unmodified for
    this) works against a WebSocket transport with zero special-casing
    elsewhere.

    Relies on one invariant already true of how TcpChannel sends data:
    channel.send() queues exactly one complete ROVC packet's bytes per
    call, and _send_loop does exactly one sendall() per queued item - so
    wrapping each sendall() as ONE WS frame naturally preserves a 1:1
    packet<->WS-message correspondence with no extra bookkeeping. Decoding
    relies on the same invariant in reverse: one complete WS frame == one
    complete ROVC packet, so recv() can hand back a whole frame's payload
    directly - WS already preserves message boundaries, so this transport
    doesn't need the MAGIC-scanning reassembly buffer the raw-TCP
    transports use for that.

    Never touches settimeout() on the underlying socket (would also cap
    sendall(), same concern already documented on the raw-TCP path in
    _connect_loop) - uses select() for readability polling instead, same
    as everywhere else in this file, with a LINK_DEAD_TIMEOUT_SEC-bounded
    deadline so a frame that never finishes arriving can't hang recv()
    forever (bounds it to a clean disconnect instead, same outcome as a
    genuinely dead raw-TCP link).
    """
    def __init__(self, sock, initial_buffer: bytes = b""):
        self._sock = sock
        self._buffer = bytearray(initial_buffer)

    def fileno(self):
        return self._sock.fileno()

    def sendall(self, data: bytes):
        self._sock.sendall(_ws_encode_frame(data))

    def recv(self, bufsize: int) -> bytes:
        deadline = time.monotonic() + LINK_DEAD_TIMEOUT_SEC
        while True:
            frame = _ws_try_decode_frame(self._buffer)
            if frame is not None:
                opcode, payload, consumed = frame
                del self._buffer[:consumed]
                if opcode == _WS_OPCODE_CLOSE:
                    return b""
                if opcode in (_WS_OPCODE_TEXT, _WS_OPCODE_BINARY):
                    return payload
                continue   # ping/pong/other control frame - no payload to hand back, keep looking
            if time.monotonic() > deadline:
                return b""   # a frame stuck assembling this long - treat like a dead link
            ready, _, _ = select.select([self._sock], [], [], RECV_POLL_INTERVAL_SEC)
            if not ready:
                continue
            try:
                chunk = self._sock.recv(RECV_BUFFER)
            except OSError:
                return b""
            if not chunk:
                return b""
            self._buffer.extend(chunk)

    def setsockopt(self, *args, **kwargs):
        return self._sock.setsockopt(*args, **kwargs)

    def close(self):
        try:
            self._sock.close()
        except OSError:
            pass


class WebSocketManualServer:
    """Same purpose as ManualConnectionServer, but for WebSocket manual
    connections - these need an actual WS handshake (HTTP Upgrade) and
    RFC 6455 framing in front of the same packet protocol, so they get
    their own listening port rather than sharing the raw-TCP one."""
    def __init__(self, node: "TcpClientNode", port: int):
        self.node = node
        self.port = port
        self._counter = 0

    def start(self):
        threading.Thread(target=self._accept_loop, daemon=True, name="ManualWsListen").start()

    def _accept_loop(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            srv.bind(("0.0.0.0", self.port))
            srv.listen(4)
        except OSError as e:
            self.node.get_logger().error(f"WS manual listener failed to bind :{self.port}: {e}")
            return
        self.node.get_logger().info(f"Manual WebSocket listener up on 0.0.0.0:{self.port}")

        srv.settimeout(1.0)
        while not self.node._stop_event.is_set():
            try:
                sock, addr = srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            # Handshake runs on its own short-lived thread so a slow/
            # stalled client mid-handshake can never block accept()ing
            # the next one.
            threading.Thread(target=self._handle_new_socket, args=(sock, addr), daemon=True).start()
        srv.close()

    def _handle_new_socket(self, sock, addr):
        try:
            sock.settimeout(5.0)
            ok, leftover = self._do_handshake(sock)
            if not ok:
                sock.close()
                return
            sock.settimeout(None)
        except OSError as e:
            self.node.get_logger().warning(f"WS handshake failed from {addr[0]}:{addr[1]}: {e}")
            try:
                sock.close()
            except OSError:
                pass
            return

        self._counter += 1
        name = f"MANUAL-WS-{addr[0]}:{addr[1]}-{self._counter}"
        self.node.get_logger().info(f"[{name}] WebSocket manual connection accepted from {addr[0]}:{addr[1]}")
        ch = self.node._add_channel(name, addr[0], addr[1])
        ch.start_manual(_WsFramedSocket(sock, initial_buffer=leftover))

    @staticmethod
    def _do_handshake(sock):
        """Reads the client's HTTP Upgrade request and replies with the
        101 Switching Protocols response. Returns (ok, leftover_bytes) -
        leftover is any bytes read past the header terminator (e.g. an
        eager client that starts sending frames before waiting for the
        101 response), preserved so _WsFramedSocket doesn't silently drop
        them."""
        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = sock.recv(4096)
            if not chunk:
                return False, b""
            buf += chunk
            if len(buf) > 16384:   # sane cap against a misbehaving/hostile client
                return False, b""
        header_part, _, leftover = buf.partition(b"\r\n\r\n")
        header_text = header_part.decode("iso-8859-1", errors="replace")
        headers = {}
        for line in header_text.split("\r\n")[1:]:
            if ":" not in line:
                continue
            k, _, v = line.partition(":")
            headers[k.strip().lower()] = v.strip()
        client_key = headers.get("sec-websocket-key")
        if not client_key:
            return False, b""
        accept_key = _ws_accept_key(client_key)
        response = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept_key}\r\n"
            "\r\n"
        ).encode("ascii")
        sock.sendall(response)
        return True, leftover


# ----------------------------------------------------------------------
# ROS2 Node
# ----------------------------------------------------------------------
class TcpClientNode(Node):
    def __init__(self):
        super().__init__('jetson_tcp_client')
        self.get_logger().info("Starting Jetson TCP Client Node (real data, 3-channel link)")

        # Publishers (for incoming commands, etc.)
        self.tel_pub = self.create_publisher(String, '/rov/telemetry', 10)
        self.state_pub = self.create_publisher(String, '/rov/state', 10)
        self.incoming_pub = self.create_publisher(String, '/rov/incoming', 10)

        # Publisher for rosbag recording control
        self.rosbag_control_pub = self.create_publisher(String, '/ros2_bag_control', 10)

        # Subscribers to actual sensor data
        self.imu_sub = self.create_subscription(Imu, '/imu', self.imu_callback, 10)
        self.depth_sub = self.create_subscription(Vector3Stamped, '/depth', self.depth_callback, 10)
        self.mag_sub = self.create_subscription(Vector3Stamped, '/imu/mag', self.mag_callback, 10)
        self.pressure_sub = self.create_subscription(FluidPressure, '/depth/pressure', self.pressure_callback, 10)
        self.temp_sub = self.create_subscription(Temperature, '/temperature', self.temp_callback, 10)
        self.humidity_sub = self.create_subscription(Float32, '/humidity', self.humidity_callback, 10)
        self.battery_sub = self.create_subscription(BatteryState, '/battery_state', self.battery_callback, 10)
        self.relay_sub = self.create_subscription(Bool, '/hardware/trigger_playload', self.relay_callback, 10)
        self.leak_sub = self.create_subscription(MarkerArray, '/leak_markers', self.leak_callback, 10)
        self.gps_loc_sub = self.create_subscription(NavSatFix, '/gps/fix', self.gps_callback, 10)

        self.emergency_stop_pub = self.create_publisher(Bool, '/hardware/emergency_stop', 10)
        self.black_box_pub = self.create_publisher(String, '/hardware/black_box', 10)
        self.joy_pub = self.create_publisher(Joy, '/joy', 10)

        # Relay /hardware/black_box entries to the GCS "Black Box Console"
        # (filtered - see BLACKBOX_FILTERED_TYPES / _blackbox_callback).
        # Subscribing to the topic (rather than hooking save_black_box()
        # directly) means any future publisher on this topic shows up too.
        self.black_box_sub = self.create_subscription(
            String, '/hardware/black_box', self._blackbox_callback, 50
        )

        # Store latest data
        self.latest = {
            'imu': None,
            'depth': None,
            'pressure': None,
            'temperature': None,
            'humidity': None,
            'battery': None,
            'relay': None,
            'mag': None,
            'leaks': [False, False, False, False],  # front, centre, rear, imu
            'gps': None,
        }

        # State
        self.mode = "manual"
        self.armed = False
        self.errors = []

        # Rosbag recording state
        self.recording_active = False   # True while a bag is being recorded
        self._recording_lock = threading.Lock()

        # Telemetry enable/disable (GCS can arm/disarm telemetry stream)
        # When False, sensor packets are NOT sent over TCP (saves bandwidth
        # so the joystick commands get through faster on the RS-485 tether).
        self.telemetry_enabled = True
        self._telemetry_lock = threading.Lock()

        # Black Box console streaming: OFF by default (unlike telemetry) -
        # this is continuous, low-value-per-byte link-management/event noise
        # the operator usually doesn't need live, so on a 10kbps tether it
        # should be opt-in rather than opt-out.
        self.blackbox_stream_enabled = False
        self._blackbox_stream_lock = threading.Lock()

        # Compass/magnetometer stream (drives the GCS heading panel): also
        # OFF by default, same bandwidth reasoning - it's an extra stream on
        # top of core telemetry, not needed unless the operator has that
        # panel open.
        self.mag_stream_enabled = False
        self._mag_lock = threading.Lock()

        # Depth hold switch -> core/depth_hold.py + core/thrustmaster.py (both
        # gate on it; OFF hands the side fins back to the joystick). Latched
        # so either node picks up the current choice after a restart. ON by
        # default: this is an autonomous vehicle and the joystick is for
        # emergencies, so the operator only switches it OFF when needed.
        self.depth_hold_enabled = True
        self._depth_hold_lock = threading.Lock()
        self.depth_hold_enable_pub = self.create_publisher(
            Bool, '/depth_hold/enable',
            QoSProfile(depth=1, durability=DurabilityPolicy.TRANSIENT_LOCAL))
        self.depth_hold_enable_pub.publish(Bool(data=self.depth_hold_enabled))
        # What depth_hold.py says it is actually doing, relayed in the state
        # message so the GCS shows more than the switch position.
        self.depth_hold_status = None
        self.depth_hold_status_time = 0.0
        self.create_subscription(String, '/depth_hold/status', self._depth_hold_status_callback, 10)

        # Set while a file upload is actively being received (see
        # FileTransferHandler) - _publish_telemetry checks this and skips
        # sending a fresh telemetry frame while it's set, so the full
        # ~10kbps tether budget goes to the transfer instead of competing
        # with the telemetry stream. Independent of telemetry_enabled (the
        # operator's own arm/disarm toggle) so this auto-pause never
        # clobbers or gets confused with that persisted preference - it's
        # restored to whatever telemetry_enabled already was once the
        # transfer ends.
        self._transfer_active = False
        self._transfer_lock = threading.Lock()

        self._stop_event = threading.Event()

        # ---- 3-channel link ----
        self.channels = [
            TcpChannel(self, cfg["name"], cfg["host"], cfg["port"], priority)
            for priority, cfg in enumerate(CHANNELS_CONFIG)
        ]
        # Protects structural changes to self.channels (append/remove) now
        # that manual/ad-hoc connections (see ManualConnectionServer below)
        # can grow or shrink this list at any time during the node's life,
        # unlike the fixed CHANNELS_CONFIG-based set above which never
        # changes after __init__. Every read site below takes a snapshot
        # via _channels_snapshot() rather than iterating self.channels
        # directly, so a concurrent append/remove can never corrupt an
        # in-progress iteration (plain list iteration has no such
        # protection the way dict/set do).
        self._channels_lock = threading.Lock()
        # Manual channels always sort after every CHANNELS_CONFIG channel
        # in the priority-ordered active/secondary selection (see
        # _update_roles) - true backups, never preempting the tether by
        # default. Each new manual connection gets the next value up.
        self._next_manual_priority = 10000
        self._active_name = None
        self._secondary_name = None
        self._forced_active = None    # operator override from the GCS "Force" button, else None = automatic
        self._roles_lock = threading.Lock()

        # Total-link-loss failsafe latch - see _update_roles(). Declared here,
        # before any channel starts, since _update_roles() reads it.
        self._link_ever_up = False
        self._link_lost_latched = False
        self.declare_parameter('estop_on_link_loss', False)

        # File transfers are handled by a single long-lived handler; which
        # channel a given transfer runs over is passed in per-call (whatever
        # channel the request arrived on), not fixed at construction time.
        self.file_handler = FileTransferHandler(self.get_logger())

        # Bridges the GCS Mission Planner's start/cancel commands to
        # mission_handler's action server. Only one mission goal is tracked
        # at a time, matching the file transfer handler's single-in-flight
        # convention above.
        self.mission_action_client = ActionClient(self, RunMissionTree, 'run_mission_tree')
        self._mission_goal_handle = None
        # Standard ROS2 action-server cancel service, called directly (not
        # via a ClientGoalHandle) so "Cancel All Missions" can clear EVERY
        # goal the server has - not just the one _mission_goal_handle above
        # happens to be tracking. That single-handle field gets overwritten
        # every time a new mission starts, so if an operator starts a fresh
        # mission without cancelling a still-running one first (the exact
        # "forgot to cancel, uploaded a new one" scenario this button is
        # for), the old goal becomes unreachable through _cancel_mission -
        # only the action server itself still knows about it. Per the
        # actions protocol (REP-2007), a CancelGoal.Request with an empty
        # goal_id (all-zero UUID, zero stamp) means "cancel every goal".
        self._mission_cancel_client = self.create_client(CancelGoal, 'run_mission_tree/_action/cancel_goal')
        # Analytics tracker for the currently-running mission (None when
        # idle) - see _start_mission_tracking/_finish_mission_tracking.
        # Always-subscribed rather than created per-mission since it's a
        # cheap no-op while _mission_track is None.
        self._mission_track = None
        self.create_subscription(Odometry, "/odometry/global", self._on_odom_for_tracking, 10)
        self.file_handler.start()

        for ch in self.channels:
            ch.start()

        # Manual/ad-hoc backup connections (GCS "Manual Connection" tab) -
        # always on standby alongside the primary channels above, never
        # replacing them (see MANUAL_LISTEN_PORT's comment and _add_channel's
        # lowest-priority assignment).
        self.manual_server = ManualConnectionServer(self, MANUAL_LISTEN_PORT)
        self.manual_server.start()
        self.manual_ws_server = WebSocketManualServer(self, MANUAL_WS_LISTEN_PORT)
        self.manual_ws_server.start()

        # Timers (telemetry rate adapted for tethered link)
        self.create_timer(0.2, self._publish_telemetry)    # 5 Hz
        self.create_timer(0.5, self._publish_compass)       # 2 Hz - only actually sends while mag_stream_enabled
        self.create_timer(1.0, self._publish_state)
        self.create_timer(HEARTBEAT_INTERVAL_SEC, self._send_heartbeat)
        self.create_timer(ROLE_UPDATE_INTERVAL_SEC, self._update_roles)

        chans = ", ".join(f"{c.name}={c.host}:{c.port}" for c in self.channels)
        self.get_logger().info(f"TCP client configured with {len(self.channels)} channels: {chans}")

    # ---------- Callbacks for sensor topics ----------
    def imu_callback(self, msg):
        self.latest['imu'] = msg

    def depth_callback(self, msg):
        self.latest['depth'] = msg

    def _depth_hold_status_callback(self, msg):
        try:
            status = json.loads(msg.data)
        except json.JSONDecodeError:
            return
        with self._depth_hold_lock:
            self.depth_hold_status = status
            self.depth_hold_status_time = time.monotonic()

    def mag_callback(self, msg):
        self.latest['mag'] = msg

    def pressure_callback(self, msg):
        self.latest['pressure'] = msg

    def temp_callback(self, msg):
        self.latest['temperature'] = msg

    def humidity_callback(self, msg):
        self.latest['humidity'] = msg

    def battery_callback(self, msg):
        self.latest['battery'] = msg

    def relay_callback(self, msg):
        self.latest['relay'] = msg

    def gps_callback(self, msg):
        self.latest['gps'] = msg

    def save_black_box(self, data_type, type_name):
        data = {"data_type": data_type, "type_name": type_name}
        data_json = json.dumps(data)
        self.black_box_pub.publish(String(data=data_json))

    def _blackbox_callback(self, msg: String):
        """Relay /hardware/black_box entries to the GCS console, dropping
        the connection-string ones (see BLACKBOX_FILTERED_TYPES). Gated by
        blackbox_stream_enabled (default off) - continuous link-management
        noise isn't worth the bytes on a 10kbps tether unless the operator
        actually has that console open."""
        with self._blackbox_stream_lock:
            if not self.blackbox_stream_enabled:
                return
        try:
            data = json.loads(msg.data)
        except (json.JSONDecodeError, TypeError):
            return
        data_type = data.get("data_type", "")
        if data_type in BLACKBOX_FILTERED_TYPES:
            return
        channel = self._active_channel()
        if channel is None:
            return
        channel.send(pack_json(PKT_JSON, {
            "type": "blackbox",
            "ts": time.time(),
            "data_type": data_type,
            "message": json.dumps(data),
        }))

    def leak_callback(self, msg):
        leaks = []
        for marker in msg.markers:
            leaks.append(marker.color.r > 0.5)
        if len(leaks) == 4:
            self.latest['leaks'] = leaks
        else:
            self.get_logger().warn(f"Unexpected leak markers count: {len(leaks)}")

    # ---------- Rosbag control helpers ----------
    def _send_rosbag_command(self, command: str):
        """
        Publish a rosbag control command to /ros2_bag_control.
        Equivalent to: ros2 topic pub -1 /ros2_bag_control std_msgs/msg/String "data: '<command>'"
        command must be 'start', 'stop', or 'save'.
        """
        valid = {"start", "stop", "save"}
        if command not in valid:
            self.get_logger().error(f"Invalid rosbag command: {command}")
            return False
        self.rosbag_control_pub.publish(String(data=command))
        self.get_logger().info(f"Rosbag command sent: {command}")
        self.save_black_box("rosbag_control", f"Rosbag command: {command}")
        return True

    # ---------- Channel role management ----------
    def _update_roles(self):
        """
        Recompute which channel is "active" (drives outbound telemetry
        authority + is where GCS commands are expected) and which is
        "secondary" (mirrored telemetry, shown as a live second feed in the
        GCS UI). Any other connected channel is a warm "backup".

        Sticky: a channel keeps its active/secondary role even after a
        higher-priority channel reconnects - it's only replaced once it
        itself disconnects. This avoids flapping back and forth between
        channels on a marginal link.
        """
        channels = self._channels_snapshot()
        with self._roles_lock:
            connected = sorted((c for c in channels if c.connected), key=lambda c: c.priority)
            connected_names = {c.name for c in connected}

            # Operator override (GCS "Force" button) wins whenever the
            # target channel is actually connected; otherwise fall back to
            # the normal sticky-priority selection below.
            if self._forced_active is not None and self._forced_active in connected_names:
                self._active_name = self._forced_active
            elif self._active_name not in connected_names:
                self._active_name = connected[0].name if connected else None

            sec_candidates = [c for c in connected if c.name != self._active_name]
            sec_names = {c.name for c in sec_candidates}
            if self._secondary_name not in sec_names:
                self._secondary_name = sec_candidates[0].name if sec_candidates else None

            active_name = self._active_name
            secondary_name = self._secondary_name

        for ch in channels:
            if not ch.connected:
                ch.role = "down"
            elif ch.name == active_name:
                ch.role = "active"
            elif ch.name == secondary_name:
                ch.role = "secondary"
            else:
                ch.role = "backup"

        # ---- Total link loss ----
        # Edge-triggered ONCE when every channel is down, having previously
        # had at least one connected (never on cold boot). By default it only
        # logs: this is an autonomous vehicle and the tether is for monitoring
        # and emergency joystick, but the e-stop this used to engage stays
        # latched until a manual E-STOP OFF, so every routine all-channel drop
        # (a GCS restart, the RS-485 channels reconnecting) left the vehicle
        # stopped with nothing on the GCS saying why. A joystick that loses
        # the link is still neutralised by thrustmaster.py's cmd_vel timeout.
        # estop_on_link_loss:=true restores the old behaviour: one
        # Bool(True), held by the hardware interface until E-STOP OFF.
        if active_name is not None:
            self._link_ever_up = True
            self._link_lost_latched = False
        elif self._link_ever_up and not self._link_lost_latched:
            self._link_lost_latched = True
            if self.get_parameter('estop_on_link_loss').value:
                self.get_logger().error(
                    "All tether channels down - engaging emergency stop (fins->90deg, thruster->1500)"
                )
                self.emergency_stop_pub.publish(Bool(data=True))
                self.save_black_box(
                    "failsafe",
                    "Tether link lost on all channels - emergency stop engaged (fins 90deg / thruster 1500)"
                )
            else:
                self.get_logger().warning(
                    "All tether channels down - no e-stop (estop_on_link_loss is false)")
                self.save_black_box(
                    "failsafe", "Tether link lost on all channels - no e-stop (estop_on_link_loss false)")

    def _force_active(self, name):
        """Pin the active channel to `name` (operator override from the GCS
        "Force" button), or clear back to automatic selection if `name` is
        falsy/unknown. Only takes effect once that channel is connected -
        see _update_roles()."""
        valid = {c.name for c in self._channels_snapshot()}
        with self._roles_lock:
            self._forced_active = name if name in valid else None
        self.get_logger().info(f"Active channel override: {self._forced_active or 'AUTO'}")

    def _channels_snapshot(self):
        """Point-in-time copy of self.channels, safe to iterate even while
        a manual channel is concurrently being added/removed on another
        thread - see the _channels_lock comment in __init__."""
        with self._channels_lock:
            return list(self.channels)

    def _add_channel(self, name, host, port) -> "TcpChannel":
        """Register a new manual/ad-hoc channel (see ManualConnectionServer
        and the WebSocket server) - always lowest priority, so it can only
        ever become "active" if every CHANNELS_CONFIG channel is down,
        matching "the current [tether] method is always on and default."
        """
        with self._channels_lock:
            priority = self._next_manual_priority
            self._next_manual_priority += 1
            ch = TcpChannel(self, name, host, port, priority)
            self.channels.append(ch)
        return ch

    def _remove_channel(self, ch: "TcpChannel"):
        """Drop a manual channel once its one-shot session ends (see
        TcpChannel._manual_session) - manual connections are ephemeral by
        nature and aren't redialled, so leaving them in the list would just
        accumulate permanently-"down" clutter in the GCS's channel list."""
        with self._channels_lock:
            if ch in self.channels:
                self.channels.remove(ch)
        self.get_logger().info(f"[{ch.name}] Manual channel removed")
        self._update_roles()

    def _active_channel(self):
        name = self._active_name
        for ch in self._channels_snapshot():
            if ch.name == name:
                return ch
        return None

    # ---------- TCP packet dispatch ----------
    def _dispatch_packet(self, channel: TcpChannel, msg_type, payload):
        try:
            if msg_type == PKT_HEARTBEAT:
                channel.send(pack_message(PKT_HEARTBEAT_ACK, b""))
            elif msg_type == PKT_HEARTBEAT_ACK:
                pass
            elif msg_type == PKT_JSON:
                # Only PKT_JSON payloads are UTF-8 text; never call
                # .decode() on any other packet type.
                try:
                    text = payload.decode("utf-8")
                    msg = json.loads(text)
                    self._handle_json(msg, channel)
                    self.incoming_pub.publish(String(data=text))
                except (UnicodeDecodeError, json.JSONDecodeError) as e:
                    self.get_logger().error(f"[{channel.name}] Bad JSON packet: {e}")
            elif msg_type in (PKT_FILE_REQ, PKT_FILE_CHUNK, PKT_FILE_COMPLETE):
                # Binary file packets go straight to FileTransferHandler;
                # they MUST NOT be decoded as UTF-8. Replies go back out on
                # the same channel the request came in on.
                self.file_handler.handle_packet(msg_type, payload, channel)
            else:
                self.get_logger().warning(f"[{channel.name}] Unknown packet type: {msg_type:02x}")
        except Exception as e:
            # Catch-all so one bad packet never kills the channel.
            self.get_logger().error(f"[{channel.name}] Packet handling error: {e}")

    # ---------- JSON command handler ----------
    def _handle_json(self, msg, channel: TcpChannel):
        cmd_type = msg.get("type")
        if cmd_type == "command":
            task = msg.get("task")
            self._handle_command(task, channel)
        elif cmd_type == "teleop":
            axes = [float(a) for a in msg.get("axes", [])]
            buttons = [int(bool(b)) for b in msg.get("buttons", [])]
            # See MIN_JOY_AXES/MIN_JOY_BUTTONS comment above - pad so a
            # short payload can never crash the downstream fin/thruster
            # controller.
            if len(axes) < MIN_JOY_AXES:
                axes += [0.0] * (MIN_JOY_AXES - len(axes))
            if len(buttons) < MIN_JOY_BUTTONS:
                buttons += [0] * (MIN_JOY_BUTTONS - len(buttons))
            joy_data = Joy()
            joy_data.axes = axes
            joy_data.buttons = buttons
            self.joy_pub.publish(joy_data)
            channel.send(pack_json(PKT_JSON, {"type": "teleop_ack", "success": True}))
        elif cmd_type == "list_topics":
            # GCS is asking for the live ROS2 graph so the operator can see
            # which topics are actually up.
            topics = self._list_ros_topics()
            channel.send(pack_json(PKT_JSON, {"type": "topic_list", "topics": topics}))
        elif cmd_type == "run_command":
            # Remote Terminal panel. Runs in its own background thread +
            # subprocess (see _run_shell_command) so it never blocks this
            # dispatch loop or the rest of the node, regardless of how long
            # it takes or whether it hangs.
            command = msg.get("command", "")
            request_id = msg.get("request_id")
            timeout = self._clamp_command_timeout(msg.get("timeout"))
            max_lines = self._clamp_command_max_lines(msg.get("max_lines"))
            if command.strip():
                threading.Thread(
                    target=self._run_shell_command,
                    args=(command, request_id, timeout, max_lines, channel),
                    daemon=True,
                ).start()
            else:
                channel.send(pack_json(PKT_JSON, {
                    "type": "command_result", "request_id": request_id, "command": command,
                    "success": False, "exit_code": None, "timed_out": False,
                    "output": "(empty command)", "truncated": False, "max_lines": max_lines,
                }))
        elif cmd_type == "list_params":
            # ROS2 Parameters panel. Gathering runs in its own background
            # thread - it shells out to `ros2 param dump` once per live node
            # (see _list_ros_params), which can take a few seconds total on
            # a system with many nodes, so it must never block the dispatch
            # loop the same way run_command/list_files/list_topics don't.
            threading.Thread(target=self._handle_list_params, args=(channel,), daemon=True).start()
        elif cmd_type == "set_param":
            node_name = msg.get("node", "")
            param_name = msg.get("name", "")
            value = msg.get("value")
            request_id = msg.get("request_id")
            threading.Thread(
                target=self._set_ros_param,
                args=(node_name, param_name, value, request_id, channel),
                daemon=True,
            ).start()
        elif cmd_type == "reboot_system":
            # Deliberately its OWN top-level cmd_type, not folded into the
            # generic "command"/task pattern - `password` only belongs on
            # this one specific request and must never accidentally ride
            # along on any other command. Runs in its own thread (see
            # _do_reboot) so this dispatch call returns immediately; the
            # password local var here is not logged and is dropped as soon
            # as _do_reboot is done with it.
            password = msg.get("password") or ""
            threading.Thread(target=self._do_reboot, args=(password,), daemon=True).start()
        elif cmd_type == "set_mode":
            mode = msg.get("mode")
            if mode in ("manual", "auto", "teleop"):
                self.mode = mode
                self.get_logger().info(f"Mode changed to {mode}")
                channel.send(pack_json(PKT_JSON, {"type": "mode_ack", "success": True, "mode": mode}))
            else:
                channel.send(pack_json(PKT_JSON, {"type": "error", "message": "Invalid mode"}))
        elif cmd_type == "request_state":
            channel.send(pack_json(PKT_JSON, self._get_state_dict()))
        elif cmd_type == "list_files":
            # GCS is asking for the directory tree under jetson_files
            tree = self._list_jetson_files()
            channel.send(pack_json(PKT_JSON, {"type": "file_list", "tree": tree}))
        elif cmd_type == "list_maps":
            # "Start Fast Mission" flow: GCS wants to know which maps are
            # already saved on the vehicle (uploaded once, e.g. over a fast
            # Ethernet/manual connection - see FileTransferHandler's
            # category="map" destination) so it can start a mission by
            # referencing one by name instead of re-transferring pgm+yaml
            # over a slow tether every time. Local directory scan, fast
            # enough to answer inline rather than needing its own thread.
            maps = self._list_saved_maps()
            channel.send(pack_json(PKT_JSON, {"type": "map_list", "maps": maps}))
        elif cmd_type == "delete_file":
            # GCS requests deletion of a specific file
            rel_path = msg.get("path", "")
            result = self._delete_jetson_file(rel_path)
            channel.send(pack_json(PKT_JSON, result))
        elif cmd_type == "delete_folder":
            # GCS requests deletion of a whole folder (recursive) - kept as
            # its own cmd_type rather than a flag on delete_file since it's
            # a meaningfully more destructive operation.
            rel_path = msg.get("path", "")
            result = self._delete_jetson_folder(rel_path)
            channel.send(pack_json(PKT_JSON, result))
        elif cmd_type == "download_folder":
            # Zips the folder server-side and sends it back over the same
            # PKT_FILE_REQ/CHUNK/COMPLETE protocol single-file downloads
            # already use (see _handle_download_folder) - runs in its own
            # thread since zipping a large folder takes real time and must
            # never block this dispatch loop. Deliberately its own,
            # independent code path rather than reusing/refactoring
            # FileTransferHandler's existing download branch, so nothing
            # about it can risk the proven single-file download path.
            rel_path = msg.get("path", "")
            threading.Thread(target=self._handle_download_folder, args=(rel_path, channel), daemon=True).start()
        elif cmd_type == "sync_config":
            filename = msg.get("filename", "config.yaml")
            self.get_logger().info(f"Sync config requested: {filename}")
            channel.send(pack_json(PKT_FILE_REQ, {
                "filename": filename,
                "direction": "download",
                "size": Path(filename).stat().st_size if Path(filename).exists() else 0
            }))
        elif cmd_type == "sync_behavior_tree":
            filename = msg.get("filename", "bt.xml")
            self.get_logger().info(f"Sync behavior tree requested: {filename}")
            channel.send(pack_json(PKT_FILE_REQ, {
                "filename": filename,
                "direction": "download",
                "size": Path(filename).stat().st_size if Path(filename).exists() else 0
            }))
        elif cmd_type == "start_mission":
            # wait_for_server() blocks, so this runs off the dispatch loop's
            # thread -- same reasoning as run_command/list_params above.
            threading.Thread(target=self._start_mission, args=(msg, channel), daemon=True).start()
        elif cmd_type == "cancel_mission":
            self._cancel_mission(channel)
        elif cmd_type == "cancel_all_missions":
            self._cancel_all_missions(channel)
        elif cmd_type == "wifi_scan":
            # Live Ops "Wifi Comms" panel's List Devices button - a real
            # rescan takes a few seconds, so this runs off the dispatch
            # thread (see _wifi_scan). `sudo_password` is optional - only
            # present when the operator typed one into the panel's own
            # Sudo Password field (needed unless passwordless sudo is
            # provisioned for nmcli - see _wifi_scan's docstring).
            sudo_password = msg.get("sudo_password") or ""
            threading.Thread(target=self._wifi_scan, args=(sudo_password, channel), daemon=True).start()
        elif cmd_type == "wifi_saved_list":
            # Fast, sudo-free read of NetworkManager's own saved profiles -
            # independent of wifi_scan so the "Saved Devices" mini-section
            # can populate as soon as the panel loads, not only after a scan.
            threading.Thread(target=self._wifi_saved_list, args=(channel,), daemon=True).start()
        elif cmd_type == "wifi_connect":
            # `password` is only present when connecting to a network for
            # the first time - reconnecting to an already-saved network
            # from the GCS omits it entirely (see _wifi_connect).
            # `sudo_password` is the operator's OWN sudo password (separate
            # from the wifi network's password above).
            ssid = msg.get("ssid") or ""
            password = msg.get("password")
            sudo_password = msg.get("sudo_password") or ""
            threading.Thread(target=self._wifi_connect, args=(ssid, password, sudo_password, channel), daemon=True).start()
        else:
            self.get_logger().warning(f"Unknown JSON type: {cmd_type}")

    # ---------- Mission Planner bridge ----------
    @staticmethod
    def _resolve_jetson_path(path_str: str) -> str:
        """Mission map paths arrive as filenames/relative paths (e.g.
        "map/site1.yaml", matching the upload category dir under
        JETSON_FILES_DIR) - nav2_map_server's LoadMap service needs a path
        it can actually open regardless of its own process CWD, so resolve
        anything non-absolute against JETSON_FILES_DIR before it goes into
        the RunMissionTree goal."""
        if not path_str:
            return path_str
        p = Path(path_str)
        return str(p) if p.is_absolute() else str(JETSON_FILES_DIR / p)

    # ---------- Mission analytics (distance/time/speed summary) ----------
    def _on_odom_for_tracking(self, msg: Odometry):
        """Accumulates total path length for the currently-running mission
        by summing consecutive position deltas from the fused map-frame
        odometry. No-op while no mission is tracked."""
        mt = self._mission_track
        if mt is None:
            return
        p = msg.pose.pose.position
        pos = (p.x, p.y)
        if mt["last_pos"] is not None:
            mt["total_distance_m"] += math.hypot(pos[0] - mt["last_pos"][0], pos[1] - mt["last_pos"][1])
        mt["last_pos"] = pos

    def _start_mission_tracking(self, msg, mission_id: str):
        now = time.time()
        self._mission_track = {
            "mission_id": mission_id,
            "start_lat": float(msg.get("start_lat", 0.0)), "start_lon": float(msg.get("start_lon", 0.0)),
            "goal_lat": float(msg.get("goal_lat", 0.0)), "goal_lon": float(msg.get("goal_lon", 0.0)),
            "map_yaml_path": msg.get("map_yaml_path", ""), "map_pgm_path": msg.get("map_pgm_path", ""),
            "waypoint_count": len(msg.get("waypoint_list", [])),
            "configured_start_delay_sec": float(msg.get("start_delay_sec", 0.0)),
            "start_time": now,
            "last_pos": None,
            "total_distance_m": 0.0,
            "current_waypoint_index": 0,
            # One entry per "leg" boundary (start -> waypoint 1 -> ... ->
            # goal), each recording cumulative distance/time AT that
            # boundary - per-leg stats are the deltas between consecutive
            # entries, computed once at mission end (see
            # _finish_mission_tracking).
            "checkpoints": [{"label": "start", "t": now, "dist": 0.0}],
        }

    def _on_mission_feedback_for_tracking(self, fb):
        mt = self._mission_track
        if mt is None:
            return
        if fb.current_waypoint_index != mt["current_waypoint_index"]:
            mt["checkpoints"].append({
                "label": f"waypoint_{mt['current_waypoint_index'] + 1}",
                "t": time.time(), "dist": mt["total_distance_m"],
            })
            mt["current_waypoint_index"] = fb.current_waypoint_index

    def _finish_mission_tracking(self, result_status: str):
        """Ends tracking for whatever mission was running and returns its
        summary dict, or None if no mission was being tracked (e.g. the
        goal was rejected before tracking started)."""
        mt = self._mission_track
        self._mission_track = None
        if mt is None:
            return None
        now = time.time()
        mt["checkpoints"].append({"label": "goal", "t": now, "dist": mt["total_distance_m"]})

        legs = []
        checkpoints = mt["checkpoints"]
        for i in range(1, len(checkpoints)):
            a, b = checkpoints[i - 1], checkpoints[i]
            dist = max(0.0, b["dist"] - a["dist"])
            dur = max(0.0, b["t"] - a["t"])
            legs.append({
                "from": a["label"], "to": b["label"],
                "distance_m": round(dist, 2), "duration_s": round(dur, 2),
                "avg_speed_mps": round(dist / dur, 3) if dur > 0 else 0.0,
            })

        total_distance = mt["total_distance_m"]
        total_duration = max(0.0, now - mt["start_time"])
        return {
            "mission_id": mt["mission_id"],
            "result_status": result_status,
            "start_lat": mt["start_lat"], "start_lon": mt["start_lon"],
            "goal_lat": mt["goal_lat"], "goal_lon": mt["goal_lon"],
            "map_yaml_path": mt["map_yaml_path"], "map_pgm_path": mt["map_pgm_path"],
            "waypoint_count": mt["waypoint_count"],
            # duration_s below is wall-clock accept-to-result, so it
            # INCLUDES this configured delay (the tether-disconnect grace
            # period) - subtract it out if "pure navigation time" is wanted.
            "configured_start_delay_sec": mt["configured_start_delay_sec"],
            "start_time_unix": mt["start_time"], "end_time_unix": now,
            "legs": legs,
            "total": {
                "distance_m": round(total_distance, 2),
                "duration_s": round(total_duration, 2),
                "avg_speed_mps": round(total_distance / total_duration, 3) if total_duration > 0 else 0.0,
            },
        }

    def _save_mission_summary(self, summary):
        """Writes the summary (and, best-effort, a copy of the map this
        mission actually used) to its own uniquely-named folder under
        JETSON_FILES_DIR/mission_history, so each mission's files are grouped
        and identifiable by folder name alone."""
        if summary is None:
            return
        ts = time.strftime("%Y%m%d_%H%M%S", time.localtime(summary["start_time_unix"]))
        folder = JETSON_FILES_DIR / "mission_history" / f"{ts}_{summary['mission_id']}"
        try:
            folder.mkdir(parents=True, exist_ok=True)
            (folder / "summary.json").write_text(json.dumps(summary, indent=2))
            for key in ("map_yaml_path", "map_pgm_path"):
                src = summary.get(key)
                if not src:
                    continue
                src_path = Path(src)
                if src_path.is_file():
                    shutil.copy2(src_path, folder / src_path.name)
            self.get_logger().info(f"Mission summary saved: {folder}")
        except OSError as e:
            self.get_logger().error(f"Failed to save mission summary to {folder}: {e}")

    def _start_mission(self, msg, channel: TcpChannel):
        if not self.mission_action_client.wait_for_server(timeout_sec=5.0):
            channel.send(pack_json(PKT_JSON, {
                "type": "mission_ack", "success": False,
                "message": "run_mission_tree action server not available",
            }))
            return

        goal = RunMissionTree.Goal()
        goal.tree_path = msg.get("tree_path", "")
        goal.pre_blackboard_config = msg.get("pre_blackboard_config", "")
        goal.restart_on_failure = bool(msg.get("restart_on_failure", False))
        goal.start_delay_sec = max(0.0, float(msg.get("start_delay_sec", 0.0)))
        goal.start_lat = float(msg.get("start_lat", 0.0))
        goal.start_lon = float(msg.get("start_lon", 0.0))
        goal.start_north = float(msg.get("start_north", 0.0))
        goal.goal_lat = float(msg.get("goal_lat", 0.0))
        goal.goal_lon = float(msg.get("goal_lon", 0.0))
        goal.map_origin_north = float(msg.get("map_origin_north", 0.0))
        goal.map_resolution = float(msg.get("map_resolution", 0.0))
        goal.map_yaml_path = self._resolve_jetson_path(msg.get("map_yaml_path", ""))
        goal.map_pgm_path = self._resolve_jetson_path(msg.get("map_pgm_path", ""))
        goal.waypoint_list = [
            Point(x=float(pt[0]), y=float(pt[1]), z=0.0) for pt in msg.get("waypoint_list", [])
        ]

        def _feedback_cb(feedback_msg, ch=channel):
            fb = feedback_msg.feedback
            self._on_mission_feedback_for_tracking(fb)
            ch.send(pack_json(PKT_JSON, {
                "type": "mission_feedback",
                "status": fb.status,
                "progress": fb.progress,
                "current_waypoint_index": fb.current_waypoint_index,
                "total_waypoints": fb.total_waypoints,
            }))

        def _goal_response_cb(future, ch=channel):
            goal_handle = future.result()
            if not goal_handle.accepted:
                ch.send(pack_json(PKT_JSON, {
                    "type": "mission_ack", "success": False, "message": "Goal rejected",
                }))
                return
            self._mission_goal_handle = goal_handle
            mission_id = ''.join(f'{b:02x}' for b in goal_handle.goal_id.uuid)
            self._start_mission_tracking(msg, mission_id)
            ch.send(pack_json(PKT_JSON, {"type": "mission_ack", "success": True}))
            goal_handle.get_result_async().add_done_callback(
                lambda f, ch=ch: self._mission_result_cb(f, ch)
            )

        send_future = self.mission_action_client.send_goal_async(goal, feedback_callback=_feedback_cb)
        send_future.add_done_callback(_goal_response_cb)

    def _mission_result_cb(self, future, channel: TcpChannel):
        result = future.result().result
        self._mission_goal_handle = None
        try:
            logs_and_errors = json.loads(result.logs_and_errors or "[]")
        except json.JSONDecodeError:
            logs_and_errors = []
        summary = self._finish_mission_tracking(result.status)
        if summary is not None:
            summary["logs_and_errors"] = logs_and_errors
        self._save_mission_summary(summary)
        channel.send(pack_json(PKT_JSON, {
            "type": "mission_result",
            "status": result.status,
            "mission_history_path": result.mission_history_path,
            "logs_and_errors": logs_and_errors,
            "summary": summary,
        }))

    def _cancel_mission(self, channel: TcpChannel):
        if self._mission_goal_handle is None:
            channel.send(pack_json(PKT_JSON, {
                "type": "mission_ack", "success": False, "message": "No active mission",
            }))
            return
        self._mission_goal_handle.cancel_goal_async()
        channel.send(pack_json(PKT_JSON, {
            "type": "mission_ack", "success": True, "message": "Cancel requested",
        }))

    def _cancel_all_missions(self, channel: TcpChannel):
        """
        "Cancel All Missions" - Mission Planner's fast reset for "forgot to
        cancel the running mission before uploading/starting a new one".

        Deliberately does NOT go through _mission_goal_handle.cancel_goal_
        async() like _cancel_mission above - that only ever knows about the
        SINGLE most-recently-started goal (see the comment on
        self._mission_cancel_client in __init__), so it can't reach a goal
        that got silently orphaned by starting a second mission without
        cancelling the first. Instead this calls the action server's own
        cancel_goal service directly with an empty goal_id, which per the
        ROS2 actions protocol (REP-2007) means "cancel every goal this
        server currently has, not just one" - a clean reset regardless of
        what this node's own bookkeeping still remembers.
        """
        if not self._mission_cancel_client.service_is_ready():
            channel.send(pack_json(PKT_JSON, {
                "type": "mission_ack", "success": False,
                "message": "cancel_goal service not available",
            }))
            return
        request = CancelGoal.Request()   # empty goal_id + zero stamp = cancel ALL goals
        future = self._mission_cancel_client.call_async(request)

        def _done(f, ch=channel):
            self._mission_goal_handle = None
            try:
                result = f.result()
                n = len(result.goals_canceling)
                ch.send(pack_json(PKT_JSON, {
                    "type": "mission_ack", "success": True,
                    "message": f"Cancel-all requested ({n} goal(s) being cancelled)",
                }))
            except Exception as e:
                ch.send(pack_json(PKT_JSON, {
                    "type": "mission_ack", "success": False, "message": str(e),
                }))
        future.add_done_callback(_done)

    def _handle_command(self, task, channel: TcpChannel):
        valid = {
            "arm", "disarm",
            "lights_on", "lights_off",
            "start_recording", "stop_recording", "save_recording",
            "enable_telemetry", "disable_telemetry",
            "enable_blackbox_stream", "disable_blackbox_stream",
            "enable_mag_stream", "disable_mag_stream",
            "enable_depth_hold", "disable_depth_hold",
        }
        if task not in valid:
            channel.send(pack_json(PKT_JSON, {
                "type": "command_ack", "success": False,
                "message": f"Unknown command: {task}"
            }))
            return
        self.get_logger().info(f"Executing command: {task}")

        if task == "arm":
            self.armed = True

        elif task == "disarm":
            self.armed = False

        elif task == "lights_on":
            self.get_logger().info("Emergency stop turned ON")
            self.emergency_stop_pub.publish(Bool(data=True))
            self.save_black_box("command", "Emergency stop turned ON")

        elif task == "lights_off":
            self.get_logger().info("Emergency stop turned OFF")
            self.emergency_stop_pub.publish(Bool(data=False))
            self.save_black_box("command", "Emergency stop turned OFF")

        elif task == "start_recording":
            with self._recording_lock:
                self._send_rosbag_command("start")
                self.recording_active = True
            self.get_logger().info("Rosbag recording STARTED")
            self.save_black_box("rosbag", "Recording started")

        elif task == "stop_recording":
            with self._recording_lock:
                self._send_rosbag_command("stop")
                self.recording_active = False
            self.get_logger().info("Rosbag recording STOPPED")
            self.save_black_box("rosbag", "Recording stopped")

        elif task == "save_recording":
            # Save (checkpoint) the current bag without stopping it
            with self._recording_lock:
                self._send_rosbag_command("save")
            self.get_logger().info("Rosbag save/checkpoint requested")
            self.save_black_box("rosbag", "Recording save requested")

        elif task == "enable_telemetry":
            with self._telemetry_lock:
                self.telemetry_enabled = True
            self.get_logger().info("Telemetry stream ENABLED")

        elif task == "disable_telemetry":
            with self._telemetry_lock:
                self.telemetry_enabled = False
            self.get_logger().info("Telemetry stream DISABLED (bandwidth reserved for joystick)")

        elif task == "enable_blackbox_stream":
            with self._blackbox_stream_lock:
                self.blackbox_stream_enabled = True
            self.get_logger().info("Black Box console streaming ENABLED")

        elif task == "disable_blackbox_stream":
            with self._blackbox_stream_lock:
                self.blackbox_stream_enabled = False
            self.get_logger().info("Black Box console streaming DISABLED (bandwidth reserved)")

        elif task == "enable_mag_stream":
            with self._mag_lock:
                self.mag_stream_enabled = True
            self.get_logger().info("Compass/magnetometer stream ENABLED")

        elif task == "disable_mag_stream":
            with self._mag_lock:
                self.mag_stream_enabled = False
            self.get_logger().info("Compass/magnetometer stream DISABLED (bandwidth reserved)")

        elif task == "enable_depth_hold":
            self._set_depth_hold(True)

        elif task == "disable_depth_hold":
            self._set_depth_hold(False)

        channel.send(pack_json(PKT_JSON, {
            "type": "command_ack", "success": True,
            "message": f"Command {task} executed"
        }))

    def _set_depth_hold(self, enabled: bool):
        with self._depth_hold_lock:
            self.depth_hold_enabled = enabled
        self.depth_hold_enable_pub.publish(Bool(data=enabled))
        word = "ON" if enabled else "OFF"
        self.get_logger().info(f"Depth hold turned {word}")
        self.save_black_box("command", f"Depth hold turned {word}")

    def _do_reboot(self, password: str):
        """
        Reboot the whole Jetson.

        Tries passwordless sudo first when no password was supplied
        (`sudo -n` - fails fast and cleanly if that's not configured,
        rather than hanging on a TTY prompt that will never come). If a
        password WAS supplied (operator retried via the GCS's password
        prompt after seeing "a password is required"), it's piped to
        `sudo -S` over stdin - deliberately NOT a command-line argument
        (those show up in `ps`/process listings on the Jetson for the
        command's lifetime), and NEVER logged or written to black_box
        anywhere in this method - only sudo's own response text
        (`message` below) is, which never contains the password itself.

        If reboot actually succeeds, the machine goes down within a second
        or two and there's nothing further to report - the GCS sees that
        as a link drop, same as any other disconnect. The case that
        matters here is FAILURE: this replies with a "reboot_result" over
        every connected channel (NOT gated by blackbox_stream_enabled -
        default off - this is direct feedback on an operator-initiated
        action, not passive background telemetry), with `needs_password`
        set whenever sudo's own message indicates a password would fix it
        (also covers a wrong password: sudo's retry-failure message also
        contains "password"), so the GCS can prompt and retry without the
        operator having to guess why it failed.
        """
        try:
            if password:
                result = subprocess.run(
                    ["sudo", "-S", "reboot"],
                    input=password + "\n",
                    capture_output=True, text=True, timeout=10.0,
                )
            else:
                result = subprocess.run(
                    ["sudo", "-n", "reboot"],
                    capture_output=True, text=True, timeout=10.0,
                )
            if result.returncode == 0:
                # Machine is going down now - nothing left to report.
                return
            detail = ((result.stdout or "") + (result.stderr or "")).strip() or f"exit code {result.returncode}"
            success, message = False, detail
        except FileNotFoundError:
            success, message = False, "'sudo' not found on PATH"
        except subprocess.TimeoutExpired:
            success, message = False, "reboot command did not return within 10s"
        except Exception as e:
            success, message = False, str(e)
        finally:
            password = None   # drop reference - not used again below

        needs_password = "password" in message.lower()
        self.get_logger().error(f"Reboot FAILED: {message}")
        self.save_black_box("command", f"Reboot FAILED: {message}")
        packet = pack_json(PKT_JSON, {
            "type": "reboot_result", "success": success, "message": message,
            "needs_password": needs_password,
        })
        for ch in self._channels_snapshot():
            if ch.connected:
                ch.send(packet)

    # ---------- Wifi Comms ----------
    @staticmethod
    def _nmcli_split(line: str):
        """Split one line of `nmcli -t -f ...` terse output on unescaped
        ':' - nmcli backslash-escapes literal ':' and '\\' inside field
        values (e.g. an SSID that itself contains a colon), so a naive
        line.split(':') would silently misalign fields for such networks."""
        fields = re.split(r'(?<!\\):', line)
        return [f.replace('\\:', ':').replace('\\\\', '\\') for f in fields]

    def _wifi_saved_connection_names(self):
        """SSIDs of wifi connection profiles NetworkManager already has
        saved on this Jetson (`nmcli connection show`, filtered to wifi
        type). No sudo needed - this only reads NM's own local config, it
        never touches the radio or does a live over-the-air scan."""
        try:
            result = subprocess.run(
                ["nmcli", "-t", "-f", "NAME,TYPE", "connection", "show"],
                capture_output=True, text=True, timeout=5.0,
            )
        except Exception as e:
            self.get_logger().warning(f"Wifi saved-connection lookup failed: {e}")
            return []
        names = []
        for line in (result.stdout or "").splitlines():
            if not line:
                continue
            fields = self._nmcli_split(line)
            if len(fields) >= 2 and fields[1] == "802-11-wireless":
                names.append(fields[0])
        return names

    def _run_sudo(self, cmd_argv, sudo_password, timeout):
        """
        Run `cmd_argv` under sudo.

        `-S` with the password piped over stdin if the operator typed one
        into the GCS Wifi Comms panel's own Sudo Password field, else `-n`
        (passwordless - fails fast and cleanly if that isn't provisioned
        for this command, rather than hanging on a TTY prompt that will
        never come). Exactly the same pattern as _do_reboot - the password
        is deliberately NEVER a command-line argument (those show up in
        `ps`/process listings for the command's lifetime) and never
        logged anywhere.
        """
        if sudo_password:
            return subprocess.run(
                ["sudo", "-S"] + cmd_argv,
                input=sudo_password + "\n",
                capture_output=True, text=True, timeout=timeout,
            )
        return subprocess.run(
            ["sudo", "-n"] + cmd_argv,
            capture_output=True, text=True, timeout=timeout,
        )

    def _wifi_scan(self, sudo_password, channel: TcpChannel):
        """
        `sudo nmcli device wifi list --rescan yes`, as requested by the
        operator from the Live Ops "Wifi Comms" panel. Runs off the
        dispatch thread since a real rescan takes a few seconds.

        Needs root - either a scoped passwordless-sudo entry for this
        exact nmcli invocation on this Jetson (see deployment notes), or
        the operator's own sudo password typed into the panel's Sudo
        Password field and passed through here as `sudo_password` (see
        _run_sudo). Without either, sudo's own failure message ("a
        password is required") comes back as `message` below so the
        operator can see exactly what's missing instead of a silently
        empty network list.

        Each result is cross-referenced against NetworkManager's own saved
        profiles (_wifi_saved_connection_names) so the frontend can show a
        "saved" badge directly in the scan list, not just in the separate
        Saved Devices mini-section.
        """
        networks = []
        success, message = True, ""
        try:
            result = self._run_sudo(
                ["nmcli", "-t", "-f", "SSID,SIGNAL,SECURITY,IN-USE",
                 "device", "wifi", "list", "--rescan", "yes"],
                sudo_password, timeout=20.0,
            )
            if result.returncode != 0:
                success = False
                message = ((result.stdout or "") + (result.stderr or "")).strip() or f"exit code {result.returncode}"
            else:
                saved = set(self._wifi_saved_connection_names())
                seen = set()
                for line in (result.stdout or "").splitlines():
                    if not line:
                        continue
                    fields = self._nmcli_split(line)
                    fields += [""] * (4 - len(fields))
                    ssid, signal, security, in_use = fields[0], fields[1], fields[2], fields[3]
                    if not ssid or ssid in seen:
                        continue   # skip hidden/blank SSIDs and duplicate BSSIDs of the same network
                    seen.add(ssid)
                    try:
                        signal_val = int(signal)
                    except ValueError:
                        signal_val = None
                    # nmcli reports an open network's SECURITY field as the
                    # literal string "--", not empty - normalize that (and
                    # blank) to None so the GCS can tell "no security" apart
                    # from "value present" with a simple truthiness check.
                    security_val = security if security and security != "--" else None
                    networks.append({
                        "ssid": ssid,
                        "signal": signal_val,
                        "security": security_val,
                        "in_use": (in_use.strip() == "*"),
                        "saved": ssid in saved,
                    })
                networks.sort(key=lambda n: (n["signal"] is None, -(n["signal"] or 0)))
        except FileNotFoundError:
            success, message = False, "'nmcli'/'sudo' not found on PATH"
        except subprocess.TimeoutExpired:
            success, message = False, "wifi scan did not return within 20s"
        except Exception as e:
            success, message = False, str(e)
        finally:
            sudo_password = None   # drop reference - not used again below

        if not success:
            self.get_logger().warning(f"Wifi scan failed: {message}")
        channel.send(pack_json(PKT_JSON, {
            "type": "wifi_scan_result", "success": success, "message": message,
            "networks": networks,
        }))

    def _wifi_active_ssid(self):
        """Currently-associated wifi SSID, if any. Reads NetworkManager's
        already-cached scan results (no --rescan, so no sudo needed) purely
        to find whichever entry carries the IN-USE marker - lets
        _wifi_saved_list mark the active network without doing a live scan."""
        try:
            result = subprocess.run(
                ["nmcli", "-t", "-f", "SSID,IN-USE", "device", "wifi", "list"],
                capture_output=True, text=True, timeout=5.0,
            )
        except Exception:
            return None
        for line in (result.stdout or "").splitlines():
            if not line:
                continue
            fields = self._nmcli_split(line)
            if len(fields) >= 2 and fields[1].strip() == "*":
                return fields[0]
        return None

    def _wifi_saved_list(self, channel: TcpChannel):
        """Fast, sudo-free listing of already-saved wifi profiles for the
        "Saved Devices" mini-section - independent of the rescan-triggering
        "List Devices" button so it can populate as soon as the panel loads."""
        active_ssid = self._wifi_active_ssid()
        networks = [
            {"ssid": n, "in_use": (n == active_ssid)}
            for n in self._wifi_saved_connection_names()
        ]
        channel.send(pack_json(PKT_JSON, {"type": "wifi_saved_list", "networks": networks}))

    def _wifi_connect(self, ssid: str, password, sudo_password, channel: TcpChannel):
        """
        Connect to a wifi network.

        - WITH a password (first time connecting to this network): runs
          `sudo nmcli device wifi connect <ssid> password <password>`.
          NetworkManager persists a connection profile after this
          succeeds, which is exactly what later lets "Saved Devices"
          reconnect with no password at all - the GCS never stores wifi
          passwords itself, this Jetson's NetworkManager is the only place
          they're ever kept.
        - WITHOUT one (operator clicked an entry already in "Saved
          Devices"): runs `sudo nmcli connection up "<ssid>"`, reusing the
          profile NM already has.

        Both need root - see _run_sudo, which uses `sudo_password` (the
        operator's OWN sudo password, from the panel's Sudo Password
        field - unrelated to `password`, the wifi network's own PSK above)
        if one was supplied, else passwordless `sudo -n`.

        List-form argv throughout (never shell=True) since ssid/password
        both arrive over the network - see _set_ros_param's docstring for
        the same reasoning applied to `ros2 param set`. The wifi network's
        password still appears as a plain nmcli argument (briefly visible
        via `ps` for the life of the call) - unlike the sudo password
        there's no stdin-based alternative for the wifi PSK in nmcli's
        `connect` subcommand, so this is an accepted, inherent limitation
        rather than an oversight.
        """
        if not ssid:
            channel.send(pack_json(PKT_JSON, {
                "type": "wifi_connect_result", "ssid": ssid,
                "success": False, "message": "SSID required",
            }))
            return
        try:
            if password:
                cmd_argv = ["nmcli", "device", "wifi", "connect", ssid, "password", password]
            else:
                cmd_argv = ["nmcli", "connection", "up", ssid]
            result = self._run_sudo(cmd_argv, sudo_password, timeout=30.0)
            success = (result.returncode == 0)
            message = ((result.stdout or "") + (result.stderr or "")).strip()
            if success and not message:
                message = "Connected"
        except FileNotFoundError:
            success, message = False, "'nmcli'/'sudo' not found on PATH"
        except subprocess.TimeoutExpired:
            success, message = False, "wifi connect did not return within 30s"
        except Exception as e:
            success, message = False, str(e)
        finally:
            password = None        # drop reference - not held onto after use
            sudo_password = None

        if success:
            self.get_logger().info(f"Wifi connected: {ssid}")
            self.save_black_box("command", f"Wifi connected: {ssid}")
        else:
            self.get_logger().warning(f"Wifi connect to '{ssid}' FAILED: {message}")
        channel.send(pack_json(PKT_JSON, {
            "type": "wifi_connect_result", "ssid": ssid,
            "success": success, "message": message,
        }))

    # ---------- ROS2 topic list ----------
    def _list_ros_topics(self):
        """
        Snapshot of the live ROS2 graph (topic name -> message type(s)), so
        the GCS operator can see which topics are actually up right now,
        the same way the "list_files" pattern shows the file tree.
        """
        try:
            pairs = self.get_topic_names_and_types()
        except Exception as e:
            self.get_logger().error(f"get_topic_names_and_types failed: {e}")
            return []
        return [{"name": name, "types": types} for name, types in sorted(pairs)]

    # ---------- Remote Terminal ----------
    def _clamp_command_timeout(self, value):
        try:
            t = float(value)
        except (TypeError, ValueError):
            t = DEFAULT_COMMAND_TIMEOUT_SEC
        return max(1.0, min(t, MAX_COMMAND_TIMEOUT_SEC))

    def _clamp_command_max_lines(self, value):
        try:
            n = int(value)
        except (TypeError, ValueError):
            n = DEFAULT_COMMAND_MAX_LINES
        return max(1, min(n, MAX_COMMAND_MAX_LINES))

    def _run_shell_command(self, command, request_id, timeout, max_lines, channel):
        """
        Runs `command` in its own OS subprocess - a dedicated separate
        process from this node, per the GCS feature request - so a hung or
        crashing command can never take down the TCP link or the rest of
        this node. Always replies exactly once with a "command_result",
        holding at most `max_lines` of output (the tail, so the most
        recent/relevant lines survive truncation - GCS-configurable per
        request, defaulting to DEFAULT_COMMAND_MAX_LINES).
        """
        self.get_logger().info(
            f"[{channel.name}] Running remote command: {command!r} "
            f"(timeout={timeout:.0f}s, max_lines={max_lines})"
        )
        self.save_black_box("run_command", f"$ {command}")

        timed_out = False
        exit_code = None
        try:
            proc = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            exit_code = proc.returncode
            output = (proc.stdout or "") + (proc.stderr or "")
        except subprocess.TimeoutExpired as e:
            timed_out = True
            stdout = e.stdout.decode("utf-8", "replace") if isinstance(e.stdout, bytes) else (e.stdout or "")
            stderr = e.stderr.decode("utf-8", "replace") if isinstance(e.stderr, bytes) else (e.stderr or "")
            output = f"{stdout}{stderr}\n[killed - exceeded {timeout:.0f}s timeout]"
        except Exception as e:
            output = f"Failed to run command: {e}"

        lines = output.splitlines()
        truncated = len(lines) > max_lines
        if truncated:
            lines = lines[-max_lines:]

        channel.send(pack_json(PKT_JSON, {
            "type": "command_result",
            "request_id": request_id,
            "command": command,
            "success": (exit_code == 0) and not timed_out,
            "exit_code": exit_code,
            "timed_out": timed_out,
            "output": "\n".join(lines),
            "truncated": truncated,
            "max_lines": max_lines,
        }))

    # ---------- ROS2 parameters ----------
    @staticmethod
    def _full_node_name(name: str, namespace: str) -> str:
        namespace = namespace or "/"
        if namespace in ("", "/"):
            return f"/{name}"
        return f"{namespace.rstrip('/')}/{name}"

    @staticmethod
    def _coerce_param_scalar(raw: str):
        """Best-effort convert a `ros2 param dump` scalar value string to a
        native Python type (bool/int/float/str, or a flat list for simple
        flow-style `[a, b, c]` sequences) - used instead of a hard PyYAML
        dependency, see _parse_param_dump."""
        raw = raw.strip()
        if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in ("'", '"'):
            return raw[1:-1]
        low = raw.lower()
        if low in ("true", "false"):
            return low == "true"
        if raw.startswith("[") and raw.endswith("]"):
            inner = raw[1:-1].strip()
            if not inner:
                return []
            return [TcpClientNode._coerce_param_scalar(p) for p in inner.split(",")]
        try:
            return int(raw)
        except ValueError:
            pass
        try:
            return float(raw)
        except ValueError:
            pass
        return raw

    @classmethod
    def _parse_param_dump(cls, text: str):
        """
        Minimal indentation-based parser for `ros2 param dump <node>` YAML
        output - deliberately avoids a hard PyYAML dependency (mirrors this
        node's general preference for subprocess + light parsing over extra
        rclpy/service-client wiring, e.g. run_command). Handles the standard
        shape:
            /node_name:
              ros__parameters:
                key1: value1
                nested:
                  key2: value2
                a_list:
                - item1
                - item2
        Array-type parameters (string-array, int-array, etc. - joint names,
        topic lists, frame_ids...) are extremely common in real ROS2 graphs,
        and PyYAML's default dump style (what `ros2 param dump` uses) puts
        their `- item` lines at the SAME indentation as the key itself, not
        one level deeper - an earlier version of this parser only recognised
        strictly-deeper indentation as "this key has children" and silently
        dropped every list-valued parameter as a result (caught via unit-
        testing against realistic sample dumps, not real hardware - see
        [[tethered-module-link-architecture]] memory for how this was found).
        Returns a flat list of (dotted_name, value) leaf pairs found under
        ros__parameters at any nesting depth (e.g. "nested.key2"), with list
        values as Python lists.
        """
        lines = []
        for raw_line in text.splitlines():
            if not raw_line.strip() or raw_line.strip().startswith("#"):
                continue
            indent = len(raw_line) - len(raw_line.lstrip(" "))
            lines.append((indent, raw_line.strip()))

        start_idx = None
        for i, (_indent, content) in enumerate(lines):
            if content == "ros__parameters:":
                start_idx = i + 1
                break
        if start_idx is None or start_idx >= len(lines):
            return []
        lines = lines[start_idx:]
        root_indent = lines[0][0]

        entries = []

        def parse_block(idx, indent, prefix):
            """Parse a mapping block whose keys are all at exactly `indent`,
            starting at `lines[idx]`. Returns the index just past this
            block (first line at a shallower indent, or len(lines))."""
            i = idx
            while i < len(lines):
                cur_indent, content = lines[i]
                if cur_indent < indent:
                    break
                if cur_indent > indent or content.startswith("- "):
                    # Stray over-indented/list line not consumed by the
                    # peek-ahead below (defensive - shouldn't normally
                    # trigger if the block structure is well-formed).
                    i += 1
                    continue
                key, _, rest = content.partition(":")
                key = key.strip().strip("'\"")
                rest = rest.strip()
                path = f"{prefix}.{key}" if prefix else key
                if rest:
                    entries.append((path, cls._coerce_param_scalar(rest)))
                    i += 1
                    continue
                # rest == "" -> peek ahead to tell a block list (item lines
                # at indent >= this key's own indent - PyYAML's default
                # non-indented-sequence style) apart from a nested mapping
                # (child lines strictly deeper).
                if i + 1 < len(lines) and lines[i + 1][0] >= indent and lines[i + 1][1].startswith("- "):
                    list_indent = lines[i + 1][0]
                    values = []
                    j = i + 1
                    while j < len(lines) and lines[j][0] == list_indent and lines[j][1].startswith("- "):
                        values.append(cls._coerce_param_scalar(lines[j][1][2:]))
                        j += 1
                    entries.append((path, values))
                    i = j
                elif i + 1 < len(lines) and lines[i + 1][0] > indent:
                    i = parse_block(i + 1, lines[i + 1][0], path)
                else:
                    # Empty mapping / parameter with no value - nothing to record.
                    i += 1
            return i

        parse_block(0, root_indent, "")
        return entries

    def _list_ros_params(self):
        """
        Snapshot of every live node's parameters (name, current value),
        gathered via `ros2 param dump` per node - same request/wait pattern
        as _list_ros_topics/_list_jetson_files, just heavier per-call so it
        always runs off the dispatch thread (see the list_params dispatch).

        Bounded by LIST_PARAMS_TOTAL_BUDGET_SEC across ALL nodes combined,
        not just per-node - returns whatever was gathered so far plus a
        list of per-node failure/skip reasons, rather than either silently
        dropping failed nodes with zero trace (the previous behavior - a
        real-world "why is this empty / why did this time out" was
        undiagnosable from the GCS) or blocking past whatever the GCS side
        is willing to wait.
        """
        try:
            node_infos = self.get_node_names_and_namespaces()
        except Exception as e:
            self.get_logger().error(f"get_node_names_and_namespaces failed: {e}")
            return [], [f"get_node_names_and_namespaces failed: {e}"]

        results = []
        errors = []
        start = time.monotonic()
        for name, namespace in sorted(node_infos):
            if time.monotonic() - start > LIST_PARAMS_TOTAL_BUDGET_SEC:
                errors.append(
                    f"stopped early after {LIST_PARAMS_TOTAL_BUDGET_SEC:.0f}s budget - some nodes not gathered"
                )
                self.get_logger().warning("list_params: total time budget exceeded, returning partial results")
                break
            full = self._full_node_name(name, namespace)
            try:
                proc = subprocess.run(
                    ["ros2", "param", "dump", full],
                    capture_output=True, text=True, timeout=PARAM_DUMP_TIMEOUT_SEC,
                )
            except subprocess.TimeoutExpired:
                msg = f"{full}: dump timed out after {PARAM_DUMP_TIMEOUT_SEC:.0f}s"
                self.get_logger().warning(f"param {msg}")
                errors.append(msg)
                continue
            except Exception as e:
                msg = f"{full}: {e}"
                self.get_logger().warning(f"param dump failed for {msg}")
                errors.append(msg)
                continue
            if proc.returncode != 0:
                detail = (proc.stderr or proc.stdout or "").strip() or f"exit code {proc.returncode}"
                msg = f"{full}: {detail}"
                self.get_logger().warning(f"param dump error for {msg}")
                errors.append(msg)
                continue
            if not proc.stdout.strip():
                continue   # no parameters on this node - not an error
            n_before = len(results)
            for pname, pval in self._parse_param_dump(proc.stdout):
                results.append({"node": full, "name": pname, "value": pval})
            self.get_logger().info(f"list_params: {full} -> {len(results) - n_before} params")

        self.get_logger().info(
            f"list_params: gathered {len(results)} params from {len(node_infos)} nodes "
            f"in {time.monotonic() - start:.1f}s ({len(errors)} node errors)"
        )
        return results, errors

    def _handle_list_params(self, channel: TcpChannel):
        """Always sends exactly one reply, even on total failure - without
        this the GCS side has no way to distinguish 'still working' from
        'crashed silently in here' and just times out blind with nothing
        to show for it."""
        try:
            params, errors = self._list_ros_params()
        except Exception as e:
            self.get_logger().error(f"list_params crashed: {e}")
            params, errors = [], [f"internal error: {e}"]
        channel.send(pack_json(PKT_JSON, {"type": "param_list", "params": params, "errors": errors}))

    def _set_ros_param(self, node_name, param_name, value, request_id, channel: TcpChannel):
        """
        Applies one parameter update via `ros2 param set` in its own
        subprocess (list-form argv, NOT shell=True - node/param/value all
        come from the network over the tether and must never be interpreted
        by a shell). Always replies exactly once with a "set_param_result"
        so the GCS panel can show a definite updated/failed acknowledgement.
        """
        if isinstance(value, bool):
            value_str = "true" if value else "false"
        else:
            value_str = str(value)

        self.get_logger().info(f"[{channel.name}] Setting param {node_name} {param_name} = {value_str!r}")
        try:
            proc = subprocess.run(
                ["ros2", "param", "set", node_name, param_name, value_str],
                capture_output=True, text=True, timeout=PARAM_SET_TIMEOUT_SEC,
            )
            success = proc.returncode == 0
            message = ((proc.stdout or "") + (proc.stderr or "")).strip()
        except subprocess.TimeoutExpired:
            success = False
            message = f"Timed out after {PARAM_SET_TIMEOUT_SEC:.0f}s"
        except Exception as e:
            success = False
            message = str(e)

        self.save_black_box(
            "param_set",
            f"{node_name} {param_name} = {value_str} -> {'ok' if success else 'FAILED: ' + message}"
        )
        channel.send(pack_json(PKT_JSON, {
            "type": "set_param_result",
            "request_id": request_id,
            "node": node_name,
            "name": param_name,
            "success": success,
            "message": message,
        }))

    # ---------- File listing / deletion ----------
    def _list_jetson_files(self):
        """
        Return a list of dicts describing the tree under JETSON_FILES_DIR.
        Each entry: { "type": "folder"|"file", "name": str, "path": str,
                      "size": int (files only), "children": [...] (folders only) }
        Paths are relative to JETSON_FILES_DIR (e.g. "bags/run1.bag").
        """
        base = JETSON_FILES_DIR
        base.mkdir(parents=True, exist_ok=True)

        def walk(directory: Path, prefix: str = "") -> list:
            entries = []
            try:
                items = sorted(directory.iterdir(), key=lambda p: (p.is_file(), p.name))
            except PermissionError:
                return entries
            for item in items:
                rel = (prefix + "/" + item.name).lstrip("/")
                if item.is_dir():
                    entries.append({
                        "type": "folder",
                        "name": item.name,
                        "path": rel,
                        "children": walk(item, rel),
                    })
                else:
                    try:
                        size = item.stat().st_size
                    except OSError:
                        size = 0
                    entries.append({
                        "type": "file",
                        "name": item.name,
                        "path": rel,
                        "size": size,
                    })
            return entries

        return walk(base)

    def _list_saved_maps(self) -> list:
        """
        List maps already saved on the vehicle under jetson_files/map/ - the
        destination FileTransferHandler._handle_request writes category="map"
        uploads to. Each map is a paired {name}.yaml + {name}.pgm (the same
        naming convention map_planner.html's Upload to Vehicle already uses,
        keyed off its "Image name" field), grouped by shared basename so the
        GCS only ever sees complete, usable maps.

        This is what makes the "Start Fast Mission" flow possible: a map
        uploaded once (e.g. over a fast Ethernet/manual connection) shows up
        here by name, and a later mission can reference it directly via
        start_direct's map_yaml_path/map_pgm_path without transferring the
        file again over a slow tether.
        """
        map_dir = JETSON_FILES_DIR / "map"
        if not map_dir.exists():
            return []
        by_name = {}
        try:
            items = sorted(map_dir.iterdir(), key=lambda p: p.name)
        except OSError:
            return []
        for item in items:
            if not item.is_file():
                continue
            stem, ext = item.stem, item.suffix.lower()
            if ext not in (".yaml", ".pgm"):
                continue
            try:
                size = item.stat().st_size
            except OSError:
                size = 0
            try:
                mtime = item.stat().st_mtime
            except OSError:
                mtime = None
            entry = by_name.setdefault(stem, {"name": stem})
            if ext == ".yaml":
                entry["yaml_size"] = size
                entry["saved_at"] = mtime
            else:
                entry["pgm_size"] = size
                entry["saved_at"] = max(entry.get("saved_at") or 0, mtime or 0)

        # Only report maps where BOTH halves are actually present - a lone
        # .yaml or .pgm (e.g. an upload that failed partway) isn't usable by
        # start_direct and would just be a confusing dead end in the list.
        maps = [m for m in by_name.values() if "yaml_size" in m and "pgm_size" in m]
        maps.sort(key=lambda m: m["name"])
        return maps

    def _delete_jetson_file(self, rel_path: str) -> dict:
        """Delete a single file from JETSON_FILES_DIR. Returns a result dict."""
        base = JETSON_FILES_DIR.resolve()
        try:
            target = _resolve_within(base, rel_path)
            if target is None:
                return {"type": "delete_ack", "success": False, "message": "Path traversal denied"}
            if not target.exists():
                return {"type": "delete_ack", "success": False, "message": "File not found"}
            if target.is_dir():
                return {"type": "delete_ack", "success": False, "message": "Cannot delete a directory"}
            target.unlink()
            self.get_logger().info(f"Deleted file: {rel_path}")
            self.save_black_box("file_delete", f"Deleted: {rel_path}")
            return {"type": "delete_ack", "success": True, "path": rel_path}
        except Exception as e:
            self.get_logger().error(f"Delete error: {e}")
            return {"type": "delete_ack", "success": False, "message": str(e)}

    def _delete_jetson_folder(self, rel_path: str) -> dict:
        """Recursively delete a folder (and everything in it) from
        JETSON_FILES_DIR. Returns a result dict. Deliberately refuses to
        delete the jetson_files root itself even if an empty/'.' path is
        somehow passed in - only a real sub-folder can be removed this way."""
        base = JETSON_FILES_DIR.resolve()
        try:
            target = _resolve_within(base, rel_path)
            if target is None:
                return {"type": "delete_ack", "success": False, "message": "Path traversal denied"}
            if target == base:
                return {"type": "delete_ack", "success": False, "message": "Refusing to delete the jetson_files root"}
            if not target.exists():
                return {"type": "delete_ack", "success": False, "message": "Folder not found"}
            if not target.is_dir():
                return {"type": "delete_ack", "success": False, "message": "Not a directory"}
            shutil.rmtree(target)
            self.get_logger().info(f"Deleted folder: {rel_path}")
            self.save_black_box("file_delete", f"Deleted folder: {rel_path}")
            return {"type": "delete_ack", "success": True, "path": rel_path}
        except Exception as e:
            self.get_logger().error(f"Delete folder error: {e}")
            return {"type": "delete_ack", "success": False, "message": str(e)}

    def _handle_download_folder(self, rel_path: str, channel: TcpChannel):
        """
        Zips a whole folder under JETSON_FILES_DIR into a temp file, then
        sends it back over the exact same PKT_FILE_REQ/CHUNK/COMPLETE
        protocol single-file downloads already use - the GCS side doesn't
        need to know or care that this came from a folder rather than a
        single file, it just sees a normal incoming file transfer named
        "<folder>.zip".

        Deliberately independent from FileTransferHandler's existing
        download branch (small duplication of the send-loop, accepted on
        purpose) rather than refactoring shared internals there - keeps
        this new, less-tested path from being able to affect the proven
        single-file download code at all.
        """
        base = JETSON_FILES_DIR.resolve()
        try:
            target = _resolve_within(base, rel_path)
            if target is None:
                channel.send(pack_json(PKT_JSON, {"type": "error", "message": "Path traversal denied"}))
                return
            if target == base:
                # An empty/missing `path` resolves to the jetson_files root
                # itself, which would otherwise pass the exists()/is_dir()
                # checks below and zip+send the ENTIRE directory - refuse
                # explicitly, matching _delete_jetson_folder's same guard.
                channel.send(pack_json(PKT_JSON, {"type": "error", "message": "Refusing to download the jetson_files root - specify a folder"}))
                return
            if not target.exists() or not target.is_dir():
                channel.send(pack_json(PKT_JSON, {"type": "error", "message": "Folder not found"}))
                return

            zip_name = target.name + ".zip"
            with tempfile.TemporaryDirectory() as tmp:
                zip_base = str(Path(tmp) / target.name)
                zip_path = Path(shutil.make_archive(zip_base, "zip", root_dir=str(target)))
                file_size = zip_path.stat().st_size
                self.get_logger().info(
                    f"[{channel.name}] Sending zipped folder: {rel_path} -> {zip_name} ({file_size} bytes)"
                )
                channel.send(pack_json(PKT_FILE_REQ, {
                    "filename": zip_name,
                    "size": file_size,
                    "direction": "download",
                }))
                with open(zip_path, "rb") as f:
                    while True:
                        chunk = f.read(FILE_CHUNK_SIZE)
                        if not chunk:
                            break
                        channel.send(pack_message(PKT_FILE_CHUNK, chunk))
                channel.send(pack_json(PKT_FILE_COMPLETE, {"status": "ok"}))
            self.save_black_box("file_download", f"Folder downloaded (zipped): {rel_path}")
        except Exception as e:
            self.get_logger().error(f"Folder download error: {e}")
            channel.send(pack_json(PKT_JSON, {"type": "error", "message": str(e)}))

    def _get_state_dict(self):
        with self._recording_lock:
            rec = self.recording_active
        with self._telemetry_lock:
            tel = self.telemetry_enabled
        with self._blackbox_stream_lock:
            bb = self.blackbox_stream_enabled
        with self._mag_lock:
            mag = self.mag_stream_enabled
        with self._depth_hold_lock:
            dh_enabled = self.depth_hold_enabled
            dh_status = self.depth_hold_status
            if time.monotonic() - self.depth_hold_status_time > DEPTH_HOLD_STATUS_TIMEOUT_SEC:
                dh_status = None
        return {
            "type": "state",
            "data": {
                "mode": self.mode,
                "armed": self.armed,
                "errors": self.errors,
                "recording": rec,
                "telemetry_enabled": tel,
                # These two mirror telemetry_enabled's pattern deliberately:
                # the GCS toggle UI syncs from THIS always-flowing state
                # message, never from the gated stream itself - syncing a
                # toggle from the very stream it turns off means the "off"
                # state can never be confirmed back to the UI once it takes
                # effect (that bug bit the telemetry toggle - see GCS side).
                "blackbox_stream_enabled": bb,
                "mag_stream_enabled": mag,
                # Switch position, plus what depth_hold.py last reported
                # (null = its status went quiet, i.e. the node is down).
                "depth_hold_enabled": dh_enabled,
                "depth_hold_status": dh_status,
                # So downstream consumers (safety_node's TetherLinkMonitor,
                # logs, GCS UI) can see reconnect behavior directly instead
                # of inferring it from silence.
                "tether_connected": self._active_name is not None,
                "active_channel": self._active_name,
                "link_channels": [ch.status() for ch in self._channels_snapshot()],
            }
        }

    # ---------- Build real telemetry ----------
    def _build_telemetry(self):
        telem = {}

        imu = self.latest['imu']
        if imu:
            telem.update({
                "orientation": {
                    "x": imu.orientation.x,
                    "y": imu.orientation.y,
                    "z": imu.orientation.z,
                    "w": imu.orientation.w
                },
                "angular_velocity": {
                    "x": imu.angular_velocity.x,
                    "y": imu.angular_velocity.y,
                    "z": imu.angular_velocity.z
                },
                "linear_acceleration": {
                    "x": imu.linear_acceleration.x,
                    "y": imu.linear_acceleration.y,
                    "z": imu.linear_acceleration.z
                }
            })

        depth_msg = self.latest['depth']
        telem["depth"] = depth_msg.vector.x if depth_msg else 0.0

        pressure_msg = self.latest['pressure']
        telem["pressure"] = pressure_msg.fluid_pressure if pressure_msg else 1013.25

        temp_msg = self.latest['temperature']
        telem["temperature"] = temp_msg.temperature if temp_msg else 25.0

        hum_msg = self.latest['humidity']
        telem["humidity"] = hum_msg.data if (hum_msg and hum_msg.data) else 0.0

        bat = self.latest['battery']
        if bat:
            telem.update({
                "battery_voltage": bat.voltage if bat.voltage else 0.0,
                "battery_current": 0.0,
                "battery_percentage": bat.percentage if hasattr(bat, 'percentage') else 0.0
            })
        else:
            telem.update({"battery_voltage": 0.0, "battery_current": 0.0, "battery_percentage": 0.0})

        relay_msg = self.latest['relay']
        telem["relay"] = relay_msg.data if relay_msg else False

        telem["leaks"] = self.latest['leaks']

        # Magnetometer/heading is NOT included here - it's its own opt-in
        # stream (see _publish_compass / mag_stream_enabled) so the compass
        # panel's bandwidth cost is zero unless the operator has it open.

        telem["gps"] = {
            "lat": self.latest['gps'].latitude  if self.latest['gps'] else 0.0,
            "lon": self.latest['gps'].longitude if self.latest['gps'] else 0.0,
            "alt": self.latest['gps'].altitude  if self.latest['gps'] else 0.0,
        }

        # Include recording & telemetry state so the GCS always knows
        with self._recording_lock:
            telem["recording"] = self.recording_active
        with self._telemetry_lock:
            telem["telemetry_enabled"] = self.telemetry_enabled

        return telem

    def _send_heartbeat(self):
        # Proactively probe every channel ourselves rather than only ACK'ing
        # heartbeats sent by the server - keeps traffic flowing in both
        # directions on ALL 3 links so a dead one is detected quickly
        # regardless of what the server side does, and regardless of role
        # (a "backup" channel still needs heartbeats to stay warm and to
        # notice quickly if it goes dead).
        for ch in self._channels_snapshot():
            ch.send(pack_message(PKT_HEARTBEAT, b""))

    def _set_transfer_active(self, active: bool):
        with self._transfer_lock:
            self._transfer_active = active

    def _is_transfer_active(self) -> bool:
        with self._transfer_lock:
            return self._transfer_active

    def _publish_telemetry(self):
        telem = self._build_telemetry()
        # Always publish locally on the ROS2 bus
        self.tel_pub.publish(String(data=json.dumps(telem)))
        # Over TCP: mirror to the active + secondary channels only (backup
        # stays heartbeat-only to save bandwidth until it's promoted).
        # Skipped entirely while a file upload is in flight (see
        # _transfer_active) so the tether's whole budget goes to the
        # transfer instead - queued telemetry frames still go out via
        # send_bulk's own priority handling either way, this just avoids
        # even generating one during the transfer's most time-sensitive
        # window (the initial file_ready handshake).
        with self._telemetry_lock:
            enabled = self.telemetry_enabled
        if enabled and not self._is_transfer_active():
            packet = pack_json(PKT_JSON, {"type": "telemetry", "data": telem})
            for ch in self._channels_snapshot():
                if ch.role in ("active", "secondary"):
                    ch.send_bulk(packet)

    def _publish_compass(self):
        """Heading-only stream for the GCS compass panel, gated by
        mag_stream_enabled (default off - see __init__). Deliberately a
        separate small message/timer rather than folding into
        _publish_telemetry so it costs nothing on the tether unless the
        operator actually has the compass panel open."""
        with self._mag_lock:
            if not self.mag_stream_enabled:
                return
        if self._is_transfer_active():
            return
        mag = self.latest['mag']
        if mag is None:
            return
        # Simple, uncalibrated heading from the raw X/Y magnetometer axes -
        # assumes a roughly level mount and does no tilt compensation or
        # hard/soft-iron correction. 0 deg/360 deg = magnetic north by this
        # formula's convention; if it reads backwards or offset against a
        # known real-world bearing during hardware testing, this is a
        # one-line sign/axis fix here, not a design problem - flagged as a
        # known follow-up rather than guessed at without real hardware to
        # verify against.
        heading = (math.degrees(math.atan2(mag.vector.y, mag.vector.x)) + 360.0) % 360.0
        packet = pack_json(PKT_JSON, {
            "type": "compass",
            "ts": time.time(),
            "heading": heading,
            "mag": {"x": mag.vector.x, "y": mag.vector.y, "z": mag.vector.z},
        })
        for ch in self._channels_snapshot():
            if ch.role in ("active", "secondary"):
                ch.send_bulk(packet)

    def _publish_state(self):
        state_dict = self._get_state_dict()
        self.state_pub.publish(String(data=json.dumps(state_dict["data"])))
        # State is normally mirrored to active + secondary regardless of the
        # telemetry enable/disable flag (it's lightweight and carries
        # arm/mode/link status the operator should always be able to see) -
        # but it's still real bytes on a ~10kbps link, so it's held back
        # like everything else while a file upload is actively in flight
        # (see _transfer_active): the operator explicitly wants the whole
        # link budget going to the transfer until it's confirmed done.
        if self._is_transfer_active():
            return
        packet = pack_json(PKT_JSON, state_dict)
        for ch in self._channels_snapshot():
            if ch.role in ("active", "secondary"):
                ch.send(packet)

    def destroy_node(self):
        self._stop_event.set()
        self.file_handler.stop()
        for ch in self._channels_snapshot():
            ch.close()
        super().destroy_node()


# ----------------------------------------------------------------------
# File Transfer Handler
# ----------------------------------------------------------------------
class FileTransferHandler:
    """
    Handles file upload (Jetson receives) and download (Jetson sends) over
    whichever channel the request arrived on.

    This class intentionally has NO background thread and does NOT own a
    recv queue.  Packets are dispatched to handle_packet() by each channel's
    own single dispatcher thread (TcpChannel._dispatch_loop).  Having a
    second consumer thread on the same queue was the root cause of
    duplicate file_ready ACKs and binary chunk payloads crashing the JSON
    decoder in an earlier version of this node.

    Only one upload is tracked at a time (matching the original single-
    channel behavior) - the channel it arrived on is remembered so an abort
    can notify the right peer.
    """
    def __init__(self, logger):
        self.logger = logger
        self.base_dir = JETSON_FILES_DIR          # absolute path, resolved at startup
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._current_upload = None
        self._upload_channel = None

    def start(self):
        self.logger.info(f"FileTransferHandler ready - base dir: {self.base_dir}")

    def stop(self):
        if self._current_upload:
            self._abort_upload()

    def cancel_for_channel(self, channel):
        """Called by TcpChannel._run_session when `channel` disconnects -
        if it was mid-upload, tear that upload down (same cleanup as an
        explicit abort) instead of leaving it dangling with no owning
        connection left to ever send PKT_FILE_COMPLETE on."""
        if self._upload_channel is channel and self._current_upload:
            self._abort_upload()

    def handle_packet(self, msg_type, payload, channel):
        if msg_type == PKT_FILE_REQ:
            self._handle_request(payload, channel)
        elif msg_type == PKT_FILE_CHUNK:
            self._handle_chunk(payload)
        elif msg_type == PKT_FILE_COMPLETE:
            self._handle_complete(payload)

    def _handle_request(self, payload, channel):
        try:
            req = json.loads(payload.decode())
            filename = Path(req["filename"]).name
            direction = req.get("direction", "upload")

            if direction == "upload":
                size = req["size"]
                # Mission Planner uploads (mission/map/failsafe) carry a
                # "category" so they land under a subdirectory instead of
                # flat in JETSON_FILES_DIR -- generic uploads (category
                # absent) keep the original flat behavior unchanged.
                category = req.get("category")
                if category in ("mission", "map", "failsafe"):
                    dest_dir = self.base_dir / category
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    dest_path = dest_dir / filename
                else:
                    dest_path = self.base_dir / filename
                self.logger.info(f"[{channel.name}] Receiving upload: {filename} ({size} bytes)")
                try:
                    f = open(dest_path, "wb")
                except OSError as e:
                    channel.send(pack_json(PKT_JSON, {"type": "error", "message": str(e)}))
                    return
                self._current_upload = {"file": f, "expected_size": size, "received": 0, "filename": filename}
                self._upload_channel = channel
                channel.node._set_transfer_active(True)
                channel.send(pack_json(PKT_JSON, {"type": "file_ready", "filename": filename}))

            elif direction == "download":
                # Support sub-paths under jetson_files (e.g. "bags/run1.bag")
                rel = req["filename"]
                file_path = _resolve_within(self.base_dir.resolve(), rel)
                if file_path is None:
                    channel.send(pack_json(PKT_JSON, {"type": "error", "message": "Path traversal denied"}))
                    return
                if not file_path.exists():
                    self.logger.error(f"File not found: {file_path}")
                    channel.send(pack_json(PKT_JSON, {"type": "error", "message": "File not found"}))
                    return
                file_size = file_path.stat().st_size
                self.logger.info(f"[{channel.name}] Sending file: {rel} ({file_size} bytes)")
                channel.send(pack_json(PKT_FILE_REQ, {
                    "filename": Path(rel).name,
                    "size": file_size,
                    "direction": "download"
                }))
                with open(file_path, "rb") as f:
                    while True:
                        chunk = f.read(FILE_CHUNK_SIZE)
                        if not chunk:
                            break
                        channel.send(pack_message(PKT_FILE_CHUNK, chunk))
                channel.send(pack_json(PKT_FILE_COMPLETE, {"status": "ok"}))
                self.logger.info(f"File {rel} sent")

        except Exception as e:
            self.logger.error(f"File request error: {e}")
            channel.send(pack_json(PKT_JSON, {"type": "error", "message": str(e)}))

    def _handle_chunk(self, payload):
        if self._current_upload:
            try:
                self._current_upload["file"].write(payload)
                self._current_upload["received"] += len(payload)
            except OSError as e:
                self.logger.error(f"Write error: {e}")
                self._abort_upload()

    def _handle_complete(self, payload):
        if self._current_upload:
            upload = self._current_upload
            upload["file"].close()
            received = upload["received"]
            expected = upload["expected_size"]
            filename = upload["filename"]
            # Verify the byte count actually matches rather than trusting
            # that PKT_FILE_COMPLETE arriving means every chunk before it
            # did too - a channel reconnect mid-transfer resets the sender's
            # queue (see TcpChannel._run_session) and can silently drop
            # chunks that were queued but not yet sent, which previously
            # showed up (if at all) only much later as a confusing
            # LoadHybridMap failure during Start Mission instead of here,
            # where it's actually diagnosable.
            success = received == expected
            if success:
                self.logger.info(f"Upload complete: {filename} ({received} bytes)")
            else:
                self.logger.error(
                    f"Upload incomplete: {filename} received {received}/{expected} bytes - "
                    f"link likely dropped data mid-transfer"
                )
            channel = self._upload_channel
            if channel is not None:
                channel.send(pack_json(PKT_JSON, {
                    "type": "upload_complete",
                    "filename": filename,
                    "success": success,
                    "received": received,
                    "expected": expected,
                    "message": "" if success else
                        f"incomplete transfer: received {received}/{expected} bytes",
                }))
                channel.node._set_transfer_active(False)
            self._current_upload = None
            self._upload_channel = None

    def _abort_upload(self):
        if self._current_upload:
            filename = self._current_upload.get("filename")
            self._current_upload["file"].close()
            self._current_upload = None
        else:
            filename = None
        if self._upload_channel:
            self._upload_channel.node._set_transfer_active(False)
            # upload_complete (not the generic "error" type) so a GCS thread
            # blocked waiting on this specific upload's completion (see
            # _upload_file_to_jetson) is unblocked immediately as a failure
            # instead of sitting out its full timeout.
            self._upload_channel.send(pack_json(PKT_JSON, {
                "type": "upload_complete",
                "filename": filename,
                "success": False,
                "message": "Upload aborted",
            }))
            self._upload_channel = None


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main(args=None):
    rclpy.init(args=args)
    node = TcpClientNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()

if __name__ == "__main__":
    os.makedirs("./jetson_files", exist_ok=True)
    os.makedirs("./jetson_files/bags", exist_ok=True)
    os.makedirs("./jetson_files/configs", exist_ok=True)
    # Create sample files for testing
    with open("./jetson_files/configs/config.yaml", "w") as f:
        f.write("mode: manual\npid: {p: 1.0, i: 0.1, d: 0.05}\n")
    with open("./jetson_files/configs/bt.xml", "w") as f:
        f.write("<root><sequence><action name='arm'/></sequence></root>")
    with open("./jetson_files/test.txt", "w") as f:
        f.write("Hello from Jetson!\n" * 100)
    main()
