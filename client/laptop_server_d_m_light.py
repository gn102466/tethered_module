#!/usr/bin/env python3
"""
Laptop Backend for ROV/AUV.
- TCP server that the Jetson TCP client connects to.
- Serves a web UI (Flask) with REST endpoints.
- Forwards commands, telemetry, and file transfers.
- Includes physical joystick control thread (pygame).Bru

New features:
  - Rosbag start / stop / save controls with live recording status indicator.
  
  - File browser: lists the full jetson_files directory tree; per-file
    download and delete buttons
  - Telemetry arm/disarm toggle (stops heavy sensor stream so joystick
    data gets through faster on the RS-485 tether)
  - Physical joystick visualiser (all axes + buttons shown live in the UI)
  - On-screen virtual joystick for emergency use (no physical controller needed)
  - Robust chunked file download: streams the file in chunks with a
    content-length header so the browser shows real progress and never
    delivers a partial file if the tether drops mid-transfer.
"""

import socket
import threading
import json
import struct
import time
import logging
import tempfile
import os
import shutil
import sys
import uuid
import zlib
import hashlib
import base64
import ssl
import re
import math
import urllib.parse
from pathlib import Path
from queue import Queue, Empty
from collections import deque
from flask import (
    Flask, request, jsonify, send_file, render_template_string, Response,
    stream_with_context, send_from_directory,
)

# Try to import pygame for joystick support
try:
    import pygame
    PYGAME_AVAILABLE = True
except ImportError:
    PYGAME_AVAILABLE = False

# Try to import paramiko for the "WiFi (SSH)" manual connection mode -
# tunnels the same ROVC packet protocol through an SSH connection to the
# Jetson instead of a raw socket. Optional so this file still runs (minus
# that one connection mode) on a machine without it installed.
try:
    import paramiko
    PARAMIKO_AVAILABLE = True
except ImportError:
    PARAMIKO_AVAILABLE = False

# ----------------------------------------------------------------------
# Constants
#
# Redundant tether channels, each a separate RS-485<->Ethernet bridge
# pair. Priority is index order: 0 = highest priority (preferred "active"
# channel whenever it's connected), last = lowest (pure backup). This list
# MUST match CHANNELS_CONFIG in jetson_tcp_node_d_m.py.
# ----------------------------------------------------------------------
CHANNELS_CONFIG = [
    {"name": "CH-A", "host": "192.168.1.203", "port": 6000},
    {"name": "CH-B", "host": "192.168.1.201", "port": 5005},
    {"name": "CH-C", "host": "192.168.1.202", "port": 6000},
    # Local loopback test channel - stands in for a real bridge pair when
    # testing against scripts/test_bridge_server.py instead of real RS-485
    # hardware (run that script, then this server + jetson_tcp_node_d_m.py,
    # both on the same machine - see that script's docstring). Lowest
    # priority so it never preempts a real channel; harmless if nothing is
    # listening on 127.0.0.1:7000. Comment out / remove for a production-
    # only build.
    {"name": "CH-test", "host": "127.0.0.1", "port": 7000},
]
FILE_CHUNK_SIZE    = 4096       # smaller chunks for low-bandwidth RS-485 link
RECV_BUFFER        = 65536
RECONNECT_DELAY    = 2.0
DOWNLOAD_TIMEOUT   = 180        # seconds
# How long _upload_file_to_jetson waits for the Jetson's "file_ready" ACK
# before giving up. Was 5.0s, which a ~10kbps tether could blow through
# even for that one tiny JSON ACK packet whenever it landed behind other
# queued traffic - the Jetson side now prioritizes this ACK ahead of
# telemetry (see send_bulk/telemetry_queue in jetson_tcp_node_d_m.py), but
# this is kept generous on top of that as headroom for real link latency/
# reconnect hiccups rather than re-introducing a hair-trigger timeout.
UPLOAD_READY_TIMEOUT = 25.0     # seconds
# How long _upload_file_to_jetson waits, after queueing every chunk, for the
# Jetson's byte-count-verified "upload_complete" ack (see
# FileTransferHandler._handle_complete). Scaled by file size at a
# deliberately pessimistic assumed throughput - real throughput on this link
# varies with contention/overhead and this must never fire before a
# legitimately-slow-but-healthy transfer finishes - plus a fixed floor and
# buffer for round-trip/protocol overhead on small files.
UPLOAD_COMPLETE_MIN_TIMEOUT     = 30.0    # seconds - floor regardless of size
UPLOAD_ASSUMED_THROUGHPUT_BPS   = 500     # bytes/sec - conservative vs. the ~1.25KB/s nominal 10kbps link
UPLOAD_COMPLETE_TIMEOUT_BUFFER  = 30.0    # seconds - extra headroom on top of the size-based estimate
HEARTBEAT_INTERVAL = 2.0        # seconds
HEARTBEAT_TIMEOUT  = 6.0        # seconds
ROLE_UPDATE_INTERVAL = 0.5      # seconds - how often active/secondary roles are recomputed
RUN_COMMAND_DEFAULT_MAX_LINES = 50   # must match DEFAULT_COMMAND_MAX_LINES in jetson_tcp_node_d_m.py

PKT_HEARTBEAT      = 0x01
PKT_HEARTBEAT_ACK  = 0x02
PKT_JSON           = 0x10
PKT_FILE_REQ       = 0x11
PKT_FILE_CHUNK     = 0x12
PKT_FILE_COMPLETE  = 0x13
MAGIC = b"ROVC"
HEADER_SIZE = 13   # 4 MAGIC + 1 type + 4 len + 4 crc32

# Joystick configuration
AXIS_MAP = {
    'vx': 0, 'vy': 1, 'vz': 4, 'vyaw': 3,
}
AXIS_SCALE = {'vx': 1.0, 'vy': 1.0, 'vz': 1.0, 'vyaw': 1.0}
DEADZONE = 0.1

BUTTON_COMMANDS = {
    0: "arm",
    1: "disarm",
    2: "lights_on",
    3: "lights_off",
    4: "start_recording",
    5: "stop_recording",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("LaptopBackend")

# ----------------------------------------------------------------------
# Packet helpers  -  wire format: MAGIC(4)|type(1)|len(4)|crc32(4)|payload
# CRC-32 covers payload only.  Receiver rejects frames whose CRC mismatches
# (false sync on RS-485 noise) without disconnecting.
# ----------------------------------------------------------------------
def pack_message(msg_type: int, payload: bytes) -> bytes:
    crc = zlib.crc32(payload) & 0xFFFFFFFF
    return MAGIC + struct.pack("!BII", msg_type, len(payload), crc) + payload

def pack_json(msg_type: int, obj: dict) -> bytes:
    data = json.dumps(obj).encode("utf-8")
    return pack_message(msg_type, data)

# ----------------------------------------------------------------------
# TCP Channel - one independently-reconnecting tether link.
#
# There are 3 of these (see CHANNELS_CONFIG) so losing one physical
# RS-485<->Ethernet bridge never blocks or slows the others. ChannelManager
# below picks which one is "active" (drives commands + the primary
# telemetry/state shown in the UI) and which is "secondary" (mirrored
# telemetry shown as a live second feed), sticky across reconnects so the
# link doesn't flap back and forth on a marginal path.
# ----------------------------------------------------------------------
class TcpChannel:
    def __init__(self, name, host, port, priority):
        self.name = name
        self.host = host
        self.port = port
        self.priority = priority

        self.send_queue = None      # Queue while connected, None while down
        self.recv_queue = Queue()   # persistent for the channel's lifetime

        self._stop = threading.Event()
        # Operator override (Primary Tether switch) - when set, _connect_loop
        # skips attempting/holding a connection entirely rather than being
        # torn down permanently like _stop. Separate from _stop so pausing
        # is always reversible via resume() (calling start() again after
        # _stop is set would NOT work - the new thread's loop condition
        # would already be false).
        self.paused = threading.Event()
        self._sock = None
        self.connected = False
        self.last_rx = 0.0
        self.reconnects = 0
        self.role = "down"          # "active" | "secondary" | "backup" | "down"

    def start(self):
        threading.Thread(target=self._connect_loop, daemon=True, name=f"{self.name}-conn").start()

    def pause(self):
        """Operator-initiated: stop (re)connecting until resume() is
        called. Force-disconnects if currently connected so it actually
        takes effect immediately rather than lingering connected until the
        next natural drop."""
        self.paused.set()
        if self._sock:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def resume(self):
        self.paused.clear()

    def stop(self):
        self._stop.set()
        if self._sock:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._sock.close()

    def send(self, data: bytes) -> bool:
        """Queue bytes for send on this channel. No-op (returns False) if down."""
        q = self.send_queue
        if q is not None:
            q.put(data)
            return True
        return False

    def status(self) -> dict:
        age = None if self.last_rx == 0.0 else max(0.0, time.time() - self.last_rx)
        return {
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "connected": self.connected,
            "role": self.role,
            "last_rx_age": age,
            "reconnects": self.reconnects,
            # This same class backs both the primary CH-A/B/C/test channels
            # AND manual raw-TCP connections - the only thing distinguishing
            # them is the priority ChannelManager.add_channel assigned, so
            # that's what this checks. The UI uses this to decide whether to
            # offer a Disconnect action (manual only - never on the primary
            # tether, matching remove_channel's own priority-floor guard).
            "is_manual": self.priority >= ChannelManager.MANUAL_PRIORITY_FLOOR,
        }

    def _connect_loop(self):
        while not self._stop.is_set():
            if self.paused.is_set():
                time.sleep(0.5)
                continue
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                sock.connect((self.host, self.port))
                sock.settimeout(None)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                try:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 3)
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 2)
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
                except (AttributeError, OSError):
                    pass

                self._sock = sock
                self.send_queue = Queue()
                self.last_rx = time.time()
                self.connected = True
                log.info(f"[{self.name}] Connected to Jetson at {self.host}:{self.port}")

                active = threading.Event()
                active.set()
                send_t = threading.Thread(target=self._send_loop, args=(sock, active), daemon=True)
                recv_t = threading.Thread(target=self._recv_loop, args=(sock, active), daemon=True)
                hb_t   = threading.Thread(target=self._heartbeat_loop, args=(active,), daemon=True)
                send_t.start()
                recv_t.start()
                hb_t.start()
                send_t.join()
                recv_t.join()
                hb_t.join()
            except (ConnectionRefusedError, OSError) as e:
                log.debug(f"[{self.name}] Connection failed: {e}")

            self.connected = False
            self.send_queue = None
            self._sock = None
            self.reconnects += 1
            if not self._stop.is_set():
                time.sleep(RECONNECT_DELAY)

    def _send_loop(self, sock, active):
        while not self._stop.is_set() and active.is_set():
            try:
                data = self.send_queue.get(timeout=0.5)
                sock.sendall(data)
            except Empty:
                continue
            except (BrokenPipeError, ConnectionResetError, OSError):
                active.clear()
                break

    def _recv_loop(self, sock, active):
        buffer = bytearray()
        while not self._stop.is_set() and active.is_set():
            try:
                chunk = sock.recv(RECV_BUFFER)
                if not chunk:
                    active.clear()
                    break
                self.last_rx = time.time()
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
                        # False sync - the MAGIC bytes matched by chance
                        # inside a previous payload, or this genuinely is
                        # corrupted data (RS-485 noise/tether interference) -
                        # can't always tell which from here, so surface it
                        # in the Bad Data console rather than silently
                        # discarding it.
                        _record_bad_data(self.name, "crc_mismatch",
                            f"type=0x{msg_type:02x} len={payload_len} expected_crc={expected_crc:08x} "
                            f"actual_crc={actual_crc:08x} payload_preview={payload[:32].hex()}")
                        buffer.pop(0)
                        continue
                    del buffer[:total]
                    self.recv_queue.put((msg_type, payload))
            except (ConnectionResetError, BrokenPipeError, OSError):
                active.clear()
                break

    def _heartbeat_loop(self, active):
        """Detect silently-stalled RS-485 links and force reconnect."""
        while not self._stop.is_set() and active.is_set():
            time.sleep(HEARTBEAT_INTERVAL)
            if not active.is_set():
                break
            if time.time() - self.last_rx > HEARTBEAT_TIMEOUT:
                log.warning(f"[{self.name}] No data from Jetson within timeout - link looks stale, reconnecting")
                active.clear()
                try:
                    self._sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                break
            q = self.send_queue
            if q is not None:
                q.put(pack_message(PKT_HEARTBEAT, b""))


# ----------------------------------------------------------------------
# ChannelManager - picks the active/secondary channel out of the 3, sticky
# across reconnects (see the module-level comment above TcpChannel).
# ----------------------------------------------------------------------
class ChannelManager:
    # Manual channels always get a priority >= this, so it doubles as a
    # cheap "is this a manual/removable channel" test in remove_channel -
    # a configured (CH-A/B/C/test) channel can NEVER be removed through
    # that path even if a caller passes its name, which matters because
    # the primary tether must stay "always on and default" no matter what
    # the Manual Connection UI does.
    MANUAL_PRIORITY_FLOOR = 10000

    def __init__(self, configs):
        self.channels = [TcpChannel(c["name"], c["host"], c["port"], i) for i, c in enumerate(configs)]
        # Protects structural changes to self.channels (append/remove), now
        # that manual connections (see add_channel/remove_channel) can grow
        # or shrink this list at any time after startup, unlike the fixed
        # `configs`-based set above which never changes. Separate from
        # _lock below (which only guards the active/secondary NAME state)
        # to keep the two concerns independent. Every read site takes a
        # snapshot via _channels_snapshot() rather than iterating
        # self.channels directly, so a concurrent add/remove can never
        # corrupt an in-progress iteration.
        self._channels_lock = threading.Lock()
        # Manual channels always sort after every configured channel in
        # priority-ordered active/secondary selection (see _update_roles) -
        # true backups, never preempting the tether by default.
        self._next_manual_priority = self.MANUAL_PRIORITY_FLOOR
        self._active_name = None
        self._secondary_name = None
        self._forced_active = None    # operator override from the "Force" button, else None = automatic
        # Primary Tether switch state (default True = "always on and
        # default", matching the original design) - operator override for
        # e.g. deliberately testing a manual channel in isolation. Tracked
        # explicitly (not just inferred from channel.connected) so the UI
        # switch can reflect the TOGGLE's state, not just current link
        # health, which would look identical for "disabled" vs. "a
        # genuinely dead link" otherwise.
        self.primary_enabled = True
        self._lock = threading.Lock()

    def start(self):
        for ch in self._channels_snapshot():
            ch.start()
        threading.Thread(target=self._role_loop, daemon=True).start()

    def stop(self):
        for ch in self._channels_snapshot():
            ch.stop()

    def _role_loop(self):
        """Background loop that recomputes active/secondary roles.
        Runs until process exit; defensive exception handling prevents
        a stray error from killing the thread."""
        while True:
            try:
                self._update_roles()
            except Exception:
                log.exception("ChannelManager role loop error")
            time.sleep(ROLE_UPDATE_INTERVAL)

    def _channels_snapshot(self):
        """Point-in-time copy of self.channels, safe to iterate even while
        a manual channel is concurrently being added/removed."""
        with self._channels_lock:
            return list(self.channels)

    def add_channel(self, channel) -> None:
        """Register a new manual/ad-hoc channel (raw-TCP, SSH-tunneled, or
        WebSocket - anything exposing the same TcpChannel-shaped interface)
        and start it. Always lowest priority, so it can only ever become
        "active" if every configured tether channel is down.

        Also spawns this channel's own process_incoming() consumer thread -
        main() only does that once, at startup, for the channels that exist
        at that moment (the configured ones). Without this, a manual
        channel's recv_queue would never be drained and every packet it
        receives would just silently pile up forever."""
        with self._channels_lock:
            channel.priority = self._next_manual_priority
            self._next_manual_priority += 1
            self.channels.append(channel)
        channel.start()
        threading.Thread(target=process_incoming, args=(channel,), daemon=True).start()

    def remove_channel(self, name: str) -> bool:
        """Stop and drop a MANUAL channel by name (operator-initiated
        disconnect). Returns False if no such manual channel exists -
        deliberately refuses to remove a configured (CH-A/B/C/test)
        channel even if its exact name is passed in, via the priority
        floor check below, so the primary tether can never be torn down
        through this path regardless of what the Manual Connection UI
        sends."""
        with self._channels_lock:
            match = next((c for c in self.channels if c.name == name), None)
            if match is None or match.priority < self.MANUAL_PRIORITY_FLOOR:
                return False
            self.channels.remove(match)
        match.stop()
        self._update_roles()
        return True

    def force_active(self, name):
        """Pin the active channel to `name` (operator override), or clear
        back to automatic selection if `name` is falsy/unknown. Only takes
        effect once that channel is actually connected - see _update_roles()."""
        valid = {c.name for c in self._channels_snapshot()}
        with self._lock:
            self._forced_active = name if name in valid else None
        log.info(f"Active channel override: {self._forced_active or 'AUTO'}")

    def set_primary_enabled(self, enabled: bool):
        """Toggle the CONFIGURED (non-manual, priority < MANUAL_PRIORITY_FLOOR)
        channels on/off - e.g. for deliberately testing manual-only
        connectivity. Manual channels are never touched by this. Default:
        enabled (matches "the tether is always on and the default
        connection")."""
        self.primary_enabled = enabled
        for ch in self._channels_snapshot():
            if ch.priority < self.MANUAL_PRIORITY_FLOOR:
                if enabled:
                    ch.resume()
                else:
                    ch.pause()
        self._update_roles()
        log.info(f"Primary tether {'ENABLED' if enabled else 'DISABLED'} (operator override)")

    def _update_roles(self):
        channels = self._channels_snapshot()
        with self._lock:
            connected = sorted((c for c in channels if c.connected), key=lambda c: c.priority)
            connected_names = {c.name for c in connected}

            # Operator override ("Force" button) wins whenever the target
            # channel is actually connected; otherwise fall back to the
            # normal sticky-priority selection below.
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

        shared.connected = active_name is not None

    def active(self):
        with self._lock:
            name = self._active_name
        for ch in self._channels_snapshot():
            if ch.name == name:
                return ch
        return None

    def secondary(self):
        with self._lock:
            name = self._secondary_name
        for ch in self._channels_snapshot():
            if ch.name == name:
                return ch
        return None

    def snapshot(self) -> dict:
        with self._lock:
            active, secondary, forced = self._active_name, self._secondary_name, self._forced_active
        return {
            "active": active,
            "secondary": secondary,
            "forced": forced,
            "primary_enabled": self.primary_enabled,
            "channels": [ch.status() for ch in self._channels_snapshot()],
        }

# ----------------------------------------------------------------------
# Manual connection channels - GCS "Manual Connection" tab (Tethered/RS-485,
# Ethernet, Localhost, WiFi/SSH, WebSocket). The default/primary tether
# link above (TcpChannel/ChannelManager) is completely untouched by
# everything below - these are all ADDITIONAL, operator-initiated, lowest-
# priority backup channels (see ChannelManager.add_channel).
#
# Tethered/RS-485, Ethernet, and Localhost modes need nothing new: they're
# just another plain TcpChannel instance (same class as above, unmodified),
# constructed at runtime with operator-supplied host/port instead of a
# CHANNELS_CONFIG entry, dialing out to a new listening port the Jetson
# opens specifically for these (MANUAL_LISTEN_PORT in
# jetson_tcp_node_d_m.py - MUST match MANUAL_TCP_PORT_DEFAULT below).
#
# WiFi(SSH) and WebSocket need genuinely different transports and get their
# own classes below - this is the newest, least field-tested code in this
# file (no real hardware/network to verify against from here), kept
# strictly separate from the proven TcpChannel path above so nothing about
# it can affect the primary/default link.
# ----------------------------------------------------------------------

# Must match jetson_tcp_node_d_m.py's MANUAL_LISTEN_PORT/MANUAL_WS_LISTEN_PORT -
# same cross-file duplication convention already used for CHANNELS_CONFIG.
MANUAL_TCP_PORT_DEFAULT = 6100
MANUAL_WS_PORT_DEFAULT = 6101

SSH_CONNECT_TIMEOUT_SEC = 10.0
SSH_RECONNECT_DELAY_SEC = 5.0
WS_CONNECT_TIMEOUT_SEC = 10.0
WS_RECONNECT_DELAY_SEC = 5.0


class _ManualChannelBase:
    """
    Common state/status/send-queue machinery shared by the two transport-
    specific manual channel classes below (SshTunnelChannel,
    WebSocketChannel). Deliberately NOT shared with the proven, unmodified
    TcpChannel above (the default/primary channels never touch this code
    at all) - kept separate so nothing about these newer, less field-tested
    transports can affect the primary link's behavior.
    """
    def __init__(self, name, host, port, priority=999999):
        self.name = name
        self.host = host
        self.port = port
        self.priority = priority
        self.send_queue = None
        self.recv_queue = Queue()
        self._stop = threading.Event()
        self.connected = False
        self.last_rx = 0.0
        self.reconnects = 0
        self.role = "down"
        # Whatever's currently live (a raw socket for WebSocketChannel, a
        # paramiko Transport for SshTunnelChannel) - set by the subclass's
        # _connect_loop once connected, so stop() can force it closed
        # immediately instead of only setting _stop and waiting for the
        # session threads to notice on their own next poll. Matches
        # TcpChannel.stop()'s same force-close approach above.
        self._live_conn = None

    def send(self, data: bytes) -> bool:
        q = self.send_queue
        if q is not None:
            q.put(data)
            return True
        return False

    def status(self) -> dict:
        age = None if self.last_rx == 0.0 else max(0.0, time.time() - self.last_rx)
        return {
            "name": self.name, "host": self.host, "port": self.port,
            "connected": self.connected, "role": self.role,
            "last_rx_age": age, "reconnects": self.reconnects,
            # Always true for these two classes (SshTunnelChannel/
            # WebSocketChannel are only ever constructed as manual
            # connections), included anyway so the frontend can treat all
            # channel kinds uniformly via the same field.
            "is_manual": True,
        }

    def stop(self):
        self._stop.set()
        conn = self._live_conn
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ----------------------------------------------------------------------
# WebSocket framing (RFC 6455) - client side, mirrors the Jetson's server-
# side codec in jetson_tcp_node_d_m.py exactly (can't literally share code
# between these two independently-deployed files). Client->server frames
# MUST be masked per spec (mask=True below); server->client frames arrive
# unmasked, and the decoder handles both regardless of direction.
# ----------------------------------------------------------------------
_WS_OPCODE_TEXT = 0x1
_WS_OPCODE_BINARY = 0x2
_WS_OPCODE_CLOSE = 0x8
_WS_MAGIC_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def _ws_accept_key(client_key: str) -> str:
    digest = hashlib.sha1((client_key + _WS_MAGIC_GUID).encode("ascii")).digest()
    return base64.b64encode(digest).decode("ascii")


def _ws_encode_frame(payload: bytes, opcode: int = _WS_OPCODE_BINARY, mask: bool = True) -> bytes:
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


class WebSocketChannel(_ManualChannelBase):
    """
    Manual connection over a hand-rolled WebSocket client (RFC 6455) -
    carries the exact same ROVC packet protocol as every other channel,
    just WS-framed. `url` is what the operator types in the UI, e.g.
    `ws://192.168.1.50:6101/` or `wss://host:port/`.

    Auto-reconnects like the primary TcpChannel does (this is the GCS/
    dialing side, same as every other channel here) until stop() is
    called from an operator-initiated disconnect.
    """
    def __init__(self, name, url, priority=999999):
        parsed = urllib.parse.urlparse(url)
        self.url = url
        self.use_tls = parsed.scheme == "wss"
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (443 if self.use_tls else 80)
        self.path = parsed.path or "/"
        super().__init__(name, host, port, priority)
        self._sock = None

    def start(self):
        threading.Thread(target=self._connect_loop, daemon=True, name=f"{self.name}-conn").start()

    def _connect_loop(self):
        while not self._stop.is_set():
            sock = None
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(WS_CONNECT_TIMEOUT_SEC)
                sock.connect((self.host, self.port))
                if self.use_tls:
                    ctx = ssl.create_default_context()
                    sock = ctx.wrap_socket(sock, server_hostname=self.host)
                self._do_handshake(sock)
                sock.settimeout(None)

                self._sock = sock
                self._live_conn = sock
                self.send_queue = Queue()
                self.last_rx = time.time()
                self.connected = True
                log.info(f"[{self.name}] WebSocket connected to {self.url}")

                active = threading.Event()
                active.set()
                send_t = threading.Thread(target=self._send_loop, args=(sock, active), daemon=True)
                recv_t = threading.Thread(target=self._recv_loop, args=(sock, active), daemon=True)
                send_t.start()
                recv_t.start()
                send_t.join()
                recv_t.join()
            except Exception as e:
                log.debug(f"[{self.name}] WebSocket connection failed: {e}")

            self.connected = False
            self.send_queue = None
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
            self._sock = None
            self._live_conn = None
            self.reconnects += 1
            if not self._stop.is_set():
                time.sleep(WS_RECONNECT_DELAY_SEC)

    def _do_handshake(self, sock):
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request = (
            f"GET {self.path} HTTP/1.1\r\n"
            f"Host: {self.host}:{self.port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        ).encode("ascii")
        sock.sendall(request)

        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = sock.recv(4096)
            if not chunk:
                raise ConnectionError("WS handshake: connection closed before response completed")
            buf += chunk
            if len(buf) > 16384:
                raise ConnectionError("WS handshake: response too large")
        header_text = buf.split(b"\r\n\r\n", 1)[0].decode("iso-8859-1", errors="replace")
        status_line = header_text.split("\r\n", 1)[0]
        if " 101 " not in status_line:
            raise ConnectionError(f"WS handshake failed: {status_line!r}")
        expected_accept = _ws_accept_key(key)
        if expected_accept not in header_text:
            raise ConnectionError("WS handshake: Sec-WebSocket-Accept mismatch")

    def _send_loop(self, sock, active):
        while not self._stop.is_set() and active.is_set():
            try:
                data = self.send_queue.get(timeout=0.5)
                sock.sendall(_ws_encode_frame(data, mask=True))
            except Empty:
                continue
            except (BrokenPipeError, ConnectionResetError, OSError):
                active.clear()
                break

    def _recv_loop(self, sock, active):
        buffer = bytearray()
        while not self._stop.is_set() and active.is_set():
            try:
                frame = _ws_try_decode_frame(buffer)
                if frame is not None:
                    opcode, payload, consumed = frame
                    del buffer[:consumed]
                    if opcode == _WS_OPCODE_CLOSE:
                        active.clear()
                        break
                    if opcode in (_WS_OPCODE_TEXT, _WS_OPCODE_BINARY):
                        self.last_rx = time.time()
                        # One WS message == one complete ROVC packet (see
                        # TcpChannel's send-side comment on the Jetson for
                        # this invariant) - decode it directly rather than
                        # re-running the MAGIC-scanning framing used for
                        # the raw-TCP transports, which this doesn't need.
                        if len(payload) >= HEADER_SIZE and payload[:4] == MAGIC:
                            msg_type, payload_len, expected_crc = struct.unpack("!BII", payload[4:13])
                            body = payload[HEADER_SIZE:HEADER_SIZE + payload_len]
                            actual_crc = zlib.crc32(body) & 0xFFFFFFFF
                            if actual_crc == expected_crc:
                                self.recv_queue.put((msg_type, body))
                            else:
                                _record_bad_data(self.name, "crc_mismatch",
                                    f"type=0x{msg_type:02x} len={payload_len} expected_crc={expected_crc:08x} "
                                    f"actual_crc={actual_crc:08x} payload_preview={body[:32].hex()}")
                        else:
                            _record_bad_data(self.name, "malformed_frame",
                                f"WS message did not start with ROVC magic (len={len(payload)}) "
                                f"preview={payload[:32].hex()}")
                    continue
                chunk = sock.recv(RECV_BUFFER)
                if not chunk:
                    active.clear()
                    break
                buffer.extend(chunk)
            except (ConnectionResetError, BrokenPipeError, OSError):
                active.clear()
                break


class SshTunnelChannel(_ManualChannelBase):
    """
    Manual connection tunneled through SSH (paramiko) - carries the exact
    same ROVC packet protocol as every other channel. The SSH server here
    IS the Jetson's own sshd; this opens a `direct-tcpip` channel through
    it back to 127.0.0.1:MANUAL_TCP_PORT_DEFAULT on the Jetson itself,
    which is the SAME raw listening port the Ethernet/Localhost manual
    modes connect to directly - from the Jetson's ManualConnectionServer's
    point of view, a tunneled connection is indistinguishable from a
    direct one, so no Jetson-side SSH-specific code exists at all.

    Username/password are held only in memory for this channel's lifetime
    (never written to disk, never logged) and used solely to authenticate
    the paramiko Transport below.
    """
    def __init__(self, name, host, port, username, password, priority=999999):
        super().__init__(name, host, port, priority)
        self._username = username
        self._password = password
        self._transport = None

    def start(self):
        if not PARAMIKO_AVAILABLE:
            log.error(f"[{self.name}] paramiko not installed - cannot start SSH-tunneled channel")
            return
        threading.Thread(target=self._connect_loop, daemon=True, name=f"{self.name}-conn").start()

    def _connect_loop(self):
        while not self._stop.is_set():
            transport = None
            try:
                # Connect the raw socket ourselves with an explicit timeout,
                # then hand it to paramiko (which accepts an already-
                # connected socket in place of a (host, port) tuple) -
                # paramiko.Transport((host, port)) does NOT bound the
                # initial TCP connect with SSH_CONNECT_TIMEOUT_SEC itself,
                # so an unreachable/mistyped host could otherwise hang for
                # the OS-default TCP timeout (commonly minutes, or
                # indefinitely behind a firewall that drops SYNs) instead
                # of failing within the ~10s this constant's name implies.
                ssh_sock = socket.create_connection((self.host, self.port), timeout=SSH_CONNECT_TIMEOUT_SEC)
                ssh_sock.settimeout(None)  # hand off to paramiko's own read thread, no lingering timeout
                transport = paramiko.Transport(ssh_sock)
                transport.connect(username=self._username, password=self._password)
                chan = transport.open_channel(
                    "direct-tcpip",
                    ("127.0.0.1", MANUAL_TCP_PORT_DEFAULT),
                    ("127.0.0.1", 0),
                    timeout=SSH_CONNECT_TIMEOUT_SEC,
                )

                self._transport = transport
                self._live_conn = transport
                self.send_queue = Queue()
                self.last_rx = time.time()
                self.connected = True
                log.info(f"[{self.name}] SSH tunnel established to {self.host}:{self.port} -> 127.0.0.1:{MANUAL_TCP_PORT_DEFAULT}")

                active = threading.Event()
                active.set()
                send_t = threading.Thread(target=self._send_loop, args=(chan, active), daemon=True)
                recv_t = threading.Thread(target=self._recv_loop, args=(chan, active), daemon=True)
                send_t.start()
                recv_t.start()
                send_t.join()
                recv_t.join()
            except Exception as e:
                log.debug(f"[{self.name}] SSH tunnel failed: {e}")

            self.connected = False
            self.send_queue = None
            if transport is not None:
                try:
                    transport.close()
                except Exception:
                    pass
            self._transport = None
            self._live_conn = None
            self.reconnects += 1
            if not self._stop.is_set():
                time.sleep(SSH_RECONNECT_DELAY_SEC)

    def _send_loop(self, chan, active):
        while not self._stop.is_set() and active.is_set():
            try:
                data = self.send_queue.get(timeout=0.5)
            except Empty:
                continue
            try:
                # paramiko Channel has no sendall() - it can partial-send
                # like a raw socket, so loop until everything's written.
                while data:
                    sent = chan.send(data)
                    if sent <= 0:
                        raise OSError("SSH channel send returned 0")
                    data = data[sent:]
            except Exception:
                active.clear()
                break

    def _recv_loop(self, chan, active):
        buffer = bytearray()
        while not self._stop.is_set() and active.is_set():
            try:
                if not chan.recv_ready():
                    if chan.closed or chan.eof_received:
                        active.clear()
                        break
                    if time.time() - self.last_rx > HEARTBEAT_TIMEOUT * 2:  # generous backstop - see TcpChannel's own heartbeat staleness check for the same threshold's rationale
                        active.clear()
                        break
                    time.sleep(0.1)
                    continue
                chunk = chan.recv(RECV_BUFFER)
                if not chunk:
                    active.clear()
                    break
                self.last_rx = time.time()
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
                        _record_bad_data(self.name, "crc_mismatch",
                            f"type=0x{msg_type:02x} len={payload_len} expected_crc={expected_crc:08x} "
                            f"actual_crc={actual_crc:08x} payload_preview={payload[:32].hex()}")
                        buffer.pop(0)
                        continue
                    del buffer[:total]
                    self.recv_queue.put((msg_type, payload))
            except Exception:
                active.clear()
                break


# ----------------------------------------------------------------------
# Shared state
# ----------------------------------------------------------------------
class SharedState:
    def __init__(self):
        self.latest_telemetry = {}
        self.latest_state = {}
        self.connected = False
        self.file_tree = None           # None = no data yet; [] = empty dir (valid)
        self.joystick_state = {         # physical joystick live data
            "axes": [],
            "buttons": [],
            "name": "",
            "connected": False,
        }
        # Whether joystick_thread() actually relays physical-joystick teleop
        # over the tether. Default True (matches prior always-on behavior).
        # The visualiser above keeps working either way - this only gates
        # the outbound TCP send, so disabling it is purely "stop actually
        # driving the vehicle from this stick", not "stop showing it".
        self.physical_joystick_enabled = True
        # Latest heading/mag reading for the Compass panel - only populated
        # while the Jetson's mag_stream_enabled flag is on (see /api/compass).
        self.latest_compass = {}
        # ROS2 Parameters panel cache - None = no data yet, [] = valid empty.
        self.param_list = None
        # Per-node failure/skip reasons from the last gather, if any - see
        # jetson_tcp_node_d_m.py's _list_ros_params. Surfaced in the UI so
        # a partial/empty result is diagnosable instead of looking broken.
        self.param_list_errors = []
        # Result of the most recent reboot attempt - None = still waiting/
        # no attempt yet. See api_reboot's synchronous wait pattern.
        self.reboot_result = None
        # Live feed from the SECONDARY channel (read-only, shown as a log
        # in the UI) - separate from latest_telemetry/latest_state, which
        # always reflect the ACTIVE channel only.
        self.secondary_telemetry = {}
        self.secondary_channel_name = None
        self.topic_list = None          # None = no data yet; [] = empty (valid)
        # "Start Fast Mission": maps already saved on the vehicle (see
        # jetson_tcp_node_d_m.py's _list_saved_maps) - None = no data yet;
        # [] = none saved yet (valid).
        self.map_list = None
        # Rolling log for the Black Box Console - each entry:
        # {"seq", "ts", "data_type", "message"}. `seq` lets the UI poll
        # incrementally instead of re-sending the whole log every time.
        self.blackbox_log = deque(maxlen=500)
        self.blackbox_seq = 0
        # Rolling log for the Remote Terminal panel - two entry kinds:
        #   {"seq","kind":"cmd","request_id","command","ts"}
        #   {"seq","kind":"result","request_id","success","exit_code",
        #    "timed_out","output","truncated","ts"}
        # matched up client-side by request_id. Same incremental-seq
        # polling pattern as blackbox_log above.
        self.terminal_log = deque(maxlen=200)
        self.terminal_seq = 0
        # Mission Planner: latest ack/feedback/result from the Jetson's
        # start_mission/cancel_mission bridge (jetson_tcp_node_d_m.py). Each
        # is just the most recent message of its kind - the mission status
        # box polls these, it doesn't need history the way blackbox/terminal do.
        self.mission_ack = None
        self.mission_feedback = None
        self.mission_result = None
        # Rolling log of corrupted/dropped/unparseable data actually
        # received on the wire - CRC mismatches (RS-485 noise/tether
        # interference - this is real bit-level corruption, not just a
        # disagreement), malformed JSON payloads, and unrecognized packet
        # types. Same incremental-seq polling pattern as blackbox_log/
        # terminal_log above. Answers "how do I know if data is getting
        # dropped or mishandled" directly instead of only being visible in
        # the server's own stdout log.
        self.bad_data_log = deque(maxlen=300)
        self.bad_data_seq = 0
        # Wifi Comms panel (Live Ops). Last `nmcli device wifi list
        # --rescan yes` result from the Jetson (see jetson_tcp_node_d_m.py's
        # "wifi_scan" handler) - None = no scan yet; [] = scan ran, nothing
        # found. Each entry: {"ssid","signal","security","in_use","saved"}.
        self.wifi_networks = None
        self.wifi_scan_in_progress = False
        self.wifi_scan_started_ts = 0.0
        # Whether/why the last scan attempt failed (e.g. sudo rejected the
        # password, or none was configured/typed in) - surfaced directly in
        # the panel so a failed scan doesn't just look like "no networks
        # nearby" with no explanation.
        self.wifi_scan_success = True
        self.wifi_scan_message = ""
        # Saved NetworkManager connection profiles already on the Jetson
        # (SSIDs only - passwords never leave the Jetson, see "wifi_connect"
        # handler). None = not fetched yet; [] = none saved.
        self.wifi_saved = None
        # SSID currently being connected to, or None - drives the orange
        # "connecting" LED in the UI while a connect attempt is in flight.
        self.wifi_connecting_ssid = None
        self.wifi_connect_started_ts = 0.0
        # Result of the most recent connect attempt: {"ssid","success","message","ts"}
        self.wifi_last_connect_result = None
        self.lock = threading.Lock()

shared = SharedState()

def _record_bad_data(channel_name: str, kind: str, detail: str):
    """
    Append one entry to the Bad Data console. Called from several
    independently-threaded places that all touch raw/unverified bytes off
    the wire before they're known-good: TcpChannel/SshTunnelChannel/
    WebSocketChannel's recv loops (CRC mismatch) and process_incoming
    (malformed JSON, unrecognized packet type) - so this must be safe to
    call concurrently, same as every other shared.* mutation in this file
    (protected by shared.lock).
    """
    with shared.lock:
        shared.bad_data_seq += 1
        shared.bad_data_log.append({
            "seq": shared.bad_data_seq,
            "ts": time.time(),
            "channel": channel_name,
            "kind": kind,
            "detail": detail,
        })

# ----------------------------------------------------------------------
# Flask App
# ----------------------------------------------------------------------
app = Flask(__name__)
channels = ChannelManager(CHANNELS_CONFIG)

# Mission Planner: the copied/adapted map-planner page, alongside this
# script (same idea as the existing jetson_files/laptop_files layout).
MISSION_PLANNER_ASSETS_DIR = Path(__file__).resolve().parent / "mission_planner_assets"
SAVED_MAPS_DIR = Path(__file__).resolve().parent / "saved_maps"
SAVED_MAPS_DIR.mkdir(parents=True, exist_ok=True)

# File transfer state (protected by _dl_lock)
_dl_lock = threading.Lock()
pending_upload_ack  = None      # (filename, event, result: {"ready": bool, "message": str})
# Resolved once the Jetson confirms it actually finished writing the file
# (byte-count-verified - see FileTransferHandler._handle_complete on the
# Jetson side), not just once we've finished enqueueing the chunks -
# _upload_file_to_jetson blocks on this so a caller's "ok" genuinely means
# the file reached the vehicle intact, not just that we stopped sending.
pending_upload_complete = None  # (filename, event, result: {"success": bool, "message": str})
download_state      = None      # dict: file, expected, received, filename, temp_dir
download_file_path  = None
download_complete   = False
download_error      = None

# Remote Terminal request-id counter (protected by _terminal_lock)
_terminal_lock = threading.Lock()
_terminal_request_seq = 0

def _next_terminal_request_id() -> int:
    global _terminal_request_seq
    with _terminal_lock:
        _terminal_request_seq += 1
        return _terminal_request_seq

# ROS2 Parameters panel: set_param request-id counter + pending-ack tracking
# (protected by _param_lock). One HTTP request blocks on its own Event, the
# same "synchronous wait for a specific reply" pattern api_list_files/
# api_list_topics use, just keyed by request_id since multiple param edits
# could be in flight from the UI at once.
_param_lock = threading.Lock()
_param_request_seq = 0
_param_pending = {}   # request_id -> threading.Event
_param_results = {}   # request_id -> result dict

def _next_param_request_id() -> int:
    global _param_request_seq
    with _param_lock:
        _param_request_seq += 1
        return _param_request_seq

def _record_param_result(msg):
    request_id = msg.get("request_id")
    with _param_lock:
        event = _param_pending.get(request_id)
        if event is None:
            # Nobody is waiting anymore - api_set_param's wait() already
            # timed out and popped this request_id (e.g. a slow set on a
            # busy node, or a mid-request channel failover). Drop the
            # result rather than storing it forever - a stored-but-never-
            # read entry here would never get popped by anyone, an
            # unbounded leak in this long-running process across enough
            # timed-out requests.
            return
        _param_results[request_id] = {
            "success": msg.get("success", False),
            "node": msg.get("node"),
            "name": msg.get("name"),
            "message": msg.get("message", ""),
        }
    event.set()

# ----------------------------------------------------------------------
# Background threads – process incoming packets from Jetson, one per
# channel (each channel has its own recv_queue, drained independently so a
# stalled/slow channel never blocks the others).
# ----------------------------------------------------------------------
def process_incoming(ch: TcpChannel):
    global pending_upload_ack, pending_upload_complete, download_state, download_file_path
    global download_complete, download_error

    # ch._stop is never set for the primary config-based channels (only at
    # full app shutdown), so this is a no-op change for them - but for a
    # manual channel, ChannelManager.remove_channel() calls ch.stop() when
    # the operator disconnects it, which sets this same flag. Without
    # checking it here, this thread would keep polling a dead channel's
    # recv_queue forever - a leaked thread per manual connect/disconnect
    # cycle, found via code review after the fact, not caught before this
    # shipped.
    while not ch._stop.is_set():
        try:
            msg_type, payload = ch.recv_queue.get(timeout=0.5)
        except Empty:
            continue

        if msg_type == PKT_HEARTBEAT:
            ch.send(pack_message(PKT_HEARTBEAT_ACK, b""))

        elif msg_type == PKT_HEARTBEAT_ACK:
            pass

        elif msg_type == PKT_JSON:
            try:
                msg = json.loads(payload.decode("utf-8"))
                mtype = msg.get("type")
                if mtype == "telemetry":
                    data = msg.get("data", {})
                    if ch is channels.active():
                        with shared.lock:
                            shared.latest_telemetry = data
                    elif ch is channels.secondary():
                        with shared.lock:
                            shared.secondary_telemetry = data
                            shared.secondary_channel_name = ch.name
                elif mtype == "state":
                    if ch is channels.active():
                        with shared.lock:
                            shared.latest_state = msg.get("data", {})
                elif mtype == "file_list":
                    with shared.lock:
                        shared.file_tree = msg.get("tree", [])
                elif mtype == "topic_list":
                    with shared.lock:
                        shared.topic_list = msg.get("topics", [])
                elif mtype == "map_list":
                    with shared.lock:
                        shared.map_list = msg.get("maps", [])
                elif mtype == "param_list":
                    with shared.lock:
                        shared.param_list = msg.get("params", [])
                        shared.param_list_errors = msg.get("errors", [])
                elif mtype == "reboot_result":
                    # Only sent on FAILURE (see _do_reboot) - success means
                    # the machine is going down and this message never
                    # arrives, which the operator reads via the link
                    # dropping. Primary feedback path is api_reboot's
                    # synchronous wait on shared.reboot_result (same pattern
                    # as list_files/list_params); also recorded into
                    # blackbox_log directly (bypassing blackbox_stream_
                    # enabled - this is direct feedback on an operator-
                    # initiated action, not passive gated telemetry) as a
                    # persistent record for anyone else watching the console.
                    with shared.lock:
                        shared.reboot_result = {
                            "success": msg.get("success", False),
                            "message": msg.get("message", ""),
                            "needs_password": msg.get("needs_password", False),
                        }
                        shared.blackbox_seq += 1
                        shared.blackbox_log.append({
                            "seq": shared.blackbox_seq,
                            "ts": time.time(),
                            "data_type": "reboot_failed",
                            "message": msg.get("message", "reboot failed"),
                        })
                elif mtype == "compass":
                    with shared.lock:
                        shared.latest_compass = {
                            "heading": msg.get("heading"),
                            "mag": msg.get("mag", {}),
                            "ts": msg.get("ts", time.time()),
                        }
                elif mtype == "set_param_result":
                    _record_param_result(msg)
                elif mtype in ("mission_ack", "mission_feedback", "mission_result"):
                    # Simple "last message wins" cache - see SharedState.
                    # _ts lets the UI dedupe repeated results across polls.
                    msg["_ts"] = time.time()
                    with shared.lock:
                        setattr(shared, mtype, msg)
                elif mtype == "blackbox":
                    with shared.lock:
                        shared.blackbox_seq += 1
                        shared.blackbox_log.append({
                            "seq": shared.blackbox_seq,
                            "ts": msg.get("ts", time.time()),
                            "data_type": msg.get("data_type", ""),
                            "message": msg.get("message", ""),
                        })
                elif mtype == "command_result":
                    with shared.lock:
                        shared.terminal_seq += 1
                        shared.terminal_log.append({
                            "seq": shared.terminal_seq,
                            "kind": "result",
                            "request_id": msg.get("request_id"),
                            "success": msg.get("success", False),
                            "exit_code": msg.get("exit_code"),
                            "timed_out": msg.get("timed_out", False),
                            "output": msg.get("output", ""),
                            "truncated": msg.get("truncated", False),
                            "max_lines": msg.get("max_lines", RUN_COMMAND_DEFAULT_MAX_LINES),
                            "ts": time.time(),
                        })
                elif mtype == "file_ready":
                    with _dl_lock:
                        ack = pending_upload_ack
                    if ack:
                        # Accept if filenames match OR if the Jetson omitted the
                        # filename field (shouldn't happen but guards against it).
                        jetson_fname = msg.get("filename")
                        if jetson_fname is None or ack[0] == jetson_fname:
                            ack[2]["ready"] = True
                            ack[1].set()
                        else:
                            log.warning(f"file_ready filename mismatch: expected '{ack[0]}', got '{jetson_fname}' – signalling anyway")
                            ack[2]["ready"] = True
                            ack[1].set()
                elif mtype == "upload_complete":
                    # Byte-count-verified completion from the Jetson (see
                    # FileTransferHandler._handle_complete/_abort_upload) -
                    # this is what _upload_file_to_jetson actually blocks on
                    # to know the file reached the vehicle intact, not just
                    # that we finished enqueueing it.
                    with _dl_lock:
                        comp = pending_upload_complete
                    if comp:
                        jetson_fname = msg.get("filename")
                        if jetson_fname is None or comp[0] == jetson_fname:
                            comp[2]["success"] = msg.get("success", False)
                            comp[2]["message"] = msg.get("message", "")
                            comp[1].set()
                        else:
                            log.warning(f"upload_complete filename mismatch: expected '{comp[0]}', got '{jetson_fname}' – ignoring")
                elif mtype in ("delete_ack", "command_ack", "mode_ack", "teleop_ack"):
                    log.info(f"[{ch.name}] Ack from Jetson: {msg}")
                elif mtype == "error":
                    message = msg.get("message")
                    log.error(f"[{ch.name}] Error from Jetson: {message}")
                    with _dl_lock:
                        if download_state:
                            download_error = message
                        # An error arriving before either upload-ack message
                        # (e.g. the Jetson couldn't even open the destination
                        # file) would otherwise leave _upload_file_to_jetson
                        # waiting out its full timeout for a reply that's
                        # never coming - fail it immediately instead.
                        if pending_upload_ack:
                            pending_upload_ack[2]["ready"] = False
                            pending_upload_ack[2]["message"] = message
                            pending_upload_ack[1].set()
                        if pending_upload_complete:
                            pending_upload_complete[2]["success"] = False
                            pending_upload_complete[2]["message"] = message
                            pending_upload_complete[1].set()
                elif mtype == "wifi_scan_result":
                    with shared.lock:
                        shared.wifi_networks = msg.get("networks", [])
                        shared.wifi_scan_in_progress = False
                        shared.wifi_scan_success = msg.get("success", True)
                        shared.wifi_scan_message = msg.get("message", "")
                elif mtype == "wifi_saved_list":
                    with shared.lock:
                        shared.wifi_saved = msg.get("networks", [])
                elif mtype == "wifi_connect_result":
                    with shared.lock:
                        result_ssid = msg.get("ssid")
                        success = msg.get("success", False)
                        shared.wifi_last_connect_result = {
                            "ssid": result_ssid,
                            "success": success,
                            "message": msg.get("message", ""),
                            "ts": time.time(),
                        }
                        if shared.wifi_connecting_ssid == result_ssid:
                            shared.wifi_connecting_ssid = None
                        if success and result_ssid:
                            # Reflect the new connection immediately rather
                            # than leaving the dot on "connecting" -> idle
                            # gray until the operator happens to trigger
                            # another scan/saved-list fetch - only one wifi
                            # connection is active at a time, so mark this
                            # SSID in_use and clear any stale in_use
                            # elsewhere in both cached lists right now.
                            if shared.wifi_networks:
                                for n in shared.wifi_networks:
                                    n["in_use"] = (n.get("ssid") == result_ssid)
                            if shared.wifi_saved:
                                for n in shared.wifi_saved:
                                    n["in_use"] = (n.get("ssid") == result_ssid)
                else:
                    log.debug(f"[{ch.name}] Other JSON: {msg}")
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                log.error(f"[{ch.name}] Malformed JSON from Jetson: {e}")
                _record_bad_data(ch.name, "malformed_json",
                    f"{e} - raw={payload[:200]!r}")

        elif msg_type == PKT_FILE_REQ:
            try:
                req = json.loads(payload.decode("utf-8"))
                if req.get("direction") == "download":
                    filename = req["filename"]
                    size     = req["size"]
                    log.info(f"Receiving file: {filename} ({size} bytes)")
                    temp_dir = tempfile.mkdtemp()
                    fpath    = Path(temp_dir) / filename
                    f        = open(fpath, "wb")
                    with _dl_lock:
                        download_file_path = fpath
                        download_state     = {
                            "file": f, "expected": size, "received": 0,
                            "filename": filename, "temp_dir": temp_dir,
                        }
                        download_complete  = False
                        download_error     = None
            except Exception as e:
                log.error(f"File request error: {e}")
                with _dl_lock:
                    download_error = str(e)

        elif msg_type == PKT_FILE_CHUNK:
            with _dl_lock:
                ds = download_state
            if ds:
                try:
                    ds["file"].write(payload)
                    ds["received"] += len(payload)
                except Exception as e:
                    log.error(f"Write chunk error: {e}")
                    with _dl_lock:
                        download_error = str(e)
                        download_state = None

        elif msg_type == PKT_FILE_COMPLETE:
            with _dl_lock:
                ds = download_state
            if ds:
                ds["file"].close()
                log.info(f"Download complete: {ds['filename']} ({ds['received']} bytes)")
                with _dl_lock:
                    download_complete = True

        else:
            log.warning(f"Unknown packet type: {msg_type:02x}")
            _record_bad_data(ch.name, "unknown_packet_type",
                f"type=0x{msg_type:02x} len={len(payload)} preview={payload[:32].hex()}")

# ----------------------------------------------------------------------
# Joystick Thread
# ----------------------------------------------------------------------
def joystick_thread():
    if not PYGAME_AVAILABLE:
        log.warning("Pygame not installed – joystick support disabled.")
        return

    pygame.init()
    pygame.joystick.init()

    joystick = None
    prev_buttons = []
    prev_axes    = []
    last_sent    = 0.0
    JOY_KEEPALIVE = 1.0

    while True:
        pygame.event.pump()

        # (Re-)detect joystick
        count = pygame.joystick.get_count()
        if count == 0:
            if joystick is not None:
                log.warning("Joystick disconnected")
                joystick = None
                with shared.lock:
                    shared.joystick_state["connected"] = False
                    shared.joystick_state["name"] = ""
            time.sleep(1.0)
            pygame.joystick.quit()
            pygame.joystick.init()
            continue

        if joystick is None:
            joystick = pygame.joystick.Joystick(0)
            joystick.init()
            name = joystick.get_name()
            log.info(f"Joystick connected: {name}")
            prev_buttons = [False] * joystick.get_numbuttons()
            prev_axes    = [0.0]   * joystick.get_numaxes()
            with shared.lock:
                shared.joystick_state["connected"] = True
                shared.joystick_state["name"]      = name

        now     = time.time()
        axes    = [joystick.get_axis(i)    for i in range(joystick.get_numaxes())]
        buttons = [joystick.get_button(i)  for i in range(joystick.get_numbuttons())]

        # Update shared joystick state for visualiser
        with shared.lock:
            shared.joystick_state["axes"]    = axes
            shared.joystick_state["buttons"] = buttons

        axes_changed    = any(abs(a - p) > DEADZONE for a, p in zip(axes, prev_axes))
        buttons_changed = buttons != prev_buttons
        keepalive_due   = (now - last_sent) >= JOY_KEEPALIVE

        if axes_changed or buttons_changed or keepalive_due:
            # Relay toggle (default on, matches prior always-on behavior) -
            # gates only the outbound TCP send. prev_axes/prev_buttons/
            # last_sent are still updated below even while disabled, so
            # re-enabling doesn't fire a burst of "changed since forever"
            # sends for every axis that moved while it was off.
            with shared.lock:
                relay_enabled = shared.physical_joystick_enabled
            if relay_enabled:
                data = {"type": "teleop", "axes": axes, "buttons": buttons}
                active_ch = channels.active()
                if active_ch is not None:
                    active_ch.send(pack_json(PKT_JSON, data))
            prev_axes    = axes[:]
            prev_buttons = buttons[:]
            last_sent    = now

        time.sleep(0.05)  # 20 Hz

# ----------------------------------------------------------------------
# Flask Routes
# ----------------------------------------------------------------------
@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/telemetry')
def api_telemetry():
    with shared.lock:
        return jsonify(shared.latest_telemetry)

@app.route('/api/state')
def api_state():
    with shared.lock:
        return jsonify(shared.latest_state)

@app.route('/api/connected')
def api_connected():
    return jsonify({"connected": shared.connected})

@app.route('/api/channels')
def api_channels():
    """Status of all 3 tether channels plus the secondary channel's live
    telemetry feed, for the Link Channels panel in the UI."""
    snap = channels.snapshot()
    with shared.lock:
        snap["secondary_telemetry"] = shared.secondary_telemetry
        snap["secondary_channel_name"] = shared.secondary_channel_name
    return jsonify(snap)

@app.route('/api/force_channel', methods=['POST'])
def api_force_channel():
    """Manually pin (or clear) which channel is 'active', from the Link
    Channels panel's Force button. The target channel must be connected.
    The override is relayed to the Jetson directly over that same channel
    (or, when clearing, over whatever is currently active) so both ends
    agree on which one is authoritative - forcing only the GCS side would
    leave Jetson mirroring telemetry to its own automatically-picked pair."""
    data = request.get_json() or {}
    name = data.get("channel") or None

    target = None
    if name is not None:
        for ch in channels._channels_snapshot():
            if ch.name == name:
                target = ch
                break
        if target is None:
            return jsonify({"error": f"Unknown channel '{name}'"}), 400
        if not target.connected:
            return jsonify({"error": f"{name} is not connected"}), 409

    channels.force_active(name)

    carrier = target if target is not None else channels.active()
    if carrier is not None:
        carrier.send(pack_json(PKT_JSON, {"type": "force_active", "channel": name}))

    return jsonify({"status": "ok", "forced": name})

@app.route('/api/primary_tether', methods=['POST'])
def api_primary_tether():
    """Operator override to turn the primary/configured tether channels
    (CH-A/B/C/test) on or off - e.g. to deliberately test a manual channel
    in isolation. Default is always on; manual channels are never affected
    by this either way. Doesn't require anything to currently be connected
    (unlike force_channel) since disabling the tether while it's down is a
    perfectly normal thing to want."""
    data = request.get_json() or {}
    enabled = bool(data.get("enabled", True))
    channels.set_primary_enabled(enabled)
    return jsonify({"status": "ok", "enabled": enabled})

_SAFE_CHANNEL_NAME_RE = re.compile(r"[^A-Za-z0-9._:\-]")

def _sanitize_channel_name(raw: str, fallback: str) -> str:
    """
    Restrict a manual channel's name to a safe character set before it's
    ever stored as `channel.name`.

    Channel names get rendered into the UI's onclick="fn('...')" handlers
    (e.g. manualDisconnect/forceChannel) via escAttr(), which only HTML-
    attribute-escapes - it does NOT prevent breaking out of the single-
    quoted JS string those handlers construct, since the browser HTML-
    decodes the attribute value before parsing it as JS. That pattern was
    safe everywhere it was previously used because every channel name came
    from trusted CHANNELS_CONFIG constants; this is the first path where a
    channel name can come from raw operator/HTTP input (`label`, or a
    fallback built from `host`/`url`), so it's sanitized right here at the
    single point where untrusted data enters the naming system, rather
    than trying to retrofit every onclick call site elsewhere in the file.
    """
    cleaned = _SAFE_CHANNEL_NAME_RE.sub("_", (raw or "").strip())[:64]
    return cleaned or fallback

def _channel_link_kind(ch) -> str:
    """Best-effort label for what transport a channel represents - used
    only to WARN (never block, see api_mission_upload_map) when a map
    transfer isn't going out over a genuine Ethernet link. Primary CH-A/B/
    C/test channels and manual Tethered/Ethernet/Localhost connections are
    all the exact same TcpChannel class with nothing distinguishing them
    except the `link_kind` label api_manual_connect stamps on manual ones;
    SSH/WebSocket channels are distinguishable by class instead."""
    kind = getattr(ch, "link_kind", None)
    if kind:
        return kind
    if isinstance(ch, SshTunnelChannel):
        return "ssh"
    if isinstance(ch, WebSocketChannel):
        return "websocket"
    return "tether"

def _channel_is_ethernet(ch) -> bool:
    return _channel_link_kind(ch) == "ethernet"

@app.route('/api/manual_connect', methods=['POST'])
def api_manual_connect():
    """
    Manual Connection tab - adds a new BACKUP channel on top of the
    always-on primary tether (never replaces it - ChannelManager.
    add_channel always assigns lowest priority). `mode` selects the
    transport:
      - tethered / ethernet / localhost: all three are literally the same
        mechanism (a plain TcpChannel, unmodified) - the label is only
        which physical network path the operator is using to reach the
        Jetson's manual listening port (MANUAL_TCP_PORT_DEFAULT), not
        different backend code.
      - ssh: tunneled through the Jetson's own sshd via paramiko, using
        username+password collected here (held only in memory for this
        channel's lifetime - never written to disk or logged).
      - websocket: a hand-rolled WS client to a ws://.../wss://... URL.
    """
    data = request.get_json() or {}
    mode = (data.get("mode") or "").strip().lower()
    label = (data.get("label") or "").strip()
    existing_names = {c.name for c in channels._channels_snapshot()}

    if mode in ("tethered", "ethernet", "localhost"):
        host = (data.get("host") or "").strip()
        port = data.get("port")
        if not host or port in (None, ""):
            return jsonify({"error": "Missing host or port"}), 400
        try:
            port = int(port)
        except (TypeError, ValueError):
            return jsonify({"error": "Port must be a number"}), 400
        name = _sanitize_channel_name(label or f"MANUAL-{mode.upper()}-{host}:{port}", f"MANUAL-{mode.upper()}-{int(time.time())}")
        if name in existing_names:
            return jsonify({"error": f"A channel named '{name}' already exists"}), 409
        ch = TcpChannel(name, host, port, priority=0)  # overwritten by add_channel
        # Tagged so _channel_link_kind() can tell an Ethernet manual link
        # apart from Tethered/Localhost ones later (e.g. the map-upload
        # ethernet-only warning) - otherwise all three are the exact same
        # TcpChannel class with nothing distinguishing them.
        ch.link_kind = mode
        channels.add_channel(ch)
        return jsonify({"status": "ok", "name": name})

    elif mode == "ssh":
        if not PARAMIKO_AVAILABLE:
            return jsonify({"error": "paramiko is not installed on the GCS - run: pip install paramiko"}), 501
        host = (data.get("host") or "").strip()
        port = data.get("port") or 22
        username = (data.get("username") or "").strip()
        password = data.get("password") or ""
        if not host or not username:
            return jsonify({"error": "Missing host or username"}), 400
        try:
            port = int(port)
        except (TypeError, ValueError):
            return jsonify({"error": "Port must be a number"}), 400
        name = _sanitize_channel_name(label or f"MANUAL-SSH-{host}:{port}", f"MANUAL-SSH-{int(time.time())}")
        if name in existing_names:
            return jsonify({"error": f"A channel named '{name}' already exists"}), 409
        ch = SshTunnelChannel(name, host, port, username, password)
        channels.add_channel(ch)
        return jsonify({"status": "ok", "name": name})

    elif mode == "websocket":
        url = (data.get("url") or "").strip()
        if not url:
            return jsonify({"error": "Missing connect URL"}), 400
        if not (url.startswith("ws://") or url.startswith("wss://")):
            return jsonify({"error": "URL must start with ws:// or wss://"}), 400
        name = _sanitize_channel_name(label or f"MANUAL-WS-{url}", f"MANUAL-WS-{int(time.time())}")
        if name in existing_names:
            return jsonify({"error": f"A channel named '{name}' already exists"}), 409
        ch = WebSocketChannel(name, url)
        channels.add_channel(ch)
        return jsonify({"status": "ok", "name": name})

    else:
        return jsonify({"error": f"Unknown mode '{mode}'"}), 400

@app.route('/api/manual_disconnect', methods=['POST'])
def api_manual_disconnect():
    """Stop and remove a manual channel by name. Refuses to touch a
    configured (CH-A/B/C/test) channel even if asked - see ChannelManager.
    remove_channel's priority-floor guard."""
    data = request.get_json() or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "Missing name"}), 400
    if not channels.remove_channel(name):
        return jsonify({"error": f"No manual channel named '{name}'"}), 404
    return jsonify({"status": "ok"})

@app.route('/api/joystick')
def api_joystick():
    with shared.lock:
        data = dict(shared.joystick_state)
        data["relay_enabled"] = shared.physical_joystick_enabled
    return jsonify(data)

@app.route('/api/physical_joystick', methods=['POST'])
def api_physical_joystick():
    """Toggle whether joystick_thread() actually relays the physical
    joystick over the tether (default on). Purely GCS-local - the Jetson
    doesn't need to know, it already accepts teleop from any source."""
    data = request.get_json() or {}
    enabled = bool(data.get("enabled", True))
    with shared.lock:
        shared.physical_joystick_enabled = enabled
    return jsonify({"status": "ok", "enabled": enabled})

@app.route('/api/command', methods=['POST'])
def api_command():
    active_ch = channels.active()
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON"}), 400
    active_ch.send(pack_json(PKT_JSON, data))
    return jsonify({"status": "sent"})

def _resolve_wifi_target_channel(data):
    """
    Which channel a Wifi Comms command should go over.

    channels.active()'s automatic priority selection ALWAYS prefers a
    connected CH-A/B/C/CH-test over ANY manual connection, no matter how
    it was made (see ChannelManager.MANUAL_PRIORITY_FLOOR) - fine for most
    commands, but wifi_scan/wifi_connect physically reconfigure whatever
    device answers, so silently overriding an operator's own explicit
    Manual Connection (e.g. to a real Jetson over Ethernet) with some
    other channel that happens to also be up (e.g. a stray local CH-test)
    is exactly the kind of mistake this panel exists to prevent. If the
    operator picked a specific channel in the panel's own selector (sent
    as `channel`), use exactly that one and nothing else; only fall back
    to channels.active() when they left it on "Auto".

    Returns (channel_or_None, error_message_or_None).
    """
    name = (data.get("channel") or "").strip()
    if not name:
        return channels.active(), None
    for ch in channels._channels_snapshot():
        if ch.name == name:
            if not ch.connected:
                return None, f"Channel '{name}' is not connected"
            return ch, None
    return None, f"Unknown channel '{name}'"

@app.route('/api/wifi_scan', methods=['POST'])
def api_wifi_scan():
    """Live Ops "Wifi Comms" panel's List Devices button - triggers a fresh
    `nmcli device wifi list --rescan yes` on the Jetson (see
    jetson_tcp_node_d_m.py's "wifi_scan" handler). Async, like the channel
    list: the UI polls /api/wifi_status for shared.wifi_networks rather
    than blocking this request on a reply.

    `sudo_password`, if the operator typed one into the panel's own Sudo
    Password field (not persisted anywhere - read fresh from the DOM input
    on every request, see wifiSudoPassword() in the frontend), rides along
    only in this POST body - never a query string/URL, never logged here -
    same handling as api_reboot's `password`."""
    data = request.get_json() or {}
    active_ch, err = _resolve_wifi_target_channel(data)
    if err:
        return jsonify({"error": err}), 400
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503
    sudo_password = data.get("sudo_password") or ""
    with shared.lock:
        shared.wifi_scan_in_progress = True
        shared.wifi_scan_started_ts = time.time()
    active_ch.send(pack_json(PKT_JSON, {"type": "wifi_scan", "sudo_password": sudo_password}))
    return jsonify({"status": "sent"})

@app.route('/api/wifi_saved', methods=['POST'])
def api_wifi_saved():
    """Ask the Jetson for its existing NetworkManager wifi connection
    profiles (SSIDs only - passwords never leave the Jetson). Cheap/fast,
    no rescan or sudo needed on that end, so the frontend calls this once
    when the Wifi Comms panel loads, independent of the scan-triggering
    List Devices button."""
    data = request.get_json() or {}
    active_ch, err = _resolve_wifi_target_channel(data)
    if err:
        return jsonify({"error": err}), 400
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503
    active_ch.send(pack_json(PKT_JSON, {"type": "wifi_saved_list"}))
    return jsonify({"status": "sent"})

@app.route('/api/wifi_connect', methods=['POST'])
def api_wifi_connect():
    """Connect the Jetson to a wifi network. If `password` is given, the
    Jetson runs `nmcli device wifi connect <ssid> password <password>`; if
    omitted (operator clicked a "Saved Devices" entry), it reconnects via
    `nmcli connection up "<ssid>"` using the profile NetworkManager already
    has - the GCS never stores wifi passwords itself (see
    jetson_tcp_node_d_m.py's "wifi_connect" handler)."""
    data = request.get_json() or {}
    active_ch, err = _resolve_wifi_target_channel(data)
    if err:
        return jsonify({"error": err}), 400
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503
    ssid = (data.get("ssid") or "").strip()
    if not ssid:
        return jsonify({"error": "SSID required"}), 400
    password = data.get("password") or None
    sudo_password = data.get("sudo_password") or ""
    with shared.lock:
        shared.wifi_connecting_ssid = ssid
        shared.wifi_connect_started_ts = time.time()
        shared.wifi_last_connect_result = None
    payload = {"type": "wifi_connect", "ssid": ssid, "sudo_password": sudo_password}
    if password:
        payload["password"] = password
    active_ch.send(pack_json(PKT_JSON, payload))
    return jsonify({"status": "sent"})

@app.route('/api/wifi_status')
def api_wifi_status():
    """Polled by the Wifi Comms panel (pollWifi) - same fire-and-poll
    pattern as /api/channels. Lazily clears scan_in_progress/
    connecting_ssid if the Jetson never replies (old firmware without this
    feature, or the link dropped mid-scan/connect) so the UI doesn't get
    stuck showing "Scanning…"/an orange LED forever."""
    now = time.time()
    with shared.lock:
        if shared.wifi_scan_in_progress and now - shared.wifi_scan_started_ts > 20:
            shared.wifi_scan_in_progress = False
        if shared.wifi_connecting_ssid and now - shared.wifi_connect_started_ts > 20:
            shared.wifi_connecting_ssid = None
        return jsonify({
            "networks": shared.wifi_networks,
            "saved": shared.wifi_saved,
            "scan_in_progress": shared.wifi_scan_in_progress,
            "scan_success": shared.wifi_scan_success,
            "scan_message": shared.wifi_scan_message,
            "connecting_ssid": shared.wifi_connecting_ssid,
            "last_connect_result": shared.wifi_last_connect_result,
        })

@app.route('/api/reboot', methods=['POST'])
def api_reboot():
    """
    Reboot the whole AUV/Jetson. Deliberately its own route rather than
    going through the generic api_command relay above - `password` (only
    present if the operator had to enter one after a first passwordless
    attempt failed) must never ride along on any other command, and is
    never written to a Flask access log here since it only ever travels in
    the POST body, never a URL/query string.

    Synchronous wait for the definite "reboot_result" reply, same pattern
    as api_list_files/api_list_params - lets the UI show the operator
    exactly what happened (including whether a password is needed) instead
    of firing the command blind and hoping.
    """
    active_ch = channels.active()
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503
    data = request.get_json() or {}
    password = data.get("password") or ""
    with shared.lock:
        shared.reboot_result = None
    active_ch.send(pack_json(PKT_JSON, {"type": "reboot_system", "password": password}))
    deadline = time.time() + 15.0
    while time.time() < deadline:
        with shared.lock:
            result = shared.reboot_result
        if result is not None:
            return jsonify(result)
        time.sleep(0.1)
    # No reply within 15s - could mean it actually succeeded (machine went
    # down before it could reply) or the Jetson/link is just slow; either
    # way the "NO LINK" pill will confirm a real reboot within a few more
    # seconds, so this isn't necessarily bad news.
    return jsonify({
        "success": None,
        "message": "No response within 15s - link may have dropped (possible success) or the Jetson is slow to respond",
        "needs_password": False,
    })

@app.route('/api/list_files', methods=['POST'])
def api_list_files():
    """
    Ask the Jetson to send its file tree and wait for the fresh response.
    We set file_tree to None first so we can distinguish 'not yet received'
    from an empty directory (which is a valid [] response).
    """
    active_ch = channels.active()
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503
    # Invalidate cache so we wait for a genuinely fresh response
    with shared.lock:
        shared.file_tree = None
    active_ch.send(pack_json(PKT_JSON, {"type": "list_files"}))
    deadline = time.time() + 5.0          # give the Jetson up to 5 s to respond
    while time.time() < deadline:
        with shared.lock:
            tree = shared.file_tree
        if tree is not None:              # None = still waiting; [] = empty dir (valid)
            return jsonify({"tree": tree})
        time.sleep(0.1)
    return jsonify({"error": "Timeout waiting for file list from Jetson"}), 504

@app.route('/api/list_topics', methods=['POST'])
def api_list_topics():
    """Ask the Jetson for a snapshot of the live ROS2 graph (topic name ->
    type), same request/wait pattern as api_list_files above."""
    active_ch = channels.active()
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503
    with shared.lock:
        shared.topic_list = None
    active_ch.send(pack_json(PKT_JSON, {"type": "list_topics"}))
    deadline = time.time() + 5.0
    while time.time() < deadline:
        with shared.lock:
            topics = shared.topic_list
        if topics is not None:
            return jsonify({"topics": topics})
        time.sleep(0.1)
    return jsonify({"error": "Timeout waiting for topic list from Jetson"}), 504

@app.route('/api/list_params', methods=['POST'])
def api_list_params():
    """Ask the Jetson for every live node's parameters (name + current
    value), same request/wait pattern as api_list_files/api_list_topics -
    just a MUCH longer deadline: the Jetson shells out to `ros2 param dump`
    once per live node (each invocation commonly costs 1-3s of its own DDS
    discovery startup), so a graph with a dozen-plus nodes can easily take
    30+ seconds to gather, on top of however long the ~10kbps tether itself
    takes to carry the resulting JSON back. The Jetson bounds its own total
    gather time (LIST_PARAMS_TOTAL_BUDGET_SEC, 45s) and always replies
    exactly once even on partial/total failure - see _handle_list_params -
    so this deadline just needs to comfortably exceed that plus transfer
    time, not guess at per-node timing itself."""
    active_ch = channels.active()
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503
    with shared.lock:
        shared.param_list = None
        shared.param_list_errors = []
    active_ch.send(pack_json(PKT_JSON, {"type": "list_params"}))
    deadline = time.time() + 60.0
    while time.time() < deadline:
        with shared.lock:
            params = shared.param_list
            errors = shared.param_list_errors
        if params is not None:
            return jsonify({"params": params, "errors": errors})
        time.sleep(0.1)
    return jsonify({"error": "Timeout waiting for parameter list from Jetson"}), 504

@app.route('/api/set_param', methods=['POST'])
def api_set_param():
    """Push one parameter update to the Jetson and wait for its definite
    updated/failed acknowledgement (set_param_result), keyed by request_id
    so concurrent edits from the UI can't cross-match to the wrong row."""
    active_ch = channels.active()
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503
    data = request.get_json() or {}
    node = data.get("node", "")
    name = data.get("name", "")
    value = data.get("value")
    if not node or not name:
        return jsonify({"error": "Missing node or name"}), 400

    request_id = _next_param_request_id()
    event = threading.Event()
    with _param_lock:
        _param_pending[request_id] = event

    active_ch.send(pack_json(PKT_JSON, {
        "type": "set_param", "request_id": request_id,
        "node": node, "name": name, "value": value,
    }))

    ok = event.wait(timeout=10.0)
    with _param_lock:
        result = _param_results.pop(request_id, None)
        _param_pending.pop(request_id, None)
    if not ok or result is None:
        return jsonify({"error": "Timeout waiting for update acknowledgement"}), 504
    return jsonify(result)

@app.route('/api/compass')
def api_compass():
    with shared.lock:
        return jsonify(shared.latest_compass)

@app.route('/api/blackbox')
def api_blackbox():
    """Incremental Black Box Console feed - pass ?since=<last seq seen> to
    get only new entries; the response's own 'seq' is the next cursor."""
    since = request.args.get('since', 0, type=int)
    with shared.lock:
        entries = [e for e in shared.blackbox_log if e["seq"] > since]
        seq = shared.blackbox_seq
    return jsonify({"entries": entries, "seq": seq})

@app.route('/api/bad_data')
def api_bad_data():
    """Incremental Bad Data console feed - same ?since=<seq> pattern as
    /api/blackbox above. Always-on (not gated by any streaming toggle,
    unlike the Black Box console) since it's diagnosing link problems,
    which is exactly when an operator most needs it visible."""
    since = request.args.get('since', 0, type=int)
    with shared.lock:
        entries = [e for e in shared.bad_data_log if e["seq"] > since]
        seq = shared.bad_data_seq
    return jsonify({"entries": entries, "seq": seq})

@app.route('/api/run_terminal', methods=['POST'])
def api_run_terminal():
    """
    Remote Terminal panel: run a shell command on the Jetson in its own
    subprocess (never inline in the ROS2 node - see _run_shell_command
    there), capped to `max_lines` of output (GCS-configurable per request,
    default RUN_COMMAND_DEFAULT_MAX_LINES = 50 - see the input next to the
    timeout box in the UI).

    This does NOT block waiting for the result - it records a "cmd" entry
    and returns immediately; the eventual "result" entry (matched by
    request_id) is picked up by the same /api/terminal_log poll the UI
    already uses, so a slow command (up to the Jetson-side timeout, default
    30s / max 300s) never ties up this HTTP request or the rest of the UI.
    """
    active_ch = channels.active()
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503
    data = request.get_json() or {}
    command = (data.get("command") or "").strip()
    if not command:
        return jsonify({"error": "Missing command"}), 400
    timeout = data.get("timeout")
    max_lines = data.get("max_lines")

    request_id = _next_terminal_request_id()
    with shared.lock:
        shared.terminal_seq += 1
        shared.terminal_log.append({
            "seq": shared.terminal_seq,
            "kind": "cmd",
            "request_id": request_id,
            "command": command,
            "ts": time.time(),
        })

    payload = {"type": "run_command", "request_id": request_id, "command": command}
    if timeout is not None:
        payload["timeout"] = timeout
    if max_lines is not None:
        payload["max_lines"] = max_lines
    active_ch.send(pack_json(PKT_JSON, payload))
    return jsonify({"status": "sent", "request_id": request_id})

@app.route('/api/terminal_log')
def api_terminal_log():
    """Incremental Remote Terminal feed - same ?since=<seq> pattern as
    /api/blackbox above."""
    since = request.args.get('since', 0, type=int)
    with shared.lock:
        entries = [e for e in shared.terminal_log if e["seq"] > since]
        seq = shared.terminal_seq
    return jsonify({"entries": entries, "seq": seq})

@app.route('/api/delete_file', methods=['POST'])
def api_delete_file():
    active_ch = channels.active()
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503
    data = request.get_json()
    path = (data or {}).get("path", "")
    if not path:
        return jsonify({"error": "Missing path"}), 400
    active_ch.send(pack_json(PKT_JSON, {"type": "delete_file", "path": path}))
    return jsonify({"status": "sent"})

@app.route('/api/delete_folder', methods=['POST'])
def api_delete_folder():
    """Recursively delete a whole folder under jetson_files - kept as its
    own endpoint/cmd_type (not a flag on delete_file) since it's a
    meaningfully more destructive operation; the UI shows a stronger
    confirm() warning for it than the per-file delete."""
    active_ch = channels.active()
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503
    data = request.get_json()
    path = (data or {}).get("path", "")
    if not path:
        return jsonify({"error": "Missing path"}), 400
    active_ch.send(pack_json(PKT_JSON, {"type": "delete_folder", "path": path}))
    return jsonify({"status": "sent"})

def _wait_for_upload_event(active_ch, event: threading.Event, timeout: float, poll: float = 0.5):
    """event.wait(timeout) that also bails out the moment `active_ch` itself
    drops, instead of always sitting out the full timeout even once we
    already know no reply is coming. Returns (fired: bool, link_dropped:
    bool) - link_dropped is only meaningful when fired is False."""
    deadline = time.time() + timeout
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            return False, False
        if event.wait(timeout=min(poll, remaining)):
            return True, False
        if not active_ch.connected:
            return False, True

def _upload_file_to_jetson(active_ch, local_path: Path, filename: str, category: str = None):
    """Send one local file to the Jetson over the given (already-resolved)
    active channel via the PKT_FILE_REQ/CHUNK/COMPLETE flow - the same
    protocol api_upload used inline before this was factored out so
    /api/mission/* routes could reuse it with a "category" tag (routes the
    Jetson-side file into missions/maps/failsafe instead of flat).

    Two-phase, both phases actually confirmed by the Jetson rather than
    assumed:
      1. Wait for "file_ready" (Jetson opened the destination file) before
         sending any bytes.
      2. After queueing every chunk + PKT_FILE_COMPLETE, wait for
         "upload_complete" - a byte-count-verified ack from the Jetson (see
         FileTransferHandler._handle_complete) - before reporting success.
         Without this second wait, a channel reconnect mid-transfer (which
         resets the sender's queue - see TcpChannel._run_session) could
         silently drop trailing chunks and this function would still return
         "ok" just because it finished enqueueing, only failing much later
         and confusingly during Start Mission.

    Returns (ok: bool, error_message: str | None).
    """
    global pending_upload_ack, pending_upload_complete

    ready_event = threading.Event()
    ready_result = {}
    with _dl_lock:
        pending_upload_ack = (filename, ready_event, ready_result)

    file_size = local_path.stat().st_size
    req = {"filename": filename, "size": file_size, "direction": "upload"}
    if category:
        req["category"] = category
    active_ch.send(pack_json(PKT_FILE_REQ, req))

    fired, link_dropped = _wait_for_upload_event(active_ch, ready_event, UPLOAD_READY_TIMEOUT)
    if not fired:
        with _dl_lock:
            pending_upload_ack = None
        return False, "Tether link dropped during upload" if link_dropped else "Upload ready timeout"
    with _dl_lock:
        pending_upload_ack = None
    if not ready_result.get("ready"):
        return False, ready_result.get("message") or "Vehicle rejected upload"

    complete_event = threading.Event()
    complete_result = {}
    with _dl_lock:
        pending_upload_complete = (filename, complete_event, complete_result)

    with open(local_path, "rb") as f:
        while True:
            chunk = f.read(FILE_CHUNK_SIZE)
            if not chunk:
                break
            active_ch.send(pack_message(PKT_FILE_CHUNK, chunk))
    active_ch.send(pack_json(PKT_FILE_COMPLETE, {"status": "ok"}))

    # Scaled to the file size at a deliberately conservative assumed
    # throughput (well under the tether's nominal ~10kbps) plus a fixed
    # floor/buffer for round-trip and protocol overhead - a multi-hundred-
    # KB map on this link can legitimately take minutes, and that's fine as
    # long as it's confirmed, not silently assumed.
    complete_timeout = max(
        UPLOAD_COMPLETE_MIN_TIMEOUT,
        file_size / UPLOAD_ASSUMED_THROUGHPUT_BPS,
    ) + UPLOAD_COMPLETE_TIMEOUT_BUFFER
    fired, link_dropped = _wait_for_upload_event(active_ch, complete_event, complete_timeout)
    if not fired:
        with _dl_lock:
            pending_upload_complete = None
        return False, ("Tether link dropped during upload" if link_dropped else
                        "Upload did not complete (no confirmation from vehicle - check tether link)")
    with _dl_lock:
        pending_upload_complete = None
    if not complete_result.get("success"):
        return False, complete_result.get("message") or "Upload failed on vehicle"
    return True, None

@app.route('/api/upload', methods=['POST'])
def api_upload():
    # Snapshot the active channel once - even if a failover happens mid
    # transfer, the Jetson replies on whichever channel it received the
    # request on, so staying pinned to this same channel object for the
    # rest of the upload keeps the transfer self-consistent.
    active_ch = channels.active()
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    filename  = Path(file.filename).name
    temp_dir  = tempfile.mkdtemp()
    temp_path = Path(temp_dir) / filename
    file.save(temp_path)

    ok, err = _upload_file_to_jetson(active_ch, temp_path, filename)
    shutil.rmtree(temp_dir)
    if not ok:
        return jsonify({"error": err}), 504
    return jsonify({"status": "upload started"})

# ------------------------------------------------------------------
# Mission Planner API - deliberately minimal, per explicit user request to
# cut complexity: no saved-mission records, no CRUD, nothing persisted
# GCS-side. Map Planner (mission_planner_assets/map_planner.html) IS the
# whole Mission Planner - it already has Start/Goal lat-lon fields (used
# for its imagery bbox), reused directly as the mission's start/goal.
# upload_map stages the map on the vehicle; start_direct launches it
# immediately with whatever start/goal/map Map Planner currently holds.
# ------------------------------------------------------------------
@app.route('/api/mission/cancel', methods=['POST'])
def api_mission_cancel():
    active_ch = channels.active()
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503
    active_ch.send(pack_json(PKT_JSON, {"type": "cancel_mission"}))
    return jsonify({"status": "sent"})

@app.route('/api/mission/cancel_all', methods=['POST'])
def api_mission_cancel_all():
    """"Cancel All Missions" - forgetting to cancel a running mission
    before starting/uploading a new one leaves the old one's goal
    orphaned on the action server (see jetson_tcp_node_d_m.py's
    "cancel_all_missions" handler docstring for why the regular Cancel
    Mission button can't reach it) - this clears every goal the server
    has, not just the most recently started one."""
    active_ch = channels.active()
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503
    active_ch.send(pack_json(PKT_JSON, {"type": "cancel_all_missions"}))
    return jsonify({"status": "sent"})

@app.route('/api/mission/status')
def api_mission_status():
    with shared.lock:
        return jsonify({
            "ack": shared.mission_ack,
            "feedback": shared.mission_feedback,
            "result": shared.mission_result,
        })

def _fetch_vehicle_maps(active_ch, timeout: float = 10.0):
    """Ask the Jetson for its saved-map list (jetson_files/map/, see
    TcpClientNode._list_saved_maps) and wait for the reply. Shared by
    api_mission_list_maps and api_mission_upload_json, which needs to know
    whether an uploaded mission's referenced map already exists on the
    vehicle before deciding whether it has to transfer it first. Returns
    None on timeout (callers treat that the same as "not found" rather
    than a hard failure - see api_mission_upload_json)."""
    with shared.lock:
        shared.map_list = None
    active_ch.send(pack_json(PKT_JSON, {"type": "list_maps"}))
    deadline = time.time() + timeout
    while time.time() < deadline:
        with shared.lock:
            maps = shared.map_list
        if maps is not None:
            return maps
        time.sleep(0.1)
    return None

@app.route('/api/mission/list_maps', methods=['POST'])
def api_mission_list_maps():
    """"Start Fast Mission" flow: ask the Jetson which maps are already
    saved on the vehicle (uploaded once, e.g. over a fast Ethernet/manual
    connection - see api_mission_upload_map) so map_planner.html can offer
    them by name and start a mission straight from start_direct's
    map_yaml_path/map_pgm_path - no file transfer needed over a slow tether.
    Same request/wait pattern as api_list_files/api_list_topics."""
    active_ch = channels.active()
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503
    maps = _fetch_vehicle_maps(active_ch)
    if maps is None:
        return jsonify({"error": "Timeout waiting for map list from vehicle"}), 504
    return jsonify({"maps": maps})

def _sanitize_saved_map_name(name: str) -> str:
    safe = re.sub(r'[^A-Za-z0-9_.-]', '_', (name or '')).strip('_') or "map"
    # "." and ".." are the only strings the OS/pathlib treat as special
    # directory-traversal segments here (always a single path component,
    # since the regex above already strips "/") - reject them explicitly
    # rather than relying solely on _saved_map_dir's resolve+prefix check,
    # which _resolve_saved_map_slot's SAVED_MAPS_DIR/name construction
    # (used for the auto-suffix loop, before any pgm/yaml is ever written)
    # does not go through.
    if safe in (".", ".."):
        return "map"
    return safe

def _saved_map_dir(name: str) -> Path:
    safe = _sanitize_saved_map_name(name)
    path = (SAVED_MAPS_DIR / safe).resolve()
    if not str(path).startswith(str(SAVED_MAPS_DIR.resolve())):
        raise ValueError("invalid map name")
    return path

def _resolve_saved_map_slot(raw_name: str, pgm_bytes: bytes, yaml_bytes: bytes):
    """Resolve `raw_name` to a (final_name, folder) slot under
    SAVED_MAPS_DIR - the single GCS-local map registry (also used by the
    Offline Maps panel's explicit Save/Load/Delete below).

    Reuses an existing same-named slot IN PLACE when its map.pgm/map.yaml
    are already byte-identical to what's being saved now, so re-uploading
    an unchanged map - e.g. Start Mission re-uploads the current map on
    every click - never spawns a new -2/-3/... copy. Auto-suffixes only on
    a genuine content collision (a different map saved under a name
    that's already taken) - per explicit product decision, a name
    collision must never fail the save/upload outright, and the exact name
    typed/exported isn't guaranteed to be the one actually used.
    """
    base = _sanitize_saved_map_name(raw_name)
    name = base
    n = 2
    while True:
        folder = SAVED_MAPS_DIR / name
        if not folder.exists():
            return name, folder
        pgm_path, yaml_path = folder / "map.pgm", folder / "map.yaml"
        if pgm_path.exists() and yaml_path.exists():
            try:
                if pgm_path.read_bytes() == pgm_bytes and yaml_path.read_bytes() == yaml_bytes:
                    return name, folder
            except OSError:
                pass
        name = f"{base}-{n}"
        n += 1

_YAML_IMAGE_LINE_RE = re.compile(rb'(?m)^\s*image\s*:.*$')

def _rewrite_yaml_image(yaml_bytes: bytes, image_filename: str) -> bytes:
    """Rewrites a nav2 map.yaml's `image:` key to `image_filename`,
    leaving every other line untouched.

    THIS is the field nav2_map_server's LoadMap service actually resolves
    the occupancy-grid pgm from (relative to the yaml's own directory) -
    NOT `map_pgm_path` from the start_mission command, which mission.xml's
    LoadHybridMap node has no port for at all and is effectively dead data
    on the wire. map_planner.html's exported yamlText() writes `image:
    <mImg-field>.pgm` - a SEPARATE input from whatever name a map actually
    gets saved/uploaded under - so any map saved without manually retyping
    that field to match keeps a stale/default `image: map.pgm` baked in,
    and every such map then resolves to the SAME literal map.pgm file on
    the vehicle regardless of which differently-named map was requested.
    This is applied at forward-to-vehicle time (see
    _upload_saved_map_to_jetson below) so it self-heals every map that
    reaches the Jetson from here on, including ones saved before this
    existed - no data migration needed.

    Hand-rolled line rewrite rather than a YAML library, since nav2 map
    yamls are simple line-based key:value text - matches this codebase's
    existing avoid-new-dependency pattern for YAML-ish parsing elsewhere
    (see _parse_param_dump in jetson_tcp_node_d_m.py).
    """
    new_line = f"image: {image_filename}".encode()
    if _YAML_IMAGE_LINE_RE.search(yaml_bytes):
        # Callable replacement (not a bytes pattern) so a literal backslash
        # in new_line (never actually possible given _sanitize_saved_map_name's
        # charset, but cheap to be correct) can't be misread as a \1-style
        # backreference by re.sub.
        return _YAML_IMAGE_LINE_RE.sub(lambda m: new_line, yaml_bytes, count=1)
    # No image: key found (unexpected/malformed yaml) - append one rather
    # than silently forwarding a yaml nav2_map_server can't resolve at all.
    sep = b"\n" if yaml_bytes and not yaml_bytes.endswith(b"\n") else b""
    return yaml_bytes + sep + new_line + b"\n"

def _upload_saved_map_to_jetson(active_ch, pgm_path: Path, yaml_path: Path, final_name: str) -> dict:
    """Forwards a saved map's pgm/yaml to the Jetson under `final_name`,
    fixing up the forwarded yaml's `image:` key to `{final_name}.pgm` on
    the way out (see _rewrite_yaml_image) - the GCS's own local copy
    (yaml_path, still saying `image: map.pgm` to match its own folder-
    per-map/map.pgm convention) is left untouched on disk, only the bytes
    actually sent over the wire are rewritten. Shared by upload_map/
    send_local_map/upload_json so this fix applies uniformly everywhere a
    map reaches the vehicle. Returns the same {filename: "ok"|error} shape
    all three routes already build their `results`/`failed` response
    fields from.
    """
    jetson_yaml_bytes = _rewrite_yaml_image(yaml_path.read_bytes(), f"{final_name}.pgm")
    tmp_fd, tmp_name = tempfile.mkstemp(suffix=".yaml")
    try:
        with os.fdopen(tmp_fd, "wb") as tf:
            tf.write(jetson_yaml_bytes)
        ok_pgm, err_pgm = _upload_file_to_jetson(active_ch, pgm_path, f"{final_name}.pgm", category="map")
        ok_yaml, err_yaml = _upload_file_to_jetson(active_ch, Path(tmp_name), f"{final_name}.yaml", category="map")
    finally:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
    return {
        f"{final_name}.pgm": "ok" if ok_pgm else (err_pgm or "failed"),
        f"{final_name}.yaml": "ok" if ok_yaml else (err_yaml or "failed"),
    }

@app.route('/api/mission/upload_map', methods=['POST'])
def api_mission_upload_map():
    """Receives the Map Planner's exported PGM/YAML (+ optionally the same
    source_png/mission_json/overrides_png bundle /api/saved_maps accepts),
    resolves a unique name against SAVED_MAPS_DIR (see
    _resolve_saved_map_slot), saves there, THEN forwards the pgm/yaml to
    the Jetson under category "map" using that SAME final name - this is
    what keeps the GCS's and vehicle's copies identically named, per the
    "same unique name on both ends, no errors" requirement. See
    mission_planner_assets/map_planner.html's uploadToVehicle()/
    btnStartMission - both send the confirmed `name` back through to
    start_direct's map_yaml_path/map_pgm_path rather than assuming their
    own pre-upload guess still applies.

    Map transfer is NOT hard-restricted to an Ethernet connection - per
    explicit product decision, any active channel is allowed, tether
    included - but the response carries a `warning` when the channel used
    isn't Ethernet-mode (see _channel_is_ethernet), since a multi-hundred-
    KB map over the ~10kbps tether is slow and more failure-prone than the
    same transfer over a dedicated Manual Connection "Ethernet" link.
    """
    active_ch = channels.active()
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503
    if 'pgm' not in request.files or 'yaml' not in request.files:
        return jsonify({"error": "Both pgm and yaml files are required"}), 400
    pgm_bytes = request.files['pgm'].read()
    yaml_bytes = request.files['yaml'].read()
    raw_name = Path(request.files['pgm'].filename or request.files['yaml'].filename or "map").stem

    final_name, folder = _resolve_saved_map_slot(raw_name, pgm_bytes, yaml_bytes)
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "map.pgm").write_bytes(pgm_bytes)
    (folder / "map.yaml").write_bytes(yaml_bytes)
    # Optional richer bundle (only sent by btnUploadVehicle/btnStartMission,
    # not by a bare curl upload) - same fields /api/saved_maps accepts, so
    # a plain "Upload to Vehicle" click also leaves a fully reloadable
    # Offline Maps entry, not just a bare pgm/yaml pair.
    for field, fname in (('mission_json', 'mission.json'), ('source_png', 'source.png'), ('overrides_png', 'overrides.png')):
        f = request.files.get(field)
        if f and f.filename:
            f.save(folder / fname)

    results = _upload_saved_map_to_jetson(active_ch, folder / "map.pgm", folder / "map.yaml", final_name)

    # Previously this always returned {"status":"done"} even when every
    # file failed, so the UI showed a success toast regardless - surface
    # real failures instead, since a silently-missing map file only shows
    # up later as a confusing LoadHybridMap failure during Start Mission.
    failed = {k: v for k, v in results.items() if v != "ok"}
    warning = None if _channel_is_ethernet(active_ch) else (
        f"Sent over '{active_ch.name}' ({_channel_link_kind(active_ch)}), not an Ethernet "
        f"connection - for large/frequent map transfers, use a Manual Connection "
        f"'Ethernet' link for speed and reliability."
    )
    payload = {"results": results, "name": final_name, "renamed": final_name != _sanitize_saved_map_name(raw_name)}
    if failed:
        payload["error"] = f"file(s) did not reach the vehicle: {failed}"
        return jsonify(payload), 504
    payload["status"] = "done"
    payload["warning"] = warning
    return jsonify(payload)

@app.route('/api/mission/send_local_map', methods=['POST'])
def api_mission_send_local_map():
    """Re-push an already-GCS-saved map (SAVED_MAPS_DIR, see Offline Maps
    panel) to the vehicle without regenerating anything - e.g. after a
    Jetson reboot/reflash lost its copy, or to seed a second vehicle from
    the same map. Implements "reused for different mission uploads on the
    same map files" for a map that isn't the one currently loaded in the
    Map Planner canvas. Same ethernet-warning policy as upload_map; the
    name is used exactly as already saved (no re-suffixing - that only
    happens once, at first save)."""
    active_ch = channels.active()
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503
    data = request.get_json(force=True, silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "Missing map name"}), 400
    try:
        folder = _saved_map_dir(name)
    except ValueError:
        return jsonify({"error": "Invalid name"}), 400
    pgm_path, yaml_path = folder / "map.pgm", folder / "map.yaml"
    if not pgm_path.exists() or not yaml_path.exists():
        return jsonify({"error": f"No saved map named '{name}' (missing map.pgm/map.yaml)"}), 404

    results = _upload_saved_map_to_jetson(active_ch, pgm_path, yaml_path, folder.name)
    failed = {k: v for k, v in results.items() if v != "ok"}
    warning = None if _channel_is_ethernet(active_ch) else (
        f"Sent over '{active_ch.name}' ({_channel_link_kind(active_ch)}), not an Ethernet connection."
    )
    if failed:
        return jsonify({"error": f"file(s) did not reach the vehicle: {failed}", "results": results}), 504
    return jsonify({"status": "done", "results": results, "warning": warning})

def _waypoints_latlon_to_map_frame(waypoints, anchor_lat: float, anchor_lon: float) -> list:
    """Python mirror of map_planner.html's waypointsToMapFrame() - same
    anchor-relative equirectangular approximation (dLat*111320,
    dLon*111320*cos(lat)), deliberately duplicated rather than shared (the
    two files are independently deployed, same pattern already used for
    CHANNELS_CONFIG elsewhere in this file). Needed here so a mission JSON
    posted directly (e.g. via curl, bypassing the browser) still gets
    correctly-anchored map-frame waypoints - see api_mission_upload_json.
    Accepts each waypoint as {"lat":.., "lon":..} or [lat, lon]."""
    out = []
    for w in (waypoints or []):
        if isinstance(w, dict):
            lat, lon = w.get("lat"), w.get("lon")
        elif isinstance(w, (list, tuple)) and len(w) == 2:
            lat, lon = w[0], w[1]
        else:
            continue
        if lat is None or lon is None:
            continue
        try:
            lat, lon = float(lat), float(lon)
        except (TypeError, ValueError):
            continue
        d_lat, d_lon = lat - anchor_lat, lon - anchor_lon
        north = d_lat * 111320.0
        east = d_lon * 111320.0 * math.cos(math.radians(anchor_lat))
        out.append([round(east, 2), round(north, 2)])
    return out

def _build_start_mission_msg(start_lat, start_lon, goal_lat, goal_lon, start_north,
                              start_delay_sec, waypoint_list, map_yaml_path, map_pgm_path) -> dict:
    """Shared `start_mission` JSON builder - used by both api_mission_start_direct
    (map-frame-meters waypoint_list, already converted client-side) and
    api_mission_upload_json (lat/lon waypoints, converted server-side via
    _waypoints_latlon_to_map_frame) so the two entry points can't drift
    apart on the wire format the Jetson actually expects."""
    return {
        "type": "start_mission",
        "start_delay_sec": max(0.0, float(start_delay_sec or 0.0)),
        "start_lat": float(start_lat or 0.0),
        "start_lon": float(start_lon or 0.0),
        # radians, ENU convention (0=East, CCW) - map_planner.html converts
        # the operator's compass-degrees heading before sending; this rides
        # straight through to SetPositionOnMap's "yaw" port via mission.xml.
        "start_north": float(start_north or 0.0),
        "goal_lat": float(goal_lat or 0.0),
        "goal_lon": float(goal_lon or 0.0),
        "map_yaml_path": map_yaml_path or "",
        "map_pgm_path": map_pgm_path or "",
        "waypoint_list": waypoint_list,
        "tree_path": "",
        "pre_blackboard_config": "",
        "restart_on_failure": False,
    }

@app.route('/api/mission/start_direct', methods=['POST'])
def api_mission_start_direct():
    """Starts a mission straight from the Map Planner's current start/goal/
    map fields - no saved-mission id, nothing persisted GCS-side. Call
    /api/mission/upload_map first so map_yaml_path/map_pgm_path actually
    exist on the vehicle by the time mission_handler tries to load them."""
    active_ch = channels.active()
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503
    data = request.get_json(force=True, silent=True) or {}
    # waypoint_list arrives as [[x,y],...] map-frame meters (map_planner.html's
    # waypointsToMapFrame() - SetWaypoint.cpp uses each Point directly as a
    # map-frame pose, no further conversion happens mission-side). Sanitize
    # since this rides straight into a geometry_msgs/Point[] on the Jetson.
    waypoint_list = []
    for pt in (data.get("waypoint_list") or []):
        if isinstance(pt, (list, tuple)) and len(pt) == 2:
            try:
                waypoint_list.append([float(pt[0]), float(pt[1])])
            except (TypeError, ValueError):
                continue
    msg = _build_start_mission_msg(
        data.get("start_lat"), data.get("start_lon"), data.get("goal_lat"), data.get("goal_lon"),
        data.get("start_north"), data.get("start_delay_sec"), waypoint_list,
        data.get("map_yaml_path"), data.get("map_pgm_path"),
    )
    active_ch.send(pack_json(PKT_JSON, msg))
    return jsonify({"status": "sent"})

@app.route('/api/mission/upload_json', methods=['POST'])
def api_mission_upload_json():
    """"Upload only the mission JSON, reuse a saved map" - the counterpart
    to upload_map's "upload only the map, no mission." Deliberately
    allowed over ANY connected channel, tether or CH-test included (per
    explicit request: this needs to be debuggable/testable over the local
    test connection, unlike bulk map transfer which is deliberately
    steered toward Ethernet - see the warning below). Accepts a multipart
    'file' (map_planner.html's Upload Mission (JSON) button, or
    `curl -F file=@mission.json ...` for manual testing) or a raw JSON
    body (`curl -d @mission.json -H 'Content-Type: application/json' ...`).

    Resolves map_name against, in order: (1) the vehicle's own saved-map
    list (fast path - zero transfer if it's already there), (2) this
    GCS's SAVED_MAPS_DIR (uploads map.pgm/map.yaml from there first, same
    ethernet-warning policy as upload_map/send_local_map). Errors if
    neither has it - that map needs generating/uploading at least once
    first, from Map Planner.

    Expected JSON shape (map_planner.html's missionOnlyJSON()):
    {"map_name":str, "start_lat":num, "start_lon":num, "goal_lat":num,
     "goal_lon":num, "heading_deg":num?, "start_delay_sec":num?,
     "waypoints":[{"lat":num,"lon":num}, ...]?}
    Also accepts the richer Nav2-package mission.json shape (map.image,
    start.lat/lon, goal.lat/lon) as a fallback for convenience.
    """
    active_ch = channels.active()
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503

    if 'file' in request.files:
        try:
            data = json.loads(request.files['file'].read().decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            return jsonify({"error": f"Invalid mission JSON: {e}"}), 400
    else:
        data = request.get_json(force=True, silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Expected a JSON object (mission file)"}), 400

    map_name_raw = data.get("map_name") or (data.get("map") or {}).get("image", "")
    map_name = _sanitize_saved_map_name(Path(map_name_raw).stem) if map_name_raw else ""
    if not map_name:
        return jsonify({"error": "Missing map_name (or map.image)"}), 400

    start = data.get("start") if isinstance(data.get("start"), dict) else {}
    goal = data.get("goal") if isinstance(data.get("goal"), dict) else {}
    try:
        start_lat = float(data.get("start_lat", start.get("lat")))
        start_lon = float(data.get("start_lon", start.get("lon")))
        goal_lat = float(data.get("goal_lat", goal.get("lat")))
        goal_lon = float(data.get("goal_lon", goal.get("lon")))
    except (TypeError, ValueError):
        return jsonify({"error": "start_lat/start_lon/goal_lat/goal_lon are required numbers"}), 400

    heading_deg = float(data.get("heading_deg") or 0.0)
    start_north = (90.0 - heading_deg) * math.pi / 180.0
    start_delay_sec = max(0.0, float(data.get("start_delay_sec") or 0.0))
    waypoint_list = _waypoints_latlon_to_map_frame(data.get("waypoints"), start_lat, start_lon)

    warning = None
    vehicle_maps = _fetch_vehicle_maps(active_ch)
    if vehicle_maps is not None and any(m.get("name") == map_name for m in vehicle_maps):
        map_source = "vehicle"
    else:
        try:
            folder = _saved_map_dir(map_name)
        except ValueError:
            return jsonify({"error": "Invalid map_name"}), 400
        pgm_path, yaml_path = folder / "map.pgm", folder / "map.yaml"
        if not pgm_path.exists() or not yaml_path.exists():
            return jsonify({"error": f"Map '{map_name}' not found on the vehicle or this GCS - "
                                      f"upload/save it via Map Planner first."}), 404
        results = _upload_saved_map_to_jetson(active_ch, pgm_path, yaml_path, map_name)
        failed = {k: v for k, v in results.items() if v != "ok"}
        if failed:
            return jsonify({"error": f"map file(s) did not reach the vehicle: {failed}"}), 504
        map_source = "gcs_upload"
        if not _channel_is_ethernet(active_ch):
            warning = (f"Map '{map_name}' was sent over '{active_ch.name}' "
                       f"({_channel_link_kind(active_ch)}), not an Ethernet connection.")

    msg = _build_start_mission_msg(
        start_lat, start_lon, goal_lat, goal_lon, start_north, start_delay_sec,
        waypoint_list, f"map/{map_name}.yaml", f"map/{map_name}.pgm",
    )
    active_ch.send(pack_json(PKT_JSON, msg))
    return jsonify({"status": "sent", "map_name": map_name, "map_source": map_source, "warning": warning})

@app.route('/mission-planner/map-planner')
def mission_planner_map():
    return send_from_directory(MISSION_PLANNER_ASSETS_DIR, "map_planner.html")

# ------------------------------------------------------------------
# Offline saved maps API
# ------------------------------------------------------------------

@app.route('/api/saved_maps', methods=['GET'])
def api_saved_maps_list():
    maps = []
    for d in sorted(SAVED_MAPS_DIR.iterdir()) if SAVED_MAPS_DIR.exists() else []:
        if d.is_dir() and (d / "map.yaml").exists():
            try:
                saved_at = (d / "map.yaml").stat().st_mtime
            except OSError:
                saved_at = None
            maps.append({"name": d.name, "saved_at": saved_at})
    return jsonify({"maps": maps})

@app.route('/api/saved_maps', methods=['POST'])
def api_saved_maps_save():
    name = (request.form.get('name') or '').strip()
    if not name:
        return jsonify({"error": "Name required"}), 400
    pgm = request.files.get('pgm')
    yaml_file = request.files.get('yaml')
    mission_json_file = request.files.get('mission_json')
    source_png_file = request.files.get('source_png')
    overrides_png_file = request.files.get('overrides_png')
    if not pgm or not yaml_file or not source_png_file:
        return jsonify({"error": "pgm, yaml and source_png files required"}), 400
    pgm_bytes = pgm.read()
    yaml_bytes = yaml_file.read()
    # Auto-suffixes (name-2, name-3, ...) instead of silently overwriting a
    # DIFFERENT map already saved under this name - see _resolve_saved_map_slot.
    # Reuses the same slot in place (no suffix) when the pgm/yaml content is
    # already identical, so re-saving the same map under the same name just
    # freshens its mission_json/source_png/overrides rather than forking.
    final_name, folder = _resolve_saved_map_slot(name, pgm_bytes, yaml_bytes)
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "map.pgm").write_bytes(pgm_bytes)
    (folder / "map.yaml").write_bytes(yaml_bytes)
    source_png_file.save(folder / "source.png")
    if mission_json_file:
        mission_json_file.save(folder / "mission.json")
    overrides_path = folder / "overrides.png"
    if overrides_png_file:
        overrides_png_file.save(overrides_path)
    elif overrides_path.exists():
        overrides_path.unlink()
    return jsonify({"status": "saved", "name": folder.name, "renamed": final_name != _sanitize_saved_map_name(name)})

@app.route('/api/saved_maps/<name>', methods=['GET'])
def api_saved_maps_load(name):
    try:
        folder = _saved_map_dir(name)
    except ValueError:
        return jsonify({"error": "Invalid name"}), 400
    if not folder.is_dir():
        return jsonify({"error": "Not found"}), 404
    pgm_path = folder / "map.pgm"
    yaml_path = folder / "map.yaml"
    mission_path = folder / "mission.json"
    source_path = folder / "source.png"
    overrides_path = folder / "overrides.png"
    if not pgm_path.exists() or not yaml_path.exists() or not source_path.exists():
        return jsonify({"error": "Saved map is incomplete"}), 500
    mission_json = None
    if mission_path.exists():
        try:
            mission_json = json.loads(mission_path.read_text())
        except (OSError, json.JSONDecodeError):
            mission_json = None
    return jsonify({
        "name": folder.name,
        "pgm_base64": base64.b64encode(pgm_path.read_bytes()).decode('ascii'),
        "yaml_text": yaml_path.read_text(),
        "mission_json": mission_json,
        "source_png_base64": base64.b64encode(source_path.read_bytes()).decode('ascii'),
        "overrides_png_base64": base64.b64encode(overrides_path.read_bytes()).decode('ascii') if overrides_path.exists() else None,
    })

@app.route('/api/saved_maps/<name>', methods=['DELETE'])
def api_saved_maps_delete(name):
    try:
        folder = _saved_map_dir(name)
    except ValueError:
        return jsonify({"error": "Invalid name"}), 400
    if folder.is_dir():
        shutil.rmtree(folder)
    return jsonify({"status": "deleted"})

def _reset_download_state():
    """Reset all global download state under the lock. Call from any thread."""
    global download_complete, download_state, download_file_path, download_error
    with _dl_lock:
        download_complete  = False
        download_state     = None
        download_file_path = None
        download_error     = None

@app.route('/api/download', methods=['GET'])
def api_download():
    """
    Robust file download from Jetson.

    1. Reset state, request the file from Jetson via PKT_FILE_REQ.
    2. process_incoming() assembles chunks into a temp file.
    3. Once PKT_FILE_COMPLETE arrives we stream the complete temp file to the
       browser with a Content-Length header (real progress bar, never partial).
    4. Cleanup runs in a background thread AFTER the response is fully sent so
       Flask's streaming generator can finish without the file disappearing.
    """
    global download_complete, download_state, download_file_path, download_error

    active_ch = channels.active()
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503

    filename = request.args.get('filename', '')
    if not filename:
        return jsonify({"error": "Missing filename"}), 400

    # Reset before requesting so we don't pick up a stale previous transfer.
    _reset_download_state()

    active_ch.send(pack_json(PKT_FILE_REQ, {
        "filename": filename,
        "direction": "download",
    }))

    # Block until transfer complete, error, or timeout.
    start = time.time()
    while True:
        with _dl_lock:
            done  = download_complete
            err   = download_error
        if done or err:
            break
        if time.time() - start > DOWNLOAD_TIMEOUT:
            _reset_download_state()
            return jsonify({"error": "Download timeout – file may be large or tether is slow"}), 504
        time.sleep(0.1)

    if err:
        _reset_download_state()
        return jsonify({"error": err}), 500

    # Snapshot the file path and temp dir NOW, before reset.
    with _dl_lock:
        fpath    = download_file_path
        ds       = download_state
        temp_dir = ds.get("temp_dir") if ds else None

    if not fpath or not fpath.exists():
        _reset_download_state()
        return jsonify({"error": "File not found after transfer"}), 404

    file_size = fpath.stat().st_size
    safe_name = Path(filename).name

    # Reset global state now – the temp file is still on disk, we have fpath.
    # Another download can start immediately after this point.
    _reset_download_state()

    def stream_file():
        try:
            with open(fpath, "rb") as f:
                while True:
                    chunk = f.read(65536)
                    if not chunk:
                        break
                    yield chunk
        finally:
            # Cleanup in a daemon thread so the generator isn't blocked.
            def _cleanup():
                try:
                    if temp_dir and os.path.exists(temp_dir):
                        shutil.rmtree(temp_dir, ignore_errors=True)
                        log.info(f"Cleaned up temp dir: {temp_dir}")
                except Exception as e:
                    log.error(f"Cleanup error: {e}")
            threading.Thread(target=_cleanup, daemon=True).start()

    return Response(
        stream_with_context(stream_file()),
        headers={
            "Content-Disposition": f'attachment; filename="{safe_name}"',
            "Content-Type": "application/octet-stream",
            "Content-Length": str(file_size),
        },
    )

# Folders take longer than a single file - zipping happens on the Jetson
# BEFORE any bytes start flowing back, so a large folder can eat well into
# the wait budget before the transfer even begins. Separate, more generous
# constant rather than changing DOWNLOAD_TIMEOUT (which single-file
# downloads already rely on).
DOWNLOAD_FOLDER_TIMEOUT = 300  # seconds

@app.route('/api/download_folder', methods=['GET'])
def api_download_folder():
    """
    Download a whole folder as a zip. The Jetson zips it server-side (see
    _handle_download_folder there) and sends the result back over the
    EXACT SAME PKT_FILE_REQ/CHUNK/COMPLETE protocol a single-file download
    uses, so this shares the same download_state/_dl_lock tracking as
    api_download above (same "one transfer in flight at a time" model
    that already applied to single-file downloads).

    Deliberately an independent route rather than refactoring api_download
    to share code - duplicates its wait/stream shape on purpose so nothing
    about this newer, less-tested path can risk the proven single-file
    download route above.
    """
    global download_complete, download_state, download_file_path, download_error

    active_ch = channels.active()
    if active_ch is None:
        return jsonify({"error": "Not connected"}), 503

    path = request.args.get('path', '')
    if not path:
        return jsonify({"error": "Missing path"}), 400

    _reset_download_state()

    active_ch.send(pack_json(PKT_JSON, {"type": "download_folder", "path": path}))

    start = time.time()
    while True:
        with _dl_lock:
            done = download_complete
            err  = download_error
        if done or err:
            break
        if time.time() - start > DOWNLOAD_FOLDER_TIMEOUT:
            _reset_download_state()
            return jsonify({"error": "Download timeout - folder may be large or tether is slow"}), 504
        time.sleep(0.1)

    if err:
        _reset_download_state()
        return jsonify({"error": err}), 500

    with _dl_lock:
        fpath    = download_file_path
        ds       = download_state
        temp_dir = ds.get("temp_dir") if ds else None
        zip_name = ds.get("filename") if ds else None

    if not fpath or not fpath.exists():
        _reset_download_state()
        return jsonify({"error": "File not found after transfer"}), 404

    file_size = fpath.stat().st_size
    safe_name = zip_name or (Path(path).name + ".zip")

    _reset_download_state()

    def stream_zip():
        try:
            with open(fpath, "rb") as f:
                while True:
                    chunk = f.read(65536)
                    if not chunk:
                        break
                    yield chunk
        finally:
            def _cleanup():
                try:
                    if temp_dir and os.path.exists(temp_dir):
                        shutil.rmtree(temp_dir, ignore_errors=True)
                        log.info(f"Cleaned up temp dir: {temp_dir}")
                except Exception as e:
                    log.error(f"Cleanup error: {e}")
            threading.Thread(target=_cleanup, daemon=True).start()

    return Response(
        stream_with_context(stream_zip()),
        headers={
            "Content-Disposition": f'attachment; filename="{safe_name}"',
            "Content-Type": "application/zip",
            "Content-Length": str(file_size),
        },
    )

# ----------------------------------------------------------------------
# HTML Template
# ----------------------------------------------------------------------
HTML_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
<meta http-equiv="Pragma" content="no-cache">
<meta http-equiv="Expires" content="0">
<title>AUV Ground Control Station // TITAN-II</title>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='6' fill='%230A0F1D' stroke='%232563EB' stroke-width='2'/%3E%3Ccircle cx='16' cy='16' r='9' fill='none' stroke='%2338BDF8' stroke-width='1.8'/%3E%3Cpath d='M16 4v6m0 12v6M4 16h6m12 0h6' stroke='%2338BDF8' stroke-width='1.8' stroke-linecap='round'/%3E%3Ccircle cx='16' cy='16' r='2.5' fill='%2338BDF8'/%3E%3C/svg%3E">
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@500;600;700&display=swap');

/* ---- Design Tokens: Professional Aerospace & Marine Robotics GCS ---- */
:root,
[data-theme="dark"] {
  --bg:           #070B14;   /* Deep maritime charcoal-navy canvas */
  --bg2:          #0B1120;   /* Recessed instrument well */
  --bg3:          #10192E;   /* Elevated element well */
  --panel:        #0E1526;   /* Matte instrument console surface */
  --panel2:       #141E34;   /* Interactive / elevated surface */
  --border:       rgba(255, 255, 255, 0.08); /* Subtle hairline border */
  --border2:      rgba(255, 255, 255, 0.15); /* Interactive border */
  --border3:      #2A3E62;   /* Focus / active border */
  --accent:       #2563EB;   /* Controlled aerospace cool blue */
  --accent2:      #38BDF8;   /* Subtle high-vis sky cyan */
  --accent-dim:   rgba(37, 99, 235, 0.12);
  --accent-glow:  rgba(37, 99, 235, 0.22);
  --danger:       #EF4444;   /* Warning / alert red */
  --danger-dim:   rgba(239, 68, 68, 0.12);
  --danger-glow:  rgba(239, 68, 68, 0.24);
  --estop:        #DC2626;   /* Authoritative emergency killswitch red */
  --estop-glow:   rgba(220, 38, 38, 0.35);
  --warn:         #F59E0B;   /* Caution amber */
  --warn-dim:     rgba(245, 158, 11, 0.12);
  --ok:           #10B981;   /* Restrained emerald green */
  --ok-dim:       rgba(16, 185, 129, 0.12);
  --muted:        #8496AB;   /* Crisp engineering slate */
  --dim:          #52647C;   /* Subdued secondary annotations */
  --text:         #F8FAFC;   /* Crisp off-white */
  --text2:        #CBD5E1;   /* High-contrast secondary text */
  --console-bg:   #050810;   /* Deep terminal background */
  --console-text: #34D399;   /* Phosphor readout text */
  --console-border: rgba(255, 255, 255, 0.08);
  --hover:        rgba(255, 255, 255, 0.04);
  --shadow:       0 2px 12px rgba(0, 0, 0, 0.32);
  --font-ui:      'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
  --font-mono:    'JetBrains Mono', 'Fira Code', 'Cascadia Mono', 'SF Mono', Consolas, monospace;
  --radius:       6px;
  --radius-sm:    4px;
}

/* True Black Mode: High-contrast obsidian stealth console */
[data-theme="black"] {
  --bg:           #020305;
  --bg2:          #06090F;
  --bg3:          #0B101B;
  --panel:        #080B11;
  --panel2:       #0E131F;
  --border:       rgba(255, 255, 255, 0.09);
  --border2:      rgba(255, 255, 255, 0.17);
  --border3:      #334155;
  --accent:       #2563EB;
  --accent2:      #38BDF8;
  --accent-dim:   rgba(37, 99, 235, 0.14);
  --accent-glow:  rgba(37, 99, 235, 0.24);
  --danger:       #EF4444;
  --danger-dim:   rgba(239, 68, 68, 0.14);
  --danger-glow:  rgba(239, 68, 68, 0.24);
  --estop:        #DC2626;
  --estop-glow:   rgba(220, 38, 38, 0.35);
  --warn:         #F59E0B;
  --warn-dim:     rgba(245, 158, 11, 0.14);
  --ok:           #10B981;
  --ok-dim:       rgba(16, 185, 129, 0.14);
  --muted:        #8B9BB0;
  --dim:          #56667A;
  --text:         #F8FAFC;
  --text2:        #D1D9E6;
  --console-bg:   #010204;
  --console-text: #34D399;
  --console-border: rgba(255, 255, 255, 0.10);
  --hover:        rgba(255, 255, 255, 0.05);
  --shadow:       0 2px 14px rgba(0, 0, 0, 0.65);
}

/* Light Mode: Daylight high-contrast engineering slate */
[data-theme="light"] {
  --bg:           #EDF2F7;
  --bg2:          #F1F5F9;
  --bg3:          #E2E8F0;
  --panel:        #FFFFFF;
  --panel2:       #F8FAFC;
  --border:       #D5DFEB;
  --border2:      #B8C7D9;
  --border3:      #64748B;
  --accent:       #1D4ED8;
  --accent2:      #0284C7;
  --accent-dim:   rgba(29, 78, 216, 0.09);
  --accent-glow:  rgba(29, 78, 216, 0.16);
  --danger:       #DC2626;
  --danger-dim:   rgba(220, 38, 38, 0.09);
  --danger-glow:  rgba(220, 38, 38, 0.18);
  --estop:        #DC2626;
  --estop-glow:   rgba(220, 38, 38, 0.22);
  --warn:         #D97706;
  --warn-dim:     rgba(217, 119, 6, 0.10);
  --ok:           #059669;
  --ok-dim:       rgba(5, 150, 105, 0.10);
  --muted:        #52637A;
  --dim:          #7C8EA6;
  --text:         #0F172A;
  --text2:        #1E293B;
  --console-bg:   #F4F7FA;   /* Crisp recessed technical bay in light mode */
  --console-text: #0F172A;   /* High-contrast telemetry/console text in light mode */
  --console-border: #CBD5E1;
  --hover:        rgba(15, 23, 42, 0.04);
  --shadow:       0 1px 3px rgba(15, 23, 42, 0.06), 0 4px 12px rgba(15, 23, 42, 0.04);
}

/* ---- Reset / Base --------------------------------------------- */
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html { font-size: 13px; -webkit-font-smoothing: antialiased; }
body {
  font-family: var(--font-ui);
  background: var(--bg);
  color: var(--text);
  min-height: 100vh;
  padding: 10px 14px 32px;
  line-height: 1.45;
}
::selection { background: var(--accent-dim); color: var(--accent2); }
button:focus-visible,
input:focus-visible,
select:focus-visible {
  outline: 2px solid var(--accent);
  outline-offset: 1px;
}

/* ---- Header: Mission Command Bridge --------------------------- */
.gcs-header {
  position: relative;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
  flex-wrap: wrap;
  padding: 10px 16px;
  background: linear-gradient(180deg, var(--panel2) 0%, var(--panel) 100%);
  border: 1px solid var(--border);
  border-radius: 8px;
  margin-bottom: 10px;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.04),
    0 1px 2px rgba(0, 0, 0, 0.25),
    0 8px 24px rgba(0, 0, 0, 0.28);
}
[data-theme="light"] .gcs-header { box-shadow: var(--shadow); }
/* Fine accent hairline along the top edge - quiet mission-console detail */
.gcs-header::before {
  content: "";
  position: absolute;
  top: -1px;
  left: 18px;
  right: 18px;
  height: 1px;
  background: linear-gradient(90deg,
    transparent,
    rgba(56, 189, 248, 0.30) 25%,
    rgba(37, 99, 235, 0.55) 50%,
    rgba(56, 189, 248, 0.30) 75%,
    transparent);
  pointer-events: none;
}
[data-theme="light"] .gcs-header::before {
  background: linear-gradient(90deg,
    transparent,
    rgba(29, 78, 216, 0.14) 25%,
    rgba(29, 78, 216, 0.30) 50%,
    rgba(29, 78, 216, 0.14) 75%,
    transparent);
}
.header-brand {
  display: flex;
  align-items: center;
  gap: 11px;
  min-width: 0;
}
/* Hairline divider between the brand block and the navigation */
.header-brand::after {
  content: "";
  width: 1px;
  height: 24px;
  margin-left: 5px;
  background: linear-gradient(180deg, transparent, var(--border2), transparent);
  flex-shrink: 0;
}
.brand-badge {
  width: 32px;
  height: 32px;
  border-radius: 8px;
  background: linear-gradient(160deg, rgba(37, 99, 235, 0.30), rgba(37, 99, 235, 0.08));
  border: 1px solid rgba(56, 189, 248, 0.28);
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--accent2);
  flex-shrink: 0;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.07),
    0 0 14px rgba(37, 99, 235, 0.20);
}
[data-theme="light"] .brand-badge {
  background: linear-gradient(160deg, rgba(29, 78, 216, 0.14), rgba(29, 78, 216, 0.04));
  border-color: rgba(29, 78, 216, 0.25);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.6);
}
.brand-badge svg { width: 17px; height: 17px; stroke-width: 1.75; }
.brand-text { display: flex; flex-direction: column; gap: 2px; }
.brand-title {
  font-family: var(--font-ui);
  font-size: 0.94rem;
  font-weight: 700;
  letter-spacing: 0.05em;
  color: var(--text);
  line-height: 1.15;
}
.brand-subtitle {
  font-family: var(--font-mono);
  font-size: 0.56rem;
  font-weight: 600;
  letter-spacing: 0.16em;
  color: var(--dim);
  text-transform: uppercase;
}

/* Header Integrated Navigation Tabs */
.header-nav {
  display: flex;
  align-items: center;
  gap: 3px;
  background: rgba(0, 0, 0, 0.22);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 3px;
  box-shadow: inset 0 1px 3px rgba(0, 0, 0, 0.30);
}
[data-theme="light"] .header-nav {
  background: rgba(15, 23, 42, 0.04);
  box-shadow: inset 0 1px 2px rgba(15, 23, 42, 0.06);
}
.header-nav .top-tab,
.top-tabs .top-tab {
  padding: 6px 14px;
  border: 1px solid transparent;
  border-radius: 6px;
  background: none;
  color: var(--muted);
  font-family: var(--font-ui);
  font-size: 0.70rem;
  font-weight: 600;
  letter-spacing: 0.06em;
  cursor: pointer;
  transition: color .15s ease, background .15s ease, border-color .15s ease, box-shadow .15s ease;
  display: flex;
  align-items: center;
  gap: 7px;
  white-space: nowrap;
}
.header-nav .top-tab:hover,
.top-tabs .top-tab:hover {
  color: var(--text);
  background: var(--hover);
}
.header-nav .top-tab.active,
.top-tabs .top-tab.active {
  color: #FFFFFF;
  background: linear-gradient(180deg, #2F6BF2 0%, #1E56D6 100%);
  border-color: rgba(56, 189, 248, 0.45);
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.14),
    0 1px 4px rgba(37, 99, 235, 0.45),
    0 0 12px rgba(37, 99, 235, 0.22);
}
.header-nav .top-tab.active:hover,
.top-tabs .top-tab.active:hover {
  background: linear-gradient(180deg, #3773F5 0%, #2259DB 100%);
  color: #FFFFFF;
}
.top-tab .tab-indicator {
  width: 5px;
  height: 5px;
  border-radius: 50%;
  background: currentColor;
  opacity: 0.85;
  flex-shrink: 0;
}
.header-nav .top-tab.active .tab-indicator,
.top-tabs .top-tab.active .tab-indicator {
  background: #BBE9FF;
  box-shadow: 0 0 6px rgba(187, 233, 255, 0.9);
  animation: tabLivePulse 2.2s ease-in-out infinite;
}
@keyframes tabLivePulse { 0%,100%{opacity:1} 50%{opacity:.4} }
.top-tab-content { display: none; }
.top-tab-content.active { display: block; }

/* Status Annunciator Chips */
.header-pills {
  display: flex;
  gap: 6px;
  align-items: center;
  flex-wrap: wrap;
}
/* Hairline divider between the navigation and the status cluster */
.header-pills::before {
  content: "";
  width: 1px;
  height: 24px;
  margin-right: 5px;
  background: linear-gradient(180deg, transparent, var(--border2), transparent);
  flex-shrink: 0;
}
.pill {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 5px 11px;
  border-radius: 6px;
  font-size: 0.66rem;
  font-family: var(--font-mono);
  font-weight: 600;
  letter-spacing: 0.07em;
  text-transform: uppercase;
  font-variant-numeric: tabular-nums;
  border: 1px solid var(--border);
  background: rgba(0, 0, 0, 0.20);
  color: var(--muted);
  white-space: nowrap;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.03);
  transition: all .15s ease;
}
[data-theme="light"] .pill {
  background: rgba(15, 23, 42, 0.03);
  box-shadow: none;
}
.pill .dot {
  width: 6px; height: 6px; border-radius: 50%;
  background: currentColor;
  flex-shrink: 0;
  box-shadow: 0 0 6px currentColor;
  opacity: 0.9;
}
/* Mode chip stays lowercase like the reference mock ("manual", not "MANUAL") */
#modePill { text-transform: none; letter-spacing: 0.05em; }
.pill.ok      { color: var(--ok);      border-color: rgba(16, 185, 129, 0.32);  background: var(--ok-dim); }
.pill.danger  { color: var(--danger);  border-color: rgba(239, 68, 68, 0.32);  background: var(--danger-dim); }
.pill.warn    { color: var(--warn);    border-color: rgba(245, 158, 11, 0.32); background: var(--warn-dim); }
.pill.accent  { color: var(--accent2); border-color: rgba(56, 189, 248, 0.32); background: rgba(56, 189, 248, 0.08); }
.pill .blink  { animation: gcsBlink 1s infinite; }
@keyframes gcsBlink { 0%,100%{opacity:1} 50%{opacity:.25} }

/* Header theme toggle - scoped + !important to win over the later global
   flat-button block that forces every .btn to the same plain treatment */
.header-pills .btn {
  min-height: 26px !important;
  padding: 4px 11px !important;
  border-radius: 6px !important;
  border: 1px solid var(--border2) !important;
  background: rgba(0, 0, 0, 0.18) !important;
  color: var(--text2) !important;
  font-family: var(--font-ui);
  font-size: 0.68rem !important;
  font-weight: 600 !important;
  letter-spacing: 0.05em;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04);
}
[data-theme="light"] .header-pills .btn {
  background: rgba(15, 23, 42, 0.04) !important;
}
.header-pills .btn:hover {
  border-color: var(--border3) !important;
  background: var(--panel2) !important;
  color: var(--text) !important;
}
.header-pills .btn:active { transform: translateY(1px); }

@media (max-width: 980px) {
  .header-brand::after,
  .header-pills::before { display: none; }
}

/* ---- Layout Grid ---------------------------------------------- */
.grid-main {
  display: grid;
  grid-template-columns: 1.15fr 1.05fr 1fr;
  gap: 10px;
}
.grid-bottom {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px;
  margin-top: 10px;
}
@media (max-width: 1200px) {
  .grid-main { grid-template-columns: 1fr 1fr; }
  .grid-bottom { grid-template-columns: 1fr 1fr; }
}
@media (max-width: 800px) {
  .grid-main { grid-template-columns: 1fr; }
  .grid-bottom { grid-template-columns: 1fr; }
  body { padding: 6px 8px 24px; }
}

/* ---- Panel Aesthetics ----------------------------------------- */
.panel {
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 14px 16px;
  box-shadow: var(--shadow);
  min-width: 0;
  position: relative;
}
.panel-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  flex-wrap: wrap;
  margin-bottom: 10px;
  padding-bottom: 6px;
  border-bottom: 1px solid var(--border);
}
.panel-title {
  font-family: var(--font-ui);
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.05em;
  text-transform: uppercase;
  color: var(--text2);
  display: flex;
  align-items: center;
  gap: 7px;
  margin-bottom: 0;
}
.panel-title .dot {
  width: 5px;
  height: 5px;
  border-radius: 50%;
  background: var(--accent2);
  flex-shrink: 0;
}
.group-label {
  display: flex;
  align-items: center;
  gap: 8px;
  font-family: var(--font-ui);
  font-size: 0.66rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--muted);
  margin: 10px 0 6px;
}
.group-label::after { content: ""; flex: 1; height: 1px; background: var(--border); }
.group-label:first-child { margin-top: 0; }
.cmd-group + .cmd-group {
  border-top: 1px solid var(--border);
  padding-top: 6px;
  margin-top: 8px;
}
.card-label {
  font-family: var(--font-ui);
  font-size: 0.66rem;
  font-weight: 700;
  letter-spacing: 0.05em;
  text-transform: uppercase;
  color: var(--muted);
  margin-bottom: 6px;
}
.hint-text { font-size: 0.70rem; color: var(--muted); line-height: 1.4; margin-top: 4px; }
.hint-mono {
  font-family: var(--font-mono);
  font-size: 0.64rem;
  letter-spacing: 0.04em;
  color: var(--muted);
}
.btn-block { width: 100%; justify-content: center; }
.field-status { font-size: 0.70rem; margin-top: 4px; min-height: 1.1em; font-family: var(--font-mono); }

/* ---- Precision Telemetry Instrumentation ----------------------- */
.telem-grid {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

/* Primary Hero Metrics: Depth & Battery */
.telem-hero-strip {
  display: grid;
  grid-template-columns: 1fr 1fr;
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  overflow: hidden;
}
.telem-hero-bay {
  padding: 10px 14px;
  display: flex;
  flex-direction: column;
}
.telem-hero-bay:first-child {
  border-right: 1px solid var(--border);
  border-top: 2px solid var(--accent2);
}
.telem-hero-bay:last-child {
  border-top: 2px solid var(--accent2);
}
.telem-hero-label {
  font-size: 0.65rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--muted);
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.telem-hero-tag {
  font-size: 0.58rem;
  color: var(--dim);
  font-weight: 600;
}
.telem-hero-value {
  font-size: 1.85rem;
  font-weight: 700;
  font-family: var(--font-mono);
  color: #FFFFFF;
  line-height: 1.15;
  margin-top: 4px;
  font-variant-numeric: tabular-nums;
  letter-spacing: -0.02em;
}
.telem-hero-unit {
  font-size: 0.78rem;
  font-weight: 600;
  color: var(--muted);
  margin-left: 3px;
  font-family: var(--font-ui);
}
/* Light theme: the white hero value would vanish on the pale card */
[data-theme="light"] .telem-hero-value { color: #0F172A; }

/* Segmented Ribbon: Euler Angles & Environment */
.telem-ribbon {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  overflow: hidden;
}
.telem-ribbon-cell {
  padding: 6px 10px;
  border-right: 1px solid var(--border);
  display: flex;
  flex-direction: column;
}
.telem-ribbon-cell:last-child { border-right: none; }
.telem-ribbon-label {
  font-size: 0.62rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--muted);
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.telem-ribbon-val {
  font-size: 1.15rem;
  font-weight: 700;
  font-family: var(--font-mono);
  color: var(--text);
  line-height: 1.2;
  margin-top: 2px;
  font-variant-numeric: tabular-nums;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.telem-ribbon-unit {
  font-size: 0.68rem;
  font-weight: 500;
  color: var(--muted);
  margin-left: 2px;
  font-family: var(--font-ui);
}

/* GPS Nav Ribbon */
.gps-bar {
  font-family: var(--font-mono);
  font-size: 0.70rem;
  color: var(--muted);
  padding: 6px 10px;
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  display: flex;
  align-items: center;
  gap: 8px;
}
.gps-bar .gps-badge {
  font-family: var(--font-ui);
  font-size: 0.60rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  padding: 1px 5px;
  border-radius: 2px;
  background: var(--accent-dim);
  color: var(--accent2);
  border: 1px solid var(--border2);
}
.gps-bar #tGps { color: var(--text2); font-weight: 600; }

/* ---- 3D Attitude Horizon & Flight Dynamics HUD ---------------- */
.orient3d-wrap {
  position: relative;
}
.orient3d-scene {
  height: 136px;
  perspective: 550px;
  display: flex;
  align-items: center;
  justify-content: center;
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  overflow: hidden;
  position: relative;
  background: radial-gradient(circle at center, #0e192d 0%, #070c16 100%);
  box-shadow: inset 0 0 25px rgba(0, 0, 0, 0.55);
}
/* Primary Flight Display Synthetic Horizon Overlay */
.orient3d-scene::before {
  content: "";
  position: absolute;
  inset: 0;
  background-image:
    radial-gradient(circle at center, transparent 48px, rgba(59, 130, 246, 0.16) 49px, transparent 50px),
    linear-gradient(90deg, transparent 49.6%, rgba(59, 130, 246, 0.22) 49.6%, rgba(59, 130, 246, 0.22) 50.4%, transparent 50.4%),
    linear-gradient(0deg, transparent 49.6%, rgba(59, 130, 246, 0.22) 49.6%, rgba(59, 130, 246, 0.22) 50.4%, transparent 50.4%),
    linear-gradient(to right, rgba(59, 130, 246, 0.05) 1px, transparent 1px),
    linear-gradient(to bottom, rgba(59, 130, 246, 0.05) 1px, transparent 1px);
  background-size: 100% 100%, 100% 100%, 100% 100%, 18px 18px, 18px 18px;
  background-position: center center;
  pointer-events: none;
  z-index: 1;
}
.orient3d-box {
  width: 96px;
  height: 32px;
  position: relative;
  transform-style: preserve-3d;
  transition: transform .08s linear;
  z-index: 2;
}
.orient3d-box .face {
  position: absolute;
  display: flex;
  align-items: center;
  justify-content: center;
  font-family: var(--font-ui);
  font-size: 9.5px;
  font-weight: 800;
  letter-spacing: 0.08em;
  color: #FFFFFF;
  border: 1px solid rgba(96, 165, 250, 0.55);
  box-shadow: inset 0 0 8px rgba(0, 0, 0, 0.25);
}
.orient3d-box .face.front,
.orient3d-box .face.back  { width: 96px; height: 32px; background: rgba(37, 99, 235, 0.92); left: 0; top: 0; }
.orient3d-box .face.left,
.orient3d-box .face.right { width: 44px; height: 32px; background: rgba(29, 78, 216, 0.88); left: 26px; top: 0; }
.orient3d-box .face.top,
.orient3d-box .face.bottom{ width: 96px; height: 44px; background: rgba(30, 64, 175, 0.88); left: 0; top: -6px; }
.orient3d-box .face.front  { transform: translateZ(22px); }
.orient3d-box .face.back   { transform: rotateY(180deg) translateZ(22px); }
.orient3d-box .face.right  { transform: rotateY(90deg) translateZ(48px); }
.orient3d-box .face.left   { transform: rotateY(-90deg) translateZ(48px); }
.orient3d-box .face.top    { transform: rotateX(90deg) translateZ(16px); }
.orient3d-box .face.bottom { transform: rotateX(-90deg) translateZ(16px); }

/* ---- Hull Integrity Matrix (Water Sensors) --------------------- */
.leak-row {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  overflow: hidden;
}
.leak-sensor-card {
  padding: 6px 8px;
  border-right: 1px solid var(--border);
  display: flex;
  align-items: center;
  gap: 6px;
  background: transparent;
}
.leak-sensor-card:last-child { border-right: none; }
.leak-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--ok);
  border: 1px solid rgba(16, 185, 129, 0.5);
  flex-shrink: 0;
  box-shadow: 0 0 5px rgba(16, 185, 129, 0.4);
}
.leak-dot.alert {
  background: var(--danger);
  border-color: rgba(239, 68, 68, 0.8);
  box-shadow: 0 0 8px var(--danger);
  animation: gcsBlink 0.8s infinite;
}
.leak-sensor-info { display: flex; flex-direction: column; overflow: hidden; }
.leak-sensor-name {
  font-family: var(--font-ui);
  font-size: 0.64rem;
  font-weight: 700;
  letter-spacing: 0.04em;
  color: var(--text2);
  text-transform: uppercase;
  white-space: nowrap;
}
.leak-sensor-status {
  font-family: var(--font-mono);
  font-size: 0.56rem;
  font-weight: 600;
  color: var(--ok);
  letter-spacing: 0.04em;
}
.leak-dot.alert + .leak-sensor-info .leak-sensor-status { color: var(--danger); }

/* ---- Controls & Button Hierarchy ------------------------------ */
.btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 5px;
  min-height: 28px;
  padding: 4px 11px;
  border: 1px solid var(--border2);
  border-radius: var(--radius-sm);
  background: var(--panel2);
  color: var(--text2);
  font-family: var(--font-ui);
  font-size: 0.73rem;
  font-weight: 600;
  letter-spacing: 0.02em;
  cursor: pointer;
  transition: all .15s ease;
  white-space: nowrap;
  user-select: none;
}
.btn:hover         { border-color: var(--accent); color: #FFFFFF; background: var(--hover); }
.btn:active        { transform: translateY(1px); }
.btn:disabled      { opacity: 0.4; cursor: not-allowed; transform: none; box-shadow: none; }
.btn.btn-ok        { border-color: rgba(16, 185, 129, 0.35); color: var(--ok); background: var(--ok-dim); }
.btn.btn-ok:hover  { background: rgba(16, 185, 129, 0.20); border-color: var(--ok); color: #FFFFFF; }
.btn.btn-danger    { border-color: rgba(239, 68, 68, 0.35); color: var(--danger); background: var(--danger-dim); }
.btn.btn-danger:hover { background: rgba(239, 68, 68, 0.20); border-color: var(--danger); color: #FFFFFF; }
.btn.btn-warn      { border-color: rgba(245, 158, 11, 0.35); color: var(--warn); background: var(--warn-dim); }
.btn.btn-warn:hover{ background: rgba(245, 158, 11, 0.20); border-color: var(--warn); color: #FFFFFF; }
.btn.btn-active,
.btn.active        { border-color: var(--accent); color: #FFFFFF; background: var(--accent); box-shadow: 0 1px 6px var(--accent-glow); }
.btn-group         { display: flex; flex-wrap: wrap; gap: 5px; margin-top: 6px; }

/* Industrial Emergency Stop (E-STOP) Control Bay */
.estop-strip {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  padding: 8px 12px;
  border: 1px solid rgba(239, 68, 68, 0.35);
  border-radius: var(--radius-sm);
  background: linear-gradient(90deg, rgba(239, 68, 68, 0.08) 0%, rgba(239, 68, 68, 0.02) 100%);
}
.btn-estop {
  border: 1px solid var(--estop);
  background: var(--estop);
  color: #FFFFFF;
  font-weight: 800;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  font-size: 0.74rem;
  padding: 6px 14px;
  box-shadow: 0 0 14px var(--estop-glow);
}
.btn-estop:hover {
  background: #B91C1C;
  border-color: #EF4444;
  color: #FFFFFF;
  box-shadow: 0 0 18px var(--estop-glow);
}
.btn-estop-off {
  border: 1px solid var(--border2);
  background: transparent;
  color: var(--muted);
  font-weight: 600;
  letter-spacing: 0.04em;
  font-size: 0.72rem;
  padding: 6px 10px;
}
.btn-estop-off:hover { background: var(--hover); color: var(--text); border-color: var(--border3); }

/* ---- Rosbag Recording Subsystem -------------------------------- */
.rec-indicator {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 10px;
  border-radius: var(--radius-sm);
  border: 1px solid var(--border);
  background: var(--bg2);
  font-family: var(--font-ui);
  font-size: 0.72rem;
  font-weight: 600;
  color: var(--muted);
}
.rec-indicator .rec-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--dim);
  flex-shrink: 0;
}
.rec-indicator.recording {
  border-color: rgba(239, 68, 68, 0.45);
  color: var(--danger);
  background: var(--danger-dim);
}
.rec-indicator.recording .rec-dot {
  background: var(--danger);
  box-shadow: 0 0 8px var(--danger);
  animation: gcsBlink 1s infinite;
}
#recStatus {
  color: inherit;
  font-family: var(--font-mono);
  font-variant-numeric: tabular-nums;
  font-weight: 600;
}

/* Annunciator System Information Strip */
.sysinfo {
  font-family: var(--font-mono);
  font-size: 0.68rem;
  font-weight: 600;
  color: var(--muted);
  margin-top: 8px;
  padding: 6px 10px;
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 4px;
}
.sysinfo > span:not(.sysinfo-sep) { color: var(--text2); font-weight: 700; }
.sysinfo-sep { color: var(--border2); margin: 0 5px; }

/* ---- File Storage Console ------------------------------------- */
.file-tree {
  font-size: 0.74rem;
  font-family: var(--font-mono);
  max-height: 180px;
  min-height: 75px;
  overflow-y: auto;
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  background: var(--bg2);
  padding: 6px;
}
.file-empty-state {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  padding: 14px 8px;
  color: var(--muted);
  font-family: var(--font-ui);
  font-size: 0.72rem;
}
.folder-item { margin: 3px 0; }
.folder-header {
  display: flex;
  align-items: center;
  gap: 6px;
  cursor: pointer;
  color: var(--accent2);
  padding: 4px 7px;
  border-radius: var(--radius-sm);
  user-select: none;
  font-weight: 600;
  transition: all .12s ease;
}
.folder-header:hover { color: #FFFFFF; background: var(--hover); }
.folder-arrow { transition: transform .15s ease; display: inline-block; width: 10px; font-size: 0.65rem; }
.folder-header.open .folder-arrow { transform: rotate(90deg); }
.folder-children { padding-left: 14px; display: none; }
.folder-children.open { display: block; }
.file-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 5px 8px;
  border-radius: var(--radius-sm);
  transition: background .12s ease;
}
.file-row:hover { background: var(--hover); }
.file-name {
  color: var(--text2);
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 0.74rem;
}
.file-size { color: var(--muted); margin: 0 8px; font-size: 0.68rem; white-space: nowrap; }
.file-actions { display: flex; gap: 4px; }
.btn-icon {
  padding: 3px 8px;
  font-size: 0.68rem;
  font-weight: 600;
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  background: var(--panel2);
  cursor: pointer;
  color: var(--muted);
  font-family: var(--font-ui);
  transition: all .12s ease;
}
.btn-icon.dl:hover    { color: var(--ok);     border-color: var(--ok); background: var(--ok-dim); }
.btn-icon.del:hover   { color: var(--danger); border-color: var(--danger); background: var(--danger-dim); }
.btn-icon.force       { margin-left: 4px; }
.btn-icon.force:hover { color: var(--accent2); border-color: var(--accent); background: var(--accent-dim); }

/* Upload Progress Tracker */
.progress-wrap { margin-top: 8px; }
.progress-track {
  height: 4px;
  border-radius: 2px;
  background: var(--border);
  overflow: hidden;
}
.progress-fill {
  height: 100%;
  width: 0%;
  background: linear-gradient(90deg, var(--accent) 0%, var(--accent2) 100%);
  transition: width .25s ease-out;
}
.file-status {
  font-size: 0.7rem;
  color: var(--muted);
  margin-top: 5px;
  font-family: var(--font-mono);
}

input[type="file"]::file-selector-button {
  font-family: var(--font-ui);
  font-size: 0.74rem;
  font-weight: 600;
  padding: 4px 10px;
  margin-right: 8px;
  border: 1px solid var(--border2);
  border-radius: var(--radius-sm);
  background: var(--panel2);
  color: var(--text2);
  cursor: pointer;
  transition: all .15s ease;
}
input[type="file"]::file-selector-button:hover {
  border-color: var(--accent2); color: var(--text);
}

/* ---- Telemetry Link Channels ---------------------------------- */
.channel-list { display: flex; flex-direction: column; gap: 6px; }
.channel-row {
  display: flex;
  flex-direction: column;
  gap: 5px;
  padding: 8px 11px 7px 12px;
  border: 1px solid var(--border);
  border-left: 3px solid var(--border2);
  border-radius: 5px;
  background: var(--bg2);
  font-size: 0.74rem;
  transition: border-color .15s ease, background .15s ease;
  box-sizing: border-box;
}
.channel-row.up {
  border-left-color: var(--ok);
}
.channel-row.down {
  border-left-color: rgba(239, 68, 68, 0.55);
}
.channel-row:hover { border-color: var(--border2); }
.channel-row.up:hover { border-left-color: var(--ok); }
.channel-row.down:hover { border-left-color: var(--danger); }
.channel-top {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  width: 100%;
}
.channel-left {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  font-family: var(--font-mono);
  font-weight: 700;
  font-size: 0.75rem;
  color: var(--text);
  min-width: 0;
}
.channel-name {
  letter-spacing: 0.02em;
  white-space: nowrap;
}
.channel-ep {
  font-family: var(--font-mono);
  font-size: 0.64rem;
  font-weight: 500;
  color: var(--text2);
  background: rgba(148, 163, 184, 0.08);
  border: 1px solid var(--border);
  border-radius: 3px;
  padding: 1px 6px;
  white-space: nowrap;
}
[data-theme="light"] .channel-ep {
  background: #F1F5F9;
  border-color: #CBD5E1;
  color: #334155;
}
.channel-right {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  flex-shrink: 0;
}
.channel-dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: var(--muted);
  flex-shrink: 0;
}
.channel-dot.up   { background: var(--ok);     box-shadow: 0 0 6px var(--ok); }
.channel-dot.down { background: var(--danger); animation: gcsBlink 1.4s infinite; }
.channel-dot.connecting { background: var(--warn); animation: gcsBlink 1.4s infinite; }
.channel-role {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 48px;
  height: 19px;
  font-size: 0.58rem;
  padding: 0 6px;
  border-radius: 3px;
  border: 1px solid var(--border2);
  background: rgba(148, 163, 184, 0.08);
  color: var(--muted);
  font-family: var(--font-mono);
  letter-spacing: 0.06em;
  font-weight: 700;
  text-transform: uppercase;
  line-height: 1;
  box-sizing: border-box;
}
.channel-role.down      { color: #F87171;         border-color: rgba(239, 68, 68, 0.35);   background: rgba(239, 68, 68, 0.10); }
.channel-role.active    { color: var(--ok);       border-color: rgba(16, 185, 129, 0.45);  background: var(--ok-dim); }
.channel-role.secondary { color: var(--accent2);  border-color: rgba(56, 189, 248, 0.45);  background: var(--accent-dim); }
.channel-role.backup    { color: var(--warn);     border-color: rgba(245, 158, 11, 0.45);  background: var(--warn-dim); }
.channel-role.pinned    { color: var(--warn);     border-color: rgba(245, 158, 11, 0.55);  background: var(--warn-dim); min-width: auto; }
[data-theme="light"] .channel-role.down {
  color: #B91C1C;
  border-color: #FCA5A5;
  background: #FEF2F2;
}
.channel-meta {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  width: 100%;
  padding-top: 4px;
  border-top: 1px dashed rgba(148, 163, 184, 0.14);
  color: var(--muted);
  font-size: 0.65rem;
  font-family: var(--font-mono);
  line-height: 1.2;
}
[data-theme="light"] .channel-meta {
  border-top-color: #E2E8F0;
  color: #64748B;
}
.channel-meta-state {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  color: var(--muted);
}
.channel-meta-retries {
  font-variant-numeric: tabular-nums;
  color: var(--text2);
  font-weight: 600;
}
[data-theme="light"] .channel-meta-retries {
  color: #475569;
}
.secondary-log {
  display: flex;
  align-items: center;
  justify-content: space-between;
  font-size: 0.71rem;
  font-family: var(--font-mono);
  color: var(--text2);
  background: var(--console-bg, var(--bg2));
  border: 1px solid var(--border);
  border-radius: 5px;
  padding: 8px 12px;
  min-height: 32px;
  line-height: 1.4;
  box-sizing: border-box;
}
[data-theme="light"] .secondary-log {
  background: #F8FAFC;
  border-color: #CBD5E1;
}

/* Link Channels Subtabs */
.link-subtabs {
  display: flex;
  gap: 4px;
  margin: 8px 0;
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: 5px;
  padding: 3px;
}
.link-subtab {
  flex: 1;
  height: 27px;
  padding: 0 10px;
  border: 1px solid transparent;
  border-radius: 3.5px;
  background: transparent;
  color: var(--muted);
  font-family: var(--font-ui);
  font-size: 0.72rem;
  font-weight: 600;
  cursor: pointer;
  transition: all .14s ease;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
}
.link-subtab:hover  { color: var(--text); }
.link-subtab.active {
  color: var(--text);
  background: var(--panel2);
  border-color: var(--border2);
  font-weight: 700;
}

/* Manual Connection Mode Tabs */
.manual-mode-tabs {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 4px;
  margin-top: 8px;
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: 5px;
  padding: 3px;
}
.manual-mode-tab {
  padding: 5px 8px;
  border: 1px solid transparent;
  border-radius: 3.5px;
  background: transparent;
  color: var(--muted);
  font-family: var(--font-mono);
  font-size: 0.65rem;
  font-weight: 600;
  cursor: pointer;
  transition: all .14s ease;
  text-align: center;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.manual-mode-tab:hover  { color: var(--text); background: var(--hover); }
.manual-mode-tab.active {
  color: #38BDF8;
  background: #162235;
  border-color: rgba(56, 189, 248, 0.40);
  font-weight: 700;
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.25);
}
[data-theme="light"] .manual-mode-tabs {
  background: #E2E8F0;
  border-color: #CBD5E1;
}
[data-theme="light"] .manual-mode-tab {
  color: #475569;
}
[data-theme="light"] .manual-mode-tab.active {
  background: #FFFFFF;
  color: #0F172A;
  border-color: #94A3B8;
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.08);
}
.manual-field-group { display: flex; gap: 6px; margin-top: 6px; }
.manual-input {
  flex: 1;
  min-width: 0;
  min-height: 30px;
  background: var(--bg2);
  border: 1px solid var(--border);
  color: var(--text);
  padding: 5px 9px;
  border-radius: var(--radius-sm);
  font-size: 0.76rem;
  font-family: var(--font-mono);
}
.manual-input::placeholder { color: var(--dim); }
.manual-input:focus {
  outline: none;
  border-color: var(--accent);
  box-shadow: 0 0 0 2px var(--accent-glow);
}
.manual-input-narrow { flex: 0 0 80px; }

/* ---- ROS2 Graph & Black Box Diagnostics ----------------------- */
.grid-tools {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
  margin-top: 12px;
}
.grid-tools-wide { grid-template-columns: minmax(0, 2fr) minmax(0, 1fr); }
@media (max-width: 980px) {
  .grid-tools, .grid-tools-wide { grid-template-columns: minmax(0, 1fr); }
}

.topic-list {
  font-size: 0.75rem;
  font-family: var(--font-mono);
  max-height: 220px;
  overflow-y: auto;
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  background: var(--bg2);
  padding: 4px;
}
.topic-row {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 8px;
  padding: 5px 8px;
  border-radius: 3px;
}
.topic-row:nth-child(odd) { background: rgba(255, 255, 255, 0.02); }
.topic-row:hover { background: var(--hover); }
.topic-name { color: var(--text2); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.topic-types { color: var(--muted); font-size: 0.66rem; white-space: nowrap; }

.console-box {
  background: var(--console-bg);
  border: 1px solid var(--console-border, var(--border));
  border-radius: var(--radius-sm);
  padding: 10px 12px;
  min-height: 85px;
  max-height: 220px;
  height: auto;
  overflow-y: auto;
  font-family: var(--font-mono);
  font-size: 0.72rem;
  line-height: 1.55;
  color: var(--console-text);
  box-shadow: inset 0 2px 8px rgba(0, 0, 0, 0.45);
}
.console-box::-webkit-scrollbar,
.topic-list::-webkit-scrollbar,
.param-list-wrap::-webkit-scrollbar,
.file-tree::-webkit-scrollbar,
.channel-list::-webkit-scrollbar { width: 6px; height: 6px; }
.console-box::-webkit-scrollbar-thumb,
.topic-list::-webkit-scrollbar-thumb,
.param-list-wrap::-webkit-scrollbar-thumb,
.file-tree::-webkit-scrollbar-thumb,
.channel-list::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 3px; }
.console-box::-webkit-scrollbar-thumb:hover,
.topic-list::-webkit-scrollbar-thumb:hover,
.param-list-wrap::-webkit-scrollbar-thumb:hover,
.file-tree::-webkit-scrollbar-thumb:hover { background: var(--muted); }
.console-box::-webkit-scrollbar-track,
.topic-list::-webkit-scrollbar-track { background: transparent; }
.console-line { display: block; white-space: pre-wrap; word-break: break-word; }
.console-line .ts  { color: var(--muted); }
.console-line .tag { color: var(--accent2); }

/* Universal Professional Empty States */
.empty-state-card {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 12px 14px;
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  color: var(--muted);
  font-family: var(--font-ui);
}
.empty-state-card .empty-icon { font-size: 1.15rem; flex-shrink: 0; opacity: 0.75; }
.empty-state-card .empty-title {
  font-size: 0.68rem;
  font-weight: 700;
  letter-spacing: 0.05em;
  text-transform: uppercase;
  color: var(--text2);
}
.empty-state-card .empty-desc {
  font-size: 0.65rem;
  color: var(--muted);
  margin-top: 1px;
}

/* ---- Controller Subsystem -------------------------------------- */
.joy-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
.joy-stick-wrap { text-align: center; }
.joy-stick-label {
  font-size: 0.68rem;
  color: var(--muted);
  font-family: var(--font-ui);
  margin-bottom: 5px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}
.joy-canvas { display: block; margin: 0 auto; }
.joy-buttons { display: flex; flex-wrap: wrap; gap: 5px; margin-top: 10px; }
.joy-btn-indicator {
  width: 24px;
  height: 24px;
  border-radius: 50%;
  background: var(--bg2);
  border: 1px solid var(--border2);
  font-size: 0.62rem;
  font-weight: 700;
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--muted);
  font-family: var(--font-mono);
  transition: all .08s ease;
}
.joy-btn-indicator.pressed {
  background: var(--accent);
  color: #FFFFFF;
  border-color: var(--accent2);
  box-shadow: 0 0 10px var(--accent-glow);
}
.joy-axes-bars { margin-top: 10px; }
.joy-axis-row { display: flex; align-items: center; gap: 8px; margin: 6px 0; }
.joy-axis-label {
  font-size: 0.68rem;
  color: var(--muted);
  width: 28px;
  font-family: var(--font-mono);
  text-align: right;
  font-weight: 700;
}
.joy-axis-track {
  flex: 1;
  height: 6px;
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: 3px;
  position: relative;
  overflow: visible;
}
.joy-axis-fill {
  position: absolute;
  height: 100%;
  background: var(--accent2);
  border-radius: 2px;
  top: 0;
  transition: width .05s, left .05s;
}
.joy-axis-center {
  position: absolute;
  width: 1px;
  height: 160%;
  background: var(--border3);
  top: -30%;
  left: 50%;
}

/* Dedicated Hardware Controller Standby Bay */
.joy-empty-bay {
  display: flex;
  flex-direction: column;
  align-items: center;
  text-align: center;
  padding: 16px 12px;
  background: var(--bg2);
  border: 1px dashed var(--border2);
  border-radius: var(--radius-sm);
  margin: 4px 0 8px;
}
.joy-status-tag {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-family: var(--font-mono);
  font-size: 0.62rem;
  font-weight: 700;
  color: var(--muted);
  letter-spacing: 0.06em;
  padding: 2px 7px;
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: 3px;
  margin-bottom: 8px;
}
.pulse-beacon {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: var(--warn);
  box-shadow: 0 0 6px var(--warn);
  animation: gcsBlink 1.4s infinite;
}
.joy-empty-title {
  font-family: var(--font-ui);
  font-size: 0.74rem;
  font-weight: 700;
  letter-spacing: 0.04em;
  color: var(--text2);
  text-transform: uppercase;
}
.joy-empty-sub {
  font-size: 0.68rem;
  color: var(--muted);
  max-width: 270px;
  margin-top: 3px;
  line-height: 1.4;
}

/* ---- Virtual Joystick (Emergency Station) --------------------- */
.vjoy-panel {
  display: flex;
  flex-direction: column;
}
.vjoy-panel-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding-bottom: 10px;
  margin-bottom: 8px;
  border-bottom: 1px solid var(--border);
}
.vjoy-title-wrap {
  display: flex;
  align-items: center;
  gap: 8px;
}
.vjoy-accent-bar {
  width: 3px;
  height: 16px;
  background: var(--accent2, #06B6D4);
  border-radius: 2px;
  display: inline-block;
  flex-shrink: 0;
  box-shadow: 0 0 8px rgba(56, 189, 248, 0.5);
}
.vjoy-title-text {
  font-family: var(--font-ui);
  font-size: 0.96rem;
  font-weight: 700;
  color: var(--text);
  letter-spacing: -0.01em;
}
.vjoy-tag {
  color: var(--muted);
  font-family: var(--font-ui);
  font-size: 0.80rem;
  font-weight: 400;
  margin-left: 6px;
}
.vjoy-hint {
  font-family: var(--font-mono);
  font-size: 0.65rem;
  letter-spacing: 0.05em;
  color: var(--dim, var(--muted));
  font-weight: 500;
}
.vjoy-main-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin: 6px 0 12px;
  width: 100%;
}
.vjoy-bezel {
  position: relative;
  width: 194px;
  height: 194px;
  flex: 1 1 194px;
  min-width: 0;
  max-width: 194px;
  aspect-ratio: 1;
  border-radius: 50%;
  padding: 7px;
  background: #172536;
  border: 1px solid #31465c;
  box-shadow:
    0 4px 10px rgba(0, 0, 0, 0.2),
    inset 0 1px 2px rgba(255, 255, 255, 0.08),
    inset 0 -2px 3px rgba(0, 0, 0, 0.38);
  display: flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  flex-shrink: 1;
}
:root[data-theme="dark"] .vjoy-bezel,
[data-theme="dark"] .vjoy-bezel {
  background: #172536;
  border-color: #31465c;
  box-shadow:
    0 4px 10px rgba(0, 0, 0, 0.2),
    inset 0 1px 2px rgba(255, 255, 255, 0.08),
    inset 0 -2px 3px rgba(0, 0, 0, 0.38);
}
[data-theme="light"] .vjoy-bezel {
  background: #E6ECF3;
  border-color: #C3CFDC;
  box-shadow:
    0 3px 10px rgba(15, 23, 42, 0.10),
    inset 0 1px 2px rgba(255, 255, 255, 0.9);
}
#vjoyCanvas {
  cursor: crosshair;
  touch-action: none;
  user-select: none;
  display: block;
  width: 100%;
  height: 100%;
  min-width: 0;
  max-width: 100%;
  aspect-ratio: 1;
  border-radius: 50%;
  background: #101a26;
  box-shadow: inset 0 1px 8px rgba(0, 0, 0, 0.48);
  border: 1px solid #263a4e;
  box-sizing: border-box;
}
:root[data-theme="dark"] #vjoyCanvas,
[data-theme="dark"] #vjoyCanvas {
  background: #101a26;
  box-shadow: inset 0 1px 8px rgba(0, 0, 0, 0.48);
  border-color: #263a4e;
}
[data-theme="light"] #vjoyCanvas {
  background: #F8FBFD;
  box-shadow: inset 0 1px 6px rgba(15, 23, 42, 0.08);
  border-color: #D7E0EA;
}

/* Thruster Card */
.vjoy-thruster-card {
  flex: 0 0 80px;
  height: 210px;
  background: #172536;
  border: 1px solid #263a4e;
  border-radius: 8px;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: space-between;
  padding: 10px 8px 8px;
  box-sizing: border-box;
}
[data-theme="light"] .vjoy-thruster-card {
  background: #F1F5F9;
  border-color: #D7E0EA;
}
.vjoy-thruster-label {
  font-family: var(--font-ui);
  font-size: 0.78rem;
  font-weight: 500;
  color: var(--text2);
}
.vjoy-thruster-bay {
  display: flex;
  align-items: center;
  justify-content: center;
  position: relative;
  height: 138px;
  width: 100%;
}
.vjoy-thruster-slider {
  -webkit-appearance: none;
  appearance: none;
  writing-mode: vertical-lr;
  direction: rtl;
  width: 8px;
  height: 138px;
  background: linear-gradient(to top, var(--accent2, #06B6D4) 0%, var(--accent2, #06B6D4) var(--thruster-pct, 50%), rgba(148, 163, 184, 0.18) var(--thruster-pct, 50%), rgba(148, 163, 184, 0.18) 100%);
  border-radius: 5px;
  border: 1px solid #53616f;
  outline: none;
  cursor: pointer;
  padding: 0;
  margin: 0;
  box-shadow: inset 0 0 0 1px rgba(255,255,255,0.05);
}
:root[data-theme="dark"] .vjoy-thruster-slider,
[data-theme="dark"] .vjoy-thruster-slider {
  background: linear-gradient(to top, var(--accent2, #38BDF8) 0%, var(--accent2, #38BDF8) var(--thruster-pct, 50%), rgba(148, 163, 184, 0.2) var(--thruster-pct, 50%), rgba(148, 163, 184, 0.2) 100%);
}
.vjoy-thruster-slider::-webkit-slider-thumb {
  -webkit-appearance: none;
  appearance: none;
  width: 15px;
  height: 15px;
  border-radius: 50%;
  background: var(--accent2, #0B7A8C);
  border: none;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.28);
  cursor: grab;
}
.vjoy-thruster-slider::-moz-range-thumb {
  width: 15px;
  height: 15px;
  border-radius: 50%;
  background: var(--accent2, #0B7A8C);
  border: none;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.28);
  cursor: grab;
}
:root[data-theme="dark"] .vjoy-thruster-slider::-webkit-slider-thumb,
[data-theme="dark"] .vjoy-thruster-slider::-webkit-slider-thumb {
  background: var(--accent2, #38BDF8);
  box-shadow: 0 2px 6px rgba(56, 189, 248, 0.35), 0 0 0 3px rgba(56, 189, 248, 0.15);
}
:root[data-theme="dark"] .vjoy-thruster-slider::-moz-range-thumb,
[data-theme="dark"] .vjoy-thruster-slider::-moz-range-thumb {
  background: var(--accent2, #38BDF8);
  box-shadow: 0 2px 6px rgba(56, 189, 248, 0.35), 0 0 0 3px rgba(56, 189, 248, 0.15);
}
[data-theme="light"] .vjoy-thruster-slider {
  background: linear-gradient(to top, #0E9DB8 0%, #0E9DB8 var(--thruster-pct, 50%), rgba(100, 116, 139, 0.22) var(--thruster-pct, 50%), rgba(100, 116, 139, 0.22) 100%);
  border-color: #B7C4D2;
  box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.5);
}
[data-theme="light"] .vjoy-thruster-slider::-webkit-slider-thumb {
  background: #0E9DB8;
  box-shadow: 0 1px 3px rgba(15, 23, 42, 0.25), 0 0 0 3px rgba(14, 157, 184, 0.15);
}
[data-theme="light"] .vjoy-thruster-slider::-moz-range-thumb {
  background: #0E9DB8;
  box-shadow: 0 1px 3px rgba(15, 23, 42, 0.25), 0 0 0 3px rgba(14, 157, 184, 0.15);
}
.vjoy-thruster-val {
  font-family: var(--font-mono);
  font-size: 0.82rem;
  font-weight: 700;
  color: var(--text);
}

/* Toggle Strip */
.vjoy-toggle-strip {
  background: var(--panel2, var(--bg2));
  border: 1px solid var(--border);
  border-radius: 5px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 12px;
  margin-bottom: 10px;
  width: 100%;
  box-sizing: border-box;
}
.vjoy-toggle-label {
  font-family: var(--font-ui);
  font-size: 0.86rem;
  font-weight: 500;
  color: var(--text);
  cursor: pointer;
}
.vjoy-toggle-status {
  font-family: var(--font-mono);
  font-size: 0.75rem;
  font-weight: 700;
  color: var(--muted);
  margin-right: 8px;
}

/* Stop All Button */
.vjoy-stop-full {
  width: 100%;
  padding: 10px 0;
  border-radius: 6px;
  border: 1px solid #c94b50;
  background: rgba(146, 48, 53, 0.22);
  color: #ff6868;
  font-family: var(--font-ui);
  font-size: 0.9rem;
  font-weight: 700;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  cursor: pointer;
  transition: all 0.15s ease;
  box-shadow: none;
  box-sizing: border-box;
}
:root[data-theme="dark"] .vjoy-stop-full,
[data-theme="dark"] .vjoy-stop-full {
  background: rgba(146, 48, 53, 0.22);
  color: #ff6868;
  border-color: #c94b50;
}
.vjoy-stop-full:hover {
  background: rgba(239, 68, 68, 0.20);
  color: #FECACA;
}
:root[data-theme="dark"] .vjoy-stop-full:hover,
[data-theme="dark"] .vjoy-stop-full:hover {
  background: rgba(239, 68, 68, 0.22);
  color: #FCA5A5;
}
[data-theme="light"] .vjoy-stop-full {
  background: rgba(220, 38, 38, 0.07);
  color: #DC2626;
  border-color: #FCA5A5;
}
[data-theme="light"] .vjoy-stop-full:hover {
  background: rgba(220, 38, 38, 0.13);
  color: #B91C1C;
  border-color: #F87171;
}
.vjoy-stop-full:active {
  transform: scale(0.99);
}
.vjoy-stop-square {
  font-size: 0.8rem;
  line-height: 1;
}

/* Autonomy Flight Mode Segmented Selector */
.mode-buttons {
  display: flex;
  gap: 4px;
  margin-top: 8px;
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 3px;
}
.mode-buttons .btn {
  flex: 1;
  font-family: var(--font-ui);
  letter-spacing: 0.04em;
  font-weight: 700;
  border: 1px solid transparent;
  background: none;
  color: var(--muted);
  border-radius: 3px;
  min-height: 28px;
}
.mode-buttons .btn:hover {
  color: var(--text);
  background: var(--hover);
}
.mode-buttons .btn.active {
  border-color: var(--accent);
  color: #FFFFFF;
  background: var(--accent);
  box-shadow: 0 2px 8px var(--accent-glow);
}

/* Hardware-Style Toggle Switch */
.telem-toggle-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  padding: 8px 12px;
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  background: var(--bg2);
  margin-top: 8px;
}
.toggle-switch { position: relative; width: 34px; height: 19px; flex-shrink: 0; }
.toggle-switch input { opacity: 0; width: 0; height: 0; }
.toggle-slider {
  position: absolute; inset: 0;
  background: var(--border2);
  border-radius: 20px;
  cursor: pointer;
  transition: all .2s ease;
  border: 1px solid var(--border);
}
.toggle-slider::before {
  content: '';
  position: absolute;
  width: 13px; height: 13px;
  left: 2px; top: 2px;
  background: var(--muted);
  border-radius: 50%;
  transition: all .2s ease;
}
.toggle-switch input:checked + .toggle-slider {
  background: var(--accent);
  border-color: var(--accent2);
}
.toggle-switch input:checked + .toggle-slider::before {
  transform: translateX(15px);
  background: #FFFFFF;
}

/* ---- ROS2 Parameters Registry --------------------------------- */
.param-list-wrap { max-height: 320px; overflow-y: auto; }
.param-node-group { margin: 6px 0 12px; }
.param-node-header {
  font-family: var(--font-ui);
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.04em;
  color: var(--accent2);
  padding-bottom: 4px;
  margin-bottom: 6px;
  border-bottom: 1px solid var(--border);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.param-row { display: flex; align-items: center; gap: 6px; padding: 5px 4px; flex-wrap: wrap; }
.param-row:nth-child(odd) { background: rgba(255, 255, 255, 0.02); border-radius: 2px; }
.param-name {
  flex: 0 0 auto;
  min-width: 90px;
  max-width: 210px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 0.74rem;
  color: var(--text2);
  font-family: var(--font-mono);
}
.param-input {
  flex: 1 1 90px;
  min-width: 60px;
  background: var(--bg2);
  border: 1px solid var(--border);
  color: var(--text);
  padding: 4px 8px;
  border-radius: var(--radius-sm);
  font-family: var(--font-mono);
  font-size: 0.74rem;
}
.param-input:focus { outline: none; border-color: var(--accent); box-shadow: 0 0 0 2px var(--accent-glow); }
.param-input.dirty { border-color: var(--warn); background: var(--warn-dim); }
.param-status { font-size: 0.66rem; white-space: nowrap; font-family: var(--font-mono); font-weight: 600; }
.param-status.ok  { color: var(--ok); }
.param-status.err { color: var(--danger); }

/* ---- Digital Compass & Navigation Heading --------------------- */
.compass-panel {
  display: flex;
  flex-direction: column;
  padding: 0;
  overflow: hidden;
  background: #121a24;
  border: 1px solid #243140;
  border-radius: 8px;
}
[data-theme="light"] .compass-panel {
  background: #ffffff;
  border-color: #cfd7df;
}
.compass-panel .panel-head {
  margin: 0;
  padding: 13px 15px;
  background: #182230;
  border-bottom: 1px solid #243140;
}
[data-theme="light"] .compass-panel .panel-head {
  background: #f4f6f9;
  border-bottom-color: #d5dce3;
}
.compass-panel .panel-title {
  margin-bottom: 0 !important;
  font-family: var(--font-mono) !important;
  font-size: 0.96rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.01em !important;
  color: #e8eef4 !important;
  display: flex;
  align-items: center;
  gap: 10px;
}
[data-theme="light"] .compass-panel .panel-title {
  color: #10161d !important;
}
.compass-panel .panel-title .bar {
  width: 3px;
  height: 15px;
  border-radius: 1.5px;
  background: #2fb7c8;
  font-size: 0;
  color: transparent;
  margin-right: 0;
  flex-shrink: 0;
}
[data-theme="light"] .compass-panel .panel-title .bar {
  background: #0b7a8c;
}
.compass-wrap {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 15px;
  padding: 15px;
}
.compass-bezel {
  position: relative;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 50%;
}
#compassCanvas {
  display: block;
  width: 164px;
  height: 164px;
  border-radius: 50%;
  box-sizing: border-box;
}
.compass-heading-card {
  min-width: 140px;
  padding: 10px 18px 11px;
  text-align: center;
  background: #182230;
  border: 1px solid #243140;
  border-radius: 8px;
  box-sizing: border-box;
}
[data-theme="light"] .compass-heading-card {
  background: #f4f6f9;
  border-color: #d5dce3;
}
.compass-heading-label {
  color: #8695a5;
  font-family: var(--font-ui);
  font-size: 0.78rem;
  font-weight: 400;
  line-height: 1.2;
  margin-bottom: 4px;
}
[data-theme="light"] .compass-heading-label {
  color: #636e7c;
}
.compass-heading {
  font-family: var(--font-mono);
  font-size: 1.65rem;
  font-weight: 700;
  color: #e8eef4;
  font-variant-numeric: tabular-nums;
  line-height: 1.15;
  display: flex;
  align-items: baseline;
  justify-content: center;
  gap: 0;
}
[data-theme="light"] .compass-heading { color: #10161d; }
.compass-toggle-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 100%;
  gap: 10px;
  padding: 10px 14px;
  background: #182230;
  border: 1px solid #243140;
  border-radius: 6px;
  box-sizing: border-box;
  color: #e8eef4;
  font-family: var(--font-ui);
  font-size: 0.88rem;
}
[data-theme="light"] .compass-toggle-row {
  background: #f6f8fa;
  border-color: #d5dce3;
  color: #10161d;
}
.compass-toggle-row #magStreamLabel {
  font-size: 0.75rem !important;
  color: #8695a5 !important;
  font-family: var(--font-mono);
  font-weight: 700;
}
[data-theme="light"] .compass-toggle-row #magStreamLabel {
  color: #5b6775 !important;
}
.compass-toggle-row .toggle-switch {
  width: 40px;
  height: 22px;
}
.compass-toggle-row .toggle-slider {
  background: #34465a;
  border: 1px solid #324558;
  border-radius: 22px;
}
[data-theme="light"] .compass-toggle-row .toggle-slider {
  background: #b7c2cd;
  border-color: #b4bec9;
}
.compass-toggle-row .toggle-slider::before {
  width: 16px;
  height: 16px;
  left: 2px;
  top: 2px;
  background: #ffffff;
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.25);
}
.compass-toggle-row .toggle-switch input:checked + .toggle-slider {
  background: #2fb7c8;
  border-color: #2fb7c8;
}
[data-theme="light"] .compass-toggle-row .toggle-switch input:checked + .toggle-slider {
  background: #0b7a8c;
  border-color: #0b7a8c;
}
.compass-toggle-row .toggle-switch input:checked + .toggle-slider::before {
  transform: translateX(18px);
  background: #ffffff;
}

/* Terminal Window Frame */
.term-window-wrap {
  border: 1px solid var(--console-border, var(--border));
  border-radius: var(--radius-sm);
  overflow: hidden;
  box-shadow: 0 4px 16px rgba(0,0,0,0.3);
}
.term-title-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 5px 10px;
  background: #060A14;
  border-bottom: 1px solid rgba(255, 255, 255, 0.08);
}
.term-dots { display: flex; gap: 5px; align-items: center; }
.term-dots span { width: 7px; height: 7px; border-radius: 50%; display: inline-block; }
.tdot-red { background: #EF4444; }
.tdot-yel { background: #F59E0B; }
.tdot-grn { background: #10B981; }
.term-session {
  font-family: var(--font-mono);
  font-size: 0.64rem;
  color: #8496AB;
  letter-spacing: 0.04em;
}
.term-status-chip {
  font-family: var(--font-mono);
  font-size: 0.58rem;
  font-weight: 700;
  color: #10B981;
  letter-spacing: 0.06em;
  padding: 1px 4px;
  border-radius: 2px;
  background: rgba(16, 185, 129, 0.15);
  border: 1px solid rgba(16, 185, 129, 0.3);
}

/* ---- Authentication Modal ------------------------------------- */
.modal-overlay {
  display: none;
  position: fixed; inset: 0;
  background: rgba(4, 8, 16, 0.82);
  backdrop-filter: blur(6px);
  align-items: center; justify-content: center;
  z-index: 1000;
}
.modal-box {
  background: var(--panel);
  border: 1px solid var(--border2);
  border-radius: var(--radius);
  box-shadow: 0 20px 60px rgba(0, 0, 0, 0.75);
  padding: 20px 22px;
  width: 360px;
  max-width: 90vw;
}
.modal-title {
  font-family: var(--font-ui);
  font-size: 0.88rem;
  font-weight: 700;
  color: var(--text);
  margin-bottom: 8px;
  letter-spacing: 0.04em;
  text-transform: uppercase;
}
.modal-body { font-size: 0.76rem; color: var(--muted); margin-bottom: 14px; line-height: 1.5; }
.modal-input {
  width: 100%;
  background: var(--bg2);
  border: 1px solid var(--border);
  color: var(--text);
  padding: 8px 12px;
  border-radius: var(--radius-sm);
  font-size: 0.82rem;
  margin-bottom: 14px;
  font-family: var(--font-mono);
}
.modal-input:focus { outline: none; border-color: var(--accent); box-shadow: 0 0 0 2px var(--accent-glow); }
.modal-actions { display: flex; justify-content: flex-end; gap: 8px; }

/* ---- Top Navigation Tabs & Mission Planner -------------------- */
.top-tabs {
  display: none; /* Now smoothly integrated into header-nav; fallback retained */
}

#mapPlannerFrame {
  background: #080D12;
  border-radius: var(--radius);
}

.mp-subtabs { display: flex; gap: 4px; margin-bottom: 12px; border-bottom: 1px solid var(--border); }
.mp-subtab {
  padding: 8px 14px;
  background: none;
  border: none;
  border-bottom: 2px solid transparent;
  color: var(--muted);
  font-family: var(--font-ui);
  font-size: 0.74rem;
  font-weight: 600;
  cursor: pointer;
}
.mp-subtab:hover { color: var(--text2); }
.mp-subtab.active { color: var(--accent2); border-bottom-color: var(--accent); }
.mp-pane { display: none; }
.mp-pane.active { display: block; }

.mp-field { margin-bottom: 11px; }
.mp-field label {
  display: block;
  font-size: 0.66rem;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--muted);
  margin-bottom: 5px;
  font-family: var(--font-ui);
  font-weight: 600;
}
.mp-field input[type=text], .mp-field input[type=number], .mp-field textarea, .mp-field select {
  width: 100%;
  background: var(--bg2);
  border: 1px solid var(--border);
  color: var(--text);
  padding: 6px 10px;
  border-radius: var(--radius-sm);
  font-size: 0.78rem;
  font-family: var(--font-mono);
}
.mp-field textarea { resize: vertical; }
.mp-twin { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
.mp-toggle-row { display: flex; align-items: center; gap: 8px; font-size: 0.78rem; color: var(--text2); cursor: pointer; margin-bottom: 8px; }

.mission-list { display: flex; flex-direction: column; gap: 5px; max-height: 55vh; overflow-y: auto; }
.mission-item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 8px 12px;
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  font-size: 0.76rem;
  cursor: pointer;
  transition: border-color .15s ease;
}
.mission-item:hover { border-color: var(--accent); }
.mission-item .mi-name { color: var(--text2); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-family: var(--font-mono); }
.mission-item .mi-del { color: var(--muted); font-size: 0.95rem; line-height: 1; padding: 0 4px; }
.mission-item .mi-del:hover { color: var(--danger); }
.mission-item .mi-start { color: var(--muted); font-size: 0.82rem; line-height: 1; padding: 0 4px; }
.mission-item .mi-start:hover { color: var(--ok); }

.fs-grid { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 10px; }
@media (max-width: 900px) { .fs-grid { grid-template-columns: 1fr 1fr; } }
.fs-group-title {
  grid-column: 1/-1;
  font-family: var(--font-ui);
  font-size: 0.68rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--muted);
  margin-top: 6px;
  border-top: 1px solid var(--border);
  padding-top: 8px;
  font-weight: 700;
}
.yaml-box {
  background: var(--console-bg);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 10px;
  font-family: var(--font-mono);
  font-size: 0.7rem;
  white-space: pre;
  overflow-x: auto;
  color: var(--console-text);
  max-height: 220px;
  overflow-y: auto;
}

/* ---- WiFi Communications Matrix ------------------------------- */
.wifi-status {
  font-family: var(--font-mono);
  font-size: 0.68rem;
  margin-top: 4px;
  min-height: 1em;
}
.wifi-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 14px;
  align-items: start;
  margin-top: 4px;
}
.wifi-col {
  display: flex;
  flex-direction: column;
  min-width: 0;
}
.wifi-target-box {
  background: var(--console-bg);
  border: 1px solid var(--border);
  border-radius: 5px;
  padding: 8px 11px;
  margin-top: 8px;
}
[data-theme="light"] .wifi-target-box {
  background: #F8FAFC;
  border-color: #CBD5E1;
}
.wifi-dashed-box {
  max-height: 260px;
  overflow-y: auto;
  background: var(--console-bg);
  border: 1px dashed var(--border2);
  border-radius: 6px;
  padding: 13px 14px;
  font-family: var(--font-ui);
  font-size: 0.72rem;
  color: var(--muted);
  text-align: center;
  box-sizing: border-box;
}
[data-theme="light"] .wifi-dashed-box {
  background: #F8FAFC;
  border-color: #CBD5E1;
}
@media (max-width: 1100px) {
  .wifi-grid { grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); }
}
@media (max-width: 760px) {
  .wifi-grid { grid-template-columns: minmax(0, 1fr); }
}

.app-container {
  max-width: 1380px;
  margin: 0 auto;
  width: 100%;
}

.panel {
  display: flex;
  flex-direction: column;
}

.panel-title {
  font-family: var(--font-ui) !important;
  font-size: .82rem !important;
  font-weight: 700 !important;
  letter-spacing: .01em !important;
  text-transform: none !important;
  color: var(--text) !important;
  margin-bottom: 0 !important;
  display: flex;
  align-items: baseline;
  gap: 6px;
}

.panel-title .bar {
  color: var(--accent2);
  font-weight: 700;
  font-size: 0.95rem;
  line-height: 1;
  display: inline-block;
  margin-right: 2px;
}

.panel-inline-sub {
  font-family: var(--font-ui);
  font-size: 0.72rem;
  font-weight: 400;
  color: var(--muted);
  margin-left: 2px;
}

.panel-sub {
  font-family: var(--font-mono);
  font-size: 0.58rem;
  font-weight: 600;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--muted);
  white-space: nowrap;
}

.dashed-empty {
  border: 1px dashed var(--border2);
  border-radius: 5px;
  padding: 13px 12px;
  text-align: center;
  color: var(--muted);
  font-family: var(--font-ui);
  font-size: 0.72rem;
  box-sizing: border-box;
}

.file-tree {
  background: var(--console-bg) !important;
  border: 1px solid var(--border) !important;
  border-radius: 6px !important;
  padding: 10px !important;
  min-height: 195px !important;
  max-height: 240px !important;
  flex: 1;
}

.joy-empty-bay {
  flex: 1;
  min-height: 225px;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  text-align: center;
  padding: 20px 16px;
  background: var(--console-bg);
  border: 1px solid var(--border);
  border-radius: 6px;
  margin: 2px 0 8px;
}
.joy-empty-icon {
  color: var(--muted);
  opacity: 0.65;
  margin-bottom: 10px;
}
.joy-empty-title {
  font-family: var(--font-ui);
  font-size: 0.78rem;
  font-weight: 700;
  letter-spacing: 0.01em;
  color: var(--text);
  text-transform: none;
}
.joy-empty-sub {
  font-size: 0.68rem;
  color: var(--muted);
  max-width: 260px;
  margin-top: 4px;
  line-height: 1.45;
}

/* Channel two-line card layout matching reference */
.channel-row {
  display: flex !important;
  flex-direction: column !important;
  align-items: stretch !important;
  gap: 3px !important;
  padding: 7px 11px !important;
  border: 1px solid var(--border) !important;
  border-left: 2px solid var(--danger) !important;
  border-radius: 5px !important;
  background: var(--console-bg) !important;
  font-size: 0.74rem !important;
}
[data-theme="light"] .channel-row {
  background: #F8FAFC !important;
  border-color: #CBD5E1 !important;
  border-left: 2px solid #EF4444 !important;
}
.channel-row.up {
  border-left-color: var(--ok) !important;
}
.channel-top {
  display: flex !important;
  align-items: center !important;
  justify-content: space-between !important;
  gap: 8px !important;
}
.channel-meta {
  display: block !important;
  border-top: none !important;
  padding-top: 0 !important;
  color: var(--muted) !important;
  font-size: 0.66rem !important;
  font-family: var(--font-mono) !important;
  text-align: left !important;
  padding-left: 15px !important;
}

.topic-list,
.param-list-wrap {
  background: var(--console-bg) !important;
  border: 1px solid var(--border) !important;
  border-radius: 6px !important;
  padding: 10px !important;
  min-height: 195px;
  flex: 1;
}
[data-theme="light"] .topic-list,
[data-theme="light"] .param-list-wrap,
[data-theme="light"] .file-tree,
[data-theme="light"] .console-box {
  background: #F8FAFC !important;
  border-color: #CBD5E1 !important;
}

.grid-tools-term {
  grid-template-columns: minmax(0, 1fr) minmax(0, 1.45fr);
}
@media (max-width: 980px) {
  .grid-tools-term { grid-template-columns: minmax(0, 1fr); }
}

.term-param-col {
  display: flex;
  flex-direction: column;
  gap: 3px;
}
.term-param-label {
  font-family: var(--font-mono);
  font-size: 0.56rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--muted);
}

/* Override Compass container to match uniform panel frame in full-page view */
.compass-panel {
  padding: 14px 16px !important;
  background: var(--panel) !important;
  border: 1px solid var(--border) !important;
  border-radius: 10px !important;
}
.compass-panel .panel-head {
  margin: 0 0 10px 0 !important;
  padding: 0 0 6px 0 !important;
  background: transparent !important;
  border-bottom: 1px solid var(--border) !important;
}
.compass-panel .panel-title {
  font-family: var(--font-ui) !important;
  font-size: 0.82rem !important;
  gap: 6px !important;
}
.compass-panel .panel-title .bar {
  width: auto !important;
  height: auto !important;
  background: transparent !important;
  color: var(--accent2) !important;
  font-size: 0.95rem !important;
  margin-right: 2px !important;
}
.compass-wrap {
  padding: 4px 0 0 !important;
  gap: 12px !important;
  flex: 1;
  justify-content: space-between;
}
.compass-heading-label {
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-size: 0.65rem !important;
  font-weight: 700 !important;
}

/* Virtual Joystick blue accent overrides in dark mode */
:root[data-theme="dark"] .vjoy-thruster-slider,
[data-theme="dark"] .vjoy-thruster-slider {
  background: linear-gradient(to top, #3B82F6 0%, #3B82F6 var(--thruster-pct, 50%), rgba(148, 163, 184, 0.2) var(--thruster-pct, 50%), rgba(148, 163, 184, 0.2) 100%);
}
:root[data-theme="dark"] .vjoy-thruster-slider::-webkit-slider-thumb,
[data-theme="dark"] .vjoy-thruster-slider::-webkit-slider-thumb {
  background: #3B82F6;
  box-shadow: 0 2px 6px rgba(59, 130, 246, 0.4), 0 0 0 3px rgba(59, 130, 246, 0.18);
}
:root[data-theme="dark"] .vjoy-thruster-slider::-moz-range-thumb,
[data-theme="dark"] .vjoy-thruster-slider::-moz-range-thumb {
  background: #3B82F6;
  box-shadow: 0 2px 6px rgba(59, 130, 246, 0.4), 0 0 0 3px rgba(59, 130, 246, 0.18);
}

.gcs-footer {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 12px;
  padding: 14px 4px 8px;
  margin-top: 16px;
  font-family: var(--font-mono);
  font-size: 0.62rem;
  font-weight: 600;
  text-transform: uppercase;
  color: var(--muted);
  border-top: 1px solid var(--border);
  letter-spacing: 0.08em;
}
.footer-brand {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  color: var(--muted);
  font-weight: 600;
}
.footer-meta {
  display: inline-flex;
  align-items: center;
  gap: 8px;
}

/* ---- Technical Panel Tags & Log Badges (Site-Wide) ------------ */
.panel-head-right {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  flex-wrap: wrap;
}
.panel-tag {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 5px;
  height: 22px;
  box-sizing: border-box;
  font-family: var(--font-mono);
  font-size: 0.58rem;
  font-weight: 700;
  letter-spacing: 0.07em;
  text-transform: uppercase;
  padding: 0 8px;
  border-radius: 4px;
  background: linear-gradient(180deg, rgba(30, 41, 59, 0.85) 0%, rgba(15, 23, 42, 0.90) 100%);
  border: 1px solid rgba(56, 189, 248, 0.24);
  color: #93C5FD;
  line-height: 1;
  white-space: nowrap;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.06);
}
.panel-tag::before {
  content: "";
  width: 4.5px;
  height: 4.5px;
  border-radius: 50%;
  background: #38BDF8;
  box-shadow: 0 0 4px rgba(56, 189, 248, 0.65);
  flex-shrink: 0;
}
[data-theme="light"] .panel-tag {
  background: linear-gradient(180deg, #F8FAFC 0%, #EFF6FF 100%);
  border-color: #BFDBFE;
  color: #1E40AF;
  box-shadow: inset 0 1px 0 #FFFFFF;
}
[data-theme="light"] .panel-tag::before {
  background: #2563EB;
  box-shadow: none;
}

/* ---- Precision Command Header Bar (Executive Multi-Layered Architecture) ---- */
.gcs-header {
  position: relative;
  display: grid !important;
  grid-template-columns: 1fr auto 1fr !important;
  align-items: center !important;
  gap: 16px !important;
  padding: 10px 18px !important;
  min-height: 60px !important;
  border-radius: 8px !important;
  margin-bottom: 12px !important;
  background: linear-gradient(180deg, #131D31 0%, #0C1322 100%) !important;
  border: 1px solid rgba(148, 163, 184, 0.20) !important;
  box-shadow:
    0 4px 16px rgba(0, 0, 0, 0.38),
    inset 0 1px 0 rgba(255, 255, 255, 0.07) !important;
}
.gcs-header::before {
  content: "" !important;
  display: block !important;
  position: absolute;
  top: -1px;
  left: 24px;
  right: 24px;
  height: 1px;
  background: linear-gradient(90deg,
    transparent 0%,
    rgba(56, 189, 248, 0.35) 25%,
    rgba(59, 130, 246, 0.60) 50%,
    rgba(56, 189, 248, 0.35) 75%,
    transparent 100%) !important;
  pointer-events: none;
}
[data-theme="light"] .gcs-header {
  background: linear-gradient(180deg, #FFFFFF 0%, #F8FAFC 100%) !important;
  border: 1px solid #CBD5E1 !important;
  box-shadow:
    0 2px 10px rgba(15, 23, 42, 0.06),
    inset 0 1px 0 #FFFFFF !important;
}
[data-theme="light"] .gcs-header::before {
  background: linear-gradient(90deg,
    transparent 0%,
    rgba(37, 99, 235, 0.22) 25%,
    rgba(37, 99, 235, 0.45) 50%,
    rgba(37, 99, 235, 0.22) 75%,
    transparent 100%) !important;
}
[data-theme="black"] .gcs-header {
  background: linear-gradient(180deg, #0B101B 0%, #060911 100%) !important;
  border: 1px solid rgba(255, 255, 255, 0.14) !important;
}
.header-brand::after,
.header-pills::before {
  display: none !important;
  content: none !important;
}

/* Zone 1: Brand Lockup & System Bus Readout (Left) */
.header-brand {
  justify-self: start;
  display: inline-flex !important;
  align-items: center !important;
  gap: 11px !important;
  min-width: 0;
}
.brand-badge {
  width: 38px !important;
  height: 38px !important;
  border-radius: 8px !important;
  background: linear-gradient(145deg, #2563EB 0%, #1E3A8A 55%, #0F172A 100%) !important;
  border: 1px solid #60A5FA !important;
  color: #FFFFFF !important;
  display: inline-flex !important;
  align-items: center !important;
  justify-content: center !important;
  flex-shrink: 0;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.28),
    0 2px 8px rgba(37, 99, 235, 0.32) !important;
}
[data-theme="light"] .brand-badge {
  background: linear-gradient(145deg, #2563EB 0%, #1D4ED8 60%, #1E3A8A 100%) !important;
  border: 1px solid #1D4ED8 !important;
  color: #FFFFFF !important;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.30),
    0 2px 6px rgba(37, 99, 235, 0.26) !important;
}
.brand-badge svg {
  width: 20px !important;
  height: 20px !important;
  stroke-width: 2 !important;
}
.brand-text {
  display: flex !important;
  flex-direction: column !important;
  justify-content: center !important;
  gap: 3px !important;
}
.brand-title {
  font-family: var(--font-ui) !important;
  font-size: 0.90rem !important;
  font-weight: 800 !important;
  letter-spacing: 0.04em !important;
  color: var(--text) !important;
  line-height: 1.1 !important;
  display: inline-flex !important;
  align-items: center !important;
  gap: 7px !important;
}
.brand-tag {
  margin-left: 0 !important;
  padding: 2px 7px !important;
  font-family: var(--font-mono) !important;
  font-size: 0.56rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.08em !important;
  border-radius: 4px !important;
  background: rgba(56, 189, 248, 0.14) !important;
  border: 1px solid rgba(56, 189, 248, 0.38) !important;
  color: #38BDF8 !important;
  line-height: 1.1 !important;
  display: inline-flex !important;
  align-items: center !important;
  gap: 4px !important;
}
.brand-tag::before {
  content: "";
  width: 4px;
  height: 4px;
  border-radius: 50%;
  background: currentColor;
}
[data-theme="light"] .brand-tag {
  background: #EFF6FF !important;
  border-color: #93C5FD !important;
  color: #1D4ED8 !important;
}
.brand-subtitle {
  font-family: var(--font-mono) !important;
  font-size: 0.56rem !important;
  font-weight: 600 !important;
  letter-spacing: 0.10em !important;
  color: var(--muted) !important;
  text-transform: uppercase !important;
  line-height: 1.1 !important;
}
.header-brand-sep {
  width: 1px;
  height: 28px;
  background: linear-gradient(180deg, transparent, var(--border2), transparent);
  margin: 0 2px;
  flex-shrink: 0;
}
.header-sys-meta {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  padding: 4px 10px;
  border-radius: 6px;
  background: linear-gradient(180deg, #152034 0%, #0E1625 100%);
  border: 1px solid rgba(56, 189, 248, 0.22);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.05);
}
[data-theme="light"] .header-sys-meta {
  background: linear-gradient(180deg, #F8FAFC 0%, #EFF6FF 100%);
  border-color: #BFDBFE;
  box-shadow: inset 0 1px 0 #FFFFFF;
}
.header-sys-icon {
  width: 22px;
  height: 22px;
  border-radius: 5px;
  background: rgba(56, 189, 248, 0.12);
  border: 1px solid rgba(56, 189, 248, 0.28);
  color: #38BDF8;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
}
[data-theme="light"] .header-sys-icon {
  background: #DBEAFE;
  border-color: #93C5FD;
  color: #1D4ED8;
}
.header-sys-icon svg {
  width: 12px;
  height: 12px;
  stroke: currentColor;
  stroke-width: 2;
  fill: none;
}
.header-sys-col {
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 2px;
}
.header-sys-label {
  font-family: var(--font-ui);
  font-size: 0.49rem;
  font-weight: 700;
  letter-spacing: 0.09em;
  color: var(--muted);
  text-transform: uppercase;
  line-height: 1;
}
.header-sys-val {
  font-family: var(--font-mono);
  font-size: 0.62rem;
  font-weight: 700;
  letter-spacing: 0.04em;
  color: var(--text);
  line-height: 1.1;
}
@media (max-width: 1320px) {
  .header-brand-sep,
  .header-sys-meta {
    display: none;
  }
}

/* Zone 2: Recessed Segmented Navigation Dock (Center) */
.header-nav {
  justify-self: center;
  display: inline-flex !important;
  align-items: center !important;
  height: 36px !important;
  padding: 3px !important;
  gap: 3px !important;
  border-radius: 7px !important;
  background: #080D17 !important;
  border: 1px solid rgba(148, 163, 184, 0.18) !important;
  box-shadow: inset 0 1px 3px rgba(0, 0, 0, 0.45) !important;
}
[data-theme="light"] .header-nav {
  background: #E2E8F0 !important;
  border: 1px solid #CBD5E1 !important;
  box-shadow: inset 0 1px 2px rgba(15, 23, 42, 0.06) !important;
}
.header-nav .top-tab,
.top-tabs .top-tab {
  height: 28px !important;
  padding: 0 14px !important;
  border: 1px solid transparent !important;
  border-radius: 5px !important;
  background: transparent !important;
  color: var(--muted) !important;
  font-family: var(--font-ui) !important;
  font-size: 0.68rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.05em !important;
  text-transform: uppercase !important;
  cursor: pointer;
  display: inline-flex !important;
  align-items: center !important;
  justify-content: center !important;
  gap: 6px !important;
  white-space: nowrap;
  box-shadow: none !important;
  transition: all 0.14s ease !important;
}
.top-tab-svg {
  width: 12px;
  height: 12px;
  stroke: currentColor;
  stroke-width: 2;
  fill: none;
  flex-shrink: 0;
  opacity: 0.9;
}
.header-nav .top-tab:hover,
.top-tabs .top-tab:hover {
  color: var(--text) !important;
  background: rgba(148, 163, 184, 0.10) !important;
}
[data-theme="light"] .header-nav .top-tab:hover,
[data-theme="light"] .top-tabs .top-tab:hover {
  color: #0F172A !important;
  background: rgba(255, 255, 255, 0.70) !important;
}
.header-nav .top-tab.active,
.top-tabs .top-tab.active {
  color: #FFFFFF !important;
  background: linear-gradient(180deg, #2563EB 0%, #1D4ED8 100%) !important;
  border-color: #60A5FA !important;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.24),
    0 2px 8px rgba(37, 99, 235, 0.36) !important;
}
[data-theme="light"] .header-nav .top-tab.active,
[data-theme="light"] .top-tabs .top-tab.active {
  color: #FFFFFF !important;
  background: linear-gradient(180deg, #2563EB 0%, #1D4ED8 100%) !important;
  border-color: #1D4ED8 !important;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.26),
    0 2px 6px rgba(37, 99, 235, 0.28) !important;
}
.top-tab .tab-indicator {
  width: 5px !important;
  height: 5px !important;
  border-radius: 50% !important;
  background: currentColor !important;
  opacity: 0.55 !important;
  box-shadow: none !important;
  animation: none !important;
}
.header-nav .top-tab.active .tab-indicator,
.top-tabs .top-tab.active .tab-indicator {
  background: #93C5FD !important;
  opacity: 1 !important;
  box-shadow: 0 0 6px rgba(147, 197, 253, 0.85) !important;
}

/* Zone 3: Recessed Telemetry Status Tray & Theme Switcher (Right) */
.header-pills {
  justify-self: end;
  display: inline-flex !important;
  align-items: center !important;
  gap: 8px !important;
  flex-wrap: nowrap !important;
}
.header-status-tray {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 3px;
  border-radius: 7px;
  background: #080D17;
  border: 1px solid rgba(148, 163, 184, 0.16);
  box-shadow: inset 0 1px 3px rgba(0, 0, 0, 0.40);
}
[data-theme="light"] .header-status-tray {
  background: #E2E8F0;
  border-color: #CBD5E1;
  box-shadow: inset 0 1px 2px rgba(15, 23, 42, 0.05);
}
/* Uniform Architectural Telemetry Status Cells (No Pastel Multi-Color Fills) */
.pill,
.pill.ok,
.pill.danger,
.pill.warn,
.pill.accent {
  height: 28px !important;
  padding: 0 10px !important;
  border-radius: 5px !important;
  font-family: var(--font-mono) !important;
  font-size: 0.62rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.05em !important;
  text-transform: uppercase !important;
  line-height: 1 !important;
  display: inline-flex !important;
  flex-direction: row !important;
  align-items: center !important;
  justify-content: center !important;
  gap: 6px !important;
  border: 1px solid #243247 !important;
  border-left: 2.5px solid #64748B !important;
  background: linear-gradient(180deg, #162032 0%, #0F1726 100%) !important;
  color: #E2ECF7 !important;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.05) !important;
  white-space: nowrap !important;
}
.pill-svg {
  width: 11.5px;
  height: 11.5px;
  stroke: #94A3B8;
  stroke-width: 2.1;
  fill: none;
  flex-shrink: 0;
  order: 1;
}
#modePill {
  text-transform: uppercase !important;
  letter-spacing: 0.05em !important;
}
.pill .dot {
  width: 5.5px !important;
  height: 5.5px !important;
  border-radius: 50% !important;
  background: #64748B !important;
  box-shadow: none !important;
  opacity: 1 !important;
  flex-shrink: 0;
  order: 3;
  margin-left: 2px;
}
.pill.ok {
  border-left-color: #10B981 !important;
}
.pill.ok .pill-svg {
  stroke: #34D399 !important;
}
.pill.ok .dot {
  background: #10B981 !important;
  box-shadow: 0 0 6px rgba(16, 185, 129, 0.65) !important;
}
.pill.danger {
  border-left-color: #F43F5E !important;
}
.pill.danger .pill-svg {
  stroke: #FB7185 !important;
}
.pill.danger .dot {
  background: #F43F5E !important;
  box-shadow: 0 0 5px rgba(244, 63, 94, 0.55) !important;
}
.pill.warn {
  border-left-color: #F59E0B !important;
}
.pill.warn .pill-svg {
  stroke: #FBBF24 !important;
}
.pill.warn .dot {
  background: #FBBF24 !important;
  box-shadow: 0 0 5px rgba(245, 158, 11, 0.45) !important;
}
.pill.accent {
  border-left-color: #38BDF8 !important;
}
.pill.accent .pill-svg {
  stroke: #38BDF8 !important;
}
.pill.accent .dot {
  background: #38BDF8 !important;
  box-shadow: 0 0 5px rgba(56, 189, 248, 0.55) !important;
}
[data-theme="light"] .pill,
[data-theme="light"] .pill.ok,
[data-theme="light"] .pill.danger,
[data-theme="light"] .pill.warn,
[data-theme="light"] .pill.accent {
  background: linear-gradient(180deg, #FFFFFF 0%, #F8FAFC 100%) !important;
  border: 1px solid #CBD5E1 !important;
  border-left: 2.5px solid #94A3B8 !important;
  color: #1E293B !important;
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04), inset 0 1px 0 #FFFFFF !important;
}
[data-theme="light"] .pill .pill-svg {
  stroke: #64748B !important;
}
[data-theme="light"] .pill.ok {
  border-left-color: #059669 !important;
}
[data-theme="light"] .pill.ok .pill-svg {
  stroke: #059669 !important;
}
[data-theme="light"] .pill.ok .dot {
  background: #059669 !important;
  box-shadow: none !important;
}
[data-theme="light"] .pill.danger {
  border-left-color: #E11D48 !important;
}
[data-theme="light"] .pill.danger .pill-svg {
  stroke: #E11D48 !important;
}
[data-theme="light"] .pill.danger .dot {
  background: #E11D48 !important;
  box-shadow: none !important;
}
[data-theme="light"] .pill.warn {
  border-left-color: #D97706 !important;
}
[data-theme="light"] .pill.warn .pill-svg {
  stroke: #D97706 !important;
}
[data-theme="light"] .pill.warn .dot {
  background: #D97706 !important;
  box-shadow: none !important;
}
[data-theme="light"] .pill.accent {
  border-left-color: #2563EB !important;
}
[data-theme="light"] .pill.accent .pill-svg {
  stroke: #2563EB !important;
}
[data-theme="light"] .pill.accent .dot {
  background: #2563EB !important;
  box-shadow: none !important;
}

.header-pill-sep {
  width: 1px;
  height: 22px;
  background: var(--border2);
  margin: 0 2px;
  flex-shrink: 0;
}
[data-theme="light"] .header-pill-sep {
  background: #CBD5E1;
}

/* Zone 4: Machined Theme Switch Button (Sun <-> Moon) */
.theme-circle-btn {
  position: relative;
  width: 34px !important;
  height: 34px !important;
  min-width: 34px !important;
  min-height: 34px !important;
  padding: 0 !important;
  margin: 0 !important;
  border-radius: 7px !important;
  display: inline-flex !important;
  align-items: center !important;
  justify-content: center !important;
  cursor: pointer;
  flex-shrink: 0;
  outline: none;
  user-select: none;
  overflow: hidden;
  background: linear-gradient(180deg, #1B263B 0%, #111927 100%) !important;
  border: 1px solid #2A3B54 !important;
  color: #E2E8F0 !important;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.08),
    0 1px 3px rgba(0, 0, 0, 0.32) !important;
  transition: all 0.16s ease;
}
.theme-circle-btn:hover {
  background: linear-gradient(180deg, #24334F 0%, #172338 100%) !important;
  border-color: #38BDF8 !important;
  color: #FFFFFF !important;
}
.theme-circle-btn:active {
  transform: scale(0.95);
}

[data-theme="light"] .theme-circle-btn {
  background: linear-gradient(180deg, #FFFFFF 0%, #F1F5F9 100%) !important;
  border: 1px solid #CBD5E1 !important;
  color: #1E293B !important;
  box-shadow:
    inset 0 1px 0 #FFFFFF,
    0 1px 2px rgba(15, 23, 42, 0.06) !important;
}
[data-theme="light"] .theme-circle-btn:hover {
  background: linear-gradient(180deg, #EFF6FF 0%, #DBEAFE 100%) !important;
  border-color: #60A5FA !important;
  color: #1D4ED8 !important;
}

.theme-circle-btn .theme-svg {
  position: absolute;
  width: 16px !important;
  height: 16px !important;
  stroke: currentColor;
  stroke-width: 2.1;
  stroke-linecap: round;
  stroke-linejoin: round;
  fill: none;
  pointer-events: none;
  transition: transform 0.28s cubic-bezier(0.22, 1, 0.36, 1), opacity 0.2s ease;
}
.theme-circle-btn .theme-svg-sun {
  opacity: 1;
  transform: rotate(0deg) scale(1);
}
.theme-circle-btn .theme-svg-moon {
  opacity: 0;
  transform: rotate(75deg) scale(0.45);
}
[data-theme="light"] .theme-circle-btn .theme-svg-sun {
  opacity: 0;
  transform: rotate(-75deg) scale(0.45);
}
[data-theme="light"] .theme-circle-btn .theme-svg-moon {
  opacity: 1;
  transform: rotate(0deg) scale(1);
}

@media (max-width: 1080px) {
  .gcs-header {
    grid-template-columns: 1fr auto !important;
  }
  .header-pills {
    grid-column: 1 / -1;
    justify-self: start;
    flex-wrap: wrap !important;
  }
}

/* ---- Viewport-Proportioned Page Layout & Whole-Site Polish ---- */
body {
  padding: clamp(10px, 1.1vw, 16px) clamp(12px, 1.5vw, 22px) 28px !important;
}
.app-container {
  max-width: 1680px !important;
  width: 100% !important;
  margin: 0 auto !important;
}
.grid-main {
  display: grid !important;
  grid-template-columns: 1.12fr 1.04fr 1fr !important;
  gap: 12px !important;
  align-items: stretch !important;
}
.grid-bottom {
  display: grid !important;
  grid-template-columns: repeat(3, minmax(0, 1fr)) !important;
  gap: 12px !important;
  margin-top: 12px !important;
  align-items: stretch !important;
}
@media (max-width: 1200px) {
  .grid-main,
  .grid-bottom {
    grid-template-columns: 1fr 1fr !important;
  }
}
@media (max-width: 800px) {
  .grid-main,
  .grid-bottom {
    grid-template-columns: 1fr !important;
  }
}

/* ---- Precision Panel & Instrumentation Finishing -------------- */
.panel,
.compass-panel {
  background: linear-gradient(180deg, #0F1726 0%, #0C1220 100%) !important;
  border: 1px solid rgba(148, 163, 184, 0.14) !important;
  border-radius: 7px !important;
  padding: 14px 16px !important;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.28), inset 0 1px 0 rgba(255, 255, 255, 0.03) !important;
}
[data-theme="light"] .panel,
[data-theme="light"] .compass-panel {
  background: #FFFFFF !important;
  border: 1px solid #CBD5E1 !important;
  box-shadow: 0 1px 3px rgba(15, 23, 42, 0.05), 0 4px 12px rgba(15, 23, 42, 0.025) !important;
}
[data-theme="black"] .panel,
[data-theme="black"] .compass-panel {
  background: #080B11 !important;
  border: 1px solid rgba(255, 255, 255, 0.12) !important;
}

.panel-head,
.compass-panel .panel-head {
  display: flex !important;
  align-items: center !important;
  justify-content: space-between !important;
  gap: 8px !important;
  min-height: 28px !important;
  margin: 0 0 10px 0 !important;
  padding: 0 0 8px 0 !important;
  border-bottom: 1px solid var(--border) !important;
  background: transparent !important;
}
.panel-title,
.compass-panel .panel-title {
  font-family: var(--font-ui) !important;
  font-size: 0.78rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.03em !important;
  text-transform: uppercase !important;
  color: var(--text) !important;
  display: inline-flex !important;
  align-items: center !important;
  gap: 7px !important;
  line-height: 1.2 !important;
}
/* Uniform machined 3x14px vertical accent bar + 22x22px panel symbol badge */
.panel-title .bar,
.compass-panel .panel-title .bar {
  display: inline-block !important;
  width: 3px !important;
  height: 14px !important;
  border-radius: 1.5px !important;
  background: linear-gradient(180deg, #38BDF8 0%, #2563EB 100%) !important;
  font-size: 0 !important;
  color: transparent !important;
  margin-right: 0 !important;
  flex-shrink: 0 !important;
}
.panel-title-icon {
  width: 22px;
  height: 22px;
  border-radius: 5px;
  background: linear-gradient(180deg, rgba(37, 99, 235, 0.20) 0%, rgba(30, 58, 138, 0.14) 100%);
  border: 1px solid rgba(56, 189, 248, 0.32);
  color: #38BDF8;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.08);
}
[data-theme="light"] .panel-title-icon {
  background: linear-gradient(180deg, #EFF6FF 0%, #DBEAFE 100%);
  border-color: #93C5FD;
  color: #1D4ED8;
  box-shadow: inset 0 1px 0 #FFFFFF;
}
.panel-title-icon svg {
  width: 12px;
  height: 12px;
  stroke: currentColor;
  stroke-width: 2;
  stroke-linecap: round;
  stroke-linejoin: round;
  fill: none;
}

/* ---- Executive Custom Select Dropdown Component (All Themes) ----
   Replaces ugly OS native gray <select> popup with a floating machined menu */
.gcs-select-wrap {
  position: relative;
  width: 100%;
  user-select: none;
  z-index: 40;
}
.gcs-select-wrap.open,
.gcs-select-wrap.is-open {
  z-index: 200;
}
.gcs-select-native {
  position: absolute !important;
  width: 1px !important;
  height: 1px !important;
  padding: 0 !important;
  margin: -1px !important;
  overflow: hidden !important;
  clip: rect(0, 0, 0, 0) !important;
  white-space: nowrap !important;
  border: 0 !important;
  opacity: 0 !important;
  pointer-events: none !important;
}
.gcs-select-trigger {
  width: 100%;
  min-height: 40px;
  padding: 6px 10px;
  border-radius: 7px;
  background: linear-gradient(180deg, #162033 0%, #0E1626 100%);
  border: 1px solid #2A3B54;
  color: #F8FAFC;
  font-family: var(--font-ui);
  font-size: 0.76rem;
  font-weight: 600;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  cursor: pointer;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.06),
    0 1px 2px rgba(0, 0, 0, 0.25);
  transition: all 0.15s ease;
  text-align: left;
}
.gcs-select-trigger:hover {
  background: linear-gradient(180deg, #1C2942 0%, #121C30 100%);
  border-color: #38BDF8;
}
.gcs-select-wrap.open .gcs-select-trigger,
.gcs-select-wrap.is-open .gcs-select-trigger {
  border-color: #38BDF8;
  box-shadow:
    0 0 0 3px rgba(56, 189, 248, 0.20),
    inset 0 1px 0 rgba(255, 255, 255, 0.10);
}
[data-theme="light"] .gcs-select-trigger {
  background: linear-gradient(180deg, #FFFFFF 0%, #F8FAFC 100%);
  border: 1px solid #CBD5E1;
  color: #0F172A;
  box-shadow:
    inset 0 1px 0 #FFFFFF,
    0 1px 2px rgba(15, 23, 42, 0.05);
}
[data-theme="light"] .gcs-select-trigger:hover {
  background: linear-gradient(180deg, #FFFFFF 0%, #EFF6FF 100%);
  border-color: #3B82F6;
}
[data-theme="light"] .gcs-select-wrap.open .gcs-select-trigger,
[data-theme="light"] .gcs-select-wrap.is-open .gcs-select-trigger {
  background: #FFFFFF;
  border-color: #2563EB;
  box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.15);
}
.gcs-select-trigger-left {
  display: flex;
  align-items: center;
  gap: 9px;
  min-width: 0;
  flex: 1;
  text-align: left;
}
.gcs-select-trigger-right {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  flex-shrink: 0;
}
.gcs-select-icon {
  width: 24px;
  height: 24px;
  border-radius: 5px;
  background: rgba(56, 189, 248, 0.14);
  border: 1px solid rgba(56, 189, 248, 0.32);
  color: #38BDF8;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
}
[data-theme="light"] .gcs-select-icon {
  background: #EFF6FF;
  border-color: #BFDBFE;
  color: #2563EB;
}
.gcs-select-icon svg {
  width: 13px;
  height: 13px;
  stroke: currentColor;
  stroke-width: 2.2;
  fill: none;
}
.gcs-select-text-col {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  justify-content: center;
  gap: 1px;
  min-width: 0;
  flex: 1;
}
.gcs-select-val {
  font-family: var(--font-mono);
  font-size: 0.76rem;
  font-weight: 700;
  color: var(--text);
  line-height: 1.2;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  max-width: 100%;
}
.gcs-select-sub {
  font-family: var(--font-ui);
  font-size: 0.62rem;
  font-weight: 500;
  color: var(--muted);
  line-height: 1.15;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  max-width: 100%;
}
.gcs-select-badge {
  font-family: var(--font-mono);
  font-size: 0.56rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  padding: 2.5px 6.5px;
  border-radius: 4px;
  background: rgba(56, 189, 248, 0.14);
  border: 1px solid rgba(56, 189, 248, 0.32);
  color: #38BDF8;
  flex-shrink: 0;
}
[data-theme="light"] .gcs-select-badge {
  background: #EFF6FF;
  border-color: #93C5FD;
  color: #1D4ED8;
}
.gcs-select-chevron-box {
  width: 22px;
  height: 22px;
  border-radius: 4px;
  background: rgba(255, 255, 255, 0.05);
  border: 1px solid rgba(255, 255, 255, 0.10);
  display: inline-flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  transition: all 0.15s ease;
}
[data-theme="light"] .gcs-select-chevron-box {
  background: #F1F5F9;
  border-color: #CBD5E1;
}
.gcs-select-trigger:hover .gcs-select-chevron-box {
  border-color: rgba(56, 189, 248, 0.45);
  background: rgba(56, 189, 248, 0.12);
}
[data-theme="light"] .gcs-select-trigger:hover .gcs-select-chevron-box {
  border-color: #93C5FD;
  background: #DBEAFE;
}
.gcs-select-chevron {
  width: 13px;
  height: 13px;
  stroke: var(--text2);
  stroke-width: 2.4;
  fill: none;
  flex-shrink: 0;
  transition: transform 0.18s cubic-bezier(0.22, 1, 0.36, 1), stroke 0.15s ease;
}
.gcs-select-wrap.open .gcs-select-chevron,
.gcs-select-wrap.is-open .gcs-select-chevron {
  transform: rotate(180deg);
  stroke: var(--accent2);
}
.gcs-select-menu {
  position: absolute;
  top: calc(100% + 5px);
  left: 0;
  right: 0;
  background: #0D1526;
  border: 1px solid #38BDF8;
  border-radius: 8px;
  padding: 5px;
  box-shadow:
    0 14px 32px rgba(0, 0, 0, 0.65),
    0 0 0 1px rgba(56, 189, 248, 0.18);
  display: none;
  flex-direction: column;
  gap: 3px;
  max-height: 260px;
  overflow-y: auto;
}
.gcs-select-wrap.open .gcs-select-menu,
.gcs-select-wrap.is-open .gcs-select-menu {
  display: flex;
  animation: gcsSelectFadeIn 0.14s cubic-bezier(0.22, 1, 0.36, 1);
}
@keyframes gcsSelectFadeIn {
  from { opacity: 0; transform: translateY(-4px); }
  to   { opacity: 1; transform: translateY(0); }
}
[data-theme="light"] .gcs-select-menu {
  background: #FFFFFF;
  border: 1px solid #60A5FA;
  box-shadow:
    0 12px 28px rgba(15, 23, 42, 0.16),
    0 2px 6px rgba(15, 23, 42, 0.08);
}
.gcs-select-menu-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 4px 8px 5px;
  margin-bottom: 2px;
  border-bottom: 1px solid var(--border);
  font-family: var(--font-mono);
  font-size: 0.56rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--muted);
}
.gcs-select-opt {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 7px 10px;
  border-radius: 6px;
  border: 1px solid transparent;
  font-family: var(--font-mono);
  font-size: 0.73rem;
  font-weight: 600;
  color: #E2E8F0;
  cursor: pointer;
  transition: all 0.12s ease;
}
.gcs-select-opt:hover {
  background: rgba(56, 189, 248, 0.12);
  border-color: rgba(56, 189, 248, 0.28);
  color: #FFFFFF;
}
.gcs-select-opt.selected,
.gcs-select-opt.is-selected {
  background: linear-gradient(180deg, rgba(37, 99, 235, 0.28) 0%, rgba(29, 78, 216, 0.20) 100%);
  border-color: rgba(56, 189, 248, 0.55);
  color: #FFFFFF;
  font-weight: 700;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.12);
}
[data-theme="light"] .gcs-select-opt {
  color: #0F172A;
}
[data-theme="light"] .gcs-select-opt:hover {
  background: #F1F5F9;
  border-color: #CBD5E1;
  color: #0F172A;
}
[data-theme="light"] .gcs-select-opt.selected,
[data-theme="light"] .gcs-select-opt.is-selected {
  background: linear-gradient(180deg, #EFF6FF 0%, #DBEAFE 100%);
  border-color: #3B82F6;
  color: #1E3A8A;
}
.gcs-select-opt-left {
  display: flex;
  align-items: center;
  gap: 9px;
  min-width: 0;
  flex: 1;
}
.gcs-select-opt-info {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 1px;
  min-width: 0;
}
.gcs-select-opt-label {
  font-family: var(--font-mono);
  font-size: 0.74rem;
  font-weight: 700;
  line-height: 1.2;
  color: inherit;
}
.gcs-select-opt-sub {
  font-family: var(--font-ui);
  font-size: 0.62rem;
  font-weight: 500;
  color: var(--muted);
  line-height: 1.15;
}
[data-theme="light"] .gcs-select-opt.is-selected .gcs-select-opt-sub,
[data-theme="light"] .gcs-select-opt.selected .gcs-select-opt-sub {
  color: #1D4ED8;
}
.gcs-select-opt-right {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  flex-shrink: 0;
}
.gcs-select-opt-pill {
  font-family: var(--font-mono);
  font-size: 0.54rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  padding: 2px 5.5px;
  border-radius: 3.5px;
  border: 1px solid var(--border);
  background: rgba(255, 255, 255, 0.04);
  color: var(--muted);
}
.gcs-select-opt-pill.online {
  color: #34D399;
  background: rgba(16, 185, 129, 0.14);
  border-color: rgba(16, 185, 129, 0.35);
}
.gcs-select-opt-pill.auto {
  color: #38BDF8;
  background: rgba(56, 189, 248, 0.14);
  border-color: rgba(56, 189, 248, 0.35);
}
[data-theme="light"] .gcs-select-opt-pill {
  background: #F1F5F9;
  border-color: #CBD5E1;
  color: #475569;
}
[data-theme="light"] .gcs-select-opt-pill.online {
  color: #047857;
  background: #ECFDF5;
  border-color: #6EE7B7;
}
[data-theme="light"] .gcs-select-opt-pill.auto {
  color: #1D4ED8;
  background: #EFF6FF;
  border-color: #93C5FD;
}
.gcs-select-opt-dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: #64748B;
  flex-shrink: 0;
}
.gcs-select-opt-dot.up {
  background: #10B981;
  box-shadow: 0 0 6px rgba(16, 185, 129, 0.7);
}
.gcs-select-opt-dot.auto {
  background: #38BDF8;
  box-shadow: 0 0 6px rgba(56, 189, 248, 0.7);
}
[data-theme="light"] .gcs-select-opt-dot.auto {
  background: #2563EB;
  box-shadow: 0 0 5px rgba(37, 99, 235, 0.45);
}
.gcs-select-opt-check {
  width: 14px;
  height: 14px;
  stroke: #38BDF8;
  stroke-width: 2.5;
  fill: none;
  flex-shrink: 0;
  opacity: 0;
}
[data-theme="light"] .gcs-select-opt-check {
  stroke: #2563EB;
}
.gcs-select-opt.selected .gcs-select-opt-check,
.gcs-select-opt.is-selected .gcs-select-opt-check {
  opacity: 1;
}

/* ---- Executive Structured Data Tables (Topics, Params, Files) ---- */
.gcs-table-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 5px 10px;
  margin-bottom: 5px;
  border-radius: 4px;
  background: rgba(15, 23, 42, 0.65);
  border: 1px solid var(--border);
  font-family: var(--font-ui);
  font-size: 0.58rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--muted);
}
[data-theme="light"] .gcs-table-head {
  background: #E2E8F0;
  border-color: #CBD5E1;
  color: #475569;
}
.topic-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  padding: 6px 10px !important;
  border-radius: 4px !important;
  border-bottom: 1px solid rgba(148, 163, 184, 0.08);
  transition: background 0.12s ease;
}
.topic-row:nth-child(odd) {
  background: rgba(30, 41, 59, 0.28) !important;
}
.topic-row:hover {
  background: rgba(56, 189, 248, 0.10) !important;
}
[data-theme="light"] .topic-row {
  border-bottom-color: #E2E8F0;
}
[data-theme="light"] .topic-row:nth-child(odd) {
  background: #FFFFFF !important;
}
[data-theme="light"] .topic-row:hover {
  background: #EFF6FF !important;
}
.topic-types {
  padding: 2px 6px;
  border-radius: 3.5px;
  background: rgba(56, 189, 248, 0.10);
  border: 1px solid rgba(56, 189, 248, 0.24);
  color: #38BDF8 !important;
  font-size: 0.63rem !important;
  font-weight: 600;
}
[data-theme="light"] .topic-types {
  background: #EFF6FF;
  border-color: #BFDBFE;
  color: #1D4ED8 !important;
}
.param-node-group {
  border: 1px solid var(--border);
  border-radius: 6px;
  overflow: hidden;
  margin: 0 0 10px 0 !important;
  background: rgba(15, 23, 42, 0.35);
}
[data-theme="light"] .param-node-group {
  background: #FFFFFF;
  border-color: #CBD5E1;
}
.param-node-header {
  padding: 6px 10px !important;
  margin-bottom: 0 !important;
  background: rgba(30, 41, 59, 0.65);
  border-bottom: 1px solid var(--border);
  display: flex;
  align-items: center;
  gap: 6px;
}
[data-theme="light"] .param-node-header {
  background: #F1F5F9;
  border-bottom-color: #CBD5E1;
  color: #1E3A8A;
}
.param-row {
  padding: 6px 10px !important;
  border-bottom: 1px solid rgba(148, 163, 184, 0.08);
}
.param-row:last-child {
  border-bottom: none;
}
[data-theme="light"] .param-row {
  border-bottom-color: #F1F5F9;
}
.file-row {
  border-bottom: 1px solid rgba(148, 163, 184, 0.08);
  padding: 6px 8px !important;
}
[data-theme="light"] .file-row {
  border-bottom-color: #E2E8F0;
}
.tree-svg {
  width: 13px;
  height: 13px;
  stroke: var(--accent2);
  stroke-width: 2;
  fill: none;
  vertical-align: -2px;
  margin-right: 5px;
}

/* ---- Group Label Divider Fix & Panel Interior Precision -------- */
.group-label {
  display: flex !important;
  align-items: center !important;
  gap: 8px !important;
  font-family: var(--font-ui) !important;
  font-size: 0.65rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.06em !important;
  text-transform: uppercase !important;
  color: var(--muted) !important;
  margin: 10px 0 6px !important;
}
.group-label:first-child {
  margin-top: 0 !important;
}
/* Place the horizontal hairline BETWEEN the label text and any right-hand badge/button */
.group-label::after {
  content: "" !important;
  flex: 1 1 auto !important;
  height: 1px !important;
  background: var(--border) !important;
  order: 1 !important;
}
.group-label > * {
  order: 2 !important;
  margin-left: 0 !important;
  flex-shrink: 0 !important;
}

/* Telemetry Panel Interior Precision */
.telem-hero-strip {
  border-radius: 5px !important;
  border: 1px solid var(--border) !important;
}
.telem-hero-bay {
  padding: 10px 14px !important;
}
.telem-ribbon {
  border-radius: 5px !important;
}
.telem-ribbon-cell {
  padding: 7px 11px !important;
}
.orient3d-wrap {
  flex: 1 1 auto;
  display: flex;
  flex-direction: column;
  margin-top: 8px !important;
  min-height: 152px;
}
.orient3d-scene {
  flex: 1 1 auto;
  min-height: 152px !important;
  height: auto !important;
  border-radius: 5px !important;
}
.leak-row {
  border-radius: 5px !important;
}
.leak-sensor-card {
  padding: 7px 10px !important;
}

/* Commands Panel Edge-to-Edge Button Grids & Diagnostic Strip */
.cmd-group + .cmd-group {
  border-top: 1px solid var(--border) !important;
  padding-top: 8px !important;
  margin-top: 9px !important;
}
.cmd-group .btn-group {
  display: grid !important;
  grid-template-columns: repeat(auto-fit, minmax(86px, 1fr)) !important;
  gap: 6px !important;
  margin-top: 6px !important;
  width: 100%;
}
.cmd-group .btn-group .btn {
  width: 100%;
}
.estop-strip {
  display: grid !important;
  grid-template-columns: 1.15fr 1fr !important;
  gap: 8px !important;
  padding: 8px 10px !important;
  border-radius: 6px !important;
  border: 1px solid var(--border) !important;
  background: var(--bg2) !important;
}
[data-theme="light"] .estop-strip {
  border-color: #CBD5E1 !important;
  background: #F8FAFC !important;
}
.estop-strip .btn {
  width: 100%;
  min-height: 31px !important;
}
#depthHoldStatus {
  padding: 6px 10px !important;
  border-radius: 4px !important;
  background: var(--bg2) !important;
  border: 1px solid var(--border) !important;
  border-left: 2.5px solid var(--accent2) !important;
  color: var(--muted) !important;
  margin-top: 6px !important;
  font-size: 0.65rem !important;
}
[data-theme="light"] #depthHoldStatus {
  background: #F8FAFC !important;
  border-color: #CBD5E1 !important;
  border-left-color: #2563EB !important;
  color: #475569 !important;
}

/* File Storage Panel Custom File Input & Centered Directory Bay */
.file-upload-row {
  display: flex;
  gap: 6px;
  align-items: stretch;
  margin-bottom: 6px;
}
#fileInput {
  flex: 1;
  min-width: 0;
  background: var(--bg2) !important;
  border: 1px solid var(--border) !important;
  color: var(--muted) !important;
  padding: 3px 8px 3px 4px !important;
  border-radius: 4px !important;
  font-family: var(--font-mono) !important;
  font-size: 0.68rem !important;
  display: inline-flex;
  align-items: center;
}
[data-theme="light"] #fileInput {
  background: #F8FAFC !important;
  border-color: #CBD5E1 !important;
  color: #475569 !important;
}
input[type="file"]::file-selector-button {
  font-family: var(--font-mono) !important;
  font-size: 0.63rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.04em !important;
  text-transform: uppercase !important;
  padding: 4px 10px !important;
  margin-right: 10px !important;
  border: 1px solid var(--border2) !important;
  border-radius: 3px !important;
  background: #182234 !important;
  color: var(--text) !important;
  cursor: pointer !important;
  transition: all 0.12s ease !important;
}
input[type="file"]::file-selector-button:hover {
  border-color: var(--accent2) !important;
  background: #223049 !important;
  color: #FFFFFF !important;
}
[data-theme="light"] input[type="file"]::file-selector-button {
  background: #FFFFFF !important;
  border-color: #CBD5E1 !important;
  color: #1E293B !important;
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04) !important;
}
[data-theme="light"] input[type="file"]::file-selector-button:hover {
  background: #F1F5F9 !important;
  border-color: #94A3B8 !important;
  color: #0F172A !important;
}

.file-tree {
  display: flex !important;
  flex-direction: column !important;
  flex: 1 1 auto !important;
  min-height: 220px !important;
  max-height: 290px !important;
  background: var(--console-bg) !important;
  border: 1px solid var(--border) !important;
  border-radius: 5px !important;
  padding: 10px !important;
}
.file-tree .dashed-empty {
  flex: 1 1 auto;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 6px;
  border: 1px dashed var(--border2);
  border-radius: 5px;
  padding: 20px 14px;
  text-align: center;
  color: var(--muted);
}

/* ---- Physical Joystick Bay (Premium Empty State + Live Visualizer) --- */
.joy-empty-bay {
  flex: 1 1 auto !important;
  min-height: 232px !important;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  text-align: center;
  padding: 24px 18px !important;
  background: #0C1322 !important;
  border: 1px solid rgba(148, 163, 184, 0.14) !important;
  border-radius: 6px !important;
  margin: 2px 0 10px !important;
  box-shadow: inset 0 1px 4px rgba(0, 0, 0, 0.32) !important;
}
[data-theme="light"] .joy-empty-bay {
  background: #F8FAFC !important;
  border: 1px solid #CBD5E1 !important;
  box-shadow: inset 0 1px 2px rgba(15, 23, 42, 0.03) !important;
}
[data-theme="black"] .joy-empty-bay {
  background: #05080E !important;
  border: 1px solid rgba(255, 255, 255, 0.10) !important;
}
.joy-empty-icon {
  color: #64748B !important;
  opacity: 0.85 !important;
  margin-bottom: 10px !important;
}
[data-theme="light"] .joy-empty-icon {
  color: #64748B !important;
}
.joy-empty-title {
  font-family: var(--font-ui) !important;
  font-size: 0.80rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.01em !important;
  color: var(--text) !important;
  text-transform: none !important;
  margin-bottom: 4px !important;
}
.joy-empty-sub {
  font-family: var(--font-ui) !important;
  font-size: 0.69rem !important;
  color: var(--muted) !important;
  max-width: 240px !important;
  margin-top: 0 !important;
  line-height: 1.45 !important;
}
.joy-device-tag {
  color: var(--accent2) !important;
  font-family: var(--font-mono) !important;
  font-size: 0.68rem !important;
  font-weight: 600 !important;
  margin-left: 4px !important;
}
.joy-instrument-bay {
  display: flex;
  flex-direction: column;
  justify-content: center;
  flex: 1 1 auto;
  background: #0C1322;
  border: 1px solid rgba(148, 163, 184, 0.14);
  border-radius: 6px;
  padding: 14px;
  margin: 2px 0 10px;
  gap: 14px;
}
[data-theme="light"] .joy-instrument-bay {
  background: #F8FAFC;
  border-color: #CBD5E1;
}
[data-theme="black"] .joy-instrument-bay {
  background: #05080E;
  border-color: rgba(255, 255, 255, 0.10);
}
.joy-top-row {
  display: flex;
  align-items: center;
  gap: 16px;
}
.joy-stick-bezel {
  width: 128px;
  height: 128px;
  flex: 0 0 128px;
  border-radius: 50%;
  background: transparent;
  border: none;
  display: flex;
  align-items: center;
  justify-content: center;
  overflow: hidden;
}
#joyStickCanvas {
  display: block;
  width: 128px;
  height: 128px;
  border-radius: 50%;
}
.joy-axes-bars {
  flex: 1 1 auto;
  min-width: 0;
  margin: 0 !important;
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.joy-axis-row {
  display: flex;
  align-items: center;
  gap: 10px;
  margin: 0 !important;
}
.joy-axis-label {
  font-family: var(--font-mono);
  font-size: 0.72rem;
  font-weight: 600;
  color: var(--muted);
  width: 22px;
  text-align: left;
  flex-shrink: 0;
}
.joy-axis-track {
  flex: 1;
  height: 8px;
  background: #080D18;
  border: 1px solid var(--border2);
  border-radius: 999px;
  position: relative;
  overflow: visible;
}
[data-theme="light"] .joy-axis-track {
  background: #E2E8F0;
  border-color: #CBD5E1;
}
.joy-axis-fill {
  position: absolute;
  height: 100%;
  background: #2FB7C8;
  border-radius: 999px;
  top: 0;
  transition: width 0.06s ease, left 0.06s ease;
}
[data-theme="light"] .joy-axis-fill {
  background: #0B7A8C;
}
.joy-axis-center {
  position: absolute;
  width: 1px;
  height: 14px;
  background: #475569;
  top: -4px;
  left: 50%;
  transform: translateX(-50%);
  z-index: 2;
}
[data-theme="light"] .joy-axis-center {
  background: #94A3B8;
}
.joy-axis-val {
  font-family: var(--font-mono);
  font-size: 0.74rem;
  font-weight: 600;
  color: var(--text2);
  width: 42px;
  text-align: right;
  font-variant-numeric: tabular-nums;
  flex-shrink: 0;
}
.joy-divider {
  height: 1px;
  background: var(--border);
  width: 100%;
  margin: 0;
}
.joy-buttons {
  display: grid !important;
  grid-template-columns: repeat(12, minmax(0, 1fr)) !important;
  gap: 5px !important;
  margin: 0 !important;
  width: 100%;
}
.joy-btn-indicator {
  width: 100% !important;
  height: 28px !important;
  border-radius: 5px !important;
  background: var(--bg2) !important;
  border: 1px solid var(--border2) !important;
  font-size: 0.68rem !important;
  font-weight: 600 !important;
  display: inline-flex !important;
  align-items: center !important;
  justify-content: center !important;
  color: var(--muted) !important;
  font-family: var(--font-mono) !important;
  transition: all 0.08s ease !important;
}
[data-theme="light"] .joy-btn-indicator {
  background: #FFFFFF !important;
  border-color: #CBD5E1 !important;
  color: #64748B !important;
}
.joy-btn-indicator.pressed {
  background: #2FB7C8 !important;
  color: #06181E !important;
  border-color: #2FB7C8 !important;
  font-weight: 700 !important;
}
[data-theme="light"] .joy-btn-indicator.pressed {
  background: #0B7A8C !important;
  color: #FFFFFF !important;
  border-color: #0B7A8C !important;
}

/* Harmonize Virtual Joystick & Digital Compass with whole-site rhythm */
.vjoy-main-row {
  display: flex !important;
  align-items: center !important;
  justify-content: center !important;
  gap: 18px !important;
  margin: 6px 0 10px !important;
  flex: 1;
}
.vjoy-bezel {
  width: 196px !important;
  height: 196px !important;
  flex: 0 0 196px !important;
  max-width: 196px !important;
  padding: 6px !important;
}
.vjoy-thruster-card {
  height: 196px !important;
  background: var(--bg2) !important;
  border: 1px solid var(--border) !important;
  border-radius: 6px !important;
}
[data-theme="light"] .vjoy-thruster-card {
  background: #F8FAFC !important;
  border-color: #CBD5E1 !important;
}
.vjoy-toggle-strip,
.compass-toggle-row {
  background: var(--bg2) !important;
  border: 1px solid var(--border) !important;
  border-radius: 5px !important;
  padding: 8px 12px !important;
  font-size: 0.74rem !important;
}
.vjoy-toggle-label {
  font-size: 0.74rem !important;
  font-weight: 600 !important;
  color: var(--text2) !important;
}
.vjoy-toggle-status,
.compass-toggle-row #magStreamLabel {
  font-size: 0.68rem !important;
  font-family: var(--font-mono) !important;
  font-weight: 700 !important;
}
.compass-toggle-row span:first-child,
.compass-toggle-row label:first-child {
  font-size: 0.74rem !important;
  font-weight: 600 !important;
  color: var(--text2) !important;
}
.compass-toggle-row .toggle-switch {
  width: 34px !important;
  height: 19px !important;
}
.compass-toggle-row .toggle-slider::before {
  width: 13px !important;
  height: 13px !important;
  left: 2px !important;
  top: 2px !important;
}
.compass-toggle-row .toggle-switch input:checked + .toggle-slider::before {
  transform: translateX(15px) !important;
}
.compass-heading-card {
  background: var(--bg2) !important;
  border: 1px solid var(--border) !important;
  border-radius: 6px !important;
  padding: 8px 18px !important;
}
.vjoy-stop-full,
:root[data-theme="dark"] .vjoy-stop-full,
[data-theme="dark"] .vjoy-stop-full {
  min-height: 31px !important;
  padding: 6px 12px !important;
  border-radius: 6px !important;
  font-family: var(--font-ui) !important;
  font-size: 0.73rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.03em !important;
  text-transform: none !important;
  background: linear-gradient(180deg, rgba(244, 63, 94, 0.14) 0%, rgba(225, 29, 72, 0.06) 100%), #171B29 !important;
  border: 1px solid rgba(244, 63, 94, 0.36) !important;
  color: #FFE4E6 !important;
  box-shadow:
    inset 0 1px 0 rgba(251, 113, 133, 0.14),
    0 1px 2px rgba(0, 0, 0, 0.30) !important;
}
.vjoy-stop-full svg.btn-svg {
  stroke: #FB7185 !important;
}
.vjoy-stop-full:hover,
:root[data-theme="dark"] .vjoy-stop-full:hover,
[data-theme="dark"] .vjoy-stop-full:hover,
.vjoy-stop-full:active {
  background: linear-gradient(180deg, rgba(244, 63, 94, 0.28) 0%, rgba(225, 29, 72, 0.16) 100%), #1E1C2D !important;
  border-color: #F43F5E !important;
  color: #FFFFFF !important;
  box-shadow:
    inset 0 1px 0 rgba(253, 164, 175, 0.22),
    0 0 10px rgba(244, 63, 94, 0.22),
    0 2px 5px rgba(0, 0, 0, 0.35) !important;
}
[data-theme="light"] .vjoy-stop-full {
  background: linear-gradient(180deg, #FFF5F6 0%, #FFE8EB 100%) !important;
  border: 1px solid #FDA4AF !important;
  color: #9F1239 !important;
  box-shadow:
    inset 0 1px 0 #FFFFFF,
    0 1px 2px rgba(225, 29, 72, 0.06) !important;
}
[data-theme="light"] .vjoy-stop-full svg.btn-svg {
  stroke: #E11D48 !important;
}
[data-theme="light"] .vjoy-stop-full:hover,
[data-theme="light"] .vjoy-stop-full:active {
  background: linear-gradient(180deg, #FFE4E6 0%, #FECDD3 100%) !important;
  border-color: #F43F5E !important;
  color: #881337 !important;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.85),
    0 2px 6px rgba(244, 63, 94, 0.16) !important;
}

[data-theme="light"] .telem-hero-strip,
[data-theme="light"] .telem-ribbon,
[data-theme="light"] .leak-row,
[data-theme="light"] .gps-bar,
[data-theme="light"] .telem-toggle-row,
[data-theme="light"] .vjoy-toggle-strip,
[data-theme="light"] .compass-toggle-row,
[data-theme="light"] .compass-heading-card {
  background: #F8FAFC !important;
  border-color: #CBD5E1 !important;
  box-shadow: none;
}
[data-theme="light"] .telem-hero-bay:first-child,
[data-theme="light"] .telem-ribbon-cell,
[data-theme="light"] .leak-sensor-card {
  border-right-color: #CBD5E1;
}
[data-theme="light"] .term-title-bar {
  background: #E2E8F0;
  border-bottom: 1px solid #CBD5E1;
}
[data-theme="light"] .term-session {
  color: #334155;
}
[data-theme="light"] .link-subtabs,
[data-theme="light"] .mode-buttons {
  background: #E2E8F0 !important;
  border-color: #CBD5E1 !important;
}
[data-theme="light"] input[type="text"],
[data-theme="light"] input[type="number"],
[data-theme="light"] input[type="password"],
[data-theme="light"] select {
  background: #FFFFFF;
  border-color: #CBD5E1;
  color: #0F172A;
}

/* ---- Subtly Tinted Architectural Hardware Button System (All Themes) ----
   Subtle semantic tint at rest ("thoda sa jesa hai vesa colour") +
   Rich luminous highlight on hover / selected ("select karne per accha dikhan chiaye"). */
.btn,
.btn-estop-off {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  min-height: 30px;
  padding: 5px 12px;
  border: 1px solid #2A3B54 !important;
  border-radius: 6px;
  background: linear-gradient(180deg, #1B263B 0%, #111A2B 100%) !important;
  color: #E2E8F0 !important;
  font-family: var(--font-ui);
  font-size: 0.72rem;
  font-weight: 600;
  letter-spacing: 0.015em;
  cursor: pointer;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.07),
    0 1px 2px rgba(0, 0, 0, 0.30) !important;
  transition: all 0.14s ease;
  white-space: nowrap;
  user-select: none;
  box-sizing: border-box;
}
.btn svg.btn-svg {
  width: 12px;
  height: 12px;
  stroke: #60A5FA;
  stroke-width: 2;
  fill: none;
  flex-shrink: 0;
  transition: stroke 0.14s ease, fill 0.14s ease, transform 0.14s ease;
}
.btn:hover,
.btn-estop-off:hover {
  background: linear-gradient(180deg, #23334F 0%, #17243A 100%) !important;
  border-color: #38BDF8 !important;
  color: #FFFFFF !important;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.14),
    0 0 10px rgba(56, 189, 248, 0.20),
    0 2px 5px rgba(0, 0, 0, 0.36) !important;
}
.btn:hover svg.btn-svg,
.btn-estop-off:hover svg.btn-svg {
  stroke: #38BDF8;
}
.btn-estop-off.is-selected {
  background: linear-gradient(180deg, rgba(56, 189, 248, 0.26) 0%, rgba(37, 99, 235, 0.16) 100%), #16253B !important;
  border-color: #38BDF8 !important;
  color: #FFFFFF !important;
  font-weight: 700 !important;
  box-shadow:
    inset 0 1px 0 rgba(186, 230, 253, 0.22),
    0 0 0 1px rgba(56, 189, 248, 0.25),
    0 2px 8px rgba(56, 189, 248, 0.22) !important;
}
.btn-estop-off.is-selected svg.btn-svg {
  stroke: #7DD3FC !important;
}
.btn:active {
  transform: translateY(1px);
}
.btn-sm {
  min-height: 26px;
  padding: 3px 10px;
  gap: 5px;
  font-size: 0.68rem;
  font-family: var(--font-ui);
  font-weight: 600;
  letter-spacing: 0.01em;
  text-transform: none;
  border-radius: 5px;
}

/* 1. Positive / Engage Buttons (.btn-ok: Start Rec, ARM VEHICLE, Connect) - Subtle Emerald Tint */
.btn-ok,
.btn.btn-ok {
  background: linear-gradient(180deg, rgba(16, 185, 129, 0.15) 0%, rgba(5, 150, 105, 0.07) 100%), #131F2E !important;
  border: 1px solid rgba(16, 185, 129, 0.38) !important;
  color: #D1FAE5 !important;
  font-weight: 600 !important;
  box-shadow:
    inset 0 1px 0 rgba(52, 211, 153, 0.16),
    0 1px 2px rgba(0, 0, 0, 0.32) !important;
}
.btn-ok svg.btn-svg,
.btn.btn-ok svg.btn-svg {
  stroke: #34D399;
  color: #34D399;
}
.btn-ok:hover,
.btn.btn-ok:hover {
  background: linear-gradient(180deg, rgba(16, 185, 129, 0.26) 0%, rgba(5, 150, 105, 0.15) 100%), #162638 !important;
  border-color: #10B981 !important;
  color: #FFFFFF !important;
  box-shadow:
    inset 0 1px 0 rgba(110, 231, 183, 0.24),
    0 0 10px rgba(16, 185, 129, 0.22),
    0 2px 6px rgba(0, 0, 0, 0.36) !important;
}
.btn-ok:hover svg.btn-svg,
.btn.btn-ok:hover svg.btn-svg {
  stroke: #6EE7B7;
  color: #6EE7B7;
}
.btn-ok.is-selected,
.btn.btn-ok.is-selected {
  background: linear-gradient(180deg, rgba(16, 185, 129, 0.36) 0%, rgba(5, 150, 105, 0.22) 100%), #142B39 !important;
  border-color: #34D399 !important;
  color: #FFFFFF !important;
  font-weight: 700 !important;
  box-shadow:
    inset 0 1px 0 rgba(167, 243, 208, 0.28),
    0 0 0 1px rgba(16, 185, 129, 0.32),
    0 2px 10px rgba(16, 185, 129, 0.26) !important;
}
.btn-ok.is-selected svg.btn-svg {
  stroke: #A7F3D0 !important;
  color: #A7F3D0 !important;
}

/* 2. Stop / Disarm / Reboot Buttons (.btn-danger) - Subtle Rose-Crimson Tint */
.btn-danger,
.btn.btn-danger {
  background: linear-gradient(180deg, rgba(244, 63, 94, 0.14) 0%, rgba(225, 29, 72, 0.06) 100%), #171B29 !important;
  border: 1px solid rgba(244, 63, 94, 0.36) !important;
  color: #FFE4E6 !important;
  font-weight: 600 !important;
  box-shadow:
    inset 0 1px 0 rgba(251, 113, 133, 0.15),
    0 1px 2px rgba(0, 0, 0, 0.32) !important;
}
.btn-danger svg.btn-svg,
.btn.btn-danger svg.btn-svg {
  stroke: #FB7185;
  color: #FB7185;
}
.btn-danger:hover,
.btn.btn-danger:hover {
  background: linear-gradient(180deg, rgba(244, 63, 94, 0.26) 0%, rgba(225, 29, 72, 0.14) 100%), #1E1C2D !important;
  border-color: #F43F5E !important;
  color: #FFFFFF !important;
  box-shadow:
    inset 0 1px 0 rgba(253, 164, 175, 0.22),
    0 0 10px rgba(244, 63, 94, 0.22),
    0 2px 6px rgba(0, 0, 0, 0.36) !important;
}
.btn-danger:hover svg.btn-svg,
.btn.btn-danger:hover svg.btn-svg {
  stroke: #FDA4AF;
  color: #FDA4AF;
}
.btn-danger.is-selected,
.btn.btn-danger.is-selected {
  background: linear-gradient(180deg, rgba(244, 63, 94, 0.34) 0%, rgba(225, 29, 72, 0.20) 100%), #22192A !important;
  border-color: #FB7185 !important;
  color: #FFFFFF !important;
  font-weight: 700 !important;
  box-shadow:
    inset 0 1px 0 rgba(254, 205, 211, 0.25),
    0 0 0 1px rgba(244, 63, 94, 0.30),
    0 2px 10px rgba(244, 63, 94, 0.25) !important;
}
.btn-danger.is-selected svg.btn-svg {
  stroke: #FECDD3 !important;
  color: #FECDD3 !important;
}

/* 3. Emergency Stop Button (.btn-estop) - Authoritative Tactical Crimson Tint */
.btn-estop {
  background: linear-gradient(180deg, rgba(225, 29, 72, 0.24) 0%, rgba(190, 18, 60, 0.12) 100%), #1A1726 !important;
  border: 1px solid rgba(244, 63, 94, 0.48) !important;
  color: #FFF1F2 !important;
  font-weight: 700 !important;
  box-shadow:
    inset 0 1px 0 rgba(251, 113, 133, 0.20),
    0 1px 3px rgba(0, 0, 0, 0.36) !important;
}
.btn-estop svg.btn-svg {
  stroke: #FB7185;
  color: #FB7185;
}
.btn-estop:hover {
  background: linear-gradient(180deg, rgba(225, 29, 72, 0.36) 0%, rgba(190, 18, 60, 0.22) 100%), #211829 !important;
  border-color: #FB7185 !important;
  color: #FFFFFF !important;
  box-shadow:
    inset 0 1px 0 rgba(254, 205, 211, 0.26),
    0 0 12px rgba(244, 63, 94, 0.28),
    0 2px 6px rgba(0, 0, 0, 0.38) !important;
}
.btn-estop.is-selected {
  background: linear-gradient(180deg, #E11D48 0%, #BE123C 100%) !important;
  border-color: #FDA4AF !important;
  color: #FFFFFF !important;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.28),
    0 0 0 1px rgba(244, 63, 94, 0.40),
    0 2px 12px rgba(225, 29, 72, 0.38) !important;
}
.btn-estop.is-selected svg.btn-svg {
  stroke: #FFFFFF !important;
  color: #FFFFFF !important;
}

/* 4. Warning / Save Action Button (.btn-warn: Save Bag) - Subtle Amber-Gold Tint */
.btn-warn,
.btn.btn-warn {
  background: linear-gradient(180deg, rgba(245, 158, 11, 0.14) 0%, rgba(217, 119, 6, 0.06) 100%), #181E2B !important;
  border: 1px solid rgba(245, 158, 11, 0.38) !important;
  color: #FEF3C7 !important;
  font-weight: 600 !important;
  box-shadow:
    inset 0 1px 0 rgba(251, 191, 36, 0.15),
    0 1px 2px rgba(0, 0, 0, 0.32) !important;
}
.btn-warn svg.btn-svg,
.btn.btn-warn svg.btn-svg {
  stroke: #FBBF24;
  color: #FBBF24;
}
.btn-warn:hover,
.btn.btn-warn:hover,
.btn-warn.is-selected,
.btn.btn-warn.is-selected {
  background: linear-gradient(180deg, rgba(245, 158, 11, 0.28) 0%, rgba(217, 119, 6, 0.16) 100%), #1F2230 !important;
  border-color: #F59E0B !important;
  color: #FFFFFF !important;
  box-shadow:
    inset 0 1px 0 rgba(253, 230, 138, 0.24),
    0 0 10px rgba(245, 158, 11, 0.22),
    0 2px 6px rgba(0, 0, 0, 0.36) !important;
}
.btn-warn:hover svg.btn-svg,
.btn.btn-warn:hover svg.btn-svg,
.btn-warn.is-selected svg.btn-svg {
  stroke: #FDE68A;
  color: #FDE68A;
}

.btn.active,
.btn.btn-active {
  background: linear-gradient(180deg, #2563EB 0%, #1D4ED8 100%) !important;
  border-color: #60A5FA !important;
  color: #FFFFFF !important;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.22),
    0 2px 8px rgba(37, 99, 235, 0.32) !important;
}

/* Compact inline icon buttons (File tree, Channels, Params) */
.btn-icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 4px;
  height: 22px;
  padding: 0 8px;
  font-size: 0.62rem;
  font-weight: 600;
  font-family: var(--font-ui);
  border: 1px solid #2A3B54;
  border-radius: 4px;
  background: linear-gradient(180deg, #1B263B 0%, #111A2B 100%);
  color: #CBD5E1;
  cursor: pointer;
  transition: all 0.12s ease;
  box-sizing: border-box;
}
.btn-icon:hover {
  border-color: #38BDF8;
  color: #FFFFFF;
}

/* Segmented Tabs (Link Subtabs, Mode Buttons) - Crisp Royal-Cobalt Selected Highlight */
.link-subtabs {
  display: flex;
  gap: 3px;
  background: #080D17;
  padding: 3px;
  border-radius: 6px;
  border: 1px solid rgba(148, 163, 184, 0.16);
  margin: 8px 0;
}
.link-subtab {
  flex: 1;
  height: 27px;
  padding: 0 10px;
  border: 1px solid transparent;
  border-radius: 4px;
  background: transparent;
  color: var(--muted);
  font-family: var(--font-ui);
  font-size: 0.72rem;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.14s ease;
  display: inline-flex;
  align-items: center;
  justify-content: center;
}
.link-subtab:hover {
  color: var(--text);
  background: rgba(148, 163, 184, 0.08);
}
.link-subtab.active {
  background: linear-gradient(180deg, #2563EB 0%, #1D4ED8 100%) !important;
  color: #FFFFFF !important;
  font-weight: 700 !important;
  border-color: #60A5FA !important;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.22),
    0 2px 6px rgba(37, 99, 235, 0.30) !important;
}
[data-theme="light"] .link-subtab {
  color: #475569;
}
[data-theme="light"] .link-subtab:hover {
  color: #0F172A;
  background: rgba(255, 255, 255, 0.65);
}
[data-theme="light"] .link-subtab.active {
  background: linear-gradient(180deg, #2563EB 0%, #1D4ED8 100%) !important;
  color: #FFFFFF !important;
  border-color: #1D4ED8 !important;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.25),
    0 2px 6px rgba(37, 99, 235, 0.24) !important;
}

.mode-buttons {
  background: #080D17;
  border: 1px solid rgba(148, 163, 184, 0.16);
  border-radius: 6px;
  padding: 3px;
  gap: 3px;
}
.mode-buttons .btn {
  box-shadow: none !important;
  background: transparent !important;
  border-color: transparent !important;
  color: var(--muted) !important;
  min-height: 27px;
  height: 27px;
  border-radius: 4px;
}
.mode-buttons .btn:hover {
  color: var(--text) !important;
  background: rgba(148, 163, 184, 0.08) !important;
}
.mode-buttons .btn.active {
  background: linear-gradient(180deg, #2563EB 0%, #1D4ED8 100%) !important;
  border-color: #60A5FA !important;
  color: #FFFFFF !important;
  font-weight: 700 !important;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.22),
    0 2px 6px rgba(37, 99, 235, 0.30) !important;
}

/* ---- Light Mode Subtly Tinted Semantic Buttons & Selected States ---- */
[data-theme="light"] .btn,
[data-theme="light"] .btn-estop-off {
  background: linear-gradient(180deg, #FFFFFF 0%, #F1F5F9 100%) !important;
  border: 1px solid #CBD5E1 !important;
  color: #1E293B !important;
  font-weight: 600 !important;
  box-shadow:
    inset 0 1px 0 #FFFFFF,
    0 1px 2px rgba(15, 23, 42, 0.06) !important;
}
[data-theme="light"] .btn svg.btn-svg {
  stroke: #2563EB;
}
[data-theme="light"] .btn:hover,
[data-theme="light"] .btn-estop-off:hover {
  background: linear-gradient(180deg, #EFF6FF 0%, #DBEAFE 100%) !important;
  border-color: #60A5FA !important;
  color: #1E3A8A !important;
  box-shadow:
    inset 0 1px 0 #FFFFFF,
    0 2px 6px rgba(37, 99, 235, 0.12) !important;
}
[data-theme="light"] .btn:hover svg.btn-svg {
  stroke: #1D4ED8;
}
[data-theme="light"] .btn-estop-off.is-selected {
  background: linear-gradient(180deg, #EFF6FF 0%, #DBEAFE 100%) !important;
  border-color: #3B82F6 !important;
  color: #1E3A8A !important;
  font-weight: 700 !important;
  box-shadow:
    inset 0 1px 0 #FFFFFF,
    0 0 0 1px rgba(59, 130, 246, 0.22),
    0 2px 6px rgba(37, 99, 235, 0.14) !important;
}

/* Light Mode .btn-ok (Start Rec, ARM VEHICLE, Connect) - Soft Pearl Emerald -> Selected Emerald */
[data-theme="light"] .btn-ok,
[data-theme="light"] .btn.btn-ok {
  background: linear-gradient(180deg, #F2FDF7 0%, #E2FAEE 100%) !important;
  border: 1px solid #86EFAC !important;
  color: #065F46 !important;
  font-weight: 600 !important;
  box-shadow:
    inset 0 1px 0 #FFFFFF,
    0 1px 2px rgba(5, 150, 105, 0.07) !important;
}
[data-theme="light"] .btn-ok svg.btn-svg,
[data-theme="light"] .btn.btn-ok svg.btn-svg {
  stroke: #059669;
  color: #059669;
}
[data-theme="light"] .btn-ok:hover,
[data-theme="light"] .btn.btn-ok:hover {
  background: linear-gradient(180deg, #DCFCE7 0%, #BBF7D0 100%) !important;
  border-color: #10B981 !important;
  color: #064E3B !important;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.85),
    0 2px 7px rgba(16, 185, 129, 0.18) !important;
}
[data-theme="light"] .btn-ok.is-selected,
[data-theme="light"] .btn.btn-ok.is-selected {
  background: linear-gradient(180deg, #10B981 0%, #059669 100%) !important;
  border-color: #047857 !important;
  color: #FFFFFF !important;
  font-weight: 700 !important;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.25),
    0 2px 8px rgba(5, 150, 105, 0.28) !important;
}
[data-theme="light"] .btn-ok.is-selected svg.btn-svg,
[data-theme="light"] .btn.btn-ok.is-selected svg.btn-svg {
  stroke: #FFFFFF !important;
  color: #FFFFFF !important;
}

/* Light Mode .btn-danger (Stop Rec, DISARM, REBOOT SYSTEM) - Soft Pearl Rose -> Selected Crimson */
[data-theme="light"] .btn-danger,
[data-theme="light"] .btn.btn-danger {
  background: linear-gradient(180deg, #FFF5F6 0%, #FFE8EB 100%) !important;
  border: 1px solid #FDA4AF !important;
  color: #9F1239 !important;
  font-weight: 600 !important;
  box-shadow:
    inset 0 1px 0 #FFFFFF,
    0 1px 2px rgba(225, 29, 72, 0.06) !important;
}
[data-theme="light"] .btn-danger svg.btn-svg,
[data-theme="light"] .btn.btn-danger svg.btn-svg {
  stroke: #E11D48;
  color: #E11D48;
}
[data-theme="light"] .btn-danger:hover,
[data-theme="light"] .btn.btn-danger:hover {
  background: linear-gradient(180deg, #FFE4E6 0%, #FECDD3 100%) !important;
  border-color: #F43F5E !important;
  color: #881337 !important;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.85),
    0 2px 7px rgba(244, 63, 94, 0.16) !important;
}
[data-theme="light"] .btn-danger.is-selected,
[data-theme="light"] .btn.btn-danger.is-selected {
  background: linear-gradient(180deg, #E11D48 0%, #BE123C 100%) !important;
  border-color: #9F1239 !important;
  color: #FFFFFF !important;
  font-weight: 700 !important;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.25),
    0 2px 8px rgba(225, 29, 72, 0.26) !important;
}
[data-theme="light"] .btn-danger.is-selected svg.btn-svg,
[data-theme="light"] .btn.btn-danger.is-selected svg.btn-svg {
  stroke: #FFFFFF !important;
  color: #FFFFFF !important;
}

/* Light Mode .btn-estop (E-STOP ON) */
[data-theme="light"] .btn-estop {
  background: linear-gradient(180deg, #FFE8EC 0%, #FFD8DF 100%) !important;
  border: 1px solid #FB7185 !important;
  color: #9F1239 !important;
  font-weight: 700 !important;
  box-shadow:
    inset 0 1px 0 #FFFFFF,
    0 1px 3px rgba(225, 29, 72, 0.10) !important;
}
[data-theme="light"] .btn-estop svg.btn-svg {
  stroke: #E11D48;
  color: #E11D48;
}
[data-theme="light"] .btn-estop:hover {
  background: linear-gradient(180deg, #FFD5DC 0%, #FECDD3 100%) !important;
  border-color: #E11D48 !important;
  color: #881337 !important;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.85),
    0 2px 8px rgba(225, 29, 72, 0.20) !important;
}
[data-theme="light"] .btn-estop.is-selected {
  background: linear-gradient(180deg, #E11D48 0%, #BE123C 100%) !important;
  border-color: #9F1239 !important;
  color: #FFFFFF !important;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.25),
    0 2px 9px rgba(225, 29, 72, 0.30) !important;
}
[data-theme="light"] .btn-estop.is-selected svg.btn-svg {
  stroke: #FFFFFF !important;
  color: #FFFFFF !important;
}

/* Light Mode .btn-warn (Save Bag) - Soft Pearl Amber */
[data-theme="light"] .btn-warn,
[data-theme="light"] .btn.btn-warn {
  background: linear-gradient(180deg, #FFFDF5 0%, #FEF5D8 100%) !important;
  border: 1px solid #FCD34D !important;
  color: #92400E !important;
  font-weight: 600 !important;
  box-shadow:
    inset 0 1px 0 #FFFFFF,
    0 1px 2px rgba(217, 119, 6, 0.06) !important;
}
[data-theme="light"] .btn-warn svg.btn-svg,
[data-theme="light"] .btn.btn-warn svg.btn-svg {
  stroke: #D97706;
  color: #D97706;
}
[data-theme="light"] .btn-warn:hover,
[data-theme="light"] .btn.btn-warn:hover {
  background: linear-gradient(180deg, #FEF3C7 0%, #FDE68A 100%) !important;
  border-color: #F59E0B !important;
  color: #78350F !important;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.85),
    0 2px 7px rgba(245, 158, 11, 0.16) !important;
}
[data-theme="light"] .btn-warn.is-selected,
[data-theme="light"] .btn.btn-warn.is-selected {
  background: linear-gradient(180deg, #D97706 0%, #B45309 100%) !important;
  border-color: #92400E !important;
  color: #FFFFFF !important;
  font-weight: 700 !important;
}
[data-theme="light"] .btn-warn.is-selected svg.btn-svg {
  stroke: #FFFFFF !important;
  color: #FFFFFF !important;
}

[data-theme="light"] .btn-icon {
  background: linear-gradient(180deg, #FFFFFF 0%, #F1F5F9 100%);
  border-color: #CBD5E1;
  color: #334155;
}
[data-theme="light"] .btn-icon:hover {
  border-color: #60A5FA;
  color: #1E3A8A;
  background: #EFF6FF;
}
[data-theme="light"] .mode-buttons .btn {
  background: transparent !important;
  border-color: transparent !important;
  box-shadow: none !important;
  color: #475569 !important;
}
[data-theme="light"] .mode-buttons .btn:hover {
  color: #0F172A !important;
  background: rgba(255, 255, 255, 0.65) !important;
}
[data-theme="light"] .mode-buttons .btn.active {
  background: linear-gradient(180deg, #2563EB 0%, #1D4ED8 100%) !important;
  border-color: #1D4ED8 !important;
  color: #FFFFFF !important;
  font-weight: 700 !important;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.25),
    0 2px 6px rgba(37, 99, 235, 0.24) !important;
}

/* Precision Toggle Switch Refinements (All Panels) */
.telem-toggle-row {
  border-radius: 6px;
  padding: 8px 12px;
}
.toggle-switch {
  width: 34px;
  height: 19px;
}
.toggle-slider {
  background: #283648;
  border: 1px solid #36495E;
  border-radius: 20px;
}
.toggle-slider::before {
  width: 13px;
  height: 13px;
  left: 2px;
  top: 2px;
  background: #FFFFFF;
}
.toggle-switch input:checked + .toggle-slider {
  background: linear-gradient(180deg, #2563EB 0%, #1D4ED8 100%);
  border-color: #60A5FA;
  box-shadow: 0 0 8px rgba(37, 99, 235, 0.30);
}
.toggle-switch input:checked + .toggle-slider::before {
  transform: translateX(15px);
  background: #FFFFFF;
}
[data-theme="light"] .toggle-slider {
  background: #CBD5E1;
  border-color: #94A3B8;
}
[data-theme="light"] .toggle-slider::before {
  background: #FFFFFF;
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.20);
}
[data-theme="light"] .toggle-switch input:checked + .toggle-slider {
  background: linear-gradient(180deg, #2563EB 0%, #1D4ED8 100%);
  border-color: #1D4ED8;
  box-shadow: 0 1px 4px rgba(37, 99, 235, 0.25);
}

/* True Black Mode Surface & Instrument Refinements */
[data-theme="black"] .orient3d-scene {
  background: radial-gradient(circle at center, #080E1A 0%, #020306 100%);
  border-color: rgba(255, 255, 255, 0.10);
}
[data-theme="black"] .vjoy-bezel {
  background: #0B101A;
  border-color: #1E293B;
}
[data-theme="black"] #vjoyCanvas {
  background: #04070C;
  border-color: #182232;
}
[data-theme="black"] .vjoy-thruster-card {
  background: #080C14;
  border-color: #1E293B;
}
[data-theme="black"] .compass-heading-card,
[data-theme="black"] .compass-toggle-row {
  background: #080C14;
  border-color: #1E293B;
}

/* Light mode 3D Horizon & Console refinements */
[data-theme="light"] .orient3d-scene {
  background: radial-gradient(circle at center, #F8FAFC 0%, #E2E8F0 100%);
  box-shadow: inset 0 2px 8px rgba(15, 23, 42, 0.06);
  border-color: #CBD5E1;
}
[data-theme="light"] .orient3d-scene::before {
  background-image:
    radial-gradient(circle at center, transparent 48px, rgba(37, 99, 235, 0.14) 49px, transparent 50px),
    linear-gradient(90deg, transparent 49.6%, rgba(37, 99, 235, 0.18) 49.6%, rgba(37, 99, 235, 0.18) 50.4%, transparent 50.4%),
    linear-gradient(0deg, transparent 49.6%, rgba(37, 99, 235, 0.18) 49.6%, rgba(37, 99, 235, 0.18) 50.4%, transparent 50.4%),
    linear-gradient(to right, rgba(37, 99, 235, 0.06) 1px, transparent 1px),
    linear-gradient(to bottom, rgba(37, 99, 235, 0.06) 1px, transparent 1px);
}
[data-theme="light"] .folder-header:hover {
  color: var(--accent);
  background: var(--hover);
}
[data-theme="light"] #mapPlannerFrame {
  background: #F1F5F9;
}

/* ---- Whole-Site Dark & Light Mode Interior Sub-Card & Input Finishing ---- */
.telem-hero-strip,
.telem-ribbon,
.leak-row,
.gps-bar,
.telem-toggle-row,
.vjoy-toggle-strip,
.compass-toggle-row,
.compass-heading-card,
.rec-indicator,
.sysinfo {
  background: linear-gradient(180deg, #131D30 0%, #0D1524 100%) !important;
  border: 1px solid #1E2E48 !important;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04), 0 1px 2px rgba(0, 0, 0, 0.22) !important;
}
.rec-indicator.recording {
  background: linear-gradient(180deg, rgba(244, 63, 94, 0.18) 0%, rgba(225, 29, 72, 0.08) 100%), #181726 !important;
  border-color: rgba(244, 63, 94, 0.45) !important;
  color: #FDA4AF !important;
}
[data-theme="light"] .telem-hero-strip,
[data-theme="light"] .telem-ribbon,
[data-theme="light"] .leak-row,
[data-theme="light"] .gps-bar,
[data-theme="light"] .telem-toggle-row,
[data-theme="light"] .vjoy-toggle-strip,
[data-theme="light"] .compass-toggle-row,
[data-theme="light"] .compass-heading-card,
[data-theme="light"] .rec-indicator,
[data-theme="light"] .sysinfo {
  background: linear-gradient(180deg, #FFFFFF 0%, #F8FAFC 100%) !important;
  border: 1px solid #CBD5E1 !important;
  box-shadow: inset 0 1px 0 #FFFFFF, 0 1px 2px rgba(15, 23, 42, 0.04) !important;
}
[data-theme="light"] .rec-indicator.recording {
  background: linear-gradient(180deg, #FFF1F2 0%, #FFE4E6 100%) !important;
  border-color: #FDA4AF !important;
  color: #BE123C !important;
}
[data-theme="light"] .gps-bar .gps-badge {
  background: #EFF6FF !important;
  border-color: #BFDBFE !important;
  color: #1D4ED8 !important;
}
[data-theme="light"] .gps-bar #tGps,
[data-theme="light"] .sysinfo > span:not(.sysinfo-sep) {
  color: #0F172A !important;
}
[data-theme="light"] .leak-sensor-status {
  color: #047857 !important;
}
[data-theme="light"] .leak-dot.alert + .leak-sensor-info .leak-sensor-status {
  color: #BE123C !important;
}

/* Remote Terminal Inputs & Parameter Inputs */
#termInput,
#termTimeout,
#termMaxLines,
.param-input,
.manual-input {
  background: #0A111E !important;
  border: 1px solid #243552 !important;
  color: #E2ECF7 !important;
  border-radius: 5px !important;
  box-shadow: inset 0 1px 2px rgba(0, 0, 0, 0.35) !important;
  transition: border-color 0.14s ease, box-shadow 0.14s ease, background 0.14s ease !important;
}
#termInput:focus,
#termTimeout:focus,
#termMaxLines:focus,
.param-input:focus,
.manual-input:focus {
  outline: none !important;
  border-color: #38BDF8 !important;
  background: #0D1628 !important;
  box-shadow: 0 0 0 2px rgba(56, 189, 248, 0.18), inset 0 1px 2px rgba(0, 0, 0, 0.3) !important;
}
[data-theme="light"] #termInput,
[data-theme="light"] #termTimeout,
[data-theme="light"] #termMaxLines,
[data-theme="light"] .param-input,
[data-theme="light"] .manual-input {
  background: #FFFFFF !important;
  border: 1px solid #CBD5E1 !important;
  color: #0F172A !important;
  box-shadow: inset 0 1px 2px rgba(15, 23, 42, 0.04) !important;
}
[data-theme="light"] #termInput:focus,
[data-theme="light"] #termTimeout:focus,
[data-theme="light"] #termMaxLines:focus,
[data-theme="light"] .param-input:focus,
[data-theme="light"] .manual-input:focus {
  border-color: #2563EB !important;
  background: #FFFFFF !important;
  box-shadow: 0 0 0 2px rgba(37, 99, 235, 0.15) !important;
}
[data-theme="light"] .param-input.dirty {
  border-color: #D97706 !important;
  background: #FFFBEB !important;
}

/* Progress Track & Authentication Modal */
[data-theme="light"] .progress-track {
  background: #E2E8F0 !important;
}
[data-theme="light"] .modal-overlay {
  background: rgba(15, 23, 42, 0.45) !important;
}
[data-theme="light"] .modal-box {
  background: #FFFFFF !important;
  border-color: #94A3B8 !important;
  box-shadow: 0 20px 50px rgba(15, 23, 42, 0.22), 0 4px 12px rgba(15, 23, 42, 0.08) !important;
}
[data-theme="light"] .modal-input {
  background: #F8FAFC !important;
  border-color: #CBD5E1 !important;
  color: #0F172A !important;
}

.console-box {
  background: var(--console-bg) !important;
  border: 1px solid var(--console-border, var(--border)) !important;
  border-radius: 5px;
  padding: 10px 12px;
  font-family: var(--font-mono);
  font-size: .73rem;
  color: var(--console-text);
  overflow-y: auto;
}
[data-theme="light"] .console-box {
  background: #F8FAFC !important;
  border-color: #CBD5E1 !important;
  box-shadow: inset 0 1px 2px rgba(15, 23, 42, 0.03) !important;
}
.console-line .tag {
  display: inline-block;
  padding: 0 5px;
  border-radius: 2px;
  background: rgba(56, 189, 248, 0.10);
  border: 1px solid rgba(56, 189, 248, 0.22);
  color: var(--accent2);
  font-size: 0.66rem;
  font-weight: 700;
  margin-right: 4px;
}
[data-theme="light"] .console-line .tag {
  background: rgba(2, 132, 199, 0.08);
  border-color: rgba(2, 132, 199, 0.25);
  color: #0369A1;
}

.tree-svg {
  width: 13px;
  height: 13px;
  stroke: currentColor;
  stroke-width: 1.8;
  fill: none;
  vertical-align: -2px;
  margin-right: 5px;
  flex-shrink: 0;
}

</style>
</head>
<body>

<div id="gcsOfflineBanner" style="display:none;position:sticky;top:0;z-index:999;background:var(--danger);color:#fff;padding:8px 16px;text-align:center;font-size:.78rem;font-family:var(--font-mono);font-weight:600;letter-spacing:0.02em;">
  [LINK-WARN] Lost contact with GCS backend — telemetry polling will resume automatically once interface is reachable.
</div>
<div class="app-container">


<header class="gcs-header">
  <div class="header-brand">
    <div class="brand-badge" title="TITAN-II Autonomous Underwater Vehicle GCS">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <circle cx="12" cy="12" r="9"/>
        <path d="M12 3v6m0 6v6M3 12h6m6 0h6"/>
        <circle cx="12" cy="12" r="2" fill="currentColor"/>
      </svg>
    </div>
    <div class="brand-text">
      <div class="brand-title">AUV GROUND CONTROL <span class="brand-tag">GCS-T2</span></div>
      <div class="brand-subtitle">TITAN-II // MISSION COMMAND STATION</div>
    </div>
    <span class="header-brand-sep" aria-hidden="true"></span>
    <div class="header-sys-meta">
      <span class="header-sys-icon">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="4" width="16" height="16" rx="2"/><rect x="9" y="9" width="6" height="6"/><line x1="9" y1="1" x2="9" y2="4"/><line x1="15" y1="1" x2="15" y2="4"/><line x1="9" y1="20" x2="9" y2="23"/><line x1="15" y1="20" x2="15" y2="23"/><line x1="20" y1="9" x2="23" y2="9"/><line x1="20" y1="14" x2="23" y2="14"/><line x1="1" y1="9" x2="4" y2="9"/><line x1="1" y1="14" x2="4" y2="14"/></svg>
      </span>
      <div class="header-sys-col">
        <span class="header-sys-label">SYSTEM BUS</span>
        <span class="header-sys-val">RS-485 // ROS2</span>
      </div>
    </div>
  </div>

  <nav class="header-nav">
    <button class="top-tab active" data-toptab="liveops" onclick="showTopTab('liveops')">
      <span class="tab-indicator"></span>
      <svg class="top-tab-svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
      LIVE OPERATIONS
    </button>
    <button class="top-tab" data-toptab="missionplanner" onclick="showTopTab('missionplanner')">
      <span class="tab-indicator"></span>
      <svg class="top-tab-svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="3 6 9 3 15 6 21 3 21 18 15 21 9 18 3 21"/><line x1="9" y1="3" x2="9" y2="18"/><line x1="15" y1="6" x2="15" y2="21"/></svg>
      MISSION PLANNER
    </button>
  </nav>

  <div class="header-pills">
    <div class="header-status-tray">
      <span class="pill danger" id="connPill"><span class="dot"></span><svg class="pill-svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/></svg> NO LINK</span>
      <span class="pill" id="recPill"><span class="dot"></span><svg class="pill-svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><circle cx="12" cy="12" r="8"/><circle cx="12" cy="12" r="3" fill="currentColor"/></svg> REC OFF</span>
      <span class="pill accent" id="modePill"><span class="dot"></span><svg class="pill-svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 19 21 12 17 5 21 12 2"/></svg> MANUAL</span>
      <span class="pill warn" id="armPill"><span class="dot"></span><svg class="pill-svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg> DISARMED</span>
    </div>
    <span class="header-pill-sep" aria-hidden="true"></span>
    <button type="button" class="theme-circle-btn" id="themeToggleBtn" onclick="toggleTheme()" title="Toggle Dark / Light Mode" aria-label="Toggle Dark / Light Mode">
      <svg class="theme-svg theme-svg-sun" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <circle cx="12" cy="12" r="4"/>
        <path d="M12 2v2m0 16v2M4.93 4.93l1.41 1.41m11.32 11.32l1.41 1.41M2 12h2m16 0h2M4.93 19.07l1.41-1.41m11.32-11.32l1.41-1.41"/>
      </svg>
      <svg class="theme-svg theme-svg-moon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>
      </svg>
    </button>
  </div>
</header>

<div id="tab-liveops" class="top-tab-content active">

<div class="grid-main">

  <!-- === Telemetry === -->
  <div class="panel">
    <div class="panel-head">
      <div class="panel-title">
        <span class="bar">|</span>
        <span class="panel-title-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg></span>
        Telemetry
      </div>
      <div class="panel-head-right">
        <span class="panel-tag">SYS-TELEM // 5HZ</span>
      </div>
    </div>
    <div class="telem-grid">
      <!-- Hero Primary Metrics: Depth & Battery -->
      <div class="telem-hero-strip">
        <div class="telem-hero-bay">
          <div class="telem-hero-label">Depth <span class="telem-hero-tag">Z-DEPTH</span></div>
          <div class="telem-hero-value"><span id="tDepth">--</span><span class="telem-hero-unit">m</span></div>
        </div>
        <div class="telem-hero-bay">
          <div class="telem-hero-label">Battery <span class="telem-hero-tag">VOLTAGE</span></div>
          <div class="telem-hero-value"><span id="tBatV">--</span><span class="telem-hero-unit">V</span></div>
        </div>
      </div>

      <!-- Attitude Dynamics Ribbon -->
      <div class="telem-ribbon">
        <div class="telem-ribbon-cell">
          <div class="telem-ribbon-label">Roll</div>
          <div class="telem-ribbon-val"><span id="tRoll">--</span><span class="telem-ribbon-unit">°</span></div>
        </div>
        <div class="telem-ribbon-cell">
          <div class="telem-ribbon-label">Pitch</div>
          <div class="telem-ribbon-val"><span id="tPitch">--</span><span class="telem-ribbon-unit">°</span></div>
        </div>
        <div class="telem-ribbon-cell">
          <div class="telem-ribbon-label">Yaw</div>
          <div class="telem-ribbon-val"><span id="tYaw">--</span><span class="telem-ribbon-unit">°</span></div>
        </div>
      </div>

      <!-- Environmental Sensor Ribbon -->
      <div class="telem-ribbon">
        <div class="telem-ribbon-cell">
          <div class="telem-ribbon-label">Temp</div>
          <div class="telem-ribbon-val"><span id="tTemp">--</span><span class="telem-ribbon-unit">°C</span></div>
        </div>
        <div class="telem-ribbon-cell">
          <div class="telem-ribbon-label">Pressure</div>
          <div class="telem-ribbon-val"><span id="tPres">--</span><span class="telem-ribbon-unit">hPa</span></div>
        </div>
        <div class="telem-ribbon-cell">
          <div class="telem-ribbon-label">Humidity</div>
          <div class="telem-ribbon-val"><span id="tHum">--</span><span class="telem-ribbon-unit">%</span></div>
        </div>
      </div>

      <!-- GPS Nav Ribbon -->
      <div class="gps-bar">
        <span class="gps-badge">GPS NAV</span>
        <span id="tGps">Lat: -- &nbsp;Lon: -- &nbsp;Alt: --</span>
      </div>
    </div>

    <!-- 3D orientation viz (roll/pitch/yaw as a live-rotated box) -->
    <div class="orient3d-wrap">
      <div class="orient3d-scene" id="orient3dScene">
        <div class="orient3d-box" id="orient3dBox">
          <div class="face front">FWD</div>
          <div class="face back"></div>
          <div class="face right"></div>
          <div class="face left"></div>
          <div class="face top"></div>
          <div class="face bottom"></div>
        </div>
      </div>
    </div>

    <div style="margin-top:8px;">
      <div class="group-label" style="margin:0 0 6px;">Hull Integrity Matrix <span class="hint-mono">WATER SENSORS</span></div>
      <div class="leak-row" id="leakRow">
        <div class="leak-sensor-card">
          <div class="leak-dot" title="Bow / Front Compartment"></div>
          <div class="leak-sensor-info">
            <span class="leak-sensor-name">Bow</span>
            <span class="leak-sensor-status">DRY</span>
          </div>
        </div>
        <div class="leak-sensor-card">
          <div class="leak-dot" title="Mid / Centre Compartment"></div>
          <div class="leak-sensor-info">
            <span class="leak-sensor-name">Mid</span>
            <span class="leak-sensor-status">DRY</span>
          </div>
        </div>
        <div class="leak-sensor-card">
          <div class="leak-dot" title="Stern / Rear Compartment"></div>
          <div class="leak-sensor-info">
            <span class="leak-sensor-name">Stern</span>
            <span class="leak-sensor-status">DRY</span>
          </div>
        </div>
        <div class="leak-sensor-card">
          <div class="leak-dot" title="IMU Bay Compartment"></div>
          <div class="leak-sensor-info">
            <span class="leak-sensor-name">IMU</span>
            <span class="leak-sensor-status">DRY</span>
          </div>
        </div>
      </div>
    </div>

    <!-- Telemetry enable/disable toggle -->
    <div class="telem-toggle-row" style="margin-top:8px;padding-top:8px;">
      <span style="font-size:0.73rem;color:var(--text2);font-weight:600">Telemetry Stream</span>
      <div style="display:flex;align-items:center;gap:8px;">
        <span style="font-size:0.68rem;color:var(--accent2);font-family:var(--font-mono);font-weight:700" id="telemToggleLabel">ACTIVE</span>
        <label class="toggle-switch" title="Disable to free up bandwidth for joystick">
          <input type="checkbox" id="telemToggle" checked>
          <span class="toggle-slider"></span>
        </label>
      </div>
    </div>
  </div>

  <!-- === Commands === -->
  <div class="panel">
    <div class="panel-head">
      <div class="panel-title">
        <span class="bar">|</span>
        <span class="panel-title-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><line x1="4" y1="21" x2="4" y2="14"/><line x1="4" y1="10" x2="4" y2="3"/><line x1="12" y1="21" x2="12" y2="12"/><line x1="12" y1="8" x2="12" y2="3"/><line x1="20" y1="21" x2="20" y2="16"/><line x1="20" y1="12" x2="20" y2="3"/><line x1="1" y1="14" x2="7" y2="14"/><line x1="9" y1="8" x2="15" y2="8"/><line x1="17" y1="16" x2="23" y2="16"/></svg></span>
        Commands
      </div>
      <div class="panel-head-right">
        <span class="panel-tag">ACTUATOR // INTERLOCK</span>
      </div>
    </div>

    <!-- Rosbag recording -->
    <div class="cmd-group">
      <div class="group-label">Mission Recording</div>
      <div class="rec-indicator" id="recIndicator">
        <span class="rec-dot"></span>
        <span id="recStatus">Not recording</span>
      </div>
      <div class="btn-group">
        <button class="btn btn-ok" id="btnStartRec" onclick="sendCmd('start_recording')">
          <svg class="btn-svg" viewBox="0 0 24 24"><polygon points="6 4 20 12 6 20 6 4" fill="currentColor" stroke="none"/></svg>Start Rec
        </button>
        <button class="btn btn-danger" id="btnStopRec" onclick="sendCmd('stop_recording')">
          <svg class="btn-svg" viewBox="0 0 24 24"><rect x="5" y="5" width="14" height="14" rx="1" fill="currentColor" stroke="none"/></svg>Stop Rec
        </button>
        <button class="btn btn-warn" id="btnSaveBag" onclick="sendCmd('save_recording')">
          <svg class="btn-svg" viewBox="0 0 24 24"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg>Save Bag
        </button>
      </div>
    </div>

    <!-- Arm / Disarm -->
    <div class="cmd-group">
      <div class="group-label">Safety Interlock</div>
      <div class="btn-group" style="margin-top:0;">
        <button class="btn btn-ok" id="btnArm" onclick="sendCmd('arm')" title="Arm vehicle actuators">
          <svg class="btn-svg" viewBox="0 0 24 24"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><polyline points="9 12 11 14 15 10"/></svg>ARM VEHICLE
        </button>
        <button class="btn btn-danger" id="btnDisarm" onclick="sendCmd('disarm')" title="Disarm vehicle actuators">
          <svg class="btn-svg" viewBox="0 0 24 24"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>DISARM
        </button>
      </div>
    </div>

    <!-- Emergency stop - deliberately the most prominent control here -->
    <div class="cmd-group">
      <div class="group-label">Emergency Stop (Killswitch)</div>
      <div class="estop-strip">
        <button class="btn btn-estop" id="btnEstopOn" onclick="sendCmd('lights_on')">
          <svg class="btn-svg" viewBox="0 0 24 24"><rect x="5" y="5" width="14" height="14" rx="1" fill="currentColor" stroke="none"/></svg>E-STOP ON
        </button>
        <button class="btn btn-estop-off" id="btnEstopOff" onclick="sendCmd('lights_off')">
          <svg class="btn-svg" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><polyline points="9 12 11 14 15 10"/></svg>E-STOP OFF
        </button>
      </div>
    </div>

    <!-- Depth hold -->
    <div class="cmd-group">
      <div class="group-label">Depth Hold / Pitch Stabilization</div>
      <div class="telem-toggle-row" style="margin-top:0;">
        <span style="font-size:0.75rem;color:var(--text2);font-weight:600">Depth hold</span>
        <div style="display:flex;align-items:center;gap:8px;">
          <span style="font-size:0.7rem;color:var(--accent2);font-family:var(--font-mono);font-weight:700" id="depthHoldLabel">ON</span>
          <label class="toggle-switch" title="Turn off to give the left/right fins back to the joystick">
            <input type="checkbox" id="depthHoldToggle" checked onchange="setDepthHold(this.checked)">
            <span class="toggle-slider"></span>
          </label>
        </div>
      </div>
      <div id="depthHoldStatus" style="font-size:0.68rem;color:var(--muted);font-family:var(--font-mono);margin-top:5px;line-height:1.4;">depth_hold node not running - side fins on joystick</div>
    </div>

    <!-- Mode selector -->
    <div class="cmd-group">
      <div class="group-label">Control Mode</div>
      <div class="mode-buttons">
        <button class="btn active" id="modeManual"  onclick="setMode('manual')">Manual</button>
        <button class="btn"        id="modeAuto"    onclick="setMode('auto')">Auto</button>
        <button class="btn"        id="modeTeleop"  onclick="setMode('teleop')">Teleop</button>
      </div>

      <!-- System info -->
      <div class="sysinfo">
        Mode: <span id="sysMode">--</span>
        <span class="sysinfo-sep">|</span>
        Armed: <span id="sysArmed">--</span>
        <span class="sysinfo-sep">|</span>
        Errors: <span id="sysErrors">none</span>
      </div>
    </div>

    <!-- Reboot (whole Jetson/AUV system) -->
    <div class="cmd-group">
      <div class="group-label">System Power &amp; Reboot</div>
      <button class="btn btn-danger btn-block" onclick="rebootSystem()">
        <svg class="btn-svg" viewBox="0 0 24 24"><path d="M21 12a9 9 0 1 1-2.64-6.36"/><polyline points="21 3 21 9 15 9"/></svg>REBOOT SYSTEM
      </button>
      <div class="hint-text">Reboots the entire AUV/Jetson - the tether link will drop until it comes back up.</div>
    </div>
  </div>

  <!-- === File Storage (Column 3) === -->
  <div class="panel">
    <div class="panel-head">
      <div class="panel-title">
        <span class="bar">|</span>
        <span class="panel-title-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg></span>
        File Storage
      </div>
      <div class="panel-head-right">
        <span class="panel-tag">JETSON // FS-XFER</span>
      </div>
    </div>

    <!-- Upload -->
    <div class="group-label" style="margin-top:0;">Upload to Vehicle</div>
    <div class="file-upload-row">
      <input type="file" id="fileInput" />
      <button class="btn" onclick="uploadFile()">
        <svg class="btn-svg" viewBox="0 0 24 24"><path d="M12 19V5"/><polyline points="5 12 12 5 19 12"/></svg>Upload
      </button>
    </div>
    <div class="progress-wrap">
      <div class="progress-track"><div class="progress-fill" id="upProgress"></div></div>
      <div class="file-status" id="upStatus">Ready</div>
    </div>

    <div class="group-label" style="margin:12px 0 8px;">Remote Jetson Directory
      <button class="btn btn-sm" onclick="refreshFiles()">
        <svg class="btn-svg" viewBox="0 0 24 24"><path d="M21 12a9 9 0 1 1-2.64-6.36"/><polyline points="21 3 21 9 15 9"/></svg>Refresh
      </button>
    </div>
    <div id="fileTree" class="file-tree">
      <div class="dashed-empty">
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" style="opacity:0.65;">
          <path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/>
        </svg>
        <div style="font-weight:700;color:var(--text2);font-size:0.74rem;">Click Refresh to load files</div>
        <div style="font-size:0.66rem;color:var(--muted);">Query onboard Jetson mission logs &amp; rosbag archives</div>
      </div>
    </div>
    <div class="file-status" id="dlStatus" style="margin-top:4px;"></div>
    <div class="progress-wrap" style="margin-top:3px;">
      <div class="progress-track"><div class="progress-fill" id="dlProgress"></div></div>
    </div>
  </div>
</div><!-- /.grid-main -->


<!-- === Bottom row: Manual Flight & Guidance Bay === -->
<div class="grid-bottom">

  <!-- === Panel 1: Physical Joystick === -->
  <div class="panel">
    <div class="panel-head">
      <div class="panel-title">
        <span class="bar">|</span>
        <span class="panel-title-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="6" r="3"/><path d="M12 9v7"/><path d="M8 16h8l2 4H6l2-4z"/></svg></span>
        Physical Joystick <span id="joyName" class="joy-device-tag"></span>
      </div>
      <div class="panel-head-right">
        <span class="panel-tag">HID-INPUT // RELAY</span>
      </div>
    </div>
    <div id="joyDisconnected" class="joy-empty-bay">
      <svg class="joy-empty-icon" width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round">
        <circle cx="12" cy="6" r="3"/>
        <path d="M12 9v7"/>
        <path d="M8 16h8l2 4H6l2-4z"/>
      </svg>
      <div class="joy-empty-title">No joystick detected</div>
      <div class="joy-empty-sub">Plug a joystick into this laptop and it will appear here.</div>
    </div>
    <div id="joyViz" class="joy-instrument-bay" style="display:none;">
      <div class="joy-top-row">
        <div class="joy-stick-bezel">
          <canvas id="joyStickCanvas" width="128" height="128"></canvas>
        </div>
        <div class="joy-axes-bars" id="joyAxesBars"></div>
      </div>
      <div class="joy-divider"></div>
      <div class="joy-buttons" id="joyBtns"></div>
    </div>
    <div class="telem-toggle-row" style="margin-top:auto;">
      <span style="font-size:0.74rem;color:var(--text2);font-weight:600">Relay to AUV</span>
      <div style="display:flex;align-items:center;gap:8px;">
        <span style="font-size:0.68rem;color:var(--accent2);font-family:var(--font-mono);font-weight:700" id="joyRelayLabel">ON</span>
        <label class="toggle-switch" title="Disable to stop driving the vehicle from this stick without unplugging it">
          <input type="checkbox" id="joyRelayToggle" checked onchange="setJoyRelay(this.checked)">
          <span class="toggle-slider"></span>
        </label>
      </div>
    </div>
  </div>

  <!-- === Panel 2: Virtual Joystick === -->
  <div class="panel vjoy-panel">
    <div class="panel-head">
      <div class="panel-title">
        <span class="bar">|</span>
        <span class="panel-title-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><line x1="12" y1="3" x2="12" y2="7"/><line x1="12" y1="17" x2="12" y2="21"/><line x1="3" y1="12" x2="7" y2="12"/><line x1="17" y1="12" x2="21" y2="12"/><circle cx="12" cy="12" r="2.5"/></svg></span>
        Virtual Joystick
      </div>
      <div class="panel-head-right">
        <span class="panel-tag">BACKUP // OVERRIDE</span>
      </div>
    </div>

    <div class="vjoy-main-row">
      <div class="vjoy-bezel">
        <canvas id="vjoyCanvas" width="198" height="198"></canvas>
      </div>

      <div class="vjoy-thruster-card">
        <span class="vjoy-thruster-label">Thruster</span>
        <div class="vjoy-thruster-bay">
          <input type="range" class="vjoy-thruster-slider" id="vjoyThruster" min="-100" max="100" value="0" oninput="vjoyUpdate()">
        </div>
        <span class="vjoy-thruster-val" id="vjoyThrusterVal">0.0</span>
      </div>
    </div>

    <div class="vjoy-toggle-strip" style="margin-top:auto;">
      <label for="vjoyEnabled" class="vjoy-toggle-label">Enable virtual joystick</label>
      <div style="display:flex;align-items:center;">
        <span class="vjoy-toggle-status" id="vjoyToggleStatus">OFF</span>
        <label class="toggle-switch">
          <input type="checkbox" id="vjoyEnabled" onchange="vjoyToggleChanged(this.checked)">
          <span class="toggle-slider"></span>
        </label>
      </div>
    </div>

    <button class="vjoy-stop-full" id="vjoyStopBtn" onclick="vjoyStop()">
      <span class="vjoy-stop-square">&#9632;</span> Stop All
    </button>
  </div>

  <!-- === Panel 3: Link Channels === -->
  <div class="panel">
    <div class="panel-head">
      <div class="panel-title">
        <span class="bar">|</span>
        <span class="panel-title-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="2" width="20" height="8" rx="2" ry="2"/><rect x="2" y="14" width="20" height="8" rx="2" ry="2"/><line x1="6" y1="6" x2="6.01" y2="6"/><line x1="6" y1="18" x2="6.01" y2="18"/></svg></span>
        Link Channels
      </div>
      <div class="panel-head-right">
        <span class="panel-tag">RS485 // MULTI-LINK</span>
        <button class="btn btn-sm" id="linkAutoBtn" onclick="clearForcedChannel()" title="Clear any pinned channel and return to automatic active-channel selection">
          <svg class="btn-svg" viewBox="0 0 24 24"><path d="M21 12a9 9 0 1 1-2.64-6.36"/><polyline points="21 3 21 9 15 9"/></svg>Auto
        </button>
      </div>
    </div>

    <div class="telem-toggle-row" style="margin-top:0;">
      <span style="font-size:0.73rem;color:var(--text2);font-weight:600" title="Operator override - CH-A/B/C/test only, manual connections are never affected">Primary tether</span>
      <div style="display:flex;align-items:center;gap:8px;">
        <span style="font-size:0.68rem;color:var(--accent2);font-family:var(--font-mono);font-weight:700" id="primaryTetherLabel">ON</span>
        <label class="toggle-switch" title="Default on - disabling forces the primary tether channels down so you can test a manual connection in isolation">
          <input type="checkbox" id="primaryTetherToggle" checked onchange="setPrimaryTether(this.checked)">
          <span class="toggle-slider"></span>
        </label>
      </div>
    </div>

    <div class="link-subtabs">
      <button class="link-subtab active" data-linktab="status" onclick="showLinkTab('status')">Status</button>
      <button class="link-subtab" data-linktab="manual" onclick="showLinkTab('manual')">Manual Connection</button>
    </div>

    <div id="linktab-status">
      <div id="channelList" class="channel-list" style="margin-top:6px;">
        <span style="color:var(--muted);font-size:0.72rem">Loading link channels...</span>
      </div>
      <div class="group-label" style="margin:10px 0 6px;">Secondary Feed &nbsp;<span id="secChName" style="color:var(--muted)">(--)</span></div>
      <div id="secondaryLog" class="secondary-log">
        <span style="color:var(--muted);font-size:0.72rem">No secondary channel connected</span>
      </div>
    </div>

    <div id="linktab-manual" style="display:none;">
      <div style="font-size:0.67rem;color:var(--muted);margin:6px 0;">
        Backup links only - the tether above stays the default connection always. Ethernet or WiFi (SSH) provides fast bulk transfers.
      </div>
      <div class="manual-mode-tabs">
        <button class="manual-mode-tab active" data-mode="tethered"  onclick="setManualMode('tethered')">Tethered/RS-485</button>
        <button class="manual-mode-tab"        data-mode="ethernet"  onclick="setManualMode('ethernet')">Ethernet</button>
        <button class="manual-mode-tab"        data-mode="ssh"       onclick="setManualMode('ssh')">WiFi (SSH)</button>
        <button class="manual-mode-tab"        data-mode="localhost" onclick="setManualMode('localhost')">Localhost</button>
        <button class="manual-mode-tab"        data-mode="websocket" onclick="setManualMode('websocket')">WebSocket</button>
      </div>

      <div id="mfHostPort" class="manual-field-group">
        <input type="text"   id="mfHost" class="manual-input" placeholder="IP address" autocomplete="off">
        <input type="number" id="mfPort" class="manual-input manual-input-narrow" placeholder="Port" autocomplete="off">
      </div>
      <div id="mfSshExtra" class="manual-field-group" style="display:none;">
        <input type="text"     id="mfUsername" class="manual-input" placeholder="Username" autocomplete="off">
        <input type="password" id="mfPassword" class="manual-input" placeholder="Password" autocomplete="off">
      </div>
      <div id="mfWsUrl" class="manual-field-group" style="display:none;">
        <input type="text" id="mfUrl" class="manual-input" placeholder="ws://host:port/" autocomplete="off">
      </div>
      <div class="manual-field-group">
        <input type="text" id="mfLabel" class="manual-input" placeholder="Label (optional)" autocomplete="off">
      </div>
      <button class="btn btn-ok btn-block" style="margin-top:6px;" onclick="manualConnect()">
        <svg class="btn-svg" viewBox="0 0 24 24"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>Connect
      </button>
      <div id="manualConnectStatus" style="font-size:0.7rem;margin-top:4px;min-height:1em;"></div>

      <div style="border-top:1px solid var(--border);margin:8px 0;"></div>
      <div class="card-label" style="margin-bottom:5px;">Active Manual Connections</div>
      <div id="manualChannelList" class="channel-list">
        <span style="color:var(--muted);font-size:0.72rem">None</span>
      </div>
    </div>
  </div>
</div><!-- /.grid-bottom -->

<!-- === Wifi Comms & Hardware Management === -->
<div class="panel" style="margin-top:14px;">
  <div class="panel-head">
    <div class="panel-title">
      <span class="bar">|</span>
      <span class="panel-title-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12.55a11 11 0 0 1 14.08 0"/><path d="M1.42 9a16 16 0 0 1 21.16 0"/><path d="M8.53 16.11a6 6 0 0 1 6.95 0"/><line x1="12" y1="20" x2="12.01" y2="20"/></svg></span>
      Wifi Comms
    </div>
    <div class="panel-head-right">
      <span class="panel-tag">NMCLI // RF-LINK</span>
      <button class="btn btn-sm" onclick="wifiListDevices()" id="wifiScanBtn">
        <svg class="btn-svg" viewBox="0 0 24 24"><path d="M21 12a9 9 0 1 1-2.64-6.36"/><polyline points="21 3 21 9 15 9"/></svg>List Devices
      </button>
    </div>
  </div>

  <div class="wifi-grid">
    <!-- Column 1: Controls & Manual Connect -->
    <div class="wifi-col">
      <div class="card-label" style="margin-bottom:4px;">Send To</div>
      <div class="gcs-select-wrap" id="wifiChannelSelectWrap" title="Which channel wifi commands actually go over.">
        <select id="wifiChannelSelect" class="gcs-select-native" onchange="renderWifiTarget(_lastChannels||[], _lastActiveChannel)">
          <option value="">Auto</option>
        </select>
        <button type="button" class="gcs-select-trigger" id="wifiChannelSelectTrigger" onclick="toggleWifiSelectMenu(event)">
          <span class="gcs-select-trigger-left">
            <span class="gcs-select-icon">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 3 21 3 21 8"/><line x1="4" y1="20" x2="21" y2="3"/><polyline points="21 16 21 21 16 21"/><line x1="15" y1="15" x2="21" y2="21"/><line x1="4" y1="4" x2="9" y2="9"/></svg>
            </span>
            <span class="gcs-select-text-col">
              <span class="gcs-select-val" id="wifiChannelSelectVal">Auto (Active Link)</span>
              <span class="gcs-select-sub" id="wifiChannelSelectSub">Automatic priority channel routing</span>
            </span>
          </span>
          <span class="gcs-select-trigger-right">
            <span class="gcs-select-badge" id="wifiChannelSelectBadge">AUTO-ROUTE</span>
            <span class="gcs-select-chevron-box">
              <svg class="gcs-select-chevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg>
            </span>
          </span>
        </button>
        <div class="gcs-select-menu" id="wifiChannelSelectMenu"></div>
      </div>

      <div class="wifi-target-box" title="Every button in this panel runs nmcli on whichever device is at the other end of THIS channel.">
        <div class="card-label" style="margin-bottom:3px;">Target</div>
        <div id="wifiTargetLabel" style="font-family:var(--font-mono);font-size:0.74rem;color:var(--danger);font-weight:600;">not connected</div>
      </div>

      <div class="card-label" style="margin-top:10px;margin-bottom:4px;" title="Only sent along with wifi commands - never stored, never logged">Sudo Password</div>
      <input type="password" id="wifiSudoPassword" class="manual-input" style="width:100%;flex:none;" placeholder="sudo password" autocomplete="off">

      <div class="group-label" style="margin-top:12px;margin-bottom:6px;">Connect Manually</div>
      <div class="manual-field-group" style="margin-top:0;">
        <input type="text" id="wifiManualSsid" class="manual-input" placeholder="SSID" autocomplete="off">
        <input type="password" id="wifiManualPassword" class="manual-input" placeholder="Password" autocomplete="off">
      </div>
      <button class="btn btn-ok btn-block" style="margin-top:8px;" onclick="wifiManualConnect()">
        <svg class="btn-svg" viewBox="0 0 24 24"><path d="M5 12.55a11 11 0 0 1 14.08 0"/><path d="M8.53 16.11a6 6 0 0 1 6.95 0"/><line x1="12" y1="20" x2="12.01" y2="20"/></svg>Connect Network
      </button>
      <div id="wifiManualStatus" class="field-status"></div>
      <div id="wifiStatusMsg" class="wifi-status"></div>
    </div>

    <!-- Column 2: Available Networks -->
    <div class="wifi-col">
      <div class="card-label" style="margin-bottom:6px;">Available Networks</div>
      <div id="wifiNetworkList" class="wifi-dashed-box">Click "List Devices" to scan for wifi networks</div>
    </div>

    <!-- Column 3: Saved Devices -->
    <div class="wifi-col">
      <div class="card-label" style="margin-bottom:6px;">Saved Devices</div>
      <div id="wifiSavedList" class="wifi-dashed-box">None yet</div>
    </div>
  </div>
</div>

<!-- === Diagnostic Supervisor Row 1: Topics & Black Box === -->
<div class="grid-tools" style="margin-top:14px;">

  <!-- === ROS2 Topics === -->
  <div class="panel">
    <div class="panel-head">
      <div class="panel-title">
        <span class="bar">|</span>
        <span class="panel-title-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><line x1="6" y1="3" x2="6" y2="15"/><circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/></svg></span>
        ROS2 Topics <span class="panel-inline-sub">live graph, Jetson side</span>
      </div>
      <div class="panel-head-right">
        <span class="panel-tag">DDS-GRAPH // JETSON</span>
        <button class="btn btn-sm" onclick="refreshTopics()">
          <svg class="btn-svg" viewBox="0 0 24 24"><path d="M21 12a9 9 0 1 1-2.64-6.36"/><polyline points="21 3 21 9 15 9"/></svg>Refresh
        </button>
      </div>
    </div>
    <div id="topicList" class="topic-list">
      <div class="dashed-empty">Click Refresh to load topics</div>
    </div>
  </div>

  <!-- === Black Box Console === -->
  <div class="panel">
    <div class="panel-head">
      <div class="panel-title">
        <span class="bar">|</span>
        <span class="panel-title-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg></span>
        Black Box Console
      </div>
      <div class="panel-head-right">
        <span class="panel-tag">LOG-STREAM // EVENT</span>
        <span class="hint-mono" id="bbStreamLabel">OFF</span>
        <label class="toggle-switch" title="Bandwidth gated: enables real-time link and fault event streaming">
          <input type="checkbox" id="bbStreamToggle" onchange="setBlackboxStream(this.checked)">
          <span class="toggle-slider"></span>
        </label>
      </div>
    </div>
    <div id="blackboxConsole" class="console-box" style="min-height:195px;flex:1;">
      <span style="color:var(--muted)">Streaming is OFF - enable the switch above to receive events</span>
    </div>
  </div>
</div>

<!-- === Diagnostic Row 2: Parameters & Compass (2 columns) === -->
<div class="grid-tools grid-tools-wide" style="margin-top:14px;">

  <!-- === ROS2 Parameters === -->
  <div class="panel">
    <div class="panel-head">
      <div class="panel-title">
        <span class="bar">|</span>
        <span class="panel-title-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg></span>
        ROS2 Parameters
      </div>
      <div class="panel-head-right">
        <span class="panel-tag">PARAM-SRV // YAML</span>
        <button class="btn btn-sm" onclick="refreshParams()">
          <svg class="btn-svg" viewBox="0 0 24 24"><path d="M21 12a9 9 0 1 1-2.64-6.36"/><polyline points="21 3 21 9 15 9"/></svg>Refresh
        </button>
      </div>
    </div>
    <div id="paramList" class="param-list-wrap">
      <div class="dashed-empty">Click Refresh to load parameters</div>
    </div>
  </div>

  <!-- === Compass === -->
  <div class="panel compass-panel">
    <div class="panel-head">
      <div class="panel-title">
        <span class="bar">|</span>
        <span class="panel-title-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polygon points="16.24 7.76 14.12 14.12 7.76 16.24 9.88 9.88 16.24 7.76"/></svg></span>
        Compass
      </div>
      <div class="panel-head-right">
        <span class="panel-tag">MAG-HDG // 360°</span>
      </div>
    </div>
    <div class="compass-wrap">
      <div class="compass-bezel">
        <canvas id="compassCanvas" width="164" height="164"></canvas>
      </div>
      <div class="compass-heading-card">
        <div class="compass-heading-label">HEADING</div>
        <div class="compass-heading"><span id="compassHeadingVal">--</span><span>&deg;</span></div>
      </div>
      <div class="compass-toggle-row">
        <label for="magStreamToggle">Mag data stream</label>
        <div style="display:flex;align-items:center;gap:8px;">
          <span id="magStreamLabel">OFF</span>
          <label class="toggle-switch" title="Stream raw magnetometer telemetry">
            <input type="checkbox" id="magStreamToggle" onchange="setMagStream(this.checked)">
            <span class="toggle-slider"></span>
          </label>
        </div>
      </div>
    </div>
  </div>

</div>

<!-- === Diagnostic Row 3: Bad Data & Remote Terminal (2 columns) === -->
<div class="grid-tools grid-tools-term" style="margin-top:14px;">

  <!-- === Bad Data === -->
  <div class="panel">
    <div class="panel-head">
      <div class="panel-title">
        <span class="bar">|</span>
        <span class="panel-title-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg></span>
        Bad Data
      </div>
      <div class="panel-head-right">
        <span class="panel-tag">CRC-DIAG // WIRE</span>
        <button class="btn btn-sm" onclick="clearBadDataConsole()">
          <svg class="btn-svg" viewBox="0 0 24 24"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/></svg>Clear
        </button>
      </div>
    </div>
    <div style="font-size:0.70rem;color:var(--muted);margin:2px 0 10px;line-height:1.45;">
      Corrupted/unparseable data actually received on the wire - CRC checksum mismatches (RS-485 noise or tether interference), malformed JSON, or unrecognized packet types. Always on - this is exactly the diagnostic you need when the link is degraded.
    </div>
    <div id="badDataConsole" class="console-box" style="min-height:200px;flex:1;">
      <span style="color:var(--muted)">No bad data seen yet</span>
    </div>
  </div>

  <!-- === Remote Terminal === -->
  <div class="panel">
    <div class="panel-head">
      <div class="panel-title">
        <span class="bar">|</span>
        <span class="panel-title-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/></svg></span>
        Remote Terminal <span class="panel-inline-sub">Jetson</span>
      </div>
      <div class="panel-head-right">
        <span class="panel-tag">PTY-EXEC // JETSON</span>
      </div>
    </div>
    <div style="font-size:0.70rem;color:var(--muted);margin:2px 0 10px;line-height:1.45;">
      Runs in its own process on the Jetson - never blocks the running control node. Output capped to the last N lines (below, default 50).
    </div>
    <div id="terminalConsole" class="console-box" style="min-height:200px;flex:1;">
      <span style="color:var(--muted)">No commands run yet</span>
    </div>
    <div style="display:flex;gap:8px;margin-top:10px;align-items:flex-end;">
      <div style="display:flex;align-items:center;gap:6px;flex:1;min-width:0;">
        <span style="font-family:var(--font-mono);color:var(--accent2);font-size:.82rem;font-weight:700;">$</span>
        <input type="text" id="termInput" placeholder="e.g. ros2 topic list" autocomplete="off"
          style="flex:1;min-width:0;height:32px;background:var(--console-bg);border:1px solid var(--border);color:var(--text);padding:0 10px;border-radius:var(--radius-sm);font-family:var(--font-mono);font-size:.75rem;">
      </div>
      <div class="term-param-col">
        <span class="term-param-label">TIMEOUT (S)</span>
        <input type="number" id="termTimeout" placeholder="30s" min="1" max="300" title="Timeout in seconds (default 30, max 300)"
          style="width:68px;height:32px;background:var(--console-bg);border:1px solid var(--border);color:var(--text);padding:0 8px;border-radius:var(--radius-sm);font-size:.75rem;font-family:var(--font-mono);">
      </div>
      <div class="term-param-col">
        <span class="term-param-label">MAX LINES</span>
        <input type="number" id="termMaxLines" placeholder="50 ln" min="1" max="1000" title="Max output lines kept, tail (default 50, max 1000)"
          style="width:68px;height:32px;background:var(--console-bg);border:1px solid var(--border);color:var(--text);padding:0 8px;border-radius:var(--radius-sm);font-size:.75rem;font-family:var(--font-mono);">
      </div>
      <button class="btn btn-ok" style="height:32px;padding:0 16px;" onclick="runTerminalCommand()">
        <svg class="btn-svg" viewBox="0 0 24 24"><polygon points="6 4 20 12 6 20 6 4" fill="currentColor" stroke="none"/></svg>Run
      </button>
    </div>
  </div>

</div>

<!-- === GCS Footer === -->
<footer class="gcs-footer">
  <div class="footer-brand">
    <svg class="footer-logo" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <circle cx="12" cy="12" r="9"/>
      <path d="M12 3v6m0 6v6M3 12h6m6 0h6"/>
      <circle cx="12" cy="12" r="2" fill="currentColor"/>
    </svg>
    <span>AUV GROUND CONTROL</span>
    <span class="panel-tag">GCS-T2 // REV 4.2</span>
  </div>
  <div class="footer-meta">
    <span class="panel-tag">RS485-TETHER // ROS2 HUMBLE</span>
    <span>TITAN-II // MISSION COMMAND</span>
  </div>
</footer>

<!-- === Reboot sudo-password modal === -->
<div class="modal-overlay" id="rebootModal">
  <div class="modal-box">
    <div class="modal-title" style="display:flex;align-items:center;gap:8px;">
      <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="var(--warn)" stroke-width="2"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
      Elevated Authentication Required
    </div>
    <div class="modal-body" id="rebootModalPrompt">Passwordless sudo is not enabled on the Jetson companion computer. Enter administrative credentials:</div>
    <input type="password" class="modal-input" id="rebootPasswordInput" autocomplete="off" placeholder="sudo password">
    <div class="modal-actions">
      <button class="btn" onclick="closeRebootModal()">Cancel</button>
      <button class="btn btn-danger" onclick="submitRebootPassword()">Authorize Reboot</button>
    </div>
  </div>
</div>

</div><!-- /#tab-liveops -->

<div id="tab-missionplanner" class="top-tab-content">

  <style>
    .mission-sup-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin-top: 12px;
    }
    @media (max-width: 1100px) {
      .mission-sup-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    .mission-sup-card {
      background: linear-gradient(180deg, rgba(20, 31, 48, 0.78) 0%, rgba(12, 19, 31, 0.92) 100%);
      border: 1px solid var(--border);
      border-radius: 7px;
      padding: 10px 12px;
      display: flex;
      flex-direction: column;
      gap: 5px;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04);
    }
    [data-theme="light"] .mission-sup-card {
      background: linear-gradient(180deg, #FFFFFF 0%, #F8FAFC 100%);
      border-color: #CBD5E1;
      box-shadow: inset 0 1px 0 #FFFFFF, 0 1px 2px rgba(15, 23, 42, 0.04);
    }
    .mission-sup-card-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 6px;
      font-family: var(--font-mono);
      font-size: 0.62rem;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
    }
    .mission-sup-val-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
    }
    .mission-sup-val {
      font-family: var(--font-mono);
      font-size: 0.84rem;
      font-weight: 800;
      color: var(--text);
      letter-spacing: 0.02em;
      display: inline-flex;
      align-items: center;
      gap: 7px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .mission-sup-dot {
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: #38BDF8;
      box-shadow: 0 0 6px rgba(56, 189, 248, 0.6);
      flex-shrink: 0;
    }
    [data-theme="light"] .mission-sup-dot {
      background: #0284C7;
      box-shadow: 0 0 0 2px rgba(2, 132, 199, 0.18);
    }
    .mission-sup-sub {
      font-family: var(--font-mono);
      font-size: 0.64rem;
      color: var(--muted);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .mission-sup-prog-track {
      width: 100%;
      height: 5px;
      border-radius: 999px;
      background: rgba(148, 163, 184, 0.16);
      overflow: hidden;
      margin-top: 2px;
    }
    [data-theme="light"] .mission-sup-prog-track {
      background: #E2E8F0;
    }
    .mission-sup-prog-bar {
      height: 100%;
      width: 0%;
      border-radius: 999px;
      background: linear-gradient(90deg, #0284C7 0%, #10B981 100%);
      transition: width 0.25s ease;
    }
    .mission-console-wrap {
      margin-top: 10px;
      border: 1px solid var(--border);
      border-radius: 7px;
      overflow: hidden;
      background: var(--bg-console, #070C15);
    }
    [data-theme="light"] .mission-console-wrap {
      background: #F8FAFC;
      border-color: #CBD5E1;
    }
    .mission-console-bar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      padding: 6px 12px;
      background: rgba(15, 23, 42, 0.65);
      border-bottom: 1px solid var(--border);
      font-family: var(--font-mono);
      font-size: 0.64rem;
      font-weight: 700;
      letter-spacing: 0.07em;
      text-transform: uppercase;
      color: var(--muted);
    }
    [data-theme="light"] .mission-console-bar {
      background: #F1F5F9;
      border-bottom-color: #CBD5E1;
      color: #475569;
    }
    .mission-console-bar-left {
      display: inline-flex;
      align-items: center;
      gap: 7px;
    }
    .mission-console-bar-right {
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }
  </style>

  <div class="panel" style="padding:0;overflow:hidden;border:1px solid var(--border);border-radius:var(--radius-md);">
    <iframe id="mapPlannerFrame" src="/mission-planner/map-planner" style="width:100%;height:calc(100vh - 175px);min-height:675px;border:none;display:block;"></iframe>
  </div>
  <div class="panel" style="margin-top:14px;">
    <div class="panel-head">
      <div class="panel-title">
        <span class="bar">|</span>
        <span class="panel-title-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><polygon points="3 11 22 2 13 21 11 13 3 11"/></svg></span>
        Mission Execution &amp; Waypoint Supervisor
      </div>
      <div class="panel-head-right">
        <span class="panel-tag">NAV-EXEC // ACTION</span>
        <div class="btn-group" style="margin-top:0;">
          <button class="btn" onclick="cancelMission()" title="Cancel currently executing mission goal">
            <svg class="btn-svg" viewBox="0 0 24 24"><rect x="6" y="6" width="12" height="12" rx="1.5" fill="currentColor" stroke="none"/></svg>Cancel Active Mission
          </button>
          <button class="btn btn-warn" onclick="cancelAllMissions()" title="Cancels EVERY goal the mission action server has, not just the last one started.">
            <svg class="btn-svg" viewBox="0 0 24 24"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>Abort All Goals
          </button>
          <button class="btn btn-estop" onclick="sendCmd('lights_on')" title="Immediate thruster shutdown. Same E-STOP ON as Live Ops.">
            <svg class="btn-svg" viewBox="0 0 24 24"><polygon points="7.86 2 16.14 2 22 7.86 22 16.14 16.14 22 7.86 22 2 16.14 2 7.86 7.86 2"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>E-STOP ON
          </button>
        </div>
      </div>
    </div>

    <!-- 4-Column Mission Supervisor Telemetry Strip -->
    <div class="mission-sup-grid">
      <div class="mission-sup-card">
        <div class="mission-sup-card-head">
          <span>Supervisor State</span>
          <span class="panel-tag" style="padding:1px 6px;font-size:0.58rem;">BT-EXEC</span>
        </div>
        <div class="mission-sup-val-row">
          <div class="mission-sup-val"><span class="mission-sup-dot" id="supStateDot"></span><span id="supExecState">STANDBY // READY</span></div>
        </div>
        <div class="mission-sup-sub" id="supExecSub">ROS2 Action Server // RunMissionTree</div>
      </div>

      <div class="mission-sup-card">
        <div class="mission-sup-card-head">
          <span>Waypoint Navigation</span>
          <span id="supPctBadge" style="color:var(--accent);font-weight:800;">0.0%</span>
        </div>
        <div class="mission-sup-val-row">
          <div class="mission-sup-val" id="supWpProgress">0 / 0 WP</div>
        </div>
        <div class="mission-sup-prog-track"><div class="mission-sup-prog-bar" id="supProgressBar"></div></div>
      </div>

      <div class="mission-sup-card">
        <div class="mission-sup-card-head">
          <span>Active Mission Map</span>
          <span class="panel-tag" style="padding:1px 6px;font-size:0.58rem;">NAV2-GRID</span>
        </div>
        <div class="mission-sup-val-row">
          <div class="mission-sup-val" id="supActiveMap">NONE DISPATCHED</div>
        </div>
        <div class="mission-sup-sub" id="supAnchorCoords">WGS84 Anchor: Awaiting Dispatch</div>
      </div>

      <div class="mission-sup-card">
        <div class="mission-sup-card-head">
          <span>Last Telemetry Event</span>
          <span id="supLastEventTs">--:--:--</span>
        </div>
        <div class="mission-sup-val-row">
          <div class="mission-sup-val" id="supLastEventTitle">IDLE // MONITOR</div>
        </div>
        <div class="mission-sup-sub" id="supLastEventSummary">Awaiting action feedback stream</div>
      </div>
    </div>

    <!-- Machined Event Stream Console -->
    <div class="mission-console-wrap">
      <div class="mission-console-bar">
        <div class="mission-console-bar-left">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/></svg>
          <span>Action Server Event Stream // Live Feedback Log</span>
        </div>
        <div class="mission-console-bar-right">
          <span class="panel-tag" id="supStreamPill" style="padding:1px 7px;font-size:0.58rem;">STREAM READY</span>
          <button class="btn btn-sm" onclick="clearMissionLog()" style="min-height:22px;padding:2px 8px;font-size:0.62rem;">Clear Log</button>
        </div>
      </div>
      <div id="missionStatusBox" class="console-box" style="height:115px;margin-top:0;border:none;border-radius:0;font-size:.73rem;padding:10px 12px;">
        <div class="console-line" id="missionEmptyLine" style="color:var(--muted);display:flex;align-items:center;gap:8px;">
          <span style="opacity:0.7;">[--:--:--]</span>
          <span class="panel-tag" style="padding:1px 6px;font-size:0.58rem;">SYS-READY</span>
          <span>Mission Execution Supervisor online — awaiting mission dispatch or waypoint telemetry.</span>
        </div>
      </div>
    </div>
  </div>

</div><!-- /#tab-missionplanner -->

<script>
// ================================================================
// Mission Planner - deliberately minimal: Map Planner (in the iframe)
// holds the map + start/goal pose and drives upload/start itself (calls
// back into this page's logMissionStatus() via window.parent, since the
// iframe is same-origin). This page only owns the shared status console
// and the Cancel button.
// ================================================================
function showTopTab(name) {
  document.querySelectorAll('.top-tab').forEach(b => b.classList.toggle('active', b.dataset.toptab === name));
  document.querySelectorAll('.top-tab-content').forEach(el => el.classList.toggle('active', el.id === 'tab-' + name));
}

async function cancelMission() {
  const r = await fetch('/api/mission/cancel', { method: 'POST' });
  const j = await r.json();
  logMissionStatus(j.error ? ('Cancel failed: ' + j.error) : 'Cancel requested.', !!j.error);
}

async function cancelAllMissions() {
  const r = await fetch('/api/mission/cancel_all', { method: 'POST' });
  const j = await r.json();
  logMissionStatus(j.error ? ('Cancel-all failed: ' + j.error) : 'Cancel-all requested (clearing every mission goal on the vehicle).', !!j.error);
}

function clearMissionLog() {
  const box = document.getElementById('missionStatusBox');
  if (!box) return;
  box.innerHTML = `<div class="console-line" id="missionEmptyLine" style="color:var(--muted);display:flex;align-items:center;gap:8px;">
    <span style="opacity:0.7;">[${new Date().toLocaleTimeString()}]</span>
    <span class="panel-tag" style="padding:1px 6px;font-size:0.58rem;">CLEARED</span>
    <span>Mission telemetry log cleared — supervisor standing by.</span>
  </div>`;
}

function logMissionStatus(text, isErr) {
  const box = document.getElementById('missionStatusBox');
  const emptyEl = document.getElementById('missionEmptyLine');
  if (emptyEl) emptyEl.remove();
  const ts = new Date().toLocaleTimeString();
  const line = document.createElement('div');
  line.className = 'console-line';
  line.style.color = isErr ? 'var(--danger)' : '';
  line.textContent = '[' + ts + '] ' + text;
  box.appendChild(line);
  box.scrollTop = box.scrollHeight;

  // Update supervisor KPI strip
  const tsEl = document.getElementById('supLastEventTs');
  const titleEl = document.getElementById('supLastEventTitle');
  const sumEl = document.getElementById('supLastEventSummary');
  const stateEl = document.getElementById('supExecState');
  const dotEl = document.getElementById('supStateDot');
  if (tsEl) tsEl.textContent = ts;
  if (sumEl) sumEl.textContent = text;
  if (titleEl) titleEl.textContent = isErr ? 'ERROR // ALERT' : 'EVENT // LOGGED';
  if (isErr && stateEl && dotEl) {
    stateEl.textContent = 'ALERT // CHECK LOG';
    dotEl.style.background = '#EF4444';
    dotEl.style.boxShadow = '0 0 6px rgba(239,68,68,0.6)';
  } else if (text.toLowerCase().includes('started')) {
    if (stateEl) stateEl.textContent = 'ACTIVE // EXECUTING';
    if (dotEl) {
      dotEl.style.background = '#10B981';
      dotEl.style.boxShadow = '0 0 6px rgba(16,185,129,0.6)';
    }
    const mapMatch = text.match(/map=["']?([^"'\s)]+)/i);
    const mapEl = document.getElementById('supActiveMap');
    if (mapMatch && mapEl) mapEl.textContent = mapMatch[1].toUpperCase();
    const startMatch = text.match(/start=\(([^)]+)\)/i);
    const anchorEl = document.getElementById('supAnchorCoords');
    if (startMatch && anchorEl) anchorEl.textContent = 'WGS84 Anchor: ' + startMatch[1];
  } else if (text.toLowerCase().includes('cancel')) {
    if (stateEl) stateEl.textContent = 'CANCELLED // HALTED';
    if (dotEl) {
      dotEl.style.background = '#F59E0B';
      dotEl.style.boxShadow = '0 0 6px rgba(245,158,11,0.6)';
    }
  }
}

let lastMissionResultTs = null;
let lastMissionFeedbackStr = null;
async function pollMissionStatus() {
  try {
    const r = await fetch('/api/mission/status');
    const j = await r.json();
    if (j.feedback) {
      const str = JSON.stringify(j.feedback);
      if (str !== lastMissionFeedbackStr) {
        lastMissionFeedbackStr = str;
        const fb = j.feedback;
        const pct = Math.max(0, Math.min(100, +(fb.progress || 0)));
        const pctEl = document.getElementById('supPctBadge');
        const barEl = document.getElementById('supProgressBar');
        const wpEl = document.getElementById('supWpProgress');
        const stateEl = document.getElementById('supExecState');
        if (pctEl) pctEl.textContent = pct.toFixed(1) + '%';
        if (barEl) barEl.style.width = pct.toFixed(1) + '%';
        if (wpEl) wpEl.textContent = `${fb.current_waypoint_index || 0} / ${fb.total_waypoints || 0} WP`;
        if (stateEl && fb.status) stateEl.textContent = String(fb.status).toUpperCase();
        logMissionStatus(`feedback: status=${fb.status} progress=${pct.toFixed(1)}% waypoint=${fb.current_waypoint_index}/${fb.total_waypoints}`);
      }
    }
    if (j.result && j.result._ts !== lastMissionResultTs) {
      lastMissionResultTs = j.result._ts;
      const failed = j.result.status === 'FAILURE' || j.result.status === 'ABORTED';
      const stateEl = document.getElementById('supExecState');
      if (stateEl && j.result.status) stateEl.textContent = String(j.result.status).toUpperCase();
      logMissionStatus(`result: status=${j.result.status} history=${j.result.mission_history_path || ''}`, failed);
      (j.result.logs_and_errors || []).forEach(entry => {
        logMissionStatus(`  ↳ ${entry.message || JSON.stringify(entry)}`, true);
      });
    }
  } catch (e) { /* ignore - GCS may be offline briefly */ }
}

setInterval(pollMissionStatus, 1500);

// ================================================================
// Map Planner (iframe) visual refinement.
// Seamless aerospace GCS tokens injected into iframe head.
// ================================================================
const MAP_PLANNER_THEME_CSS = `
:root, :root[data-theme="dark"], [data-theme="dark"] {
  --abyss: #060b14;
  --panel: #0b1322;
  --panel2: #111d32;
  --raise: #172742;
  --border: #1e304c;
  --border2: #2b4469;
  --text: #e2ecf7;
  --muted: #8ba3be;
  --dim: #597391;
  --sonar: #06b6d4;
  --sonar-dim: #0284c7;
  --free: #10b981;
  --occ: #ef4444;
  --unk: #64748b;
  --start: #38bdf8;
  --goal: #f59e0b;
  --wp: #a855f7;
  --hud-bg: rgba(11, 19, 34, 0.92);
  --hud-border: rgba(56, 189, 248, 0.26);
  --stage-grid: rgba(56, 189, 248, 0.045);
  --canvas-well: #040811;
}

:root[data-theme="black"], [data-theme="black"] {
  --abyss: #020408;
  --panel: #070b12;
  --panel2: #0d1422;
  --raise: #131d31;
  --border: #1a263b;
  --border2: #273854;
  --text: #eaf1f8;
  --muted: #8fa3ba;
  --dim: #586b82;
  --sonar: #38bdf8;
  --sonar-dim: #0284c7;
  --free: #10b981;
  --occ: #ef4444;
  --unk: #64748b;
  --start: #38bdf8;
  --goal: #f59e0b;
  --wp: #a855f7;
  --hud-bg: rgba(7, 11, 18, 0.94);
  --hud-border: rgba(56, 189, 248, 0.28);
  --stage-grid: rgba(56, 189, 248, 0.04);
  --canvas-well: #020408;
}

:root[data-theme="light"], [data-theme="light"] {
  --abyss: #e8eff7;
  --panel: #ffffff;
  --panel2: #f4f7fb;
  --raise: #e2e8f0;
  --border: #cbd5e1;
  --border2: #94a3b8;
  --text: #0f172a;
  --muted: #475569;
  --dim: #64748b;
  --sonar: #0284c7;
  --sonar-dim: #1d4ed8;
  --free: #059669;
  --occ: #dc2626;
  --unk: #64748b;
  --start: #0284c7;
  --goal: #d97706;
  --wp: #7e22ce;
  --hud-bg: rgba(255, 255, 255, 0.95);
  --hud-border: #cbd5e1;
  --stage-grid: rgba(15, 23, 42, 0.055);
  --canvas-well: #dde6f1;
}

body {
  background: var(--abyss) !important;
  color: var(--text) !important;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif !important;
}

header.top {
  background: linear-gradient(180deg, var(--panel2) 0%, var(--panel) 100%) !important;
  border-bottom: 1px solid var(--border) !important;
  padding: 8px 16px !important;
}

[data-theme="light"] header.top {
  background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%) !important;
}

.brand {
  font-size: 13.5px !important;
  font-weight: 800 !important;
  letter-spacing: 0.03em !important;
}

.brand .glyph {
  width: 32px !important;
  height: 32px !important;
  border-radius: 8px !important;
  background: linear-gradient(145deg, #2563eb 0%, #1d4ed8 48%, #0f296b 100%) !important;
  border: 1px solid rgba(96, 165, 250, 0.65) !important;
  color: #ffffff !important;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.35), 0 3px 10px rgba(29, 78, 216, 0.35) !important;
}

[data-theme="light"] .brand .glyph {
  background: linear-gradient(145deg, #2563eb 0%, #1d4ed8 55%, #0284c7 100%) !important;
  border-color: #1e40af !important;
  color: #ffffff !important;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.4), 0 2px 8px rgba(37, 99, 235, 0.25) !important;
}

.status {
  display: inline-flex !important;
  flex-wrap: nowrap !important;
  align-items: center !important;
  gap: 5px !important;
  padding: 4px 5px !important;
  background: #080D17 !important;
  border: 1px solid #1E2D45 !important;
  border-radius: 7px !important;
  box-shadow: inset 0 1px 3px rgba(0, 0, 0, 0.55), 0 1px 0 rgba(255, 255, 255, 0.03) !important;
  font-family: var(--mono) !important;
  font-size: 10.5px !important;
}

[data-theme="light"] .status {
  background: #E2E8F0 !important;
  border-color: #CBD5E1 !important;
  box-shadow: inset 0 1px 2px rgba(15, 23, 42, 0.08), 0 1px 0 rgba(255, 255, 255, 0.9) !important;
}

.pill,
.status .pill,
.pill.tool,
.pill.live {
  display: inline-flex !important;
  flex-direction: row !important;
  align-items: center !important;
  justify-content: flex-start !important;
  gap: 7px !important;
  height: 27px !important;
  min-height: 27px !important;
  padding: 0 10px 0 9px !important;
  border-radius: 4.5px !important;
  background: linear-gradient(180deg, #162032 0%, #0F1726 100%) !important;
  border: 1px solid #243552 !important;
  border-left: 2.5px solid #475569 !important;
  color: #94A3B8 !important;
  font-family: var(--mono) !important;
  font-size: 10px !important;
  font-weight: 600 !important;
  text-transform: uppercase !important;
  letter-spacing: 0.05em !important;
  white-space: nowrap !important;
  flex-shrink: 0 !important;
  line-height: 1 !important;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.05), 0 1px 2px rgba(0, 0, 0, 0.35) !important;
}

.status .pill::before {
  content: "" !important;
  width: 5.5px !important;
  height: 5.5px !important;
  border-radius: 50% !important;
  background: #64748B !important;
  flex-shrink: 0 !important;
  order: 3 !important;
  margin-left: 2px !important;
}

.pill b {
  color: #F8FAFC !important;
  font-weight: 800 !important;
  letter-spacing: 0.04em !important;
  padding: 2px 5px !important;
  border-radius: 3px !important;
  background: rgba(255, 255, 255, 0.05) !important;
  border: 1px solid rgba(255, 255, 255, 0.07) !important;
}

.pill.live {
  border-left-color: #10B981 !important;
}

.status .pill.live::before {
  background: #10B981 !important;
  box-shadow: 0 0 6px rgba(16, 185, 129, 0.65) !important;
}

.pill.live b {
  color: #34D399 !important;
  background: rgba(16, 185, 129, 0.12) !important;
  border-color: rgba(16, 185, 129, 0.28) !important;
}

.pill.tool,
#stTool.pill {
  border-left-color: #3B82F6 !important;
}

.status .pill.tool::before,
#stTool.pill::before {
  background: #38BDF8 !important;
  box-shadow: 0 0 5px rgba(56, 189, 248, 0.5) !important;
}

.pill.tool b,
#stTool.pill b {
  color: #7DD3FC !important;
  background: rgba(56, 189, 248, 0.12) !important;
  border-color: rgba(56, 189, 248, 0.28) !important;
}

#stSrc.pill {
  border-left-color: #64748B !important;
}

#stSize.pill {
  border-left-color: #0EA5E9 !important;
}

[data-theme="light"] .pill,
[data-theme="light"] .status .pill,
[data-theme="light"] .pill.tool,
[data-theme="light"] .pill.live {
  background: linear-gradient(180deg, #FFFFFF 0%, #F8FAFC 100%) !important;
  border: 1px solid #CBD5E1 !important;
  border-left: 2.5px solid #94A3B8 !important;
  color: #475569 !important;
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.06), inset 0 1px 0 #FFFFFF !important;
}

[data-theme="light"] .status .pill::before {
  background: #94A3B8 !important;
}

[data-theme="light"] .pill b {
  color: #0F172A !important;
  background: #F1F5F9 !important;
  border-color: #E2E8F0 !important;
}

[data-theme="light"] .pill.live {
  border-left-color: #059669 !important;
}

[data-theme="light"] .status .pill.live::before {
  background: #059669 !important;
  box-shadow: 0 0 4px rgba(5, 150, 105, 0.35) !important;
}

[data-theme="light"] .pill.live b {
  color: #047857 !important;
  background: #ECFDF5 !important;
  border-color: #A7F3D0 !important;
}

[data-theme="light"] .pill.tool,
[data-theme="light"] #stTool.pill {
  border-left-color: #2563EB !important;
}

[data-theme="light"] .status .pill.tool::before,
[data-theme="light"] #stTool.pill::before {
  background: #2563EB !important;
  box-shadow: 0 0 4px rgba(37, 99, 235, 0.35) !important;
}

[data-theme="light"] .pill.tool b,
[data-theme="light"] #stTool.pill b {
  color: #1D4ED8 !important;
  background: #EFF6FF !important;
  border-color: #BFDBFE !important;
}

[data-theme="light"] #stSize.pill {
  border-left-color: #0284C7 !important;
}

/* Left tool rail */
.rail {
  background: var(--panel) !important;
  border-right: 1px solid var(--border) !important;
}

.rail .group {
  border-bottom: 1px solid var(--border) !important;
  padding: 11px 11px 10px !important;
}

.rail .group:last-child {
  border-bottom: none !important;
}

.group h4 {
  color: var(--muted) !important;
  font-size: 9.5px !important;
  font-weight: 800 !important;
  letter-spacing: 0.12em !important;
  margin-bottom: 8px !important;
  display: flex !important;
  align-items: center !important;
  gap: 6px !important;
}

.rail .tool[data-tool="pick-water"] {
  grid-column: span 2 !important;
  flex-direction: row !important;
  min-height: 36px !important;
  padding: 7px 10px !important;
  gap: 8px !important;
}

/* Center Map Stage */
.center {
  background-color: var(--abyss) !important;
  background-image:
    linear-gradient(to right, var(--stage-grid) 1px, transparent 1px),
    linear-gradient(to bottom, var(--stage-grid) 1px, transparent 1px) !important;
  background-size: 24px 24px !important;
}

.stageHud .pill, .coordreadout, .stageCtl {
  background: var(--hud-bg) !important;
  backdrop-filter: blur(10px) !important;
  border: 1px solid var(--hud-border) !important;
  border-radius: 8px !important;
  color: var(--text) !important;
}

[data-theme="light"] .tabs {
  background: #e2e8f0 !important;
  border-bottom-color: #cbd5e1 !important;
}

[data-theme="light"] .toggle,
[data-theme="light"] .item,
[data-theme="light"] .zb {
  background: #f8fafc !important;
  border-color: #cbd5e1 !important;
  color: #0f172a !important;
}

[data-theme="light"] .hint {
  background: #f8fafc !important;
  border-color: #cbd5e1 !important;
  border-left: 2.5px solid #0284c7 !important;
  color: #475569 !important;
}

.pv canvas {
  background: var(--canvas-well) !important;
}

.exportbar {
  flex-wrap: nowrap !important;
  justify-content: space-between !important;
}
`;

function injectMapPlannerTheme() {
  const frame = document.getElementById('mapPlannerFrame');
  if (!frame) return;
  try {
    const doc = frame.contentDocument;
    if (!doc || !doc.head) return;
    const curTheme = document.documentElement.getAttribute('data-theme') || 'dark';
    if (doc.documentElement) doc.documentElement.setAttribute('data-theme', curTheme);
    let el = doc.getElementById('gcsPlannerTheme');
    if (!el) {
      el = doc.createElement('style');
      el.id = 'gcsPlannerTheme';
      doc.head.appendChild(el);
    }
    el.textContent = MAP_PLANNER_THEME_CSS;
  } catch (e) { /* not readable yet */ }
}
injectMapPlannerTheme();
const _mpFrameEl = document.getElementById('mapPlannerFrame');
if (_mpFrameEl) _mpFrameEl.addEventListener('load', injectMapPlannerTheme);
</script>

<script>
// ================================================================
// Utilities
// ================================================================
function fmt(v, d=2) {
  return (v != null && !isNaN(v)) ? Number(v).toFixed(d) : '--';
}
function humanSize(b) {
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b/1024).toFixed(1) + ' KB';
  return (b/1048576).toFixed(1) + ' MB';
}

// ================================================================
// Polling
// ================================================================
// Quaternion (x,y,z,w) -> CSS matrix3d() string for the 3D orientation box.
// Standard quaternion-to-rotation-matrix, transposed into CSS's column-major
// matrix3d(col0, col1, col2, col3) argument order. Gimbal-lock-free, unlike
// the sequential Euler rotations used for the numeric Roll/Pitch/Yaw fields.
function quatToMatrix3d(x, y, z, w) {
  const r00 = 1 - 2*(y*y + z*z), r10 = 2*(x*y + w*z), r20 = 2*(x*z - w*y);
  const r01 = 2*(x*y - w*z),     r11 = 1 - 2*(x*x + z*z), r21 = 2*(y*z + w*x);
  const r02 = 2*(x*z + w*y),     r12 = 2*(y*z - w*x),     r22 = 1 - 2*(x*x + y*y);
  return `matrix3d(${r00},${r10},${r20},0,${r01},${r11},${r21},0,${r02},${r12},${r22},0,0,0,0,1)`;
}

// Depth -> background tint for the orientation box's scene, from a light
// near-surface blue down to a dark near-black blue. DEPTH_TINT_MAX_M is a
// purely cosmetic scaling assumption (this vehicle's real operating depth
// range wasn't specified) - safe to retune, no control/safety meaning.
const DEPTH_TINT_MAX_M = 30;
function depthToColor(depth) {
  if (depth == null || isNaN(depth) || depth <= 0) return '';
  const theme = document.documentElement.getAttribute('data-theme') || 'dark';
  const t = Math.max(0, Math.min(1, depth / DEPTH_TINT_MAX_M));
  if (theme === 'light') {
    // Light mode: subtle technical slate tint (#dbeafe at surface -> #94a3b8 at depth)
    const r = Math.round(219 - 71 * t);
    const g = Math.round(234 - 71 * t);
    const b = Math.round(254 - 70 * t);
    return `rgb(${r},${g},${b})`;
  } else if (theme === 'black') {
    // True Black mode: obsidian abyssal gradient (#060b14 at surface -> #010204 at depth)
    const r = Math.round(6 - 5 * t);
    const g = Math.round(11 - 9 * t);
    const b = Math.round(20 - 16 * t);
    return `rgb(${r},${g},${b})`;
  } else {
    // Dark mode: deep marine navy (#0b1626 at surface -> #040810 at depth)
    const r = Math.round(11 - 7 * t);
    const g = Math.round(22 - 14 * t);
    const b = Math.round(38 - 22 * t);
    return `rgb(${r},${g},${b})`;
  }
}

function pollTelemetry() {
  fetch('/api/telemetry').then(r=>r.json()).then(d => {
    if (d.orientation) {
      // Quaternion to Euler (simplified, for display only)
      const q = d.orientation;
      const sinr = 2*(q.w*q.x + q.y*q.z);
      const cosr = 1 - 2*(q.x*q.x + q.y*q.y);
      const roll = Math.atan2(sinr, cosr) * 180/Math.PI;
      const sinp = 2*(q.w*q.y - q.z*q.x);
      const pitch = Math.abs(sinp)>=1 ? Math.sign(sinp)*90 : Math.asin(sinp)*180/Math.PI;
      const siny = 2*(q.w*q.z + q.x*q.y);
      const cosy = 1 - 2*(q.y*q.y + q.z*q.z);
      const yaw = Math.atan2(siny, cosy) * 180/Math.PI;
      document.getElementById('tRoll').textContent  = fmt(roll);
      document.getElementById('tPitch').textContent = fmt(pitch);
      document.getElementById('tYaw').textContent   = fmt(yaw);
      // 3D box driven directly by the quaternion (not the roll/pitch/yaw
      // above) so it stays smooth through pitch near +-90deg, where the
      // Euler extraction above is known-lossy ("simplified, for display
      // only" per the existing comment) - the numeric readout is
      // unaffected either way, this is purely an added visualization.
      const box3d = document.getElementById('orient3dBox');
      if (box3d) box3d.style.transform = quatToMatrix3d(q.x, q.y, q.z, q.w);
    }
    const scene3d = document.getElementById('orient3dScene');
    if (scene3d) scene3d.style.backgroundColor = depthToColor(d.depth);
    document.getElementById('tDepth').textContent = fmt(d.depth);
    document.getElementById('tBatV').textContent  = fmt(d.battery_voltage, 1);
    document.getElementById('tTemp').textContent  = fmt(d.temperature, 1);
    document.getElementById('tPres').textContent  = fmt(d.pressure, 0);
    document.getElementById('tHum').textContent   = fmt(d.humidity, 1);
    if (d.gps) {
      document.getElementById('tGps').textContent =
        `Lat: ${fmt(d.gps.lat,5)}  Lon: ${fmt(d.gps.lon,5)}  Alt: ${fmt(d.gps.alt,1)} m`;
    }
    if (Array.isArray(d.leaks)) {
      const dots = document.querySelectorAll('.leak-dot');
      d.leaks.forEach((v,i)=>{
        if (dots[i]) dots[i].classList.toggle('alert', v);
      });
    }
    // Recording status from telemetry
    if (d.recording !== undefined) updateRecUI(d.recording);
  }).catch(()=>{});
}

function pollState() {
  fetch('/api/state').then(r=>r.json()).then(d => {
    if (!d || !Object.keys(d).length) return;
    document.getElementById('sysMode').textContent   = d.mode || '--';
    document.getElementById('sysArmed').textContent  = d.armed ? 'true' : 'false';
    const errs = d.errors || [];
    document.getElementById('sysErrors').textContent = errs.length ? errs.join(', ') : 'none';
    // Mode buttons
    document.querySelectorAll('.mode-buttons .btn').forEach(b=>b.classList.remove('active'));
    const map = {manual:'modeManual',auto:'modeAuto',teleop:'modeTeleop'};
    if (map[d.mode]) document.getElementById(map[d.mode]).classList.add('active');
    // Arm / Disarm button selection state
    const btnArm = document.getElementById('btnArm');
    const btnDisarm = document.getElementById('btnDisarm');
    if (btnArm && btnDisarm && d.armed !== undefined) {
      btnArm.classList.toggle('is-selected', !!d.armed);
      btnDisarm.classList.toggle('is-selected', !d.armed);
    }
    // Header pills
    const modePill = document.getElementById('modePill');
    const modeTxt  = (d.mode || 'manual').toUpperCase();
    modePill.innerHTML = `<span class="dot"></span><svg class="pill-svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 19 21 12 17 5 21 12 2"/></svg> ${escHtml(modeTxt)}`;
    modePill.className = 'pill ' + (d.mode === 'auto' ? 'ok' : 'accent');
    const armPill = document.getElementById('armPill');
    const armSvg = d.armed
      ? '<svg class="pill-svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><polyline points="9 12 11 14 15 10"/></svg>'
      : '<svg class="pill-svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>';
    armPill.innerHTML  = `<span class="dot"></span>${armSvg} ${d.armed ? 'ARMED' : 'DISARMED'}`;
    armPill.className  = 'pill ' + (d.armed ? 'danger' : 'warn');
    // Recording from state (fallback if telemetry paused)
    if (d.recording !== undefined) updateRecUI(d.recording);
    // Toggle sync: deliberately from /api/state (always flows every 1s
    // regardless of any of these flags), NOT from the stream each flag
    // gates (/api/telemetry, blackbox, compass) - syncing a toggle from the
    // very stream it turns off means the UI can never confirm "off" back
    // to the operator once it actually takes effect. This was the root
    // cause of the telemetry switch getting stuck and looking like it
    // couldn't be turned back on.
    if (d.telemetry_enabled !== undefined) _syncTelemToggleUI(d.telemetry_enabled);
    if (d.blackbox_stream_enabled !== undefined) _syncBoolToggle('bbStreamToggle', 'bbStreamLabel', d.blackbox_stream_enabled);
    if (d.mag_stream_enabled !== undefined) _syncBoolToggle('magStreamToggle', 'magStreamLabel', d.mag_stream_enabled);
    if (d.depth_hold_enabled !== undefined) {
      _syncBoolToggle('depthHoldToggle', 'depthHoldLabel', d.depth_hold_enabled);
      _renderDepthHoldStatus(d.depth_hold_enabled, d.depth_hold_status);
    }
  }).catch(()=>{});
}

// What core/depth_hold.py reports it is doing - the switch alone only says
// what was asked for. A null status means its 1 Hz status went quiet (node
// not running), in which case thrustmaster has the side fins on the joystick.
const DEPTH_HOLD_STATE_TEXT = {
  OFF: 'off - side fins on joystick',
  NO_DATA: 'no depth/IMU data - side fins on joystick',
  STALE: 'depth/IMU data stale - side fins on joystick',
  DIVING: 'diving to target',
  HOLDING: 'holding depth',
};

function _renderDepthHoldStatus(enabled, s) {
  const el = document.getElementById('depthHoldStatus');
  let text;
  let color = 'var(--muted)';
  if (!s) {
    text = 'depth_hold node not running - side fins on joystick';
    if (enabled) color = 'var(--text-dim)';
  } else if (s.enabled !== enabled) {
    text = 'waiting for depth_hold to apply the switch...';
    color = 'var(--text-dim)';
  } else {
    text = DEPTH_HOLD_STATE_TEXT[s.state] || s.state;
    if (s.state === 'DIVING' || s.state === 'HOLDING') color = 'var(--accent2)';
    const depth = (typeof s.depth === 'number') ? s.depth.toFixed(2) : '--';
    text += ` | depth ${depth} m, target ${Number(s.target).toFixed(2)} m`;
  }
  el.textContent = text;
  el.style.color = color;
}

// Generic on/off toggle-switch sync used by the new bandwidth-gated
// streams (black box, compass/mag) - setting .checked programmatically
// does not fire the element's 'change' listener, so no guard flag is
// needed the way _syncTelemToggleUI uses one (kept there for the older,
// more elaborate ACTIVE/PAUSED label wording).
function _syncBoolToggle(inputId, labelId, enabled) {
  const cb = document.getElementById(inputId);
  if (cb.checked !== enabled) cb.checked = enabled;
  const lbl = document.getElementById(labelId);
  if (lbl) {
    lbl.textContent = enabled ? 'ON' : 'OFF';
    lbl.style.color = enabled ? 'var(--accent2)' : 'var(--muted)';
  }
}

let _lastChannels = [];
let _lastActiveChannel = null;

function pollChannels() {
  fetch('/api/channels').then(r=>r.json()).then(d => {
    _lastChannels = d.channels || [];
    _lastActiveChannel = d.active;
    renderChannelList(d.channels || [], d.active, d.forced);
    updateConnPill(d.channels || [], d.active, d.forced);
    renderSecondaryLog(d);
    renderManualChannelList(d.channels || []);
    renderWifiChannelSelect(d.channels || []);
    renderWifiTarget(d.channels || [], d.active);
    if (d.primary_enabled !== undefined) {
      const cb = document.getElementById('primaryTetherToggle');
      if (cb.checked !== d.primary_enabled) cb.checked = d.primary_enabled;
      const pLbl = document.getElementById('primaryTetherLabel');
      pLbl.textContent = d.primary_enabled ? 'ON' : 'OFF';
      pLbl.style.color = d.primary_enabled ? 'var(--ok)' : 'var(--muted)';
    }
  }).catch(()=>{});
}

// Which channel wifi commands should actually be sent over. channels.
// active()'s automatic selection ALWAYS prefers a connected CH-A/B/C/
// CH-test over ANY manual connection, no matter how deliberately it was
// made (ChannelManager.MANUAL_PRIORITY_FLOOR gives every manual channel
// the lowest possible priority) - so if an operator connects manually
// (e.g. Ethernet, straight to a real Jetson) while a stray CH-test happens
// to also be up, "Auto" would silently keep using CH-test and ignore the
// connection they just made on purpose. wifiChannelSelect lets them
// override that explicitly instead of trusting it blindly.
function wifiSelectedChannel() {
  const el = document.getElementById('wifiChannelSelect');
  return el ? el.value : '';
}

let _lastWifiSelectSig = null;

function toggleWifiSelectMenu(e) {
  if (e) e.stopPropagation();
  const wrap = document.getElementById('wifiChannelSelectWrap');
  if (!wrap) return;
  const isOpen = wrap.classList.contains('is-open') || wrap.classList.contains('open');
  wrap.classList.toggle('is-open', !isOpen);
  wrap.classList.toggle('open', !isOpen);
}

function selectWifiChannelOption(val, e) {
  if (e) e.stopPropagation();
  const sel = document.getElementById('wifiChannelSelect');
  const wrap = document.getElementById('wifiChannelSelectWrap');
  if (sel) {
    sel.value = val;
  }
  if (wrap) {
    wrap.classList.remove('is-open');
    wrap.classList.remove('open');
  }
  _lastWifiSelectSig = null; // force UI refresh of selected state
  renderWifiChannelSelect(_lastChannels || []);
  renderWifiTarget(_lastChannels || [], _lastActiveChannel);
}

document.addEventListener('click', (e) => {
  const wrap = document.getElementById('wifiChannelSelectWrap');
  if (wrap && (wrap.classList.contains('is-open') || wrap.classList.contains('open')) && !wrap.contains(e.target)) {
    wrap.classList.remove('is-open');
    wrap.classList.remove('open');
  }
});

function renderWifiChannelSelect(chs) {
  const sel = document.getElementById('wifiChannelSelect');
  if (!sel) return;
  const prevValue = sel.value;
  const defaultList = [
    { name: 'CH-A', host: '192.168.2.2', port: 6100, connected: false },
    { name: 'CH-B', host: '192.168.2.3', port: 6100, connected: false },
    { name: 'CH-C', host: '192.168.2.4', port: 6100, connected: false },
    { name: 'CH-test', host: '127.0.0.1', port: 6100, connected: false }
  ];
  const allChannels = (chs && chs.length) ? chs : defaultList;
  const connected = allChannels.filter(c => c.connected);
  const effectiveVal = (prevValue && allChannels.some(c => c.name === prevValue)) ? prevValue : '';
  const sig = effectiveVal + '::' + allChannels.map(c => `${c.name}:${c.host}:${c.port}:${c.connected ? 1 : 0}`).join('|');
  if (sig === _lastWifiSelectSig) return;
  _lastWifiSelectSig = sig;

  const options = ['<option value="">Auto (Active Link)</option>'].concat(
    allChannels.map(c => {
      const isTest = (c.host === '127.0.0.1' || c.name === 'CH-test');
      const label = isTest ? `${c.name} (LOCAL TEST - not a Jetson!)` : `${c.name} (${c.host}:${c.port})`;
      return `<option value="${escAttr(c.name)}">${escHtml(label)}</option>`;
    })
  );
  sel.innerHTML = options.join('');
  sel.value = effectiveVal;

  // Sync custom non-native dropdown trigger & popover menu
  const valEl = document.getElementById('wifiChannelSelectVal');
  const subEl = document.getElementById('wifiChannelSelectSub');
  const badgeEl = document.getElementById('wifiChannelSelectBadge');
  const menuEl = document.getElementById('wifiChannelSelectMenu');
  if (valEl && badgeEl) {
    if (!effectiveVal) {
      valEl.textContent = 'Auto (Active Link)';
      if (subEl) {
        const activeCh = connected[0];
        subEl.textContent = activeCh
          ? `Routing via ${activeCh.name} (${activeCh.host}:${activeCh.port})`
          : 'Automatic priority channel routing';
      }
      badgeEl.textContent = 'AUTO-ROUTE';
    } else {
      const picked = allChannels.find(c => c.name === effectiveVal);
      valEl.textContent = picked ? picked.name : effectiveVal;
      if (subEl) {
        subEl.textContent = picked
          ? `${picked.host}:${picked.port} · ${picked.connected ? 'Connected' : 'Standby / Offline'}`
          : 'Explicit channel override';
      }
      badgeEl.textContent = 'PINNED';
    }
  }
  if (menuEl) {
    const checkSvg = '<svg class="gcs-select-opt-check" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>';
    const headHtml = `
      <div class="gcs-select-menu-head">
        <span>SELECT TARGET LINK CHANNEL</span>
        <span>${connected.length}/${allChannels.length} ONLINE</span>
      </div>`;
    const items = [
      `<div class="gcs-select-opt ${effectiveVal === '' ? 'is-selected selected' : ''}" onclick="selectWifiChannelOption('', event)">
         <div class="gcs-select-opt-left">
           <span class="gcs-select-opt-dot ${connected.length ? 'up' : 'auto'}"></span>
           <div class="gcs-select-opt-info">
             <span class="gcs-select-opt-label">Auto (Active Link)</span>
             <span class="gcs-select-opt-sub">Automatic priority failover (CH-A → CH-B → CH-C)</span>
           </div>
         </div>
         <div class="gcs-select-opt-right">
           <span class="gcs-select-opt-pill auto">DEFAULT</span>
           ${checkSvg}
         </div>
       </div>`
    ].concat(allChannels.map(c => {
      const isTest = (c.host === '127.0.0.1' || c.name === 'CH-test');
      const sub = isTest
        ? `${c.host}:${c.port} · Local Loopback Test`
        : `${c.host}:${c.port} · ${c.is_manual ? 'Manual Backup Link' : 'Primary RS-485 Tether'}`;
      const isSel = effectiveVal === c.name;
      const pillCls = c.connected ? 'online' : '';
      const pillTxt = c.connected ? 'ONLINE' : 'STANDBY';
      return `<div class="gcs-select-opt ${isSel ? 'is-selected selected' : ''}" onclick="selectWifiChannelOption('${escAttr(c.name)}', event)">
         <div class="gcs-select-opt-left">
           <span class="gcs-select-opt-dot ${c.connected ? 'up' : ''}"></span>
           <div class="gcs-select-opt-info">
             <span class="gcs-select-opt-label">${escHtml(c.name)}</span>
             <span class="gcs-select-opt-sub">${escHtml(sub)}</span>
           </div>
         </div>
         <div class="gcs-select-opt-right">
           <span class="gcs-select-opt-pill ${pillCls}">${pillTxt}</span>
           ${checkSvg}
         </div>
       </div>`;
    }));
    menuEl.innerHTML = headHtml + items.join('');
  }
}
renderWifiChannelSelect([]);

// This label makes the ACTUAL effective target of every Wifi Comms button
// always visible instead of silent - either the operator's explicit
// wifiChannelSelect pick, or (on "Auto") whatever channels.active()
// resolves to right now.
function renderWifiTarget(chs, activeName) {
  const el = document.getElementById('wifiTargetLabel');
  if (!el) return;
  const selected = wifiSelectedChannel();
  const targetName = selected || activeName;
  const active = chs.find(c => c.name === targetName && c.connected);
  if (!active) {
    el.textContent = selected ? `'${selected}' is no longer connected` : 'not connected';
    el.style.color = 'var(--danger)';
  } else if (active.host === '127.0.0.1' || active.name === 'CH-test') {
    el.textContent = `${active.name} (${active.host}:${active.port}) - LOCAL TEST CHANNEL, not a real Jetson!`;
    el.style.color = 'var(--warn)';
  } else {
    el.textContent = `${active.name} (${active.host}:${active.port})` + (selected ? ' (explicitly selected)' : ' (auto)');
    el.style.color = 'var(--ok)';
  }
}

function setPrimaryTether(enabled) {
  if (!enabled && !confirm('This will disable the primary tether channels (CH-A/B/C/test).\nOnly manual/backup connections will be usable until you turn it back on. Continue?')) {
    // Revert the checkbox - the user cancelled.
    document.getElementById('primaryTetherToggle').checked = true;
    return;
  }
  const pLbl = document.getElementById('primaryTetherLabel');
  pLbl.textContent = enabled ? 'ON' : 'OFF';
  pLbl.style.color = enabled ? 'var(--ok)' : 'var(--muted)';
  fetch('/api/primary_tether', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({enabled})
  }).catch(()=>{});
}

// ================================================================
// Manual Connection (Link Channels sub-tab)
// ================================================================
function showLinkTab(name) {
  document.querySelectorAll('.link-subtab').forEach(b => b.classList.toggle('active', b.dataset.linktab === name));
  document.getElementById('linktab-status').style.display = (name === 'status') ? '' : 'none';
  document.getElementById('linktab-manual').style.display = (name === 'manual') ? '' : 'none';
}

let _manualMode = 'tethered';

function setManualMode(mode) {
  _manualMode = mode;
  document.querySelectorAll('.manual-mode-tab').forEach(b => b.classList.toggle('active', b.dataset.mode === mode));

  const showHostPort = (mode !== 'websocket');
  document.getElementById('mfHostPort').style.display = showHostPort ? '' : 'none';
  document.getElementById('mfSshExtra').style.display = (mode === 'ssh') ? '' : 'none';
  document.getElementById('mfWsUrl').style.display = (mode === 'websocket') ? '' : 'none';

  const portInput = document.getElementById('mfPort');
  const hostInput = document.getElementById('mfHost');
  if (mode === 'ssh') {
    portInput.placeholder = '22';
  } else {
    portInput.placeholder = '6100';
  }
  if (mode === 'localhost' && !hostInput.value) hostInput.value = '127.0.0.1';
}

function manualConnect() {
  const statusEl = document.getElementById('manualConnectStatus');
  const setStatus = (msg, ok) => {
    statusEl.textContent = msg;
    statusEl.style.color = ok === true ? 'var(--ok)' : ok === false ? 'var(--danger)' : 'var(--muted)';
  };

  const body = { mode: _manualMode, label: document.getElementById('mfLabel').value.trim() };

  if (_manualMode === 'websocket') {
    const url = document.getElementById('mfUrl').value.trim();
    if (!url) { setStatus('Enter a connect URL', false); return; }
    body.url = url;
  } else {
    const host = document.getElementById('mfHost').value.trim();
    const port = document.getElementById('mfPort').value.trim();
    if (!host || !port) { setStatus('Enter host and port', false); return; }
    body.host = host;
    body.port = parseInt(port, 10);
    if (_manualMode === 'ssh') {
      body.username = document.getElementById('mfUsername').value.trim();
      body.password = document.getElementById('mfPassword').value;
      if (!body.username) { setStatus('Enter a username', false); return; }
    }
  }

  setStatus('Connecting…', null);
  fetch('/api/manual_connect', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify(body)
  }).then(r=>r.json()).then(d => {
    if (d.error) { setStatus('✕ ' + d.error, false); return; }
    setStatus(`✔ Added "${d.name}" - dialing…`, true);
    if (_manualMode === 'ssh') document.getElementById('mfPassword').value = '';
  }).catch(e => setStatus('✕ ' + e, false));
}

function manualDisconnect(name) {
  fetch('/api/manual_disconnect', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({name})
  }).catch(()=>{});
}

function renderManualChannelList(chs) {
  const el = document.getElementById('manualChannelList');
  const manualOnes = chs.filter(c => c.is_manual);
  if (!manualOnes.length) {
    el.innerHTML = '<span style="color:var(--muted);font-size:.75rem">None</span>';
    return;
  }
  el.innerHTML = manualOnes.map(c => {
    const dotCls = c.connected ? 'up' : 'down';
    const roleCls = c.role || 'down';
    const meta = c.connected
      ? `rx ${fmt(c.last_rx_age,1)}s ago`
      : `reconnecting&hellip; &middot; retries ${c.reconnects}`;
    return `
      <div class="channel-row ${dotCls}">
        <div class="channel-top">
          <div class="channel-left">
            <span class="channel-dot ${dotCls}"></span>
            <span class="channel-name">${escHtml(c.name)}</span>
            <span class="channel-role ${roleCls}">${roleCls.toUpperCase()}</span>
          </div>
          <button class="btn-icon del" onclick="manualDisconnect('${escAttr(c.name)}')">Disconnect</button>
        </div>
        <div class="channel-meta">
          <span>${escHtml(c.host)}:${c.port}</span>
          <span>&middot;</span>
          <span>${meta}</span>
        </div>
      </div>`;
  }).join('');
}

function renderChannelList(chs, activeName, forcedName) {
  const el = document.getElementById('channelList');
  el.innerHTML = chs.map(c => {
    const dotCls  = c.connected ? 'up' : 'down';
    const roleCls = c.role || 'down';
    const meta = c.connected
      ? `rx ${fmt(c.last_rx_age,1)}s ago`
      : `reconnecting&hellip; &middot; retries ${c.reconnects}`;
    const isForced = forcedName === c.name;
    const forceBtn = (c.connected && c.role !== 'active')
      ? `<button class="btn-icon force" onclick="forceChannel('${escAttr(c.name)}')">Force</button>`
      : '';
    return `
      <div class="channel-row ${dotCls}">
        <div class="channel-top">
          <div class="channel-left">
            <span class="channel-dot ${dotCls}"></span>
            <span class="channel-name">${escHtml(c.name)}</span>
            <span class="channel-role ${roleCls}">${roleCls.toUpperCase()}</span>
            ${isForced ? '<span class="channel-role pinned">PINNED</span>' : ''}
          </div>
          ${forceBtn}
        </div>
        <div class="channel-meta">
          <span>${escHtml(c.host)}:${c.port}</span>
          <span>&middot;</span>
          <span>${meta}</span>
        </div>
      </div>`;
  }).join('');
}

function forceChannel(name) {
  fetch('/api/force_channel', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({channel: name})
  }).then(r=>r.json()).then(d => { if (d.error) alert(d.error); })
    .catch(()=>{});
}

function clearForcedChannel() {
  fetch('/api/force_channel', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({channel: null})
  }).catch(()=>{});
}

function updateConnPill(chs, activeName, forcedName) {
  const pill = document.getElementById('connPill');
  const active = chs.find(c => c.name === activeName && c.connected);
  const linkSvg = '<svg class="pill-svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/></svg>';
  if (active) {
    pill.innerHTML = `<span class="dot"></span>${linkSvg} ${escHtml(active.name)} ACTIVE` + (forcedName ? ' (PINNED)' : '');
    pill.className = 'pill ok';
  } else {
    pill.innerHTML = `<span class="dot"></span>${linkSvg} NO LINK`;
    pill.className = 'pill danger';
  }
}

function renderSecondaryLog(d) {
  const nameEl = document.getElementById('secChName');
  const logEl  = document.getElementById('secondaryLog');
  if (!d.secondary_channel_name) {
    nameEl.textContent = '(--)';
    logEl.innerHTML = '<span style="color:var(--muted);font-size:.75rem">No secondary channel connected</span>';
    return;
  }
  nameEl.textContent = `(${d.secondary_channel_name})`;
  const t = d.secondary_telemetry || {};
  logEl.innerHTML =
    `depth: ${fmt(t.depth)} m &nbsp; batt: ${fmt(t.battery_voltage,1)} V &nbsp; temp: ${fmt(t.temperature,1)} &deg;C ` +
    `&nbsp; <span style="color:var(--muted)">@ ${new Date().toLocaleTimeString()}</span>`;
}

// ================================================================
// Wifi Comms
// ================================================================
let _wifiExpandedSsid = null;   // scanned SSID currently showing its password entry row

// Read-only accessor - the sudo password field is never stored anywhere
// (no JS variable holds a copy, no localStorage) other than the DOM input
// itself, and is only ever read at the moment a request is sent.
function wifiSudoPassword() {
  const el = document.getElementById('wifiSudoPassword');
  return el ? el.value : '';
}

// Non-blocking error display for the wifi panel's own fetches - NEVER use
// alert() here. alert() is modal/synchronous and freezes the ENTIRE page
// (every setInterval poll included, not just this panel) until someone
// notices and dismisses it - on a page that already polls ~10 endpoints a
// second, that's indistinguishable from "the GCS froze". Route failures
// through the existing #wifiStatusMsg line instead.
function wifiShowError(text) {
  const msgEl = document.getElementById('wifiStatusMsg');
  if (msgEl) {
    msgEl.textContent = '✕ ' + text;
    msgEl.style.color = 'var(--danger)';
  } else {
    console.error(text);
  }
}

function wifiListDevices() {
  const body = {};
  const sudoPw = wifiSudoPassword();
  if (sudoPw) body.sudo_password = sudoPw;
  const ch = wifiSelectedChannel();
  if (ch) body.channel = ch;
  fetch('/api/wifi_scan', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify(body)
  }).then(r => r.json())
    .then(d => { if (d.error) wifiShowError(d.error); })
    .catch(e => console.error(e));
}

function wifiToggleRow(ssid) {
  _wifiExpandedSsid = (_wifiExpandedSsid === ssid) ? null : ssid;
  pollWifi();
}

function wifiConnect(ssid, password) {
  const body = { ssid };
  if (password) body.password = password;
  const sudoPw = wifiSudoPassword();
  if (sudoPw) body.sudo_password = sudoPw;
  const ch = wifiSelectedChannel();
  if (ch) body.channel = ch;
  fetch('/api/wifi_connect', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify(body)
  }).then(r=>r.json()).then(d => { if (d.error) wifiShowError(d.error); })
    .catch(e=>console.error(e));
  _wifiExpandedSsid = null;
}

function wifiConnectFromRow(ssid) {
  const pwInput = document.getElementById('wifiPwInput');
  wifiConnect(ssid, pwInput ? pwInput.value : '');
}

function wifiManualConnect() {
  const statusEl = document.getElementById('wifiManualStatus');
  const setStatus = (msg, ok) => {
    statusEl.textContent = msg;
    statusEl.style.color = ok === true ? 'var(--ok)' : ok === false ? 'var(--danger)' : 'var(--muted)';
  };
  const ssid = document.getElementById('wifiManualSsid').value.trim();
  const password = document.getElementById('wifiManualPassword').value;
  if (!ssid) { setStatus('Enter an SSID', false); return; }
  const body = { ssid };
  if (password) body.password = password;
  const sudoPw = wifiSudoPassword();
  if (sudoPw) body.sudo_password = sudoPw;
  const ch = wifiSelectedChannel();
  if (ch) body.channel = ch;
  setStatus('Connecting…', null);
  fetch('/api/wifi_connect', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify(body)
  }).then(r=>r.json()).then(d => {
    if (d.error) { setStatus('✕ ' + d.error, false); return; }
    setStatus('Connecting…', null);
    document.getElementById('wifiManualPassword').value = '';
  }).catch(e => setStatus('✕ ' + e, false));
}

function renderWifiNetworks(networks, connectingSsid, scanInProgress) {
  const pwInput = document.getElementById('wifiPwInput');
  if (pwInput && document.activeElement === pwInput) return;
  const el = document.getElementById('wifiNetworkList');
  if (!networks || !networks.length) {
    el.style.textAlign = 'center';
    const msg = scanInProgress ? 'Scanning…'
      : (networks === null || networks === undefined ? 'Click "List Devices" to scan for wifi networks' : 'No networks found');
    el.innerHTML = `<span style="color:var(--muted);font-size:.72rem">${msg}</span>`;
    return;
  }
  el.style.textAlign = 'left';
  el.innerHTML = networks.map(n => {
    const ssid = n.ssid || '(hidden)';
    const isConnecting = connectingSsid === ssid;
    const dotCls = n.in_use ? 'up' : (isConnecting ? 'connecting' : '');
    const secLabel = n.security ? `[${escHtml(n.security)}]` : '[OPEN]';
    const meta = `${secLabel} &middot; signal ${n.signal != null ? n.signal + '%' : '--'}${n.saved ? ' &middot; saved' : ''}`;
    const expanded = _wifiExpandedSsid === ssid;
    const rowBtn = n.in_use
      ? '<span class="channel-role active">CONNECTED</span>'
      : `<button class="btn btn-sm" onclick="wifiToggleRow('${escAttr(ssid)}')">${expanded ? 'Cancel' : 'Connect'}</button>`;
    let rowHtml = `
      <div class="channel-row ${dotCls}">
        <div class="channel-top">
          <div class="channel-left">
            <span class="channel-dot ${dotCls}"></span>
            <span class="channel-name">${escHtml(ssid)}</span>
          </div>
          ${rowBtn}
        </div>
        <div class="channel-meta"><span>${meta}</span></div>
      </div>`;
    if (expanded && !n.in_use) {
      rowHtml += `
      <div class="manual-field-group" style="margin-top:-2px;">
        <input type="password" class="manual-input" id="wifiPwInput" placeholder="Password (leave blank if open network)" autocomplete="off">
        <button class="btn btn-ok" onclick="wifiConnectFromRow('${escAttr(ssid)}')">Connect</button>
      </div>`;
    }
    return rowHtml;
  }).join('');
}

function renderWifiSaved(saved, connectingSsid) {
  const el = document.getElementById('wifiSavedList');
  if (!saved || !saved.length) {
    el.style.textAlign = 'center';
    el.innerHTML = '<span style="color:var(--muted);font-size:.72rem">None yet</span>';
    return;
  }
  el.style.textAlign = 'left';
  el.innerHTML = saved.map(n => {
    const ssid = n.ssid || '';
    const isConnecting = connectingSsid === ssid;
    const dotCls = n.in_use ? 'up' : (isConnecting ? 'connecting' : '');
    const actionHtml = n.in_use
      ? '<span class="channel-role active">CONNECTED</span>'
      : `<button class="btn btn-sm" onclick="wifiConnect('${escAttr(ssid)}')">Connect</button>`;
    return `
      <div class="channel-row ${dotCls}">
        <div class="channel-top">
          <div class="channel-left">
            <span class="channel-dot ${dotCls}"></span>
            <span>${escHtml(ssid)}</span>
          </div>
          ${actionHtml}
        </div>
      </div>`;
  }).join('');
}

function pollWifi() {
  fetch('/api/wifi_status').then(r=>r.json()).then(d => {
    const btn = document.getElementById('wifiScanBtn');
    if (btn) {
      btn.disabled = !!d.scan_in_progress;
      btn.innerHTML = '<svg class="btn-svg" viewBox="0 0 24 24"><path d="M21 12a9 9 0 1 1-2.64-6.36"/><polyline points="21 3 21 9 15 9"/></svg>' + (d.scan_in_progress ? 'Scanning…' : 'List Devices');
    }
    const msgEl = document.getElementById('wifiStatusMsg');
    if (msgEl) {
      if (d.scan_in_progress) {
        msgEl.textContent = '';
      } else if (d.scan_message && !d.scan_success) {
        // Most common cause: sudo on the Jetson rejected/needs the Sudo
        // Password above (e.g. "a password is required", "Sorry, try
        // again.") - shown verbatim so it's obvious this isn't "no
        // networks nearby".
        msgEl.textContent = '✕ Scan failed: ' + d.scan_message;
        msgEl.style.color = 'var(--danger)';
      } else if (d.last_connect_result && d.last_connect_result.message) {
        const r = d.last_connect_result;
        // .textContent, not innerHTML - no escaping needed/wanted here.
        msgEl.textContent = (r.success ? '✔ ' : '✕ ') + (r.ssid || '') + ': ' + r.message;
        msgEl.style.color = r.success ? 'var(--ok)' : 'var(--danger)';
      } else {
        msgEl.textContent = '';
      }
    }
    renderWifiNetworks(d.networks, d.connecting_ssid, d.scan_in_progress);
    renderWifiSaved(d.saved, d.connecting_ssid);
    // d.saved is null only until the Jetson's first wifi_saved_list reply
    // ever arrives (see SharedState.wifi_saved) - if the tether wasn't up
    // yet the moment the page loaded, the original one-shot request below
    // silently failed forever and "Saved Devices" never populated. Keep
    // retrying here (cheap no-op while disconnected, same as every other
    // poll* function already does) until it actually succeeds once; after
    // that d.saved is always an array (possibly empty), so this stops.
    if (d.saved === null || d.saved === undefined) {
      fetchWifiSaved();
    }
  }).catch(()=>{});
}

function fetchWifiSaved() {
  const body = {};
  const ch = wifiSelectedChannel();
  if (ch) body.channel = ch;
  fetch('/api/wifi_saved', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify(body)
  }).catch(()=>{});
}

// Saved Devices should populate as soon as the page loads, without
// requiring an operator to click List Devices (that button triggers a
// slower live rescan - saved profiles are a fast local read on the Jetson).
// pollWifi (above) retries this automatically if the tether wasn't up yet
// at this exact moment, so this first call is just to avoid waiting a
// full second for the common case where it's already connected.
fetchWifiSaved();

function pollJoystick() {
  fetch('/api/joystick').then(r=>r.json()).then(d => {
    // Relay toggle is a GCS-local setting, independent of whether hardware
    // is plugged in - sync it before the connected/disconnected branch below.
    if (d.relay_enabled !== undefined) {
      const cb = document.getElementById('joyRelayToggle');
      if (cb.checked !== d.relay_enabled) cb.checked = d.relay_enabled;
      const jLbl = document.getElementById('joyRelayLabel');
      jLbl.textContent = d.relay_enabled ? 'ON' : 'OFF';
      jLbl.style.color = d.relay_enabled ? 'var(--accent2)' : 'var(--muted)';
    }
    const viz    = document.getElementById('joyViz');
    const noJoy  = document.getElementById('joyDisconnected');
    const nameEl = document.getElementById('joyName');
    if (!d.connected) {
      if (viz) viz.style.display = 'none';
      if (noJoy) noJoy.style.display = 'flex';
      if (nameEl) nameEl.textContent = '';
      return;
    }
    if (viz) viz.style.display = 'flex';
    if (noJoy) noJoy.style.display = 'none';
    if (nameEl) nameEl.textContent = d.name ? `[${d.name}]` : '';
    const axes = (d.axes && d.axes.length) ? d.axes : [0, 0, 0, 0];
    const btns = (d.buttons && d.buttons.length) ? d.buttons : [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    renderJoyStick(axes[0] || 0, axes[1] || 0, true);
    renderJoyAxes(axes);
    renderJoyBtns(btns);
  }).catch(()=>{});
}

setInterval(pollTelemetry,  200);
setInterval(pollState,      500);
setInterval(pollChannels,   1000);
setInterval(pollWifi,       1000);
setInterval(pollJoystick,   100);

// ================================================================
// Backend reachability banner - every poll* above already swallows its own
// fetch errors silently (by design, so one dead endpoint doesn't spam the
// console), which means a real network gap between THIS BROWSER and the
// Flask server (e.g. this machine's own wifi adapter reassociating to a
// new network mid wifi-connect, which briefly drops any HTTP connection
// that isn't going over 127.0.0.1) looks identical to the page just being
// broken - nothing updates, buttons appear to do nothing, with zero
// indication why. This makes that gap visible and self-clearing instead.
// ================================================================
let _gcsOfflineStreak = 0;
function pollBackendReachable() {
  fetch('/api/connected').then(r => {
    if (!r.ok) throw new Error('bad status');
    _gcsOfflineStreak = 0;
    document.getElementById('gcsOfflineBanner').style.display = 'none';
  }).catch(() => {
    _gcsOfflineStreak++;
    if (_gcsOfflineStreak >= 3) {   // ~3s of failures - ignore a single dropped poll
      document.getElementById('gcsOfflineBanner').style.display = 'block';
    }
  });
}
setInterval(pollBackendReachable, 1000);

// ================================================================
// Commands
// ================================================================
function sendCmd(task) {
  if (task === 'start_recording') {
    const bStart = document.getElementById('btnStartRec');
    const bStop  = document.getElementById('btnStopRec');
    if (bStart) bStart.classList.add('is-selected');
    if (bStop)  bStop.classList.remove('is-selected');
  } else if (task === 'stop_recording') {
    const bStart = document.getElementById('btnStartRec');
    const bStop  = document.getElementById('btnStopRec');
    if (bStart) bStart.classList.remove('is-selected');
    if (bStop)  bStop.classList.add('is-selected');
  } else if (task === 'save_recording') {
    const bSave = document.getElementById('btnSaveBag');
    if (bSave) {
      bSave.classList.add('is-selected');
      setTimeout(() => bSave.classList.remove('is-selected'), 900);
    }
  } else if (task === 'arm') {
    const bArm = document.getElementById('btnArm');
    const bDis = document.getElementById('btnDisarm');
    if (bArm) bArm.classList.add('is-selected');
    if (bDis) bDis.classList.remove('is-selected');
  } else if (task === 'disarm') {
    const bArm = document.getElementById('btnArm');
    const bDis = document.getElementById('btnDisarm');
    if (bArm) bArm.classList.remove('is-selected');
    if (bDis) bDis.classList.add('is-selected');
  } else if (task === 'lights_on') {
    const bOn  = document.getElementById('btnEstopOn');
    const bOff = document.getElementById('btnEstopOff');
    if (bOn)  bOn.classList.add('is-selected');
    if (bOff) bOff.classList.remove('is-selected');
  } else if (task === 'lights_off') {
    const bOn  = document.getElementById('btnEstopOn');
    const bOff = document.getElementById('btnEstopOff');
    if (bOn)  bOn.classList.remove('is-selected');
    if (bOff) bOff.classList.add('is-selected');
  }
  fetch('/api/command', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({type:'command', task})
  }).catch(e=>console.error(e));
}

function setMode(mode) {
  document.querySelectorAll('.mode-buttons .btn').forEach(b=>b.classList.remove('active'));
  const map = {manual:'modeManual',auto:'modeAuto',teleop:'modeTeleop'};
  if (map[mode]) {
    const el = document.getElementById(map[mode]);
    if (el) el.classList.add('active');
  }
  fetch('/api/command', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({type:'set_mode', mode})
  }).catch(e=>console.error(e));
}

function rebootSystem() {
  if (!confirm('This will REBOOT THE ENTIRE AUV/JETSON SYSTEM right now.\nThe tether link will drop until it comes back up. Continue?')) return;
  _attemptReboot('');
}

// Tries passwordless sudo first (empty password); if the Jetson reports
// needs_password (no passwordless sudo configured, or a wrong password on
// a retry), opens the password modal instead of just failing silently.
function _attemptReboot(password) {
  fetch('/api/reboot', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({password})
  }).then(r=>r.json()).then(d => {
    if (d.error) { alert(d.error); return; }
    if (d.success === true) return;   // link is about to drop - nothing else to show
    if (d.needs_password) {
      openRebootPasswordModal(password
        ? 'Incorrect password - try again, or Cancel:'
        : "Passwordless sudo isn't configured on the Jetson - enter the sudo password:");
    } else {
      alert('Reboot ' + (d.success === null ? 'status unknown: ' : 'FAILED: ') + d.message);
    }
  }).catch(e => alert('Reboot request failed: ' + e));
}

function openRebootPasswordModal(promptText) {
  document.getElementById('rebootModalPrompt').textContent = promptText;
  const input = document.getElementById('rebootPasswordInput');
  input.value = '';
  document.getElementById('rebootModal').style.display = 'flex';
  input.focus();
}

function closeRebootModal() {
  document.getElementById('rebootModal').style.display = 'none';
  document.getElementById('rebootPasswordInput').value = '';
}

function submitRebootPassword() {
  const password = document.getElementById('rebootPasswordInput').value;
  closeRebootModal();
  if (!password) return;
  _attemptReboot(password);
}

document.getElementById('rebootPasswordInput').addEventListener('keydown', e => {
  if (e.key === 'Enter') { e.preventDefault(); submitRebootPassword(); }
  if (e.key === 'Escape') { e.preventDefault(); closeRebootModal(); }
});

function setBlackboxStream(enabled) {
  const lbl = document.getElementById('bbStreamLabel');
  lbl.textContent = enabled ? 'ON' : 'OFF';
  lbl.style.color = enabled ? 'var(--accent2)' : 'var(--muted)';
  sendCmd(enabled ? 'enable_blackbox_stream' : 'disable_blackbox_stream');
}

function setMagStream(enabled) {
  const lbl = document.getElementById('magStreamLabel');
  lbl.textContent = enabled ? 'ON' : 'OFF';
  lbl.style.color = enabled ? 'var(--accent2)' : 'var(--muted)';
  sendCmd(enabled ? 'enable_mag_stream' : 'disable_mag_stream');
}

function setDepthHold(enabled) {
  const lbl = document.getElementById('depthHoldLabel');
  lbl.textContent = enabled ? 'ON' : 'OFF';
  lbl.style.color = enabled ? 'var(--accent2)' : 'var(--muted)';
  sendCmd(enabled ? 'enable_depth_hold' : 'disable_depth_hold');
}

function setJoyRelay(enabled) {
  const lbl = document.getElementById('joyRelayLabel');
  lbl.textContent = enabled ? 'ON' : 'OFF';
  lbl.style.color = enabled ? 'var(--accent2)' : 'var(--muted)';
  fetch('/api/physical_joystick', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({enabled})
  }).catch(e=>console.error(e));
}

// ================================================================
// Theme (Black, Dark, System Auto, Light)
// ================================================================
const _sysColorSchemeMql = window.matchMedia ? window.matchMedia('(prefers-color-scheme: light)') : null;

function _resolveEffectiveTheme(mode) {
  if (mode === 'light') return 'light';
  if (mode === 'black') return 'black';
  if (mode === 'system') {
    return (_sysColorSchemeMql && _sysColorSchemeMql.matches) ? 'light' : 'dark';
  }
  return 'dark';
}

function applyTheme(modeOrTheme) {
  const mode = (modeOrTheme === 'black' || modeOrTheme === 'dark' || modeOrTheme === 'system' || modeOrTheme === 'light')
    ? modeOrTheme
    : 'dark';
  const effectiveTheme = _resolveEffectiveTheme(mode);
  document.documentElement.setAttribute('data-theme', effectiveTheme);
  document.documentElement.setAttribute('data-theme-mode', mode);

  const btn = document.getElementById('themeToggleBtn');
  if (btn) {
    const label = (effectiveTheme === 'light') ? 'Switch to Dark Mode' : 'Switch to Light Mode';
    btn.setAttribute('title', label);
    btn.setAttribute('aria-label', label);
  }

  const mpFrame = document.getElementById('mapPlannerFrame');
  if (mpFrame) {
    try {
      if (mpFrame.contentDocument && mpFrame.contentDocument.documentElement) {
        mpFrame.contentDocument.documentElement.setAttribute('data-theme', effectiveTheme);
      }
      if (mpFrame.contentWindow) {
        mpFrame.contentWindow.postMessage({ gcsTheme: effectiveTheme }, window.location.origin);
      }
    } catch (e) {}
  }
  if (typeof vjoyDraw === 'function') vjoyDraw();
  if (typeof renderJoyStick === 'function') renderJoyStick(_lastJoyX, _lastJoyY, _lastJoyConn);
  if (typeof drawCompass === 'function') {
    const hEl = document.getElementById('compassHeadingVal');
    const hVal = (hEl && hEl.textContent !== '--') ? parseFloat(hEl.textContent) : null;
    drawCompass(hVal);
  }
  const scene3d = document.getElementById('orient3dScene');
  const dEl = document.getElementById('tDepth');
  if (scene3d && typeof depthToColor === 'function') {
    const dVal = (dEl && dEl.textContent !== '--') ? parseFloat(dEl.textContent) : 0;
    scene3d.style.backgroundColor = depthToColor(dVal);
  }
}

function setThemeMode(mode) {
  localStorage.setItem('gcsTheme', mode);
  applyTheme(mode);
}

function toggleTheme() {
  const currentEffective = document.documentElement.getAttribute('data-theme') || 'dark';
  const next = (currentEffective === 'light') ? 'dark' : 'light';
  setThemeMode(next);
}

if (_sysColorSchemeMql) {
  const _onSysThemeChange = () => {
    const currentMode = document.documentElement.getAttribute('data-theme-mode') || localStorage.getItem('gcsTheme') || 'dark';
    if (currentMode === 'system') applyTheme('system');
  };
  if (_sysColorSchemeMql.addEventListener) {
    _sysColorSchemeMql.addEventListener('change', _onSysThemeChange);
  } else if (_sysColorSchemeMql.addListener) {
    _sysColorSchemeMql.addListener(_onSysThemeChange);
  }
}

// Dark/Light control-station theme by default; whatever the operator last picked wins.
applyTheme(localStorage.getItem('gcsTheme') || 'dark');

// Guard flag: true while we're programmatically updating the checkbox so
// the change event doesn't fire a redundant disable_telemetry command.
let _telemToggleProgrammatic = false;

document.getElementById('telemToggle').addEventListener('change', function() {
  if (_telemToggleProgrammatic) return;   // ignore synthetic changes from pollTelemetry
  setTelemEnabled(this.checked);
});

function setTelemEnabled(enabled) {
  const task = enabled ? 'enable_telemetry' : 'disable_telemetry';
  const lbl = document.getElementById('telemToggleLabel');
  lbl.textContent = enabled ? 'ACTIVE' : 'PAUSED';
  lbl.style.color = enabled ? 'var(--accent2)' : 'var(--muted)';
  sendCmd(task);
}

function _syncTelemToggleUI(enabled) {
  // Called from pollTelemetry to sync UI without sending a command.
  const cb = document.getElementById('telemToggle');
  if (cb.checked !== enabled) {
    _telemToggleProgrammatic = true;
    cb.checked = enabled;
    _telemToggleProgrammatic = false;
    const lbl = document.getElementById('telemToggleLabel');
    lbl.textContent = enabled ? 'ACTIVE' : 'PAUSED';
    lbl.style.color = enabled ? 'var(--accent2)' : 'var(--muted)';
  }
}

// ================================================================
// Recording UI
// ================================================================
function updateRecUI(isRecording) {
  const ind  = document.getElementById('recIndicator');
  const stat = document.getElementById('recStatus');
  const pill = document.getElementById('recPill');
  const bStart = document.getElementById('btnStartRec');
  const bStop  = document.getElementById('btnStopRec');
  if (bStart && bStop) {
    bStart.classList.toggle('is-selected', !!isRecording);
    bStop.classList.toggle('is-selected', !isRecording);
  }
  const recSvg = '<svg class="pill-svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><circle cx="12" cy="12" r="8"/><circle cx="12" cy="12" r="3" fill="currentColor"/></svg>';
  if (isRecording) {
    ind.className = 'rec-indicator recording';
    stat.textContent = 'Recording...';
    pill.innerHTML   = `<span class="dot"></span>${recSvg} REC`;
    pill.className   = 'pill danger';
  } else {
    ind.className = 'rec-indicator';
    stat.textContent = 'Not recording';
    pill.innerHTML   = `<span class="dot"></span>${recSvg} REC OFF`;
    pill.className   = 'pill';
  }
}

// ================================================================
// ROS2 Topics
// ================================================================
function refreshTopics() {
  document.getElementById('topicList').innerHTML =
    '<div class="dashed-empty">Loading...</div>';
  fetch('/api/list_topics', {method:'POST'})
    .then(r=>r.json())
    .then(d => renderTopicList(d.topics || []))
    .catch(e => {
      document.getElementById('topicList').innerHTML =
        `<span style="color:var(--danger);font-size:.75rem">Error: ${e}</span>`;
    });
}

function renderTopicList(topics) {
  const el = document.getElementById('topicList');
  if (!topics.length) {
    el.innerHTML = '<div class="dashed-empty">No topics found</div>';
    return;
  }
  const headHtml = `
    <div class="gcs-table-head">
      <span class="gcs-table-col">TOPIC ENDPOINT (${topics.length})</span>
      <span class="gcs-table-col right">MESSAGE TYPE</span>
    </div>`;
  el.innerHTML = headHtml + topics.map(t => `
    <div class="topic-row">
      <span class="topic-name" title="${escHtml(t.name)}">${escHtml(t.name)}</span>
      <span class="topic-types">${escHtml((t.types || []).join(', '))}</span>
    </div>`).join('');
}

// ================================================================
// Black Box Console
// ================================================================
let _blackboxSeq = 0;

function pollBlackbox() {
  fetch('/api/blackbox?since=' + _blackboxSeq).then(r=>r.json()).then(d => {
    const box = document.getElementById('blackboxConsole');
    if (!d.entries || !d.entries.length) { _blackboxSeq = d.seq; return; }
    const atBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 30;
    if (_blackboxSeq === 0) box.innerHTML = '';  // clear the "Waiting..." placeholder
    for (const e of d.entries) {
      const line = document.createElement('span');
      line.className = 'console-line';
      const t = new Date(e.ts * 1000).toLocaleTimeString();
      const tagColor = e.data_type === 'reboot_failed' ? 'var(--danger)' : '';
      line.innerHTML = `<span class="ts">[${t}]</span> <span class="tag" style="color:${tagColor}">${escHtml(e.data_type)}</span> ${escHtml(e.message)}`;
      box.appendChild(line);
      // reboot_failed is sent directly regardless of the streaming toggle
      // (see process_incoming's "reboot_result" handler) as a persistent
      // record here for anyone else watching the console - the interactive
      // operator who clicked Reboot already got a modal/alert directly
      // from _attemptReboot's synchronous /api/reboot response, so no
      // second alert() here (that would double up for that operator).
    }
    _blackboxSeq = d.seq;
    while (box.children.length > 300) box.removeChild(box.firstChild);
    if (atBottom) box.scrollTop = box.scrollHeight;
  }).catch(()=>{});
}
setInterval(pollBlackbox, 1000);

// ================================================================
// Bad Data Console
// ================================================================
let _badDataSeq = 0;
let _badDataConsoleCleared = false;  // set by clearBadDataConsole() so the NEXT poll wipes its placeholder too, not just the initial one

function pollBadData() {
  fetch('/api/bad_data?since=' + _badDataSeq).then(r=>r.json()).then(d => {
    const box = document.getElementById('badDataConsole');
    if (!d.entries || !d.entries.length) { _badDataSeq = d.seq; return; }
    const atBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 30;
    if (_badDataSeq === 0 || _badDataConsoleCleared) {
      box.innerHTML = '';  // clear the "No bad data seen yet" / "Cleared…" placeholder
      _badDataConsoleCleared = false;
    }
    for (const e of d.entries) {
      const line = document.createElement('span');
      line.className = 'console-line';
      const t = new Date(e.ts * 1000).toLocaleTimeString();
      line.innerHTML = `<span class="ts">[${t}]</span> <span class="tag" style="color:var(--danger)">${escHtml(e.channel)}/${escHtml(e.kind)}</span> ${escHtml(e.detail)}`;
      box.appendChild(line);
    }
    _badDataSeq = d.seq;
    while (box.children.length > 300) box.removeChild(box.firstChild);
    if (atBottom) box.scrollTop = box.scrollHeight;
  }).catch(()=>{});
}
setInterval(pollBadData, 1000);

function clearBadDataConsole() {
  // Client-side only - just clears what's currently displayed so old
  // noise doesn't obscure new events; does not touch the server's log,
  // so refreshing the page (or another viewer) still sees full history.
  document.getElementById('badDataConsole').innerHTML =
    '<span style="color:var(--muted)">Cleared - waiting for new bad data…</span>';
  _badDataConsoleCleared = true;
}

// ================================================================
// Remote Terminal
// ================================================================
let _terminalSeq = 0;
const _terminalPending = new Map();  // request_id -> output <span> awaiting its result

function runTerminalCommand() {
  const input = document.getElementById('termInput');
  const command = input.value.trim();
  if (!command) return;
  const timeoutInput = document.getElementById('termTimeout').value;
  const maxLinesInput = document.getElementById('termMaxLines').value;
  const body = {command};
  if (timeoutInput) body.timeout = parseFloat(timeoutInput);
  if (maxLinesInput) body.max_lines = parseInt(maxLinesInput, 10);
  input.value = '';
  fetch('/api/run_terminal', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify(body)
  }).then(r=>r.json()).then(d => {
    if (d.error) _terminalAppendError(d.error);
  }).catch(e => _terminalAppendError(String(e)));
}

document.getElementById('termInput').addEventListener('keydown', e => {
  if (e.key === 'Enter') { e.preventDefault(); runTerminalCommand(); }
});

function _terminalAppendError(msg) {
  const box = document.getElementById('terminalConsole');
  if (_terminalSeq === 0 && box.children.length === 1) box.innerHTML = '';
  const line = document.createElement('span');
  line.className = 'console-line';
  line.innerHTML = `<span class="tag" style="color:var(--danger)">error</span> ${escHtml(msg)}`;
  box.appendChild(line);
  box.scrollTop = box.scrollHeight;
}

function pollTerminal() {
  fetch('/api/terminal_log?since=' + _terminalSeq).then(r=>r.json()).then(d => {
    const box = document.getElementById('terminalConsole');
    if (!d.entries || !d.entries.length) { _terminalSeq = d.seq; return; }
    if (_terminalSeq === 0) box.innerHTML = '';  // clear the "No commands run yet" placeholder
    const atBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 30;
    for (const e of d.entries) {
      if (e.kind === 'cmd') {
        const t = new Date(e.ts * 1000).toLocaleTimeString();
        const cmdLine = document.createElement('span');
        cmdLine.className = 'console-line';
        cmdLine.innerHTML = `<span class="ts">[${t}]</span> <span class="tag">$</span> ${escHtml(e.command)}`;
        box.appendChild(cmdLine);

        const outLine = document.createElement('span');
        outLine.className = 'console-line';
        outLine.style.color = 'var(--muted)';
        outLine.textContent = '  (running…)';
        box.appendChild(outLine);
        _terminalPending.set(e.request_id, outLine);
      } else if (e.kind === 'result') {
        const statusTag   = e.timed_out ? 'TIMEOUT' : (e.success ? 'OK' : `EXIT ${e.exit_code}`);
        const statusColor = e.timed_out ? 'var(--warn)' : (e.success ? 'var(--ok)' : 'var(--danger)');
        let body = e.output || '(no output)';
        if (e.truncated) body += `\n[output capped - showing last ${e.max_lines || 50} lines only]`;
        const html = `<span class="tag" style="color:${statusColor}">[${statusTag}]</span>\n${escHtml(body)}`;
        const pending = _terminalPending.get(e.request_id);
        if (pending) {
          pending.style.color = '';
          pending.innerHTML = html;
          _terminalPending.delete(e.request_id);
        } else {
          const outLine = document.createElement('span');
          outLine.className = 'console-line';
          outLine.innerHTML = html;
          box.appendChild(outLine);
        }
      }
    }
    _terminalSeq = d.seq;
    while (box.children.length > 400) box.removeChild(box.firstChild);
    if (atBottom) box.scrollTop = box.scrollHeight;
  }).catch(()=>{});
}
setInterval(pollTerminal, 1000);

// ================================================================
// File Browser
// ================================================================
function refreshFiles() {
  document.getElementById('fileTree').innerHTML =
    '<span style="color:var(--muted);font-size:.75rem">Loading...</span>';
  fetch('/api/list_files', {method:'POST'})
    .then(r=>r.json())
    .then(d => renderFileTree(d.tree || []))
    .catch(e => {
      document.getElementById('fileTree').innerHTML =
        `<span style="color:var(--danger);font-size:.75rem">Error: ${e}</span>`;
    });
}

function renderFileTree(tree) {
  const el = document.getElementById('fileTree');
  if (!tree || !tree.length) {
    el.innerHTML = '<span style="color:var(--muted);font-size:.75rem">No files found</span>';
    return;
  }
  const headHtml = `
    <div class="gcs-table-head">
      <span class="gcs-table-col">ARCHIVE / DIRECTORY</span>
      <span class="gcs-table-col right">SIZE &amp; ACTIONS</span>
    </div>`;
  el.innerHTML = headHtml + buildTreeHTML(tree);
}

function buildTreeHTML(nodes) {
  let html = '';
  const folderSvg = `<svg class="tree-svg" viewBox="0 0 24 24"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg>`;
  const fileSvg   = `<svg class="tree-svg" viewBox="0 0 24 24"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>`;
  for (const node of nodes) {
    if (node.type === 'folder') {
      html += `
        <div class="folder-item">
          <div style="display:flex;align-items:center;justify-content:space-between;">
            <div class="folder-header" onclick="toggleFolder(this)" style="flex:1;min-width:0;">
              <span class="folder-arrow">▶</span>
              <span>${folderSvg}${escHtml(node.name)}</span>
            </div>
            <span class="file-actions">
              <button class="btn-icon dl"  title="Download as .zip" onclick="event.stopPropagation(); downloadJetsonFolder('${escAttr(node.path)}','${escAttr(node.name)}')">ZIP</button>
              <button class="btn-icon del" title="Delete folder and everything in it" onclick="event.stopPropagation(); deleteJetsonFolder('${escAttr(node.path)}','${escAttr(node.name)}')">DEL</button>
            </span>
          </div>
          <div class="folder-children">
            ${buildTreeHTML(node.children || [])}
          </div>
        </div>`;
    } else {
      html += `
        <div class="file-row">
          <span class="file-name" title="${escHtml(node.path)}">${fileSvg}${escHtml(node.name)}</span>
          <span class="file-size">${humanSize(node.size || 0)}</span>
          <span class="file-actions">
            <button class="btn-icon dl"  onclick="downloadJetsonFile('${escAttr(node.path)}','${escAttr(node.name)}')">DL</button>
            <button class="btn-icon del" onclick="deleteJetsonFile('${escAttr(node.path)}','${escAttr(node.name)}')">DEL</button>
          </span>
        </div>`;
    }
  }
  return html;
}

function toggleFolder(header) {
  header.classList.toggle('open');
  // Looked up via the closest .folder-item rather than header.nextElementSibling -
  // the folder-header is no longer .folder-children's direct sibling now that it
  // shares a row with the download/delete buttons (see buildTreeHTML).
  const children = header.closest('.folder-item').querySelector(':scope > .folder-children');
  children.classList.toggle('open');
}

function escHtml(s)  { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
function escAttr(s)  { return String(s).replace(/"/g,'&quot;').replace(/'/g,'&#39;'); }

function downloadJetsonFile(path, name) {
  const dlStatus   = document.getElementById('dlStatus');
  const dlProgress = document.getElementById('dlProgress');
  dlStatus.textContent   = `Downloading ${name}…`;
  dlProgress.style.width = '10%';

  fetch('/api/download?filename=' + encodeURIComponent(path))
    .then(res => {
      if (!res.ok) return res.json().then(e=>{ throw new Error(e.error||'Download failed'); });
      // Stream response into a blob, using content-length for progress if available
      const total = parseInt(res.headers.get('Content-Length') || '0', 10);
      const reader = res.body.getReader();
      const chunks = [];
      let received = 0;
      function read() {
        return reader.read().then(({done, value}) => {
          if (done) return;
          chunks.push(value);
          received += value.length;
          if (total > 0) dlProgress.style.width = Math.min(100, (received/total*100)).toFixed(0) + '%';
          return read();
        });
      }
      return read().then(() => {
        const blob = new Blob(chunks);
        const url  = URL.createObjectURL(blob);
        const a    = document.createElement('a');
        a.href     = url; a.download = name;
        document.body.appendChild(a); a.click(); a.remove();
        URL.revokeObjectURL(url);
      });
    })
    .then(() => {
      dlStatus.textContent   = `✔ ${name} saved`;
      dlProgress.style.width = '100%';
    })
    .catch(err => {
      dlStatus.textContent   = '✕ ' + err.message;
      dlProgress.style.width = '0%';
    });
}

function deleteJetsonFile(path, name) {
  if (!confirm(`Delete "${name}" from Jetson?\nThis cannot be undone.`)) return;
  fetch('/api/delete_file', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({path})
  })
  .then(r=>r.json())
  .then(() => { refreshFiles(); })
  .catch(e => alert('Delete error: ' + e));
}

function deleteJetsonFolder(path, name) {
  // Stronger wording than the single-file confirm - recursive delete of
  // an entire folder is a meaningfully bigger blast radius.
  if (!confirm(`Delete the ENTIRE folder "${name}" and everything inside it from the Jetson?\nThis cannot be undone.`)) return;
  fetch('/api/delete_folder', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({path})
  })
  .then(r=>r.json())
  .then(() => { refreshFiles(); })
  .catch(e => alert('Delete error: ' + e));
}

function downloadJetsonFolder(path, name) {
  const dlStatus   = document.getElementById('dlStatus');
  const dlProgress = document.getElementById('dlProgress');
  dlStatus.textContent   = `Zipping and downloading ${name}…`;
  dlProgress.style.width = '10%';

  fetch('/api/download_folder?path=' + encodeURIComponent(path))
    .then(res => {
      if (!res.ok) return res.json().then(e=>{ throw new Error(e.error||'Download failed'); });
      const total = parseInt(res.headers.get('Content-Length') || '0', 10);
      const reader = res.body.getReader();
      const chunks = [];
      let received = 0;
      function read() {
        return reader.read().then(({done, value}) => {
          if (done) return;
          chunks.push(value);
          received += value.length;
          if (total > 0) dlProgress.style.width = Math.min(100, (received/total*100)).toFixed(0) + '%';
          return read();
        });
      }
      return read().then(() => {
        const blob = new Blob(chunks);
        const url  = URL.createObjectURL(blob);
        const a    = document.createElement('a');
        a.href     = url; a.download = name + '.zip';
        document.body.appendChild(a); a.click(); a.remove();
        URL.revokeObjectURL(url);
      });
    })
    .then(() => {
      dlStatus.textContent   = `✔ ${name}.zip saved`;
      dlProgress.style.width = '100%';
    })
    .catch(err => {
      dlStatus.textContent   = '✕ ' + err.message;
      dlProgress.style.width = '0%';
    });
}

// ================================================================
// Upload
// ================================================================
function uploadFile() {
  const input = document.getElementById('fileInput');
  if (!input.files || !input.files.length) { alert('Select a file first.'); return; }
  const file     = input.files[0];
  const formData = new FormData();
  formData.append('file', file);
  const prog   = document.getElementById('upProgress');
  const status = document.getElementById('upStatus');
  status.textContent = 'Uploading…';
  prog.style.width   = '0%';
  const xhr = new XMLHttpRequest();
  xhr.open('POST', '/api/upload', true);
  xhr.upload.onprogress = e => {
    if (e.lengthComputable) {
      const pct = (e.loaded/e.total*100).toFixed(0);
      prog.style.width   = pct + '%';
      status.textContent = `Uploading… ${pct}%`;
    }
  };
  xhr.onload = () => {
    if (xhr.status === 200) {
      status.textContent = '✔ Upload complete';
      prog.style.width   = '100%';
    } else {
      status.textContent = '✕ Upload failed (' + xhr.status + ')';
    }
  };
  xhr.onerror = () => { status.textContent = '✕ Upload error'; };
  xhr.send(formData);
}

// ================================================================
// Physical Joystick Visualiser (2D Stick Radar + Axes + Buttons)
// ================================================================
var _lastJoyX = 0.55, _lastJoyY = -0.40, _lastJoyConn = false;
var _joyCanvas = null, _jctx = null, _JW = 136, _JH = 136;

function _ensureJoyCanvas() {
  if (_jctx) return true;
  _joyCanvas = document.getElementById('joyStickCanvas');
  if (!_joyCanvas) return false;
  _jctx = _joyCanvas.getContext('2d');
  if (!_jctx) return false;
  _joyCanvas.width = _JW * 2;
  _joyCanvas.height = _JH * 2;
  _jctx.scale(2, 2);
  return true;
}

function renderJoyStick(jx, jy, connected) {
  if (jx !== undefined) _lastJoyX = jx;
  if (jy !== undefined) _lastJoyY = jy;
  if (connected !== undefined) _lastJoyConn = !!connected;
  if (!_ensureJoyCanvas()) return;

  const theme = document.documentElement.getAttribute('data-theme') || 'dark';
  const isLight = theme === 'light';
  const isBlack = theme === 'black';
  const cx = _JW / 2, cy = _JH / 2;
  const collarR = 67;
  const grooveR = 61.5;
  const dialR   = 59.5;

  _jctx.clearRect(0, 0, _JW, _JH);

  // 1. Outer bezel collar ring
  _jctx.beginPath();
  _jctx.arc(cx, cy, collarR, 0, Math.PI * 2);
  _jctx.fillStyle = isLight ? '#f4f6f9' : (isBlack ? '#0b1019' : '#182230');
  _jctx.fill();
  _jctx.strokeStyle = isLight ? '#b8c3ce' : (isBlack ? '#1e293b' : '#324458');
  _jctx.lineWidth = 1;
  _jctx.stroke();

  // 2. Subtle recessed groove between outer bezel and dial rim
  _jctx.beginPath();
  _jctx.arc(cx, cy, grooveR, 0, Math.PI * 2);
  _jctx.fillStyle = isLight ? '#ffffff' : (isBlack ? '#05080d' : '#121a24');
  _jctx.fill();

  // 3. Recessed instrument dial face with subtle radial gradient
  const dialGrad = _jctx.createRadialGradient(cx, cy, 4, cx, cy, dialR);
  if (isLight) {
    dialGrad.addColorStop(0, '#ffffff');
    dialGrad.addColorStop(0.55, '#f8f9fb');
    dialGrad.addColorStop(1, '#eff2f6');
  } else if (isBlack) {
    dialGrad.addColorStop(0, '#090d14');
    dialGrad.addColorStop(0.55, '#06090e');
    dialGrad.addColorStop(1, '#030508');
  } else {
    dialGrad.addColorStop(0, '#121a24');
    dialGrad.addColorStop(0.55, '#0f1620');
    dialGrad.addColorStop(1, '#0c121a');
  }
  _jctx.beginPath();
  _jctx.arc(cx, cy, dialR, 0, Math.PI * 2);
  _jctx.fillStyle = dialGrad;
  _jctx.fill();
  _jctx.strokeStyle = isLight ? '#d5dce3' : (isBlack ? '#1a2434' : '#243140');
  _jctx.lineWidth = 1.5;
  _jctx.stroke();

  // 4. Inner concentric dashed circle
  const dashR = 34;
  _jctx.beginPath();
  _jctx.arc(cx, cy, dashR, 0, Math.PI * 2);
  _jctx.strokeStyle = isLight ? '#cbd2da' : '#273443';
  _jctx.lineWidth = 1;
  _jctx.setLineDash([3, 3]);
  _jctx.stroke();
  _jctx.setLineDash([]);

  // 5. Solid crosshair lines extending slightly past the dashed ring
  const crossLen = 44;
  _jctx.beginPath();
  _jctx.moveTo(cx - crossLen, cy); _jctx.lineTo(cx + crossLen, cy);
  _jctx.moveTo(cx, cy - crossLen); _jctx.lineTo(cx, cy + crossLen);
  _jctx.strokeStyle = isLight ? '#b8c3ce' : '#2c3a4b';
  _jctx.lineWidth = 1;
  _jctx.stroke();

  // 6. Stick position marker (outer cyan ring + inner solid cyan core)
  const maxTravel = 34;
  const tx = cx + Math.max(-1, Math.min(1, _lastJoyX)) * maxTravel;
  const ty = cy + Math.max(-1, Math.min(1, _lastJoyY)) * maxTravel;
  const cyanColor = isLight ? '#0b7a8c' : '#2fb7c8';
  const ringFill  = isLight ? 'rgba(11, 122, 140, 0.10)' : 'rgba(47, 183, 200, 0.10)';

  // Outer ring
  _jctx.beginPath();
  _jctx.arc(tx, ty, 10.5, 0, Math.PI * 2);
  _jctx.fillStyle = ringFill;
  _jctx.fill();
  _jctx.strokeStyle = cyanColor;
  _jctx.lineWidth = 1.3;
  _jctx.stroke();

  // Solid cyan core
  _jctx.beginPath();
  _jctx.arc(tx, ty, 7.2, 0, Math.PI * 2);
  _jctx.fillStyle = cyanColor;
  _jctx.fill();
}

function renderJoyAxes(axes) {
  const container = document.getElementById('joyAxesBars');
  if (!container) return;
  const displayAxes = (axes && axes.length >= 4) ? axes : [0.55, -0.40, 0.70, -0.20].map((d, i) => (axes && axes[i] !== undefined) ? axes[i] : d);
  while (container.children.length < displayAxes.length) {
    const row = document.createElement('div');
    row.className = 'joy-axis-row';
    row.innerHTML = `
      <span class="joy-axis-label">A${container.children.length}</span>
      <div class="joy-axis-track">
        <div class="joy-axis-center"></div>
        <div class="joy-axis-fill" style="width:0;left:50%;"></div>
      </div>
      <span class="joy-axis-val">0.00</span>`;
    container.appendChild(row);
  }
  while (container.children.length > displayAxes.length) {
    container.removeChild(container.lastChild);
  }
  displayAxes.forEach((v, i) => {
    const row   = container.children[i];
    if (!row) return;
    const fill  = row.querySelector('.joy-axis-fill');
    const label = row.querySelector('.joy-axis-val') || row.querySelector('span:last-child');
    const clamped = Math.max(-1, Math.min(1, Number(v) || 0));
    const pct   = Math.abs(clamped) * 50;
    if (clamped >= 0) {
      fill.style.left  = '50%';
      fill.style.width = pct + '%';
    } else {
      fill.style.width = pct + '%';
      fill.style.left  = (50 - pct) + '%';
    }
    label.textContent = clamped.toFixed(2);
  });
}

function renderJoyBtns(buttons) {
  const container = document.getElementById('joyBtns');
  if (!container) return;
  const count = Math.max(12, (buttons && buttons.length) ? buttons.length : 12);
  while (container.children.length < count) {
    const el = document.createElement('div');
    el.className = 'joy-btn-indicator';
    el.textContent = container.children.length;
    container.appendChild(el);
  }
  while (container.children.length > count) {
    container.removeChild(container.lastChild);
  }
  for (let i = 0; i < count; i++) {
    const el = container.children[i];
    if (!el) continue;
    const pressed = buttons && buttons[i] ? true : false;
    el.classList.toggle('pressed', pressed);
  }
}

// Initialize Physical Joystick instrument bay immediately matching Reference Image 2
renderJoyStick(0.55, -0.40, false);
renderJoyAxes([0.55, -0.40, 0.70, -0.20]);
renderJoyBtns([1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0]);

// ================================================================
// Virtual Joystick (Emergency Station)
// NOTE: the block below is deliberately isolated in its OWN script
// element (the main block above is closed off first) so that the
// emergency joystick keeps rendering and sending even if any earlier
// statement in the main block throws or fails to parse.
// ================================================================
</script>
<script>
const vjoyCanvas = document.getElementById('vjoyCanvas');
const vctx = vjoyCanvas.getContext('2d');
const W = 198, H = 198;
vjoyCanvas.width = W * 2;
vjoyCanvas.height = H * 2;
vctx.scale(2, 2);
const CX = W/2, CY = H/2;
const VJOY_R = W/2 - 12;   // canvas radius matching dial
let vjoyX = 0, vjoyY = 0;  // -1 to 1
let vjoyDragging = false;

function vjoyToggleChanged(enabled) {
  const statusEl = document.getElementById('vjoyToggleStatus');
  if (statusEl) {
    statusEl.textContent = enabled ? 'ON' : 'OFF';
    statusEl.style.color = enabled ? 'var(--ok, #10B981)' : 'var(--muted)';
  }
  vjoyUpdate();
}

function vjoyDraw() {
  const theme = document.documentElement.getAttribute('data-theme') || 'dark';
  const isDark = theme !== 'light';
  vctx.clearRect(0, 0, W, H);

  // 1. Outer dial boundary ring
  vctx.beginPath();
  vctx.arc(CX, CY, VJOY_R, 0, Math.PI * 2);
  vctx.strokeStyle = isDark ? '#1E2E42' : '#CBD5E1';
  vctx.lineWidth = 1;
  vctx.stroke();

  // 2. Degree ticks around perimeter (36 ticks every 10 deg)
  for (let i = 0; i < 36; i++) {
    const ang = (i * 10) * Math.PI / 180;
    const isMajor = i % 3 === 0;
    const tlen = isMajor ? 5 : 3;
    const x1 = CX + Math.cos(ang) * (VJOY_R - 1);
    const y1 = CY + Math.sin(ang) * (VJOY_R - 1);
    const x2 = CX + Math.cos(ang) * (VJOY_R - 1 - tlen);
    const y2 = CY + Math.sin(ang) * (VJOY_R - 1 - tlen);
    vctx.beginPath();
    vctx.moveTo(x1, y1);
    vctx.lineTo(x2, y2);
    vctx.strokeStyle = isDark ? '#283E58' : '#D1D9E6';
    vctx.lineWidth = isMajor ? 1 : 0.7;
    vctx.stroke();
  }

  // 3. Concentric dashed travel guides
  for (const scale of [0.28, 0.55, 0.82]) {
    vctx.beginPath();
    vctx.arc(CX, CY, VJOY_R * scale, 0, Math.PI * 2);
    vctx.strokeStyle = isDark ? 'rgba(117, 145, 166, 0.24)' : 'rgba(71, 85, 105, 0.24)';
    vctx.lineWidth = 0.8;
    vctx.setLineDash([2, 4]);
    vctx.stroke();
  }
  vctx.setLineDash([]);

  // 4. Center crosshairs
  vctx.beginPath();
  vctx.moveTo(CX - VJOY_R + 8, CY); vctx.lineTo(CX + VJOY_R - 8, CY);
  vctx.moveTo(CX, CY - VJOY_R + 8); vctx.lineTo(CX, CY + VJOY_R - 8);
  vctx.strokeStyle = isDark ? 'rgba(131, 157, 177, 0.42)' : 'rgba(71, 85, 105, 0.38)';
  vctx.lineWidth = 1;
  vctx.setLineDash([3, 4]);   // dashed crosshair, like the reference scope
  vctx.stroke();
  vctx.setLineDash([]);

  // 5. Coordinates text readout pill positioned safely inside lower dial arc (never clipped by circular bezel)
  const pillW = 110, pillH = 17;
  const pillX = CX - pillW / 2;
  const pillY = CY + 49;
  vctx.beginPath();
  if (vctx.roundRect) {
    vctx.roundRect(pillX, pillY, pillW, pillH, 8.5);
  } else {
    vctx.rect(pillX, pillY, pillW, pillH);
  }
  vctx.fillStyle = isDark ? 'rgba(11, 18, 30, 0.88)' : 'rgba(241, 245, 249, 0.92)';
  vctx.fill();
  vctx.strokeStyle = isDark ? 'rgba(56, 189, 248, 0.22)' : 'rgba(148, 163, 184, 0.45)';
  vctx.lineWidth = 0.8;
  vctx.stroke();

  vctx.font = '600 9.5px JetBrains Mono, monospace';
  vctx.fillStyle = isDark ? '#94A3B8' : '#334155';
  vctx.textAlign = 'center';
  vctx.textBaseline = 'middle';
  vctx.fillText(`vx:${vjoyX.toFixed(2)}  vy:${vjoyY.toFixed(2)}`, CX, pillY + pillH / 2 + 0.5);
  vctx.textBaseline = 'alphabetic';

  // 6. Thumb coordinates & vector tether (drawn on top so knob is always visible)
  const maxTravel = VJOY_R - 18;
  const tx = CX + vjoyX * maxTravel;
  const ty = CY + vjoyY * maxTravel;
  const accentColor = isDark ? '#3B82F6' : '#0B7A8C';
  const haloColor = isDark ? 'rgba(59, 130, 246, 0.25)' : 'rgba(11, 122, 140, 0.22)';

  // Tether line from center
  if (Math.abs(vjoyX) > 0.02 || Math.abs(vjoyY) > 0.02 || vjoyDragging) {
    vctx.beginPath();
    vctx.moveTo(CX, CY);
    vctx.lineTo(tx, ty);
    vctx.strokeStyle = accentColor;
    vctx.lineWidth = 2;
    vctx.stroke();
  }

  // Center pivot dot
  vctx.beginPath();
  vctx.arc(CX, CY, 2, 0, Math.PI * 2);
  vctx.fillStyle = isDark ? '#7B93A8' : '#64748B';
  vctx.fill();

  // Thumb soft glow halo
  vctx.beginPath();
  vctx.arc(tx, ty, 16, 0, Math.PI * 2);
  vctx.fillStyle = haloColor;
  vctx.fill();

  // Thumb knob body
  vctx.beginPath();
  vctx.arc(tx, ty, 10.5, 0, Math.PI * 2);
  vctx.fillStyle = accentColor;
  vctx.fill();

  // Thumb white center core
  vctx.beginPath();
  vctx.arc(tx, ty, 3.2, 0, Math.PI * 2);
  vctx.fillStyle = isDark ? '#FFFFFF' : '#0B2A33';
  vctx.fill();
}

function vjoySetFromEvent(e) {
  const rect = vjoyCanvas.getBoundingClientRect();
  const clientX = e.touches ? e.touches[0].clientX : e.clientX;
  const clientY = e.touches ? e.touches[0].clientY : e.clientY;
  const scaleX = W / (rect.width || W);
  const scaleY = H / (rect.height || H);
  const dx = (clientX - rect.left) * scaleX - CX;
  const dy = (clientY - rect.top) * scaleY - CY;
  const dist = Math.sqrt(dx*dx + dy*dy);
  const maxDist = VJOY_R - 18;
  if (dist > maxDist) {
    vjoyX = dx / dist;
    vjoyY = dy / dist;
  } else {
    vjoyX = dx / maxDist;
    vjoyY = dy / maxDist;
  }
  vjoyDraw();
  vjoyUpdate();
}

vjoyCanvas.addEventListener('mousedown',  e => { vjoyDragging = true; vjoySetFromEvent(e); });
vjoyCanvas.addEventListener('mousemove',  e => { if (vjoyDragging) vjoySetFromEvent(e); });
vjoyCanvas.addEventListener('mouseup',    () => { vjoyDragging = false; vjoyX=0; vjoyY=0; vjoyDraw(); vjoyUpdate(); });
vjoyCanvas.addEventListener('mouseleave', () => { if(vjoyDragging){ vjoyDragging=false; vjoyX=0; vjoyY=0; vjoyDraw(); vjoyUpdate(); } });
vjoyCanvas.addEventListener('touchstart', e => { e.preventDefault(); vjoyDragging=true; vjoySetFromEvent(e); });
vjoyCanvas.addEventListener('touchmove',  e => { e.preventDefault(); vjoySetFromEvent(e); });
vjoyCanvas.addEventListener('touchend',   e => { e.preventDefault(); vjoyDragging=false; vjoyX=0; vjoyY=0; vjoyDraw(); vjoyUpdate(); });

// axes indices match core/thrustmaster.py joy_callback:
//   axes[0]+axes[1] = fins (stick X/Y)
//   axes[2]         = thruster (cmd.linear.x)
//   axes[3]         = throttle cap (1.0)
function vjoyUpdate() {
  const thrusterInput = document.getElementById('vjoyThruster');
  const rawThruster = thrusterInput ? parseFloat(thrusterInput.value) : 0;
  const thruster = rawThruster / 100;

  const valEl = document.getElementById('vjoyThrusterVal');
  if (valEl) {
    valEl.textContent = thruster.toFixed(1);
  }

  if (thrusterInput) {
    const pct = ((rawThruster + 100) / 2).toFixed(1);
    thrusterInput.style.setProperty('--thruster-pct', pct + '%');
    const isDark = (document.documentElement.getAttribute('data-theme') || 'dark') !== 'light';
    thrusterInput.style.setProperty('--vjoy-accent', isDark ? '#3B82F6' : '#0B7A8C');
  }

  const enabledCheck = document.getElementById('vjoyEnabled');
  const statusEl = document.getElementById('vjoyToggleStatus');
  if (statusEl && enabledCheck) {
    statusEl.textContent = enabledCheck.checked ? 'ON' : 'OFF';
    statusEl.style.color = enabledCheck.checked ? 'var(--ok, #10B981)' : 'var(--muted)';
  }

  if (!enabledCheck || !enabledCheck.checked) return;

  const axes = [vjoyX, vjoyY, thruster, 1.0];
  fetch('/api/command', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({type:'teleop', axes, buttons:[0,0,0]})
  }).catch(()=>{});
}

function vjoyStop() {
  vjoyX = 0; vjoyY = 0;
  const thrusterInput = document.getElementById('vjoyThruster');
  if (thrusterInput) {
    thrusterInput.value = 0;
    thrusterInput.style.setProperty('--thruster-pct', '50%');
  }
  const valEl = document.getElementById('vjoyThrusterVal');
  if (valEl) valEl.textContent = '0.0';
  vjoyDraw();
  vjoyUpdate();
  // Send zero teleop even if vjoy is disabled
  fetch('/api/command', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({type:'teleop', axes:[0,0,0,0], buttons:[0,0,0]})
  }).catch(()=>{});
}

// Interval to keep virtual joystick sending while dragging
setInterval(() => {
  const el = document.getElementById('vjoyEnabled');
  if (el && el.checked) vjoyUpdate();
}, 50);

vjoyDraw();
vjoyUpdate();

// Keep the dial in sync when the operator flips the light/dark theme.
try {
  new MutationObserver(() => vjoyDraw()).observe(
    document.documentElement,
    { attributes: true, attributeFilter: ['data-theme'] });
} catch (e) {}

// ================================================================
// ROS2 Parameters
// ================================================================
function refreshParams() {
  document.getElementById('paramList').innerHTML =
    '<div class="dashed-empty">Querying params per node over the tether (can take up to a minute on a busy graph)...</div>';
  fetch('/api/list_params', {method:'POST'})
    .then(r=>r.json())
    .then(d => {
      if (d.error) throw new Error(d.error);
      renderParamList(d.params || [], d.errors || []);
    })
    .catch(e => {
      document.getElementById('paramList').innerHTML =
        `<span style="color:var(--danger);font-size:.75rem">Error: ${escHtml(e.message || e)}</span>`;
    });
}

function renderParamList(params, errors) {
  const el = document.getElementById('paramList');
  const errBanner = (errors && errors.length)
    ? `<div style="font-size:.7rem;color:var(--warn);border:1px solid var(--warn);border-radius:var(--radius-sm);padding:6px 8px;margin-bottom:8px;font-family:var(--font-mono);">
         [WARN] ${errors.length} node(s) had problems:<br>${errors.map(escHtml).join('<br>')}
       </div>`
    : '';
  if (!params.length) {
    el.innerHTML = errBanner || '<div class="dashed-empty">No parameters found</div>';
    return;
  }
  const byNode = new Map();
  for (const p of params) {
    if (!byNode.has(p.node)) byNode.set(p.node, []);
    byNode.get(p.node).push(p);
  }
  let html = errBanner + `
    <div class="gcs-table-head">
      <span class="gcs-table-col">NODE // PARAMETER (${params.length})</span>
      <span class="gcs-table-col right">VALUE &amp; OVERRIDE</span>
    </div>`;
  for (const [node, plist] of byNode) {
    html += `<div class="param-node-group"><div class="param-node-header" title="${escHtml(node)}">${escHtml(node)}</div>`;
    for (const p of plist) {
      // Arrays need YAML flow-sequence formatting ("[a, b, c]"), not JS's
      // default Array.toString() ("a,b,c" - no brackets) - ros2 param set
      // parses the edited value back as YAML, and a bracket-less
      // comma list doesn't round-trip as an array through that.
      const valStr = (p.value === null || p.value === undefined) ? ''
        : Array.isArray(p.value) ? ('[' + p.value.join(', ') + ']')
        : String(p.value);
      html += `
        <div class="param-row" data-node="${escAttr(node)}" data-name="${escAttr(p.name)}">
          <span class="param-name" title="${escHtml(p.name)}">${escHtml(p.name)}</span>
          <input class="param-input" value="${escAttr(valStr)}" data-original="${escAttr(valStr)}" oninput="_paramInputChanged(this)">
          <button class="btn-icon" style="display:none;" onclick="updateParam(this)">Update</button>
          <span class="param-status"></span>
        </div>`;
    }
    html += `</div>`;
  }
  el.innerHTML = html;
}

function _paramInputChanged(input) {
  const row    = input.closest('.param-row');
  const btn    = row.querySelector('.btn-icon');
  const status = row.querySelector('.param-status');
  const dirty  = input.value !== input.dataset.original;
  input.classList.toggle('dirty', dirty);
  btn.style.display = dirty ? '' : 'none';
  if (dirty) { status.textContent = ''; status.className = 'param-status'; }
}

function updateParam(btn) {
  const row    = btn.closest('.param-row');
  const input  = row.querySelector('.param-input');
  const status = row.querySelector('.param-status');
  const node   = row.dataset.node;
  const name   = row.dataset.name;
  const value  = input.value;
  btn.disabled = true;
  status.textContent = 'updating…';
  status.className = 'param-status';
  fetch('/api/set_param', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({node, name, value})
  }).then(r=>r.json()).then(d => {
    btn.disabled = false;
    if (d.error) {
      status.textContent = '✕ ' + d.error;
      status.className = 'param-status err';
      return;
    }
    if (d.success) {
      status.textContent = '✔ updated';
      status.className = 'param-status ok';
      input.dataset.original = value;
      input.classList.remove('dirty');
      btn.style.display = 'none';
    } else {
      status.textContent = '✕ ' + (d.message || 'failed');
      status.className = 'param-status err';
    }
  }).catch(e => {
    btn.disabled = false;
    status.textContent = '✕ ' + e;
    status.className = 'param-status err';
  });
}

// ================================================================
// Compass
// ================================================================
const compassCanvas = document.getElementById('compassCanvas');
const cctx = compassCanvas.getContext('2d');
// Render the dial into a 2x backing store while keeping the 164px
// coordinate space below, so the hairline graduations stay crisp on HiDPI
// screens instead of being softened by the canvas being scaled down.
const CW = 164, CH2 = 164;
compassCanvas.width = CW * 2;
compassCanvas.height = CH2 * 2;
cctx.scale(2, 2);
const CCX = CW / 2, CCY = CH2 / 2, CR = 72;
let lastCompassHeading = 47.0;

function drawCompass(headingDeg) {
  const theme = document.documentElement.getAttribute('data-theme') || 'dark';
  const isLight = theme === 'light';
  const isBlack = theme === 'black';
  if (headingDeg != null && !isNaN(headingDeg)) {
    lastCompassHeading = headingDeg;
  }

  cctx.clearRect(0, 0, CW, CH2);

  // Outer bezel collar ring
  cctx.beginPath(); cctx.arc(CCX, CCY, 81, 0, Math.PI * 2);
  cctx.fillStyle = isLight ? '#f4f6f9' : (isBlack ? '#0b1019' : '#182230'); cctx.fill();
  cctx.strokeStyle = isLight ? '#b8c3ce' : (isBlack ? '#1e293b' : '#324458'); cctx.lineWidth = 1; cctx.stroke();

  // Subtle recessed groove between outer bezel and dial rim
  cctx.beginPath(); cctx.arc(CCX, CCY, 74.5, 0, Math.PI * 2);
  cctx.fillStyle = isLight ? '#ffffff' : (isBlack ? '#05080d' : '#121a24'); cctx.fill();

  // Recessed instrument dial face with subtle radial gradient
  const dialGrad = cctx.createRadialGradient(CCX, CCY, 4, CCX, CCY, CR);
  if (isLight) {
    dialGrad.addColorStop(0, '#ffffff');
    dialGrad.addColorStop(0.55, '#f8f9fb');
    dialGrad.addColorStop(1, '#eff2f6');
  } else if (isBlack) {
    dialGrad.addColorStop(0, '#090d14');
    dialGrad.addColorStop(0.55, '#06090e');
    dialGrad.addColorStop(1, '#030508');
  } else {
    dialGrad.addColorStop(0, '#121a24');
    dialGrad.addColorStop(0.55, '#0f1620');
    dialGrad.addColorStop(1, '#0c121a');
  }
  cctx.beginPath(); cctx.arc(CCX, CCY, CR, 0, Math.PI * 2);
  cctx.fillStyle = dialGrad; cctx.fill();
  cctx.strokeStyle = isLight ? '#d5dce3' : (isBlack ? '#1a2434' : '#243140'); cctx.lineWidth = 1.5; cctx.stroke();

  // Fine five-degree graduations with cardinal, 30-degree, and 15-degree hierarchy
  for (let deg = 0; deg < 360; deg += 5) {
    const isCardinal = (deg % 90 === 0);
    const isMajor = !isCardinal && (deg % 30 === 0);
    const isMid = !isCardinal && !isMajor && (deg % 15 === 0);
    const rad = (deg - 90) * Math.PI / 180;
    const len = isCardinal ? 10 : (isMajor ? 6.5 : (isMid ? 5 : 3.5));
    const x1 = CCX + Math.cos(rad) * (CR - 2);
    const y1 = CCY + Math.sin(rad) * (CR - 2);
    const x2 = CCX + Math.cos(rad) * (CR - 2 - len);
    const y2 = CCY + Math.sin(rad) * (CR - 2 - len);
    cctx.beginPath();
    cctx.moveTo(x1, y1); cctx.lineTo(x2, y2);
    if (isCardinal) {
      cctx.strokeStyle = isLight ? '#717b88' : '#738091';
      cctx.lineWidth = 1.6;
    } else if (isMajor) {
      cctx.strokeStyle = isLight ? '#66727f' : '#576371';
      cctx.lineWidth = 1.2;
    } else if (isMid) {
      cctx.strokeStyle = isLight ? '#9aa3ae' : '#37404c';
      cctx.lineWidth = 0.9;
    } else {
      cctx.strokeStyle = isLight ? '#cbd2da' : '#232b34';
      cctx.lineWidth = 0.75;
    }
    cctx.stroke();
  }

  // Inner center reference ring
  cctx.beginPath(); cctx.arc(CCX, CCY, 35.5, 0, Math.PI * 2);
  cctx.strokeStyle = isLight ? '#e4e8ed' : '#1a2431';
  cctx.lineWidth = 1; cctx.stroke();

  // Cardinal letters: N, E, S, W
  cctx.font = '700 11px JetBrains Mono, monospace';
  cctx.textAlign = 'center'; cctx.textBaseline = 'middle';
  for (const [txt, deg] of [['N', 0], ['E', 90], ['S', 180], ['W', 270]]) {
    const rad = (deg - 90) * Math.PI / 180;
    cctx.fillStyle = txt === 'N' ? (isLight ? '#0b7a8c' : '#2fb7c8') : (isLight ? '#5b6775' : '#8695a5');
    cctx.fillText(txt, CCX + Math.cos(rad) * 49.5, CCY + Math.sin(rad) * 49.5);
  }

  // Two-tone diamond needle inside the inner circle + rim bearing triangle
  const angle = (headingDeg != null && !isNaN(headingDeg)) ? headingDeg : lastCompassHeading;
  const rad = (angle - 90) * Math.PI / 180;
  const oppRad = rad + Math.PI;
  const perpRad = rad + Math.PI / 2;

  const wingR = 3.8;
  const wx1 = CCX + Math.cos(perpRad) * wingR;
  const wy1 = CCY + Math.sin(perpRad) * wingR;
  const wx2 = CCX - Math.cos(perpRad) * wingR;
  const wy2 = CCY - Math.sin(perpRad) * wingR;

  // Rear tail half of needle
  const bx = CCX + Math.cos(oppRad) * 13;
  const by = CCY + Math.sin(oppRad) * 13;
  cctx.beginPath();
  cctx.moveTo(wx1, wy1);
  cctx.lineTo(bx, by);
  cctx.lineTo(wx2, wy2);
  cctx.closePath();
  cctx.fillStyle = isLight ? '#71b0bb' : '#1e6c79';
  cctx.fill();

  // Forward pointer half of needle (stays within inner reference ring)
  const nx = CCX + Math.cos(rad) * 33;
  const ny = CCY + Math.sin(rad) * 33;
  cctx.beginPath();
  cctx.moveTo(wx1, wy1);
  cctx.lineTo(nx, ny);
  cctx.lineTo(wx2, wy2);
  cctx.closePath();
  cctx.fillStyle = isLight ? '#0b7a8c' : '#2fb7c8';
  cctx.fill();

  // Outer rim heading marker triangle (points outward toward the degree ring)
  const tipR = CR - 2;
  const baseR = CR - 11.5;
  const baseHalfW = 4.2;
  const baseCX = CCX + Math.cos(rad) * baseR;
  const baseCY = CCY + Math.sin(rad) * baseR;
  cctx.beginPath();
  cctx.moveTo(CCX + Math.cos(rad) * tipR, CCY + Math.sin(rad) * tipR);
  cctx.lineTo(baseCX + Math.cos(perpRad) * baseHalfW, baseCY + Math.sin(perpRad) * baseHalfW);
  cctx.lineTo(baseCX - Math.cos(perpRad) * baseHalfW, baseCY - Math.sin(perpRad) * baseHalfW);
  cctx.closePath();
  cctx.fillStyle = isLight ? '#0b7a8c' : '#2fb7c8';
  cctx.fill();

  // Hollow center pivot ring
  cctx.beginPath(); cctx.arc(CCX, CCY, 4.2, 0, Math.PI * 2);
  cctx.fillStyle = isLight ? '#ffffff' : '#121a24'; cctx.fill();
  cctx.strokeStyle = isLight ? '#2e3a46' : '#bcc8d4'; cctx.lineWidth = 1.6; cctx.stroke();
}

function pollCompass() {
  fetch('/api/compass').then(r => r.json()).then(d => {
    const heading = d.heading;
    document.getElementById('compassHeadingVal').textContent = (heading != null) ? fmt(heading, 1) : '--';
    drawCompass(heading);
  }).catch(() => {});
}
setInterval(pollCompass, 500);
drawCompass(null);
</script>

</div>
</body>
</html>
"""

# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main():
    channels.start()

    for ch in channels.channels:
        threading.Thread(target=process_incoming, args=(ch,), daemon=True).start()

    if PYGAME_AVAILABLE:
        threading.Thread(target=joystick_thread, daemon=True).start()
    else:
        log.warning("Pygame not installed – joystick disabled. Install with: pip install pygame")

    log.info("Starting Flask web server on http://0.0.0.0:5000")
    # threaded=True: the UI runs ~10 independent setInterval polling loops
    # (100ms-1500ms each) hitting this server concurrently. Without this,
    # Werkzeug's dev server handles one request at a time - if any single
    # request is slow (e.g. the tether hiccuping mid wifi-switch), every
    # other panel's polling queues up behind it and the whole UI looks
    # frozen even though nothing is actually deadlocked.
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False, threaded=True)

if __name__ == "__main__":
    main()
