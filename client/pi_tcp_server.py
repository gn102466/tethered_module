#!/usr/bin/env python3
"""
Raspberry Pi server for tethered ROV/AUV communication.

Supports:
  - Bidirectional JSON messages (telemetry, commands, state)
  - Binary file upload / download (any type, streaming, no base64)
  - Watchdog with automatic reconnect
  - Real‑time control (teleop, state requests, config sync)
  - Version‑controlled config & behaviour tree sync

Run with: python3 pi_server.py
"""
import socket
import threading
import json
import struct
import time
import logging
import random
import os
from pathlib import Path
from queue import Queue, Empty

# ----------------------------------------------------------------------
# Constants (must match the laptop node)
# ----------------------------------------------------------------------
HOST = "0.0.0.0"
PORT = 9000
HEARTBEAT_INTERVAL = 1.0
FILE_CHUNK_SIZE = 65536          # 64 KB
RECV_BUFFER = 65536

PKT_HEARTBEAT      = 0x01
PKT_HEARTBEAT_ACK  = 0x02
PKT_JSON           = 0x10
PKT_FILE_REQ       = 0x11
PKT_FILE_CHUNK     = 0x12
PKT_FILE_COMPLETE  = 0x13

MAGIC = b"ROVC"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("PiServer")

# ----------------------------------------------------------------------
# Protocol helpers
# ----------------------------------------------------------------------
def pack_message(msg_type: int, payload: bytes) -> bytes:
    return MAGIC + struct.pack("!BI", msg_type, len(payload)) + payload

def pack_json(msg_type: int, obj: dict) -> bytes:
    data = json.dumps(obj).encode("utf-8")
    return pack_message(msg_type, data)

# ----------------------------------------------------------------------
# Connection manager (one client at a time)
# ----------------------------------------------------------------------
class Connection:
    def __init__(self, sock, addr):
        self.sock = sock
        self.addr = addr
        self.send_queue = Queue()
        self.recv_queue = Queue()
        self._stop = threading.Event()
        self._send_thread = None
        self._recv_thread = None

    def start(self):
        self._send_thread = threading.Thread(target=self._send_loop, daemon=True)
        self._recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        self._send_thread.start()
        self._recv_thread.start()
        log.info(f"Connected to {self.addr}")

    def stop(self):
        self._stop.set()
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()
        if self._send_thread:
            self._send_thread.join(timeout=2)
        if self._recv_thread:
            self._recv_thread.join(timeout=2)

    def _send_loop(self):
        while not self._stop.is_set():
            try:
                data = self.send_queue.get(timeout=0.5)
                self.sock.sendall(data)
            except Empty:
                continue
            except (BrokenPipeError, ConnectionResetError, OSError) as e:
                log.error(f"Send error: {e}")
                self._stop.set()
                break

    def _recv_loop(self):
        buffer = bytearray()
        while not self._stop.is_set():
            try:
                chunk = self.sock.recv(RECV_BUFFER)
                if not chunk:
                    log.info("Client disconnected")
                    self._stop.set()
                    break
                buffer.extend(chunk)
                while len(buffer) >= 9:   # MAGIC + TYPE + LEN
                    if buffer[:4] != MAGIC:
                        log.error("Invalid magic, discarding byte")
                        buffer.pop(0)
                        continue
                    msg_type = buffer[4]
                    payload_len = struct.unpack("!I", buffer[5:9])[0]
                    total_len = 9 + payload_len
                    if len(buffer) < total_len:
                        break
                    payload = bytes(buffer[9:total_len])
                    del buffer[:total_len]
                    self.recv_queue.put((msg_type, payload))
            except (ConnectionResetError, BrokenPipeError, OSError) as e:
                log.error(f"Receive error: {e}")
                self._stop.set()
                break

# ----------------------------------------------------------------------
# Telemetry and state simulation
# ----------------------------------------------------------------------
def generate_telemetry():
    return {
        "roll": round(random.uniform(-30, 30), 2),
        "pitch": round(random.uniform(-30, 30), 2),
        "yaw": round(random.uniform(0, 360), 2),
        "depth": round(random.uniform(0, 100), 2),
        "battery": round(random.uniform(11.0, 12.6), 2),
        "temperature": round(random.uniform(10, 40), 1),
        "gps": {
            "lat": round(random.uniform(-90, 90), 6),
            "lon": round(random.uniform(-180, 180), 6),
            "alt": round(random.uniform(-50, 500), 2)
        }
    }

class SystemState:
    def __init__(self):
        self.mode = "manual"    # manual, auto, teleop
        self.armed = False
        self.errors = []

    def to_dict(self):
        return {
            "type": "state",
            "data": {
                "mode": self.mode,
                "armed": self.armed,
                "errors": self.errors
            }
        }

# ----------------------------------------------------------------------
# File transfer handler
# ----------------------------------------------------------------------
class FileTransfer:
    def __init__(self, conn: Connection):
        self.conn = conn
        self.base_dir = Path("./pi_files")
        self.base_dir.mkdir(exist_ok=True)
        self._current_upload = None

    def request_download(self, filename: str):
        """Initiate a file download to the laptop (send full file)."""
        safe_name = Path(filename).name
        file_path = self.base_dir / safe_name
        if not file_path.exists():
            self.conn.send_queue.put(pack_json(PKT_JSON, {"type": "error", "message": "File not found"}))
            return
        file_size = file_path.stat().st_size
        log.info(f"Sending file: {safe_name} ({file_size} bytes)")
        self.conn.send_queue.put(pack_json(PKT_FILE_REQ, {
            "filename": safe_name,
            "size": file_size,
            "direction": "download"
        }))
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(FILE_CHUNK_SIZE)
                if not chunk:
                    break
                # name_bytes = safe_name.encode()
                # chunk_msg = struct.pack("!I", len(name_bytes)) + name_bytes + struct.pack("!I", len(chunk)) + chunk
                # self.conn.send_queue.put(pack_message(PKT_FILE_CHUNK, chunk_msg))
                self.conn.send_queue.put(pack_message(PKT_FILE_CHUNK, chunk))

                time.sleep(0)
        self.conn.send_queue.put(pack_json(PKT_FILE_COMPLETE, {"status": "ok"}))
        log.info(f"File {safe_name} sent")

    def process(self, msg_type, payload):
        if msg_type == PKT_FILE_REQ:
            self._handle_request(payload)
        elif msg_type == PKT_FILE_CHUNK:
            self._handle_chunk(payload)
        elif msg_type == PKT_FILE_COMPLETE:
            self._handle_complete(payload)

    def _handle_request(self, payload):
        try:
            req = json.loads(payload.decode())
            filename = Path(req["filename"]).name
            direction = req.get("direction", "upload")
            if direction == "upload":
                size = req["size"]
                dest_path = self.base_dir / filename
                log.info(f"Receiving upload: {filename} ({size} bytes)")
                try:
                    f = open(dest_path, "wb")
                except OSError as e:
                    self.conn.send_queue.put(pack_json(PKT_JSON, {"type": "error", "message": str(e)}))
                    return
                self._current_upload = {"file": f, "expected_size": size, "received": 0}
                # Send readiness ack
                self.conn.send_queue.put(pack_json(PKT_JSON, {"type": "file_ready", "filename": filename}))
            elif direction == "download":
                self.request_download(filename)
        except Exception as e:
            log.error(f"File request error: {e}")
            self.conn.send_queue.put(pack_json(PKT_JSON, {"type": "error", "message": str(e)}))

    def _handle_chunk(self, payload):
        if not self._current_upload:
            return
        try:
            self._current_upload["file"].write(payload)
            self._current_upload["received"] += len(payload)
        except OSError as e:
            log.error(f"Write error: {e}")
            self._abort_upload()

    def _handle_complete(self, payload):
        if self._current_upload:
            self._current_upload["file"].close()
            log.info(f"Upload complete: {self._current_upload['expected_size']} bytes")
            self._current_upload = None

    def _abort_upload(self):
        if self._current_upload:
            self._current_upload["file"].close()
            self._current_upload = None
        self.conn.send_queue.put(pack_json(PKT_JSON, {"type": "error", "message": "Upload aborted"}))

# ----------------------------------------------------------------------
# Command handler
# ----------------------------------------------------------------------
class CommandHandler:
    def __init__(self, conn: Connection, state: SystemState, ft: FileTransfer):
        self.conn = conn
        self.state = state
        self.ft = ft

    def process(self, cmd: dict):
        cmd_type = cmd.get("type", "command")
        if cmd_type == "command":
            task = cmd.get("task")
            self._handle_command(task)
        elif cmd_type == "teleop":
            vx = cmd.get("vx", 0.0)
            vy = cmd.get("vy", 0.0)
            vz = cmd.get("vz", 0.0)
            vyaw = cmd.get("vyaw", 0.0)
            log.info(f"Teleop: vx={vx}, vy={vy}, vz={vz}, vyaw={vyaw}")
            self.conn.send_queue.put(pack_json(PKT_JSON, {"type": "teleop_ack", "success": True}))
        elif cmd_type == "set_mode":
            mode = cmd.get("mode", "manual")
            if mode in ("manual", "auto", "teleop"):
                self.state.mode = mode
                log.info(f"Mode changed to {mode}")
                self.conn.send_queue.put(pack_json(PKT_JSON, {"type": "mode_ack", "success": True, "mode": mode}))
            else:
                self.conn.send_queue.put(pack_json(PKT_JSON, {"type": "error", "message": "Invalid mode"}))
        elif cmd_type == "request_state":
            self.conn.send_queue.put(pack_json(PKT_JSON, self.state.to_dict()))
        elif cmd_type == "sync_config":
            filename = cmd.get("filename", "config.yaml")
            log.info(f"Sync config requested: {filename}")
            self.ft.request_download(filename)
        elif cmd_type == "sync_behavior_tree":
            filename = cmd.get("filename", "bt.xml")
            log.info(f"Sync behavior tree requested: {filename}")
            self.ft.request_download(filename)

    def _handle_command(self, task):
        valid = {"arm", "disarm", "lights_on", "lights_off", "start_recording", "stop_recording"}
        if task not in valid:
            self.conn.send_queue.put(pack_json(PKT_JSON, {"type": "command_ack", "success": False, "message": f"Unknown command: {task}"}))
            return
        log.info(f"Executing command: {task}")
        if task == "arm":
            self.state.armed = True
        elif task == "disarm":
            self.state.armed = False
        self.conn.send_queue.put(pack_json(PKT_JSON, {"type": "command_ack", "success": True, "message": f"Command {task} executed"}))

# ----------------------------------------------------------------------
# Main server loop
# ----------------------------------------------------------------------
def main():
    # Prepare directories and sample files
    os.makedirs("./pi_files", exist_ok=True)
    with open("./pi_files/test.txt", "w") as f:
        f.write("Hello from Raspberry Pi!\n" * 1000)
    with open("./pi_files/config.yaml", "w") as f:
        f.write("mode: manual\npid: {p: 1.0, i: 0.1, d: 0.05}\n")
    with open("./pi_files/bt.xml", "w") as f:
        f.write("<root><sequence><action name='arm'/></sequence></root>")

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((HOST, PORT))
    server_sock.listen(1)
    log.info(f"Pi server listening on {HOST}:{PORT}")

    while True:
        client_sock, addr = server_sock.accept()
        conn = Connection(client_sock, addr)
        conn.start()

        state = SystemState()
        ft = FileTransfer(conn)
        cmd_handler = CommandHandler(conn, state, ft)
        stop_flag = threading.Event()

        # Background threads
        def heartbeat_sender():
            while not stop_flag.is_set():
                conn.send_queue.put(pack_message(PKT_HEARTBEAT, b""))
                time.sleep(HEARTBEAT_INTERVAL)
        threading.Thread(target=heartbeat_sender, daemon=True).start()

        def telemetry_sender():
            while not stop_flag.is_set():
                telem = generate_telemetry()
                conn.send_queue.put(pack_json(PKT_JSON, {"type": "telemetry", "data": telem}))
                time.sleep(0.01)
        threading.Thread(target=telemetry_sender, daemon=True).start()

        def state_sender():
            while not stop_flag.is_set():
                conn.send_queue.put(pack_json(PKT_JSON, state.to_dict()))
                time.sleep(1.0)
        threading.Thread(target=state_sender, daemon=True).start()

        # Main incoming packet loop
        try:
            while True:
                try:
                    msg_type, payload = conn.recv_queue.get(timeout=0.5)
                except Empty:
                    continue

                if msg_type == PKT_HEARTBEAT:
                    conn.send_queue.put(pack_message(PKT_HEARTBEAT_ACK, b""))
                elif msg_type == PKT_HEARTBEAT_ACK:
                    pass   # No action needed, just acknowledge receipt silently
                elif msg_type == PKT_JSON:
                    try:
                        msg = json.loads(payload.decode())
                        if msg.get("type") in ("command", "teleop", "set_mode", "request_state",
                                               "sync_config", "sync_behavior_tree"):
                            cmd_handler.process(msg)
                    except json.JSONDecodeError:
                        log.error("Malformed JSON")
                elif msg_type in (PKT_FILE_REQ, PKT_FILE_CHUNK, PKT_FILE_COMPLETE):
                    ft.process(msg_type, payload)
                else:
                    log.warning(f"Unknown packet type: {msg_type:02x}")
        except (ConnectionError, Exception) as e:
            log.error(f"Connection lost: {e}")
        finally:
            stop_flag.set()
            conn.stop()
            log.info("Client disconnected, waiting for next connection...")

if __name__ == "__main__":
    main()