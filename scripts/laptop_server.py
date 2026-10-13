#!/usr/bin/env python3
"""
Laptop client for ROV/AUV communication.

Connects to the Pi server, displays telemetry, sends commands,
performs file upload/download.

Start with: python3 laptop_client.py [pi_ip_address]  (default localhost)
"""
import socket
import threading
import json
import struct
import time
import logging
import os
import sys
import select
from queue import Queue, Empty
from pathlib import Path

# Configuration
HEARTBEAT_TIMEOUT = 5.0
RECV_BUFFER = 65536
FILE_CHUNK_SIZE = 65536

# Packet types (must match server)
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
log = logging.getLogger("Laptop")

# ----------------------------------------------------------------------
# Framing (identical to server)
# ----------------------------------------------------------------------
def pack_message(msg_type: int, payload: bytes) -> bytes:
    return MAGIC + struct.pack("!BI", msg_type, len(payload)) + payload

def pack_json(msg_type: int, obj: dict) -> bytes:
    data = json.dumps(obj).encode("utf-8")
    return pack_message(msg_type, data)

# ----------------------------------------------------------------------
# Connection with reconnect
# ----------------------------------------------------------------------
class LaptopConnection:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = None
        self.send_queue = Queue()
        self.recv_queue = Queue()
        self._stop = threading.Event()

    def connect(self):
        """Block until connected. Returns when socket is established."""
        while not self._stop.is_set():
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.settimeout(5)
                self.sock.connect((self.host, self.port))
                self.sock.settimeout(None)
                log.info(f"Connected to {self.host}:{self.port}")
                self._send_thread = threading.Thread(target=self._send_loop, daemon=True)
                self._recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
                self._send_thread.start()
                self._recv_thread.start()
                return
            except (ConnectionRefusedError, OSError) as e:
                log.warning(f"Connection failed: {e}. Retrying in 2s...")
                time.sleep(2)

    def stop(self):
        self._stop.set()
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.sock.close()
        if hasattr(self, '_send_thread'):
            self._send_thread.join(timeout=1)
        if hasattr(self, '_recv_thread'):
            self._recv_thread.join(timeout=1)

    def _send_loop(self):
        while not self._stop.is_set():
            try:
                data = self.send_queue.get(timeout=0.5)
                self.sock.sendall(data)
            except Empty:
                continue
            except (BrokenPipeError, ConnectionResetError, OSError):
                self._stop.set()
                break

    def _recv_loop(self):
        buffer = bytearray()
        while not self._stop.is_set():
            try:
                chunk = self.sock.recv(RECV_BUFFER)
                if not chunk:
                    log.info("Server closed connection")
                    self._stop.set()
                    break
                buffer.extend(chunk)
                while len(buffer) >= 9:
                    if buffer[:4] != MAGIC:
                        buffer.pop(0)
                        continue
                    msg_type = buffer[4]
                    payload_len = struct.unpack("!I", buffer[5:9])[0]
                    total = 9 + payload_len
                    if len(buffer) < total:
                        break
                    payload = bytes(buffer[9:total])
                    del buffer[:total]
                    self.recv_queue.put((msg_type, payload))
            except (ConnectionResetError, BrokenPipeError, OSError):
                self._stop.set()
                break

# ----------------------------------------------------------------------
# Heartbeat monitor
# ----------------------------------------------------------------------
class HeartbeatMonitor:
    def __init__(self, conn: LaptopConnection):
        self.conn = conn
        self.last_beat = time.monotonic()
        self._timeout_cb = None

    def set_timeout_cb(self, cb):
        self._timeout_cb = cb

    def feed(self):
        self.last_beat = time.monotonic()

    def check(self):
        if time.monotonic() - self.last_beat > HEARTBEAT_TIMEOUT:
            log.error("Heartbeat timeout")
            if self._timeout_cb:
                self._timeout_cb()

# ----------------------------------------------------------------------
# File transfer client-side
# ----------------------------------------------------------------------
class FileTransferClient:
    def __init__(self, conn: LaptopConnection):
        self.conn = conn
        self.base_dir = Path("./laptop_files")
        self.base_dir.mkdir(exist_ok=True)
        self._current_download = None

    def upload_file(self, local_path):
        path = Path(local_path)
        if not path.is_file():
            log.error(f"File not found: {local_path}")
            return
        size = path.stat().st_size
        filename = path.name
        log.info(f"Uploading {filename} ({size} bytes)")
        # Send request
        self.conn.send_queue.put(pack_json(PKT_FILE_REQ, {
            "filename": filename,
            "size": size,
            "direction": "upload"
        }))
        # Wait for ready (simple sleep, in production wait for a specific JSON ack)
        time.sleep(0.5)
        # Stream chunks
        with open(path, "rb") as f:
            while True:
                chunk = f.read(FILE_CHUNK_SIZE)
                if not chunk:
                    break
                name_bytes = filename.encode("utf-8")
                chunk_msg = struct.pack("!I", len(name_bytes)) + name_bytes + struct.pack("!I", len(chunk)) + chunk
                self.conn.send_queue.put(pack_message(PKT_FILE_CHUNK, chunk_msg))
        # Send complete
        self.conn.send_queue.put(pack_json(PKT_FILE_COMPLETE, {"status": "ok"}))
        log.info(f"Upload of {filename} complete")

    def request_download(self, remote_filename):
        log.info(f"Requesting download of {remote_filename}")
        self.conn.send_queue.put(pack_json(PKT_FILE_REQ, {
            "filename": remote_filename,
            "direction": "download"
        }))

    def process(self, msg_type, payload):
        if msg_type == PKT_FILE_REQ:
            # This is a download initiation from server (file about to be sent)
            try:
                req = json.loads(payload.decode())
                if req.get("direction") == "download":
                    filename = req["filename"]
                    size = req["size"]
                    dest_path = self.base_dir / filename
                    log.info(f"Receiving file {filename} ({size} bytes)")
                    f = open(dest_path, "wb")
                    self._current_download = {"file": f, "expected": size, "received": 0}
            except Exception as e:
                log.error(f"Download init error: {e}")
        elif msg_type == PKT_FILE_CHUNK:
            if self._current_download:
                try:
                    self._current_download["file"].write(payload)
                    self._current_download["received"] += len(payload)
                except OSError as e:
                    log.error(f"Write error: {e}")
                    self._current_download["file"].close()
                    self._current_download = None
        elif msg_type == PKT_FILE_COMPLETE:
            if self._current_download:
                self._current_download["file"].close()
                log.info(f"Download complete: {self._current_download['expected']} bytes")
                self._current_download = None

# ----------------------------------------------------------------------
# Main interactive loop
# ----------------------------------------------------------------------
def main():
    host = "192.168.1.100" #sys.argv[1] if len(sys.argv) > 1 else "localhost"
    conn = LaptopConnection(host, 9000)
    conn.connect()

    hb_monitor = HeartbeatMonitor(conn)
    hb_monitor.set_timeout_cb(conn.stop)
    ft_client = FileTransferClient(conn)

    # create a test file for upload demo
    os.makedirs("./laptop_files", exist_ok=True)
    with open("./laptop_files/laptop_test.txt", "w") as f:
        f.write("Hello from laptop!\n" * 500)

    stop_flag = False

    # Heartbeat monitoring thread
    def heartbeat_checker():
        while not stop_flag and not conn._stop.is_set():
            time.sleep(1)
            hb_monitor.check()
    threading.Thread(target=heartbeat_checker, daemon=True).start()

    log.info("Ready. Type commands: arm, disarm, lights_on/off, upload <file>, download <file>, quit")
    log.info("Local files for upload: laptop_files/laptop_test.txt")

    # Main loop
    while not conn._stop.is_set() and not stop_flag:
        # Process incoming packets
        try:
            msg_type, payload = conn.recv_queue.get(timeout=0.1)
        except Empty:
            pass
        else:
            if msg_type == PKT_HEARTBEAT:
                hb_monitor.feed()
                conn.send_queue.put(pack_message(PKT_HEARTBEAT_ACK, b""))
            elif msg_type == PKT_HEARTBEAT_ACK:
                hb_monitor.feed()
            elif msg_type == PKT_JSON:
                try:
                    msg = json.loads(payload.decode())
                    msg_type_name = msg.get("type", "unknown")
                    if msg_type_name == "telemetry":
                        data = msg["data"]
                        log.info(f"TELEM: roll={data['roll']:6.1f} pitch={data['pitch']:6.1f} "
                                 f"yaw={data['yaw']:6.1f} depth={data['depth']:5.1f} "
                                 f"bat={data['battery']:.2f}V")
                    elif msg_type_name == "command_ack":
                        log.info(f"Command ack: {msg.get('message')}")
                    elif msg_type_name == "file_ready":
                        log.info(f"Server ready for upload of {msg.get('filename')}")
                    elif msg_type_name == "error":
                        log.error(f"Server error: {msg.get('message')}")
                    else:
                        log.debug(f"JSON message: {msg}")
                except json.JSONDecodeError:
                    log.error("Malformed JSON packet")
            elif msg_type in (PKT_FILE_REQ, PKT_FILE_CHUNK, PKT_FILE_COMPLETE):
                ft_client.process(msg_type, payload)

        # Check for keyboard input (non-blocking)
        if sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
            line = sys.stdin.readline().strip()
            if not line:
                continue
            parts = line.split()
            cmd = parts[0].lower()

            if cmd == "arm":
                conn.send_queue.put(pack_json(PKT_JSON, {"type": "command", "task": "arm"}))
            elif cmd == "disarm":
                conn.send_queue.put(pack_json(PKT_JSON, {"type": "command", "task": "disarm"}))
            elif cmd == "lights_on":
                conn.send_queue.put(pack_json(PKT_JSON, {"type": "command", "task": "lights_on"}))
            elif cmd == "lights_off":
                conn.send_queue.put(pack_json(PKT_JSON, {"type": "command", "task": "lights_off"}))
            elif cmd == "start_recording":
                conn.send_queue.put(pack_json(PKT_JSON, {"type": "command", "task": "start_recording"}))
            elif cmd == "stop_recording":
                conn.send_queue.put(pack_json(PKT_JSON, {"type": "command", "task": "stop_recording"}))
            elif cmd == "upload" and len(parts) == 2:
                ft_client.upload_file(parts[1])
            elif cmd == "download" and len(parts) == 2:
                ft_client.request_download(parts[1])
            elif cmd == "quit":
                stop_flag = True
            else:
                print("Valid commands: arm, disarm, lights_on/off, start_recording, stop_recording,"
                      " upload <local_path>, download <remote_filename>, quit")

    conn.stop()
    log.info("Client terminated")

if __name__ == "__main__":
    main()