#!/usr/bin/env python3
"""
Laptop Backend for ROV/AUV.
- TCP client that connects to Jetson (192.168.1.10:9000).
- Serves a web UI (Flask) with REST endpoints.
- Forwards commands, telemetry, and file transfers.
- Includes joystick control thread.
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
from pathlib import Path
from queue import Queue, Empty
from flask import Flask, request, jsonify, send_file, render_template_string

# Try to import pygame for joystick support
try:
    import pygame
    PYGAME_AVAILABLE = True
except ImportError:
    PYGAME_AVAILABLE = False

# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------
JETSON_IP = "192.168.1.100"          # <<< CHANGE TO YOUR JETSON IP
JETSON_PORT = 6000
FILE_CHUNK_SIZE = 65536
RECV_BUFFER = 65536
RECONNECT_DELAY = 2.0
DOWNLOAD_TIMEOUT = 60               # seconds

PKT_HEARTBEAT      = 0x01
PKT_HEARTBEAT_ACK  = 0x02
PKT_JSON           = 0x10
PKT_FILE_REQ       = 0x11
PKT_FILE_CHUNK     = 0x12
PKT_FILE_COMPLETE  = 0x13
MAGIC = b"ROVC"

# Joystick configuration (adjust to your controller)
AXIS_MAP = {
    'vx': 0,      # Left stick horizontal (axis 0)
    'vy': 1,      # Left stick vertical (axis 1)
    'vz': 4,      # Right stick vertical or trigger (axis 4)
    'vyaw': 3,    # Right stick horizontal (axis 3)
}
AXIS_SCALE = {
    'vx': 1.0,
    'vy': 1.0,
    'vz': 1.0,
    'vyaw': 1.0
}
DEADZONE = 0.1

# Button mapping (button index -> command string)
BUTTON_COMMANDS = {
    0: "arm",          # A button
    1: "disarm",       # B button
    2: "lights_on",    # X button
    3: "lights_off",   # Y button
    4: "start_recording", # Left bumper
    5: "stop_recording"   # Right bumper
}

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("LaptopBackend")

# ----------------------------------------------------------------------
# Packet helpers
# ----------------------------------------------------------------------
def pack_message(msg_type: int, payload: bytes) -> bytes:
    return MAGIC + struct.pack("!BI", msg_type, len(payload)) + payload

def pack_json(msg_type: int, obj: dict) -> bytes:
    data = json.dumps(obj).encode("utf-8")
    return pack_message(msg_type, data)

# ----------------------------------------------------------------------
# TCP Client
# ----------------------------------------------------------------------
class TcpClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.send_queue = Queue()
        self.recv_queue = Queue()
        self._stop = threading.Event()
        self._connected = False
        self._sock = None
        self._state_cb = None

    def set_state_callback(self, cb):
        self._state_cb = cb

    def start(self):
        threading.Thread(target=self._connect_loop, daemon=True).start()

    def stop(self):
        self._stop.set()
        if self._sock:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._sock.close()

    def _connect_loop(self):
        while not self._stop.is_set():
            try:
                self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._sock.settimeout(5)
                self._sock.connect((self.host, self.port))
                self._sock.settimeout(None)
                self._connected = True
                if self._state_cb:
                    self._state_cb(True)
                log.info(f"Connected to Jetson at {self.host}:{self.port}")
                send_t = threading.Thread(target=self._send_loop, daemon=True)
                recv_t = threading.Thread(target=self._recv_loop, daemon=True)
                send_t.start()
                recv_t.start()
                send_t.join()
                recv_t.join()
            except (ConnectionRefusedError, OSError) as e:
                log.debug(f"Connection failed: {e}")
            self._connected = False
            if self._state_cb:
                self._state_cb(False)
            time.sleep(RECONNECT_DELAY)

    def _send_loop(self):
        while not self._stop.is_set() and self._connected:
            try:
                data = self.send_queue.get(timeout=0.5)
                self._sock.sendall(data)
            except Empty:
                continue
            except (BrokenPipeError, ConnectionResetError, OSError):
                self._connected = False
                break

    def _recv_loop(self):
        buffer = bytearray()
        while not self._stop.is_set() and self._connected:
            try:
                chunk = self._sock.recv(RECV_BUFFER)
                if not chunk:
                    self._connected = False
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
                self._connected = False
                break

# ----------------------------------------------------------------------
# Shared state for Flask
# ----------------------------------------------------------------------
class SharedState:
    def __init__(self):
        self.latest_telemetry = {}
        self.latest_state = {}
        self.connected = False
        self.lock = threading.Lock()

shared = SharedState()

# ----------------------------------------------------------------------
# Flask App
# ----------------------------------------------------------------------
app = Flask(__name__)
tcp_client = TcpClient(JETSON_IP, JETSON_PORT)

# File transfer state
pending_upload_ack = None          # (filename, event)
download_state = None              # dict with file, expected, received, filename, temp_dir
download_file_path = None          # Path to the downloaded file
download_complete = False
download_error = None

# ----------------------------------------------------------------------
# Background thread to process incoming packets from Jetson
# ----------------------------------------------------------------------
def process_incoming():
    global pending_upload_ack, download_state, download_file_path, download_complete, download_error
    while True:
        try:
            msg_type, payload = tcp_client.recv_queue.get(timeout=0.5)
        except Empty:
            continue

        if msg_type == PKT_HEARTBEAT:
            tcp_client.send_queue.put(pack_message(PKT_HEARTBEAT_ACK, b""))
        elif msg_type == PKT_HEARTBEAT_ACK:
            pass
        elif msg_type == PKT_JSON:
            try:
                msg = json.loads(payload.decode())
                msg_type_name = msg.get("type")
                if msg_type_name == "telemetry":
                    with shared.lock:
                        shared.latest_telemetry = msg.get("data", {})
                elif msg_type_name == "state":
                    with shared.lock:
                        shared.latest_state = msg.get("data", {})
                elif msg_type_name == "file_ready":
                    if pending_upload_ack and pending_upload_ack[0] == msg.get("filename"):
                        pending_upload_ack[1].set()
                elif msg_type_name == "error":
                    log.error(f"Error from Jetson: {msg.get('message')}")
                    if download_state:
                        download_error = msg.get('message')
                else:
                    log.debug(f"Other JSON: {msg}")
            except json.JSONDecodeError:
                log.error("Malformed JSON")
        elif msg_type == PKT_FILE_REQ:
            # Jetson is responding to a download request (sending a file)
            try:
                req = json.loads(payload.decode())
                if req.get("direction") == "download":
                    filename = req["filename"]
                    size = req["size"]
                    log.info(f"Receiving file: {filename} ({size} bytes)")
                    # Create temporary directory and file
                    temp_dir = tempfile.mkdtemp()
                    download_file_path = Path(temp_dir) / filename
                    f = open(download_file_path, "wb")
                    download_state = {
                        "file": f,
                        "expected": size,
                        "received": 0,
                        "filename": filename,
                        "temp_dir": temp_dir
                    }
                    download_complete = False
                    download_error = None
            except Exception as e:
                log.error(f"File request error: {e}")
                download_error = str(e)
        elif msg_type == PKT_FILE_CHUNK:
            if download_state:
                try:
                    download_state["file"].write(payload)
                    download_state["received"] += len(payload)
                    log.debug(f"Chunk received: {download_state['received']}/{download_state['expected']}")
                except Exception as e:
                    log.error(f"Write chunk error: {e}")
                    download_error = str(e)
                    download_state = None
        elif msg_type == PKT_FILE_COMPLETE:
            if download_state:
                download_state["file"].close()
                log.info(f"Download complete: {download_state['filename']} ({download_state['received']} bytes)")
                download_complete = True
        else:
            log.warning(f"Unknown packet type: {msg_type:02x}")

# ----------------------------------------------------------------------
# Joystick Thread
# ----------------------------------------------------------------------
def joystick_thread():
    if not PYGAME_AVAILABLE:
        log.warning("Pygame not installed. Joystick support disabled.")
        return

    pygame.init()
    pygame.joystick.init()
    joystick_count = pygame.joystick.get_count()
    if joystick_count == 0:
        log.warning("No joystick/controller detected. Joystick control disabled.")
        return

    joystick = pygame.joystick.Joystick(0)
    joystick.init()
    log.info(f"Joystick connected: {joystick.get_name()}")

    # Track previous button states to detect presses
    prev_buttons = [False] * joystick.get_numbuttons()
    # Track previous axis values to avoid spamming (only send on change)
    prev_axes = [0.0] * joystick.get_numaxes()

    while True:
        pygame.event.pump()
        # now = time.time()


        data_to_send = {"type":'teleop',
                        "axes":[joystick.get_axis(i) for i in range(joystick.get_numaxes())],
                        "buttons":[joystick.get_button(i) for i in range(joystick.get_numbuttons())]
                        }
        # data_to_send = json.dumps(data_to_send)
        tcp_client.send_queue.put(pack_json(PKT_JSON, data_to_send))
        
        # Read axes
        # axes = {}
        # for name, idx in AXIS_MAP.items():
        #     if idx < joystick.get_numaxes():
        #         val = joystick.get_axis(idx)
        #         # Apply deadzone
        #         if abs(val) < DEADZONE:
        #             val = 0.0
        #         else:
        #             # Scale and clamp
        #             val = val * AXIS_SCALE.get(name, 1.0)
        #             val = max(-1.0, min(1.0, val))
        #         axes[name] = val
        #     else:
        #         axes[name] = 0.0

        # # Check if any axis changed significantly (avoid flooding)
        # send_teleop = False
        # for i, (name, val) in enumerate(axes.items()):
        #     if abs(val - prev_axes[i]) > 0.01:
        #         send_teleop = True
        #         prev_axes[i] = val
        #         break

        # # Send teleop command if axes changed
        # if send_teleop:
        #     teleop_msg = {
        #         "type": "teleop",
        #         "vx": axes.get('vx', 0.0),
        #         "vy": axes.get('vy', 0.0),
        #         "vz": axes.get('vz', 0.0),
        #         "vyaw": axes.get('vyaw', 0.0)
        #     }
        #     if shared.connected:
        #         tcp_client.send_queue.put(pack_json(PKT_JSON, teleop_msg))

        # # Handle buttons (only on press)
        # for i in range(joystick.get_numbuttons()):
        #     pressed = joystick.get_button(i)
        #     if pressed and not prev_buttons[i]:
        #         # Button press detected
        #         if i in BUTTON_COMMANDS:
        #             cmd = BUTTON_COMMANDS[i]
        #             log.info(f"Joystick button {i}: {cmd}")
        #             if shared.connected:
        #                 tcp_client.send_queue.put(pack_json(PKT_JSON, {
        #                     "type": "command",
        #                     "task": cmd
        #                 }))
        #         else:
        #             log.debug(f"Unmapped button {i} pressed")
        #     prev_buttons[i] = pressed

        # Short sleep to control loop rate
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

@app.route('/api/command', methods=['POST'])
def api_command():
    if not shared.connected:
        return jsonify({"error": "Not connected"}), 503
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON"}), 400
    tcp_client.send_queue.put(pack_json(PKT_JSON, data))
    return jsonify({"status": "sent"})

@app.route('/api/upload', methods=['POST'])
def api_upload():
    if not shared.connected:
        return jsonify({"error": "Not connected"}), 503
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    filename = Path(file.filename).name
    temp_dir = tempfile.mkdtemp()
    temp_path = Path(temp_dir) / filename
    file.save(temp_path)
    file_size = temp_path.stat().st_size

    # Send upload request
    tcp_client.send_queue.put(pack_json(PKT_FILE_REQ, {
        "filename": filename,
        "size": file_size,
        "direction": "upload"
    }))

    # Wait for file_ready ack
    global pending_upload_ack
    event = threading.Event()
    pending_upload_ack = (filename, event)
    if not event.wait(timeout=5.0):
        pending_upload_ack = None
        shutil.rmtree(temp_dir)
        return jsonify({"error": "Upload ready timeout"}), 504

    # Stream chunks
    with open(temp_path, "rb") as f:
        while True:
            chunk = f.read(FILE_CHUNK_SIZE)
            if not chunk:
                break
            tcp_client.send_queue.put(pack_message(PKT_FILE_CHUNK, chunk))
    tcp_client.send_queue.put(pack_json(PKT_FILE_COMPLETE, {"status": "ok"}))
    shutil.rmtree(temp_dir)
    return jsonify({"status": "upload started"})

@app.route('/api/download', methods=['GET'])
def api_download():
    if not shared.connected:
        return jsonify({"error": "Not connected"}), 503
    filename = request.args.get('filename')
    if not filename:
        return jsonify({"error": "Missing filename"}), 400

    # Request download from Jetson
    tcp_client.send_queue.put(pack_json(PKT_FILE_REQ, {
        "filename": filename,
        "direction": "download"
    }))

    # Wait for completion or error
    global download_complete, download_state, download_file_path, download_error
    start = time.time()
    while not download_complete and download_error is None and time.time() - start < DOWNLOAD_TIMEOUT:
        time.sleep(0.1)

    if download_error:
        return jsonify({"error": download_error}), 500
    if not download_complete:
        return jsonify({"error": "Download timeout"}), 504

    # Serve the file
    if download_file_path and download_file_path.exists():
        # Capture temp_dir for cleanup
        temp_dir = download_state.get("temp_dir") if download_state else None

        # Send file
        response = send_file(download_file_path, as_attachment=True, download_name=download_file_path.name)

        # Clean up after response is sent
        def cleanup():
            try:
                if temp_dir and os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    log.info(f"Cleaned up temp dir: {temp_dir}")
                # Reset global state
                global download_complete, download_state, download_file_path, download_error
                download_complete = False
                download_state = None
                download_file_path = None
                download_error = None
            except Exception as e:
                log.error(f"Cleanup error: {e}")

        response.call_on_close(cleanup)
        return response
    else:
        return jsonify({"error": "File not found"}), 404

# ----------------------------------------------------------------------
# HTML Template (updated with recording status)
# ----------------------------------------------------------------------
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ROV Dashboard</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: Arial, sans-serif; background: #1a1a2e; color: #eee; padding: 20px; }
        h1 { color: #7ec8e3; text-align: center; margin-bottom: 20px; }
        .container { max-width: 1000px; margin: 0 auto; display: grid; grid-template-columns: 2fr 1fr; gap: 20px; }
        .panel { background: #2a2a3f; border-radius: 10px; padding: 20px; }
        .panel h2 { color: #b0c7d9; border-bottom: 1px solid #3a3a55; padding-bottom: 8px; margin-bottom: 12px; font-size: 1.1rem; }
        .telemetry-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
        .item { background: #1e1e30; padding: 8px; border-radius: 6px; text-align: center; }
        .item .label { font-size: 0.75rem; color: #8a9baa; }
        .item .value { font-size: 1.3rem; font-weight: bold; color: #7ec8e3; }
        .gps { grid-column: span 2; }
        .controls { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 10px; }
        .controls button { background: #3a4a6a; border: none; color: white; padding: 6px 14px; border-radius: 4px; cursor: pointer; }
        .controls button:hover { background: #4a6a8a; }
        .mode-selector { display: flex; gap: 6px; margin: 8px 0; }
        .mode-selector button { background: #2a3a4a; }
        .mode-selector button.active { background: #4a8a6a; }
        .sliders label { display: flex; justify-content: space-between; margin: 4px 0; }
        .sliders input[type="range"] { width: 120px; }
        .file-mgr { display: flex; flex-direction: column; gap: 10px; }
        .file-mgr .row { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
        .file-mgr input[type="text"], .file-mgr input[type="file"] { background: #1e1e30; border: 1px solid #3a3a55; color: #eee; padding: 4px 8px; border-radius: 4px; flex: 1; min-width: 120px; }
        .file-mgr button { background: #3a6a7a; border: none; color: white; padding: 4px 14px; border-radius: 4px; cursor: pointer; }
        .file-mgr button:hover { background: #4a8a9a; }
        .status { margin-top: 8px; font-size: 0.9rem; }
        .progress-bar { width: 100%; height: 6px; background: #2a2a3f; border-radius: 3px; margin-top: 4px; overflow: hidden; }
        .progress-bar .fill { height: 100%; background: #7ec8e3; width: 0%; transition: width 0.3s; }
        #connStatus { color: #f0a0a0; }
        #connStatus.connected { color: #80d080; }
        @media (max-width: 700px) { .container { grid-template-columns: 1fr; } }
    </style>
</head>
<body>
    <h1>🤖 ROV Control Dashboard</h1>
    <div class="container">
        <div>
            <div class="panel">
                <h2>📡 Telemetry</h2>
                <div class="telemetry-grid" id="telemGrid">
                    <div class="item"><div class="label">Roll</div><div class="value" id="roll">--</div></div>
                    <div class="item"><div class="label">Pitch</div><div class="value" id="pitch">--</div></div>
                    <div class="item"><div class="label">Yaw</div><div class="value" id="yaw">--</div></div>
                    <div class="item"><div class="label">Depth</div><div class="value" id="depth">--</div></div>
                    <div class="item"><div class="label">Battery</div><div class="value" id="battery">--</div></div>
                    <div class="item"><div class="label">Temperature</div><div class="value" id="temperature">--</div></div>
                    <div class="item gps"><div class="label">GPS</div><div class="value" id="gps">Lat: --, Lon: --, Alt: --</div></div>
                </div>
                <div class="status">
                    <span>State: </span><span id="stateDisplay">Unknown</span>
                    <span style="margin-left:20px;">🔌 <span id="connStatus">Disconnected</span></span>
                </div>
            </div>
            <div class="panel" style="margin-top:20px;">
                <h2>🎮 Commands</h2>
                <div class="controls">
                    <button onclick="sendCmd('arm')">Arm</button>
                    <button onclick="sendCmd('disarm')">Disarm</button>
                    <button onclick="sendCmd('lights_on')">Emergency stop ON</button>
                    <button onclick="sendCmd('lights_off')">Emergency stop Off</button>
                    <button onclick="sendCmd('start_recording')">Start Rec</button>
                    <button onclick="sendCmd('stop_recording')">Stop Rec</button>
                </div>
                <div class="mode-selector">
                    <button id="modeManual" class="active" onclick="setMode('manual')">Manual</button>
                    <button id="modeAuto" onclick="setMode('auto')">Auto</button>
                    <button id="modeTeleop" onclick="setMode('teleop')">Teleop</button>
                </div>
                <div class="sliders">
                    <label>Vx <input type="range" id="vx" min="-1" max="1" step="0.01" value="0" /></label>
                    <label>Vy <input type="range" id="vy" min="-1" max="1" step="0.01" value="0" /></label>
                    <label>Vz <input type="range" id="vz" min="-1" max="1" step="0.01" value="0" /></label>
                    <label>Vyaw <input type="range" id="vyaw" min="-1" max="1" step="0.01" value="0" /></label>
                </div>
                <button onclick="sendTeleop()">Send Teleop</button>
                <button onclick="requestState()" style="margin-left:10px;">Request State</button>
            </div>
        </div>
        <div>
            <div class="panel">
                <h2>📁 File Manager</h2>
                <div class="file-mgr">
                    <div class="row">
                        <span>Upload:</span>
                        <input type="file" id="fileInput" />
                        <button onclick="uploadFile()">Upload</button>
                    </div>
                    <div class="row">
                        <span>Download:</span>
                        <input type="text" id="downloadFilename" placeholder="filename (e.g., test.txt)" />
                        <button onclick="downloadFile()">Download</button>
                    </div>
                    <div id="fileStatus" class="status">Ready</div>
                    <div class="progress-bar"><div class="fill" id="fileProgress"></div></div>
                </div>
            </div>
            <div class="panel" style="margin-top:20px;">
                <h2>📋 System</h2>
                <p>Mode: <span id="currentMode">manual</span></p>
                <p>Armed: <span id="armedStatus">false</span></p>
                <p>Recording: <span id="recordingStatus">Unknown</span></p>
                <p>Errors: <span id="errorList">None</span></p>
            </div>
        </div>
    </div>
    <script>
        // Poll telemetry and state
        function pollTelemetry() {
    fetch('/api/telemetry')
        .then(r => r.json())
        .then(data => {

            document.getElementById('depth').textContent =
                (data.depth ?? '--').toFixed ? data.depth.toFixed(2) : data.depth;

            document.getElementById('temperature').textContent =
                data.temperature ?? '--';

            document.getElementById('battery').textContent =
                (data.battery_voltage ?? '--') + " V";

            if(data.gps){
                document.getElementById('gps').textContent =
                    `Lat: ${data.gps.lat}, Lon: ${data.gps.lon}, Alt: ${data.gps.alt}`;
            }

            if(data.orientation){
                const q = data.orientation;

                // (Temporary)
                document.getElementById('roll').textContent = q.x.toFixed(3);
                document.getElementById('pitch').textContent = q.y.toFixed(3);
                document.getElementById('yaw').textContent = q.z.toFixed(3);
            }
        })
        .catch(console.error);
}

        function pollState() {
            fetch('/api/state')
                .then(r => r.json())
                .then(data => {
                    if (data) {
                        document.getElementById('currentMode').textContent = data.mode || 'unknown';
                        document.getElementById('armedStatus').textContent = data.armed ? 'true' : 'false';
                        document.getElementById('recordingStatus').textContent = data.recording ? '🔴 Yes' : '⚪ No';
                        const errs = data.errors || [];
                        document.getElementById('errorList').textContent = errs.length ? errs.join(', ') : 'None';
                        document.querySelectorAll('.mode-selector button').forEach(b => b.classList.remove('active'));
                        const map = {'manual':'modeManual','auto':'modeAuto','teleop':'modeTeleop'};
                        const btn = map[data.mode];
                        if (btn) document.getElementById(btn).classList.add('active');
                        document.getElementById('stateDisplay').textContent = data.mode + (data.armed ? ' (armed)' : ' (disarmed)');
                    }
                })
                .catch(e => console.error('Poll state error', e));
        }

        function pollConnection() {
            fetch('/api/connected')
                .then(r => r.json())
                .then(data => {
                    const el = document.getElementById('connStatus');
                    if (data.connected) {
                        el.textContent = 'Connected';
                        el.className = 'connected';
                    } else {
                        el.textContent = 'Disconnected';
                        el.className = '';
                    }
                })
                .catch(e => {});
        }

        setInterval(pollTelemetry, 200);
        setInterval(pollState, 500);
        setInterval(pollConnection, 2000);

        function sendCmd(task) {
            fetch('/api/command', {
                method: 'POST',
                headers: {'Content-Type':'application/json'},
                body: JSON.stringify({type:'command', task:task})
            }).then(r => r.json()).then(d => console.log(d)).catch(e => alert('Error: '+e));
        }

        function setMode(mode) {
            fetch('/api/command', {
                method: 'POST',
                headers: {'Content-Type':'application/json'},
                body: JSON.stringify({type:'set_mode', mode:mode})
            }).then(r => r.json()).then(d => console.log(d)).catch(e => alert('Error: '+e));
        }

        function sendTeleop() {
            const vx = parseFloat(document.getElementById('vx').value);
            const vy = parseFloat(document.getElementById('vy').value);
            const vz = parseFloat(document.getElementById('vz').value);
            const vyaw = parseFloat(document.getElementById('vyaw').value);
            fetch('/api/command', {
                method: 'POST',
                headers: {'Content-Type':'application/json'},
                body: JSON.stringify({type:'teleop', vx, vy, vz, vyaw})
            }).then(r => r.json()).then(d => console.log(d)).catch(e => alert('Error: '+e));
        }

        function requestState() {
            fetch('/api/command', {
                method: 'POST',
                headers: {'Content-Type':'application/json'},
                body: JSON.stringify({type:'request_state'})
            }).then(r => r.json()).then(d => console.log(d)).catch(e => alert('Error: '+e));
        }

        function uploadFile() {
            const input = document.getElementById('fileInput');
            if (!input.files || input.files.length === 0) {
                alert('Select a file');
                return;
            }
            const file = input.files[0];
            const formData = new FormData();
            formData.append('file', file);
            const progress = document.getElementById('fileProgress');
            const status = document.getElementById('fileStatus');
            status.textContent = 'Uploading...';
            progress.style.width = '0%';
            const xhr = new XMLHttpRequest();
            xhr.open('POST', '/api/upload', true);
            xhr.upload.onprogress = function(e) {
                if (e.lengthComputable) {
                    const pct = (e.loaded / e.total * 100).toFixed(1);
                    progress.style.width = pct + '%';
                    status.textContent = `Uploading... ${pct}%`;
                }
            };
            xhr.onload = function() {
                if (xhr.status === 200) {
                    status.textContent = 'Upload complete';
                    progress.style.width = '100%';
                } else {
                    status.textContent = 'Upload failed: ' + xhr.status;
                }
            };
            xhr.onerror = function() {
                status.textContent = 'Upload error';
            };
            xhr.send(formData);
        }

        function downloadFile() {
            const filename = document.getElementById('downloadFilename').value.trim();
            if (!filename) {
                alert('Enter a filename (e.g., test.txt)');
                return;
            }
            const status = document.getElementById('fileStatus');
            const progress = document.getElementById('fileProgress');
            status.textContent = 'Downloading...';
            progress.style.width = '0%';
            fetch('/api/download?filename=' + encodeURIComponent(filename))
                .then(response => {
                    if (!response.ok) {
                        return response.json().then(err => { throw new Error(err.error || 'Download failed'); });
                    }
                    const contentDisposition = response.headers.get('Content-Disposition');
                    let filename = 'downloaded_file';
                    if (contentDisposition) {
                        const match = contentDisposition.match(/filename="?(.+)"?/);
                        if (match) filename = match[1];
                    }
                    return response.blob().then(blob => ({ blob, filename }));
                })
                .then(({ blob, filename }) => {
                    const url = URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = filename;
                    document.body.appendChild(a);
                    a.click();
                    a.remove();
                    URL.revokeObjectURL(url);
                    status.textContent = 'Download complete';
                    progress.style.width = '100%';
                })
                .catch(err => {
                    status.textContent = 'Error: ' + err.message;
                    progress.style.width = '0%';
                });
        }
    </script>
</body>
</html>
"""

# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main():
    tcp_client.set_state_callback(lambda conn: setattr(shared, 'connected', conn))
    tcp_client.start()

    threading.Thread(target=process_incoming, daemon=True).start()

    # Start joystick thread if available
    if PYGAME_AVAILABLE:
        threading.Thread(target=joystick_thread, daemon=True).start()
    else:
        log.warning("Pygame not installed – joystick disabled. Install with: pip install pygame")

    log.info("Starting Flask web server on http://0.0.0.0:5000")
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)

if __name__ == "__main__":
    main()