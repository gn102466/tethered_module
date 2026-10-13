#!/usr/bin/env python3
"""
IMU / Magnetometer Dashboard
-----------------------------
Standalone ROS2 -> Flask dashboard for reviewing a played-back rosbag.

- Subscribes directly to `/imu/mag` (geometry_msgs/Vector3Stamped, raw
  magnetometer x/y/z) and `/imu` (sensor_msgs/Imu, used only to show the
  IMU-reported yaw alongside the magnetometer-derived heading for
  comparison).
- Serves a self-contained web dashboard: live strip charts of the
  magnetometer x/y/z components, and a compass visual that computes
  heading from the raw magnetometer vector (with adjustable axis
  mapping / declination offset, since raw heading depends on how the
  sensor is mounted).

Run (after sourcing the workspace and starting `ros2 bag play ...`):
    source install/setup.bash
    python3 src/tethered_module/client/imu_mag_dashboard.py
Then open:
    http://<this-machine>:5002
"""

import math
import threading
import time
from collections import deque

import rclpy
from rclpy.node import Node
from geometry_msgs.msg import Vector3Stamped
from sensor_msgs.msg import Imu
from flask import Flask, jsonify, render_template_string

# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------
MAG_TOPIC = "/imu/mag"
IMU_TOPIC = "/imu"
HISTORY_LEN = 600       # points kept for the strip charts / heading trail
WEB_PORT = 5002

# ----------------------------------------------------------------------
# Shared state
# ----------------------------------------------------------------------
class SharedState:
    def __init__(self):
        self.connected = False
        self.latest_mag = {}
        self.latest_imu_yaw = None
        self.mag_history = deque(maxlen=HISTORY_LEN)
        self._msg_count = 0
        self._window_start = time.time()
        self.msg_rate = 0.0
        self.last_msg_time = None
        self.lock = threading.Lock()

shared = SharedState()

# ----------------------------------------------------------------------
# ROS2 node
# ----------------------------------------------------------------------
def quaternion_to_yaw(qx, qy, qz, qw):
    return math.atan2(2.0 * (qw * qz + qx * qy), 1.0 - 2.0 * (qy * qy + qz * qz))


class ImuMagListener(Node):
    def __init__(self):
        super().__init__('imu_mag_dashboard')
        self.create_subscription(Vector3Stamped, MAG_TOPIC, self.mag_callback, 10)
        self.create_subscription(Imu, IMU_TOPIC, self.imu_callback, 10)
        self.get_logger().info(f"Listening for magnetometer data on '{MAG_TOPIC}' "
                                f"and IMU orientation on '{IMU_TOPIC}'.")

    def mag_callback(self, msg: Vector3Stamped):
        now = time.time()
        x, y, z = msg.vector.x, msg.vector.y, msg.vector.z

        with shared.lock:
            shared.connected = True
            shared._msg_count += 1
            shared.last_msg_time = now
            elapsed = now - shared._window_start
            if elapsed >= 1.0:
                shared.msg_rate = shared._msg_count / elapsed
                shared._msg_count = 0
                shared._window_start = now

            shared.latest_mag = {"x": x, "y": y, "z": z}
            shared.mag_history.append({"ts": now, "x": x, "y": y, "z": z})

    def imu_callback(self, msg: Imu):
        yaw = quaternion_to_yaw(msg.orientation.x, msg.orientation.y,
                                 msg.orientation.z, msg.orientation.w)
        with shared.lock:
            shared.latest_imu_yaw = math.degrees(yaw) % 360.0


def ros_spin_thread():
    rclpy.init()
    node = ImuMagListener()
    try:
        rclpy.spin(node)
    finally:
        node.destroy_node()
        rclpy.shutdown()

# ----------------------------------------------------------------------
# Flask app
# ----------------------------------------------------------------------
app = Flask(__name__)

@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route("/api/imu_mag")
def api_imu_mag():
    with shared.lock:
        return jsonify({
            "connected": shared.connected,
            "msg_rate": round(shared.msg_rate, 1),
            "last_msg_time": shared.last_msg_time,
            "latest_mag": shared.latest_mag,
            "latest_imu_yaw": shared.latest_imu_yaw,
            "mag_history": list(shared.mag_history),
        })

# ----------------------------------------------------------------------
# HTML / CSS / JS
# ----------------------------------------------------------------------
HTML_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>IMU / Mag Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<style>
  :root{
    --bg-abyss:#050f16;
    --panel:#0a1e2a;
    --panel-raised:#0d2432;
    --grid:#143242;
    --hairline:#1c3b4a;
    --text-hi:#d9f2ec;
    --text-dim:#6e8a94;
    --accent:#4df0c4;
    --accent-dim:#2c8a72;
    --amber:#ffb454;
    --violet:#c792ea;
    --alert:#ff5c5c;
  }
  *{box-sizing:border-box;margin:0;padding:0;}
  body{
    background:
      radial-gradient(circle at 15% -10%, #0c2432 0%, transparent 55%),
      var(--bg-abyss);
    color:var(--text-hi);
    font-family:'JetBrains Mono', monospace;
    padding:18px;
    min-height:100vh;
  }
  .topbar{
    display:flex; align-items:baseline; justify-content:space-between;
    flex-wrap:wrap; gap:10px;
    border-bottom:1px solid var(--hairline);
    padding-bottom:14px; margin-bottom:16px;
  }
  .topbar h1{
    font-family:'Space Grotesk', sans-serif;
    font-weight:700; font-size:1.5rem; letter-spacing:0.02em;
    color:var(--text-hi);
  }
  .topbar h1 span{ color:var(--accent); }
  .topbar .sub{ color:var(--text-dim); font-size:0.75rem; letter-spacing:0.08em; text-transform:uppercase; margin-top:2px;}
  .status-cluster{ display:flex; align-items:center; gap:18px; font-size:0.8rem; }
  .status-cluster .stat{ text-align:right; }
  .status-cluster .stat .lbl{ color:var(--text-dim); font-size:0.65rem; letter-spacing:0.1em; text-transform:uppercase; }
  .status-cluster .stat .val{ font-size:0.95rem; font-weight:600; }
  .dot{ width:9px; height:9px; border-radius:50%; display:inline-block; margin-right:6px; background:var(--alert); box-shadow:0 0 8px var(--alert); }
  .dot.live{ background:var(--accent); box-shadow:0 0 8px var(--accent); }

  .layout{
    display:grid;
    grid-template-columns: minmax(360px, 1fr) 380px;
    gap:16px;
  }
  @media (max-width:900px){ .layout{ grid-template-columns:1fr; } }

  .panel{
    background:var(--panel);
    border:1px solid var(--hairline);
    border-radius:4px;
    padding:14px 16px;
  }
  .panel h2{
    font-family:'Space Grotesk', sans-serif;
    font-size:0.7rem; letter-spacing:0.14em; text-transform:uppercase;
    color:var(--text-dim); font-weight:600;
    margin-bottom:10px;
    display:flex; align-items:center; gap:8px;
  }
  .panel h2::before{ content:''; width:14px; height:1px; background:var(--accent-dim); }

  /* Fixed-height wrapper is required: Chart.js measures its parent to
     stay responsive, and a canvas sized only by CSS (with no fixed-size
     ancestor) creates a resize feedback loop that shrinks the chart a
     little more on every redraw. */
  .chart-wrap{ position:relative; height:140px; width:100%; }
  .chart-wrap canvas{ width:100%; height:100%; }

  .readouts{ display:grid; grid-template-columns:1fr 1fr 1fr; gap:8px; margin-top:12px; }
  .cell{ background:var(--panel-raised); border:1px solid var(--hairline); border-radius:3px; padding:8px 10px; }
  .cell .lbl{ color:var(--text-dim); font-size:0.62rem; letter-spacing:0.08em; text-transform:uppercase; }
  .cell .val{ font-size:1.05rem; font-weight:600; color:var(--text-hi); margin-top:2px; }
  .cell .unit{ color:var(--text-dim); font-size:0.68rem; margin-left:3px; }

  /* --- Compass --- */
  .compass-wrap{ position:relative; margin-top:2px; }
  canvas#compass{ width:100%; aspect-ratio:1/1; display:block; border-radius:3px; background:#040c12; }
  .heading-hero{ text-align:center; margin-top:10px; }
  .heading-hero .val{ font-size:2.2rem; font-weight:700; color:var(--accent); font-family:'Space Grotesk', sans-serif; }
  .heading-hero .lbl{ color:var(--text-dim); font-size:0.65rem; letter-spacing:0.1em; text-transform:uppercase; }

  .controls{ margin-top:14px; display:flex; flex-direction:column; gap:10px; }
  .ctrl-row{ display:flex; align-items:center; justify-content:space-between; gap:10px; font-size:0.72rem; color:var(--text-dim); }
  .ctrl-row select, .ctrl-row input[type=number]{
    background:var(--panel-raised); color:var(--text-hi); border:1px solid var(--hairline);
    border-radius:3px; padding:4px 6px; font-family:'JetBrains Mono', monospace; font-size:0.72rem; width:110px;
  }
  .ctrl-row label{ display:flex; align-items:center; gap:6px; }
  .ctrl-row input[type=checkbox]{ accent-color: var(--accent); }

  .footer-note{ margin-top:16px; color:var(--text-dim); font-size:0.68rem; text-align:center; letter-spacing:0.04em; }
</style>
</head>
<body>

  <div class="topbar">
    <div>
      <h1>IMU / MAG <span>DASHBOARD</span></h1>
      <div class="sub">/imu/mag &middot; Vector3Stamped &middot; rosbag playback</div>
    </div>
    <div class="status-cluster">
      <div class="stat"><span class="lbl">Link</span><br><span class="val"><span class="dot" id="connDot"></span><span id="connText">searching&hellip;</span></span></div>
      <div class="stat"><span class="lbl">Msg Rate</span><br><span class="val" id="msgRate">&mdash;</span></div>
      <div class="stat"><span class="lbl">Age</span><br><span class="val" id="msgAge">&mdash;</span></div>
    </div>
  </div>

  <div class="layout">

    <div>
      <div class="panel">
        <h2>Magnetometer X (&mu;T)</h2>
        <div class="chart-wrap"><canvas id="magChartX"></canvas></div>
        <div class="readouts" style="grid-template-columns:1fr;">
          <div class="cell"><div class="lbl">Mag X</div><div class="val" id="magX">&mdash;</div></div>
        </div>
      </div>

      <div class="panel" style="margin-top:16px;">
        <h2>Magnetometer Y (&mu;T)</h2>
        <div class="chart-wrap"><canvas id="magChartY"></canvas></div>
        <div class="readouts" style="grid-template-columns:1fr;">
          <div class="cell"><div class="lbl">Mag Y</div><div class="val" id="magY">&mdash;</div></div>
        </div>
      </div>

      <div class="panel" style="margin-top:16px;">
        <h2>Magnetometer Z (&mu;T)</h2>
        <div class="chart-wrap"><canvas id="magChartZ"></canvas></div>
        <div class="readouts" style="grid-template-columns:1fr;">
          <div class="cell"><div class="lbl">Mag Z</div><div class="val" id="magZ">&mdash;</div></div>
        </div>
      </div>
    </div>

    <div>
      <div class="panel">
        <h2>Heading (from magnetometer)</h2>
        <div class="compass-wrap">
          <canvas id="compass" width="600" height="600"></canvas>
        </div>
        <div class="heading-hero">
          <div class="val" id="headingVal">&mdash;&deg;</div>
          <div class="lbl" id="headingCompass">&mdash;</div>
        </div>
        <div class="controls">
          <div class="ctrl-row">
            <label for="axisMap">Forward axis</label>
            <select id="axisMap">
              <option value="xy">x=fwd, y=left (default)</option>
              <option value="yx">y=fwd, x=left</option>
              <option value="xy_inv">x=fwd, -y=left</option>
              <option value="yx_inv">y=fwd, -x=left</option>
            </select>
          </div>
          <div class="ctrl-row">
            <label for="declination">Declination offset</label>
            <input type="number" id="declination" value="0" step="0.5">
          </div>
          <div class="ctrl-row">
            <span>IMU orientation yaw (reference)</span>
            <span id="imuYaw" style="color:var(--text-hi);">&mdash;</span>
          </div>
        </div>
      </div>
    </div>

  </div>

  <div class="footer-note">
    polling /api/imu_mag every 200ms &middot; heading = atan2 of the selected axis pair, uncalibrated (no hard/soft-iron correction) &middot;
    adjust "forward axis" / declination to match your sensor's mounting orientation
  </div>

<script>
const fmt = (v, d=3) => (v === null || v === undefined || Number.isNaN(v)) ? '&mdash;' : Number(v).toFixed(d);

// ---------------- Per-axis magnetometer strip charts ----------------
function makeAxisChart(canvasId, color) {
  const ctx = document.getElementById(canvasId).getContext('2d');
  return new Chart(ctx, {
    type: 'line',
    data: {
      labels: [],
      datasets: [
        { label: canvasId, data: [], borderColor: color, backgroundColor: 'transparent',
          borderWidth: 1.5, pointRadius: 0, tension: 0.25 },
      ]
    },
    options: {
      animation: false, responsive: true, maintainAspectRatio: false,
      interaction: { intersect: false },
      plugins: { legend: { display: false }, tooltip: { enabled: false } },
      scales: {
        x: { display: false },
        y: { grid: { color: '#143242' }, ticks: { color: '#6e8a94', font: { family: 'JetBrains Mono', size: 9 } } }
      }
    }
  });
}

const magChartX = makeAxisChart('magChartX', '#4df0c4');
const magChartY = makeAxisChart('magChartY', '#ffb454');
const magChartZ = makeAxisChart('magChartZ', '#c792ea');

// ---------------- Compass ----------------
const compassCanvas = document.getElementById('compass');
const compassCtx = compassCanvas.getContext('2d');

function resizeCompass() {
  const size = compassCanvas.clientWidth;
  compassCanvas.width = size * devicePixelRatio;
  compassCanvas.height = size * devicePixelRatio;
}
window.addEventListener('resize', resizeCompass);

const COMPASS_DIRS = ['N','NE','E','SE','S','SW','W','NW'];
function compassLabel(deg) {
  const idx = Math.round(deg / 45) % 8;
  return COMPASS_DIRS[idx];
}

function computeHeadingDeg(x, y) {
  const mode = document.getElementById('axisMap').value;
  const decl = parseFloat(document.getElementById('declination').value) || 0;
  let fwd, left;
  if (mode === 'xy')      { fwd = x;  left = y;  }
  else if (mode === 'yx') { fwd = y;  left = x;  }
  else if (mode === 'xy_inv') { fwd = x;  left = -y; }
  else /* yx_inv */       { fwd = y;  left = -x; }
  // heading measured clockwise from "forward" axis, left-hand positive => atan2(left, fwd)
  let heading = Math.atan2(left, fwd) * 180 / Math.PI;
  heading = (heading + decl + 360) % 360;
  return heading;
}

function drawCompass(headingDeg, hasData) {
  const W = compassCanvas.width, H = compassCanvas.height;
  const cx = W / 2, cy = H / 2;
  const R = Math.min(W, H) * 0.42;
  compassCtx.clearRect(0, 0, W, H);

  if (!hasData) {
    compassCtx.fillStyle = '#1c3b4a';
    compassCtx.font = `${14 * devicePixelRatio}px 'JetBrains Mono'`;
    compassCtx.textAlign = 'center';
    compassCtx.fillText('NO MAG DATA', cx, cy);
    return;
  }

  // outer ring + ticks
  compassCtx.strokeStyle = '#143242';
  compassCtx.lineWidth = 1.5 * devicePixelRatio;
  compassCtx.beginPath();
  compassCtx.arc(cx, cy, R, 0, Math.PI * 2);
  compassCtx.stroke();

  for (let deg = 0; deg < 360; deg += 15) {
    const rad = (deg - 90) * Math.PI / 180;
    const major = deg % 90 === 0;
    const r0 = R * (major ? 0.86 : 0.93);
    compassCtx.strokeStyle = major ? '#4df0c4' : '#2c4a58';
    compassCtx.lineWidth = (major ? 2 : 1) * devicePixelRatio;
    compassCtx.beginPath();
    compassCtx.moveTo(cx + r0 * Math.cos(rad), cy + r0 * Math.sin(rad));
    compassCtx.lineTo(cx + R * Math.cos(rad), cy + R * Math.sin(rad));
    compassCtx.stroke();
  }

  compassCtx.fillStyle = '#d9f2ec';
  compassCtx.font = `${13 * devicePixelRatio}px 'Space Grotesk'`;
  compassCtx.textAlign = 'center';
  compassCtx.textBaseline = 'middle';
  ['N','E','S','W'].forEach((label, i) => {
    const rad = (i * 90 - 90) * Math.PI / 180;
    const r0 = R * 0.72;
    compassCtx.fillText(label, cx + r0 * Math.cos(rad), cy + r0 * Math.sin(rad));
  });

  // needle
  const rad = (headingDeg - 90) * Math.PI / 180;
  compassCtx.save();
  compassCtx.translate(cx, cy);
  compassCtx.rotate(rad);
  compassCtx.fillStyle = '#4df0c4';
  compassCtx.shadowColor = '#4df0c4';
  compassCtx.shadowBlur = 14 * devicePixelRatio;
  const s = R * 0.14;
  compassCtx.beginPath();
  compassCtx.moveTo(R * 0.75, 0);
  compassCtx.lineTo(-s * 0.4, s * 0.6);
  compassCtx.lineTo(-s * 0.4, -s * 0.6);
  compassCtx.closePath();
  compassCtx.fill();
  compassCtx.strokeStyle = '#0a2a20';
  compassCtx.lineWidth = 1.5 * devicePixelRatio;
  compassCtx.stroke();
  compassCtx.restore();

  compassCtx.fillStyle = '#0a1e2a';
  compassCtx.beginPath();
  compassCtx.arc(cx, cy, R * 0.06, 0, Math.PI * 2);
  compassCtx.fill();
  compassCtx.strokeStyle = '#4df0c4';
  compassCtx.lineWidth = 1.5 * devicePixelRatio;
  compassCtx.stroke();
}

// ---------------- Polling / updates ----------------
async function poll() {
  try {
    const res = await fetch('/api/imu_mag');
    const d = await res.json();

    const dot = document.getElementById('connDot');
    const txt = document.getElementById('connText');
    if (d.connected) { dot.classList.add('live'); txt.textContent = 'connected'; }
    else { dot.classList.remove('live'); txt.textContent = 'disconnected'; }
    document.getElementById('msgRate').textContent = d.msg_rate ? `${d.msg_rate} Hz` : '—';
    if (d.last_msg_time) {
      const age = (Date.now()/1000) - d.last_msg_time;
      document.getElementById('msgAge').textContent = `${age.toFixed(1)}s`;
    }

    const m = d.latest_mag || {};
    document.getElementById('magX').textContent = fmt(m.x, 3);
    document.getElementById('magY').textContent = fmt(m.y, 3);
    document.getElementById('magZ').textContent = fmt(m.z, 3);

    document.getElementById('imuYaw').textContent =
      (d.latest_imu_yaw === null || d.latest_imu_yaw === undefined) ? '—' : `${d.latest_imu_yaw.toFixed(1)}°`;

    const hist = d.mag_history || [];
    if (hist.length) {
      const labels = hist.map((_, i) => i);

      magChartX.data.labels = labels;
      magChartX.data.datasets[0].data = hist.map(p => p.x);
      magChartX.update('none');

      magChartY.data.labels = labels;
      magChartY.data.datasets[0].data = hist.map(p => p.y);
      magChartY.update('none');

      magChartZ.data.labels = labels;
      magChartZ.data.datasets[0].data = hist.map(p => p.z);
      magChartZ.update('none');

      const latest = hist[hist.length - 1];
      const heading = computeHeadingDeg(latest.x, latest.y);
      document.getElementById('headingVal').textContent = `${heading.toFixed(1)}°`;
      document.getElementById('headingCompass').textContent = compassLabel(heading);
      drawCompass(heading, true);
    } else {
      drawCompass(0, false);
    }
  } catch (e) {
    console.error('poll error', e);
  }
}

resizeCompass();
setInterval(poll, 200);
poll();
</script>
</body>
</html>
"""

# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main():
    threading.Thread(target=ros_spin_thread, daemon=True).start()
    print(f"Starting IMU/Mag dashboard on http://0.0.0.0:{WEB_PORT}")
    app.run(host="0.0.0.0", port=WEB_PORT, debug=False, use_reloader=False)

if __name__ == "__main__":
    main()
