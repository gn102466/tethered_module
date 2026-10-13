#!/usr/bin/env python3
"""
ROS2 test suite for jetson_tcp_node.py (ROV/AUV TCP server node).

Covers:
  1. Connectivity  - IP/port reachability ("ping" equivalent for the TCP service)
  2. Handshake     - client can connect and heartbeat works
  3. Telemetry     - sensor topics -> TCP telemetry, schema + range/garbage-value checks
  4. Commands/state- arm/disarm, mode changes, request_state
  5. File transfer - upload + download round trip, integrity check via hash

Package layout (pralay2_ros2_isaacsim/src/tethered_module):
    tethered_module/
    ├── tethered_module/
    │   └── jetson_tcp_node.py      <- module under test
    └── test/
        └── test_jetson_tcp_node.py <- this file goes here

Prerequisites:
    source /opt/ros/<distro>/setup.bash
    pip install pytest --break-system-packages
    # rclpy, std_msgs, sensor_msgs, geometry_msgs, visualization_msgs come from your ROS2 install

Build once so `tethered_module` is a real importable package, then run:
    cd ~/pralay2_ros2_isaacsim
    colcon build --packages-select tethered_module
    source install/setup.bash
    colcon test --packages-select tethered_module --pytest-args -k test_jetson_tcp_node -v
or standalone once the workspace is sourced:
    pytest src/tethered_module/test/test_jetson_tcp_node.py -v -s

NOTE on test isolation: the node's internal state (mode, armed, latest sensor
values) persists across the whole test session because it's created once with
module scope. Tests are written to not depend on a specific run order, but if
you add new tests that need a truly clean slate, reset node.latest / node.mode
/ node.armed at the start of that test.
"""

import hashlib
import json
import math
import socket
import struct
import subprocess
import time
from pathlib import Path

import pytest
import rclpy
from geometry_msgs.msg import Vector3Stamped
from sensor_msgs.msg import BatteryState, FluidPressure, Imu, NavSatFix
from visualization_msgs.msg import Marker, MarkerArray

# `tethered_module` only resolves once the workspace has been built and
# `install/setup.bash` sourced - it's a real installed package, not a
# loose script, so no sys.path hack is needed (or wanted) here.
from tethered_module.jetson_tcp_node import (  # noqa: E402
    PORT,
    PKT_HEARTBEAT,
    PKT_HEARTBEAT_ACK,
    PKT_JSON,
    PKT_FILE_REQ,
    PKT_FILE_CHUNK,
    PKT_FILE_COMPLETE,
    TcpServerNode,
    pack_json,
    pack_message,
)

TEST_HOST = "127.0.0.1"


# ----------------------------------------------------------------------
# Low-level TCP test client - mirrors the laptop backend's wire protocol
# ----------------------------------------------------------------------
class RawTestClient:
    def __init__(self, host=TEST_HOST, port=PORT, timeout=5.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.buffer = bytearray()

    def send_json(self, msg_type, obj):
        self.sock.sendall(pack_json(msg_type, obj))

    def send_raw(self, msg_type, payload):
        self.sock.sendall(pack_message(msg_type, payload))

    def recv_packet(self, timeout=5.0):
        self.sock.settimeout(timeout)
        while len(self.buffer) < 9 or len(self.buffer) < 9 + struct.unpack(
            "!I", self.buffer[5:9]
        )[0]:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("socket closed by server")
            self.buffer.extend(chunk)
        payload_len = struct.unpack("!I", self.buffer[5:9])[0]
        msg_type = self.buffer[4]
        total = 9 + payload_len
        payload = bytes(self.buffer[9:total])
        del self.buffer[:total]
        return msg_type, payload

    def recv_json(self, expect_type=None, timeout=5.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            mt, payload = self.recv_packet(timeout=max(0.1, deadline - time.time()))
            if mt == PKT_JSON:
                obj = json.loads(payload.decode())
                if expect_type is None or obj.get("type") == expect_type:
                    return obj
            elif expect_type is None:
                return {"_raw_type": mt, "_raw_payload": payload}
        raise TimeoutError(f"timed out waiting for JSON type={expect_type!r}")

    def recv_until_type(self, expected_msg_type, timeout=5.0):
        """
        The node streams telemetry (20Hz) and state (1Hz) continuously and
        unprompted, independent of anything the client sends. Any raw,
        non-JSON reply (like PKT_HEARTBEAT_ACK) can arrive interleaved with
        those broadcasts, so grabbing "the next packet" isn't safe - this
        skips anything that doesn't match while waiting for the one we want.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            mt, payload = self.recv_packet(timeout=max(0.1, deadline - time.time()))
            if mt == expected_msg_type:
                return mt, payload
        raise TimeoutError(f"timed out waiting for packet type={expected_msg_type!r}")

    def close(self):
        self.sock.close()


def _wait_for_port(host, port, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"{host}:{port} never came up")


def _is_finite_number(x):
    return isinstance(x, (int, float)) and math.isfinite(x)


# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------
@pytest.fixture(scope="module")
def ros_context():
    rclpy.init()
    yield
    rclpy.shutdown()


@pytest.fixture(scope="module")
def node(ros_context):
    import threading

    n = TcpServerNode()
    spin_thread = threading.Thread(target=lambda: rclpy.spin(n), daemon=True)
    spin_thread.start()
    _wait_for_port(TEST_HOST, PORT, timeout=5.0)
    yield n
    n.destroy_node()


@pytest.fixture(scope="module")
def client(node):
    """
    ONE persistent connection reused for every test in this file - matching
    how a real client (the laptop backend) actually uses this server, and
    avoiding hammering the server's slow single-client accept/teardown
    cycle (send/recv thread joins + FileTransferHandler teardown can take
    several seconds per reconnect). Reconnecting fresh per test caused
    cascading connection timeouts once enough tests piled up.
    """
    c = RawTestClient()
    yield c
    c.close()


# ----------------------------------------------------------------------
# 1. Connectivity
# ----------------------------------------------------------------------
class TestConnectivity:
    def test_port_is_reachable(self, node):
        """TCP-connect check - the practical 'ping' for this service."""
        with socket.create_connection((TEST_HOST, PORT), timeout=3.0) as s:
            assert s.fileno() != -1

    def test_icmp_ping(self):
        """
        Real ICMP ping. Loopback always succeeds trivially - swap `target`
        for the actual Jetson IP (e.g. 192.168.1.10) when running this over
        the ethernet link to verify physical/network-layer reachability.
        """
        target = "127.0.0.1"
        try:
            result = subprocess.run(
                ["ping", "-c", "1", "-W", "2", target],
                capture_output=True,
                timeout=5,
            )
        except FileNotFoundError:
            pytest.skip("ping not available on this system")
        assert result.returncode == 0, f"ping failed: {result.stdout}"


# ----------------------------------------------------------------------
# 2. Handshake
# ----------------------------------------------------------------------
class TestHandshake:
    def test_heartbeat_ack(self, client):
        client.send_raw(PKT_HEARTBEAT, b"")
        mt, _ = client.recv_until_type(PKT_HEARTBEAT_ACK, timeout=5.0)
        assert mt == PKT_HEARTBEAT_ACK

    def test_request_state(self, client):
        client.send_json(PKT_JSON, {"type": "request_state"})
        obj = client.recv_json(expect_type="state")
        assert "data" in obj
        assert "mode" in obj["data"]
        assert "armed" in obj["data"]

    def test_malformed_json_does_not_crash_connection(self, client):
        # Send garbage payload under the JSON packet type
        client.send_raw(PKT_JSON, b"{not valid json")
        # Connection should stay alive - heartbeat still works afterwards
        client.send_raw(PKT_HEARTBEAT, b"")
        mt, _ = client.recv_until_type(PKT_HEARTBEAT_ACK, timeout=5.0)
        assert mt == PKT_HEARTBEAT_ACK


# ----------------------------------------------------------------------
# 3. Telemetry - schema + garbage-value checks
# ----------------------------------------------------------------------
EXPECTED_TELEMETRY_KEYS = {
    "depth",
    "pressure",
    "temperature",
    "humidity",
    "battery_voltage",
    "battery_current",
    "battery_percentage",
    "relay",
    "leaks",
    "mag",
    "gps",
}


class TestTelemetry:
    def test_telemetry_schema(self, node, client):
        obj = client.recv_json(expect_type="telemetry", timeout=2.0)
        data = obj["data"]
        missing = EXPECTED_TELEMETRY_KEYS - data.keys()
        assert not missing, f"telemetry missing expected keys: {missing}"

    def test_default_pressure_is_reasonable_pascals(self, node, client):
        """
        Sensor msg is in Pascals; standard atmosphere is ~101325 Pa.
        The current fallback default (1013.25) looks like hPa, not Pa -
        this is expected to FAIL against the current implementation and
        is intended to surface that unit mismatch.
        """
        obj = client.recv_json(expect_type="telemetry", timeout=2.0)
        p = obj["data"]["pressure"]
        assert p > 50000, f"pressure default looks like hPa, not Pa: {p}"

    def test_imu_orientation_is_normalized_quaternion(self, node, client):
        imu = Imu()
        imu.orientation.x, imu.orientation.y = 0.0, 0.0
        imu.orientation.z, imu.orientation.w = 0.0, 1.0
        imu.angular_velocity.x = 0.1
        imu.linear_acceleration.z = 9.81
        node.imu_callback(imu)

        obj = client.recv_json(expect_type="telemetry", timeout=2.0)
        ori = obj["data"]["orientation"]
        q_norm = math.sqrt(ori["x"] ** 2 + ori["y"] ** 2 + ori["z"] ** 2 + ori["w"] ** 2)
        assert math.isclose(q_norm, 1.0, abs_tol=0.05), (
            f"quaternion not normalized (garbage IMU data): norm={q_norm}"
        )
        for v in obj["data"]["angular_velocity"].values():
            assert _is_finite_number(v)

    def test_depth_is_non_negative_and_finite(self, node, client):
        depth = Vector3Stamped()
        depth.vector.x = 12.5
        node.depth_callback(depth)

        obj = client.recv_json(expect_type="telemetry", timeout=2.0)
        d = obj["data"]["depth"]
        assert _is_finite_number(d) and d >= 0, f"depth is garbage/out of range: {d}"

    def test_pressure_range_after_real_reading(self, node, client):
        pressure = FluidPressure()
        pressure.fluid_pressure = 202650.0  # ~2 atm, i.e. ~10m depth
        node.pressure_callback(pressure)

        obj = client.recv_json(expect_type="telemetry", timeout=2.0)
        p = obj["data"]["pressure"]
        assert _is_finite_number(p) and 50000 < p < 1_000_000, (
            f"pressure out of plausible range: {p}"
        )

    def test_battery_percentage_bounds(self, node, client):
        bat = BatteryState()
        bat.voltage = 15.8
        bat.current = -2.1
        bat.percentage = 0.76
        node.battery_callback(bat)

        obj = client.recv_json(expect_type="telemetry", timeout=2.0)
        pct = obj["data"]["battery_percentage"]
        assert _is_finite_number(pct) and 0.0 <= pct <= 1.0, (
            f"battery percentage is garbage: {pct}"
        )
        assert _is_finite_number(obj["data"]["battery_voltage"])
        assert _is_finite_number(obj["data"]["battery_current"])

    def test_gps_lat_lon_bounds(self, node, client):
        fix = NavSatFix()
        fix.latitude = 19.0760
        fix.longitude = 72.8777
        fix.altitude = 5.2
        node.gps_callback(fix)

        obj = client.recv_json(expect_type="telemetry", timeout=2.0)
        gps = obj["data"]["gps"]
        assert -90.0 <= gps["lat"] <= 90.0, f"latitude out of range: {gps['lat']}"
        assert -180.0 <= gps["lon"] <= 180.0, f"longitude out of range: {gps['lon']}"

    def test_leak_marker_parsing(self, node, client):
        arr = MarkerArray()
        for wet in (True, False, True, False):
            m = Marker()
            m.color.r = 1.0 if wet else 0.0
            arr.markers.append(m)
        node.leak_callback(arr)

        obj = client.recv_json(expect_type="telemetry", timeout=2.0)
        assert obj["data"]["leaks"] == [True, False, True, False]

    def test_malformed_leak_array_does_not_corrupt_state(self, node, client):
        good_leaks = list(node.latest["leaks"])
        bad_arr = MarkerArray()
        bad_arr.markers.append(Marker())  # only 1 marker; node expects 4
        node.leak_callback(bad_arr)
        # node should log a warning and keep the previous (valid) leak state
        assert node.latest["leaks"] == good_leaks
        assert len(node.latest["leaks"]) == 4


# ----------------------------------------------------------------------
# 4. Commands / state
# ----------------------------------------------------------------------
class TestCommands:
    @pytest.mark.parametrize(
        "task", ["arm", "disarm", "lights_on", "lights_off"]
    )
    def test_valid_commands_ack_success(self, client, task):
        client.send_json(PKT_JSON, {"type": "command", "task": task})
        obj = client.recv_json(expect_type="command_ack")
        assert obj["success"] is True

    def test_unknown_command_rejected(self, client):
        client.send_json(PKT_JSON, {"type": "command", "task": "not_a_real_command"})
        obj = client.recv_json(expect_type="command_ack")
        assert obj["success"] is False

    def test_mode_change_valid(self, client):
        client.send_json(PKT_JSON, {"type": "set_mode", "mode": "auto"})
        obj = client.recv_json(expect_type="mode_ack")
        assert obj["success"] is True
        assert obj["mode"] == "auto"

    def test_mode_change_invalid(self, client):
        client.send_json(PKT_JSON, {"type": "set_mode", "mode": "warp_speed"})
        obj = client.recv_json(expect_type="error")
        assert "invalid mode" in obj["message"].lower()


# ----------------------------------------------------------------------
# 5. File transfer
# ----------------------------------------------------------------------
class TestFileTransfer:
    def test_download_known_file(self, client):
        """
        NOTE: uses a raw PKT_FILE_REQ{direction:"download"} - this is the
        path that actually streams bytes (via FileTransferHandler). The
        higher-level "sync_config"/"sync_behavior_tree" JSON commands only
        send file metadata today and don't trigger byte streaming - worth
        confirming with the team whether that's intentional.
        """
        client.send_json(PKT_FILE_REQ, {"filename": "config.yaml", "direction": "download"})
        mt, payload = client.recv_packet()
        if mt == PKT_JSON:
            obj = json.loads(payload.decode())
            if obj.get("type") == "error":
                pytest.skip(
                    f"config.yaml not found ({obj['message']}) - the sample "
                    f"files are only created by the `if __name__ == "
                    f"'__main__':` block, which doesn't run when this module "
                    f"is imported for in-process testing"
                )
        assert mt == PKT_FILE_REQ
        meta = json.loads(payload.decode())
        expected_size = meta["size"]
        assert expected_size > 0

        received = bytearray()
        while len(received) < expected_size:
            mt, payload = client.recv_packet()
            assert mt in (PKT_FILE_CHUNK, PKT_FILE_COMPLETE)
            if mt == PKT_FILE_CHUNK:
                received.extend(payload)
            else:
                break
        assert len(received) == expected_size, (
            "downloaded file size mismatch - truncated/garbage transfer"
        )

    def test_download_nonexistent_file_returns_error(self, client):
        client.send_json(
            PKT_FILE_REQ, {"filename": "does_not_exist.yaml", "direction": "download"}
        )
        obj = client.recv_json(expect_type="error", timeout=3.0)
        assert "not found" in obj["message"].lower()

    def test_upload_roundtrip_integrity(self, client):
        payload = b"integration test payload " * 500
        digest_sent = hashlib.sha256(payload).hexdigest()
        filename = "pytest_upload.bin"

        client.send_json(
            PKT_FILE_REQ,
            {"filename": filename, "direction": "upload", "size": len(payload)},
        )
        ready = client.recv_json(expect_type="file_ready", timeout=5.0)
        assert ready["filename"] == filename

        for i in range(0, len(payload), 4096):
            client.send_raw(PKT_FILE_CHUNK, payload[i : i + 4096])
        client.send_raw(PKT_FILE_COMPLETE, b"{}")

        time.sleep(0.5)  # let FileTransferHandler flush + close the file
        written = Path("jetson_files") / filename
        assert written.exists(), "uploaded file never landed on disk"
        on_disk = written.read_bytes()
        assert hashlib.sha256(on_disk).hexdigest() == digest_sent, (
            "uploaded file is corrupted - bytes on disk don't match what was sent"
        )
        written.unlink()


# ----------------------------------------------------------------------
# 6. Garbage-value fuzzing
# ----------------------------------------------------------------------
class TestGarbageValueFuzzing:
    """
    Throws NaN / Infinity / physically-impossible values at the sensor
    callbacks and checks whether they leak through onto the wire.

    The node currently does no input validation, so most of these are
    expected to FAIL against the current implementation - that's the
    point. They document exactly what "garbage in" looks like on the
    client end of the socket, and will start passing the moment
    validation/clamping is added upstream.
    """

    def test_nan_imu_leaks_through(self, node, client):
        imu = Imu()
        imu.orientation.x = float("nan")
        node.imu_callback(imu)
        obj = client.recv_json(expect_type="telemetry", timeout=2.0)
        x = obj["data"]["orientation"]["x"]
        assert _is_finite_number(x), (
            f"NaN from a sensor propagated all the way to the client: {x!r}"
        )

    def test_infinite_depth_leaks_through(self, node, client):
        depth = Vector3Stamped()
        depth.vector.x = float("inf")
        node.depth_callback(depth)
        obj = client.recv_json(expect_type="telemetry", timeout=2.0)
        d = obj["data"]["depth"]
        assert _is_finite_number(d), f"infinite depth propagated to client: {d!r}"

    def test_negative_infinite_pressure_leaks_through(self, node, client):
        pressure = FluidPressure()
        pressure.fluid_pressure = float("-inf")
        node.pressure_callback(pressure)
        obj = client.recv_json(expect_type="telemetry", timeout=2.0)
        p = obj["data"]["pressure"]
        assert _is_finite_number(p), f"-infinity pressure propagated to client: {p!r}"

    def test_extreme_battery_percentage_out_of_bounds(self, node, client):
        bat = BatteryState()
        bat.percentage = 5.0  # 500% - a clearly miscalibrated/garbage reading
        node.battery_callback(bat)
        obj = client.recv_json(expect_type="telemetry", timeout=2.0)
        pct = obj["data"]["battery_percentage"]
        assert 0.0 <= pct <= 1.0, f"battery percentage out of physical bounds: {pct}"

    def test_extreme_gps_coordinates_out_of_bounds(self, node, client):
        fix = NavSatFix()
        fix.latitude = 999.0
        fix.longitude = -999.0
        node.gps_callback(fix)
        obj = client.recv_json(expect_type="telemetry", timeout=2.0)
        gps = obj["data"]["gps"]
        assert -90.0 <= gps["lat"] <= 90.0, f"impossible latitude passed through: {gps['lat']}"
        assert -180.0 <= gps["lon"] <= 180.0, f"impossible longitude passed through: {gps['lon']}"

    def test_negative_temperature_extreme_still_finite(self, node, client):
        from sensor_msgs.msg import Temperature

        temp = Temperature()
        temp.temperature = -999.0  # colder than absolute zero - sensor fault
        node.temp_callback(temp)
        obj = client.recv_json(expect_type="telemetry", timeout=2.0)
        t = obj["data"]["temperature"]
        # We don't clamp physically-implausible values here, just guard
        # against NaN/Inf; a tighter bound (-273.15 floor) is a candidate
        # validation rule if you want to enforce physical plausibility.
        assert _is_finite_number(t), f"non-finite temperature propagated: {t!r}"
