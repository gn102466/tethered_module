#!/usr/bin/env python3
"""
Hardware / network integration tests.

Run this against your REAL Jetson Nano, over the REAL ethernet link, with
jetson_tcp_node.py actually running on it. Unlike the other two suites:

  - test_jetson_tcp_node.py drives the node in-process via rclpy (no network).
  - test_laptop_server.py mocks the far end entirely (no Jetson needed).
  - THIS file speaks the wire protocol directly to a live node over a live
    physical link, and has zero dependency on ROS2/rclpy - it can run from
    your laptop, a CI runner on the same subnet, anywhere with Python.

Configure the target before running:
    export JETSON_TEST_HOST=192.168.1.10   # your Jetson's real IP
    export JETSON_TEST_PORT=9000
    pytest test_hardware_integration.py -v -s

Fast smoke test only (skip slow streaming/throughput tests):
    pytest test_hardware_integration.py -v -m "not slow"

Tests that require you to physically unplug/replug the ethernet cable are
marked `manual` and skipped by default:
    pytest test_hardware_integration.py -v -m manual -s

To silence "unknown marker" warnings, add to pytest.ini:
    [pytest]
    markers =
        slow: exercises real network throughput/timing, can take a while
        manual: requires physically intervening on the hardware/cable
"""

import hashlib
import json
import math
import os
import random
import socket
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path
from queue import Queue, Empty

import pytest

# ----------------------------------------------------------------------
# Protocol constants - duplicated on purpose so this file has NO import
# dependency on jetson_tcp_node.py / ROS2 being installed locally.
# ----------------------------------------------------------------------
MAGIC = b"ROVC"
PKT_HEARTBEAT = 0x01
PKT_HEARTBEAT_ACK = 0x02
PKT_JSON = 0x10
PKT_FILE_REQ = 0x11
PKT_FILE_CHUNK = 0x12
PKT_FILE_COMPLETE = 0x13

JETSON_HOST = os.environ.get("JETSON_TEST_HOST", "192.168.1.10")
JETSON_PORT = int(os.environ.get("JETSON_TEST_PORT", "9000"))

TELEMETRY_EXPECTED_KEYS = {
    "depth", "pressure", "temperature", "humidity",
    "battery_voltage", "battery_current", "battery_percentage",
    "relay", "leaks", "mag", "gps",
}


def pack_message(msg_type, payload):
    return MAGIC + struct.pack("!BI", msg_type, len(payload)) + payload


def pack_json(msg_type, obj):
    return pack_message(msg_type, json.dumps(obj).encode("utf-8"))


def _is_finite_number(x):
    return isinstance(x, (int, float)) and math.isfinite(x)


# ----------------------------------------------------------------------
# Raw client speaking the ROVC wire protocol
# ----------------------------------------------------------------------
class RawClient:
    def __init__(self, host=JETSON_HOST, port=JETSON_PORT, timeout=10.0):
        t0 = time.monotonic()
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.connect_latency = time.monotonic() - t0
        self.buffer = bytearray()

    def send_json(self, msg_type, obj):
        self.sock.sendall(pack_json(msg_type, obj))

    def send_raw(self, msg_type, payload):
        self.sock.sendall(pack_message(msg_type, payload))

    def recv_packet(self, timeout=10.0):
        self.sock.settimeout(timeout)
        while len(self.buffer) < 9 or len(self.buffer) < 9 + struct.unpack(
            "!I", self.buffer[5:9]
        )[0]:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("socket closed by peer")
            self.buffer.extend(chunk)
        payload_len = struct.unpack("!I", self.buffer[5:9])[0]
        msg_type = self.buffer[4]
        total = 9 + payload_len
        payload = bytes(self.buffer[9:total])
        del self.buffer[:total]
        return msg_type, payload

    def recv_json(self, expect_type=None, timeout=10.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            mt, payload = self.recv_packet(timeout=max(0.1, deadline - time.monotonic()))
            if mt == PKT_JSON:
                obj = json.loads(payload.decode())
                if expect_type is None or obj.get("type") == expect_type:
                    return obj
            elif expect_type is None:
                return {"_raw_type": mt, "_raw_payload": payload}
        raise TimeoutError(f"timed out waiting for JSON type={expect_type!r}")

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def close_abrupt(self):
        """Force an RST instead of a clean FIN, to simulate an abrupt drop."""
        try:
            l_onoff, l_linger = 1, 0
            self.sock.setsockopt(
                socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", l_onoff, l_linger)
            )
        except OSError:
            pass
        self.close()


# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------
@pytest.fixture(scope="module")
def require_reachable():
    """
    Fail once, fast, with a clear diagnostic instead of every single test
    below timing out separately if the Jetson is simply unreachable.
    """
    try:
        with socket.create_connection((JETSON_HOST, JETSON_PORT), timeout=3.0):
            pass
    except OSError as e:
        pytest.fail(
            f"Cannot reach Jetson at {JETSON_HOST}:{JETSON_PORT} - is "
            f"jetson_tcp_node.py running, and is the ethernet link up? "
            f"Set JETSON_TEST_HOST/JETSON_TEST_PORT if these defaults are "
            f"wrong. Underlying error: {e}"
        )


@pytest.fixture
def client(require_reachable):
    c = RawClient()
    yield c
    c.close()


# ----------------------------------------------------------------------
# 1. Network layer (below the application protocol)
# ----------------------------------------------------------------------
class TestNetworkLayer:
    def test_icmp_ping_reachable(self):
        try:
            result = subprocess.run(
                ["ping", "-c", "5", "-W", "2", JETSON_HOST],
                capture_output=True, text=True, timeout=15,
            )
        except FileNotFoundError:
            pytest.skip("ping not available on this system")
        assert result.returncode == 0, (
            f"ICMP ping to {JETSON_HOST} failed entirely:\n{result.stdout}\n{result.stderr}"
        )

    def test_icmp_packet_loss_acceptable(self):
        """Flags a flaky physical link (loose cable, bad switch port, etc.)."""
        try:
            result = subprocess.run(
                ["ping", "-c", "20", "-i", "0.2", "-W", "2", JETSON_HOST],
                capture_output=True, text=True, timeout=20,
            )
        except FileNotFoundError:
            pytest.skip("ping not available on this system")

        loss_pct = None
        for line in result.stdout.splitlines():
            if "packet loss" in line:
                for token in line.split(","):
                    token = token.strip()
                    if token.endswith("% packet loss"):
                        loss_pct = float(token.split("%")[0])
        if loss_pct is None:
            pytest.skip(f"couldn't parse ping output on this platform:\n{result.stdout}")
        assert loss_pct <= 10.0, (
            f"{loss_pct}% ICMP packet loss to {JETSON_HOST} - check the "
            f"ethernet cable/switch, this will cause dropped telemetry too"
        )

    def test_tcp_connect_latency_reasonable(self, require_reachable):
        c = RawClient()
        try:
            assert c.connect_latency < 1.0, (
                f"TCP connect took {c.connect_latency:.3f}s - unexpectedly "
                f"slow for a direct ethernet link"
            )
        finally:
            c.close()


# ----------------------------------------------------------------------
# 2. Application-level handshake
# ----------------------------------------------------------------------
class TestHandshake:
    def test_heartbeat_roundtrip(self, client):
        t0 = time.monotonic()
        client.send_raw(PKT_HEARTBEAT, b"")
        mt, _ = client.recv_packet(timeout=5.0)
        rtt = time.monotonic() - t0
        assert mt == PKT_HEARTBEAT_ACK
        assert rtt < 1.0, f"heartbeat RTT was {rtt:.3f}s - check for link congestion"

    def test_request_state(self, client):
        client.send_json(PKT_JSON, {"type": "request_state"})
        obj = client.recv_json(expect_type="state", timeout=5.0)
        assert "mode" in obj["data"]
        assert "armed" in obj["data"]


# ----------------------------------------------------------------------
# 3. Connection resilience - the part a mock can't meaningfully cover
# ----------------------------------------------------------------------
class TestConnectionResilience:
    def test_reconnect_after_clean_disconnect(self, require_reachable):
        c1 = RawClient()
        c1.send_raw(PKT_HEARTBEAT, b"")
        c1.recv_packet(timeout=5.0)
        c1.close()

        time.sleep(0.5)  # give the server's accept loop a moment to cycle

        c2 = RawClient()
        c2.send_raw(PKT_HEARTBEAT, b"")
        mt, _ = c2.recv_packet(timeout=5.0)
        assert mt == PKT_HEARTBEAT_ACK, "server didn't accept a fresh client after a clean disconnect"
        c2.close()

    def test_reconnect_after_abrupt_disconnect(self, require_reachable):
        """Simulates a dropped/killed connection (RST) rather than a clean close."""
        c1 = RawClient()
        c1.send_raw(PKT_HEARTBEAT, b"")
        c1.recv_packet(timeout=5.0)
        c1.close_abrupt()

        time.sleep(0.5)

        c2 = RawClient()
        c2.send_raw(PKT_HEARTBEAT, b"")
        mt, _ = c2.recv_packet(timeout=5.0)
        assert mt == PKT_HEARTBEAT_ACK, "server didn't recover after an abrupt (RST) disconnect"
        c2.close()

    def test_second_client_is_queued_not_rejected(self, require_reachable):
        """
        The server does `listen(1)` and serves one client at a time inside
        its accept loop. This documents the resulting behavior: a second
        connection is accepted at the OS/TCP level but gets no application
        traffic until the first client fully disconnects.
        """
        first = RawClient()
        first.send_raw(PKT_HEARTBEAT, b"")
        first.recv_packet(timeout=5.0)  # confirm first is actually being served

        second = RawClient(timeout=5.0)  # TCP connect should still succeed (backlog)

        # Second client should NOT get served while first is still connected
        second.sock.settimeout(2.0)
        with pytest.raises((socket.timeout, TimeoutError)):
            second.recv_packet(timeout=2.0)

        # Once first disconnects, second should start being served
        first.close()
        time.sleep(0.5)
        second.send_raw(PKT_HEARTBEAT, b"")
        mt, _ = second.recv_packet(timeout=8.0)
        assert mt == PKT_HEARTBEAT_ACK, (
            "second client was never served even after the first disconnected - "
            "if you need true multi-client support, the server design needs to change"
        )
        second.close()

    @pytest.mark.slow
    def test_rapid_connect_disconnect_cycles_dont_wedge_server(self, require_reachable):
        """Guards against thread/resource leaks from connection churn over time."""
        for _ in range(20):
            c = RawClient(timeout=5.0)
            c.close()
            time.sleep(0.05)

        # server should still be perfectly healthy afterward
        c = RawClient()
        c.send_raw(PKT_HEARTBEAT, b"")
        mt, _ = c.recv_packet(timeout=5.0)
        assert mt == PKT_HEARTBEAT_ACK, "server became unresponsive after connection churn"
        c.close()


# ----------------------------------------------------------------------
# 4. Telemetry over the real link
# ----------------------------------------------------------------------
@pytest.mark.slow
class TestTelemetryOverRealLink:
    def test_telemetry_schema_and_ranges_over_time(self, client):
        n_samples = 60
        violations = []
        for i in range(n_samples):
            obj = client.recv_json(expect_type="telemetry", timeout=5.0)
            data = obj["data"]
            missing = TELEMETRY_EXPECTED_KEYS - data.keys()
            if missing:
                violations.append(f"sample {i}: missing keys {missing}")
                continue
            if not (_is_finite_number(data["depth"]) and data["depth"] >= 0):
                violations.append(f"sample {i}: bad depth={data['depth']}")
            if not (0.0 <= data.get("battery_percentage", -1) <= 1.0):
                violations.append(f"sample {i}: bad battery_percentage={data['battery_percentage']}")
            gps = data.get("gps", {})
            if not (-90 <= gps.get("lat", 999) <= 90):
                violations.append(f"sample {i}: bad gps.lat={gps.get('lat')}")
            if not (-180 <= gps.get("lon", 999) <= 180):
                violations.append(f"sample {i}: bad gps.lon={gps.get('lon')}")

        assert not violations, (
            f"{len(violations)}/{n_samples} telemetry samples had schema/range "
            f"problems over the real link:\n" + "\n".join(violations[:10])
        )

    def test_telemetry_arrival_rate_close_to_20hz(self, client):
        timestamps = []
        for _ in range(30):
            client.recv_json(expect_type="telemetry", timeout=5.0)
            timestamps.append(time.monotonic())

        intervals = [b - a for a, b in zip(timestamps, timestamps[1:])]
        avg_interval = sum(intervals) / len(intervals)
        # nominal is 50ms (20Hz); be generous for real-world jitter
        assert 0.02 < avg_interval < 0.3, (
            f"average telemetry interval was {avg_interval*1000:.1f}ms "
            f"(expected ~50ms) - possible network congestion or the "
            f"publish timer isn't running at the expected rate"
        )


# ----------------------------------------------------------------------
# 5. File transfer over the real link
# ----------------------------------------------------------------------
@pytest.mark.slow
class TestFileTransferOverRealLink:
    @pytest.mark.parametrize("filename", ["test.txt", "config.yaml", "bt.xml"])
    def test_known_sample_files_download(self, client, filename):
        """
        NOTE: these sample files are only auto-created by the `if __name__
        == "__main__":` block at the bottom of jetson_tcp_node.py, i.e.
        only when the script is run directly with `python3
        jetson_tcp_node.py`. If you launch it via `ros2 run`/a console-
        script entry point, that block never executes and these files
        won't exist - this test will skip (not fail) in that case so it
        doesn't block your suite, but it's worth confirming which startup
        path you're actually using in production.
        """
        client.send_json(PKT_FILE_REQ, {"filename": filename, "direction": "download"})
        mt, payload = client.recv_packet(timeout=5.0)
        if mt == PKT_JSON:
            obj = json.loads(payload.decode())
            if obj.get("type") == "error":
                pytest.skip(
                    f"{filename} not found on Jetson ({obj['message']}) - likely "
                    f"started via ros2 run rather than `python3 jetson_tcp_node.py`"
                )
        assert mt == PKT_FILE_REQ
        meta = json.loads(payload.decode())
        expected_size = meta["size"]
        received = bytearray()
        while len(received) < expected_size:
            mt, payload = client.recv_packet(timeout=10.0)
            if mt == PKT_FILE_CHUNK:
                received.extend(payload)
            elif mt == PKT_FILE_COMPLETE:
                break
        assert len(received) == expected_size, f"{filename} arrived truncated/corrupted"

    def test_upload_then_download_roundtrip_over_real_network(self, client):
        payload = bytes(random.getrandbits(8) for _ in range(50_000))
        digest_sent = hashlib.sha256(payload).hexdigest()
        filename = "hw_roundtrip_test.bin"

        client.send_json(PKT_FILE_REQ, {
            "filename": filename, "direction": "upload", "size": len(payload)
        })
        ready = client.recv_json(expect_type="file_ready", timeout=10.0)
        assert ready["filename"] == filename

        for i in range(0, len(payload), 65536):
            client.send_raw(PKT_FILE_CHUNK, payload[i:i + 65536])
        client.send_raw(PKT_FILE_COMPLETE, b"{}")
        time.sleep(0.5)

        # now download it back
        client.send_json(PKT_FILE_REQ, {"filename": filename, "direction": "download"})
        mt, meta_payload = client.recv_packet(timeout=10.0)
        assert mt == PKT_FILE_REQ
        expected_size = json.loads(meta_payload.decode())["size"]

        received = bytearray()
        while len(received) < expected_size:
            mt, chunk = client.recv_packet(timeout=10.0)
            if mt == PKT_FILE_CHUNK:
                received.extend(chunk)
            elif mt == PKT_FILE_COMPLETE:
                break

        assert hashlib.sha256(bytes(received)).hexdigest() == digest_sent, (
            "file corrupted somewhere in the real upload -> disk -> download "
            "round trip over the physical link"
        )

    def test_large_file_throughput(self, client):
        size_mb = 5
        payload = bytes(random.getrandbits(8) for _ in range(size_mb * 1024 * 1024))
        filename = "hw_throughput_test.bin"

        client.send_json(PKT_FILE_REQ, {
            "filename": filename, "direction": "upload", "size": len(payload)
        })
        client.recv_json(expect_type="file_ready", timeout=10.0)

        t0 = time.monotonic()
        for i in range(0, len(payload), 65536):
            client.send_raw(PKT_FILE_CHUNK, payload[i:i + 65536])
        client.send_raw(PKT_FILE_COMPLETE, b"{}")
        elapsed = time.monotonic() - t0

        throughput_mbps = size_mb / elapsed
        print(f"\nUpload throughput: {throughput_mbps:.2f} MB/s over {elapsed:.2f}s")
        # generous floor - a healthy 100Mbps+ ethernet link should comfortably clear this
        assert throughput_mbps > 0.5, (
            f"upload throughput was only {throughput_mbps:.2f} MB/s - "
            f"check cable quality/switch speed/duplex settings"
        )


# ----------------------------------------------------------------------
# 6. Commands over the real link
# ----------------------------------------------------------------------
class TestCommandsOverRealLink:
    @pytest.mark.parametrize("task", ["arm", "disarm", "lights_on", "lights_off"])
    def test_command_ack(self, client, task):
        client.send_json(PKT_JSON, {"type": "command", "task": task})
        obj = client.recv_json(expect_type="command_ack", timeout=5.0)
        assert obj["success"] is True


# ----------------------------------------------------------------------
# 7. Manual physical fault injection - skipped unless explicitly requested
# ----------------------------------------------------------------------
class TestManualPhysicalFaults:
    @pytest.mark.manual
    def test_recovers_after_cable_unplug(self, require_reachable):
        c = RawClient()
        c.send_raw(PKT_HEARTBEAT, b"")
        c.recv_packet(timeout=5.0)
        print(f"\nConnected OK to {JETSON_HOST}:{JETSON_PORT}.")

        input(">>> Now UNPLUG the ethernet cable, then press Enter here...")

        # confirm the link is actually down
        deadline = time.monotonic() + 15.0
        dropped = False
        while time.monotonic() < deadline:
            try:
                c.send_raw(PKT_HEARTBEAT, b"")
                c.recv_packet(timeout=1.0)
            except (OSError, TimeoutError, ConnectionError):
                dropped = True
                break
        assert dropped, "connection never registered as dropped - was the cable actually unplugged?"
        c.close()

        input(">>> Now REPLUG the ethernet cable, wait a few seconds, then press Enter...")

        deadline = time.monotonic() + 30.0
        recovered = False
        while time.monotonic() < deadline:
            try:
                c2 = RawClient(timeout=3.0)
                c2.send_raw(PKT_HEARTBEAT, b"")
                mt, _ = c2.recv_packet(timeout=3.0)
                if mt == PKT_HEARTBEAT_ACK:
                    recovered = True
                    c2.close()
                    break
                c2.close()
            except (OSError, TimeoutError, ConnectionError):
                time.sleep(1.0)
        assert recovered, "server did not become reachable again within 30s of replugging the cable"
