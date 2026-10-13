#!/usr/bin/env python3
"""
Test suite for laptop_server.py.

This is NOT a ROS2 node (no rclpy dependency) - it's a Flask app + a raw
TCP client, so it's tested with plain pytest: a lightweight fake "Jetson"
TCP server stands in for the real one, and Flask's test_client() drives
the REST API without needing a live HTTP server.

File location (confirmed):
    pralay2_ros2_isaacsim/src/tethered_module/client/laptop_server.py

`client/` is NOT part of the installed `tethered_module` Python package
(only `tethered_module/tethered_module/` is), so laptop_server.py stays a
plain script and this test imports it via the sys.path insert below -
that's intentional, not a workaround to fix later.

Prerequisites:
    pip install pytest flask --break-system-packages

Run (from anywhere, no colcon/ROS2 needed):
    cd ~/pralay2_ros2_isaacsim/src/tethered_module/client
    pytest test_laptop_server.py -v -s
"""

import hashlib
import json
import math
import socket
import struct
import sys
import threading
import time
from io import BytesIO
from pathlib import Path
from queue import Queue, Empty

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import laptop_server as ls  # noqa: E402
from laptop_server import (  # noqa: E402
    JETSON_PORT,
    MAGIC,
    PKT_JSON,
    PKT_FILE_REQ,
    PKT_FILE_CHUNK,
    PKT_FILE_COMPLETE,
    app,
    shared,
    tcp_client,
    process_incoming,
    pack_json,
    pack_message,
)

TEST_HOST = "0.0.0.0"


def _wait_until(predicate, timeout=5.0, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


# ----------------------------------------------------------------------
# Fake Jetson - stands in for jetson_tcp_node.py's TCP server
# ----------------------------------------------------------------------
class MockJetsonServer:
    def __init__(self, host=TEST_HOST, port=JETSON_PORT):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))
        self.sock.listen(1)
        self.conn = None
        self.received = Queue()
        self._stop = threading.Event()
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def _accept_loop(self):
        self.sock.settimeout(1.0)
        while not self._stop.is_set():
            try:
                conn, _ = self.sock.accept()
                self.conn = conn
                threading.Thread(target=self._recv_loop, args=(conn,), daemon=True).start()
            except socket.timeout:
                continue
            except OSError:
                break

    def _recv_loop(self, conn):
        buffer = bytearray()
        while not self._stop.is_set():
            try:
                chunk = conn.recv(65536)
                if not chunk:
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
                    self.received.put((msg_type, payload))
            except OSError:
                break

    def wait_for_connection(self, timeout=10.0):
        if not _wait_until(lambda: self.conn is not None, timeout=timeout):
            raise TimeoutError("laptop backend never connected to the mock Jetson")

    def send_json(self, msg_type, obj):
        self.conn.sendall(pack_json(msg_type, obj))

    def send_raw(self, msg_type, payload):
        self.conn.sendall(pack_message(msg_type, payload))

    def recv_packet(self, timeout=5.0):
        return self.received.get(timeout=timeout)

    def stop(self):
        self._stop.set()
        try:
            self.sock.close()
        except OSError:
            pass
        if self.conn:
            try:
                self.conn.close()
            except OSError:
                pass


# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------
@pytest.fixture(scope="module")
def mock_jetson():
    server = MockJetsonServer()
    yield server
    server.stop()


@pytest.fixture(scope="module")
def running_app(mock_jetson):
    tcp_client.set_state_callback(lambda conn: setattr(shared, "connected", conn))
    tcp_client.start()
    processor_thread = threading.Thread(target=process_incoming, daemon=True)
    processor_thread.start()
    app._processor_thread = processor_thread  # exposed on the actual yielded object
    mock_jetson.wait_for_connection(timeout=10.0)
    assert _wait_until(lambda: shared.connected, timeout=5.0), "shared.connected never went True"
    yield app
    tcp_client.stop()


@pytest.fixture
def flask_client(running_app):
    return running_app.test_client()


@pytest.fixture(autouse=True)
def _drain_stray_packets(mock_jetson):
    """
    mock_jetson.received is ONE shared Queue across every test in this file
    (mock_jetson is module-scoped, mirroring one persistent TCP connection -
    matching how the real laptop backend holds a single long-lived socket).
    If any test's drain logic ever exits one packet early, that leftover
    packet silently becomes the *next* test's first recv_packet() result,
    producing confusing, cascading failures far from the actual cause.
    Flush anything still sitting in the queue before each test starts, as a
    safety net on top of fixing the drain logic itself.
    """
    while True:
        try:
            mock_jetson.received.get_nowait()
        except Empty:
            break
    yield


# ----------------------------------------------------------------------
# 1. Connection status
# ----------------------------------------------------------------------
class TestConnectionStatus:
    def test_connected_flag_true_after_handshake(self, flask_client):
        resp = flask_client.get("/api/connected")
        assert resp.status_code == 200
        assert resp.get_json()["connected"] is True


# ----------------------------------------------------------------------
# 2. Command forwarding
# ----------------------------------------------------------------------
class TestCommandForwarding:
    def test_command_forwarded_to_jetson(self, mock_jetson, flask_client):
        resp = flask_client.post(
            "/api/command", json={"type": "command", "task": "arm"}
        )
        assert resp.status_code == 200
        assert resp.get_json()["status"] == "sent"

        mt, payload = mock_jetson.recv_packet(timeout=5.0)
        assert mt == PKT_JSON
        obj = json.loads(payload.decode())
        assert obj == {"type": "command", "task": "arm"}

    def test_command_rejects_invalid_json(self, flask_client):
        resp = flask_client.post(
            "/api/command", data="not json", content_type="application/json"
        )
        assert resp.status_code in (400, 415)

    def test_command_returns_503_when_disconnected(self, flask_client):
        original = shared.connected
        shared.connected = False
        try:
            resp = flask_client.post(
                "/api/command", json={"type": "command", "task": "disarm"}
            )
            assert resp.status_code == 503
        finally:
            shared.connected = original


# ----------------------------------------------------------------------
# 3. Telemetry / state passthrough
# ----------------------------------------------------------------------
class TestTelemetryAndState:
    def test_telemetry_endpoint_reflects_jetson_push(self, mock_jetson, flask_client):
        payload = {
            "depth": 3.2,
            "battery_percentage": 0.81,
            "gps": {"lat": 19.076, "lon": 72.8777, "alt": 4.1},
        }
        mock_jetson.send_json(PKT_JSON, {"type": "telemetry", "data": payload})
        assert _wait_until(
            lambda: shared.latest_telemetry.get("depth") == 3.2, timeout=3.0
        ), "telemetry from Jetson never reached shared state"

        resp = flask_client.get("/api/telemetry")
        assert resp.status_code == 200
        assert resp.get_json() == payload

    def test_state_endpoint_reflects_jetson_push(self, mock_jetson, flask_client):
        state_data = {"mode": "teleop", "armed": True, "errors": []}
        mock_jetson.send_json(PKT_JSON, {"type": "state", "data": state_data})
        assert _wait_until(
            lambda: shared.latest_state.get("mode") == "teleop", timeout=3.0
        ), "state from Jetson never reached shared state"

        resp = flask_client.get("/api/state")
        assert resp.status_code == 200
        assert resp.get_json() == state_data

    def test_nan_telemetry_is_not_valid_json_for_a_browser(self, mock_jetson, flask_client):
        """
        Python's json.dumps()/Flask's jsonify() allow NaN/Infinity by
        default and emit the bare literal `NaN`, which is NOT valid JSON
        per RFC 8259. A browser's `response.json()` will throw on this.
        This test documents that gap rather than asserting it's fine -
        if someone adds `allow_nan=False` handling upstream, this should
        be updated to assert a clean error instead.
        """
        mock_jetson.send_json(
            PKT_JSON, {"type": "telemetry", "data": {"depth": float("nan")}}
        )
        assert _wait_until(
            lambda: "depth" in shared.latest_telemetry
            and isinstance(shared.latest_telemetry["depth"], float)
            and math.isnan(shared.latest_telemetry["depth"]),
            timeout=6.0,
        )
        resp = flask_client.get("/api/telemetry")
        body = resp.get_data(as_text=True)
        assert "NaN" in body, (
            "expected the un-sanitized NaN literal in the response body - "
            "this is what would break a strict JSON parser downstream"
        )


# ----------------------------------------------------------------------
# 4. File upload
# ----------------------------------------------------------------------
class TestFileUpload:
    def test_upload_roundtrip_integrity(self, mock_jetson, flask_client):
        payload = b"hello from pytest upload " * 300
        digest_expected = hashlib.sha256(payload).hexdigest()
        result = {}

        def do_upload():
            data = {"file": (BytesIO(payload), "pytest_upload.bin")}
            resp = flask_client.post(
                "/api/upload", data=data, content_type="multipart/form-data"
            )
            result["status_code"] = resp.status_code
            result["json"] = resp.get_json()

        t = threading.Thread(target=do_upload)
        t.start()

        mt, req_payload = mock_jetson.recv_packet(timeout=5.0)
        assert mt == PKT_FILE_REQ
        req = json.loads(req_payload.decode())
        assert req["direction"] == "upload"
        assert req["size"] == len(payload)
        mock_jetson.send_json(PKT_JSON, {"type": "file_ready", "filename": req["filename"]})

        received = bytearray()
        while True:
            mt, chunk_payload = mock_jetson.recv_packet(timeout=5.0)
            if mt == PKT_FILE_CHUNK:
                received.extend(chunk_payload)
            elif mt == PKT_FILE_COMPLETE:
                break
            else:
                pytest.fail(f"unexpected packet type {mt} while draining upload chunks")

        t.join(timeout=5.0)
        assert result["status_code"] == 200
        assert hashlib.sha256(bytes(received)).hexdigest() == digest_expected, (
            "corrupted upload - bytes received by Jetson don't match what was sent"
        )

    def test_upload_ready_timeout_returns_504(self, mock_jetson, flask_client):
        """Jetson never sends file_ready -> laptop should give up, not hang forever."""
        payload = b"never acknowledged"
        result = {}

        def do_upload():
            data = {"file": (BytesIO(payload), "never_acked.bin")}
            resp = flask_client.post(
                "/api/upload", data=data, content_type="multipart/form-data"
            )
            result["status_code"] = resp.status_code

        t = threading.Thread(target=do_upload)
        t.start()
        mt, _ = mock_jetson.recv_packet(timeout=5.0)
        assert mt == PKT_FILE_REQ
        # deliberately do NOT send file_ready
        t.join(timeout=8.0)  # endpoint's internal timeout is 5s
        assert result["status_code"] == 504

    def test_upload_no_file_returns_400(self, flask_client):
        resp = flask_client.post("/api/upload", data={}, content_type="multipart/form-data")
        assert resp.status_code == 400


# ----------------------------------------------------------------------
# 5. File download
# ----------------------------------------------------------------------
class TestFileDownload:
    def test_process_incoming_download_logic_isolated(self, running_app):
        """
        Diagnostic test: test_download_roundtrip_integrity has failed
        identically 3 times in a row (download_state never gets set, no
        exception, process_incoming() confirmed alive, recv_queue confirmed
        empty) with no bug found in process_incoming()'s code on review.

        This bypasses the socket/mock_jetson/framing layer ENTIRELY and
        puts packets directly onto tcp_client.recv_queue - exactly what
        process_incoming() consumes. If THIS test also fails, the bug is
        inside process_incoming()'s own PKT_FILE_REQ/CHUNK/COMPLETE
        handling. If it passes, the bug is somewhere between the mock's
        socket writes and that queue (framing/parsing/threading), and
        test_download_roundtrip_integrity's failure is a real bug in that
        layer worth chasing separately.
        """
        payload = b"isolated test payload " * 100
        filename = "isolated_test.bin"

        # Reset any leftover state from other tests so this is a clean read
        ls.download_state = None
        ls.download_complete = False
        ls.download_error = None

        ls.tcp_client.recv_queue.put((
            PKT_FILE_REQ,
            json.dumps({
                "filename": filename, "size": len(payload), "direction": "download"
            }).encode(),
        ))

        assert _wait_until(lambda: ls.download_state is not None, timeout=3.0), (
            f"download_state was never set even via DIRECT queue injection "
            f"(bypassing the socket entirely) - the bug is inside "
            f"process_incoming()'s own PKT_FILE_REQ handling. "
            f"download_error={ls.download_error!r}"
        )

        for i in range(0, len(payload), 4096):
            ls.tcp_client.recv_queue.put((PKT_FILE_CHUNK, payload[i:i + 4096]))
        ls.tcp_client.recv_queue.put((PKT_FILE_COMPLETE, b"{}"))

        assert _wait_until(lambda: ls.download_complete, timeout=3.0), (
            f"download_complete never went True after direct injection. "
            f"download_error={ls.download_error!r}, "
            f"download_state is None: {ls.download_state is None}"
        )
        assert ls.download_file_path.read_bytes() == payload, (
            "bytes written to disk don't match what was injected - "
            "corruption is happening inside process_incoming() itself"
        )

        # Clean up so this test's state can't leak into later download
        # tests via api_download()'s missing state reset (see
        # test_download_roundtrip_integrity's docstring/failure for why
        # that matters).
        if ls.download_file_path and ls.download_file_path.exists():
            ls.download_file_path.unlink()
        ls.download_state = None
        ls.download_complete = False
        ls.download_error = None
        ls.download_file_path = None

    def test_download_roundtrip_integrity(self, mock_jetson, flask_client, running_app):
        payload = b"a file living on the jetson " * 400
        filename = "pytest_download.bin"
        result = {}

        def do_download():
            try:
                resp = flask_client.get(f"/api/download?filename={filename}")
                result["status_code"] = resp.status_code
                result["data"] = resp.data
            except Exception as e:
                # Otherwise an exception inside this thread just vanishes and
                # the main thread only ever sees a confusing missing key.
                result["exception"] = repr(e)

        t = threading.Thread(target=do_download)
        t.start()

        mt, req_payload = mock_jetson.recv_packet(timeout=5.0)
        assert mt == PKT_FILE_REQ
        req = json.loads(req_payload.decode())
        assert req["direction"] == "download"
        assert req["filename"] == filename

        mock_jetson.send_json(
            PKT_FILE_REQ, {"filename": filename, "size": len(payload), "direction": "download"}
        )
        for i in range(0, len(payload), 4096):
            mock_jetson.send_raw(PKT_FILE_CHUNK, payload[i : i + 4096])
        mock_jetson.send_json(PKT_FILE_COMPLETE, {"status": "ok"})

        t.join(timeout=15.0)

        if t.is_alive() or "status_code" not in result:
            processor_thread = getattr(running_app, "_processor_thread", None)
            pytest.fail(
                "download request never completed in time. Diagnostic state:\n"
                f"  thread still alive: {t.is_alive()}\n"
                f"  exception in thread: {result.get('exception')}\n"
                f"  ls.shared.connected: {ls.shared.connected}\n"
                f"  ls.download_complete: {ls.download_complete}\n"
                f"  ls.download_error: {ls.download_error}\n"
                f"  ls.download_state is None: {ls.download_state is None}\n"
                f"  process_incoming() thread alive: "
                f"{processor_thread.is_alive() if processor_thread else 'unknown'}\n"
                f"  tcp_client.recv_queue size (unprocessed packets): "
                f"{ls.tcp_client.recv_queue.qsize()}\n"
                "If process_incoming() thread alive is False, an earlier "
                "uncaught exception silently killed that background thread "
                "and every download from that point on will hang until "
                "DOWNLOAD_TIMEOUT (60s). If recv_queue size is > 0, the "
                "packet arrived but is stuck unprocessed - likely means "
                "process_incoming() is blocked, not dead."
            )

        assert result["status_code"] == 200
        assert result["data"] == payload, "downloaded bytes don't match source file"

    def test_download_missing_filename_returns_400(self, flask_client):
        resp = flask_client.get("/api/download")
        assert resp.status_code == 400

    def test_download_early_error_times_out_instead_of_failing_fast(
        self, mock_jetson, flask_client, monkeypatch
    ):
        """
        Known gap: process_incoming() only records an "error" JSON into
        download_error if download_state has already been opened by a
        prior FILE_REQ. If the Jetson reports an error (e.g. file not
        found) *before* ever sending that FILE_REQ, the error is silently
        dropped and /api/download just times out (504) instead of
        returning a fast, clear error. DOWNLOAD_TIMEOUT is shrunk here so
        the test doesn't take 60s to demonstrate it. If this later starts
        returning 500, the bug's been fixed - update this test to match.
        """
        monkeypatch.setattr(ls, "DOWNLOAD_TIMEOUT", 1)
        # Explicit clean slate - don't rely solely on api_download()'s own
        # reset (if any) or on the previous test's response-close callback
        # having actually fired, since a stray 200 here previously turned
        # out to mean stale state from an earlier test leaking through.
        ls.download_state = None
        ls.download_complete = False
        ls.download_error = None
        ls.download_file_path = None

        filename = "ghost_file.bin"
        result = {}

        def do_download():
            resp = flask_client.get(f"/api/download?filename={filename}")
            result["status_code"] = resp.status_code
            result["data"] = resp.data

        t = threading.Thread(target=do_download)
        t.start()
        mt, _ = mock_jetson.recv_packet(timeout=5.0)
        assert mt == PKT_FILE_REQ
        mock_jetson.send_json(PKT_JSON, {"type": "error", "message": "File not found"})
        t.join(timeout=5.0)
        assert result["status_code"] == 504, (
            f"expected a timeout (504) but got {result['status_code']}. "
            f"Response body: {result.get('data')!r}. A 200 here almost "
            f"certainly means stale data leaked in from a previous test's "
            f"download rather than this test's own flow (which never sends "
            f"real file data) - check whether download_state is being reset "
            f"everywhere it needs to be, in api_download() and/or its "
            f"response-close cleanup callback."
        )
