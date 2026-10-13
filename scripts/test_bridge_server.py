#!/usr/bin/env python3
"""
Local loopback test bridge for tethered_module.

Stands in for ONE physical RS-485<->Ethernet bridge pair, so
jetson_tcp_node_d_m.py and laptop_server_d_m.py can be run and tested
end-to-end on a single machine with no RS-485 hardware attached at all.

Both of those files already carry a "CH-test" entry in their
CHANNELS_CONFIG pointing at 127.0.0.1:7000 (lowest priority, so it never
interferes with the real CH-A/B/C channels). To use it:

    1. Run this script:            python3 test_bridge_server.py
    2. Run the Jetson-side node:   ros2 run tethered_module jetson_tcp_node_d_m
    3. Run the laptop-side server: python3 laptop_server_d_m.py
       (any order for steps 2/3 - both just keep retrying until this
       script is up)

CH-test should show "connected" in the GCS's Link Channels panel once both
are up. Killing/restarting either process, or this script, exercises the
normal reconnect logic exactly like a real cable pull.

How it works: exactly like the real RS-485<->Ethernet bridge modules, this
script has NO understanding of the tethered_module wire protocol (the
ROVC-framed JSON/heartbeat/file-transfer packets) - it only accepts two TCP
connections and splices raw bytes between them in both directions. If
either side disconnects, BOTH connections are torn down together (a real
bridge pair going down takes the whole channel with it, not just one end),
and the script goes back to waiting for a fresh pair.
"""
import argparse
import functools
import socket
import threading

# Unbuffered so connect/disconnect events show up immediately regardless of
# whether stdout is a terminal or redirected to a log file - the whole
# point of this script is watching link activity in real time.
print = functools.partial(print, flush=True)


def _relay(src: socket.socket, dst: socket.socket, stop_event: threading.Event):
    try:
        while not stop_event.is_set():
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        stop_event.set()
        # A thread blocked in the OTHER direction's recv() won't notice
        # stop_event until its next loop iteration, which may never come if
        # that peer stays silent. Proactively shut down both sockets here
        # so that blocked recv() unblocks immediately (with an EOF/error)
        # instead of hanging - this is what makes "one side drops -> both
        # sides drop together" actually true rather than just documented.
        for sock in (src, dst):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def _bridge_pair(conn_a, addr_a, conn_b, addr_b):
    print(f"[bridge] Paired {addr_a} <-> {addr_b} - relaying...")
    stop_event = threading.Event()
    t1 = threading.Thread(target=_relay, args=(conn_a, conn_b, stop_event), daemon=True)
    t2 = threading.Thread(target=_relay, args=(conn_b, conn_a, stop_event), daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    for c in (conn_a, conn_b):
        try:
            c.close()
        except OSError:
            pass
    print(f"[bridge] Pair {addr_a} <-> {addr_b} disconnected - waiting for a new pair...")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default="127.0.0.1", help="Address to listen on (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=7000, help="Port to listen on (default: 7000, matches CH-test)")
    args = parser.parse_args()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((args.host, args.port))
    server.listen(5)
    print(f"[bridge] CH-test loopback bridge listening on {args.host}:{args.port}")
    print("[bridge] Waiting for both sides to connect (jetson_tcp_node_d_m.py + laptop_server_d_m.py)...")

    try:
        while True:
            conn_a, addr_a = server.accept()
            print(f"[bridge] First side connected from {addr_a}, waiting for the second...")
            conn_b, addr_b = server.accept()
            print(f"[bridge] Second side connected from {addr_b}")
            threading.Thread(target=_bridge_pair, args=(conn_a, addr_a, conn_b, addr_b), daemon=True).start()
    except KeyboardInterrupt:
        print("\n[bridge] Shutting down.")
    finally:
        server.close()


if __name__ == "__main__":
    main()
