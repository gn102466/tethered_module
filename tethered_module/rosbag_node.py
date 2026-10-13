#!/usr/bin/env python3
"""
AUV bag recording control node -- v3, corrected after bench testing.

Bug history:
  v1 -> v2: Record() opens paused, not writing (fixed: record->resume->verify).
            Record.srv's return_code wasn't checked (fixed: checked explicitly).
  v2 -> v3: is_paused alone cannot distinguish CLOSED from PAUSED. Stop()
            internally sets the paused flag true as part of shutdown and
            never resets it, so after a real Stop, is_paused keeps
            returning paused=True forever -- indistinguishable from a
            session that's merely paused-but-still-open. Confirmed from
            recorder logs: Stop() always logs "Topics discovery stopped."
            right before "Pausing recording." / "Recording stopped", while
            a fresh Record() always logs "Topics discovery started."
            immediately. So CLOSED-vs-OPEN is now determined by
            is_discovery_running, and is_paused is only consulted once we
            already know a session is open.

UPDATED: single-mcap-file mode. Per requirement, the launch file no longer
splits files by time, and the `save`/`split` command is now DISABLED here
(see control_cb) since split_bagfile always creates a second file by
definition -- incompatible with "exactly one file per session." The
handle_split() method below is kept but unreachable, in case this
requirement changes later.

Confirmed interfaces (from bench output):
  Record.srv             : string uri  ---  int32 return_code, string error_string
  Stop.srv                : (empty)     ---  (empty)
  SplitBagfile             : (empty)     ---  (empty)
  IsPaused.srv             : (empty)     ---  bool paused
  Resume.srv               : (empty)     ---  (empty)
  IsDiscoveryRunning.srv   : (empty)     ---  bool running

State determination (two-signal check):
  discovery.running == False                        -> CLOSED
  discovery.running == True, is_paused.paused == True  -> PAUSED
  discovery.running == True, is_paused.paused == False -> WRITING

KNOWN LIMITATION: this assumes discovery is only ever toggled by this
node's own record()/stop() calls (true on your current launch config,
which doesn't pass --no-discovery). If discovery is ever started/stopped
independently via the start_discovery/stop_discovery services, or if the
recorder is launched with discovery disabled, this heuristic breaks and
needs a different signal.
"""

import datetime
import os

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy
from std_msgs.msg import String, Bool
from rosbag2_interfaces.srv import (
    Record, Stop, SplitBagfile, IsPaused, Resume, IsDiscoveryRunning,
)


RELIABLE_QOS = QoSProfile(
    reliability=ReliabilityPolicy.RELIABLE,
    history=HistoryPolicy.KEEP_LAST,
    depth=10,
)

# MUST match BAG_ROOT in bag_record.launch.py -- see fix note in handle_start().
BAG_ROOT = '/home/gofloat/pralay2_ros2_isaacsim/jetson_files'

SERVICE_WAIT_SEC = 2.0
MAX_RETRIES = 3
WATCHDOG_PERIOD_SEC = 5.0
STATUS_PERIOD_SEC = 1.0
POST_ACTION_VERIFY_DELAY_SEC = 0.3   # let the recorder settle before re-checking

UNKNOWN = 'unknown'   # haven't confirmed real state yet -- refuse to guess
CLOSED = 'closed'     # no open session
PAUSED = 'paused'     # session open, not writing
WRITING = 'writing'   # session open, actively writing


class BagControlNode(Node):
    def __init__(self):
        super().__init__('bag_control_node')

        self.state = UNKNOWN
        self.recorder_alive = False

        self.record_client = self.create_client(Record, '/rosbag2_recorder/record')
        self.stop_client = self.create_client(Stop, '/rosbag2_recorder/stop')
        self.resume_client = self.create_client(Resume, '/rosbag2_recorder/resume')
        self.split_client = self.create_client(SplitBagfile, '/rosbag2_recorder/split_bagfile')
        self.is_paused_client = self.create_client(IsPaused, '/rosbag2_recorder/is_paused')
        self.is_discovery_client = self.create_client(
            IsDiscoveryRunning, '/rosbag2_recorder/is_discovery_running')

        self.create_subscription(String, '/ros2_bag_control', self.control_cb, RELIABLE_QOS)
        self.status_pub = self.create_publisher(String, '/ros2_bag_control/recording_status', RELIABLE_QOS)
        self.alive_pub = self.create_publisher(Bool, '/ros2_bag_control/recorder_alive', RELIABLE_QOS)

        self.create_timer(WATCHDOG_PERIOD_SEC, self.watchdog_cb)
        self.create_timer(STATUS_PERIOD_SEC, self.publish_status)

        self.get_logger().info('Bag control node up. Verifying recorder state...')
        self.resync(on_done=lambda: self.get_logger().info(f'Initial state: {self.state}'))

    # ---------- the single source of truth (two-signal check) ----------

    def resync(self, on_done=None):
        """Determine real state via is_discovery_running, then is_paused. Never guess."""
        if not self.is_discovery_client.wait_for_service(timeout_sec=SERVICE_WAIT_SEC):
            self.recorder_alive = False
            self.state = UNKNOWN
            self.get_logger().error('is_discovery_running service unreachable -- state UNKNOWN.')
            if on_done:
                on_done()
            return

        future = self.is_discovery_client.call_async(IsDiscoveryRunning.Request())

        def _discovery_done(f):
            try:
                discovery_res = f.result()
                self.recorder_alive = True
            except Exception as e:
                self.recorder_alive = False
                self.state = UNKNOWN
                self.get_logger().error(f'is_discovery_running call failed ({e}) -- state UNKNOWN.')
                if on_done:
                    on_done()
                return

            if not discovery_res.running:
                # No open session.
                self._set_state(CLOSED, on_done)
                return

            # Session is open -- now check whether it's actively writing.
            if not self.is_paused_client.wait_for_service(timeout_sec=SERVICE_WAIT_SEC):
                self.get_logger().error('is_paused unreachable while discovery is running -- state UNKNOWN.')
                self._set_state(UNKNOWN, on_done)
                return

            paused_future = self.is_paused_client.call_async(IsPaused.Request())

            def _paused_done(pf):
                try:
                    paused_res = pf.result()
                    new_state = PAUSED if paused_res.paused else WRITING
                    self._set_state(new_state, on_done)
                except Exception as e:
                    self.get_logger().error(f'is_paused call failed ({e}) -- state UNKNOWN.')
                    self._set_state(UNKNOWN, on_done)

            paused_future.add_done_callback(_paused_done)

        future.add_done_callback(_discovery_done)

    def _set_state(self, new_state, on_done):
        if self.state != UNKNOWN and new_state != self.state:
            self.get_logger().warn(f'State mismatch: node had {self.state}, recorder reports {new_state}. Correcting.')
        self.state = new_state
        if on_done:
            on_done()

    def _verify_after(self, expected_state, ok_msg):
        """One-shot delayed re-check, so we confirm reality instead of trusting a response."""
        def _check():
            timer.cancel()
            self.resync(on_done=lambda: self._log_verify_result(expected_state, ok_msg))
        timer = self.create_timer(POST_ACTION_VERIFY_DELAY_SEC, _check)

    def _log_verify_result(self, expected_state, ok_msg):
        if self.state == expected_state:
            self.get_logger().info(ok_msg)
        else:
            self.get_logger().error(
                f'VERIFICATION MISMATCH: expected {expected_state} after action, '
                f'recorder actually reports {self.state}. Investigate immediately.'
            )

    # ---------- watchdog / status ----------

    def watchdog_cb(self):
        was_alive = self.recorder_alive
        self.resync()
        if was_alive and not self.recorder_alive:
            self.get_logger().error(
                'RECORDER UNREACHABLE. Expect auto-respawn per launch file into a new '
                'segment. Previously closed segments remain valid.'
            )
        elif not was_alive and self.recorder_alive:
            self.get_logger().info('Recorder reachable again.')

    def publish_status(self):
        s = String()
        s.data = self.state
        self.status_pub.publish(s)
        a = Bool()
        a.data = self.recorder_alive
        self.alive_pub.publish(a)

    # ---------- commands ----------

    def control_cb(self, msg: String):
        cmd = msg.data.strip().lower()
        if cmd == 'start':
            self.handle_start()
        elif cmd == 'stop':
            self.handle_stop()
        elif cmd in ('save', 'split'):
            self.get_logger().warn(
                'save/split is DISABLED: single-mcap-file mode is required, and '
                'split_bagfile always creates a second file by definition. '
                'No action taken.'
            )
        elif cmd == 'status':
            self.get_logger().info(f'state={self.state}, recorder_alive={self.recorder_alive}')
        else:
            self.get_logger().warn(f'Unknown command "{cmd}" -- ignored, no state change.')

    def handle_start(self):
        if self.state == UNKNOWN:
            self.get_logger().error('State unknown -- refusing to guess. Re-syncing before acting.')
            self.resync()
            return
        if self.state == WRITING:
            self.get_logger().info('Already recording -- skipping start.')
            return
        if self.state == PAUSED:
            self.get_logger().info('Session open but paused -- resuming.')
            self._call(self.resume_client, Resume.Request(), 'resume',
                       on_ok=lambda: self._verify_after(WRITING, 'Recording resumed.'))
            return

        # CLOSED -> open a brand new session
        # NOTE: bug fix -- a bare relative uri resolves against the recorder
        # process's cwd (wherever `ros2 launch` was invoked from), NOT
        # BAG_ROOT. Confirmed from bench log: sessions opened via start()
        # were landing in ~/pralay2_ros2_isaacsim instead of /home/tanay/auv_bags.
        # Always use an absolute path so every session lands in the same
        # known place regardless of launch cwd.
        req = Record.Request()
        req.uri = os.path.join(
            BAG_ROOT, 'bag_' + datetime.datetime.now().strftime('%Y_%m_%d-%H_%M_%S')
        )

        def on_record_response(res):
            if res.return_code != 0:
                self.get_logger().error(f'Record REJECTED: {res.error_string}. State left as-is.')
                self.resync()
                return
            self.get_logger().info('Session opened, resuming to start writing...')
            self._call(self.resume_client, Resume.Request(), 'resume',
                       on_ok=lambda: self._verify_after(WRITING, 'Recording started.'))

        self._call_checked(self.record_client, req, 'record', on_response=on_record_response)

    def handle_stop(self):
        if self.state == CLOSED:
            self.get_logger().info('Not recording -- skipping stop.')
            return
        if self.state == UNKNOWN:
            self.get_logger().warn('State unknown -- attempting stop anyway to avoid a dangling session.')
        self._call(self.stop_client, Stop.Request(), 'stop',
                   on_ok=lambda: self._verify_after(CLOSED, 'Stop issued, bag should be finalized.'))

    def handle_split(self):
        """DISABLED -- unreachable from control_cb. Kept in case single-file
        mode requirement changes later. split_bagfile always creates a
        second file, incompatible with the current one-file requirement."""
        if self.state != WRITING:
            self.get_logger().warn(f'Not actively writing (state={self.state}) -- nothing meaningful to save.')
            return
        self._call(self.split_client, SplitBagfile.Request(), 'split_bagfile',
                   on_ok=lambda: self.get_logger().info('Checkpoint saved (bag split).'))

    # ---------- generic call helpers ----------

    def _call(self, client, request, name, on_ok, retries_left=MAX_RETRIES):
        if not client.wait_for_service(timeout_sec=SERVICE_WAIT_SEC):
            if retries_left > 0:
                self.get_logger().warn(f'{name} service not ready, retrying ({retries_left} left)...')
                self._call(client, request, name, on_ok, retries_left - 1)
            else:
                self.get_logger().error(f'{name} FAILED: service unreachable after retries.')
                self.resync()
            return

        future = client.call_async(request)

        def _done(f):
            try:
                f.result()
                on_ok()
            except Exception as e:
                if retries_left > 0:
                    self.get_logger().warn(f'{name} call failed ({e}), retrying ({retries_left} left)...')
                    self._call(client, request, name, on_ok, retries_left - 1)
                else:
                    self.get_logger().error(f'{name} FAILED after retries: {e}')
                    self.resync()

        future.add_done_callback(_done)

    def _call_checked(self, client, request, name, on_response, retries_left=MAX_RETRIES):
        """Like _call, but hands the actual response (with return_code) to the caller."""
        if not client.wait_for_service(timeout_sec=SERVICE_WAIT_SEC):
            if retries_left > 0:
                self.get_logger().warn(f'{name} service not ready, retrying ({retries_left} left)...')
                self._call_checked(client, request, name, on_response, retries_left - 1)
            else:
                self.get_logger().error(f'{name} FAILED: service unreachable after retries.')
                self.resync()
            return

        future = client.call_async(request)

        def _done(f):
            try:
                res = f.result()
                on_response(res)
            except Exception as e:
                if retries_left > 0:
                    self.get_logger().warn(f'{name} call failed ({e}), retrying ({retries_left} left)...')
                    self._call_checked(client, request, name, on_response, retries_left - 1)
                else:
                    self.get_logger().error(f'{name} FAILED after retries: {e}')
                    self.resync()

        future.add_done_callback(_done)


def main():
    rclpy.init()
    node = BagControlNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == '__main__':
    main()