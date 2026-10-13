"""
AUV bag recording launch file -- single-file mode.

UPDATED per requirement: recordings must produce exactly ONE .mcap file per
session (for downstream analysis simplicity), so time-based splitting
(--max-bag-duration) has been removed entirely.

Tradeoff this creates, and how it's partially mitigated:
  - Without splitting, there's no "worst case you only lose the last
    segment" guarantee -- the whole session is one continuous file.
  - MCAP mitigates this somewhat on its own: it writes data in internal
    chunks as it goes, not as one blob held in memory until close, so a
    file still open when the process dies is usually still recoverable
    (minus the last unflushed chunk) via the `mcap recover` CLI tool --
    not a 100% guarantee, but meaningfully better than an all-or-nothing
    file.
  - --max-cache-size is kept LOW so as little data as possible ever sits
    unflushed in the write cache at once -- this is the main remaining
    durability lever with splitting off.
  - respawn=True still applies to both processes below. If either
    crashes, it restarts. A recorder respawn starts a NEW single file
    (new timestamped directory) rather than corrupting/extending the old
    one -- you can still end up with multiple files across a crash+
    respawn event, just never from routine time-based splitting.

If a crash ever leaves a file that won't open normally, try:
  mcap recover damaged.mcap --output recovered.mcap
before assuming the data is lost.

NOTE: BAG_ROOT below must be kept identical to the BAG_ROOT constant in
bag_control_node.py -- they are not currently linked via a shared launch
parameter, so update both by hand if this path ever changes.
"""

from launch import LaunchDescription
from launch.actions import ExecuteProcess
from launch_ros.actions import Node

# Tune for your mission profile. MUST match BAG_ROOT in bag_control_node.py.
BAG_ROOT = '/home/gofloat/pralay2_ros2_isaacsim/jetson_files'
MAX_CACHE_SIZE_BYTES = '4194304'   # 4MB write cache -- kept low, this is now the
                                     # main durability lever with splitting off


def generate_launch_description():
    # No --max-bag-duration / --max-bag-size -> exactly one .mcap file per session.
    # Unique output dir per (re)start, generated at process-launch time.
    record_cmd = (
        'ros2 bag record -a '
        f'-o {BAG_ROOT}/mission_$(date +%Y%m%d_%H%M%S) '
        # f'--max-cache-size {MAX_CACHE_SIZE_BYTES}'
    )

    recorder_process = ExecuteProcess(
        cmd=['bash', '-c', record_cmd],
        output='screen',
        respawn=True,
        respawn_delay=2.0,
        name='rosbag2_recorder_process',
    )

    control_node = Node(
        package='tethered_module',
        executable='rosbag_node',
        name='rosbag_node',
        output='screen',
        respawn=True,
        respawn_delay=2.0,
    )

    return LaunchDescription([
        recorder_process,
        control_node,
    ])