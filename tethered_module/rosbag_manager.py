import subprocess
import signal
from pathlib import Path
from datetime import datetime


class RosbagManager:

    def __init__(self):
        self.process = None
        self.recording = False

        self.base_dir = Path.home() / "bags"
        self.base_dir.mkdir(exist_ok=True)

    def start_recording(self):

        if self.recording:
            return False, "Already Recording"

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        bag_name = self.base_dir / f"mission_{timestamp}"

        command = [
            "ros2",
            "bag",
            "record",
            "-a",
            "-o",
            str(bag_name)
        ]

        self.process = subprocess.Popen(command)

        self.recording = True

        return True, str(bag_name)

    def stop_recording(self):

        if not self.recording:
            return False, "Recording not running"

        self.process.send_signal(signal.SIGINT)

        self.process.wait()

        self.process = None
        self.recording = False

        return True, "Recording Stopped"

    def is_recording(self):
        # return self.recording
        if self.process is None:
            return False

        if self.process.poll() is not None:
            self.process = None
            self.recording = False
            return False

        return True