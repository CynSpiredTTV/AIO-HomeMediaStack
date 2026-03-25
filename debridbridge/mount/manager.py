"""rclone subprocess manager — mounts the WebDAV server as a local FUSE filesystem."""

import asyncio
import logging
import os
import signal
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

RCLONE_CONF_TEMPLATE = """\
[debridbridge]
type = webdav
url = http://localhost:{port}/dav
vendor = other
pacer_min_sleep = 0
"""

# Exponential backoff for restart: 5s, 10s, 30s, 60s max
BACKOFF_SEQUENCE = [5, 10, 30, 60]


class MountManager:
    """Manages the rclone FUSE mount subprocess."""

    def __init__(self, mount_path: str, webdav_port: int = 8181):
        self.mount_path = mount_path
        self.webdav_port = webdav_port
        self._process: subprocess.Popen | None = None
        self._running = False
        self._restart_count = 0
        self._config_path: str | None = None

    def preflight_check(self):
        """Check that FUSE is available. Call before starting."""
        if not os.path.exists("/dev/fuse"):
            raise RuntimeError(
                "FUSE not available: /dev/fuse does not exist. "
                "Ensure the container has /dev/fuse mounted and SYS_ADMIN capability. "
                "In your Portainer stack compose, add:\n"
                "  devices:\n"
                "    - /dev/fuse:/dev/fuse:rwm\n"
                "  cap_add:\n"
                "    - SYS_ADMIN"
            )

        mount_dir = Path(self.mount_path)
        if not mount_dir.exists():
            raise RuntimeError(
                f"Mount path does not exist: {self.mount_path}\n"
                f"On the ZimaOS host, create: /DATA/Media/realdebrid\n"
                f"(This maps to {self.mount_path} inside the container)"
            )

        # Check rclone is installed
        try:
            result = subprocess.run(
                ["rclone", "version"], capture_output=True, text=True, timeout=5
            )
            if result.returncode != 0:
                raise RuntimeError("rclone not found or not working")
            logger.info("rclone version: %s", result.stdout.split("\n")[0])
        except FileNotFoundError:
            raise RuntimeError(
                "rclone binary not found. Ensure it is installed in the Docker image."
            )

    def _write_config(self) -> str:
        """Write rclone.conf to a temporary file and return the path."""
        config_dir = Path("/config")
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "rclone.conf"
        config_path.write_text(
            RCLONE_CONF_TEMPLATE.format(port=self.webdav_port)
        )
        return str(config_path)

    def start(self):
        """Start the rclone mount subprocess."""
        self._config_path = self._write_config()
        self._running = True
        self._launch()

    def _launch(self):
        """Launch the rclone mount process."""
        cmd = [
            "rclone", "mount", "debridbridge:", self.mount_path,
            "--config", self._config_path,
            "--allow-non-empty",
            "--allow-other",
            "--uid=1000",
            "--gid=1000",
            "--umask=002",
            "--dir-cache-time=10s",
            "--vfs-read-ahead=256M",
            "--vfs-cache-mode=off",
            "--no-checksum",
            "--no-modtime",
            "--log-level=INFO",
        ]

        logger.info("Launching rclone: %s", " ".join(cmd))

        self._process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        import threading
        # Start a thread to read rclone output
        threading.Thread(
            target=self._read_output, daemon=True, name="rclone-output"
        ).start()

        # Start periodic health check thread
        if not hasattr(self, "_health_thread_started"):
            self._health_thread_started = True
            threading.Thread(
                target=self._health_check_loop, daemon=True, name="rclone-health"
            ).start()

    def _read_output(self):
        """Read rclone stdout/stderr and log it."""
        if not self._process:
            return
        for line in self._process.stdout:
            line = line.rstrip()
            if line:
                logger.info("[rclone] %s", line)

        # Process has exited
        returncode = self._process.wait()
        if self._running:
            logger.warning("rclone exited with code %d", returncode)
            self._handle_restart()

    def _handle_restart(self):
        """Restart rclone with exponential backoff."""
        if not self._running:
            return

        backoff_idx = min(self._restart_count, len(BACKOFF_SEQUENCE) - 1)
        delay = BACKOFF_SEQUENCE[backoff_idx]
        self._restart_count += 1

        logger.warning(
            "Restarting rclone in %ds (attempt %d)...", delay, self._restart_count
        )
        import time
        time.sleep(delay)

        if self._running:
            self._launch()

    def _health_check_loop(self):
        """Periodically check mount health."""
        import time as _time
        while self._running:
            _time.sleep(30)
            if self._running and not self.is_healthy():
                logger.warning("Mount health check failed — rclone mount may be stale")

    def is_healthy(self) -> bool:
        """Check if the mount is alive by stat-ing the mount path."""
        try:
            os.stat(self.mount_path)
            # Reset restart counter on healthy mount
            if self._restart_count > 0:
                self._restart_count = 0
            return True
        except OSError:
            return False

    def stop(self):
        """Gracefully stop rclone."""
        self._running = False

        if self._process and self._process.poll() is None:
            logger.info("Sending SIGTERM to rclone (pid=%d)", self._process.pid)
            self._process.send_signal(signal.SIGTERM)
            try:
                self._process.wait(timeout=10)
                logger.info("rclone stopped gracefully")
            except subprocess.TimeoutExpired:
                logger.warning("rclone did not stop in 10s, sending SIGKILL")
                self._process.kill()
                self._process.wait()
