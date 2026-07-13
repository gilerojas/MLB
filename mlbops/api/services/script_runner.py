"""Bounded subprocess execution for card-generation scripts."""

from __future__ import annotations

import os
import signal
import subprocess
import threading
from pathlib import Path


class ScriptCapacityError(RuntimeError):
    """Raised when every card-generation slot is busy."""


class ScriptTimeoutError(RuntimeError):
    """Raised when a card script exceeds its execution deadline."""


class ScriptFailedError(RuntimeError):
    """Raised when a card script exits unsuccessfully."""

    def __init__(self, returncode: int, stderr: str):
        super().__init__(f"Card script exited with status {returncode}")
        self.returncode = returncode
        self.stderr = stderr


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, value))


class ScriptRunner:
    def __init__(
        self,
        *,
        max_concurrency: int = 2,
        timeout_seconds: float = 300,
        queue_wait_seconds: float = 5,
        terminate_grace_seconds: float = 5,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.queue_wait_seconds = queue_wait_seconds
        self.terminate_grace_seconds = terminate_grace_seconds
        self._slots = threading.BoundedSemaphore(max(1, max_concurrency))

    @classmethod
    def from_env(cls) -> "ScriptRunner":
        return cls(
            max_concurrency=_env_int(
                "MLBOPS_CARD_SCRIPT_MAX_CONCURRENCY", 2, minimum=1, maximum=8
            ),
            timeout_seconds=_env_int(
                "MLBOPS_CARD_SCRIPT_TIMEOUT_SECONDS", 110, minimum=10, maximum=1800
            ),
            queue_wait_seconds=_env_int(
                "MLBOPS_CARD_SCRIPT_QUEUE_WAIT_SECONDS", 5, minimum=1, maximum=60
            ),
        )

    def run(self, cmd: list[str], *, cwd: Path) -> tuple[str, str]:
        if not self._slots.acquire(timeout=self.queue_wait_seconds):
            raise ScriptCapacityError("Card generation is at capacity; retry shortly.")

        process: subprocess.Popen[str] | None = None
        try:
            process = subprocess.Popen(
                cmd,
                cwd=str(cwd),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
            try:
                stdout, stderr = process.communicate(timeout=self.timeout_seconds)
            except subprocess.TimeoutExpired as exc:
                self._stop_process_group(process)
                raise ScriptTimeoutError(
                    f"Card script exceeded {self.timeout_seconds:g} seconds."
                ) from exc

            if process.returncode != 0:
                raise ScriptFailedError(process.returncode, stderr)
            return stdout, stderr
        finally:
            if process is not None and process.poll() is None:
                self._stop_process_group(process)
            self._slots.release()

    def _stop_process_group(self, process: subprocess.Popen[str]) -> None:
        if process.poll() is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError, OSError):
            try:
                process.terminate()
            except ProcessLookupError:
                return

        try:
            process.communicate(timeout=self.terminate_grace_seconds)
            return
        except subprocess.TimeoutExpired:
            pass

        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            try:
                process.kill()
            except ProcessLookupError:
                return
        process.communicate()


default_script_runner = ScriptRunner.from_env()
