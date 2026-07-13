import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from mlbops.api.services.script_runner import (
    ScriptCapacityError,
    ScriptFailedError,
    ScriptRunner,
    ScriptTimeoutError,
)


class ScriptRunnerTests(unittest.TestCase):
    def test_returns_output(self):
        runner = ScriptRunner(timeout_seconds=2)
        with tempfile.TemporaryDirectory() as tmp:
            stdout, stderr = runner.run(
                [sys.executable, "-c", "print('card-ready')"], cwd=Path(tmp)
            )
        self.assertEqual(stdout.strip(), "card-ready")
        self.assertEqual(stderr, "")

    def test_reports_failed_process(self):
        runner = ScriptRunner(timeout_seconds=2)
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ScriptFailedError) as raised:
                runner.run(
                    [sys.executable, "-c", "import sys; sys.stderr.write('bad card'); sys.exit(7)"],
                    cwd=Path(tmp),
                )
        self.assertEqual(raised.exception.returncode, 7)
        self.assertIn("bad card", raised.exception.stderr)

    def test_stops_timed_out_process(self):
        runner = ScriptRunner(timeout_seconds=0.05, terminate_grace_seconds=0.05)
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ScriptTimeoutError):
                runner.run(
                    [sys.executable, "-c", "import time; time.sleep(5)"], cwd=Path(tmp)
                )

    def test_rejects_work_when_capacity_is_full(self):
        runner = ScriptRunner(
            max_concurrency=1, timeout_seconds=2, queue_wait_seconds=0.02
        )
        with tempfile.TemporaryDirectory() as tmp:
            cwd = Path(tmp)
            first = threading.Thread(
                target=runner.run,
                kwargs={
                    "cmd": [
                        sys.executable,
                        "-c",
                        "from pathlib import Path; import time; Path('started').touch(); time.sleep(.3)",
                    ],
                    "cwd": cwd,
                },
            )
            first.start()
            deadline = time.monotonic() + 1
            while not (cwd / "started").exists() and time.monotonic() < deadline:
                time.sleep(0.01)

            with self.assertRaises(ScriptCapacityError):
                runner.run([sys.executable, "-c", "pass"], cwd=cwd)
            first.join(timeout=1)
            self.assertFalse(first.is_alive())


if __name__ == "__main__":
    unittest.main()
