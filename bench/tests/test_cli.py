"""Public CLI interruption tests using executable boundary fixtures.

These fixtures prove process ownership only; test_smoke.py proves real SQL.
"""

import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

from test_adapter import config

FAKE = r"""
import json,os,pathlib,subprocess,sys,time
op=sys.argv[1]
if op in ('start','run'):
    if op=='run':
        c=json.loads(pathlib.Path(sys.argv[sys.argv.index('--config')+1]).read_text())
        name=pathlib.Path(c['output']).name
    else: name='server'
    child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(600)'])
    marker=pathlib.Path(os.environ['CRDB_TEST_MARKERS'])/(name+'.json')
    marker.write_text(json.dumps({'parent':os.getpid(),'child':child.pid}))
    time.sleep(600)
elif op=='version': print('FAKE ORCHESTRATION FIXTURE')
"""


def running(pid):
    check = subprocess.run(
        ["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True
    )
    return (
        check.returncode == 0
        and bool(check.stdout.strip())
        and not check.stdout.strip().startswith("Z")
    )


class StopCLI(unittest.TestCase):
    def exercise_stop(self, explicit):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            markers = root / "markers"
            markers.mkdir()
            fake = root / "fixture"
            fake.write_text("#!" + sys.executable + "\n" + FAKE)
            fake.chmod(0o755)
            c = config()
            c["artifacts"] = dict(
                cockroach=str(fake),
                sql_client=str(fake),
                source_revision="fake-process-fixture",
            )
            c["load"] = dict(mode="closed", concurrency=2, processes=2)
            c["measurement"].update(
                warmup_seconds=0, duration_seconds=20, timeout_seconds=30
            )
            c["runtime"].update(
                deadline_seconds=90, cleanup_margin_seconds=5, max_attempts=1
            )
            c["matrix"] = dict(axes={"workload.read_fraction": [0.5, 0.9]})
            path = root / "config.json"
            path.write_text(json.dumps(c))
            output = root / "batch"
            env = {**os.environ, "CRDB_TEST_MARKERS": str(markers)}
            sentinel = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                start_new_session=True,
            )
            process = None
            try:
                with (root / "stdout.log").open("w") as out, (root / "stderr.log").open(
                    "w"
                ) as err:
                    process = subprocess.Popen(
                        [
                            sys.executable,
                            "-m",
                            "crdb_bench",
                            "run",
                            "--config",
                            str(path),
                            "--output",
                            str(output),
                        ],
                        env=env,
                        stdout=out,
                        stderr=err,
                    )
                    deadline = time.monotonic() + 10
                    while (
                        len(list(markers.glob("*.json"))) < 3
                        and time.monotonic() < deadline
                        and process.poll() is None
                    ):
                        time.sleep(0.05)
                    self.assertEqual(
                        len(list(markers.glob("*.json"))),
                        3,
                        (root / "stderr.log").read_text(),
                    )
                    if explicit:
                        stop = subprocess.run(
                            [
                                sys.executable,
                                "-m",
                                "crdb_bench",
                                "stop",
                                "--output",
                                str(output),
                            ],
                            env=env,
                            capture_output=True,
                            text=True,
                            timeout=15,
                        )
                        self.assertEqual(stop.returncode, 0, stop.stderr)
                    else:
                        process.send_signal(signal.SIGINT)
                    process.wait(timeout=15)
                summary = json.loads((output / "summary.json").read_text())
                self.assertNotEqual(summary["status"], "success", summary)
                self.assertEqual(len(summary["cells"]), 1, summary)
                self.assertEqual(summary["cells"][0]["status"], "interrupted", summary)
                self.assertEqual(summary["completed_cell_count"], 0)
                for marker in markers.glob("*.json"):
                    for pid in json.loads(marker.read_text()).values():
                        self.assertFalse(
                            running(pid), f"Owned process survived stop: {pid}"
                        )
                self.assertIsNone(sentinel.poll(), "Unrelated process was stopped")
                attempts = list(output.glob("cells/*/attempt-*/exits.json"))
                self.assertEqual(len(attempts), 1)
                self.assertEqual(
                    json.loads(attempts[0].read_text())["cleanup_errors"], []
                )
                if os.environ.get("CRDB_STOP_EVIDENCE"):
                    target = Path(os.environ["CRDB_STOP_EVIDENCE"]) / (
                        "explicit-stop" if explicit else "ctrl-c"
                    )
                    shutil.copytree(root, target)
            finally:
                if process is not None and process.poll() is None:
                    process.terminate()
                    process.wait(timeout=15)
                sentinel.terminate()
                sentinel.wait(timeout=5)

    def test_ctrl_c_stops_owned_servers_clients_and_children(self):
        self.exercise_stop(False)

    def test_explicit_stop_stops_owned_servers_clients_and_children(self):
        self.exercise_stop(True)
