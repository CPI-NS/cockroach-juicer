"""Opt-in real SQL smoke; delegates process ownership to experiment-common."""

import json
import os
from pathlib import Path
import socket
import unittest

from experiment_common.allocation import allocate_clients
from experiment_common.collection import collect
from experiment_common.process import Executor
from crdb_bench import CRDBAdapter
from test_adapter import config


@unittest.skipUnless(os.environ.get("CRDB_SMOKE_BINARY"), "Opt-in real CRDB smoke")
class RealSQLSmoke(unittest.TestCase):
    def test_two_configs_use_real_sql_and_collect_retry_evidence(self):
        root = Path(
            os.environ.get("CRDB_SMOKE_OUTPUT", "/tmp/crdb-issue04-smoke")
        ).resolve()
        root.mkdir(parents=True, exist_ok=True)
        adapter = CRDBAdapter()
        results = []
        for mode, enabled in [("closed", False), ("open", True)]:
            if mode not in os.environ.get("CRDB_SMOKE_MODES", "closed,open").split(","):
                continue
            c = config()
            c["experiment_id"] = f"smoke-{mode}"
            c["workload"].update(keys=100, read_fraction=0.5)
            c["measurement"].update(
                warmup_seconds=1, duration_seconds=3, timeout_seconds=30
            )
            c["topology"]["splits"] = 1
            allocated_ports = set()
            for key in ("rpc_port", "sql_port", "http_port"):
                while True:
                    with socket.socket() as sock:
                        sock.bind(("127.0.0.1", 0))
                        port = sock.getsockname()[1]
                    if port not in allocated_ports:
                        break
                allocated_ports.add(port)
                c["topology"]["nodes"][0][key] = port
            c["load"] = (
                dict(mode="closed", concurrency=2, processes=2)
                if mode == "closed"
                else dict(
                    mode="open", rate=20, arrival="poisson", max_inflight=2, processes=2
                )
            )
            c["juicer"]["enabled"] = enabled
            c["artifacts"] = dict(
                cockroach=os.environ["CRDB_SMOKE_BINARY"],
                sql_client=os.environ["CRDB_SMOKE_CLIENT"],
                source_revision=os.environ["CRDB_SMOKE_SOURCE_REVISION"],
            )
            if os.environ.get("CRDB_SMOKE_SOURCE_MANIFEST"):
                c["artifacts"]["source_manifest"] = os.environ[
                    "CRDB_SMOKE_SOURCE_MANIFEST"
                ]
            cell = root / mode
            workspace = cell / "workspace"
            cell.mkdir(parents=True, exist_ok=True)
            (cell / "config.json").write_text(json.dumps(c, indent=2) + "\n")
            plan = adapter.plan_cell(c, allocate_clients(c), workspace)
            events = []

            def event(kind, **kw):
                events.append(dict(kind=kind, **kw))
                if kind.endswith(("started", "finished")):
                    print(
                        f'SMOKE {mode} {kind} {kw.get("command",{}).get("name","")}',
                        flush=True,
                    )

            executor = Executor(c, cell, event=event)
            try:
                for f in plan.files:
                    executor.write_file(f.host_id, f.path, f.content, root=workspace)
                for cmd in plan.prepare:
                    status = executor.execute(cmd)
                    self.assertEqual(status["returncode"], 0, status)
                for cmd in plan.servers:
                    executor.start(cmd)
                for cmd in plan.readiness:
                    status = executor.execute(cmd)
                    self.assertEqual(status["returncode"], 0, status)
                handles = [executor.start(cmd) for cmd in plan.clients]
                for h in handles:
                    status = executor.wait(h)
                    self.assertEqual(status["returncode"], 0, status)
                for cmd in plan.finalize:
                    status = executor.execute(cmd)
                    self.assertEqual(status["returncode"], 0, status)
            finally:
                executor.stop_all()
                (cell / "events.json").write_text(json.dumps(events, indent=2) + "\n")
            collect(executor, plan.collect, workspace, cell / "collected")
            result = adapter.parse_results(c, cell / "collected")
            (cell / "result.json").write_text(json.dumps(result, indent=2) + "\n")
            self.assertTrue(result["validity"]["valid"], result["validity"])
            self.assertGreater(result["metrics"]["commits"], 0)
            self.assertGreater(len(result["latency_samples_ms"]), 0)
            self.assertEqual(len(result["diagnostics"]["server_windows"]), 1)
            results.append(result["metrics"])
        (root / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
