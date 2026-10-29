"""Contract tests for generated native commands and collected results."""

import copy
import json
from pathlib import Path
import tempfile
import unittest
from crdb_bench import CRDBAdapter


def config():
    return {
        "schema_version": 1,
        "experiment_id": "tiny",
        "system": "crdb",
        "mode": "local",
        "inventory": [
            {
                "id": "local",
                "address": "localhost",
                "roles": ["server", "client"],
                "cpus": 4,
                "max_client_processes": 4,
                "work_dir": "/tmp/crdb-test",
            }
        ],
        "topology": {
            "nodes": [
                {
                    "id": "n1",
                    "host_id": "local",
                    "rpc_port": 27357,
                    "sql_port": 27358,
                    "http_port": 28080,
                }
            ],
            "replicas": 1,
        },
        "workload": {
            "keys": 10_000_000,
            "transaction_size": 5,
            "distinct_keys": False,
            "zipf": 0.99,
            "read_fraction": 0.9,
            "read_mode": "per_transaction",
            "seed": 42,
            "retries": 0,
        },
        "load": {"mode": "open", "rate": 10, "arrival": "poisson", "max_inflight": 3},
        "juicer": {"enabled": False},
        "measurement": {
            "warmup_seconds": 0,
            "duration_seconds": 2,
            "timeout_seconds": 30,
        },
        "runtime": {"reuse_data": False},
        "artifacts": {
            "cockroach": "/cached/cockroach",
            "sql_client": "/cached/sql-client",
            "source_revision": "test-revision",
        },
        "matrix": {},
        "repeats": 1,
    }


class AdapterPlan(unittest.TestCase):
    def test_readhold_candidates_reach_native_server_environment(self):
        for rules in ("readhold-ordered", "readhold-ordered-readpass", "readpass-guarded"):
            with self.subTest(rules=rules):
                c = config()
                c["juicer"] = dict(enabled=True, env={"RULES": rules})
                plan = CRDBAdapter().plan_cell(
                    c,
                    [dict(host_id="local", process_id=0, slots=3, rate=10, cpu_ids=[0, 1, 2])],
                    Path("/tmp/cell"),
                )
                self.assertEqual(plan.servers[0].env["COCKROACH_JUICER_RULES"], rules)

    def test_plan_uses_one_global_allocation_and_post_start_load(self):
        c = config()
        plan = CRDBAdapter().plan_cell(
            c,
            [dict(host_id="local", process_id=0, slots=3, rate=10, cpu_ids=[0, 1, 2])],
            Path("/tmp/cell"),
        )
        self.assertEqual(len(plan.servers), 1)
        self.assertEqual(len(plan.clients), 1)
        self.assertEqual(plan.servers[0].env["COCKROACH_JUICER"], "false")
        self.assertEqual(
            [cmd.argv[1] for cmd in plan.readiness], ["init", "ready", "load"]
        )
        cfg = json.loads(
            next(f.content for f in plan.files if f.path.endswith("client-0.json"))
        )
        self.assertEqual((cfg["slots"], cfg["rate"], cfg["ops_per_txn"]), (3, 10, 5))
        self.assertTrue(cfg["duplicates"])
        self.assertFalse(any("run_cell" in a for cmd in plan.clients for a in cmd.argv))


class CollectedMetrics(unittest.TestCase):
    def test_merges_samples_and_preserves_retry_layers_and_server_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            c = config()
            c["load"] = dict(mode="closed", concurrency=2, processes=2)
            (root / "n1.evidence.json").write_text(
                json.dumps(
                    dict(
                        valid=True, expected_enabled=False, interception_observed=False
                    )
                )
            )
            (root / "client-0.load.json").write_text(
                json.dumps(dict(rows=c["workload"]["keys"], seed=42, reset=True))
            )
            for name in ("n1", "client-0", "client-1"):
                (root / f"{name}.provenance.json").write_text(
                    json.dumps(
                        dict(
                            source_revision="test-revision",
                            sha256=dict(cockroach="a" * 64, sql_client="b" * 64),
                        )
                    )
                )
            for pid, latencies in enumerate(([1, 100], [2])):
                prefix = root / f"client-{pid}"
                events = [
                    dict(
                        id=i,
                        measured=True,
                        terminal="committed",
                        latency_ms=lat,
                        start_unix_ns=1_000_000_000,
                        complete_unix_ns=1_000_000_000 + int(lat * 1e6),
                    )
                    for i, lat in enumerate(latencies)
                ]
                Path(str(prefix) + ".events.jsonl").write_text(
                    "".join(json.dumps(r) + "\n" for r in events)
                )
                counters = []
                if pid == 0:
                    for at, commits, aborted in (
                        (1_010_000_000, 10, 2),
                        (2_900_000_000, 20, 17),
                    ):
                        raw = "\n".join(
                            f"{k} {v}"
                            for k, v in dict(
                                txn_commits=commits,
                                txn_restarts_txnaborted=aborted,
                                txn_restarts_writetooold=0,
                                txn_restarts_serializable=0,
                                txn_restarts_txnpush=0,
                                sql_txn_auto_retry_count=aborted + 2,
                                txn_restarts_readwithinuncertainty=1,
                            ).items()
                        )
                        counters.append(
                            dict(
                                url="http://localhost:28080/_status/vars",
                                start_unix_ns=at,
                                end_unix_ns=at + 1000,
                                raw=raw,
                            )
                        )
                Path(str(prefix) + ".summary.json").write_text(
                    json.dumps(
                        dict(
                            schema_version=1,
                            complete=True,
                            start_unix_ns=1_000_000_000,
                            end_unix_ns=3_000_000_000,
                            duration_seconds=2,
                            config=dict(slots=1, output=str(prefix)),
                            commits=len(events),
                            attempts=len(events),
                            attempt_errors=0,
                            client_retries=0,
                            visible_40001=0,
                            errors=0,
                            dropped=0,
                            counter_snapshots=counters,
                        )
                    )
                )
            result = CRDBAdapter().parse_results(c, root)
            self.assertTrue(result["validity"]["valid"], result["validity"])
            self.assertEqual(result["metrics"]["throughput_tps"], 1.5)
            self.assertEqual(result["metrics"]["latency_ms"], dict(p50=2, p99=100))
            self.assertEqual(
                result["diagnostics"]["retry_layers"]["cc_restarts_per_server_commit"][
                    "value"
                ],
                1.5,
            )
            self.assertEqual(
                result["diagnostics"]["retry_layers"]["cc_restarts_per_server_commit"][
                    "denominator"
                ],
                10,
            )
            self.assertEqual(result["metrics"]["attempts"], 3)
            # Treatment evidence is an acceptance condition, not just an output file.
            (root / "n1.evidence.json").write_text(
                json.dumps(
                    dict(
                        valid=False, expected_enabled=False, interception_observed=True
                    )
                )
            )
            self.assertFalse(CRDBAdapter().parse_results(c, root)["validity"]["valid"])
            # A missing process must never become a valid partial result.
            (root / "client-1.summary.json").unlink()
            self.assertFalse(CRDBAdapter().parse_results(c, root)["validity"]["valid"])


class CanonicalConfiguration(unittest.TestCase):
    def test_rejects_read_mode_reinterpretation_and_native_aliases(self):
        c = config()
        c["workload"]["read_mode"] = "per_operation"
        with self.assertRaisesRegex(ValueError, "per_transaction"):
            CRDBAdapter().validate(c)
        c = config()
        c["workload"]["ops_per_txn"] = 5
        with self.assertRaisesRegex(ValueError, "Unsupported workload"):
            CRDBAdapter().validate(c)
        c = config()
        c["workload"].update(keys=1, distinct_keys=True)
        with self.assertRaisesRegex(ValueError, "Distinct"):
            CRDBAdapter().validate(c)

    def test_ssh_plan_uses_deployed_artifacts_and_exact_multi_url_capacity(self):
        c = config()
        c["mode"] = "ssh"
        c["inventory"].append(dict(c["inventory"][0], id="remote", address="192.0.2.2"))
        c["topology"]["nodes"].append(
            dict(c["topology"]["nodes"][0], id="n2", host_id="remote")
        )
        c["artifacts"]["cockroach"] = dict(
            path="/control/cockroach",
            deploy_to="/worker/cockroach",
            hosts=["local", "remote"],
        )
        c["artifacts"]["sql_client"] = dict(
            path="/control/client",
            deploy_to="/worker/client",
            hosts=["local", "remote"],
        )
        c["workload"]["read_fraction"] = 0.333
        plan = CRDBAdapter().plan_cell(
            c,
            [dict(host_id="remote", process_id=0, slots=3, rate=10, cpu_ids=[0])],
            Path("/tmp/ssh-cell"),
        )
        self.assertEqual(plan.servers[1].host_id, "remote")
        self.assertEqual(plan.servers[0].argv[0], "/worker/cockroach")
        self.assertEqual(plan.clients[0].argv[0], "/worker/client")
        native = json.loads(
            next(f.content for f in plan.files if f.path.endswith("client-0.json"))
        )
        self.assertEqual(len(native["urls"]), 2)
        self.assertEqual(native["slots"], 3)
        self.assertAlmostEqual(native["read_txn_pct"], 33.3)
        self.assertEqual(native["mode"], "open")


class ArtifactPaths(unittest.TestCase):
    def test_local_mapping_runs_control_artifact_not_worker_destination(self):
        c = config()
        c["artifacts"]["cockroach"] = dict(
            path="/control/crdb", deploy_to="/worker/crdb"
        )
        plan = CRDBAdapter().plan_cell(
            c,
            [dict(host_id="local", process_id=0, slots=3, rate=10, cpu_ids=[0])],
            Path("/tmp/cell"),
        )
        self.assertEqual(plan.servers[0].argv[0], "/control/crdb")


if __name__ == "__main__":
    unittest.main()
