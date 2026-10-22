"""Public SQL client CLI tests. No database is needed for workload traces."""

import json
import os
import re
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class ClientCLI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.binary = str(Path(cls.temp.name) / "sqlclient")
        subprocess.run(
            [
                "go",
                "build",
                *(["-race"] if os.environ.get("CRDB_TEST_RACE") else []),
                "-o",
                cls.binary,
                ".",
            ],
            cwd=ROOT / "sqlclient",
            env={**os.environ, "GOPROXY": "off", "GOSUMDB": "off"},
            check=True,
        )

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def trace(self, **changes):
        cfg = dict(
            keys=1,
            ops_per_txn=5,
            zipf_theta=0.99,
            seed=42,
            read_txn_pct=100,
            duplicates=True,
            scramble=True,
            mode="open",
            arrival="regular",
            rate=10,
            slots=2,
            warmup_seconds=0,
            duration_seconds=1,
        )
        cfg.update(changes)
        path = Path(self.temp.name) / "trace-config.json"
        path.write_text(json.dumps(cfg))
        result = subprocess.run(
            [self.binary, "trace", "--config", str(path), "--count", "8"],
            capture_output=True,
            text=True,
            check=True,
        )
        return [json.loads(line) for line in result.stdout.splitlines()]

    def test_duplicate_keys_preserve_all_five_draws(self):
        rows = self.trace()
        self.assertEqual(rows[0]["keys"], [0, 0, 0, 0, 0])
        self.assertEqual(rows[1]["offset_ns"], 100_000_000)
        self.assertEqual(rows[0]["txn_type"], "readOnly")


# A PostgreSQL wire boundary fixture: exercises the real pgx path, not a SQL executor mock.
import socketserver
import struct
import threading
import time


class PGServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), PGHandler)
        self.active = self.maximum = self.requests = 0
        self.lock = threading.Lock()
        self.retry_once = True
        self.queries = []


class PGHandler(socketserver.StreamRequestHandler):
    def send(self, kind, payload):
        self.wfile.write(kind + struct.pack("!I", len(payload) + 4) + payload)
        self.wfile.flush()

    def handle(self):
        try:
            length = struct.unpack("!I", self.rfile.read(4))[0]
            startup = self.rfile.read(length - 4)
            if startup[:4] == struct.pack("!I", 80877102):
                return
            self.send(b"R", struct.pack("!I", 0))
            self.send(b"S", b"standard_conforming_strings\0on\0")
            self.send(b"S", b"client_encoding\0UTF8\0")
            self.send(b"Z", b"I")
            while True:
                kind = self.rfile.read(1)
                if not kind or kind == b"X":
                    return
                n = struct.unpack("!I", self.rfile.read(4))[0]
                query = self.rfile.read(n - 4)
                if kind != b"Q":
                    raise AssertionError(kind)
                with self.server.lock:
                    self.server.active += 1
                    self.server.requests += 1
                    self.server.queries.append(query.decode())
                    retry = self.server.retry_once
                    self.server.retry_once = False
                    self.server.maximum = max(self.server.maximum, self.server.active)
                time.sleep(0.04)
                if retry:
                    self.send(b"E", b"SERROR\0C40001\0Mretry fixture\0\0")
                elif query.upper().startswith(b"UPSERT"):
                    self.send(b"C", b"INSERT 0 5\0")
                else:
                    field = b"count\0" + struct.pack("!IhIhih", 0, 0, 20, 8, -1, 0)
                    self.send(b"T", struct.pack("!h", 1) + field)
                    self.send(b"D", struct.pack("!hi", 1, 1) + b"1")
                    self.send(b"C", b"SELECT 1\0")
                with self.server.lock:
                    self.server.active -= 1
                self.send(b"Z", b"I")
        except (BrokenPipeError, ConnectionResetError):
            return


class SchedulerCLI(ClientCLI):
    def test_wait_cap_retry_latency_and_cutoff_through_real_pgx(self):
        server = PGServer()
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            cfg = dict(
                keys=1,
                ops_per_txn=5,
                zipf_theta=0.99,
                seed=42,
                read_txn_pct=100,
                duplicates=True,
                scramble=True,
                mode="open",
                arrival="regular",
                rate=1000,
                slots=1,
                warmup_seconds=0,
                duration_seconds=0.22,
                urls=[
                    f"postgresql://root@127.0.0.1:{server.server_address[1]}/defaultdb?sslmode=disable"
                ],
                output=str(Path(self.temp.name) / "run"),
                max_retries=1,
                retry_backoff_seconds=0.001,
                request_timeout_seconds=1,
            )
            path = Path(self.temp.name) / "run-config.json"
            path.write_text(json.dumps(cfg))
            proc = subprocess.run(
                [self.binary, "run", "--config", str(path)],
                capture_output=True,
                text=True,
                timeout=5,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            summary = json.loads(Path(cfg["output"] + ".summary.json").read_text())
            rows = [
                json.loads(line)
                for line in Path(cfg["output"] + ".events.jsonl")
                .read_text()
                .splitlines()
            ]
            measured = [r for r in rows if r["measured"]]
            self.assertTrue(summary["complete"])
            self.assertEqual(server.maximum, 1)
            self.assertGreaterEqual(summary["capacity_wait_count"], 1)
            self.assertGreaterEqual(summary["client_retries"], 1)
            self.assertEqual(summary["dropped"], 0)
            self.assertTrue(any(r["terminal"] == "unfinished" for r in rows))
            self.assertTrue(
                all(r["complete_unix_ns"] <= summary["end_unix_ns"] for r in measured)
            )
            self.assertTrue(
                all(35 <= r["latency_ms"] < 210 for r in measured), measured
            )
            self.assertGreater(summary["attempts"], summary["commits"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_blind_duplicate_inputs_are_not_merged(self):
        server = PGServer()
        server.retry_once = False
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            cfg = dict(
                keys=1,
                ops_per_txn=5,
                zipf_theta=0.99,
                seed=42,
                read_txn_pct=0,
                duplicates=True,
                scramble=True,
                mode="open",
                arrival="regular",
                rate=10,
                slots=1,
                warmup_seconds=0,
                duration_seconds=0.12,
                urls=[
                    f"postgresql://root@127.0.0.1:{server.server_address[1]}/defaultdb?sslmode=disable"
                ],
                output=str(Path(self.temp.name) / "blind"),
                request_timeout_seconds=1,
            )
            path = Path(self.temp.name) / "blind.json"
            path.write_text(json.dumps(cfg))
            proc = subprocess.run(
                [self.binary, "run", "--config", str(path)],
                capture_output=True,
                text=True,
                timeout=5,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertGreaterEqual(len(server.queries), 1)
            for query in server.queries:
                self.assertTrue(query.startswith("UPSERT INTO usertable"))
                self.assertEqual(len(re.findall(r"\(\s*'0'\s*,", query)), 5, query)
            summary = json.loads(Path(cfg["output"] + ".summary.json").read_text())
            self.assertGreaterEqual(summary["commits"], 1)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


class WorkloadCLI(ClientCLI):
    def test_ten_million_keys_zipf_and_poisson_are_parameterized_and_seeded(self):
        a = self.trace(keys=10_000_000, arrival="poisson", duplicates=False)
        b = self.trace(keys=10_000_000, arrival="poisson", duplicates=False)
        self.assertEqual(a, b)
        self.assertEqual(len(a[0]["keys"]), 5)
        self.assertTrue(all(len(set(row["keys"])) == 5 for row in a))
        self.assertTrue(all(0 <= key < 10_000_000 for row in a for key in row["keys"]))
        self.assertGreater(max(key for row in a for key in row["keys"]), 1000)
        self.assertNotEqual(a[1]["offset_ns"], 100_000_000)

    def test_scramble_matches_current_ycsbt_coprime_stride(self):
        # pkg/workload/ycsbt chooses the greatest coprime <= floor(.733*20): 13.
        ranks = self.trace(keys=20, scramble=False, duplicates=True)
        keys = self.trace(keys=20, scramble=True, duplicates=True)
        self.assertEqual(keys[0]["keys"], [(k * 13) % 20 for k in ranks[0]["keys"]])


class LayoutPlanCLI(ClientCLI):
    def test_top_hot_rank_plan_uses_distinct_ranges_and_balanced_node_targets(self):
        cfg = dict(
            keys=1000,
            ops_per_txn=5,
            zipf_theta=0.99,
            seed=42,
            read_txn_pct=90,
            duplicates=True,
            scramble=True,
            mode="closed",
            slots=3,
            warmup_seconds=0,
            duration_seconds=3,
            replicas=3,
            node_ids=["east", "west", "central"],
            urls=[
                "postgresql://root@n1/defaultdb",
                "postgresql://root@n2/defaultdb",
                "postgresql://root@n3/defaultdb",
            ],
            layout=dict(
                hot_ranks=16,
                lease_nodes=["east", "west", "central"],
                disable_rebalancing=True,
                monitor_interval_seconds=0.5,
            ),
        )
        path = Path(self.temp.name) / "layout.json"
        path.write_text(json.dumps(cfg))
        proc = subprocess.run(
            [self.binary, "layout-plan", "--config", str(path)],
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        result = json.loads(proc.stdout)
        self.assertEqual([r["key"] for r in result["hot_keys"][:4]], [0, 733, 466, 199])
        self.assertEqual(
            [r["target_node"] for r in result["hot_keys"][:4]],
            ["east", "west", "central", "east"],
        )
        self.assertEqual(len(set(r["key"] for r in result["hot_keys"])), 16)
        self.assertIn(734, result["split_keys"])


if __name__ == "__main__":
    unittest.main()
