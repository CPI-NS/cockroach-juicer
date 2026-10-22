"""SQL text classification from the existing raw server scrape capture."""

import copy
import json
from pathlib import Path
import tempfile
import unittest

from crdb_bench import CRDBAdapter
from crdb_bench.evaluation_metrics import RESTART_CAUSES, sql_retry_text_attribution
from crdb_bench.results import _counter_layers
from test_adapter import config
from test_evaluation import collected_fixture

PREFIX = "sql_txn_auto_retry_text_reason_"
URL = "http://localhost:28080/_status/vars"


def snapshots(counts=None, total=10, url=URL, offset=0):
    counts = (
        counts if counts is not None else dict(serializable=4, writetooold=3, unknown=3)
    )
    rows = []
    for index, at in enumerate((1_010_000_000 + offset, 2_900_000_000 + offset)):
        values = {
            PREFIX + cause + "_count": counts.get(cause, 0) if index else 0
            for cause in RESTART_CAUSES
        }
        values.update(
            sql_txn_auto_retry_count=total if index else 0,
            txn_commits=100 if index else 0,
            sys_uptime=100 + index * 2,
        )
        rows.append(
            dict(
                url=url,
                start_unix_ns=at,
                end_unix_ns=at + 1000,
                raw="\n".join(f"{name} {value}" for name, value in values.items()),
            )
        )
    return rows


def observe(rows, c=None):
    c = c or config()
    reasons = []
    _, _, windows = _counter_layers(
        [
            dict(
                start_unix_ns=1_000_000_000,
                end_unix_ns=3_000_000_000,
                counter_snapshots=rows,
            )
        ],
        reasons,
    )
    return sql_retry_text_attribution(c, windows)


class SQLRetryObservation(unittest.TestCase):
    def test_mixed_classes_unknown_and_exact_sql_denominator(self):
        report = observe(snapshots())
        self.assertEqual(report["status"], "available")
        self.assertEqual(report["pooled"]["sql_auto_retries"], 10)
        self.assertEqual(report["pooled"]["counts"]["unknown"], 3)
        self.assertEqual(report["pooled"]["fractions"]["serializable"], 0.4)
        self.assertEqual(report["pooled"]["fractions"]["unknown"], 0.3)
        self.assertIn("TEXT CLASSIFICATION", report["classification"])
        node = report["per_server"][URL]
        self.assertEqual(node["window"]["first_start_unix_ns"], 1_010_000_000)
        self.assertEqual(len(node["observation"]["first_raw_sha256"]), 64)

    def test_zero_population_retains_zeros_and_null_fractions(self):
        report = observe(snapshots({}, total=0))
        self.assertEqual(report["status"], "available")
        self.assertEqual(set(report["pooled"]["counts"].values()), {0})
        self.assertEqual(set(report["pooled"]["fractions"].values()), {None})

    def test_only_complete_configured_servers_pool_with_own_windows(self):
        c = config()
        node = copy.deepcopy(c["topology"]["nodes"][0])
        node.update(id="n2", http_port=28081)
        c["topology"]["nodes"].append(node)
        url2 = "http://localhost:28081/_status/vars"
        rows = snapshots() + snapshots(dict(txnpush=2), total=2, url=url2, offset=1000)
        report = observe(rows, c)
        self.assertEqual(report["pooled"]["sql_auto_retries"], 12)
        self.assertEqual(report["pooled"]["fractions"]["txnpush"], 2 / 12)
        self.assertNotEqual(
            report["per_server"][URL]["window"], report["per_server"][url2]["window"]
        )
        for partial in (rows[:2], rows[:3]):
            report = observe(partial, c)
            self.assertEqual(report["status"], "unavailable")
            self.assertIsNone(report["pooled"])
            self.assertEqual(report["per_server"][URL]["status"], "available")
        report = observe(rows)  # Unexpected servers cannot replace configured nodes.
        self.assertIsNone(report["pooled"])
        self.assertEqual(report["unexpected_servers"], [url2])

    def test_missing_series_labels_resets_and_process_restart(self):
        for mutation in (
            "legacy",
            "missing",
            "appeared",
            "disappeared",
            "labels",
            "partial_labels",
            "duplicate",
            "reset",
            "restart",
            "no_uptime",
            "fractional",
        ):
            with self.subTest(mutation=mutation):
                rows = snapshots()
                metric = PREFIX + "unknown_count"
                if mutation in ("legacy", "missing", "no_uptime"):
                    for row in rows:
                        row["raw"] = "\n".join(
                            line
                            for line in row["raw"].splitlines()
                            if not line.startswith(
                                PREFIX
                                if mutation == "legacy"
                                else "sys_uptime" if mutation == "no_uptime" else metric
                            )
                        )
                elif mutation == "appeared":
                    rows[0]["raw"] = rows[0]["raw"].replace(metric + " 0", "")
                elif mutation == "disappeared":
                    rows[-1]["raw"] = rows[-1]["raw"].replace(metric + " 3", "")
                elif mutation == "labels":
                    rows[-1]["raw"] = rows[-1]["raw"].replace(
                        metric + " ", metric + '{node="other"} '
                    )
                elif mutation == "partial_labels":
                    rows[0]["raw"] += "\n" + metric + '{extra="lost"} 0'
                elif mutation == "duplicate":
                    rows[-1]["raw"] += "\n" + metric + " 3"
                elif mutation == "reset":
                    rows[0]["raw"] = rows[0]["raw"].replace(
                        metric + " 0", metric + " 4"
                    )
                elif mutation == "restart":
                    rows[-1]["raw"] = rows[-1]["raw"].replace(
                        "sys_uptime 102", "sys_uptime 1"
                    )
                elif mutation == "fractional":
                    rows[-1]["raw"] = rows[-1]["raw"].replace(
                        metric + " 3", metric + " 3.5"
                    )
                report = observe(rows)
                self.assertEqual(report["status"], "unavailable")
                self.assertIsNone(report["pooled"])
                self.assertTrue(report["per_server"][URL]["reasons"])

    def test_mismatched_sum_is_invalid_without_repair(self):
        report = observe(snapshots(total=11))
        self.assertEqual(report["status"], "invalid")
        self.assertIsNone(report["pooled"])
        node = report["per_server"][URL]
        self.assertEqual(node["residual_sql_minus_text_reasons"], 1)
        self.assertEqual(node["counts"]["unknown"], 3)
        self.assertEqual(set(node["fractions"].values()), {None})

    def test_stable_native_labels_are_supported(self):
        rows = snapshots()
        for row in rows:
            row["raw"] = "\n".join(
                line.replace(" ", '{node_id="1",tenant="system"} ', 1)
                for line in row["raw"].splitlines()
            )
        self.assertEqual(observe(rows)["status"], "available")

    def test_enrich_keeps_kv_comparison_and_legacy_validity(self):
        for mode in ("legacy", "complete", "mismatch", "partial"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                c = collected_fixture(root)
                path = root / "client-0.summary.json"
                summary = json.loads(path.read_text())
                if mode != "legacy":
                    for row, extra in zip(
                        summary["counter_snapshots"], snapshots(total=10)
                    ):
                        row["raw"] += "\n" + "\n".join(
                            line
                            for line in extra["raw"].splitlines()
                            if line.startswith((PREFIX, "sys_uptime"))
                        )
                    if mode == "mismatch":
                        summary["counter_snapshots"][-1]["raw"] = summary[
                            "counter_snapshots"
                        ][-1]["raw"].replace(
                            PREFIX + "unknown_count 3", PREFIX + "unknown_count 2"
                        )
                    if mode == "partial":
                        summary["counter_snapshots"][0]["raw"] = summary[
                            "counter_snapshots"
                        ][0]["raw"].replace(PREFIX + "unknown_count 0", "")
                path.write_text(json.dumps(summary))
                result = CRDBAdapter().parse_results(c, root)
                self.assertTrue(result["validity"]["valid"], result["validity"])
                report = result["diagnostics"]["evaluation"]
                self.assertEqual(report["sql_retry_cause_identity"]["kv_cause_sum"], 10)
                self.assertEqual(
                    report["sql_retry_cause_identity"]["histogram_transaction_count"], 7
                )
                attr = report["sql_retry_cause_attribution"]
                self.assertEqual(
                    attr["status"],
                    {
                        "legacy": "unavailable",
                        "complete": "available",
                        "mismatch": "invalid",
                        "partial": "unavailable",
                    }[mode],
                )
                if mode == "complete":
                    self.assertEqual(attr["pooled"]["counts"]["serializable"], 4)
                    self.assertEqual(report["kv_restart_causes"]["serializable"], 1)
                    self.assertEqual(attr["pooled"]["fractions"]["serializable"], 0.4)
                    self.assertEqual(
                        report["sql_auto_retries_per_commit"]["value"], 0.5
                    )

    def test_sql_terminal_window_is_not_kv_restart_window(self):
        early = snapshots({}, total=0)
        late = snapshots()
        late[0]["start_unix_ns"] += 10_000
        late[0]["end_unix_ns"] += 10_000
        for rows, before, after in ((early, 0, 10), (late, 10, 10)):
            rows[0]["raw"] += f"\ntxn_restarts_serializable {before}"
            rows[-1]["raw"] += f"\ntxn_restarts_serializable {after}"

        def layers(rows):
            return _counter_layers(
                [
                    dict(
                        start_unix_ns=1_000_000_000,
                        end_unix_ns=3_000_000_000,
                        counter_snapshots=rows,
                    )
                ],
                [],
            )

        _, early_kv, early_windows = layers(early)
        _, late_kv, late_windows = layers(late)
        a = sql_retry_text_attribution(config(), early_windows)
        b = sql_retry_text_attribution(config(), late_windows)
        self.assertEqual(early_kv["txn_restarts_serializable"], 10)
        self.assertEqual(a["pooled"]["sql_auto_retries"], 0)
        self.assertIsNone(a["pooled"]["fractions"]["serializable"])
        self.assertEqual(late_kv["txn_restarts_serializable"], 0)
        self.assertEqual(b["pooled"]["sql_auto_retries"], 10)
        self.assertEqual(b["pooled"]["fractions"]["serializable"], 0.4)
        self.assertNotEqual(
            a["per_server"][URL]["window"], b["per_server"][URL]["window"]
        )

    def test_raw_capture_checks_intermediate_loss_reset_and_boundary_identity(self):
        for mode in ("complete", "missing", "reset", "identity", "malformed"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                c = collected_fixture(root)
                path = root / "client-0.summary.json"
                summary = json.loads(path.read_text())
                for row, extra in zip(summary["counter_snapshots"], snapshots()):
                    row["raw"] += "\n" + "\n".join(
                        line
                        for line in extra["raw"].splitlines()
                        if line.startswith((PREFIX, "sys_uptime"))
                    )
                path.write_text(json.dumps(summary))
                rows = copy.deepcopy(summary["counter_snapshots"])
                middle = copy.deepcopy(rows[-1])
                middle.update(start_unix_ns=2_000_000_000, end_unix_ns=2_000_001_000)
                if mode == "missing":
                    middle["raw"] = middle["raw"].replace(
                        PREFIX + "unknown_count 3", ""
                    )
                if mode == "reset":
                    middle["raw"] = middle["raw"].replace(
                        PREFIX + "unknown_count 3", PREFIX + "unknown_count 4"
                    )
                if mode == "identity":
                    rows[-1]["raw"] += "\n# different captured bytes"
                rows.insert(1, middle)
                raw = root / "client-0.metrics.jsonl"
                raw.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
                if mode == "malformed":
                    raw.write_text('{"missing": "scrape fields"}\n')
                result = CRDBAdapter().parse_results(c, root)
                self.assertTrue(result["validity"]["valid"], result["validity"])
                attr = result["diagnostics"]["evaluation"][
                    "sql_retry_cause_attribution"
                ]
                self.assertEqual(
                    attr["status"], "available" if mode == "complete" else "unavailable"
                )
                observation = attr["per_server"][URL]["observation"]
                if mode == "complete":
                    self.assertEqual(observation["validation_samples"], 3)
                    self.assertEqual(observation["validation_source"], "raw capture")
                else:
                    self.assertTrue(observation["issues"])
