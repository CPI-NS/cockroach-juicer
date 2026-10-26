"""Evaluation features through generated plans, collected reports and real CLIs."""

import json
from pathlib import Path
import unittest
import tempfile
from crdb_bench import CRDBAdapter
from crdb_bench.evaluation_metrics import calibration_report
from test_adapter import config


class EvaluationPlan(unittest.TestCase):
    def test_plan_places_hot_ranks_and_requests_observe_only_calibration(self):
        c = config()
        c["topology"]["layout"] = dict(
            hot_ranks=16,
            lease_nodes=["n1"],
            disable_rebalancing=True,
            monitor_interval_seconds=0.5,
        )
        c["juicer"] = dict(enabled=True, hold_calibration=True, env={"EPOCH_MS": 250})
        plan = CRDBAdapter().plan_cell(
            c,
            [dict(host_id="local", process_id=0, slots=3, rate=10, cpu_ids=[0])],
            Path("/tmp/evaluation"),
        )
        self.assertEqual(plan.readiness[-1].argv[1], "layout")
        self.assertEqual(plan.observers[0].argv[1], "watch-layout")
        self.assertEqual(plan.finalize[-1].argv[1], "layout-check")
        self.assertEqual(
            plan.servers[0].env["COCKROACH_JUICER_HOLD_CALIBRATION"], "true"
        )
        native = json.loads(
            next(f.content for f in plan.files if f.path.endswith("client-0.json"))
        )
        self.assertEqual(native["layout"]["hot_ranks"], 16)
        self.assertEqual(native["node_ids"], ["n1"])
        self.assertTrue(
            any(p.endswith(".layout-monitor.jsonl") for p in plan.collect["local"])
        )


def collected_fixture(root, sql_retries=10):
    c = config()
    c["load"] = dict(mode="closed", concurrency=1, processes=1)
    c["topology"]["metrics"] = dict(require_retry_identity=True)
    c["juicer"].update(enabled=True, hold_calibration=True)
    for name in ("n1", "client-0"):
        (root / f"{name}.provenance.json").write_text(
            json.dumps(
                dict(
                    source_revision="test-revision",
                    sha256=dict(cockroach="a" * 64, sql_client="b" * 64),
                )
            )
        )
    (root / "client-0.load.json").write_text(
        json.dumps(dict(rows=c["workload"]["keys"], seed=42, reset=True))
    )
    logs = [
        "JuicerInterceptor: HOLD CALIBRATION mode: nominated keys are observed, never held, delayed or sorted",
        "hold calibration: epoch=1 key=k spans=3 commits=2 aborts=1 p50=16us p99=16us unresolved=2 untracked=0 hist=1:1,17:2",
        "hold calibration: epoch=1 key=q spans=1 commits=1 aborts=0 p50=3967us p99=3967us unresolved=0 untracked=0 hist=4095:1",
        "hold calibration: epoch=1 pooled spans=4 commits=3 aborts=1 p50=16us p99=3967us",
    ]
    logs = [
        logs[0],
        "I700101 00:00:01.250000 epoch summary: epoch=0 holds_created=0 messages{intercepted=20 direct=20 queued=0 dropped=0 held=0}",
        *["I700101 00:00:01.500000 " + line for line in logs[1:]],
        "I700101 00:00:01.500000 epoch summary: epoch=1 holds_created=0 messages{intercepted=40 direct=40 queued=0 dropped=0 held=0}",
    ]
    (root / "n1.evidence.json").write_text(
        json.dumps(
            dict(
                valid=True,
                expected_enabled=True,
                interception_observed=True,
                calibration_observed=True,
                juicer_log_lines=logs,
            )
        )
    )
    event = dict(
        id=0,
        measured=True,
        terminal="committed",
        latency_ms=4,
        start_unix_ns=1_100_000_000,
        complete_unix_ns=1_104_000_000,
    )
    (root / "client-0.events.jsonl").write_text(json.dumps(event) + "\n")
    counts = dict(
        txn_commits=20,
        txn_restarts_txnaborted=3,
        txn_restarts_writetooold=2,
        txn_restarts_serializable=1,
        txn_restarts_txnpush=0,
        txn_restarts_readwithinuncertainty=4,
        txn_restarts_asyncwritefailure=0,
        txn_restarts_commitdeadlineexceeded=0,
        txn_restarts_exclusionviolation=0,
        txn_restarts_unknown=0,
        txn_restarts_sum=10,
        txn_restarts_count=7,
        sql_txn_auto_retry_count=sql_retries,
        txnwaitqueue_deadlocks_total=2,
        txn_server_side_1PC_success=2,
        txn_server_side_1PC_failure=3,
        txn_commits1PC=2,
    )
    snapshots = []
    for at, zero in ((1_010_000_000, True), (2_900_000_000, False)):
        snapshots.append(
            dict(
                url="http://localhost:28080/_status/vars",
                start_unix_ns=at,
                end_unix_ns=at + 1000,
                raw="\n".join(f"{k} {0 if zero else v}" for k, v in counts.items()),
            )
        )
    (root / "client-0.summary.json").write_text(
        json.dumps(
            dict(
                schema_version=1,
                complete=True,
                start_unix_ns=1_000_000_000,
                end_unix_ns=3_000_000_000,
                duration_seconds=2,
                config=dict(slots=1),
                commits=1,
                attempts=1,
                attempt_errors=0,
                client_retries=0,
                visible_40001=0,
                errors=0,
                dropped=0,
                counter_snapshots=snapshots,
            )
        )
    )
    return c


# These spellings follow the canonical juicer/hold_calibration.go,
# juicer/epoch_summary.go and root juicer_banner.go format strings. They are
# literals so the maintained standalone CRDB tests need no adjacent checkout.
SHARED_CALIBRATION_BANNER = (
    "juicer-config: spec=1.2 queued=Get,Set,GetForPut observed=Prepare,Commit,Abort "
    "cancel=withdraw level1Aborts=failed-reply,abort-message-keys "
    "level1Count=once-per-txn-key latePolicy=queue queueRemoval=drain "
    "skipQueueing=false skipTracking=false fifo=false holdCalibration=true"
)


def shared_epoch_line(epoch, unix_ns, messages=20):
    # The logger prefix is deliberately outside the measurement window:
    # the shared native observation timestamp is the epoch boundary.
    cumulative = messages * (epoch + 1)
    created = "2[k,q]" if epoch == 0 else "0[]"
    return (
        f"I700101 00:00:09.000000 juicer-epoch: unix_ns={unix_ns} "
        f"state_seq=2 live_keys=[k,q] epoch={epoch} "
        "managed=[k{W=0s r=1 n=10 arrivals=10 queued=0} "
        "q{W=0s r=1 n=10 arrivals=10 queued=0}] "
        f"queues live=2 max=2 mean=2.00 created={created} draining=0[] "
        "deleted=0[] cancelledRemoval=0[] drainPassthrough=0 | late queued=0 "
        "bypassed=0 | holds created=0 ended event=0 expiry=0 cancel=0 evicted=0 "
        "expiryShare=0.0% life p50<=0s p99<=0s live=0 | markers cancelled=0 | "
        f"msgs intercepted={messages} direct={messages} queued=0 dropped=0 "
        f"(cumulative intercepted={cumulative} direct={cumulative} queued=0 dropped=0 held=0)"
    )


def shared_collected_fixture(root):
    c = collected_fixture(root)
    path = root / "n1.evidence.json"
    proof = json.loads(path.read_text())
    proof.pop("calibration_observed")  # Require the real banner, not a flag.
    proof["juicer_log_lines"] = [
        SHARED_CALIBRATION_BANNER,
        shared_epoch_line(0, 1_250_000_000),
        "hold calibration: epoch=1 key=k spans=3 commits=2 aborts=1 p50=16us "
        "p99=16us unresolved=2 open=1 untracked=0 overflow=0 hist=1:1,17:2",
        "hold calibration: epoch=1 key=q (queue removed) spans=1 commits=1 "
        "aborts=0 p50=3967us p99=3967us unresolved=0 open=0 untracked=0 "
        "overflow=0 hist=4095:1",
        "hold calibration: epoch=1 pooled keys=2 spans=4 commits=3 aborts=1 "
        "p50=16us p99=3967us unresolved=2 open=1 untracked=0 overflow=0 "
        "(H candidate = pooled p99, rounded up to whole ms, at least 1 ms)",
        shared_epoch_line(1, 1_500_000_000),
    ]
    path.write_text(json.dumps(proof))
    return c


class SharedCalibrationLogs(unittest.TestCase):
    def meter(self, root):
        summary = json.loads((root / "client-0.summary.json").read_text())
        return calibration_report(root, [summary])

    def rewrite(self, root, transform):
        path = root / "n1.evidence.json"
        proof = json.loads(path.read_text())
        proof["juicer_log_lines"] = transform(proof["juicer_log_lines"])
        path.write_text(json.dumps(proof))

    def test_actual_shared_formats_work_through_adapter(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            c = shared_collected_fixture(root)
            result = CRDBAdapter().parse_results(c, root)
            self.assertTrue(result["validity"]["valid"], result["validity"])
            meter = result["diagnostics"]["hold_calibration"]
            self.assertTrue(meter["valid"], meter)
            self.assertEqual((meter["spans"], meter["commits"], meter["aborts"]), (4, 3, 1))
            self.assertEqual((meter["p50_us"], meter["p99_us"]), (16, 3967))
            self.assertEqual(meter["recommended_max_hold_ms"], 4)
            self.assertEqual(meter["overflow"], 0)
            self.assertEqual(meter["unresolved"], 2)
            self.assertEqual(len(meter["sources"]), 2)  # Never double-count pooled.
            self.assertEqual(meter["sources"][0]["open"], 1)
            self.assertEqual(meter["sources"][1]["key"], "q (queue removed)")
            behavior = meter["observe_only_behavior"][0]
            self.assertEqual((behavior["start_unix_ns"], behavior["end_unix_ns"]), (1_250_000_000, 1_500_000_000))

    def test_shared_boundaries_use_intersection_of_client_windows(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            shared_collected_fixture(root)
            summary = json.loads((root / "client-0.summary.json").read_text())
            second = dict(summary, start_unix_ns=1_300_000_000)
            meter = calibration_report(root, [summary, second])
            self.assertFalse(meter["valid"])
            self.assertEqual(meter["spans"], 0)
            self.assertEqual(meter["excluded_epoch_key_rows"], 2)
            self.assertIsNone(meter["p99_us"])
            self.assertIsNone(meter["recommended_max_hold_ms"])

    def test_shared_incomplete_or_missing_behavior_is_not_zero(self):
        replacements = (
            ("holds created=0 ended", "holds ended"),
            ("p99<=0s live=0", "p99<=0s"),
            (" dropped=0 held=0)", " dropped=0)"),
            ("unix_ns=1500000000 ", ""),
        )
        for old, new in replacements:
            with self.subTest(missing=old), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                shared_collected_fixture(root)
                self.rewrite(root, lambda lines: [line.replace(old, new) for line in lines])
                meter = self.meter(root)
                self.assertFalse(meter["valid"], meter)
                self.assertIsNone(meter["recommended_max_hold_ms"])

    def test_shared_actual_holds_queueing_and_lost_tracking_are_invalid(self):
        replacements = (
            ("holds created=0 ended", "holds created=1 ended"),
            ("p99<=0s live=0", "p99<=0s live=1"),
            ("msgs intercepted=20 direct=20 queued=0", "msgs intercepted=20 direct=19 queued=1"),
            (" dropped=0 held=0)", " dropped=0 held=1)"),
            ("untracked=0 overflow=0 hist=1:1,17:2", "untracked=1 overflow=0 hist=1:1,17:2"),
        )
        for old, new in replacements:
            with self.subTest(violation=old), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                shared_collected_fixture(root)
                self.rewrite(root, lambda lines: [line.replace(old, new) for line in lines])
                meter = self.meter(root)
                self.assertFalse(meter["valid"], meter)
                self.assertEqual(meter["spans"], 4)

    def test_shared_overflow_is_pooled_and_only_finite_p99_is_eligible(self):
        for overflow, finite in ((1, True), (2, False)):
            with self.subTest(overflow=overflow), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                shared_collected_fixture(root)

                def replace_rows(lines):
                    rows = []
                    totals = (
                        "spans=100 commits=99 aborts=1 p50=991us "
                        f"p99={991 if finite else 1048576}us unresolved=0 "
                        f"open=0 untracked=0 overflow={overflow}"
                    )
                    for line in lines:
                        if "key=k " in line:
                            line = (
                                f"hold calibration: epoch=1 key=k {totals} "
                                f"hist=1023:{100-overflow},>top:{overflow}"
                            )
                        elif "key=q " in line:
                            line = (
                                "hold calibration: epoch=1 key=q spans=0 commits=0 "
                                "aborts=0 p50=0us p99=0us unresolved=0 open=0 "
                                "untracked=0 overflow=0 hist=-"
                            )
                        elif "pooled keys=" in line:
                            line = (
                                f"hold calibration: epoch=1 pooled keys=2 {totals} "
                                "(H candidate = pooled p99, rounded up to whole ms, at least 1 ms)"
                            )
                        elif "juicer-epoch:" in line:
                            epoch = 0 if "epoch=0 " in line else 1
                            line = shared_epoch_line(
                                epoch, 1_250_000_000 if epoch == 0 else 1_500_000_000,
                                messages=100,
                            )
                        rows.append(line)
                    return rows

                self.rewrite(root, replace_rows)
                meter = self.meter(root)
                self.assertEqual(meter["spans"], 100)
                self.assertEqual(meter["overflow"], overflow)
                self.assertEqual(meter["valid"], finite, meter)
                self.assertEqual(meter["p99_us"], 991 if finite else None)
                self.assertEqual(meter["recommended_max_hold_ms"], 1 if finite else None)

    def test_shared_explicit_overflow_must_match_histogram(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            shared_collected_fixture(root)
            self.rewrite(root, lambda lines: [line.replace("overflow=0 hist=1:1,17:2", "overflow=1 hist=1:1,17:2") for line in lines])
            with self.assertRaisesRegex(ValueError, "overflow counts disagree"):
                self.meter(root)

    def test_shared_zero_spans_or_malformed_key_row_cannot_certify(self):
        for empty in (True, False):
            with self.subTest(empty=empty), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                shared_collected_fixture(root)
                def rewrite_rows(lines):
                    if empty:
                        return [line for line in lines if "hold calibration:" not in line]
                    return [line.replace("open=1 untracked=0", "open=1 missing_untracked=0") for line in lines]
                self.rewrite(root, rewrite_rows)
                meter = self.meter(root)
                self.assertFalse(meter["valid"], meter)
                self.assertIsNone(meter["recommended_max_hold_ms"])

    def test_shared_duplicate_rows_deduplicate_but_conflicting_boundaries_fail(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            shared_collected_fixture(root)
            self.rewrite(root, lambda lines: lines + [lines[2], lines[-1]])
            self.assertEqual(self.meter(root)["spans"], 4)
            self.rewrite(root, lambda lines: lines + [lines[-1].replace("holds created=0 ended", "holds created=1 ended")])
            with self.assertRaisesRegex(ValueError, "Conflicting native epoch-summary"):
                self.meter(root)

    def test_shared_redaction_and_explicit_disabled_declaration(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            shared_collected_fixture(root)
            self.rewrite(root, lambda lines: [line.replace("epoch=1", "epoch=‹1›").replace("open=1", "open=‹1›") for line in lines])
            self.assertTrue(self.meter(root)["valid"])
            self.rewrite(root, lambda lines: [line.replace("holdCalibration=true", "holdCalibration=false") for line in lines])
            self.assertFalse(self.meter(root)["valid"])


class EvaluationMetrics(unittest.TestCase):
    def test_report_keeps_cause_sum_identity_distinct_from_histogram_transaction_count(
        self,
    ):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            c = collected_fixture(root)
            result = CRDBAdapter().parse_results(c, root)
            self.assertTrue(result["validity"]["valid"], result["validity"])
            report = result["diagnostics"]["evaluation"]
            self.assertEqual(report["sql_retry_cause_identity"]["kv_cause_sum"], 10)
            self.assertEqual(
                report["sql_retry_cause_identity"]["histogram_transaction_count"], 7
            )
            self.assertEqual(
                report["sql_retry_cause_identity"]["residual_sql_minus_kv"], 0
            )
            self.assertTrue(report["sql_retry_cause_identity"]["identity_holds"])
            self.assertEqual(report["cc_restarts_per_commit"]["value"], 0.3)
            self.assertEqual(report["one_pc_fallback"]["numerator"], 3)
            self.assertEqual(report["one_pc_fallback"]["denominator"], 5)
            self.assertEqual(report["uncertainty_per_commit"]["value"], 0.2)

    def test_evaluation_requires_each_server_cause_not_only_aggregate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            c = collected_fixture(root)
            path = root / "client-0.summary.json"
            summary = json.loads(path.read_text())
            extra = json.loads(json.dumps(summary["counter_snapshots"]))
            for row in extra:
                row["url"] = "http://localhost:28081/_status/vars"
                row["raw"] = "\n".join(
                    line
                    for line in row["raw"].splitlines()
                    if not line.startswith("txn_restarts_unknown ")
                )
            summary["counter_snapshots"].extend(extra)
            path.write_text(json.dumps(summary))
            result = CRDBAdapter().parse_results(c, root)
            missing = result["diagnostics"]["evaluation"]["missing_counters_by_server"]
            self.assertEqual(missing[extra[0]["url"]], ["txn_restarts_unknown"])

    def test_calibration_merges_key_histograms_not_epoch_quantiles(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            c = collected_fixture(root)
            result = CRDBAdapter().parse_results(c, root)
            meter = result["diagnostics"]["hold_calibration"]
            self.assertEqual(meter["spans"], 4)
            self.assertEqual(meter["p50_us"], 16)
            self.assertEqual(meter["p99_us"], 3967)
            self.assertEqual(meter["recommended_max_hold_ms"], 4)
            self.assertEqual(meter["unresolved"], 2)

    def test_calibration_excludes_initialization_epochs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            c = collected_fixture(root)
            path = root / "n1.evidence.json"
            proof = json.loads(path.read_text())
            proof["juicer_log_lines"].append(
                "I700101 00:00:00.750000 hold calibration: epoch=0 key=setup spans=100 commits=100 aborts=0 p50=49151us p99=49151us unresolved=0 untracked=0 hist=51199:100"
            )
            path.write_text(json.dumps(proof))
            result = CRDBAdapter().parse_results(c, root)
            self.assertEqual(result["diagnostics"]["hold_calibration"]["spans"], 4)

    def test_calibration_refuses_any_actual_hold_in_observe_only_epochs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            c = collected_fixture(root)
            path = root / "n1.evidence.json"
            proof = json.loads(path.read_text())
            proof["juicer_log_lines"] = [
                line.replace("epoch=1 holds_created=0", "epoch=1 holds_created=1")
                for line in proof["juicer_log_lines"]
            ]
            path.write_text(json.dumps(proof))
            self.assertFalse(CRDBAdapter().parse_results(c, root)["validity"]["valid"])

    def test_retry_cause_mismatch_is_reported_and_strict_policy_rejects(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            c = collected_fixture(root, sql_retries=11)
            result = CRDBAdapter().parse_results(c, root)
            self.assertFalse(result["validity"]["valid"])
            identity = result["diagnostics"]["evaluation"]["sql_retry_cause_identity"]
            self.assertEqual(identity["residual_sql_minus_kv"], 1)
            self.assertFalse(identity["identity_holds"])
            c["topology"]["metrics"]["require_retry_identity"] = False
            self.assertTrue(CRDBAdapter().parse_results(c, root)["validity"]["valid"])

    def test_layout_report_rejects_drift_even_when_snapshot_claims_valid(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            c = collected_fixture(root)
            c["workload"]["keys"] = 20
            (root / "client-0.load.json").write_text(
                json.dumps(dict(rows=20, seed=42, reset=True))
            )
            c["topology"]["layout"] = dict(
                hot_ranks=2,
                lease_nodes=["n1"],
                disable_rebalancing=True,
                monitor_interval_seconds=0.5,
            )
            rows = [
                dict(
                    range_id=501 + i,
                    logical_start_hex=f"000{i+1}",
                    logical_end_hex=f"000{i+2}",
                    replicas=[5],
                    voting_replicas=[5],
                    non_voting_replicas=[],
                    learner_replicas=[],
                    lease_holder=5,
                )
                for i in range(2)
            ]
            hot = [
                dict(
                    rank=rank,
                    key=key,
                    target_node="n1",
                    range_id=501 + rank,
                    lease_store=5,
                )
                for rank, key in enumerate([0, 13])
            ]
            settings = {
                "kv.allocator.load_based_rebalancing": "off",
                "kv.lease_queue.enabled": "false",
            }
            initial = dict(
                valid=True,
                start_unix_ns=500_000_000,
                nodes=[dict(name="n1", node_id=41, store_id=5)],
                ranges=rows,
                desired=dict(hot_keys=hot),
                settings=settings,
            )
            (root / "client-0.layout.json").write_text(json.dumps(initial))
            final = dict(
                valid=True,
                start_unix_ns=3_100_000_000,
                end_unix_ns=3_200_000_000,
                ranges=rows,
                hot_keys=hot,
                settings=settings,
            )
            (root / "client-0.layout-final.json").write_text(json.dumps(final))
            monitor = dict(
                final, start_unix_ns=1_900_000_000, end_unix_ns=2_000_000_000
            )
            (root / "client-0.layout-monitor.jsonl").write_text(
                json.dumps(monitor) + "\n"
            )
            self.assertTrue(CRDBAdapter().parse_results(c, root)["validity"]["valid"])
            final["ranges"][0]["lease_holder"] = 999
            (root / "client-0.layout-final.json").write_text(json.dumps(final))
            self.assertFalse(CRDBAdapter().parse_results(c, root)["validity"]["valid"])


class EvaluationArtifacts(unittest.TestCase):
    def test_card_export_uses_server_abort_denominator_and_labels_calibration(self):
        from crdb_bench.evaluation import export_report

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            evidence = root / "collected"
            evidence.mkdir()
            c = collected_fixture(evidence)
            result = CRDBAdapter().parse_results(c, evidence)
            out = root / "report"
            exported = export_report(
                [
                    dict(
                        cell_id="known",
                        repeat=0,
                        batch_id="fixture",
                        environment_hash="fixture-environment",
                        config=c,
                        result=result,
                        source_result_hash="fixture",
                        source_batch="fixture",
                    )
                ],
                out,
            )
            self.assertEqual(exported["cells"][0]["cc_restarts_per_commit"], 0.3)
            self.assertEqual(exported["cells"][0]["throughput_tps"], 0.5)
            self.assertFalse(exported["cells"][0]["comparison_eligible"])
            self.assertTrue((out / "figure-7.8b.svg").is_file())
            self.assertTrue((out / "retry-layers.csv").is_file())
            self.assertEqual(
                json.loads((out / "hold-calibration.json").read_text())["known"][
                    "recommended_max_hold_ms"
                ],
                4,
            )
            layers = (out / "retry-layers.csv").read_text()
            self.assertIn("one_pc_fallback", layers)


if __name__ == "__main__":
    unittest.main()
