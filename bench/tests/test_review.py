"""Review regressions at the collected-result, selection and report CLI seams."""

import contextlib
import copy
import csv
import io
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET

from experiment_common.config import logical_plan_id
from experiment_common.analysis import select_workpoint
from experiment_common.io import digest, file_hash, write_json
from crdb_bench import CRDBAdapter
from crdb_bench.evaluation import main, export_report
from test_evaluation import collected_fixture


def report_batch(batch, environment, loads):
    records = []
    for load in loads:
        for repeat in (0, 1):
            attempt = batch / f"cells/load-{load}-r{repeat}"
            collected = attempt / "collected"
            collected.mkdir(parents=True)
            config = collected_fixture(collected)
            config["load"]["concurrency"] = load
            path = collected / "client-0.summary.json"
            summary = json.loads(path.read_text())
            summary["config"]["slots"] = load
            summary["commits"] = summary["attempts"] = load
            events = collected / "client-0.events.jsonl"
            event = json.loads(events.read_text())
            events.write_text(
                "".join(json.dumps({**event, "id": i}) + "\n" for i in range(load))
            )
            path.write_text(json.dumps(summary))
            result = CRDBAdapter().parse_results(config, collected)
            if not result["validity"]["valid"]:
                raise AssertionError(result["validity"])
            write_json(attempt / "result.json", result)
            files = [
                dict(path=p.name, size=p.stat().st_size, sha256=file_hash(p))
                for p in collected.iterdir()
            ]
            write_json(
                collected / "manifest.json", dict(verified=True, errors=[], files=files)
            )
            records.append(
                dict(
                    cell_id=f"load-{load}-r{repeat}",
                    repeat=repeat,
                    status="success",
                    environment_hash=environment,
                    config=config,
                    config_hash=digest(config),
                    result_hash=digest(result),
                    attempt_path=str(attempt.relative_to(batch)),
                )
            )
    frozen = {
        **config,
        "load": {**config["load"], "concurrency": 1},
        "matrix": {"axes": {"load.concurrency": [1, 2, 3, 4]}},
        "repeats": 2,
    }
    write_json(
        batch / "batch.json",
        dict(
            batch_id=batch.name,
            config=frozen,
            frozen_config_hash=digest(frozen),
            logical_plan_id=logical_plan_id(frozen),
        ),
    )
    write_json(batch / "summary.json", dict(cells=records))


class ReviewRegressions(unittest.TestCase):
    def test_measured_capacity_wait_prevents_automatic_plateau_selection(self):
        rules = dict(
            version=1,
            min_throughput_fraction=0.9,
            max_p99_growth=2,
            max_throughput_gain=0.1,
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = collected_fixture(root)
            config["load"] = dict(
                mode="open", rate=20, arrival="regular", max_inflight=1, processes=1
            )
            path = root / "client-0.summary.json"
            summary = json.loads(path.read_text())
            for count, elapsed in ((3, 800000), (0, 800000), (0, 0)):
                with self.subTest(count=count, elapsed=elapsed):
                    summary.update(capacity_wait_count=count, capacity_wait_ns=elapsed)
                    path.write_text(json.dumps(summary))
                    result = CRDBAdapter().parse_results(config, root)
                    self.assertTrue(result["validity"]["valid"], result["validity"])
                    diagnostics = result["diagnostics"]
                    self.assertEqual(diagnostics["capacity_wait_count"], count)
                    self.assertEqual(diagnostics["capacity_wait_ns"], elapsed)
                    points = [
                        dict(load=10, throughput_tps=100, p99_ms=2),
                        dict(
                            load=20,
                            throughput_tps=190,
                            p99_ms=3,
                            capacity_limited=diagnostics.get("capacity_limited", False),
                        ),
                        dict(load=30, throughput_tps=200, p99_ms=12),
                    ]
                    if count or elapsed:
                        with self.assertRaisesRegex(ValueError, "capacity-limited"):
                            select_workpoint(points, rules)
                    else:
                        self.assertIs(diagnostics["capacity_limited"], False)
                        self.assertEqual(
                            select_workpoint(points, rules)["selected_load"], 20
                        )
                    self.assertEqual(
                        diagnostics["evaluation"]["one_pc_fallback"]["denominator"], 5
                    )
                    self.assertEqual(
                        diagnostics["evaluation"]["cc_restarts_per_commit"]["value"],
                        0.3,
                    )

    def test_null_restart_ratios_break_lines_and_remain_null(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            records = []
            for load in (1, 2, 3):
                collected = root / str(load)
                collected.mkdir()
                config = collected_fixture(collected)
                path = collected / "client-0.summary.json"
                summary = json.loads(path.read_text())
                if load == 2:
                    for row in summary["counter_snapshots"]:
                        row["raw"] = row["raw"].replace(
                            "txn_commits 20", "txn_commits 0"
                        )
                    path.write_text(json.dumps(summary))
                result = CRDBAdapter().parse_results(config, collected)
                self.assertTrue(result["validity"]["valid"], result["validity"])
                config["load"]["concurrency"] = load
                records.append(
                    dict(
                        cell_id=str(load),
                        repeat=0,
                        batch_id="fixture",
                        environment_hash="a" * 64,
                        config=config,
                        result=result,
                    )
                )
            out = root / "report"
            report = export_report(records, out)
            self.assertIsNone(report["cells"][1]["cc_restarts_per_commit"])
            ratio = report["evidence"]["2"]["evaluation"]["cc_restarts_per_commit"]
            self.assertEqual((ratio["numerator"], ratio["denominator"]), (6, 0))
            self.assertIsNotNone(ratio["reason"])
            svg = ET.parse(out / "figure-7.8b-cc_restarts_per_commit.svg")
            segments = svg.findall(".//{http://www.w3.org/2000/svg}polyline")
            self.assertEqual(
                [len(line.attrib["points"].split()) for line in segments], [1, 1]
            )
            report = export_report([records[1]], out)
            self.assertIsNone(report["plots"]["cc_restarts_per_commit"]["path"])
            self.assertTrue(report["plots"]["cc_restarts_per_commit"]["reason"])
            self.assertFalse((out / "figure-7.8b-cc_restarts_per_commit.svg").exists())

    def test_wait_on_second_client_limits_the_global_point(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = collected_fixture(root)
            config["load"] = dict(
                mode="open", rate=20, arrival="regular", max_inflight=2, processes=2
            )
            first = root / "client-0.summary.json"
            summary = json.loads(first.read_text())
            summary.update(capacity_wait_count=0, capacity_wait_ns=0)
            first.write_text(json.dumps(summary))
            second = copy.deepcopy(summary)
            second.update(capacity_wait_count=2, capacity_wait_ns=50000)
            (root / "client-1.summary.json").write_text(json.dumps(second))
            for suffix in ("events.jsonl", "provenance.json"):
                (root / f"client-1.{suffix}").write_bytes(
                    (root / f"client-0.{suffix}").read_bytes()
                )
            result = CRDBAdapter().parse_results(config, root)
            self.assertTrue(result["validity"]["valid"], result["validity"])
            self.assertIs(result["diagnostics"]["capacity_limited"], True)
            self.assertEqual(result["diagnostics"]["capacity_wait_count"], 2)
            self.assertEqual(result["diagnostics"]["capacity_wait_ns"], 50000)

    def test_cli_keeps_environment_and_repeat_curves_separate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report_batch(root / "first", "a" * 64, (1, 2))
            report_batch(root / "continuation", "b" * 64, (3, 4))
            output = root / "report"
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(
                    main(
                        [
                            "--batch",
                            str(root / "first"),
                            "--batch",
                            str(root / "continuation"),
                            "--output",
                            str(output),
                        ]
                    ),
                    0,
                )
            report = json.loads((output / "evaluation-report.json").read_text())
            identities = {(r["environment_hash"], r["repeat"]) for r in report["cells"]}
            self.assertEqual(
                identities, {("a" * 64, 0), ("a" * 64, 1), ("b" * 64, 0), ("b" * 64, 1)}
            )
            with (output / "figure-7.8b.csv").open() as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(
                {r["environment_hash"] for r in rows}, {"a" * 64, "b" * 64}
            )
            svg = ET.parse(output / "figure-7.8b.svg")
            lines = svg.findall(".//{http://www.w3.org/2000/svg}polyline")
            # Three panels, four independent environment/repeat curves, two loads each.
            self.assertEqual(len(lines), 12)
            self.assertTrue(
                all(len(line.attrib["points"].split()) == 2 for line in lines)
            )
            labels = {line.attrib["data-series"] for line in lines}
            self.assertEqual(len(labels), 4)
            self.assertTrue(
                all("environment=" in name and "repeat=" in name for name in labels)
            )


if __name__ == "__main__":
    unittest.main()
