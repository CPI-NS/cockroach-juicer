"""Export CRDB evaluation-card inputs using achieved throughput and named layers.

This is system analysis, not a second lifecycle or experiment runner.
"""

import argparse
import csv
import hashlib
import json
from pathlib import Path
import subprocess
import xml.etree.ElementTree as ET

from experiment_common.analysis import merge_results
from experiment_common.research_plot import plot_series
from . import CRDBAdapter


def _write(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _figure(path, cells):
    groups = {}
    for cell in cells:
        key = (cell["series"], cell["environment_hash"], cell["repeat"])
        groups.setdefault(key, []).append(cell)
    panels = []
    outputs = {}
    for metric, title in (
        ("p99_ms", "p99 (ms)"),
        ("p50_ms", "p50 (ms)"),
        ("cc_restarts_per_commit", "CC restarts / server KV commit"),
    ):
        series = []
        for (condition, environment, repeat), rows in sorted(groups.items()):
            name = f"{condition}; environment={environment}; repeat={repeat}"
            points = [
                (r["throughput_tps"], r[metric])
                for r in sorted(rows, key=lambda r: r["configured_load"])
                if r["throughput_tps"] is not None
            ]
            series.append((name, points))
        panel = path.with_name(f"figure-7.8b-{metric}.svg")
        if not any(y is not None for _, points in series for _, y in points):
            panel.unlink(missing_ok=True)
            outputs[metric] = dict(
                path=None,
                reason="No available throughput/metric pairs; nulls remain in JSON/CSV",
            )
            continue
        plot_series(
            panel,
            f"CRDB Figure 7.8b inputs: {title}",
            series,
            xlabel="Achieved throughput (transactions/s)",
            ylabel=title,
        )
        panels.append(ET.parse(panel).getroot())
        outputs[metric] = dict(path=panel.name, reason=None)
    # Compose native metric panels; all axes, lines, null breaks and legends are common-owned.
    width = sum(int(panel.attrib["width"]) for panel in panels)
    height = max((int(panel.attrib["height"]) for panel in panels), default=0)
    composite = ET.Element(
        "{http://www.w3.org/2000/svg}svg",
        width=str(width),
        height=str(height),
        viewBox=f"0 0 {width} {height}",
    )
    left = 0
    for panel in panels:
        panel.set("x", str(left))
        panel.set("y", "0")
        composite.append(panel)
        left += int(panel.attrib["width"])
    ET.ElementTree(composite).write(path, encoding="unicode")
    return outputs


def export_report(records, output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    cells = []
    evidence = {}
    layers = []
    calibrations = {}
    layouts = {}
    for record in records:
        c = record["config"]
        result = record["result"]
        if not result["validity"]["valid"]:
            raise ValueError("Cannot export an invalid CRDB measurement")
        cell_id = record["cell_id"]
        d = result["diagnostics"]
        evaluation = d["evaluation"]
        m = result["metrics"]
        condition = dict(
            workload={k: v for k, v in c["workload"].items() if k != "seed"},
            mode=c["load"]["mode"],
            arrival=c["load"].get("arrival") if c["load"]["mode"] == "open" else None,
            topology=c["topology"],
        )
        condition_id = hashlib.sha256(
            json.dumps(condition, sort_keys=True).encode()
        ).hexdigest()[:10]
        setting_id = hashlib.sha256(
            json.dumps(c["juicer"], sort_keys=True).encode()
        ).hexdigest()[:8]
        calibration = c["juicer"].get("hold_calibration", False)
        treatment = (
            "observe-only calibration"
            if calibration
            else ("Juicer on" if c["juicer"]["enabled"] else "Juicer off")
        )
        row = dict(
            cell_id=cell_id,
            batch_id=record["batch_id"],
            repeat=record["repeat"],
            environment_hash=record["environment_hash"],
            configured_load=c["load"][
                "rate" if c["load"]["mode"] == "open" else "concurrency"
            ],
            condition_id=condition_id,
            series=f"{condition_id}; {c['load']['mode']}; {treatment}; setting={setting_id}",
            comparison_eligible=not calibration,
            throughput_tps=m["throughput_tps"],
            p50_ms=m["latency_ms"]["p50"],
            p99_ms=m["latency_ms"]["p99"],
            cc_restarts_per_commit=evaluation["cc_restarts_per_commit"]["value"],
            sql_auto_retries_per_commit=evaluation["sql_auto_retries_per_commit"][
                "value"
            ],
            uncertainty_per_commit=evaluation["uncertainty_per_commit"]["value"],
            one_pc_fallback=evaluation["one_pc_fallback"]["value"],
        )
        cells.append(row)
        for name in (
            "cc_restarts_per_commit",
            "sql_auto_retries_per_commit",
            "uncertainty_per_commit",
            "deadlocks_per_commit",
            "one_pc_fallback",
            "visible_40001_per_client_attempt",
            "terminal_40001_per_client_terminal",
        ):
            metric = evaluation[name]
            layers.append(
                dict(
                    cell_id=cell_id,
                    batch_id=record["batch_id"],
                    environment_hash=record["environment_hash"],
                    repeat=record["repeat"],
                    layer=name,
                    value=metric["value"],
                    numerator=metric["numerator"],
                    denominator=metric["denominator"],
                    window=json.dumps(metric.get("window"), sort_keys=True),
                    denominator_population=(
                        "1PC batches"
                        if name == "one_pc_fallback"
                        else ("client" if "client" in name else "server KV commits")
                    ),
                )
            )
        server_commits = evaluation["cc_restarts_per_commit"]["denominator"]
        for cause, numerator in evaluation["kv_restart_causes"].items():
            layers.append(
                dict(
                    cell_id=cell_id,
                    batch_id=record["batch_id"],
                    environment_hash=record["environment_hash"],
                    repeat=record["repeat"],
                    layer="kv_restart_" + cause,
                    value=(
                        numerator / server_commits
                        if numerator is not None and server_commits
                        else None
                    ),
                    numerator=numerator,
                    denominator=server_commits,
                    window="server_counter_scrapes",
                    denominator_population="server KV commits",
                )
            )
        evidence[cell_id] = dict(
            environment_hash=record["environment_hash"],
            repeat=record["repeat"],
            source_batch=record.get("source_batch"),
            source_result_hash=record.get("source_result_hash"),
            derived_result_sha256=hashlib.sha256(
                json.dumps(result, sort_keys=True).encode()
            ).hexdigest(),
            evaluation=evaluation,
            server_windows=d["server_windows"],
            measurement=result["measurement"],
            files=result["files"],
        )
        if "hold_calibration" in d:
            calibrations[cell_id] = d["hold_calibration"]
        if "layout" in d:
            layouts[cell_id] = d["layout"]
    if not cells:
        raise ValueError("No valid CRDB cells to export")
    package = Path(__file__).parent
    revision = subprocess.run(
        ["git", "-C", str(package), "rev-parse", "HEAD"], capture_output=True, text=True
    )
    report = dict(
        schema_version=1,
        cards=["T-CR-00", "T-CR-01"],
        cells=cells,
        evidence=evidence,
        parser_revision=revision.stdout.strip() if revision.returncode == 0 else None,
        parser_source_sha256={
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(package.glob("*.py"))
        },
        figure_semantics="Achieved throughput and independently defined numerator/denominator windows; per-cell points, not a paired-block estimator",
        calibration=calibrations,
        layout=layouts,
    )
    report["plots"] = _figure(output / "figure-7.8b.svg", cells)
    _write(output / "evaluation-report.json", report)
    _write(output / "hold-calibration.json", calibrations)
    _write(output / "layout.json", layouts)
    for name, rows in [("figure-7.8b.csv", cells), ("retry-layers.csv", layers)]:
        with (output / name).open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]), lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Export verified CRDB layout, calibration and Figure 7.8b metric inputs."
    )
    parser.add_argument("--batch", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    records = []
    for candidate in merge_results(args.batch).values():
        if candidate["config"]["system"] != "crdb":
            raise ValueError("Only CRDB batches are supported")
        collected = (
            Path(candidate["batch_path"]) / candidate["attempt_path"] / "collected"
        )
        # Recompute into a separate derived report; original raw/batch results stay immutable.
        result = CRDBAdapter().parse_results(candidate["config"], collected)
        records.append(
            dict(
                cell_id=candidate["cell_id"],
                repeat=candidate["repeat"],
                batch_id=candidate["batch_id"],
                environment_hash=candidate["environment_hash"],
                config=candidate["config"],
                result=result,
                source_batch=candidate["batch_path"],
                source_result_hash=candidate["result_hash"],
            )
        )
    report = export_report(records, args.output)
    print(
        json.dumps(dict(cells=len(report["cells"]), output=str(args.output.resolve())))
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
