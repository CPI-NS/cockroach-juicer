"""Native event timestamp stability fixtures, without database execution."""
import json
import csv
import shutil
import subprocess
import sys

import pytest
from experiment_common.config import logical_plan_id
from experiment_common.io import digest, file_hash, read_json, write_json
from crdb_bench import CRDBAdapter
from test_evaluation import collected_fixture


def policy():
    # Synthetic test bindings, not adopted scientific thresholds.
    return dict(criterion="completion_bin_spread_v1", bin_seconds=.5, min_bins=4,
                min_terminals_per_bin=1, min_commit_rate_ratio=.5, max_p99_ratio=2,
                stop_on_unstable_off=True)


def fixture(root, latencies=(4, 4, 4, 4), counts=(1, 1, 1, 1), monotonic=False):
    config = collected_fixture(root)
    config["juicer"] = dict(enabled=False)
    config["load"] = dict(mode="open", rate=10, max_inflight=1, arrival="regular", processes=1)
    config["measurement"]["stability"] = policy()
    if monotonic:
        config["measurement"]["warmup_seconds"] = .2
    evidence = read_json(root / "n1.evidence.json")
    evidence.update(expected_enabled=False, interception_observed=False)
    write_json(root / "n1.evidence.json", evidence)
    rows = []
    for index, (latency, count) in enumerate(zip(latencies, counts)):
        for sample in range(count):
            complete = int((.3 + .5 * index + sample * .01) * 1e9)
            event = dict(id=len(rows), measured=True, terminal="committed", latency_ms=latency,
                         start_unix_ns=1_000_000_000 + complete - int(latency * 1e6),
                         complete_unix_ns=1_000_000_000 + complete)
            if monotonic:
                warmup = int(config["measurement"].get("warmup_seconds", 0) * 1e9)
                event.update(start_offset_ns=warmup + complete - int(latency * 1e6),
                             complete_offset_ns=warmup + complete)
            rows.append(event)
    summary = read_json(root / "client-0.summary.json")
    summary.update(commits=len(rows), attempts=len(rows), errors=0)
    write_json(root / "client-0.summary.json", summary)
    (root / "client-0.events.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows)
        + json.dumps(dict(id=999, measured=False, terminal="unfinished", latency_ms=999999)) + "\n")
    return config


@pytest.mark.parametrize("monotonic", [False, True])
def test_native_bins_preserve_population_numerators_and_stable_decision(tmp_path, monotonic):
    config = fixture(tmp_path, monotonic=monotonic)
    result = CRDBAdapter().parse_results(config, tmp_path)
    assert result["validity"]["valid"], result["validity"]
    stability = result["diagnostics"]["stability"]
    assert stability["status"] == "stable"
    assert [b["commits"] for b in stability["bins"]] == [1, 1, 1, 1]
    assert [b["commit_rate_tps"] for b in stability["bins"]] == [2, 2, 2, 2]
    assert [b["p99_ms"] for b in stability["bins"]] == [4, 4, 4, 4]
    assert stability["commit_rate_ratio"] == dict(numerator=2, denominator=2, value=1)
    assert stability["p99_ratio"] == dict(numerator=4, denominator=4, value=1)
    assert stability["policy"] == policy()
    assert stability["sources"][0]["clock"] == ("native_monotonic_offset" if monotonic else "native_window_relative_wall")
    assert "unfinished" in stability["population"]


@pytest.mark.parametrize("latencies,counts,decision", [
    ((4, 4, 4, 20), (1, 1, 1, 1), "unstable"),
    ((4, 4, 4, 4), (4, 4, 1, 1), "unstable"),
    ((4, 4, 4, 4), (1, 1, 0, 0), "unstable"),
    ((0, 0, 0, 0), (1, 1, 1, 1), "insufficient"),
    ((4, 8, 4, 8), (2, 2, 1, 1), "stable"),
])
def test_spread_or_missing_quantile_denominator_never_corrupts_a_measurement(tmp_path, latencies, counts, decision):
    config = fixture(tmp_path, latencies, counts)
    result = CRDBAdapter().parse_results(config, tmp_path)
    assert result["validity"]["valid"], result["validity"]
    stability = result["diagnostics"]["stability"]
    assert stability["status"] == decision
    assert bool(stability["reasons"]) is (decision != "stable")


def test_insufficient_sample_binding_and_unbound_policy_are_explicit(tmp_path):
    config = fixture(tmp_path)
    config["measurement"]["stability"]["min_terminals_per_bin"] = 2
    result = CRDBAdapter().parse_results(config, tmp_path)
    assert result["validity"]["valid"]
    assert result["diagnostics"]["stability"]["status"] == "insufficient"
    del config["measurement"]["stability"]
    result = CRDBAdapter().parse_results(config, tmp_path)
    assert result["diagnostics"]["stability"]["status"] == "not_assessed"


def test_two_client_windows_pool_events_before_quantiles_without_wall_clock_pairing(tmp_path):
    config = fixture(tmp_path)
    config["load"].update(max_inflight=2, processes=2)
    shutil.copyfile(tmp_path / "client-0.provenance.json", tmp_path / "client-1.provenance.json")
    summary = read_json(tmp_path / "client-0.summary.json")
    shift = 10_000_000_000
    summary["start_unix_ns"] += shift
    summary["end_unix_ns"] += shift
    summary["counter_snapshots"] = []
    write_json(tmp_path / "client-1.summary.json", summary)
    events = []
    for line in (tmp_path / "client-0.events.jsonl").read_text().splitlines():
        row = json.loads(line)
        if not row["measured"]:
            continue
        row["complete_unix_ns"] += shift
        row["start_unix_ns"] = row["complete_unix_ns"] - 8_000_000
        row["latency_ms"] = 8
        events.append(row)
    (tmp_path / "client-1.events.jsonl").write_text("".join(json.dumps(r) + "\n" for r in events))
    result = CRDBAdapter().parse_results(config, tmp_path)
    assert result["validity"]["valid"], result["validity"]
    stability = result["diagnostics"]["stability"]
    assert [b["commits"] for b in stability["bins"]] == [2] * 4
    assert [b["commit_rate_tps"] for b in stability["bins"]] == [4] * 4
    assert [b["p99_ms"] for b in stability["bins"]] == [8] * 4  # Not average(client p99)=6.
    assert len(stability["sources"]) == 2 and len(stability["process_bins"]) == 8


def test_public_evaluation_cli_retains_unstable_series_and_original_source_hashes(tmp_path):
    batch = tmp_path / "batch"
    attempt = batch / "cells/cell/attempt-1"
    raw = attempt / "collected"
    raw.mkdir(parents=True)
    config = fixture(raw, latencies=(4, 4, 4, 20))
    result = CRDBAdapter().parse_results(config, raw)
    write_json(attempt / "result.json", result)
    write_json(raw / "manifest.json", dict(verified=True, errors=[], files=[
        dict(path=p.name, size=p.stat().st_size, sha256=file_hash(p)) for p in raw.iterdir()]))
    record = dict(cell_id="cell", repeat=0, status="success", config=config, config_hash=digest(config),
                  environment_hash="fixture", result_hash=digest(result), attempt_path="cells/cell/attempt-1")
    write_json(batch / "summary.json", dict(cells=[record]))
    write_json(batch / "batch.json", dict(batch_id="fixture", config=config, frozen_config_hash=digest(config),
                                         logical_plan_id=logical_plan_id(config)))
    before = {p: file_hash(p) for p in batch.rglob("*") if p.is_file()}
    process = subprocess.run([sys.executable, "-m", "crdb_bench.evaluation", "--batch", str(batch),
                              "--output", str(tmp_path / "export")], capture_output=True, text=True, timeout=10)
    assert process.returncode == 0, process.stderr
    report = read_json(tmp_path / "export/evaluation-report.json")
    assert report["cells"][0]["stability_status"] == "unstable"
    assert report["cells"][0]["comparison_eligible"] is False
    assert report["stability"]["cell"]["p99_ratio"]["value"] == 5
    with (tmp_path / "export/stability-series.csv").open() as stream:
        points = list(csv.DictReader(stream))
    assert [float(r["p99_ms"]) for r in points] == [4, 4, 4, 20]
    assert all(file_hash(p) == h for p, h in before.items())


@pytest.mark.parametrize("mutation", ["missing", "criterion", "zero", "partial-bin", "boolean"])
def test_policy_validation_requires_explicit_frozen_bindings(tmp_path, mutation):
    config = fixture(tmp_path)
    settings = config["measurement"]["stability"]
    if mutation == "missing":
        del settings["max_p99_ratio"]
    elif mutation == "criterion":
        settings["criterion"] = "plateau"
    elif mutation == "zero":
        settings["bin_seconds"] = 0
    elif mutation == "partial-bin":
        settings["bin_seconds"] = .7
    else:
        settings["stop_on_unstable_off"] = 1
    with pytest.raises(ValueError, match="stability"):
        CRDBAdapter().validate(config)
