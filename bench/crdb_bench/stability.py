"""Explicitly bound completion-bin stability; independent of result integrity."""
import math

from experiment_common.analysis import quantile


def validate_policy(config):
    if "stability" not in config["measurement"]:
        return None
    policy = config["measurement"]["stability"]
    fields = {"criterion", "bin_seconds", "min_bins", "min_terminals_per_bin",
              "min_commit_rate_ratio", "max_p99_ratio", "stop_on_unstable_off"}
    if not isinstance(policy, dict) or set(policy) != fields:
        raise ValueError("measurement.stability requires every explicit criterion/threshold binding")
    if policy["criterion"] != "completion_bin_spread_v1" or policy["stop_on_unstable_off"] is not True:
        raise ValueError("stability requires completion_bin_spread_v1 and stop_on_unstable_off=true")
    for field in fields - {"criterion", "stop_on_unstable_off"}:
        value = policy[field]
        if type(value) not in (int, float) or not math.isfinite(value):
            raise ValueError("Invalid stability threshold: " + field)
    if (policy["bin_seconds"] <= 0 or type(policy["min_bins"]) is not int or policy["min_bins"] < 2
            or type(policy["min_terminals_per_bin"]) is not int or policy["min_terminals_per_bin"] < 1
            or not 0 <= policy["min_commit_rate_ratio"] <= 1 or policy["max_p99_ratio"] < 1):
        raise ValueError("Invalid stability threshold bounds")
    width = policy["bin_seconds"] * 1e9
    bins = config["measurement"]["duration_seconds"] / policy["bin_seconds"]
    if (not math.isclose(width, round(width), abs_tol=1e-6, rel_tol=0)
            or not math.isclose(bins, round(bins), abs_tol=1e-9, rel_tol=0)
            or round(bins) < policy["min_bins"]):
        raise ValueError("stability requires whole native-nanosecond bins covering the window and min_bins")
    return policy


def assess(config, processes):
    policy = validate_policy(config)
    if policy is None:
        return {"status": "not_assessed", "policy": None, "bins": [],
                "reasons": ["Explicit measurement.stability scientific bindings are required"]}
    width = round(policy["bin_seconds"] * 1e9)
    count = round(config["measurement"]["duration_seconds"] / policy["bin_seconds"])
    buckets = [[] for _ in range(count)]
    sources, process_bins = [], []
    for process in processes:
        events = process["events"]
        sources.append({k: v for k, v in process.items() if k != "events"})
        local = [[] for _ in range(count)]
        for event in events:
            index = event["complete_relative_ns"] // width
            if not 0 <= index < count:
                raise ValueError("Native stability event offset is outside the frozen measurement window")
            local[index].append(event)
            buckets[index].append(event)
        for index, rows in enumerate(local):
            process_bins.append({"process_id": process["process_id"], "bin": index,
                                 "terminals": len(rows), "commits": sum(r["terminal"] == "committed" for r in rows)})
    bins = []
    for index, rows in enumerate(buckets):
        commits = sum(r["terminal"] == "committed" for r in rows)
        bins.append({"bin": index, "start_seconds": index * policy["bin_seconds"],
                     "end_seconds": (index + 1) * policy["bin_seconds"],
                     "terminals": len(rows), "commits": commits, "errors": len(rows) - commits,
                     "exposure_seconds": policy["bin_seconds"], "commit_rate_tps": commits / policy["bin_seconds"],
                     "p99_ms": quantile([r["latency_ms"] for r in rows], .99)})
    rates = [r["commit_rate_tps"] for r in bins]
    enough = all(r["terminals"] >= policy["min_terminals_per_bin"] for r in bins)
    p99s = [r["p99_ms"] for r in bins if r["p99_ms"] is not None]
    rate = {"numerator": min(rates), "denominator": max(rates),
            "value": min(rates) / max(rates) if max(rates) else None}
    latency = {"numerator": max(p99s) if p99s else None, "denominator": min(p99s) if p99s else None,
               "value": max(p99s) / min(p99s) if enough and p99s and min(p99s) > 0 else None}
    violations, missing = [], []
    if rate["value"] is not None and rate["value"] < policy["min_commit_rate_ratio"]:
        violations.append("Minimum/maximum bin commit-rate ratio below frozen threshold")
    if latency["value"] is not None and latency["value"] > policy["max_p99_ratio"]:
        violations.append("Maximum/minimum bin p99 ratio above frozen threshold")
    if not enough:
        missing.append("Some bins have fewer than min_terminals_per_bin")
    if rate["value"] is None:
        missing.append("No positive bin commit-rate denominator")
    if latency["value"] is None:
        missing.append("Bin p99 spread lacks adequate samples or a positive denominator")
    return {"status": "unstable" if violations else "insufficient" if missing else "stable",
            "policy": policy, "reasons": violations + missing, "bins": bins, "process_bins": process_bins,
            "sources": sources, "commit_rate_ratio": rate, "p99_ratio": latency,
            "population": "Only native measured terminal transactions whose execution starts and completes within each client window; unfinished/warmup events excluded. Completion-time bins align relative to each process measurement start, not cross-host wall clocks. p99 includes committed and error terminals; commit rate counts only commits per bin exposure, not offered rate or capacity waits.",
            "criterion": "Full-window nonoverlapping completion bins; compare min/max committed-rate and max/min nearest-rank p99. A measured rate violation is unstable even if empty bins make p99 insufficient. Passing this explicitly chosen criterion is not a stationarity proof or a throughput-plateau judgment."}
