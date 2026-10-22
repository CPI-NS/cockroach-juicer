"""Parse immutable client events and keep retry layers in their own windows."""

import hashlib
import json
import math
import re
from collections import defaultdict
from pathlib import Path

from experiment_common.allocation import allocate_clients

CC_RESTARTS = (
    "txn_restarts_txnaborted",
    "txn_restarts_writetooold",
    "txn_restarts_serializable",
    "txn_restarts_txnpush",
)
REQUIRED_COUNTERS = (
    *CC_RESTARTS,
    "txn_commits",
    "sql_txn_auto_retry_count",
    "txn_restarts_readwithinuncertainty",
)
SAMPLE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{.*\})?\s+([^\s]+)(?:\s+\d+)?$")


def _series(raw):
    values = {}
    for line in raw.splitlines():
        match = SAMPLE.match(line)
        if match:
            name, labels, value = match.groups()
            value = float(value)
            if not math.isfinite(value):
                raise ValueError(f"Nonfinite Prometheus counter {name}")
            values[(name.lower(), labels or "")] = value
    return values


def _counter_layers(summaries, reasons):
    by_url = defaultdict(list)
    for s in summaries:
        for row in s.get("counter_snapshots") or []:
            # Both endpoints of every scrape must lie inside the client window.
            if (
                not row.get("error")
                and row["start_unix_ns"] >= s["start_unix_ns"]
                and row["end_unix_ns"] <= s["end_unix_ns"]
            ):
                by_url[row["url"]].append(row)
    totals, windows = defaultdict(float), {}
    for url, rows in by_url.items():
        rows.sort(key=lambda r: r["start_unix_ns"])
        if len(rows) < 2:
            reasons.append(f"Fewer than two in-window counter scrapes: {url}")
            continue
        first, last = rows[0], rows[-1]
        before, after = _series(first["raw"]), _series(last["raw"])
        delta = defaultdict(float)
        names = set()
        for key, end in after.items():
            name = key[0]
            if name.startswith(
                ("txn_", "sql_txn_", "txnwaitqueue_")
            ) and not name.endswith("_bucket"):
                if key not in before:
                    reasons.append(
                        f"Counter series appeared inside measurement: {url}: {key}"
                    )
                    continue
                diff = end - before[key]
                if diff < 0:
                    # txn_durations is a histogram, not a monotonic scalar.
                    if name in REQUIRED_COUNTERS or name.startswith("txn_restarts_"):
                        reasons.append(
                            f"Counter reset inside measurement: {url}: {name}"
                        )
                    continue
                delta[name] += diff
                names.add(name)
        for name in REQUIRED_COUNTERS:
            if name not in names:
                reasons.append(f"Required retry counter unavailable: {url}: {name}")
        for name, value in delta.items():
            totals[name] += value
        windows[url] = dict(
            first_start_unix_ns=first["start_unix_ns"],
            first_end_unix_ns=first["end_unix_ns"],
            last_start_unix_ns=last["start_unix_ns"],
            last_end_unix_ns=last["end_unix_ns"],
            duration_seconds=(last["end_unix_ns"] - first["end_unix_ns"]) / 1e9,
            counter_deltas=dict(delta),
        )
    denominator = totals.get("txn_commits")

    def ratio(names):
        numerator = (
            sum(totals[k] for k in names) if all(k in totals for k in names) else None
        )
        return dict(
            value=(
                numerator / denominator
                if numerator is not None and denominator
                else None
            ),
            numerator=numerator,
            denominator=denominator,
            numerator_counters=list(names),
            denominator_counter="txn_commits",
            window="server_counter_scrapes",
            unit="events per server KV commit",
            reason=(
                None
                if numerator is not None and denominator
                else "Missing counter or no server KV commit denominator"
            ),
        )

    layers = dict(
        cc_restarts_per_server_commit=ratio(CC_RESTARTS),
        sql_auto_retries_per_server_commit=ratio(("sql_txn_auto_retry_count",)),
        uncertainty_restarts_per_server_commit=ratio(
            ("txn_restarts_readwithinuncertainty",)
        ),
        deadlocks_per_server_commit=ratio(("txnwaitqueue_deadlocks_total",)),
        one_phase_commits_per_server_commit=ratio(("txn_commits1pc",)),
    )
    failure = totals.get("txn_server_side_1pc_failure")
    success = totals.get("txn_server_side_1pc_success")
    total = success + failure if success is not None and failure is not None else None
    layers["one_pc_fallback_per_attempt"] = dict(
        value=failure / total if total else None,
        numerator=failure,
        denominator=total,
        numerator_counter="txn_server_side_1PC_failure",
        denominator_counters=[
            "txn_server_side_1PC_success",
            "txn_server_side_1PC_failure",
        ],
        window="server_counter_scrapes",
        unit="failed 1PC batches per attempted 1PC batch",
        reason=None if total else "No observed 1PC attempts or a missing 1PC counter",
    )
    return layers, dict(totals), windows


def parse_results(config, root):
    from .stability import assess, validate_policy

    stability_policy = validate_policy(config)
    stability_processes = []
    reasons, files, summaries, latencies = [], [], [], []
    commits = attempts = attempt_errors = retries = visible = errors = slots = 0
    seen = set()
    expected = {p["process_id"] for p in allocate_clients(config)}
    for path in sorted(root.rglob("client-*.summary.json")):
        try:
            pid = int(path.name.split(".")[0].split("-")[1])
            if pid in seen:
                raise ValueError(f"Duplicate process summary {pid}")
            seen.add(pid)
            s = json.loads(path.read_text())
            if s.get("schema_version") != 1 or not s.get("complete"):
                raise ValueError(f"Incomplete client measurement: {path.name}")
            duration = config["measurement"]["duration_seconds"]
            if not math.isclose(s["duration_seconds"], duration) or not math.isclose(
                (s["end_unix_ns"] - s["start_unix_ns"]) / 1e9, duration, abs_tol=1e-6
            ):
                raise ValueError(
                    "Client measurement duration differs from frozen config"
                )
            if s.get("dropped") != 0:
                raise ValueError("Client dropped arrivals")
            slots += s["config"]["slots"]
            event_path = path.with_name(
                path.name.replace(".summary.json", ".events.jsonl")
            )
            observed_commits = observed_errors = 0
            event_ids = set()
            stability_events, stability_clocks = [], set()
            for line in event_path.read_text().splitlines():
                row = json.loads(line)
                if row["id"] in event_ids:
                    raise ValueError("Duplicate transaction event")
                event_ids.add(row["id"])
                if not row["measured"]:
                    continue
                if not (
                    s["start_unix_ns"]
                    <= row["start_unix_ns"]
                    <= row["complete_unix_ns"]
                    < s["end_unix_ns"]
                ):
                    raise ValueError(
                        "Latency includes a transaction outside the measurement window"
                    )
                if row["terminal"] not in ("committed", "error"):
                    raise ValueError(
                        "Unfinished transaction included in latency population"
                    )
                latency = row["latency_ms"]
                if "start_offset_ns" in row:
                    observed = (
                        row["complete_offset_ns"] - row["start_offset_ns"]
                    ) / 1e6
                    tolerance = 0.000001
                else:
                    # Older collected events use monotonic elapsed time but wall
                    # timestamps; tolerate small wall/monotonic clock skew.
                    observed = (row["complete_unix_ns"] - row["start_unix_ns"]) / 1e6
                    tolerance = 0.01
                if (
                    not math.isfinite(latency)
                    or latency < 0
                    or not math.isclose(observed, latency, abs_tol=tolerance)
                ):
                    raise ValueError("Latency disagrees with execution timestamps")
                latencies.append(latency)
                if stability_policy:
                    monotonic = "complete_offset_ns" in row
                    relative = (row["complete_offset_ns"] - round(config["measurement"].get("warmup_seconds", 0) * 1e9)
                                if monotonic else row["complete_unix_ns"] - s["start_unix_ns"])
                    stability_events.append(dict(complete_relative_ns=relative, terminal=row["terminal"], latency_ms=latency))
                    stability_clocks.add("native_monotonic_offset" if monotonic else "native_window_relative_wall")
                observed_commits += row["terminal"] == "committed"
                observed_errors += row["terminal"] == "error"
            if observed_commits != s["commits"] or observed_errors != s["errors"]:
                raise ValueError("Terminal event counts disagree with client summary")
            summaries.append(s)
            if stability_policy:
                stability_processes.append(dict(process_id=pid, summary=str(path.relative_to(root)),
                    events_path=str(event_path.relative_to(root)), start_unix_ns=s["start_unix_ns"], end_unix_ns=s["end_unix_ns"],
                    clock=next(iter(stability_clocks)) if len(stability_clocks) == 1 else "mixed_native_offsets_and_wall",
                    events=stability_events))
            for name in (
                "commits",
                "attempts",
                "attempt_errors",
                "client_retries",
                "visible_40001",
                "errors",
            ):
                value = s[name]
                if not isinstance(value, int) or value < 0:
                    raise ValueError(f"Invalid {name} counter")
            commits += s["commits"]
            attempts += s["attempts"]
            attempt_errors += s["attempt_errors"]
            retries += s["client_retries"]
            visible += s["visible_40001"]
            errors += s["errors"]
        except (ValueError, OSError, KeyError, TypeError) as exc:
            reasons.append(f"{path}: {exc}")
    if seen != expected:
        reasons.append(
            f"Client process set differs from allocation: expected={sorted(expected)}, found={sorted(seen)}"
        )
    target = (
        config["load"].get("concurrency")
        if config["load"]["mode"] == "closed"
        else config["load"]["max_inflight"]
    )
    if slots != target:
        reasons.append("Collected slots differ from global allocation")
    if not latencies:
        reasons.append("No completed terminal transactions in measurement")
    try:
        layers, counters, windows = _counter_layers(summaries, reasons)
    except (KeyError, ValueError, TypeError) as exc:
        reasons.append(f"Invalid server counters: {exc}")
        layers, counters, windows = {}, {}, {}
    if len(windows) != len(config["topology"]["nodes"]):
        reasons.append("In-window server counter evidence missing for some CRDB nodes")
    layers["client"] = dict(
        attempts=attempts,
        attempt_errors=attempt_errors,
        retries=retries,
        visible_40001=visible,
        terminal_errors=errors,
        attempt_window="Each SQL attempt starts and ends inside its client measurement window",
        terminal_window="Logical transaction execution starts and ends inside its client measurement window",
        abort_ratio=attempt_errors / attempts if attempts else None,
    )
    latencies.sort()

    def quantile(q):
        return (
            latencies[max(0, math.ceil(q * len(latencies)) - 1)] if latencies else None
        )

    def one_file(name):
        matches = list(root.rglob(name))
        if len(matches) != 1:
            raise ValueError(f"Expected one collected {name}; found {len(matches)}")
        return json.loads(matches[0].read_text())

    try:
        loaded = one_file("client-0.load.json")
        if (
            loaded.get("rows") != config["workload"]["keys"]
            or loaded.get("seed") != config["workload"]["seed"]
            or loaded.get("reset") is not True
        ):
            reasons.append("Loaded data does not match the frozen workload reset")
        provenance_names = [f"client-{pid}" for pid in expected]
        for node in config["topology"]["nodes"]:
            evidence = one_file(f'{node["id"]}.evidence.json')
            enabled = config["juicer"]["enabled"]
            if (
                evidence.get("valid") is not True
                or evidence.get("expected_enabled") is not enabled
                or evidence.get("interception_observed") is not enabled
            ):
                reasons.append(
                    f'Juicer evidence differs from requested treatment: {node["id"]}'
                )
            provenance_names.append(node["id"])
        for name in provenance_names:
            proof = one_file(f"{name}.provenance.json")
            if proof.get("source_revision") != config["artifacts"]["source_revision"]:
                reasons.append(f"Artifact source revision mismatch: {name}")
            for artifact in ("cockroach", "sql_client"):
                actual = proof.get("sha256", {}).get(artifact, "")
                if not re.fullmatch("[0-9a-f]{64}", actual):
                    reasons.append(f"Artifact SHA-256 missing: {name}: {artifact}")
                expected_hash = (
                    config["artifacts"][artifact].get("sha256")
                    if isinstance(config["artifacts"][artifact], dict)
                    else config["artifacts"].get(artifact + "_sha256")
                )
                if expected_hash and actual != expected_hash:
                    reasons.append(f"Artifact SHA-256 mismatch: {name}: {artifact}")
    except (KeyError, ValueError, OSError, TypeError) as exc:
        reasons.append(f"Missing or invalid preparation/treatment evidence: {exc}")
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.name != "manifest.json":
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            files.append(
                dict(
                    path=str(path.relative_to(root)),
                    sha256=digest,
                    bytes=path.stat().st_size,
                )
            )
    result = dict(
        schema_version=1,
        metrics=dict(
            throughput_tps=(
                commits / config["measurement"]["duration_seconds"]
                if summaries
                else None
            ),
            latency_ms=dict(p50=quantile(0.5), p99=quantile(0.99)),
            commits=commits if summaries else None,
            attempts=attempts if summaries else None,
            aborts=layers.get("cc_restarts_per_server_commit", {}).get("numerator"),
        ),
        latency_samples_ms=latencies,
        measurement=dict(
            duration_seconds=config["measurement"]["duration_seconds"],
            semantics="Execution start to terminal completion, including retries; excludes capacity waiting and unfinished-at-end transactions; per-process windows retained",
            client_windows=[
                dict(start_unix_ns=s["start_unix_ns"], end_unix_ns=s["end_unix_ns"])
                for s in summaries
            ],
        ),
        diagnostics=dict(
            retry_layers=layers,
            server_counter_deltas=counters,
            server_windows=windows,
            capacity_limited=any(
                s.get("capacity_wait_count", 0) > 0 or s.get("capacity_wait_ns", 0) > 0
                for s in summaries
            ),
            capacity_wait_count=sum(s.get("capacity_wait_count", 0) for s in summaries),
            capacity_wait_ns=sum(s.get("capacity_wait_ns", 0) for s in summaries),
            unavailable={
                "one_pc_fallback_attempts": (
                    None
                    if "txn_server_side_1pc_failure" in counters
                    else "Counter not exposed"
                ),
                "deadlocks": (
                    None
                    if "txnwaitqueue_deadlocks_total" in counters
                    else "Counter not exposed"
                ),
            },
            provenance=config.get("artifacts", {}),
        ),
        validity=dict(valid=not reasons, reasons=reasons),
        files=files,
    )

    from .evaluation_metrics import enrich

    enrich(config, root, result, summaries)
    result["diagnostics"]["stability"] = assess(config, stability_processes)
    return result
