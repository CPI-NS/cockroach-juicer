"""Evaluation definitions grounded in current CRDB and Juicer counter sources."""

import json
import math
import re
import datetime
from collections import Counter

RESTART_CAUSES = (
    "txnaborted",
    "writetooold",
    "serializable",
    "txnpush",
    "readwithinuncertainty",
    "asyncwritefailure",
    "commitdeadlineexceeded",
    "exclusionviolation",
    "unknown",
)
CALIBRATION_ROW = re.compile(
    r"hold calibration: epoch=(\d+) key=(.*?) spans=(\d+) commits=(\d+) aborts=(\d+) p50=(\d+)us p99=(\d+)us unresolved=(\d+) untracked=(\d+) hist=(\S+)"
)


def calibration_report(root, summaries):
    window_start = max(s["start_unix_ns"] for s in summaries)
    window_end = min(s["end_unix_ns"] for s in summaries)
    excluded = 0
    behavior = []
    behavior_complete = True
    buckets = Counter()
    spans = commits = aborts = unresolved = untracked = 0
    seen = {}
    sources = []
    flags = []
    for path in sorted(root.rglob("*.evidence.json")):
        proof = json.loads(path.read_text())
        lines = proof.get("juicer_log_lines", [])
        observed = proof.get(
            "calibration_observed",
            any("HOLD CALIBRATION mode:" in line for line in lines),
        )
        flags.append(observed)
        boundaries = {}
        epoch_lines = {}
        for raw in lines:
            stamp = re.match(r"^[IWEF](\d{6}) (\d{2}:\d{2}:\d{2}\.\d{6})", raw)
            epoch = re.search(r"epoch summary: epoch=(\d+)", raw)
            if stamp and epoch:
                dt = datetime.datetime.strptime(
                    stamp[1] + " " + stamp[2], "%y%m%d %H:%M:%S.%f"
                ).replace(tzinfo=datetime.timezone.utc)
                boundaries[int(epoch[1])] = (
                    int(dt.timestamp()) * 1_000_000_000 + dt.microsecond * 1000
                )
                epoch_lines[int(epoch[1])] = raw
        node_behavior = []
        for epoch, end in sorted(boundaries.items()):
            left = boundaries.get(epoch - 1)
            if left is None or left < window_start or end > window_end:
                continue
            line = epoch_lines[epoch].replace("‹", "").replace("›", "")
            holds = re.search(r"holds_created=(\d+)", line)
            messages = re.search(
                r"messages\{intercepted=(\d+) direct=(\d+) queued=(\d+) dropped=(\d+) held=(\d+)\}",
                line,
            )
            if not holds or not messages:
                behavior_complete = False
                continue
            node_behavior.append(
                dict(
                    file=str(path.relative_to(root)),
                    epoch=epoch,
                    start_unix_ns=left,
                    end_unix_ns=end,
                    holds_created=int(holds[1]),
                    queued=int(messages[3]),
                    held=int(messages[5]),
                )
            )
        if not node_behavior:
            behavior_complete = False
        behavior.extend(node_behavior)
        for raw in lines:
            line = raw.replace("‹", "").replace("›", "")
            match = CALIBRATION_ROW.search(line)
            if not match:
                continue
            epoch, key, n, commit, abort, p50, p99, pending, lost, hist = match.groups()
            left = boundaries.get(int(epoch) - 1)
            right = boundaries.get(int(epoch))
            if (
                left is None
                or right is None
                or left < window_start
                or right > window_end
            ):
                excluded += 1
                continue
            identity = (str(path.relative_to(root)), epoch, key)
            if identity in seen:
                if seen[identity] != match.group(0):
                    raise ValueError("Conflicting calibration key/epoch records")
                continue
            seen[identity] = match.group(0)
            row = Counter()
            if hist != "-":
                for item in hist.split(","):
                    bound, count = item.split(":")
                    row[bound] += int(count)
            n, commit, abort, pending, lost = map(
                int, (n, commit, abort, pending, lost)
            )
            if sum(row.values()) != n or commit + abort != n:
                raise ValueError("Calibration histogram/span/terminal counts disagree")
            buckets.update(row)
            spans += n
            commits += commit
            aborts += abort
            unresolved += pending
            untracked += lost
            sources.append(
                dict(
                    file=str(path.relative_to(root)), epoch=int(epoch), key=key, spans=n
                )
            )

    def quantile(q):
        if not spans:
            return None
        want = math.ceil(spans * q)
        count = 0
        for upper, n in sorted((int(k), v) for k, v in buckets.items() if k.isdigit()):
            count += n
            if count >= want:
                width = 1 if upper < 16 else 1 << (upper.bit_length() - 1 - 3)
                return (upper - width + 1 + upper) // 2
        return None

    p99 = quantile(0.99)
    valid = (
        bool(flags)
        and all(flags)
        and spans > 0
        and untracked == 0
        and p99 is not None
        and behavior_complete
        and all(
            row["holds_created"] == 0 and row["held"] == 0 and row["queued"] == 0
            for row in behavior
        )
    )
    return dict(
        valid=valid,
        spans=spans,
        commits=commits,
        aborts=aborts,
        unresolved=unresolved,
        untracked=untracked,
        p50_us=quantile(0.5),
        p99_us=p99,
        histogram_upper_us=dict(buckets),
        sources=sources,
        recommended_max_hold_ms=max(1, math.ceil(p99 / 1000)) if valid else None,
        config_override=(
            {"juicer.env.MAX_HOLD_MS": max(1, math.ceil(p99 / 1000))} if valid else None
        ),
        observe_only_behavior=behavior,
        excluded_epoch_key_rows=excluded,
        window=dict(
            start_unix_ns=window_start,
            end_unix_ns=window_end,
            policy="Complete epochs bracketed by consecutive observed native epoch-summary timestamps inside the intersection of client measurement windows",
        ),
        population="First marker on a nominated server/key to commit or abort applied there; spans completing in selected native epochs. Individual span starts are not logged; read-only/unresolved spans are not imputed",
        reason=(
            None
            if valid
            else "Missing observe-only declaration/epoch behavior/completed histograms, actual holds/queueing, lost tracking, or p99 outside finite histogram"
        ),
    )


def enrich(config, root, result, summaries):
    diagnostics = result["diagnostics"]
    reasons = result["validity"]["reasons"]
    counters = diagnostics["server_counter_deltas"]
    layers = diagnostics["retry_layers"]
    by_reason = {name: counters.get("txn_restarts_" + name) for name in RESTART_CAUSES}
    cause_sum = (
        sum(by_reason.values())
        if all(v is not None for v in by_reason.values())
        else None
    )
    sql = counters.get("sql_txn_auto_retry_count")
    residual = sql - cause_sum if sql is not None and cause_sum is not None else None
    identity = dict(
        kv_cause_counts=by_reason,
        kv_cause_sum=cause_sum,
        sql_auto_retries=sql,
        residual_sql_minus_kv=residual,
        identity_holds=residual == 0 if residual is not None else None,
        histogram_transaction_count=counters.get("txn_restarts_count"),
        histogram_epoch_sum=counters.get("txn_restarts_sum"),
        window=diagnostics["server_windows"],
        interpretation="KV causes and SQL retry total have different instrumentation/terminal boundaries. Equality is an observed same-scrape-window identity, not per-SQL cause instrumentation; histogram_count counts transactions, not retries",
    )
    terminals = Counter()
    for path in root.rglob("client-*.events.jsonl"):
        for raw in path.read_text().splitlines():
            row = json.loads(raw)
            if row.get("measured") and row.get("terminal") == "error":
                terminals[row.get("sqlstate") or "non_sql_error"] += 1
    attempt_states = Counter()
    for s in summaries:
        attempt_states.update(s.get("attempt_sqlstates", {}))
    client = layers["client"]
    terminal_population = (
        result["metrics"]["commits"] + client["terminal_errors"]
        if result["metrics"]["commits"] is not None
        else None
    )

    def client_ratio(n, d, window):
        return dict(
            value=n / d if d else None,
            numerator=n,
            denominator=d,
            window=window,
            reason=None if d else "No denominator population",
        )

    report = dict(
        cc_restarts_per_commit=layers["cc_restarts_per_server_commit"],
        sql_auto_retries_per_commit=layers["sql_auto_retries_per_server_commit"],
        uncertainty_per_commit=layers["uncertainty_restarts_per_server_commit"],
        deadlocks_per_commit=layers["deadlocks_per_server_commit"],
        one_pc_fallback=layers["one_pc_fallback_per_attempt"],
        sql_retry_cause_identity=identity,
        kv_restart_causes=by_reason,
        sql_retry_cause_attribution=None,
        attribution_reason="SQL source has a total retry counter, not a per-cause SQL counter; KV cause counts are separately named even when the identity holds",
        visible_40001_per_client_attempt=client_ratio(
            client["visible_40001"],
            client["attempts"],
            "client SQL attempts fully inside measurement",
        ),
        terminal_40001_per_client_terminal=client_ratio(
            terminals.get("40001", 0),
            terminal_population,
            "client logical transactions fully inside measurement",
        ),
        attempt_sqlstates=dict(attempt_states),
        terminal_sqlstates=dict(terminals),
    )
    policy = config["topology"].get("metrics")
    required = [
        *("txn_restarts_" + name for name in RESTART_CAUSES),
        "sql_txn_auto_retry_count",
        "txnwaitqueue_deadlocks_total",
        "txn_server_side_1pc_success",
        "txn_server_side_1pc_failure",
    ]
    report["missing_counters"] = [name for name in required if name not in counters]
    report["missing_counters_by_server"] = {
        url: [name for name in required if name not in window["counter_deltas"]]
        for url, window in diagnostics["server_windows"].items()
        if any(name not in window["counter_deltas"] for name in required)
    }
    if policy is not None:
        if report["missing_counters_by_server"]:
            reasons.append(
                "Evaluation counters unavailable by server: "
                + json.dumps(report["missing_counters_by_server"], sort_keys=True)
            )
        if report["missing_counters"]:
            reasons.append(
                "Evaluation counters unavailable: "
                + ", ".join(report["missing_counters"])
            )
        if policy.get("require_retry_identity", False) and residual != 0:
            reasons.append(
                f"SQL retry/cause sum identity differs: SQL minus KV = {residual}"
            )
    diagnostics["evaluation"] = report
    result["metrics"]["cc_restarts_per_commit"] = report["cc_restarts_per_commit"][
        "value"
    ]
    if config["juicer"].get("hold_calibration", False):
        meter = calibration_report(root, summaries)
        diagnostics["hold_calibration"] = meter
        if not meter["valid"]:
            reasons.append("Invalid hold calibration: " + str(meter["reason"]))
    if "layout" in config["topology"]:
        try:
            diagnostics["layout"] = layout_report(config, root, summaries)
        except (ValueError, OSError, KeyError, TypeError) as exc:
            reasons.append("Invalid evaluation layout: " + str(exc))
    result["validity"]["valid"] = not reasons


def layout_report(config, root, summaries):
    def one(suffix):
        paths = list(root.rglob("client-*" + suffix))
        if len(paths) != 1:
            raise ValueError(f"Expected one {suffix} evidence file")
        return paths[0]

    initial = json.loads(one(".layout.json").read_text())
    final = json.loads(one(".layout-final.json").read_text())
    observations = [
        json.loads(line)
        for line in one(".layout-monitor.jsonl").read_text().splitlines()
    ]
    if (
        not initial.get("valid")
        or not final.get("valid")
        or not observations
        or not all(row.get("valid") for row in observations)
    ):
        raise ValueError("Missing or invalid initial/runtime/final layout")
    layout = config["topology"]["layout"]
    nodes = initial["nodes"]
    stores = {node["name"]: node["store_id"] for node in nodes}
    if set(stores) != set(layout["lease_nodes"]) or len(set(stores.values())) != len(
        stores
    ):
        raise ValueError("Observed node/store inventory differs from frozen layout")
    count = config["workload"]["keys"]
    multiplier = 1
    if config["workload"].get("scramble", True):
        multiplier = max(1, count * 733 // 1000)
        while math.gcd(multiplier, count) != 1:
            multiplier -= 1
    hot = initial["desired"]["hot_keys"]
    if len(hot) != layout["hot_ranks"] or len({r["range_id"] for r in hot}) != len(hot):
        raise ValueError("Hot ranks are not isolated in distinct ranges")
    for rank, row in enumerate(hot):
        target = layout["lease_nodes"][rank % len(stores)]
        if (
            row["rank"] != rank
            or row["key"] != rank * multiplier % count
            or row["target_node"] != target
            or row["lease_store"] != stores[target]
        ):
            raise ValueError("Hot rank/key/lease target differs from frozen layout")

    def canonical(rows):
        result = []
        for i, r in enumerate(rows):
            if r["logical_start_hex"] >= r["logical_end_hex"] or (
                i and rows[i - 1]["logical_end_hex"] != r["logical_start_hex"]
            ):
                raise ValueError(
                    "Layout does not continuously cover its logical table span"
                )
            if (
                set(r["replicas"]) != set(stores.values())
                or set(r["voting_replicas"]) != set(stores.values())
                or r["non_voting_replicas"]
                or r["learner_replicas"]
            ):
                raise ValueError("Incomplete/foreign replica placement")
            result.append(
                (r["logical_start_hex"], r["logical_end_hex"], r["lease_holder"])
            )
        return result

    expected = canonical(initial["ranges"])
    for row in [*observations, final]:
        if canonical(row["ranges"]) != expected:
            raise ValueError("Runtime/final range-to-lease map changed")
        if row["settings"].get("kv.allocator.load_based_rebalancing") != "off" or row[
            "settings"
        ].get("kv.lease_queue.enabled") not in ("f", "false"):
            raise ValueError("Automatic lease controls changed")
    lease_counts = Counter(r["lease_holder"] for r in initial["ranges"])
    distribution = [lease_counts[store] for store in stores.values()]
    if max(distribution) - min(distribution) > 1:
        raise ValueError("Complete range lease distribution is not balanced")
    start = min(s["start_unix_ns"] for s in summaries)
    end = max(s["end_unix_ns"] for s in summaries)
    in_window = [r for r in observations if start <= r["end_unix_ns"] <= end]
    if not in_window or final["start_unix_ns"] < end:
        raise ValueError(
            "Layout observations do not cover the client measurement/final boundary"
        )
    return dict(
        valid=True,
        hot_keys=hot,
        nodes=nodes,
        range_count=len(initial["ranges"]),
        leases_by_node={name: lease_counts[store] for name, store in stores.items()},
        initial=initial["start_unix_ns"],
        final=final["end_unix_ns"],
        measurement_samples=len(in_window),
        observation_count=len(observations),
        continuity="Discrete verified snapshots plus final map; movement away and back between snapshots is not excluded",
    )
