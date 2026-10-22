"""SQL workload semantics; process ownership and SSH belong to experiment-common."""

import json
import math
import re
from pathlib import Path

from experiment_common.types import CellPlan, Command, GeneratedFile
from experiment_common.build import artifact_path


def _fields(value, allowed, section):
    extra = set(value) - set(allowed.split())
    if extra:
        raise ValueError(f"Unsupported {section} fields: {sorted(extra)}")


def _number(value, name, minimum=0, integer=False):
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        raise ValueError(f"{name} must be a finite number")
    if value < minimum or (integer and not isinstance(value, int)):
        raise ValueError(f"Invalid {name}: {value}")


class CRDBAdapter:
    name = "crdb"

    def validate(self, config):
        from .stability import validate_policy

        validate_policy(config)
        if config.get("system") != self.name or config.get("mode") not in (
            "local",
            "ssh",
        ):
            raise ValueError("CRDB requires system=crdb and mode=local|ssh")
        w = config["workload"]
        _fields(
            w,
            "name keys transaction_size zipf distinct_keys read_fraction read_mode seed retries scramble request_timeout_seconds retry_backoff_seconds load_batch_rows",
            "workload",
        )
        if w.get("name", "ycsbt") != "ycsbt" or w.get("read_mode") != "per_transaction":
            raise ValueError("CRDB ycsbt requires read_mode=per_transaction")
        for name in ("keys", "transaction_size"):
            _number(w[name], f"workload.{name}", 1, True)
        if w["keys"] > 1_000_000_000 or w["transaction_size"] > 10000:
            raise ValueError(
                "CRDB supports at most 1e9 keys and 10000 operations per transaction"
            )
        for name in ("zipf", "read_fraction"):
            _number(w[name], f"workload.{name}")
        if w["zipf"] >= 1 or w["read_fraction"] > 1:
            raise ValueError("zipf must be in [0,1); read_fraction in [0,1]")
        if not isinstance(w["distinct_keys"], bool):
            raise ValueError("distinct_keys must be boolean")
        if w["distinct_keys"] and w["transaction_size"] > w["keys"]:
            raise ValueError("Distinct transaction keys exceed accessed key space")
        _number(w["seed"], "workload.seed", 0, True)
        if w["seed"] >= 2**64:
            raise ValueError("seed must fit uint64")
        _number(w.get("retries", 0), "workload.retries", 0, True)
        for key in ("request_timeout_seconds", "retry_backoff_seconds"):
            if key in w:
                _number(
                    w[key],
                    f"workload.{key}",
                    0.000001 if key.startswith("request") else 0,
                )
        if "load_batch_rows" in w:
            _number(w["load_batch_rows"], "workload.load_batch_rows", 1, True)
        if "scramble" in w and not isinstance(w["scramble"], bool):
            raise ValueError("scramble must be boolean")
        t = config["topology"]
        _fields(
            t,
            "nodes replicas splits cache_size max_sql_memory layout metrics",
            "topology",
        )
        hosts = {h["id"]: h for h in config["inventory"]}
        if not t["nodes"]:
            raise ValueError("At least one CRDB node is required")
        ids, ports = set(), set()
        for n in t["nodes"]:
            _fields(
                n, "id host_id rpc_port sql_port http_port advertise_address", "node"
            )
            if (
                n["id"].startswith("client-")
                or n["id"] in ids
                or not n["id"].replace("-", "").replace("_", "").isalnum()
            ):
                raise ValueError("Node IDs must be unique safe path components")
            ids.add(n["id"])
            if (
                n["host_id"] not in hosts
                or "server" not in hosts[n["host_id"]]["roles"]
            ):
                raise ValueError("Node host must have server role")
            for key in ("rpc_port", "sql_port", "http_port"):
                _number(n[key], key, 1, True)
                identity = (
                    n["host_id"] if config["mode"] == "ssh" else "local",
                    n[key],
                )
                if n[key] > 65535 or identity in ports:
                    raise ValueError("Node ports must be unique on each physical host")
                ports.add(identity)
        _number(t.get("replicas", min(3, len(ids))), "topology.replicas", 1, True)
        if t.get("replicas", min(3, len(ids))) > len(ids):
            raise ValueError("replicas exceeds node count")
        _number(t.get("splits", 0), "topology.splits", 0, True)
        if t.get("splits", 0) >= w["keys"]:
            raise ValueError("splits must be smaller than keys")
        if "layout" in t:
            layout = t["layout"]
            _fields(
                layout,
                "hot_ranks lease_nodes disable_rebalancing monitor_interval_seconds",
                "topology.layout",
            )
            _number(layout["hot_ranks"], "layout.hot_ranks", 1, True)
            if layout["hot_ranks"] > w["keys"]:
                raise ValueError("Hot ranks exceed accessed keys")
            if (
                not isinstance(layout["lease_nodes"], list)
                or len(set(layout["lease_nodes"])) != len(layout["lease_nodes"])
                or set(layout["lease_nodes"]) != ids
                or t.get("replicas", min(3, len(ids))) != len(ids)
            ):
                raise ValueError(
                    "Fixed layout requires every configured node as a voter and lease target"
                )
            if layout.get("disable_rebalancing") is not True:
                raise ValueError("Fixed layout requires disable_rebalancing=true")
            _number(
                layout.get("monitor_interval_seconds", 1),
                "layout.monitor_interval_seconds",
                0.05,
            )
        if "metrics" in t:
            _fields(t["metrics"], "require_retry_identity", "topology.metrics")
            if not isinstance(t["metrics"].get("require_retry_identity", False), bool):
                raise ValueError("metrics.require_retry_identity must be boolean")
        j = config["juicer"]
        _fields(j, "enabled env hold_calibration", "juicer")
        if not isinstance(j.get("enabled"), bool):
            raise ValueError("juicer.enabled must be boolean")
        calibration = j.get("hold_calibration", False)
        if not isinstance(calibration, bool):
            raise ValueError("juicer.hold_calibration must be boolean")
        if calibration and (
            not j["enabled"]
            or str(j.get("env", {}).get("AW", "false")).lower() != "false"
            or j.get("env", {}).get("MANUAL_QUEUE_KEYS")
        ):
            raise ValueError(
                "Hold calibration requires the enabled observe-only interceptor without AW/manual queues"
            )
        # This allowlist follows pkg/rpc/juicer_spi.go and juicer_spec.go.
        allowed = {
            "RULES",
            "FIXED_WAIT_MS",
            "MAX_HOLD_MS",
            "FILTER_K",
            "FILTER_S",
            "EPOCH_MS",
            "IDLE_EPOCHS",
            "LATE_POLICY",
            "DIAG",
            "AW",
            "FIFO",
            "SKIP_QUEUEING",
            "FILTER_DIAGNOSTIC",
            "MANUAL_QUEUE_KEYS",
        }
        for key, value in j.get("env", {}).items():
            if key not in allowed or not isinstance(value, (str, int, float, bool)):
                raise ValueError(f"Unsupported Juicer environment setting: {key}")
        a = config["artifacts"]
        _fields(
            a,
            "cockroach sql_client source_revision common_revision sql_client_revision cockroach_sha256 sql_client_sha256 source_manifest client_source go cockroach_build",
            "artifacts",
        )
        for name in ("cockroach", "sql_client"):
            if (
                name not in a
                or not isinstance(artifact_path(config, name), str)
                or not Path(artifact_path(config, name)).is_absolute()
            ):
                raise ValueError(
                    f"artifacts.{name} must be an absolute executable path"
                )
        for name in (
            "cockroach",
            "sql_client",
            *(("source_manifest",) if "source_manifest" in a else ()),
        ):
            if isinstance(a[name], dict):
                value = a[name]
                _fields(value, "path deploy_to hosts sha256", f"artifacts.{name}")
                if (
                    not isinstance(value.get("path"), str)
                    or not Path(value["path"]).is_absolute()
                ):
                    raise ValueError(f"artifacts.{name}.path must be absolute")
                if "hosts" in value and (
                    not isinstance(value["hosts"], list)
                    or set(value["hosts"]) - set(hosts)
                ):
                    raise ValueError(f"artifacts.{name}.hosts contains unknown hosts")
                if "sha256" in value and not re.fullmatch(
                    "[0-9a-f]{64}", value["sha256"]
                ):
                    raise ValueError(
                        f"artifacts.{name}.sha256 must be a lowercase SHA-256"
                    )
        if not a.get("source_revision"):
            raise ValueError("artifacts.source_revision is required for provenance")
        if config.get("runtime", {}).get("reuse_data", False):
            raise ValueError(
                "CRDB cells use isolated fresh stores; reuse_data=true is unsupported"
            )
        l = config["load"]
        if l["mode"] not in ("closed", "open"):
            raise ValueError("load.mode must be closed or open")
        _number(
            l["concurrency"] if l["mode"] == "closed" else l["max_inflight"],
            "load capacity",
            1,
            True,
        )
        if l["mode"] == "open":
            _number(l["rate"], "load.rate", 0.000000001)
            if l.get("arrival", "regular") not in ("regular", "poisson"):
                raise ValueError("load.arrival must be regular or poisson")
        for name in ("duration_seconds", "timeout_seconds"):
            _number(config["measurement"][name], f"measurement.{name}", 0.000001)
        _number(
            config["measurement"].get("warmup_seconds", 0), "measurement.warmup_seconds"
        )

    def build_commands(self, config):
        self.validate(config)
        a = config["artifacts"]
        commands = []
        if "cockroach_build" in a:
            b = a["cockroach_build"]
            _fields(b, "argv cwd host_id env", "cockroach_build")
            if (
                not isinstance(b.get("argv"), list)
                or not b["argv"]
                or not all(isinstance(x, str) for x in b["argv"])
            ):
                raise ValueError(
                    "cockroach_build.argv must be an explicit native build argv"
                )
            commands.append(Command(**b, name="build-cockroach"))
        if a.get("client_source"):
            commands.append(
                Command(
                    [
                        a.get("go", "go"),
                        "build",
                        "-mod=readonly",
                        "-o",
                        artifact_path(config, "sql_client", for_build=True),
                        ".",
                    ],
                    cwd=a["client_source"],
                    env={"GOPROXY": "off", "GOSUMDB": "off", "GOTOOLCHAIN": "local"},
                    name="build-sql-client",
                )
            )
        return commands

    def plan_cell(self, config, allocation, workspace):
        self.validate(config)
        total = (
            config["load"]["concurrency"]
            if config["load"]["mode"] == "closed"
            else config["load"]["max_inflight"]
        )
        if not allocation or sum(p["slots"] for p in allocation) != total:
            raise ValueError("Allocation must exactly preserve global CRDB slots")
        if config["load"]["mode"] == "open" and not math.isclose(
            sum(p["rate"] for p in allocation), config["load"]["rate"]
        ):
            raise ValueError("Allocation must preserve global new-transaction rate")
        if len({p["process_id"] for p in allocation}) != len(allocation):
            raise ValueError("Process IDs must be globally unique")
        hosts = {h["id"]: h for h in config["inventory"]}
        t, w, a = config["topology"], config["workload"], config["artifacts"]
        root = Path(workspace)
        cockroach, client = artifact_path(config, "cockroach"), artifact_path(
            config, "sql_client"
        )
        plan = CellPlan()

        def endpoint(n, key):
            address = n.get(
                "advertise_address", hosts[n["host_id"]].get("address", "localhost")
            )
            if ":" in address and not address.startswith("["):
                address = f"[{address}]"
            return f"{address}:{n[key]}"

        urls = [
            f'postgresql://root@{endpoint(n,"sql_port")}/defaultdb?sslmode=disable'
            for n in t["nodes"]
        ]
        metrics = [f'http://{endpoint(n,"http_port")}/_status/vars' for n in t["nodes"]]
        joins = ",".join(endpoint(n, "rpc_port") for n in t["nodes"])
        env = {
            "COCKROACH_JUICER": str(config["juicer"]["enabled"]).lower(),
            "COCKROACH_JUICER_HOLD_CALIBRATION": str(
                config["juicer"].get("hold_calibration", False)
            ).lower(),
        }
        env.update(
            {
                f"COCKROACH_JUICER_{k}": (
                    str(v).lower() if isinstance(v, bool) else str(v)
                )
                for k, v in config["juicer"].get("env", {}).items()
            }
        )
        for n in t["nodes"]:
            directory = root / n["id"]
            plan.prepare.append(
                Command(
                    ["mkdir", "-p", str(directory)],
                    host_id=n["host_id"],
                    name=f'prepare-{n["id"]}',
                )
            )
            argv = [
                cockroach,
                "start",
                "--insecure",
                "--store",
                str(directory / "store"),
                "--listen-addr",
                f'0.0.0.0:{n["rpc_port"]}',
                "--advertise-addr",
                endpoint(n, "rpc_port"),
                "--sql-addr",
                f'0.0.0.0:{n["sql_port"]}',
                "--advertise-sql-addr",
                endpoint(n, "sql_port"),
                "--http-addr",
                f'0.0.0.0:{n["http_port"]}',
                "--join",
                joins,
                "--cache",
                str(t.get("cache_size", "64MiB")),
                "--max-sql-memory",
                str(t.get("max_sql_memory", "64MiB")),
                "--log",
                json.dumps(
                    {
                        "file-defaults": {"dir": str(directory / "logs")},
                        "sinks": {"stderr": {"filter": "INFO"}},
                    }
                ),
            ]
            plan.servers.append(
                Command(argv, host_id=n["host_id"], env=env, name=f'crdb-{n["id"]}')
            )
        timeout = config["measurement"]["timeout_seconds"]
        plan.readiness.append(
            Command(
                [
                    cockroach,
                    "init",
                    "--insecure",
                    "--host",
                    endpoint(t["nodes"][0], "rpc_port"),
                ],
                host_id=t["nodes"][0]["host_id"],
                name="init-cluster",
                timeout_seconds=timeout,
            )
        )
        for i, p in enumerate(allocation):
            if (
                p["host_id"] not in hosts
                or "client" not in hosts[p["host_id"]]["roles"]
            ):
                raise ValueError("Allocated host must have client role")
            _number(p["slots"], "allocated slots", 1, True)
            pid = p["process_id"]
            if not isinstance(pid, int) or pid < 0:
                raise ValueError("process_id must be a nonnegative integer")
            native = dict(
                keys=w["keys"],
                ops_per_txn=w["transaction_size"],
                zipf_theta=w["zipf"],
                duplicates=not w["distinct_keys"],
                read_txn_pct=w["read_fraction"] * 100,
                seed=(w["seed"] + pid) % 2**64,
                scramble=w.get("scramble", True),
                mode=config["load"]["mode"],
                arrival=config["load"].get("arrival", "regular"),
                rate=p.get("rate", 0),
                slots=p["slots"],
                urls=urls,
                node_ids=[n["id"] for n in t["nodes"]],
                layout=t.get("layout"),
                hold_calibration=config["juicer"].get("hold_calibration", False),
                warmup_seconds=config["measurement"].get("warmup_seconds", 0),
                duration_seconds=config["measurement"]["duration_seconds"],
                request_timeout_seconds=w.get("request_timeout_seconds", 30),
                max_retries=w.get("retries", 0),
                retry_backoff_seconds=w.get("retry_backoff_seconds", 0.001),
                load_batch_rows=w.get("load_batch_rows", 10000),
                replicas=t.get("replicas", min(3, len(t["nodes"]))),
                splits=t.get("splits", 0),
                metrics_urls=metrics if i == 0 else [],
                output=str(root / f"client-{pid}"),
                artifact_paths={"cockroach": cockroach, "sql_client": client},
                artifact_sha256={
                    k: (
                        a[k].get("sha256", a.get(k + "_sha256", ""))
                        if isinstance(a[k], dict)
                        else a.get(k + "_sha256", "")
                    )
                    for k in ("cockroach", "sql_client")
                },
                source_revision=a["source_revision"],
                source_manifest=(
                    artifact_path(config, "source_manifest")
                    if "source_manifest" in a
                    else ""
                ),
            )
            path = str(root / f"client-{pid}.json")
            plan.files.append(
                GeneratedFile(
                    path,
                    json.dumps(native, sort_keys=True, indent=2) + "\n",
                    host_id=p["host_id"],
                )
            )
            plan.prepare.append(
                Command(
                    [client, "verify", "--config", path],
                    host_id=p["host_id"],
                    name=f"verify-client-{pid}",
                    timeout_seconds=timeout,
                )
            )
            plan.collect.setdefault(p["host_id"], []).append(
                str(root / f"client-{pid}.provenance.json")
            )
            if i == 0:
                for n in t["nodes"]:
                    node_native = dict(
                        native,
                        output=str(root / n["id"]),
                        metrics_urls=[],
                        log_dir=str(root / n["id"] / "logs"),
                        juicer_enabled=config["juicer"]["enabled"],
                    )
                    node_path = str(root / f'node-{n["id"]}.json')
                    plan.files.append(
                        GeneratedFile(
                            node_path,
                            json.dumps(node_native, indent=2) + "\n",
                            host_id=n["host_id"],
                        )
                    )
                    plan.prepare.append(
                        Command(
                            [client, "verify", "--config", node_path],
                            host_id=n["host_id"],
                            name=f'verify-{n["id"]}',
                            timeout_seconds=timeout,
                        )
                    )
                    plan.finalize.append(
                        Command(
                            [client, "evidence", "--config", node_path],
                            host_id=n["host_id"],
                            name=f'evidence-{n["id"]}',
                            timeout_seconds=timeout,
                        )
                    )
                    plan.collect.setdefault(n["host_id"], []).extend(
                        [
                            str(root / f'{n["id"]}.provenance.json'),
                            str(root / f'{n["id"]}.evidence.json'),
                        ]
                    )
                plan.readiness.extend(
                    [
                        Command(
                            [
                                client,
                                "ready",
                                "--config",
                                path,
                                "--timeout",
                                str(timeout),
                            ],
                            host_id=p["host_id"],
                            name="sql-ready",
                            timeout_seconds=timeout,
                        ),
                        Command(
                            [client, "load", "--config", path],
                            host_id=p["host_id"],
                            name="reset-load",
                            timeout_seconds=timeout,
                        ),
                    ]
                )
                plan.collect.setdefault(p["host_id"], []).extend(
                    [
                        str(root / f"client-{pid}.load.json"),
                        str(root / f"client-{pid}.metrics.jsonl"),
                    ]
                )
            plan.clients.append(
                Command(
                    [client, "run", "--config", path],
                    host_id=p["host_id"],
                    name=f"sql-client-{pid}",
                    timeout_seconds=timeout,
                )
            )
            plan.collect.setdefault(p["host_id"], []).extend(
                str(root / f"client-{pid}.{suffix}")
                for suffix in ("events.jsonl", "summary.json")
            )
        if "layout" in t:
            first = allocation[0]
            host = first["host_id"]
            prefix = root / f"client-{first['process_id']}"
            path = str(prefix) + ".json"
            plan.readiness.append(
                Command(
                    [client, "layout", "--config", path, "--timeout", str(timeout)],
                    host_id=host,
                    name="place-hot-ranges",
                    timeout_seconds=timeout,
                )
            )
            plan.observers.append(
                Command(
                    [client, "watch-layout", "--config", path],
                    host_id=host,
                    name="verify-live-layout",
                    timeout_seconds=timeout,
                )
            )
            plan.finalize.append(
                Command(
                    [client, "layout-check", "--config", path],
                    host_id=host,
                    name="verify-final-layout",
                    timeout_seconds=timeout,
                )
            )
            plan.collect.setdefault(host, []).extend(
                str(prefix) + suffix
                for suffix in (
                    ".layout.json",
                    ".layout-monitor.jsonl",
                    ".layout-final.json",
                )
            )
        plan.metadata = dict(
            adapter="crdb_bench",
            adapter_version="0.1.0",
            source_revision=a["source_revision"],
            artifacts=a,
            allocation=allocation,
            workload=config["workload"],
            juicer=config["juicer"],
            data_reset="Fresh per-cell stores, deterministic SQL generate_series load",
            path_layout="Workspace root shared logical path on every host; collector prefixes host_id",
            metric_window="Client execution start and terminal completion both within measurement; server counters separate scrape window",
            security="Insecure SQL/RPC; use only isolated trusted experiment networks",
        )
        plan.metadata["measurement_coordination"] = "native-gate-v1"
        return plan

    def parse_results(self, config, collected_dir):
        from .results import parse_results

        return parse_results(config, Path(collected_dir))
