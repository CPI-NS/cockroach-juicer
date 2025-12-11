import queue
import subprocess
import time
import logging
import socket
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime


from .config_parser import ExperimentConfig, generate_experiment_matrix
from .workload_generator import WorkloadGenerator
from .cluster_manager import ClusterManager
from .metrics_collector import MetricsCollector, ExperimentResults, BenchmarkMetrics

def format_ns(ns: int) -> str:
    if ns < 1_000:
        return f"{ns} ns"
    elif ns < 1_000_000:
        return f"{ns / 1_000:.3f} µs"
    elif ns < 1_000_000_000:
        return f"{ns / 1_000_000:.3f} ms"
    else:
        return f"{ns / 1_000_000_000:.3f} s"


class BenchmarkRunner:

    def __init__(
        self,
        config: ExperimentConfig,
        benchmark_bin: str = "../../bin/benchmark",
        cockroach_bin: str = "../../cockroach",
        skip_deploy: bool = False
    ):

        self.config = config
        self.benchmark_bin = benchmark_bin
        self.cockroach_bin = cockroach_bin
        self.skip_deploy = skip_deploy
        self.workload_gen = WorkloadGenerator(cockroach_bin)
        self.cluster_mgr: Optional[ClusterManager] = None
        self.logger = logging.getLogger(__name__)

        self.output_dir = Path(config.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def run_all(self) -> ExperimentResults:
        
        results = ExperimentResults(self.config.name)

        experiments = generate_experiment_matrix(self.config)
        total_experiments = len(experiments) * self.config.repeat_count

        self.logger.info(f"Starting experiment: {self.config.name}")
        self.logger.info(f"Total configurations: {len(experiments)}")
        self.logger.info(f"Repeat count: {self.config.repeat_count}")
        self.logger.info(f"Total runs: {total_experiments}")

        # Prepare remote node list if remote mode
        remote_nodes = []
        if self.config.remote.deployment_mode == "remote":
            # Convert RemoteNodeConfig to dict format expected by ClusterManager
            for node in self.config.remote.server_nodes:
                remote_nodes.append({
                    'hostname': node.hostname,
                    'internal_ip': node.internal_ip,
                    'role': node.role,
                    'node_id': node.node_id,
                    'ssh_port': node.ssh_port
                })
            for node in self.config.remote.client_nodes:
                remote_nodes.append({
                    'hostname': node.hostname,
                    'internal_ip': node.internal_ip,
                    'role': node.role,
                    'node_id': node.node_id,
                    'ssh_port': node.ssh_port
                })

        self.cluster_mgr = ClusterManager(
            cockroach_bin=self.cockroach_bin,
            benchmark_bin=self.benchmark_bin,
            data_dir=self.config.cluster.data_dir,
            log_dir=self.config.cluster.log_dir,
            num_nodes=self.config.cluster.num_nodes,
            base_port=self.config.cluster.base_port,
            base_http_port=self.config.cluster.base_http_port,
            # Remote deployment parameters
            remote_mode=(self.config.remote.deployment_mode == "remote"),
            remote_nodes=remote_nodes,
            ssh_user=self.config.remote.ssh_user,
            ssh_key=self.config.remote.ssh_key,
            remote_cockroach_bin=self.config.remote.cockroach_bin_remote,
            remote_benchmark_bin=self.config.remote.benchmark_bin_remote,
            skip_deploy=self.skip_deploy
        )

        self.logger.info(f"Starting {self.config.cluster.num_nodes}-node cluster")
        self.logger.info(f"  Base port: {self.config.cluster.base_port}")
        self.logger.info(f"  Base HTTP port: {self.config.cluster.base_http_port}")

        if not self.cluster_mgr.start(store_size=self.config.cluster.store_size):
            self.logger.error("Failed to start cluster")
            return results

        # Configure Raft replication (for evaluation experiments)
        if self.config.cluster.num_nodes >= 3:
            self.logger.info("Configuring Raft replication factor...")
            if not self._configure_replication(replicas=3):
                self.logger.warning("Failed to configure replication (continuing anyway)")

        # Give cluster time to fully stabilize
        # Multi-region clusters need more time for Raft and DistSender to be ready
        self.logger.info("Waiting for cluster to stabilize...")
        time.sleep(30)

        # Initialize data ONCE at the start (same data used for all configs)
        if self.config.data_init.enabled:
            self.logger.info("Initializing benchmark data (one-time setup)...")
            if not self._initialize_data():
                self.logger.error("Failed to initialize data")
                self.cluster_mgr.stop()
                return results
            self.logger.info("Data initialization completed")
            time.sleep(2)

        try:
            for exp_idx, exp_params in enumerate(experiments):
                config_id = self._generate_config_id(exp_params)
                self.logger.info(f"\n{'='*80}")
                self.logger.info(f"Configuration {exp_idx+1}/{len(experiments)}: {config_id}")
                self.logger.info(f"Parameters: {exp_params}")

                for trial in range(self.config.repeat_count):
                    self.logger.info(f"  Trial {trial+1}/{self.config.repeat_count}")

                    # Reset database between trials to clear any residual state
                    # This wipes transaction history but keeps the initialized data
                    self.cluster_mgr.reset_database()
                    time.sleep(2)

                    metrics = self._run_single_benchmark(exp_params, config_id, trial)

                    results.add_result(config_id, exp_params, metrics)

                    time.sleep(1)

        finally:
            if self.cluster_mgr:
                self.cluster_mgr.stop()

        self._save_results(results)

        return results

    def _run_single_benchmark(
        self,
        exp_params: Dict[str, Any],
        config_id: str,
        trial: int
    ) -> BenchmarkMetrics:

        # For open-loop mode, create bench_params directly
        if exp_params.get('open_loop', False):
            bench_params = {
                'workload': {
                    'open_loop': True,
                    'target_rate': exp_params['target_rate'],
                    'duration_seconds': exp_params['duration_seconds'],
                    'warmup_percent': exp_params['warmup_percent'],
                    'cooldown_percent': exp_params['cooldown_percent'],
                    'openloop_inflight': exp_params.get('openloop_inflight', 100),
                    'ops_per_tx': exp_params['ops_per_tx'],
                    'key_range': exp_params['key_range'],
                    'distribution': exp_params['distribution'],
                    'zipfian_s': exp_params.get('zipfian_s'),
                    'zipfian_v': exp_params.get('zipfian_v'),
                    'read_write_ratio': exp_params['read_write_ratio']
                },
                'num_clients': exp_params['num_clients'],
                'total_workers': exp_params['target_rate'],  # For open-loop, total_workers = target_rate
                'juicer_enabled': exp_params['juicer_enabled'],
                'flush_time_us': exp_params['flush_time_us'],
                'protocol': exp_params['protocol'],
                'db_url': self.cluster_mgr.get_connection_string()
            }
        else:
            # Closed-loop mode: use workload generator
            workload_spec = self.workload_gen.generate_workload(
                tx_count=exp_params['tx_count'],
                ops_per_tx=exp_params['ops_per_tx'],
                key_range=exp_params['key_range'],
                distribution=exp_params['distribution'],
                zipfian_s=exp_params.get('zipfian_s'),
                zipfian_v=exp_params.get('zipfian_v'),
                read_write_ratio=exp_params['read_write_ratio']
            )

            bench_params = self.workload_gen.create_benchmark_params(
                workload_spec=workload_spec,
                num_clients=exp_params['num_clients'],
                workers_per_client=exp_params['workers_per_client'],
                juicer_enabled=exp_params['juicer_enabled'],
                flush_time_us=exp_params['flush_time_us'],
                protocol=exp_params['protocol'],
                db_url=self.cluster_mgr.get_connection_string()
            )

        log_dir = self.output_dir / config_id / f"trial_{trial}"
        log_dir.mkdir(parents=True, exist_ok=True)

        # Route to remote or local execution
        if self.config.remote.deployment_mode == "remote":
            return self._run_remote_distributed_benchmark(bench_params, log_dir)
        else:
            return self._run_local_benchmark(bench_params, log_dir)

    def _run_local_benchmark(
        self,
        bench_params: Dict[str, Any],
        log_dir: Path
    ) -> BenchmarkMetrics:
        """Run benchmark locally (original implementation)."""
        cmd = self._build_benchmark_command(bench_params)

        stdout_file = log_dir / "stdout.log"
        stderr_file = log_dir / "stderr.log"
        self.logger.info(f"    Executing locally: {' '.join(cmd)}")

        try:
            with open(stdout_file, 'w') as stdout_f, open(stderr_file, 'w') as stderr_f:
                result = subprocess.run(
                    cmd,
                    stdout=stdout_f,
                    stderr=stderr_f,
                    timeout=self.config.timeout_seconds
                )

            with open(stdout_file, 'r') as f:
                stdout_content = f.read()

            metrics = MetricsCollector.parse_benchmark_output(stdout_content)

            self.logger.info(f"    Result: Latency P50={metrics.latency_p50:.2f}ms, "
                           f"P99={metrics.latency_p99:.2f}ms, "
                           f"Throughput={metrics.throughput:.2f} ops/sec, "
                           f"Abort Rate={metrics.abort_rate:.2f}%")

            return metrics

        except subprocess.TimeoutExpired:
            self.logger.error(f"    Benchmark timeout after {self.config.timeout_seconds}s")
            metrics = BenchmarkMetrics()
            metrics.errors.append(f"Timeout after {self.config.timeout_seconds}s")
            return metrics

        except Exception as e:
            self.logger.error(f"    Benchmark failed: {e}")
            metrics = BenchmarkMetrics()
            metrics.errors.append(str(e))
            return metrics

    def _run_remote_distributed_benchmark(
        self,
        bench_params: Dict[str, Any],
        log_dir: Path
    ) -> BenchmarkMetrics:
        """Run benchmark distributed across remote client instances."""
        import threading
        import queue
        import time

        client_nodes = self.config.remote.client_nodes
        num_clients = len(client_nodes)

        self.logger.info(f"    Distributing benchmark across {num_clients} remote clients")

        # Calculate synchronized start time
        # Give clients 10 seconds to connect and prepare
        sync_start_delay_seconds = 10
        sync_start_time_ms = int((time.time() + sync_start_delay_seconds) * 1000)

        self.logger.info(f"    Synchronized start time: {sync_start_time_ms}ms (+{sync_start_delay_seconds}s from now)")

        # Calculate workload per client
        # In open-loop mode, we don't divide transactions - each client runs for the same duration
        is_open_loop = bench_params['workload'].get('open_loop', False)
        if is_open_loop:
            tx_per_client = 0  # Not used in open-loop mode
            remainder = 0
        else:
            total_tx = bench_params['workload']['tx_count']
            tx_per_client = total_tx // num_clients
            remainder = total_tx % num_clients

        # Prepare to collect results from all clients
        results_queue = queue.Queue()
        threads = []

        def run_on_client(client_idx, client_node, tx_count, start_time_ms):
            """Execute benchmark on a single remote client."""
            hostname = client_node.hostname
            port = client_node.ssh_port

            try:
                # Build command for this client
                client_bench_params = bench_params.copy()
                client_bench_params['workload'] = bench_params['workload'].copy()

                # Only set tx_count in closed-loop mode
                if not bench_params['workload'].get('open_loop', False):
                    client_bench_params['workload']['tx_count'] = tx_count

                # Use remote benchmark binary path (same binary for both modes)
                remote_benchmark_bin = self.config.remote.benchmark_bin_remote or "/home/ubuntu/benchmark"

                # Check if open-loop mode is enabledh
                if bench_params['workload'].get('open_loop', False):
                    cmd = self._build_remote_openloop_command(client_bench_params, remote_benchmark_bin, start_time_ms)
                    duration = bench_params['workload'].get('duration_seconds', 60)
                    self.logger.info(f"      Client {client_idx} ({hostname}): Running for {duration}s")
                else:
                    cmd = self._build_remote_command(client_bench_params, remote_benchmark_bin)
                    self.logger.info(f"      Client {client_idx} ({hostname}): Running {tx_count} transactions")

                cmd_str = " ".join(cmd)
                self.logger.debug(f"      Command: {cmd_str}")

                # Execute on remote client
                rc, stdout, stderr = self.cluster_mgr._run_remote_command(
                    hostname,
                    cmd_str,
                    background=False,
                    port=port,
                    timeout=self.config.timeout_seconds
                )

                # Save logs for this client
                client_log_dir = log_dir / f"client_{client_idx}"
                client_log_dir.mkdir(parents=True, exist_ok=True)

                with open(client_log_dir / "stdout.log", 'w') as f:
                    f.write(stdout)
                with open(client_log_dir / "stderr.log", 'w') as f:
                    f.write(stderr)

                if rc == 0:
                    # Parse metrics from stdout
                    metrics = MetricsCollector.parse_benchmark_output(stdout)
                    results_queue.put(('success', client_idx, metrics, tx_count))
                    self.logger.info(f"      Client {client_idx}: ✓ Completed")
                else:
                    self.logger.error(f"      Client {client_idx}: Failed with return code {rc}")
                    self.logger.error(f"      stderr: {stderr[:500]}")
                    results_queue.put(('error', client_idx, stderr, tx_count))

            except Exception as e:
                self.logger.error(f"      Client {client_idx}: Exception: {e}")
                results_queue.put(('error', client_idx, str(e), tx_count))

        # Launch benchmarks on all clients in parallel
        for i, client_node in enumerate(client_nodes):
            # Give remainder transactions to first few clients
            tx_count = tx_per_client + (1 if i < remainder else 0)

            thread = threading.Thread(
                target=run_on_client,
                args=(i, client_node, tx_count, sync_start_time_ms)
            )
            thread.start()
            threads.append(thread)

        # Wait for all clients to complete
        for i, thread in enumerate(threads):
            thread.join(timeout=self.config.timeout_seconds)
            if thread.is_alive():
                self.logger.error(f"      Client {i} thread still running after {self.config.timeout_seconds}s timeout")
                # Thread will be left as daemon and eventually terminated
                # Put error result in queue so we don't wait indefinitely
                results_queue.put(('error', i, f"Thread timeout after {self.config.timeout_seconds}s", 0))

        # Aggregate results from all clients
        return self._aggregate_client_results(results_queue, num_clients)

    def _aggregate_client_results(
        self,
        results_queue: queue.Queue[Tuple[str, int, BenchmarkMetrics, int]],
        num_clients: int
    ) -> BenchmarkMetrics:
        """Aggregate metrics from multiple client instances."""
        successful_results = []
        errors = []
        total_tx = 0

        # Collect all results
        while not results_queue.empty():
            result = results_queue.get()
            if result[0] == 'success':
                _, client_idx, metrics, tx_count = result
                successful_results.append((metrics, metrics.tx_count))
                total_tx += tx_count
            else:
                _, client_idx, error_msg, _ = result
                errors.append(f"Client {client_idx}: {error_msg}")

        if not successful_results:
            self.logger.error("  *********************************  All clients failed! *********************************")
            time.sleep(240)
            metrics = BenchmarkMetrics()
            metrics.errors = errors
            return metrics

        if len(successful_results) < num_clients:
            self.logger.warning(f"    Only {len(successful_results)}/{num_clients} clients succeeded")

        # Weighted average for latencies (by transaction count)
        total_weight = sum(tx_count for _, tx_count in successful_results)
        total_attempts_count = sum(m.attempts_count for m, tx_count in successful_results)

        weighted_p50 = sum(m.latency_p50 * tx_count for m, tx_count in successful_results) / total_weight
        weighted_p99 = sum(m.latency_p99 * tx_count for m, tx_count in successful_results) / total_weight
        weighted_p999 = sum(m.latency_p999 * tx_count for m, tx_count in successful_results) / total_weight

        # Sum throughput (ops/sec from all clients combined)
        total_throughput = sum(m.throughput for m, _ in successful_results)

        # Weighted average for abort rate
        weighted_abort_rate = sum(m.abort_rate * m.attempts_count for m, tx_count in successful_results) / total_attempts_count
        
        
        
        # Create aggregated metrics
        aggregated = BenchmarkMetrics(
            latency_p50=weighted_p50 / 1000000,
            latency_p99=weighted_p99 / 1000000,
            latency_p999=weighted_p999 / 1000000,
            throughput=total_throughput,
            abort_rate=weighted_abort_rate
        )
        aggregated.errors = errors

        self.logger.info(f"    Aggregated Result: Latency P50={format_ns(weighted_p50)}, "
                    f"P99={format_ns(weighted_p99)}, "
                    f"P999={format_ns(weighted_p999)}, "
                    f"Throughput={aggregated.throughput:.2f} ops/sec, "
                    f"Abort Rate={aggregated.abort_rate:.2f}%")

        return aggregated

    
    def _initialize_data(self) -> bool:
        """Initialize benchmark data using the benchmark binary's --init flag."""
        cfg = self.config.data_init

        # Get server addresses from cluster manager (use public IPs for multi-region)
        addrs = self.cluster_mgr.get_server_addresses(use_public_ips=True)
        addrs_str = ",".join(addrs)

        # Check if we should use hash-based keys
        use_hash_keys = cfg.use_hash_keys if hasattr(cfg, 'use_hash_keys') else False

        if self.config.remote.deployment_mode == "remote":
            # Run initialization from first remote client
            client_node = self.config.remote.client_nodes[0]
            hostname = client_node.hostname
            port = client_node.ssh_port

            remote_benchmark_bin = self.config.remote.benchmark_bin_remote or "/home/ubuntu/benchmark"

            cmd = [
                remote_benchmark_bin,
                f"--addrs={addrs_str}",
                "--insecure",
                "--init",
                f"--init-keys={cfg.num_keys}",
                f"--init-prefix={cfg.key_prefix}",
                f"--init-range={cfg.key_range}",
                f"--init-batch={cfg.batch_size}",
                f"--init-concurrent={cfg.concurrent}"
            ]

            if cfg.use_bulk:
                cmd.append("--init-bulk")

            if use_hash_keys:
                # --use-hash-keys defaults to true in benchmark binary
                cmd.append("--use-hash-keys")

            cmd_str = " ".join(cmd)
            self.logger.info(f"  Running data init on remote client {hostname}: {cmd_str}")

            # Use separate timeout for data initialization (default 30 minutes)
            init_timeout = getattr(cfg, 'init_timeout_seconds', 1800)
            self.logger.info(f"  Init timeout: {init_timeout}s ({init_timeout/60:.1f} minutes)")

            try:
                self.logger.info(f"  Connecting to {hostname}:{port}...")

                # Force fresh SSH connection for init (avoid stale connection issues)
                client_key = f"{hostname}:{port}"
                if client_key in self.cluster_mgr.ssh_clients:
                    self.logger.info(f"  Closing existing SSH connection to {hostname}:{port}")
                    old_client = self.cluster_mgr.ssh_clients.pop(client_key)
                    try:
                        old_client.close()
                    except:
                        pass

                rc, stdout, stderr = self.cluster_mgr._run_remote_command(
                    hostname,
                    cmd_str,
                    background=False,
                    port=port,
                    timeout=init_timeout
                )

                if rc != 0:
                    self.logger.error(f"Data initialization failed (rc={rc})")
                    self.logger.error(f"STDERR: {stderr}")
                    self.logger.error(f"STDOUT: {stdout}")
                    return False

                self.logger.info("Data initialization completed successfully")
                self.logger.info(stdout)
                return True

            except socket.timeout:
                self.logger.error(f"Data initialization timed out after {init_timeout}s")
                self.logger.error("This may indicate network issues or the cluster is not responding")
                return False
            except Exception as e:
                self.logger.error(f"Data initialization error: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                return False

        else:
            # Local execution
            cmd = [
                self.benchmark_bin,
                f"--addrs={addrs_str}",
                "--insecure",
                "--init",
                f"--init-keys={cfg.num_keys}",
                f"--init-prefix={cfg.key_prefix}",
                f"--init-range={cfg.key_range}",
                f"--init-batch={cfg.batch_size}",
                f"--init-concurrent={cfg.concurrent}"
            ]

            if cfg.use_bulk:
                cmd.append("--init-bulk")

            if use_hash_keys:
                # --use-hash-keys defaults to true in benchmark binary
                cmd.append("--use-hash-keys")

            self.logger.info(f"  Running: {' '.join(cmd)}")

            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=self.config.timeout_seconds
                )

                if result.returncode != 0:
                    self.logger.error(f"Data initialization failed: {result.stderr}")
                    return False

                self.logger.info(result.stdout)
                return True

            except subprocess.TimeoutExpired:
                self.logger.error(f"Data initialization timeout after {self.config.timeout_seconds}s")
                return False
            except Exception as e:
                self.logger.error(f"Data initialization error: {e}")
                return False

    def _configure_replication(self, replicas: int = 3) -> bool:
        """Configure CockroachDB Raft replication factor."""
        try:
            # Get server addresses from cluster manager
            addrs = self.cluster_mgr.get_server_addresses()
            if not addrs:
                return False

            host = addrs[0]  # Use first server for SQL commands

            self.logger.info(f"  Setting replication factor to {replicas}...")

            # Check if running in remote mode
            if self.config.remote.deployment_mode == "remote":
                # Run on remote server
                server_node = self.config.remote.server_nodes[0]
                hostname = server_node.hostname
                port = server_node.ssh_port

                remote_bin_path = self.config.remote.cockroach_bin_remote or "/home/ubuntu/cockroach"

                cmd_str = f"{remote_bin_path} sql --insecure --host={host} --execute=\"ALTER RANGE default CONFIGURE ZONE USING num_replicas = {replicas};\""

                rc, stdout, stderr = self.cluster_mgr._run_remote_command(
                    hostname, cmd_str, background=False, port=port, timeout=30
                )

                if rc != 0:
                    self.logger.error(f"Failed to configure replication: {stderr}")
                    return False

                self.logger.info(f"  ✓ Replication factor set to {replicas}")
                return True
            else:
                # Run locally
                cmd = [
                    self.cockroach_bin,
                    "sql",
                    "--insecure",
                    f"--host={host}",
                    f"--execute=ALTER RANGE default CONFIGURE ZONE USING num_replicas = {replicas};",
                ]

                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=30
                )

                if result.returncode != 0:
                    self.logger.error(f"Failed to configure replication: {result.stderr}")
                    return False

                self.logger.info(f"  ✓ Replication factor set to {replicas}")
                return True

        except Exception as e:
            self.logger.error(f"Replication configuration error: {e}")
            return False

    def _build_benchmark_command(self, bench_params: Dict[str, Any]) -> List[str]:
        """Build benchmark command using new --addrs flag instead of --url."""
        # Get server addresses from cluster manager (handles both local and remote)
        addrs = self.cluster_mgr.get_server_addresses()
        addrs_str = ",".join(addrs)

        workload = bench_params['workload']

        # Check if open-loop mode
        if workload.get('open_loop', False):
            # Open-loop mode command
            cmd = [
                self.benchmark_bin,
                f"--addrs={addrs_str}",
                "--insecure",
                "--open-loop",
                f"--target-rate={workload['target_rate']}",
                f"--duration={workload['duration_seconds']}",
                f"--warmup-percent={workload['warmup_percent']}",
                f"--cooldown-percent={workload['cooldown_percent']}",
                f"--openloop-inflight={workload.get('openloop_inflight', 100)}",
                f"--ops-per-tx={workload['ops_per_tx']}",
                f"--key-range={workload['key_range']}",
                f"--key-prefix=key",
                f"--distribution={workload['distribution']}",
                f"--read-write-ratio={workload['read_write_ratio']}",
                f"--protocol={bench_params['protocol']}",
                "--use-hash-keys",
            ]
        else:
            # Closed-loop mode command
            cmd = [
                self.benchmark_bin,
                f"--addrs={addrs_str}",
                "--insecure",
                f"--tx-count={workload['tx_count']}",
                f"--ops-per-tx={workload['ops_per_tx']}",
                f"--key-range={workload['key_range']}",
                f"--distribution={workload['distribution']}",
                f"--read-write-ratio={workload['read_write_ratio']}",
                f"--workers={bench_params['total_workers']}",
                f"--protocol={bench_params['protocol']}",
            ]

        if bench_params['juicer_enabled']:
            cmd.append("--juicer")
            # Note: flush_time is currently hardcoded in pkg/rpc/juicer.go at 100μs
            # TODO: Add --juicer-flush-time flag to benchmark binary if configurable flush time is needed

        if workload['distribution'] == 'zipfian':
            cmd.append(f"--zipfian-s={workload['zipfian_s']}")
            cmd.append(f"--zipfian-v={workload['zipfian_v']}")

        return cmd

    def _build_remote_benchmark_command(self, bench_params: Dict[str, Any], remote_bin_path: str) -> List[str]:
        """Build benchmark command for remote execution."""
        # Get server addresses (use public IPs for multi-region deployments)
        addrs = self.cluster_mgr.get_server_addresses(use_public_ips=True)
        addrs_str = ",".join(addrs)

        cmd = [
            remote_bin_path,
            f"--addrs={addrs_str}",
            "--insecure",
            f"--tx-count={bench_params['workload']['tx_count']}",
            f"--ops-per-tx={bench_params['workload']['ops_per_tx']}",
            f"--key-range={bench_params['workload']['key_range']}",
            f"--distribution={bench_params['workload']['distribution']}",
            f"--read-write-ratio={bench_params['workload']['read_write_ratio']}",
            f"--workers={bench_params['total_workers']}",
            f"--protocol={bench_params['protocol']}",
        ]

        if bench_params['juicer_enabled']:
            cmd.append("--juicer")

        if bench_params['workload']['distribution'] == 'zipfian':
            cmd.append(f"--zipfian-s={bench_params['workload']['zipfian_s']}")
            cmd.append(f"--zipfian-v={bench_params['workload']['zipfian_v']}")

        return cmd

    def _build_remote_openloop_command(self, bench_params: Dict[str, Any], remote_bin_path: str, start_time_ms: int) -> List[str]:
        """Build open-loop benchmark command for remote execution with synchronized start."""
        # Get server addresses (use public IPs for multi-region deployments)
        addrs = self.cluster_mgr.get_server_addresses(use_public_ips=True)
        addrs_str = ",".join(addrs)

        # Calculate target rate per client
        # For open-loop, we distribute the target rate across clients
        # For now, use a simple approach: divide total workers by number of clients
        target_rate_per_client = bench_params.get('total_workers', 10)

        # Get workload params
        workload = bench_params['workload']
        duration_seconds = workload.get('duration_seconds', 60)
        warmup_percent = workload.get('warmup_percent', 0.25)
        cooldown_percent = workload.get('cooldown_percent', 0.25)
        openloop_inflight = workload.get('openloop_inflight', 100)

        cmd = [
            remote_bin_path,
            f"--addrs={addrs_str}",
            "--insecure",
            "--open-loop",  # Enable open-loop mode
            f"--target-rate={target_rate_per_client}",
            f"--duration={duration_seconds}",
            f"--start-time={start_time_ms}",
            f"--warmup-percent={warmup_percent}",
            f"--cooldown-percent={cooldown_percent}",
            f"--openloop-inflight={openloop_inflight}",
            f"--ops-per-tx={workload['ops_per_tx']}",
            f"--key-range={workload['key_range']}",
            f"--key-prefix=key",
            f"--distribution={workload['distribution']}",
            f"--read-write-ratio={workload['read_write_ratio']}",
            f"--protocol={bench_params['protocol']}",
            "--use-hash-keys",
        ]

        if bench_params['juicer_enabled']:
            cmd.append("--juicer")

        if workload['distribution'] == 'zipfian':
            cmd.append(f"--zipfian-s={workload['zipfian_s']}")
            cmd.append(f"--zipfian-v={workload['zipfian_v']}")

        return cmd

    def _generate_config_id(self, exp_params: Dict[str, Any]) -> str:

        parts = []

        parts.append(exp_params['protocol'])

        if exp_params['juicer_enabled']:
            parts.append(f"juicer-{exp_params['flush_time_us']}us")
        else:
            parts.append("nojuicer")

        # Handle both open-loop (target_rate) and closed-loop (tx_count) modes
        if exp_params.get('open_loop', False):
            parts.append(f"rate{exp_params['target_rate']}")
            parts.append(f"dur{exp_params['duration_seconds']}s")
        else:
            parts.append(f"tx{exp_params['tx_count']}")

        parts.append(f"ops{exp_params['ops_per_tx']}")
        parts.append(f"keys{exp_params['key_range']}")

        if exp_params['distribution'] == 'zipfian':
            parts.append(f"zipf{exp_params.get('zipfian_s', 1.1)}")
        else:
            parts.append("uniform")

        parts.append(f"c{exp_params['num_clients']}w{exp_params['workers_per_client']}")

        return "_".join(parts)

    def _save_results(self, results: ExperimentResults):
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        csv_path = self.output_dir / f"results_{timestamp}.csv"
        results.export_csv(str(csv_path))
        self.logger.info(f"Results saved to: {csv_path}")

        json_path = self.output_dir / f"results_{timestamp}.json"
        results.export_json(str(json_path))
        self.logger.info(f"Results saved to: {json_path}")

        summary_path = self.output_dir / f"summary_{timestamp}.txt"
        with open(summary_path, 'w') as f:
            f.write(results.summary())
        self.logger.info(f"Summary saved to: {summary_path}")