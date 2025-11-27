import subprocess
import time
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

from .config_parser import ExperimentConfig, generate_experiment_matrix
from .workload_generator import WorkloadGenerator
from .cluster_manager import ClusterManager
from .metrics_collector import MetricsCollector, ExperimentResults, BenchmarkMetrics


class BenchmarkRunner:

    def __init__(
        self,
        config: ExperimentConfig,
        benchmark_bin: str = "../../bin/benchmark",
        cockroach_bin: str = "../../cockroach"
    ):
        
        self.config = config
        self.benchmark_bin = benchmark_bin
        self.cockroach_bin = cockroach_bin
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

        self.cluster_mgr = ClusterManager(
            cockroach_bin=self.cockroach_bin,
            data_dir=self.config.cluster.data_dir,
            log_dir=self.config.cluster.log_dir,
            num_nodes=self.config.cluster.num_nodes,
            base_port=self.config.cluster.base_port,
            base_http_port=self.config.cluster.base_http_port
        )

        self.logger.info(f"Starting {self.config.cluster.num_nodes}-node cluster")
        self.logger.info(f"  Base port: {self.config.cluster.base_port}")
        self.logger.info(f"  Base HTTP port: {self.config.cluster.base_http_port}")

        if not self.cluster_mgr.start(store_size=self.config.cluster.store_size):
            self.logger.error("Failed to start cluster")
            return results

        try:
            for exp_idx, exp_params in enumerate(experiments):
                config_id = self._generate_config_id(exp_params)
                self.logger.info(f"\n{'='*80}")
                self.logger.info(f"Configuration {exp_idx+1}/{len(experiments)}: {config_id}")
                self.logger.info(f"Parameters: {exp_params}")

                for trial in range(self.config.repeat_count):
                    self.logger.info(f"  Trial {trial+1}/{self.config.repeat_count}")

                    self.cluster_mgr.reset_database()

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

        cmd = self._build_benchmark_command(bench_params)

        log_dir = self.output_dir / config_id / f"trial_{trial}"
        log_dir.mkdir(parents=True, exist_ok=True)

        stdout_file = log_dir / "stdout.log"
        stderr_file = log_dir / "stderr.log"
        self.logger.info(f"    Executing: {' '.join(cmd)}")

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

    def _build_benchmark_command(self, bench_params: Dict[str, Any]) -> List[str]:
        cmd = [
            self.benchmark_bin,
            f"--url={bench_params['db_url']}",
            f"--tx-count={bench_params['workload']['tx_count']}",
            f"--ops-per-tx={bench_params['workload']['ops_per_tx']}",
            f"--key-range={bench_params['workload']['key_range']}",
            f"--distribution={bench_params['workload']['distribution']}",
            f"--workers={bench_params['total_workers']}",
            f"--protocol={bench_params['protocol']}"
        ]

        if bench_params['juicer_enabled']:
            cmd.append(f"--juicer-flush-time={bench_params['flush_time_us']}")

        if bench_params['workload']['distribution'] == 'zipfian':
            cmd.append(f"--zipfian-s={bench_params['workload']['zipfian_s']}")
            cmd.append(f"--zipfian-v={bench_params['workload']['zipfian_v']}")

        return cmd

    def _generate_config_id(self, exp_params: Dict[str, Any]) -> str:
                
        parts = []

        parts.append(exp_params['protocol'])

        if exp_params['juicer_enabled']:
            parts.append(f"juicer-{exp_params['flush_time_us']}us")
        else:
            parts.append("nojuicer")

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