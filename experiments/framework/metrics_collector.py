import json
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
import statistics


@dataclass
class BenchmarkMetrics:
    tx_count: int = 0
    commit_count: int = 0
    abort_count: int = 0
    abort_rate: float = 0.0
    latency_p50: float = 0.0
    latency_p90: float = 0.0
    latency_p95: float = 0.0
    latency_p99: float = 0.0
    latency_p999: float = 0.0
    throughput: float = 0.0
    duration_seconds: float = 0.0
    errors: List[str] = field(default_factory=list)


@dataclass
class AggregatedMetrics:
    mean_latency_p50: float = 0.0
    std_latency_p50: float = 0.0
    mean_latency_p99: float = 0.0
    std_latency_p99: float = 0.0
    mean_throughput: float = 0.0
    std_throughput: float = 0.0
    mean_abort_rate: float = 0.0
    std_abort_rate: float = 0.0
    sample_count: int = 0


class MetricsCollector:

    @staticmethod
    def parse_benchmark_output(output: str) -> BenchmarkMetrics:
        
        metrics = BenchmarkMetrics()

        if match := re.search(r'Total.*?:\s*(\d+)', output, re.IGNORECASE):
            metrics.tx_count = int(match.group(1))

        if match := re.search(r'Commit.*?:\s*(\d+)', output, re.IGNORECASE):
            metrics.commit_count = int(match.group(1))

        if match := re.search(r'Abort.*?:\s*(\d+)', output, re.IGNORECASE):
            metrics.abort_count = int(match.group(1))

        
        if match := re.search(r'Abort\s+rate.*?:\s*([\d.]+)%', output, re.IGNORECASE):
            metrics.abort_rate = float(match.group(1))

        latency_patterns = {
            'p50': r'P50.*?:\s*([\d.]+)\s*(ms|μs|us)',
            'p90': r'P90.*?:\s*([\d.]+)\s*(ms|μs|us)',
            'p95': r'P95.*?:\s*([\d.]+)\s*(ms|μs|us)',
            'p99': r'P99[^.].*?:\s*([\d.]+)\s*(ms|μs|us)',
            'p999': r'P99\.9.*?:\s*([\d.]+)\s*(ms|μs|us)'
        }

        for pct, pattern in latency_patterns.items():
            if match := re.search(pattern, output, re.IGNORECASE):
                value = float(match.group(1))
                unit = match.group(2).lower()
                
                if unit in ('μs', 'us'):
                    value = value / 1000.0
                setattr(metrics, f'latency_{pct}', value)

        if match := re.search(r'Throughput.*?:\s*([\d.]+)', output, re.IGNORECASE):
            metrics.throughput = float(match.group(1))

        
        if match := re.search(r'Duration.*?:\s*([\d.]+)\s*s', output, re.IGNORECASE):
            metrics.duration_seconds = float(match.group(1))

        error_patterns = [
            r'ERROR:.*',
            r'FATAL:.*',
            r'panic:.*'
        ]
        
        for pattern in error_patterns:
            for match in re.finditer(pattern, output, re.MULTILINE):
                metrics.errors.append(match.group(0))

        return metrics

    @staticmethod
    def aggregate_metrics(metrics_list: List[BenchmarkMetrics]) -> AggregatedMetrics:
        
        if not metrics_list:
            return AggregatedMetrics()

        agg = AggregatedMetrics()
        agg.sample_count = len(metrics_list)

        p50_values = [m.latency_p50 for m in metrics_list if m.latency_p50 > 0]
        if p50_values:
            agg.mean_latency_p50 = statistics.mean(p50_values)
            agg.std_latency_p50 = statistics.stdev(p50_values) if len(p50_values) > 1 else 0.0

        p99_values = [m.latency_p99 for m in metrics_list if m.latency_p99 > 0]
        if p99_values:
            agg.mean_latency_p99 = statistics.mean(p99_values)
            agg.std_latency_p99 = statistics.stdev(p99_values) if len(p99_values) > 1 else 0.0

        tput_values = [m.throughput for m in metrics_list if m.throughput > 0]
        if tput_values:
            agg.mean_throughput = statistics.mean(tput_values)
            agg.std_throughput = statistics.stdev(tput_values) if len(tput_values) > 1 else 0.0

        abort_values = [m.abort_rate for m in metrics_list]
        if abort_values:
            agg.mean_abort_rate = statistics.mean(abort_values)
            agg.std_abort_rate = statistics.stdev(abort_values) if len(abort_values) > 1 else 0.0

        return agg


class ExperimentResults:

    def __init__(self, name: str):

        self.name = name
        self.results: Dict[str, Dict[str, Any]] = {}

    def add_result(
        self,
        config_id: str,
        config_params: Dict[str, Any],
        metrics: BenchmarkMetrics
    ):

        if config_id not in self.results:
            self.results[config_id] = {
                'params': config_params,
                'runs': []
            }

        self.results[config_id]['runs'].append(metrics)

    def compute_aggregates(self):
        
        for config_id, data in self.results.items():
            data['aggregated'] = MetricsCollector.aggregate_metrics(data['runs'])

    def export_csv(self, output_path: str):
        
        import csv

        self.compute_aggregates()

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)

            header = ['config_id'] + list(self.results[list(self.results.keys())[0]]['params'].keys()) + [
                'mean_latency_p50', 'std_latency_p50',
                'mean_latency_p99', 'std_latency_p99',
                'mean_throughput', 'std_throughput',
                'mean_abort_rate', 'std_abort_rate',
                'sample_count'
            ]
            writer.writerow(header)

            for config_id, data in self.results.items():
                agg = data['aggregated']
                row = [config_id] + list(data['params'].values()) + [
                    f"{agg.mean_latency_p50:.3f}", f"{agg.std_latency_p50:.3f}",
                    f"{agg.mean_latency_p99:.3f}", f"{agg.std_latency_p99:.3f}",
                    f"{agg.mean_throughput:.2f}", f"{agg.std_throughput:.2f}",
                    f"{agg.mean_abort_rate:.2f}", f"{agg.std_abort_rate:.2f}",
                    agg.sample_count
                ]
                writer.writerow(row)

    def export_json(self, output_path: str):
        self.compute_aggregates()

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        export_data = {
            'experiment_name': self.name,
            'results': {}
        }

        for config_id, data in self.results.items():
            export_data['results'][config_id] = {
                'params': data['params'],
                'aggregated': {
                    'mean_latency_p50': data['aggregated'].mean_latency_p50,
                    'std_latency_p50': data['aggregated'].std_latency_p50,
                    'mean_latency_p99': data['aggregated'].mean_latency_p99,
                    'std_latency_p99': data['aggregated'].std_latency_p99,
                    'mean_throughput': data['aggregated'].mean_throughput,
                    'std_throughput': data['aggregated'].std_throughput,
                    'mean_abort_rate': data['aggregated'].mean_abort_rate,
                    'std_abort_rate': data['aggregated'].std_abort_rate,
                    'sample_count': data['aggregated'].sample_count
                }
            }

        with open(output_file, 'w') as f:
            json.dump(export_data, f, indent=2)

    def summary(self) -> str:

        self.compute_aggregates()

        lines = [f"Experiment: {self.name}", "=" * 80]

        for config_id, data in self.results.items():
            agg = data['aggregated']
            lines.append(f"\nConfiguration: {config_id}")
            lines.append(f"  Parameters: {data['params']}")
            lines.append(f"  Latency P50: {agg.mean_latency_p50:.3f} ± {agg.std_latency_p50:.3f} ms")
            lines.append(f"  Latency P99: {agg.mean_latency_p99:.3f} ± {agg.std_latency_p99:.3f} ms")
            lines.append(f"  Throughput: {agg.mean_throughput:.2f} ± {agg.std_throughput:.2f} ops/sec")
            lines.append(f"  Abort Rate: {agg.mean_abort_rate:.2f} ± {agg.std_abort_rate:.2f} %")
            lines.append(f"  Sample Count: {agg.sample_count}")

        return "\n".join(lines)