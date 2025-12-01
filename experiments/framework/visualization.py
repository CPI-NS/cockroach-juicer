import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
import numpy as np
from pathlib import Path
from typing import Optional

from .metrics_collector import ExperimentResults


class ResultsVisualizer:
    
    def __init__(self, results: ExperimentResults, output_dir: str = "./plots"):
        
        self.results = results
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.results.compute_aggregates()

    def plot_latency_vs_flush_time(self, save_path: Optional[str] = None):

        flush_times = {}
        for config_id, data in self.results.results.items():
            params = data['params']
            if params.get('juicer_enabled'):
                flush_time = params['flush_time_us']
                if flush_time not in flush_times:
                    flush_times[flush_time] = {'p50': [], 'p99': []}

                agg = data['aggregated']
                flush_times[flush_time]['p50'].append(agg.mean_latency_p50)
                flush_times[flush_time]['p99'].append(agg.mean_latency_p99)

        if not flush_times:
            print("No Juicer-enabled results to plot")
            return

        sorted_times = sorted(flush_times.keys())
        p50_values = [np.mean(flush_times[t]['p50']) for t in sorted_times]
        p99_values = [np.mean(flush_times[t]['p99']) for t in sorted_times]

        fig, ax = plt.subplots(figsize=(10, 6))

        ax.plot(sorted_times, p50_values, marker='o', label='P50', linewidth=2)
        ax.plot(sorted_times, p99_values, marker='s', label='P99', linewidth=2)

        ax.set_xlabel('Flush Time (μs)', fontsize=12)
        ax.set_ylabel('Latency (ms)', fontsize=12)
        ax.set_title('Latency vs Juicer Flush Time', fontsize=14, fontweight='bold')
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3)

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        else:
            plt.savefig(self.output_dir / 'latency_vs_flush_time.png', dpi=300, bbox_inches='tight')

        plt.close()

    def plot_throughput_vs_concurrency(self, save_path: Optional[str] = None):
        
        worker_counts = {}
        for config_id, data in self.results.results.items():
            params = data['params']
            total_workers = params['num_clients'] * params['workers_per_client']

            if total_workers not in worker_counts:
                worker_counts[total_workers] = {'juicer': [], 'baseline': []}

            agg = data['aggregated']
            if params.get('juicer_enabled'):
                worker_counts[total_workers]['juicer'].append(agg.mean_throughput)
            else:
                worker_counts[total_workers]['baseline'].append(agg.mean_throughput)

        sorted_workers = sorted(worker_counts.keys())
        juicer_tput = [np.mean(worker_counts[w]['juicer']) if worker_counts[w]['juicer'] else 0
                       for w in sorted_workers]
        baseline_tput = [np.mean(worker_counts[w]['baseline']) if worker_counts[w]['baseline'] else 0
                         for w in sorted_workers]

        fig, ax = plt.subplots(figsize=(10, 6))

        x = np.arange(len(sorted_workers))
        width = 0.35

        ax.bar(x - width/2, juicer_tput, width, label='Juicer', alpha=0.8)
        ax.bar(x + width/2, baseline_tput, width, label='Baseline', alpha=0.8)

        ax.set_xlabel('Total Workers', fontsize=12)
        ax.set_ylabel('Throughput (ops/sec)', fontsize=12)
        ax.set_title('Throughput vs Concurrency Level', fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(sorted_workers)
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3, axis='y')

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        else:
            plt.savefig(self.output_dir / 'throughput_vs_concurrency.png', dpi=300, bbox_inches='tight')

        plt.close()

    def plot_abort_rate_comparison(self, save_path: Optional[str] = None):

        configs = {}
        for _, data in self.results.results.items():
            params = data['params']

            key = (params['tx_count'], params['ops_per_tx'], params['key_range'],
                   params['distribution'], params['num_clients'], params['workers_per_client'])

            if key not in configs:
                configs[key] = {'juicer': None, 'baseline': None}

            agg = data['aggregated']
            if params.get('juicer_enabled'):
                configs[key]['juicer'] = agg.mean_abort_rate
            else:
                configs[key]['baseline'] = agg.mean_abort_rate

        paired_configs = [(k, v) for k, v in configs.items()
                         if v['juicer'] is not None and v['baseline'] is not None]

        if not paired_configs:
            print("No paired Juicer/Baseline results to compare")
            return

        juicer_aborts = [v['juicer'] for k, v in paired_configs]
        baseline_aborts = [v['baseline'] for k, v in paired_configs]
        labels = [f"Config {i+1}" for i in range(len(paired_configs))]

        fig, ax = plt.subplots(figsize=(12, 6))

        x = np.arange(len(labels))
        width = 0.35

        ax.bar(x - width/2, juicer_aborts, width, label='Juicer', alpha=0.8, color='#2E86AB')
        ax.bar(x + width/2, baseline_aborts, width, label='Baseline', alpha=0.8, color='#A23B72')

        ax.set_xlabel('Configuration', fontsize=12)
        ax.set_ylabel('Abort Rate (%)', fontsize=12)
        ax.set_title('Abort Rate: Juicer vs Baseline', fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=45, ha='right')
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3, axis='y')

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        else:
            plt.savefig(self.output_dir / 'abort_rate_comparison.png', dpi=300, bbox_inches='tight')

        plt.close()

    def plot_latency_vs_contention(self, save_path: Optional[str] = None):
        
        key_ranges = {}
        for _, data in self.results.results.items():
            params = data['params']
            kr = params['key_range']

            if kr not in key_ranges:
                key_ranges[kr] = {'juicer_p50': [], 'baseline_p50': [],
                                  'juicer_p99': [], 'baseline_p99': []}

            agg = data['aggregated']
            if params.get('juicer_enabled'):
                key_ranges[kr]['juicer_p50'].append(agg.mean_latency_p50)
                key_ranges[kr]['juicer_p99'].append(agg.mean_latency_p99)
            else:
                key_ranges[kr]['baseline_p50'].append(agg.mean_latency_p50)
                key_ranges[kr]['baseline_p99'].append(agg.mean_latency_p99)

        sorted_ranges = sorted(key_ranges.keys())
        juicer_p50 = [np.mean(key_ranges[kr]['juicer_p50']) if key_ranges[kr]['juicer_p50'] else 0
                      for kr in sorted_ranges]
        baseline_p50 = [np.mean(key_ranges[kr]['baseline_p50']) if key_ranges[kr]['baseline_p50'] else 0
                        for kr in sorted_ranges]

        fig, ax = plt.subplots(figsize=(10, 6))

        ax.plot(sorted_ranges, juicer_p50, marker='o', label='Juicer P50', linewidth=2)
        ax.plot(sorted_ranges, baseline_p50, marker='s', label='Baseline P50', linewidth=2)

        ax.set_xlabel('Key Range (lower = higher contention)', fontsize=12)
        ax.set_ylabel('Latency P50 (ms)', fontsize=12)
        ax.set_title('Latency vs Contention Level', fontsize=14, fontweight='bold')
        ax.set_xscale('log')
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3)

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        else:
            plt.savefig(self.output_dir / 'latency_vs_contention.png', dpi=300, bbox_inches='tight')

        plt.close()

    def plot_eval_graph7_knee(self, save_path: Optional[str] = None):
        """
        Graph 7: Median Latencwhyy vs. Throughput (Line Graph)
        X-axis: Throughput (ops/sec)
        Y-axis: Median latency (ms)
        Lines: Different client counts (to find the "knee")
        """
        worker_data = {}  # worker_count -> {juicer: [(throughput, latency)], baseline: [...]}

        for config_id, data in self.results.results.items():
            params = data['params']
            total_workers = params['num_clients'] * params['workers_per_client']
            agg = data['aggregated']

            if total_workers not in worker_data:
                worker_data[total_workers] = {'juicer': [], 'baseline': []}

            point = (agg.mean_throughput, agg.mean_latency_p50)
            if params.get('juicer_enabled'):
                worker_data[total_workers]['juicer'].append(point)
            else:
                worker_data[total_workers]['baseline'].append(point)

        fig, ax = plt.subplots(figsize=(10, 7))

        # Plot baseline
        sorted_workers = sorted(worker_data.keys())
        baseline_throughputs = []
        baseline_latencies = []
        for w in sorted_workers:
            if worker_data[w]['baseline']:
                tput, lat = np.mean(worker_data[w]['baseline'], axis=0)
                baseline_throughputs.append(tput)
                baseline_latencies.append(lat)

        ax.plot(baseline_throughputs, baseline_latencies, marker='o', linewidth=2.5,
                markersize=8, label='Baseline', color='#A23B72')

        # Plot juicer
        juicer_throughputs = []
        juicer_latencies = []
        for w in sorted_workers:
            if worker_data[w]['juicer']:
                tput, lat = np.mean(worker_data[w]['juicer'], axis=0)
                juicer_throughputs.append(tput)
                juicer_latencies.append(lat)

        ax.plot(juicer_throughputs, juicer_latencies, marker='s', linewidth=2.5,
                markersize=8, label='Juicer', color='#2E86AB')

        ax.set_xlabel('Throughput (ops/sec)', fontsize=13)
        ax.set_ylabel('Median Latency (ms)', fontsize=13)
        ax.set_title('Graph 7: Finding the Knee - Latency vs. Throughput', fontsize=14, fontweight='bold')
        ax.legend(fontsize=12, loc='upper left')
        ax.grid(True, alpha=0.3)

        plt.tight_layout()

        save_file = save_path or (self.output_dir / 'eval_graph7_knee.png')
        plt.savefig(save_file, dpi=300, bbox_inches='tight')
        plt.close()

    def plot_eval_graph8_throughput_zipf(self, save_path: Optional[str] = None):
        """
        Graph 8: Throughput vs. Zipf (Bar Graph)
        X-axis: Zipf values [0.1, 0.4, 0.6, 0.8, 0.99, 1.2]
        Y-axis: Throughput (ops/sec)
        Bars: Baseline vs Juicer (grouped)
        """
        zipf_data = {}  # zipf_s -> {juicer: [], baseline: []}

        for config_id, data in self.results.results.items():
            params = data['params']
            if params['distribution'] != 'zipfian':
                continue

            zipf_s = params['zipfian_s']
            agg = data['aggregated']

            if zipf_s not in zipf_data:
                zipf_data[zipf_s] = {'juicer': [], 'baseline': []}

            if params.get('juicer_enabled'):
                zipf_data[zipf_s]['juicer'].append(agg.mean_throughput)
            else:
                zipf_data[zipf_s]['baseline'].append(agg.mean_throughput)

        sorted_zipf = sorted(zipf_data.keys())
        juicer_tput = [np.mean(zipf_data[z]['juicer']) if zipf_data[z]['juicer'] else 0
                       for z in sorted_zipf]
        baseline_tput = [np.mean(zipf_data[z]['baseline']) if zipf_data[z]['baseline'] else 0
                         for z in sorted_zipf]

        fig, ax = plt.subplots(figsize=(10, 6))

        x = np.arange(len(sorted_zipf))
        width = 0.35

        ax.bar(x - width/2, baseline_tput, width, label='Baseline', alpha=0.9, color='#A23B72')
        ax.bar(x + width/2, juicer_tput, width, label='Juicer', alpha=0.9, color='#2E86AB')

        ax.set_xlabel('Zipf Skew Parameter (s)', fontsize=13)
        ax.set_ylabel('Throughput (ops/sec)', fontsize=13)
        ax.set_title('Graph 8: Throughput vs. Contention (Zipf)', fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels([f"{z:.2f}" for z in sorted_zipf])
        ax.legend(fontsize=12)
        ax.grid(True, alpha=0.3, axis='y')

        plt.tight_layout()

        save_file = save_path or (self.output_dir / 'eval_graph8_throughput_zipf.png')
        plt.savefig(save_file, dpi=300, bbox_inches='tight')
        plt.close()

    def plot_eval_graph9_abort_zipf(self, save_path: Optional[str] = None):
        """
        Graph 9: Abort Rate vs. Zipf (Bar Graph)
        X-axis: Zipf values [0.1, 0.4, 0.6, 0.8, 0.99, 1.2]
        Y-axis: Abort rate (%)
        Bars: Baseline vs Juicer (grouped)
        """
        zipf_data = {}  # zipf_s -> {juicer: [], baseline: []}

        for config_id, data in self.results.results.items():
            params = data['params']
            if params['distribution'] != 'zipfian':
                continue

            zipf_s = params['zipfian_s']
            agg = data['aggregated']

            if zipf_s not in zipf_data:
                zipf_data[zipf_s] = {'juicer': [], 'baseline': []}

            if params.get('juicer_enabled'):
                zipf_data[zipf_s]['juicer'].append(agg.mean_abort_rate)
            else:
                zipf_data[zipf_s]['baseline'].append(agg.mean_abort_rate)

        sorted_zipf = sorted(zipf_data.keys())
        juicer_abort = [np.mean(zipf_data[z]['juicer']) if zipf_data[z]['juicer'] else 0
                        for z in sorted_zipf]
        baseline_abort = [np.mean(zipf_data[z]['baseline']) if zipf_data[z]['baseline'] else 0
                          for z in sorted_zipf]

        fig, ax = plt.subplots(figsize=(10, 6))

        x = np.arange(len(sorted_zipf))
        width = 0.35

        ax.bar(x - width/2, baseline_abort, width, label='Baseline', alpha=0.9, color='#A23B72')
        ax.bar(x + width/2, juicer_abort, width, label='Juicer', alpha=0.9, color='#2E86AB')

        ax.set_xlabel('Zipf Skew Parameter (s)', fontsize=13)
        ax.set_ylabel('Abort Rate (%)', fontsize=13)
        ax.set_title('Graph 9: Abort Rate vs. Contention (Zipf)', fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels([f"{z:.2f}" for z in sorted_zipf])
        ax.legend(fontsize=12)
        ax.grid(True, alpha=0.3, axis='y')

        plt.tight_layout()

        save_file = save_path or (self.output_dir / 'eval_graph9_abort_zipf.png')
        plt.savefig(save_file, dpi=300, bbox_inches='tight')
        plt.close()

    def generate_all_plots(self):

        print("Generating plots...")

        try:
            self.plot_latency_vs_flush_time()
            print(f"\tLatency vs Flush Time: {self.output_dir}/latency_vs_flush_time.png")
        except Exception as e:
            print(f"\tLatency vs Flush Time: {e}")

        try:
            self.plot_throughput_vs_concurrency()
            print(f"\tThroughput vs Concurrency: {self.output_dir}/throughput_vs_concurrency.png")
        except Exception as e:
            print(f"\tThroughput vs Concurrency: {e}")

        try:
            self.plot_abort_rate_comparison()
            print(f"\tAbort Rate Comparison: {self.output_dir}/abort_rate_comparison.png")
        except Exception as e:
            print(f"\tAbort Rate Comparison: {e}")

        try:
            self.plot_latency_vs_contention()
            print(f"\tContention Study: {self.output_dir}/latency_vs_contention.png")
        except Exception as e:
            print(f"\tContention Study: {e}")

        # Evaluation graphs (for paper)
        try:
            self.plot_eval_graph7_knee()
            print(f"\t[EVAL] Graph 7 (Knee): {self.output_dir}/eval_graph7_knee.png")
        except Exception as e:
            print(f"\t[EVAL] Graph 7 (Knee): {e}")

        try:
            self.plot_eval_graph8_throughput_zipf()
            print(f"\t[EVAL] Graph 8 (Throughput vs Zipf): {self.output_dir}/eval_graph8_throughput_zipf.png")
        except Exception as e:
            print(f"\t[EVAL] Graph 8 (Throughput vs Zipf): {e}")

        try:
            self.plot_eval_graph9_abort_zipf()
            print(f"\t[EVAL] Graph 9 (Abort vs Zipf): {self.output_dir}/eval_graph9_abort_zipf.png")
        except Exception as e:
            print(f"\t[EVAL] Graph 9 (Abort vs Zipf): {e}")