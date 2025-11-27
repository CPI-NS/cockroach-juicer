#!/usr/bin/env python3
"""
Usage:
    # Run from YAML config
    python run_experiment.py config ../configs/juicer_flush_sweep.yaml

    # Generate plots from existing results
    python run_experiment.py plot ../results/flush_sweep/results_20250126_143022.json
"""

import sys
import argparse
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from framework.config_parser import load_config
from framework.benchmark_runner import BenchmarkRunner
from framework.visualization import ResultsVisualizer
from framework.metrics_collector import ExperimentResults
import json


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def run_from_config(config_path: str, dry_run: bool = False):
    print(f"Loading configuration from: {config_path}")
    config = load_config(config_path)

    if dry_run:
        print(f"\nExperiment: {config.name}")
        print(f"Output directory: {config.output_dir}")
        print(f"Repeat count: {config.repeat_count}")

        from framework.config_parser import generate_experiment_matrix
        experiments = generate_experiment_matrix(config)

        print(f"\nTotal configurations: {len(experiments)}")
        print(f"Total runs: {len(experiments) * config.repeat_count}")

        print("\nExample configurations:")
        for i, exp in enumerate(experiments[:5]):
            print(f"\n  Config {i+1}:")
            for key, value in sorted(exp.items()):
                print(f"    {key}: {value}")

        if len(experiments) > 5:
            print(f"\n  ... and {len(experiments) - 5} more configurations")

        return

    runner = BenchmarkRunner(config)
    results = runner.run_all()

    print("\n" + "="*80)
    print(results.summary())

    output_dir = Path(config.output_dir) / "plots"
    viz = ResultsVisualizer(results, str(output_dir))
    viz.generate_all_plots()

    print(f"\nPlots saved to: {output_dir}")


def generate_plots(results_json: str, output_dir: str = None):

    print(f"Loading results from: {results_json}")

    with open(results_json, 'r') as f:
        data = json.load(f)

    results = ExperimentResults(data['experiment_name'])

    for config_id, config_data in data['results'].items():
        params = config_data['params']
        agg = config_data['aggregated']

        from framework.metrics_collector import BenchmarkMetrics
        metrics = BenchmarkMetrics(
            latency_p50=agg['mean_latency_p50'],
            latency_p99=agg['mean_latency_p99'],
            throughput=agg['mean_throughput'],
            abort_rate=agg['mean_abort_rate']
        )
        results.add_result(config_id, params, metrics)

    if output_dir is None:
        output_dir = str(Path(results_json).parent / "plots")

    viz = ResultsVisualizer(results, output_dir)
    viz.generate_all_plots()

    print(f"\nPlots saved to: {output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description="Cockroach-Juicer Experiment Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    config_parser = subparsers.add_parser('config', help='Run from YAML config')
    config_parser.add_argument('config_file', help='Path to YAML configuration file')
    config_parser.add_argument('--dry-run', action='store_true', help='Show experiment matrix without running')
    config_parser.add_argument('-v', '--verbose', action='store_true', help='Verbose logging')

    plot_parser = subparsers.add_parser('plot', help='Generate plots from results JSON')
    plot_parser.add_argument('results_json', help='Path to results JSON file')
    plot_parser.add_argument('-o', '--output-dir', help='Output directory for plots')
    plot_parser.add_argument('-v', '--verbose', action='store_true', help='Verbose logging')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    setup_logging(args.verbose if hasattr(args, 'verbose') else False)

    try:
        if args.command == 'config':
            run_from_config(args.config_file, args.dry_run)
        elif args.command == 'plot':
            generate_plots(args.results_json, args.output_dir if hasattr(args, 'output_dir') else None)

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(130)
    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
