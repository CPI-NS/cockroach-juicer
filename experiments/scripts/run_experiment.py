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
import subprocess
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def check_and_setup_venv():
    """Check if required dependencies are available, offer to set up venv if not."""
    experiments_dir = Path(__file__).parent.parent.resolve()
    venv_dir = experiments_dir / "venv"
    setup_script = experiments_dir / "setup_venv.sh"

    # Try importing required packages
    missing_packages = []
    try:
        import yaml
    except ImportError:
        missing_packages.append("pyyaml")

    try:
        import numpy
    except ImportError:
        missing_packages.append("numpy")

    try:
        import matplotlib
    except ImportError:
        missing_packages.append("matplotlib")

    try:
        import paramiko
    except ImportError:
        missing_packages.append("paramiko")

    if not missing_packages:
        return True  # All dependencies available

    print("=" * 80)
    print("MISSING DEPENDENCIES")
    print("=" * 80)
    print(f"The following packages are required but not installed:")
    for pkg in missing_packages:
        print(f"  - {pkg}")
    print()

    if setup_script.exists():
        print(f"Setup script found at: {setup_script}")
        print()
        response = input("Would you like to set up the virtual environment now? (y/n): ").strip().lower()

        if response == 'y':
            print("\nRunning setup script...")
            try:
                result = subprocess.run(
                    ["bash", str(setup_script)],
                    cwd=str(experiments_dir),
                    check=True,
                    capture_output=False
                )
                print("\n" + "=" * 80)
                print("Setup complete!")
                print()
                if venv_dir.exists():
                    print("Virtual environment created. Rerun with:")
                    print(f"  source {venv_dir}/bin/activate")
                    print(f"  python3 scripts/{Path(__file__).name} <your-arguments>")
                else:
                    print("Dependencies installed to user directory.")
                    print("Simply rerun the script:")
                    print(f"  python3 scripts/{Path(__file__).name} <your-arguments>")
                print("=" * 80)
                sys.exit(0)
            except subprocess.CalledProcessError as e:
                print(f"\nError running setup script: {e}")
                sys.exit(1)
        else:
            print("\nPlease install dependencies manually:")
            print(f"  cd {experiments_dir}")
            print("  python3 -m venv venv")
            print("  source venv/bin/activate")
            print("  pip install -r requirements.txt")
            sys.exit(1)
    else:
        print("Please install dependencies:")
        print(f"  cd {experiments_dir}")
        print("  pip install -r requirements.txt")
        sys.exit(1)


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

    # Find cockroach-juicer root directory (2 levels up from experiments/)
    experiments_dir = Path(__file__).parent.parent.resolve()
    cockroach_juicer_root = experiments_dir.parent

    cockroach_bin = str(cockroach_juicer_root / "cockroach")
    benchmark_bin = str(cockroach_juicer_root / "bin" / "benchmark")

    runner = BenchmarkRunner(
        config,
        benchmark_bin=benchmark_bin,
        cockroach_bin=cockroach_bin
    )
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

    # Check and setup virtual environment if needed
    check_and_setup_venv()

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
