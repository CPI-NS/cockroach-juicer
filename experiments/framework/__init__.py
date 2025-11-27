"""
Experimental Framework for Cockroach-Juicer

This framework automates the process of running experiments on the CockroachDB-integrated
Juicer implementation. It supports:
- Workload generation with configurable parameters
- Automated experiment execution
- Results collection and statistical analysis
- Visualization and comparison plotting
"""

from .experiment_builder import ExperimentBuilder
from .config_parser import load_config, ExperimentConfig
from .metrics_collector import MetricsCollector, ExperimentResults

__all__ = [
    'ExperimentBuilder',
    'load_config',
    'ExperimentConfig',
    'MetricsCollector',
    'ExperimentResults',
]

__version__ = '0.1.0'
