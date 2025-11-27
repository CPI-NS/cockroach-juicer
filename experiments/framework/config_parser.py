import yaml
from dataclasses import dataclass, field
from typing import List, Dict, Any
from pathlib import Path


@dataclass
class WorkloadConfig:
    tx_count: List[int] = field(default_factory=lambda: [1000])
    ops_per_tx: List[int] = field(default_factory=lambda: [10])
    key_range: List[int] = field(default_factory=lambda: [100])
    distribution: List[str] = field(default_factory=lambda: ["uniform"])
    zipfian_s: List[float] = field(default_factory=lambda: [1.1])
    zipfian_v: List[float] = field(default_factory=lambda: [1.0])
    read_write_ratio: List[float] = field(default_factory=lambda: [0.5])


@dataclass
class ConcurrencyConfig:
    num_clients: List[int] = field(default_factory=lambda: [1])
    workers_per_client: List[int] = field(default_factory=lambda: [10])


@dataclass
class JuicerConfig:
    enabled: List[bool] = field(default_factory=lambda: [True, False])
    flush_time_us: List[int] = field(default_factory=lambda: [100])


@dataclass
class ProtocolConfig:
    type: List[str] = field(default_factory=lambda: ["2PL-WW"])


@dataclass
class ExperimentConfig:
    name: str = "cool-experiment-name"
    output_dir: str = "./results"
    repeat_count: int = 1
    timeout_seconds: int = 300
    workload: WorkloadConfig = field(default_factory=WorkloadConfig)
    concurrency: ConcurrencyConfig = field(default_factory=ConcurrencyConfig)
    juicer: JuicerConfig = field(default_factory=JuicerConfig)
    protocol: ProtocolConfig = field(default_factory=ProtocolConfig)


def load_config(config_path: str) -> ExperimentConfig:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path, 'r') as f:
        data = yaml.safe_load(f)

    if not data:
        raise ValueError(f"Empty or invalid YAML file: {config_path}")

    exp_data = data.get('experiment', {})
    name = exp_data.get('name', 'unnamed-experiment')
    output_dir = exp_data.get('output_dir', './results')
    repeat_count = exp_data.get('repeat_count', 1)
    timeout_seconds = exp_data.get('timeout_seconds', 300)

    workload_data = data.get('workload', {})
    workload = WorkloadConfig(
        tx_count=_ensure_list(workload_data.get('tx_count', [1000])),
        ops_per_tx=_ensure_list(workload_data.get('ops_per_tx', [10])),
        key_range=_ensure_list(workload_data.get('key_range', [100])),
        distribution=_ensure_list(workload_data.get('distribution', ['uniform'])),
        zipfian_s=_ensure_list(workload_data.get('zipfian_s', [1.1])),
        zipfian_v=_ensure_list(workload_data.get('zipfian_v', [1.0])),
        read_write_ratio=_ensure_list(workload_data.get('read_write_ratio', [0.5]))
    )

    conc_data = data.get('concurrency', {})
    concurrency = ConcurrencyConfig(
        num_clients=_ensure_list(conc_data.get('num_clients', [1])),
        workers_per_client=_ensure_list(conc_data.get('workers_per_client', [10]))
    )

    juicer_data = data.get('juicer', {})
    juicer = JuicerConfig(
        enabled=_ensure_list(juicer_data.get('enabled', [True, False])),
        flush_time_us=_ensure_list(juicer_data.get('flush_time_us', [100]))
    )

    protocol_data = data.get('protocol', {})
    protocol = ProtocolConfig(
        type=_ensure_list(protocol_data.get('type', ['2PL-WW']))
    )

    return ExperimentConfig(
        name=name,
        output_dir=output_dir,
        repeat_count=repeat_count,
        timeout_seconds=timeout_seconds,
        workload=workload,
        concurrency=concurrency,
        juicer=juicer,
        protocol=protocol
    )


def _ensure_list(value):
    """Ensure value is a list."""
    if not isinstance(value, list):
        return [value]
    return value


def generate_experiment_matrix(config: ExperimentConfig) -> List[Dict[str, Any]]:
    """generates all experiment combinations from configuration"""
    import itertools

    #getting the cartesian product of all parameter lists so we 
    #cover all outcomes and comparisons
    param_names = []
    param_values = []

    
    param_names.extend(['tx_count', 'ops_per_tx', 'key_range', 'distribution',
                        'zipfian_s', 'zipfian_v', 'read_write_ratio'])
    param_values.extend([
        config.workload.tx_count,
        config.workload.ops_per_tx,
        config.workload.key_range,
        config.workload.distribution,
        config.workload.zipfian_s,
        config.workload.zipfian_v,
        config.workload.read_write_ratio
    ])

    param_names.extend(['num_clients', 'workers_per_client'])
    param_values.extend([
        config.concurrency.num_clients,
        config.concurrency.workers_per_client
    ])

    
    param_names.extend(['juicer_enabled', 'flush_time_us'])
    param_values.extend([
        config.juicer.enabled,
        config.juicer.flush_time_us
    ])

    
    param_names.append('protocol')
    param_values.append(config.protocol.type)

    
    experiments = []
    for values in itertools.product(*param_values):
        exp_params = dict(zip(param_names, values))

        if exp_params['distribution'] == 'uniform':
            exp_params['zipfian_s'] = None
            exp_params['zipfian_v'] = None
        experiments.append(exp_params)

    return experiments


def example():
    import sys
    if len(sys.argv) > 1:
        config = load_config(sys.argv[1])
        print(f"Loaded config: {config.name}")
        experiments = generate_experiment_matrix(config)
        print(f"Generated {len(experiments)} experiment configurations")
        if experiments:
            print(f"Example experiment: {experiments[0]}")

if __name__ == '__main__':
    example()