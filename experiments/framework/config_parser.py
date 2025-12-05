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
    # Open-loop mode settings
    open_loop: bool = False
    target_rate: List[int] = field(default_factory=lambda: [100])
    duration_seconds: int = 60
    warmup_percent: float = 0.25
    cooldown_percent: float = 0.25


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
class ClusterConfig:
    """Configuration for CockroachDB cluster setup."""
    num_nodes: int = 1
    base_port: int = 26257
    base_http_port: int = 8080
    data_dir: str = "./cockroach-data"
    log_dir: str = "./logs"
    store_size: str = "10GB"


@dataclass
class DataInitConfig:
    """Configuration for benchmark data initialization."""
    enabled: bool = True
    num_keys: int = 10000
    key_prefix: str = "key"
    key_range: int = 10000
    batch_size: int = 100
    concurrent: int = 1
    use_bulk: bool = False


@dataclass
class RemoteNodeConfig:
    """Configuration for a single remote node."""
    hostname: str
    internal_ip: str
    role: str  # 'cockroach' or 'benchmark'
    node_id: int = 1
    ssh_port: int = 22  # SSH port (default 22)


@dataclass
class RemoteConfig:
    """Configuration for remote deployment (CloudLab/AWS)."""
    deployment_mode: str = "local"  # 'local' or 'remote'
    ssh_user: str = ""
    ssh_key: str = "~/.ssh/id_rsa"
    server_nodes: List[RemoteNodeConfig] = field(default_factory=list)
    client_nodes: List[RemoteNodeConfig] = field(default_factory=list)
    cockroach_bin_remote: str = ""
    benchmark_bin_remote: str = ""
    deploy_binaries: bool = True
    local_build_dir: str = "../../"


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
    cluster: ClusterConfig = field(default_factory=ClusterConfig)
    data_init: DataInitConfig = field(default_factory=DataInitConfig)
    remote: RemoteConfig = field(default_factory=RemoteConfig)


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
        read_write_ratio=_ensure_list(workload_data.get('read_write_ratio', [0.5])),
        # Open-loop mode settings
        open_loop=workload_data.get('open_loop', False),
        target_rate=_ensure_list(workload_data.get('target_rate', [100])),
        duration_seconds=workload_data.get('duration_seconds', 60),
        warmup_percent=workload_data.get('warmup_percent', 0.25),
        cooldown_percent=workload_data.get('cooldown_percent', 0.25)
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

    cluster_data = data.get('cluster', {})
    cluster = ClusterConfig(
        num_nodes=cluster_data.get('num_nodes', 1),
        base_port=cluster_data.get('base_port', 26257),
        base_http_port=cluster_data.get('base_http_port', 8080),
        data_dir=cluster_data.get('data_dir', './cockroach-data'),
        log_dir=cluster_data.get('log_dir', './logs'),
        store_size=cluster_data.get('store_size', '10GB')
    )

    data_init_data = data.get('data_init', {})
    data_init = DataInitConfig(
        enabled=data_init_data.get('enabled', True),
        num_keys=data_init_data.get('num_keys', 10000),
        key_prefix=data_init_data.get('key_prefix', 'key'),
        key_range=data_init_data.get('key_range', 10000),
        batch_size=data_init_data.get('batch_size', 100),
        concurrent=data_init_data.get('concurrent', 1),
        use_bulk=data_init_data.get('use_bulk', False)
    )

    # Parse remote deployment configuration
    remote_data = data.get('remote', {})
    deployment_mode = exp_data.get('deployment_mode', 'local')

    server_nodes = []
    client_nodes = []

    if remote_data:
        # Parse server nodes
        nodes_data = remote_data.get('nodes', {})
        for server_data in nodes_data.get('servers', []):
            server_nodes.append(RemoteNodeConfig(
                hostname=server_data['hostname'],
                internal_ip=server_data['internal_ip'],
                role=server_data.get('role', 'cockroach'),
                node_id=server_data.get('node_id', 1),
                ssh_port=server_data.get('ssh_port', 22)
            ))

        # Parse client nodes
        for client_data in nodes_data.get('clients', []):
            client_nodes.append(RemoteNodeConfig(
                hostname=client_data['hostname'],
                internal_ip=client_data['internal_ip'],
                role=client_data.get('role', 'benchmark'),
                node_id=client_data.get('node_id', 1),
                ssh_port=client_data.get('ssh_port', 22)
            ))

    remote_paths = remote_data.get('remote_paths', {})
    remote = RemoteConfig(
        deployment_mode=deployment_mode,
        ssh_user=remote_data.get('ssh_user', ''),
        ssh_key=remote_data.get('ssh_key', '~/.ssh/id_rsa'),
        server_nodes=server_nodes,
        client_nodes=client_nodes,
        cockroach_bin_remote=remote_paths.get('cockroach_bin', ''),
        benchmark_bin_remote=remote_paths.get('benchmark_bin', ''),
        deploy_binaries=remote_data.get('deploy_binaries', True),
        local_build_dir=remote_data.get('local_build_dir', '../../')
    )

    return ExperimentConfig(
        name=name,
        output_dir=output_dir,
        repeat_count=repeat_count,
        timeout_seconds=timeout_seconds,
        workload=workload,
        concurrency=concurrency,
        juicer=juicer,
        protocol=protocol,
        cluster=cluster,
        data_init=data_init,
        remote=remote
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

    
    # Check if open-loop mode is enabled
    if config.workload.open_loop:
        # For open-loop, use target_rate instead of tx_count/workers_per_client
        param_names.extend(['target_rate', 'ops_per_tx', 'key_range', 'distribution',
                            'zipfian_s', 'zipfian_v', 'read_write_ratio'])
        param_values.extend([
            config.workload.target_rate,
            config.workload.ops_per_tx,
            config.workload.key_range,
            config.workload.distribution,
            config.workload.zipfian_s,
            config.workload.zipfian_v,
            config.workload.read_write_ratio
        ])
    else:
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

        # Add open-loop mode settings if enabled
        if config.workload.open_loop:
            exp_params['open_loop'] = True
            exp_params['duration_seconds'] = config.workload.duration_seconds
            exp_params['warmup_percent'] = config.workload.warmup_percent
            exp_params['cooldown_percent'] = config.workload.cooldown_percent

        experiments.append(exp_params)

    return experiments