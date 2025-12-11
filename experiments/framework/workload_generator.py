#! this is just a dummy file for now -- i'm not sure how the acutal workload
# for crdb looks like so once i know that, this file will be updated. 

from pathlib import Path
from typing import Dict, Any, Optional
import json


class WorkloadGenerator:
    
    def __init__(self, cockroach_bin: str = "cockroach"):
        
        self.cockroach_bin = cockroach_bin

    def generate_workload(
        self,
        tx_count: int,
        ops_per_tx: int,
        key_range: int,
        distribution: str = "uniform",
        zipfian_s: Optional[float] = None,
        read_write_ratio: float = 0.5,
        output_file: Optional[str] = None
    ) -> Dict[str, Any]:
        
        workload_spec = {
            'tx_count': tx_count,
            'ops_per_tx': ops_per_tx,
            'key_range': key_range,
            'distribution': distribution,
            'read_write_ratio': read_write_ratio
        }

        if distribution == "zipfian":
            if zipfian_s is None:
                raise ValueError("zipfian_s required for zipfian distribution")
            workload_spec['zipfian_s'] = zipfian_s

        if output_file:
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w') as f:
                json.dump(workload_spec, f, indent=2)

        return workload_spec

    def create_benchmark_params(
        self,
        workload_spec: Dict[str, Any],
        num_clients: int,
        workers_per_client: int,
        juicer_enabled: bool,
        flush_time_us: int,
        protocol: str,
        db_url: str = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
    ) -> Dict[str, Any]:
        
        params = {
            'workload': workload_spec,
            'num_clients': num_clients,
            'workers_per_client': workers_per_client,
            'total_workers': num_clients * workers_per_client,
            'juicer_enabled': juicer_enabled,
            'flush_time_us': flush_time_us if juicer_enabled else 0,
            'protocol': protocol,
            'db_url': db_url,
            'key_prefix': 'rmw-key-'
        }

        return params

    @staticmethod
    def get_key_name(key_id: int, prefix: str = "rmw-key-") -> str:
        
        return f"{prefix}{key_id:04d}"

    @staticmethod
    def estimate_runtime(
        tx_count: int,
        ops_per_tx: int,
        num_clients: int,
        workers_per_client: int,
        avg_latency_ms: float = 10.0
    ) -> float:
        total_workers = num_clients * workers_per_client
        txs_per_worker = tx_count / total_workers
        runtime_seconds = (txs_per_worker * avg_latency_ms) / 1000.0
        return runtime_seconds