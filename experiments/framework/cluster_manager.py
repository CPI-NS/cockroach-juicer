import subprocess
import time
import os
import signal
from pathlib import Path
from typing import Optional, List
import logging


class ClusterManager:

    def __init__(
        self,
        cockroach_bin: str = "../../cockroach",
        data_dir: str = "./cockroach-data",
        log_dir: str = "./logs",
        num_nodes: int = 1,
        base_port: int = 26257,
        base_http_port: int = 8080,
    ):
        """
        Initialize cluster manager.

        Args:
            cockroach_bin: Path to cockroach binary
            data_dir: Base data directory for CockroachDB nodes
            log_dir: Log directory
            num_nodes: Number of nodes in the cluster (1 for single-node)
            base_port: Base SQL port (nodes will use base_port, base_port+1, etc.)
            base_http_port: Base HTTP port (nodes will use base_http_port, base_http_port+1, etc.)
        """
        self.cockroach_bin = Path(cockroach_bin)
        self.data_dir = Path(data_dir)
        self.log_dir = Path(log_dir)
        self.num_nodes = num_nodes
        self.base_port = base_port
        self.base_http_port = base_http_port
        self.processes: List[subprocess.Popen] = []
        self.logger = logging.getLogger(__name__)

    def start(self, insecure: bool = True, store_size: str = "10GB") -> bool:

        if self.is_running():
            self.logger.warning("Cluster already running")
            return True

        if not self.cockroach_bin.exists():
            self.logger.error(
                f"CockroachDB binary not found at: {self.cockroach_bin.resolve()}"
            )
            self.logger.error("Please build CockroachDB first:")
            self.logger.error("  cd /path/to/cockroach-juicer")
            self.logger.error("  ./dev build cockroach")
            return False

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        if self.num_nodes == 1:
            return self._start_single_node(insecure, store_size)
        else:
            return self._start_multi_node(insecure, store_size)

    def _start_single_node(self, insecure: bool, store_size: str) -> bool:
        port = self.base_port
        http_port = self.base_http_port

        cmd = [
            str(self.cockroach_bin),
            "start-single-node",
            f"--store=path={self.data_dir},size={store_size}",
            f"--listen-addr=localhost:{port}",
            f"--http-addr=localhost:{http_port}",
            f"--log-dir={self.log_dir}",
        ]

        if insecure:
            cmd.append("--insecure")

        try:
            log_file = self.log_dir / "cockroach.log"
            with open(log_file, "w") as f:
                process = subprocess.Popen(
                    cmd, stdout=f, stderr=subprocess.STDOUT, preexec_fn=os.setsid
                )
            self.processes.append(process)

            self.logger.info(f"Started single-node cluster on port {port}")
            return self.wait_until_ready(timeout=30)

        except Exception as e:
            self.logger.error(f"Failed to start single-node cluster: {e}")
            return False

    def _start_multi_node(self, insecure: bool, store_size: str) -> bool:

        join_addrs = [f"localhost:{self.base_port + i}" for i in range(self.num_nodes)]
        join_str = ",".join(join_addrs)

        self.logger.info(f"Starting {self.num_nodes}-node cluster")
        self.logger.info(f"Join addresses: {join_str}")

        for node_id in range(self.num_nodes):
            port = self.base_port + node_id
            http_port = self.base_http_port + node_id
            node_data_dir = self.data_dir / f"node{node_id + 1}"
            node_log_dir = self.log_dir / f"node{node_id + 1}"

            node_data_dir.mkdir(parents=True, exist_ok=True)
            node_log_dir.mkdir(parents=True, exist_ok=True)

            cmd = [
                str(self.cockroach_bin),
                "start",
                f"--store=path={node_data_dir},size={store_size}",
                f"--listen-addr=localhost:{port}",
                f"--http-addr=localhost:{http_port}",
                f"--join={join_str}",
                f"--log-dir={node_log_dir}",
            ]

            if insecure:
                cmd.append("--insecure")

            try:
                log_file = node_log_dir / "cockroach.log"
                with open(log_file, "w") as f:
                    process = subprocess.Popen(
                        cmd, stdout=f, stderr=subprocess.STDOUT, preexec_fn=os.setsid
                    )
                self.processes.append(process)
                self.logger.info(f"Started node {node_id + 1} on port {port}")

            except Exception as e:
                self.logger.error(f"Failed to start node {node_id + 1}: {e}")
                self.stop()
                return False

            time.sleep(1)

        self.logger.info("Initializing cluster...")
        init_cmd = [
            str(self.cockroach_bin),
            "init",
            "--insecure" if insecure else "",
            f"--host=localhost:{self.base_port}",
        ]
        try:
            result = subprocess.run(init_cmd, capture_output=True, timeout=10)
            if result.returncode != 0:
                self.logger.warning(
                    f"Cluster init returned {result.returncode}: {result.stderr.decode()}"
                )
        except Exception as e:
            self.logger.warning(
                f"Cluster init failed (may already be initialized): {e}"
            )

        return self.wait_until_ready(timeout=10)

    def stop(self, graceful: bool = True) -> bool:
        
        if not self.processes:
            return True

        self.logger.info(f"Stopping {len(self.processes)} node(s)...")

        try:
            for i, process in enumerate(self.processes):
                if process.poll() is None: 
                    if graceful:
                        
                        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                        
                        try:
                            process.wait(timeout=10)
                            self.logger.info(f"Node {i + 1} stopped gracefully")
                        except subprocess.TimeoutExpired:
                            self.logger.warning(
                                f"Node {i + 1} shutdown timeout, forcing..."
                            )
                            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                    else:
                        
                        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                        process.wait(timeout=5)
                        self.logger.info(f"Node {i + 1} force killed")

            self.processes = []
            return True

        except Exception as e:
            self.logger.error(f"Failed to stop cluster: {e}")
            return False

    def is_running(self) -> bool:
        
        if not self.processes:
            return False

        return any(p.poll() is None for p in self.processes)

    def wait_until_ready(self, timeout: int = 30) -> bool:
        
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                
                result = subprocess.run(
                    [
                        str(self.cockroach_bin),
                        "sql",
                        "--insecure",
                        f"--host=localhost:{self.base_port}",
                        "--execute=SELECT 1",
                    ],
                    capture_output=True,
                    timeout=2,
                )

                if result.returncode == 0:
                    self.logger.info("Cluster is ready")
                    return True

            except subprocess.TimeoutExpired:
                pass
            except Exception as e:
                self.logger.debug(f"Waiting for cluster: {e}")

            time.sleep(1)

        self.logger.error("Cluster did not become ready within timeout")
        return False

    def reset_database(self, db_name: str = "defaultdb") -> bool:
        try:
            
            result = subprocess.run(
                [
                    str(self.cockroach_bin),
                    "sql",
                    "--insecure",
                    f"--host=localhost:{self.base_port}",
                    "--execute=DROP DATABASE IF EXISTS benchmark CASCADE; CREATE DATABASE benchmark;",
                ],
                capture_output=True,
                timeout=10,
            )

            return result.returncode == 0

        except Exception as e:
            self.logger.error(f"Failed to reset database: {e}")
            return False

    def cleanup(self):
        
        if self.is_running():
            self.stop()

        if self.data_dir.exists():
            import shutil

            shutil.rmtree(self.data_dir, ignore_errors=True)

    def get_connection_string(
        self, db_name: str = "defaultdb", insecure: bool = True
    ) -> str:
        ssl_mode = "disable" if insecure else "require"
        return (
            f"postgresql://root@localhost:{self.base_port}/{db_name}?sslmode={ssl_mode}"
        )