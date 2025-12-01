import subprocess
import time
import os
import signal
from pathlib import Path
from typing import Optional, List, Dict, Any
import logging

# Optional: paramiko for SSH (remote deployments)
try:
    import paramiko
    PARAMIKO_AVAILABLE = True
except ImportError:
    PARAMIKO_AVAILABLE = False


class ClusterManager:

    def __init__(
        self,
        cockroach_bin: str = "../../cockroach",
        data_dir: str = "./cockroach-data",
        log_dir: str = "./logs",
        num_nodes: int = 1,
        base_port: int = 26257,
        base_http_port: int = 8080,
        # Remote deployment options
        remote_mode: bool = False,
        remote_nodes: Optional[List[Dict[str, Any]]] = None,
        ssh_user: Optional[str] = None,
        ssh_key: Optional[str] = None,
        remote_cockroach_bin: Optional[str] = None,
        remote_benchmark_bin: Optional[str] = None,
    ):
        """
        Initialize cluster manager (local or remote).

        Args:
            cockroach_bin: Path to local cockroach binary (for deployment)
            data_dir: Base data directory for CockroachDB nodes
            log_dir: Log directory
            num_nodes: Number of nodes in the cluster (1 for single-node)
            base_port: Base SQL port (nodes will use base_port, base_port+1, etc.)
            base_http_port: Base HTTP port (nodes will use base_http_port, base_http_port+1, etc.)
            remote_mode: If True, deploy to remote machines via SSH
            remote_nodes: List of remote node dicts with 'hostname', 'internal_ip', 'role'
            ssh_user: SSH username for remote access
            ssh_key: Path to SSH private key
            remote_cockroach_bin: Path to cockroach binary on remote machines
            remote_benchmark_bin: Path to benchmark binary on remote machines
        """
        self.cockroach_bin = Path(cockroach_bin)
        self.data_dir = Path(data_dir)
        self.log_dir = Path(log_dir)
        self.num_nodes = num_nodes
        self.base_port = base_port
        self.base_http_port = base_http_port
        self.processes: List[subprocess.Popen] = []
        self.logger = logging.getLogger(__name__)

        # Remote deployment settings
        self.remote_mode = remote_mode
        self.remote_nodes = remote_nodes or []
        self.ssh_user = ssh_user
        self.ssh_key = Path(ssh_key).expanduser() if ssh_key else None
        self.ssh_clients: Dict[str, Any] = {}
        self.remote_cockroach_bin = remote_cockroach_bin
        self.remote_benchmark_bin = remote_benchmark_bin

        if self.remote_mode and not PARAMIKO_AVAILABLE:
            raise RuntimeError("Remote mode requires 'paramiko' package. Install with: pip install paramiko")

    # ========================================================================
    # SSH Helper Methods (Remote Deployment)
    # ========================================================================

    def _get_ssh_client(self, hostname: str, port: int = 22):
        """Get or create SSH client for a remote host."""
        client_key = f"{hostname}:{port}"
        if client_key in self.ssh_clients:
            return self.ssh_clients[client_key]

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            if self.ssh_key:
                client.connect(
                    hostname,
                    port=port,
                    username=self.ssh_user,
                    key_filename=str(self.ssh_key),
                    timeout=10
                )
            else:
                client.connect(hostname, port=port, username=self.ssh_user, timeout=10)

            self.ssh_clients[client_key] = client
            self.logger.info(f"SSH connection established to {hostname}:{port}")
            return client

        except Exception as e:
            self.logger.error(f"Failed to connect to {hostname}:{port}: {e}")
            raise

    def _run_remote_command(
        self, hostname: str, command: str, background: bool = False, port: int = 22
    ) -> tuple[int, str, str]:
        """
        Execute command on remote host via SSH.

        Returns: (return_code, stdout, stderr)
        """
        client = self._get_ssh_client(hostname, port)

        try:
            if background:
                # Run in background using nohup
                bg_command = f"nohup {command} > /dev/null 2>&1 &"
                stdin, stdout, stderr = client.exec_command(bg_command)
                return (0, "", "")
            else:
                stdin, stdout, stderr = client.exec_command(command)
                stdout_str = stdout.read().decode()
                stderr_str = stderr.read().decode()
                return_code = stdout.channel.recv_exit_status()
                return (return_code, stdout_str, stderr_str)

        except Exception as e:
            self.logger.error(f"Command failed on {hostname}: {e}")
            return (-1, "", str(e))

    def _deploy_binary(self, local_path: Path, remote_path: str) -> bool:
        """Deploy binary to all remote server nodes."""
        if not self.remote_mode:
            return True

        server_nodes = [n for n in self.remote_nodes if n.role == 'cockroach']

        for node in server_nodes:
            hostname = node.hostname
            port = node.ssh_port
            self.logger.info(f"Deploying {local_path.name} to {hostname}:{port} -> {remote_path}")

            try:
                client = self._get_ssh_client(hostname, port)
                sftp = client.open_sftp()

                # Create remote directory if needed
                remote_dir = str(Path(remote_path).parent)
                try:
                    sftp.stat(remote_dir)
                except FileNotFoundError:
                    self.logger.info(f"Creating remote directory: {remote_dir}")
                    sftp.mkdir(remote_dir)

                # Upload binary
                sftp.put(str(local_path), remote_path)
                sftp.chmod(remote_path, 0o755)  # Make executable
                sftp.close()

                self.logger.info(f"Successfully deployed to {hostname}")

            except Exception as e:
                self.logger.error(f"Failed to deploy to {hostname}: {e}")
                return False

        return True

    def _close_ssh_connections(self):
        """Close all SSH connections."""
        for hostname, client in self.ssh_clients.items():
            try:
                client.close()
                self.logger.debug(f"Closed SSH connection to {hostname}")
            except Exception as e:
                self.logger.warning(f"Error closing connection to {hostname}: {e}")

        self.ssh_clients = {}

    # ========================================================================
    # Cluster Management (Local & Remote)
    # ========================================================================

    def start(self, insecure: bool = True, store_size: str = "10GB") -> bool:
        """Start CockroachDB cluster (local or remote)."""
        if self.is_running():
            self.logger.warning("Cluster already running")
            return True

        # Route to remote or local deployment
        if self.remote_mode:
            return self._start_remote_cluster(insecure, store_size)

        # Local deployment
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

    def _start_remote_cluster(self, insecure: bool, store_size: str) -> bool:
        """Start CockroachDB cluster on remote nodes."""
        server_nodes = [n for n in self.remote_nodes if n.get('role') == 'cockroach']

        if not server_nodes:
            self.logger.error("No server nodes defined in remote configuration")
            return False

        self.logger.info(f"Starting remote {len(server_nodes)}-node cluster")

        # Deploy binary to all nodes (local path -> remote path)
        remote_bin_path = self.remote_cockroach_bin or str(self.cockroach_bin)
        if not self._deploy_binary(self.cockroach_bin, remote_bin_path):
            self.logger.error("Failed to deploy CockroachDB binary")
            return False

        # Build join addresses using internal IPs
        join_addrs = [
            f"{node['internal_ip']}:{self.base_port}" for node in server_nodes
        ]
        join_str = ",".join(join_addrs)

        self.logger.info(f"Join addresses: {join_str}")

        # Start each node
        for i, node in enumerate(server_nodes):
            hostname = node['hostname']
            internal_ip = node['internal_ip']
            node_id = node.get('node_id', i + 1)

            self.logger.info(f"Starting node {node_id} on {hostname} ({internal_ip})")

            # Create remote directories
            self._run_remote_command(hostname, f"mkdir -p {self.data_dir}")
            self._run_remote_command(hostname, f"mkdir -p {self.log_dir}")

            # Build start command (use remote binary path)
            cmd_parts = [
                remote_bin_path,
                "start" if len(server_nodes) > 1 else "start-single-node",
                f"--store=path={self.data_dir}/node{node_id},size={store_size}",
                f"--listen-addr={internal_ip}:{self.base_port}",
                f"--http-addr={internal_ip}:{self.base_http_port}",
                f"--log-dir={self.log_dir}",
                "--cluster-name=default",
            ]

            if len(server_nodes) > 1:
                cmd_parts.append(f"--join={join_str}")

            if insecure:
                cmd_parts.append("--insecure")

            cmd = " ".join(cmd_parts)

            # Start node in background
            rc, stdout, stderr = self._run_remote_command(hostname, cmd, background=True)

            if rc != 0:
                self.logger.error(f"Failed to start node {node_id}: {stderr}")
                return False

            self.logger.info(f"Node {node_id} started on {hostname}")
            time.sleep(2)

        # Initialize cluster if multi-node
        if len(server_nodes) > 1:
            self.logger.info("Initializing remote cluster...")
            first_node = server_nodes[0]
            init_cmd = [
                remote_bin_path,
                "init",
                "--insecure" if insecure else "",
                f"--host={first_node['internal_ip']}:{self.base_port}",
            ]
            init_cmd_str = " ".join(init_cmd)

            rc, stdout, stderr = self._run_remote_command(
                first_node['hostname'], init_cmd_str
            )

            if rc != 0:
                self.logger.warning(
                    f"Cluster init returned {rc}: {stderr} (may already be initialized)"
                )

        # Wait for cluster to be ready
        return self._wait_until_ready_remote(timeout=30)

    def _wait_until_ready_remote(self, timeout: int = 30) -> bool:
        """Wait for remote cluster to be ready."""
        server_nodes = [n for n in self.remote_nodes if n.get('role') == 'cockroach']
        if not server_nodes:
            return False

        first_node = server_nodes[0]
        start_time = time.time()

        self.logger.info("Waiting for remote cluster to be ready...")

        while time.time() - start_time < timeout:
            try:
                remote_bin_path = self.remote_cockroach_bin or str(self.cockroach_bin)
                check_cmd = [
                    remote_bin_path,
                    "sql",
                    "--insecure",
                    f"--host={first_node['internal_ip']}:{self.base_port}",
                    "--execute=SELECT 1",
                ]
                check_cmd_str = " ".join(check_cmd)

                rc, stdout, stderr = self._run_remote_command(
                    first_node['hostname'], check_cmd_str
                )

                if rc == 0:
                    self.logger.info("Remote cluster is ready")
                    return True

            except Exception as e:
                self.logger.debug(f"Waiting for remote cluster: {e}")

            time.sleep(2)

        self.logger.error("Remote cluster did not become ready within timeout")
        return False

    # ========================================================================
    # Local Cluster Methods
    # ========================================================================

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
            "--cluster-name=default",  # Match benchmark client's default cluster name
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
                "--cluster-name=default",  # Match benchmark client's default cluster name
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
            f"--host=localhost:{self.base_port}",
            "--cluster-name=default",
        ]
        if insecure:
            init_cmd.append("--insecure")

        try:
            result = subprocess.run(init_cmd, capture_output=True, timeout=30)
            if result.returncode != 0:
                self.logger.warning(
                    f"Cluster init returned {result.returncode}: {result.stderr.decode()}"
                )
        except Exception as e:
            self.logger.warning(
                f"Cluster init failed (may already be initialized): {e}"
            )

        return self.wait_until_ready(timeout=60)

    def stop(self, graceful: bool = True) -> bool:
        """Stop cluster (local or remote)."""
        if self.remote_mode:
            return self._stop_remote_cluster(graceful)

        # Local stop
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

    def _stop_remote_cluster(self, graceful: bool = True) -> bool:
        """Stop remote CockroachDB cluster."""
        server_nodes = [n for n in self.remote_nodes if n.get('role') == 'cockroach']

        if not server_nodes:
            return True

        self.logger.info(f"Stopping {len(server_nodes)} remote node(s)...")

        for i, node in enumerate(server_nodes):
            hostname = node['hostname']
            self.logger.info(f"Stopping node {i + 1} on {hostname}")

            try:
                if graceful:
                    # Send SIGTERM to cockroach processes
                    stop_cmd = "pkill -TERM cockroach"
                else:
                    # Force kill
                    stop_cmd = "pkill -KILL cockroach"

                rc, _, _ = self._run_remote_command(hostname, stop_cmd)

                if rc == 0:
                    self.logger.info(f"Node {i + 1} stopped on {hostname}")
                else:
                    self.logger.warning(f"Stop command returned {rc} on {hostname}")

            except Exception as e:
                self.logger.error(f"Failed to stop node on {hostname}: {e}")

        # Close SSH connections
        self._close_ssh_connections()
        return True

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
        """Cleanup cluster data (local or remote)."""
        if self.is_running():
            self.stop()

        if self.remote_mode:
            self._cleanup_remote()
        else:
            # Local cleanup
            if self.data_dir.exists():
                import shutil
                shutil.rmtree(self.data_dir, ignore_errors=True)

    def _cleanup_remote(self):
        """Cleanup remote cluster data."""
        server_nodes = [n for n in self.remote_nodes if n.get('role') == 'cockroach']

        for node in server_nodes:
            hostname = node['hostname']
            self.logger.info(f"Cleaning up data on {hostname}")

            try:
                cleanup_cmd = f"rm -rf {self.data_dir}"
                self._run_remote_command(hostname, cleanup_cmd)
            except Exception as e:
                self.logger.warning(f"Cleanup failed on {hostname}: {e}")

        self._close_ssh_connections()

    def get_connection_string(
        self, db_name: str = "defaultdb", insecure: bool = True
    ) -> str:
        """Get PostgreSQL connection string for cluster."""
        ssl_mode = "disable" if insecure else "require"

        if self.remote_mode:
            # Use first server node's IP
            server_nodes = [n for n in self.remote_nodes if n.get('role') == 'cockroach']
            if server_nodes:
                host = server_nodes[0]['internal_ip']
                return f"postgresql://root@{host}:{self.base_port}/{db_name}?sslmode={ssl_mode}"

        # Local mode
        return f"postgresql://root@localhost:{self.base_port}/{db_name}?sslmode={ssl_mode}"

    def get_server_addresses(self) -> List[str]:
        """Get list of server addresses for benchmark clients."""
        if self.remote_mode:
            server_nodes = [n for n in self.remote_nodes if n.get('role') == 'cockroach']
            return [f"{node['internal_ip']}:{self.base_port}" for node in server_nodes]
        else:
            # Local mode
            return [f"localhost:{self.base_port + i}" for i in range(self.num_nodes)]