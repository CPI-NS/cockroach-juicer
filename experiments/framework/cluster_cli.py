#!/usr/bin/env python3
"""
Simple CLI wrapper around ClusterManager to start/stop/cleanup Cockroach clusters
(local or remote). Designed for quick operational control from the shell.

Usage examples:
  # Local 3-node cluster
  python cluster_cli.py start --nodes 3 --cockroach-bin ../../cockroach

  # Stop local cluster
  python cluster_cli.py stop

  # Remote cluster (nodes described in JSON)
  python cluster_cli.py start --remote \
    --nodes-file /path/nodes.json \
    --ssh-user ubuntu \
    --ssh-key ~/.ssh/aws-juicer-key.pem \
    --cockroach-bin ../../cockroach \
    --remote-cockroach-bin /home/ubuntu/cockroach \
    --remote-benchmark-bin /home/ubuntu/benchmark

  # Cleanup remote data dirs
  python cluster_cli.py cleanup --remote --nodes-file /path/nodes.json --ssh-user ubuntu --ssh-key ~/.ssh/aws-juicer-key.pem

Commands: start, stop, restart, status, reset-db, cleanup
"""

import argparse
import json
import sys
from pathlib import Path

from cluster_manager import ClusterManager


def load_remote_nodes(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"nodes file not found: {path}")
    with path.open("r") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("nodes file must be a JSON list of node dicts")
    return data


def build_manager(args: argparse.Namespace) -> ClusterManager:
    remote_nodes = []
    if args.remote:
        if not args.nodes_file:
            raise ValueError("--nodes-file is required in remote mode")
        remote_nodes = load_remote_nodes(Path(args.nodes_file))

    mgr = ClusterManager(
        cockroach_bin=args.cockroach_bin,
        benchmark_bin=args.benchmark_bin,
        data_dir=args.data_dir,
        log_dir=args.log_dir,
        num_nodes=args.nodes,
        base_port=args.base_port,
        base_http_port=args.base_http_port,
        remote_mode=args.remote,
        remote_nodes=remote_nodes,
        ssh_user=args.ssh_user,
        ssh_key=args.ssh_key,
        remote_cockroach_bin=args.remote_cockroach_bin,
        remote_benchmark_bin=args.remote_benchmark_bin,
        skip_deploy=args.skip_deploy,
    )
    return mgr


def cmd_start(mgr: ClusterManager, args: argparse.Namespace):
    ok = mgr.start(insecure=not args.secure, store_size=args.store_size)
    sys.exit(0 if ok else 1)


def cmd_stop(mgr: ClusterManager, args: argparse.Namespace):
    ok = mgr.stop(graceful=not args.force)
    sys.exit(0 if ok else 1)


def cmd_restart(mgr: ClusterManager, args: argparse.Namespace):
    mgr.stop(graceful=not args.force)
    ok = mgr.start(insecure=not args.secure, store_size=args.store_size)
    sys.exit(0 if ok else 1)


def cmd_cleanup(mgr: ClusterManager, args: argparse.Namespace):
    mgr.cleanup()
    sys.exit(0)


def cmd_reset_db(mgr: ClusterManager, args: argparse.Namespace):
    ok = mgr.reset_database(db_name=args.db)
    sys.exit(0 if ok else 1)


def cmd_status(mgr: ClusterManager, args: argparse.Namespace):
    addrs = mgr.get_server_addresses(use_public_ips=args.use_public_ips)
    if not addrs:
        print("No server addresses found")
        sys.exit(1)

    host = addrs[0]
    if mgr.remote_mode:
        # Run a simple SELECT via SSH on first node
        server_nodes = [n for n in mgr.remote_nodes if n.get("role") == "cockroach"]
        if not server_nodes:
            print("No remote server nodes available")
            sys.exit(1)
        first = server_nodes[0]
        remote_bin_path = mgr.remote_cockroach_bin or "/home/ubuntu/cockroach"
        cmd = f"{remote_bin_path} sql --insecure --host={host} --execute=SELECT 1"
        rc, out, err = mgr._run_remote_command(first["hostname"], cmd, port=first.get("ssh_port", 22), timeout=10)
        if rc == 0:
            print("Cluster reachable:")
            print(out.strip())
            sys.exit(0)
        print("Cluster not reachable:")
        print(err.strip())
        sys.exit(1)
    else:
        import subprocess

        cmd = [str(mgr.cockroach_bin), "sql", "--insecure", f"--host={host}", "--execute=SELECT 1"]
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode == 0:
            print("Cluster reachable:")
            print(res.stdout.strip())
            sys.exit(0)
        print("Cluster not reachable:")
        print(res.stderr.strip())
        sys.exit(1)


def parse_args():
    p = argparse.ArgumentParser(description="Cluster management CLI using ClusterManager")
    sub = p.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--cockroach-bin", default="../../cockroach", help="Local cockroach binary path")
    common.add_argument("--benchmark-bin", default="../../bin/benchmark", help="Local benchmark binary path")
    common.add_argument("--data-dir", default="./cockroach-data", help="Data directory (local or remote)")
    common.add_argument("--log-dir", default="./logs", help="Log directory (local or remote)")
    common.add_argument("--nodes", type=int, default=1, help="Number of nodes (local mode)")
    common.add_argument("--base-port", type=int, default=26257, help="Base SQL port")
    common.add_argument("--base-http-port", type=int, default=8080, help="Base HTTP port")
    common.add_argument("--store-size", default="10GB", help="Store size for start")
    common.add_argument("--remote", action="store_true", help="Enable remote mode")
    common.add_argument("--nodes-file", help="JSON file with remote node definitions")
    common.add_argument("--ssh-user", default="ubuntu", help="SSH username")
    common.add_argument("--ssh-key", help="SSH private key path")
    common.add_argument("--remote-cockroach-bin", default="/home/ubuntu/cockroach", help="Remote cockroach binary path")
    common.add_argument("--remote-benchmark-bin", default="/home/ubuntu/benchmark", help="Remote benchmark binary path")
    common.add_argument("--skip-deploy", action="store_true", help="Skip binary deployment in remote mode")
    common.add_argument("--secure", action="store_true", help="Run with security (default insecure)")

    # start
    s = sub.add_parser("start", parents=[common], help="Start cluster")
    s.set_defaults(func=cmd_start)

    # stop
    s = sub.add_parser("stop", parents=[common], help="Stop cluster")
    s.add_argument("--force", action="store_true", help="Force kill")
    s.set_defaults(func=cmd_stop)

    # restart
    s = sub.add_parser("restart", parents=[common], help="Restart cluster")
    s.add_argument("--force", action="store_true", help="Force kill on stop")
    s.set_defaults(func=cmd_restart)

    # reset-db
    s = sub.add_parser("reset-db", parents=[common], help="Drop and recreate database 'benchmark'")
    s.add_argument("--db", default="benchmark", help="Database name to recreate")
    s.set_defaults(func=cmd_reset_db)

    # cleanup
    s = sub.add_parser("cleanup", parents=[common], help="Stop and delete data directories")
    s.set_defaults(func=cmd_cleanup)

    # status
    s = sub.add_parser("status", parents=[common], help="Ping cluster with SELECT 1")
    s.add_argument("--use-public-ips", action="store_true", help="Use public hostnames for status")
    s.set_defaults(func=cmd_status)

    return p.parse_args()


def main():
    args = parse_args()
    mgr = build_manager(args)
    args.func(mgr, args)


if __name__ == "__main__":
    main()
