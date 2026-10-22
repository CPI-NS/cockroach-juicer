"""File entry for the standalone CockroachDB experiment CLI."""

from crdb_bench.__main__ import entrypoint


if __name__ == "__main__":
    raise SystemExit(entrypoint())
