"""The standalone CLI delegates lifecycle to the same shared implementation."""

from experiment_common.cli import main
from . import CRDBAdapter


def entrypoint():
    return main(CRDBAdapter())


if __name__ == "__main__":
    raise SystemExit(entrypoint())
