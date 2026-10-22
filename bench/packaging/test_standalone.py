"""Opt-in installed-package acceptance; never starts a native workload.

Run with STANDALONE_BENCH pointing to an independently archived bench directory
whose .venv has the locked dependencies and this adapter wheel installed.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import tomllib
import unittest
import zipfile

CONFIG = "examples/local.json"
CELL_COUNT = 1


@unittest.skipUnless(os.environ.get("STANDALONE_BENCH"), "requires explicit standalone installation")
class InstalledPackageTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bench = Path(os.environ["STANDALONE_BENCH"]).resolve()
        cls.pin = json.loads((cls.bench / "packaging/common-pin.json").read_text())
        cls.metadata = tomllib.loads((cls.bench / "pyproject.toml").read_text())
        cls.console = next(iter(cls.metadata["project"]["scripts"]))
        cls.module = cls.metadata["project"]["scripts"][cls.console].split(".")[0]
        cls.venv = cls.bench / ".venv"
        cls.environment = {key: value for key, value in os.environ.items()
                           if key not in {"PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV"}}
        cls.environment["PYTHONNOUSERSITE"] = "1"

    def command(self, executable, *args):
        # Neither the live system source nor juicer-project is the working dir.
        with tempfile.TemporaryDirectory(prefix="installed-package-cwd-") as cwd:
            result = subprocess.run([str(executable), *map(str, args)], cwd=cwd,
                                    env=self.environment, text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout

    def test_artifact_and_generated_lock_pin(self):
        wheel = self.bench / self.pin["wheel"]
        digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
        self.assertEqual(digest, self.pin["wheel_sha256"])
        lock = tomllib.loads((self.bench / "uv.lock").read_text())
        common = next(p for p in lock["package"] if p["name"] == "experiment-common")
        self.assertEqual(common["version"], self.pin["version"])
        self.assertEqual(common["source"], {"path": self.pin["wheel"]})
        self.assertEqual(common["wheels"][0]["hash"], "sha256:" + digest)
        self.assertEqual(self.pin["source_kind"], "local_git_archive")
        self.assertFalse(self.pin["remote_publication_verified"])

    def test_isolated_runtime_and_source_bytes(self):
        script = """
import importlib, importlib.metadata as md, json, pathlib, sys
modules = [importlib.import_module(name) for name in sys.argv[1:]]
print(json.dumps({"prefix": sys.prefix, "origins": [m.__file__ for m in modules],
                  "versions": {name: md.version(name) for name in
                               ["experiment-common", sys.argv[2].replace("_", "-"), "PyYAML"]},
                  "adapters": [e.name for e in md.entry_points(group="experiment_common.adapters")]}))
"""
        data = json.loads(self.command(self.venv / "bin/python", "-I", "-c", script,
                                       "experiment_common", self.module, "yaml"))
        self.assertEqual(Path(data["prefix"]).resolve(), self.venv.resolve())
        for origin in data["origins"]:
            self.assertTrue(Path(origin).resolve().is_relative_to(self.venv))
        self.assertEqual(data["versions"], {"experiment-common": "0.1.0",
                                           self.metadata["project"]["name"]: "0.1.0",
                                           "PyYAML": "6.0.3"})
        self.assertIn("crdb", data["adapters"])
        common_root = Path(data["origins"][0]).parent.parent
        with zipfile.ZipFile(self.bench / self.pin["wheel"]) as archive:
            sources = [name for name in archive.namelist()
                       if name.startswith("experiment_common/") and name.endswith(".py")]
            self.assertGreater(len(sources), 0)
            for name in sources:
                self.assertEqual((common_root / name).read_bytes(), archive.read(name), name)
            # A template-only common fix must reach the installed data files too.
            resource = "share/experiment-common/examples/evaluation/T-CC-09.yaml"
            member = next(name for name in archive.namelist()
                          if name.endswith(".data/data/" + resource))
            self.assertEqual((self.venv / resource).read_bytes(), archive.read(member))

    def test_locked_sync_rejects_missing_or_changed_artifact(self):
        # Exercise uv's enforcement, rather than only comparing our manifest.
        import shutil
        for case in ("missing", "changed"):
            with self.subTest(case=case), tempfile.TemporaryDirectory(prefix="bad-package-") as directory:
                bench = Path(directory) / "bench"
                bench.mkdir()
                for name in ("pyproject.toml", "uv.lock"):
                    shutil.copy2(self.bench / name, bench / name)
                if case == "changed":
                    wheel = bench / self.pin["wheel"]
                    wheel.parent.mkdir(parents=True)
                    shutil.copy2(self.bench / self.pin["wheel"], wheel)
                    with zipfile.ZipFile(wheel, "a") as archive:
                        archive.writestr("intentional-test-change.txt", "modified artifact")
                result = subprocess.run(
                    [os.environ.get("UV_BIN", "uv"), "sync", "--frozen", "--offline",
                     "--no-install-project", "--no-python-downloads", "--python",
                     str(self.venv / "bin/python"), "--project", str(bench)],
                    cwd=directory, env=self.environment, text=True,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn(Path(self.pin["wheel"]).name, result.stdout)
                if case == "changed":
                    self.assertIn("Hash mismatch", result.stdout)


    def test_public_console_and_discovered_adapter(self):
        with tempfile.TemporaryDirectory(prefix="installed-validate-output-") as output:
            for index, console in enumerate((self.console, "experiment-common")):
                actual = json.loads(self.command(self.venv / "bin" / console, "validate",
                    "--config", self.bench / CONFIG, "--output", Path(output) / str(index)))
                self.assertEqual(actual, {"valid": True, "cell_count": CELL_COUNT})
        self.command(self.venv / "bin/experiment-infra", "--help")


if __name__ == "__main__":
    unittest.main(verbosity=2)

