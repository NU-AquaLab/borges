#!/usr/bin/env python3
"""Smoke-test a built wheel the way a ``pip install borges`` user gets it.

Installs the wheel into a fresh virtual environment, runs ``borges init`` in an
empty directory, and checks that the default configuration (prompts,
blocklists, PeeringDB exclusions) and the reference favicons are present.
No network access beyond installing dependencies, and no API key.

Usage: python scripts/check_wheel.py dist/borges-*.whl
"""

import subprocess
import sys
import tempfile
import venv
from pathlib import Path


def run(cmd, cwd=None) -> str:
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        sys.exit(f"FAILED: {' '.join(map(str, cmd))}\n{result.stdout}{result.stderr}")
    return result.stdout


def main() -> None:
    wheel = Path(sys.argv[1]).resolve()
    with tempfile.TemporaryDirectory() as tmp:
        env_dir, project = Path(tmp) / "env", Path(tmp) / "project"
        venv.create(env_dir, with_pip=True)
        bindir = env_dir / ("Scripts" if sys.platform == "win32" else "bin")
        python, borges = bindir / "python", bindir / "borges"
        run([python, "-m", "pip", "install", "--quiet", str(wheel)])

        project.mkdir()
        run([borges, "init"], cwd=project)
        print(run([borges, "version"], cwd=project).strip())
        run([borges, "pipeline", "list"], cwd=project)

        check = """
import yaml
from borges.resources import default_config_path, negative_samples_dir
cfg = yaml.safe_load(open("config.yaml"))["processing"]
for key in ("prompts", "asn_blocklist", "peeringdb_asn_exclusions",
            "favicon_blocklist", "domain_blocklist"):
    assert cfg.get(key), f"config.yaml from borges init is missing {key}"
assert 4004 in cfg["peeringdb_asn_exclusions"], "AS4004 exclusion missing"
assert "site-packages" in str(default_config_path()), default_config_path()
samples = list(negative_samples_dir().glob("*.png"))
assert len(samples) >= 10, f"only {len(samples)} reference favicons"
print(f"ok: full default config, {len(samples)} reference favicons")
"""
        print(run([python, "-c", check], cwd=project).strip())


if __name__ == "__main__":
    main()
