#!/usr/bin/env python3
"""Try an LLM backend with a few real requests before a full run.

Loads the settings, sends two AS detection requests (and one favicon request
with ``--vision``) and prints what came back. It makes real requests: about
three, so a few cents or a sliver of a free quota. The answer cache is turned
off, so every run asks the server.

It checks the backend, not the model: it fails only when a request fails, the
answer cannot be parsed, the quota is used up or the settings are wrong. Siblings
that differ from the expected ones are printed as a warning.

Usage:
    python scripts/check_llm_backend.py                  # api.llm of config.yaml
    python scripts/check_llm_backend.py --llm workers-ai
    python scripts/check_llm_backend.py --llm workers-ai --vision
    python scripts/check_llm_backend.py --llm path/to/profile.yaml
"""

import argparse
import sys
import tempfile
import time
from pathlib import Path

import yaml

from borges.analyzers.llm_analyzer import ASRelationshipAnalyzer, FaviconAnalyzer
from borges.config import load_config, set_config
from borges.resources import (
    default_config_path,
    llm_profile_path,
    negative_samples_dir,
)
from borges.utils.llm_client import LLMQuotaExceeded

# (asn, notes, aka, expected siblings). Documentation ASNs (RFC 5398)
ROWS = [
    (
        64496,
        "We also operate AS64497 (our Brazilian network) and AS64498.",
        "",
        [64497, 64498],
    ),
    (64499, "Transit from AS64500 and AS64501. Peering policy: open.", "", []),
]


def load(config_path: Path, llm_profile: str | None, workdir: Path):
    """The configuration, with api.llm from ``llm_profile`` and no answer cache."""
    data = yaml.safe_load(config_path.read_text())
    if llm_profile is not None:
        llm = yaml.safe_load(llm_profile_path(llm_profile).read_text())
        data["api"] = {"llm": llm}
    data["api"]["llm"].pop("cache_dir", None)
    data["paths"]["base_dir"] = str(workdir / "data")
    data["logging"]["log_dir"] = str(workdir / "logs")
    merged = workdir / "config.yaml"
    merged.write_text(yaml.safe_dump(data))
    return load_config(merged)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config.yaml") if Path("config.yaml").exists() else None,
        help="Borges configuration (default: ./config.yaml, else the packaged one)",
    )
    parser.add_argument(
        "--llm",
        metavar="PROFILE",
        help="api.llm settings to use: a YAML file or a shipped profile name "
        "(workers-ai, ollama, ...)",
    )
    parser.add_argument(
        "--vision", action="store_true", help="also try the vision model"
    )
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        try:
            config = load(args.config or default_config_path(), args.llm, Path(tmp))
        except FileNotFoundError as e:
            print(f"Settings problem: {e}")
            return 1
        set_config(config)
        llm = config.api.llm
        print(f"Server: {llm.base_url or 'OpenAI API'}")
        print(f"Model: {llm.model}")

        failures = mismatches = 0
        try:
            analyzer = ASRelationshipAnalyzer()
            print("\nAS detection (the model's own answer, before the regex adds ASNs)")
            for asn, notes, aka, expected in ROWS:
                started = time.time()
                try:
                    response = analyzer.llm_client.invoke(
                        analyzer._messages(asn, notes, aka),
                        **analyzer._request_kwargs(),
                    )
                    got = sorted(analyzer.parser.parse(response.content)["ASs"])
                    status = "OK" if got == expected else "differs (model, not backend)"
                    mismatches += got != expected
                except LLMQuotaExceeded:
                    raise
                except Exception as e:
                    got, status = None, f"FAILED: {e}"
                    failures += 1
                print(
                    f"  AS{asn} ({time.time() - started:.1f} s): "
                    f"expected {expected}, got {got} {status}"
                )

            if args.vision:
                print(f"\nVision model: {llm.vision_model}")
                favicon = (negative_samples_dir() / "wordpress.png").read_bytes()
                started = time.time()
                analysis = FaviconAnalyzer().analyze_favicon(
                    favicon, ["https://example.com", "https://example.org"]
                )
                answer = analysis.llm_response or ""
                failed = not answer or answer.startswith("Error:")
                failures += failed
                print(f"  WordPress favicon ({time.time() - started:.1f} s): {answer}")
        except LLMQuotaExceeded as e:
            print(f"\nQuota used up: {e}")
            return 1
        except ValueError as e:
            print(f"\nSettings problem: {e}")
            return 1

        if failures:
            print(f"\n{failures} request(s) failed: the backend does not work yet.")
            return 1
        print("\nThe backend works.")
        if mismatches:
            print(
                f"Warning: {mismatches} answer(s) differ from the expected siblings. "
                "That is about the model's quality, not the backend; compare a "
                "sample of results before relying on this model."
            )
        return 0


if __name__ == "__main__":
    sys.exit(main())
