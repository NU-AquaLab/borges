"""Default files shipped with Borges: configuration and reference favicons.

Wheels carry them under ``borges/defaults/`` (copied at build time from the
repository's ``config.yaml`` and ``data/reference/``). Editable installs read
the repository files directly.
"""

from pathlib import Path

_PACKAGED = Path(__file__).parent / "defaults"
_REPO_ROOT = Path(__file__).resolve().parents[2]


def _first_existing(*candidates: Path) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Borges default file not found; looked in: "
        + ", ".join(str(c) for c in candidates)
    )


def default_config_path() -> Path:
    """The default ``config.yaml``: paper prompts, blocklists and exclusions."""
    return _first_existing(_PACKAGED / "config.yaml", _REPO_ROOT / "config.yaml")


def negative_samples_dir() -> Path:
    """Reference favicons of web frameworks and hosting defaults."""
    return _first_existing(
        _PACKAGED / "negative_samples",
        _REPO_ROOT / "data" / "reference" / "negative_samples",
    )
