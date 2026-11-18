"""Default files shipped with Borges: configuration and reference favicons.

Wheels carry them under ``borges/defaults/`` (copied at build time from the
repository's ``config.yaml``, ``data/reference/`` and ``docs/llm-backends/``). Editable installs read
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


def llm_profiles_dir() -> Path:
    """Example ``api.llm`` settings for other backends (docs/llm-backends)."""
    return _first_existing(
        _PACKAGED / "llm-backends", _REPO_ROOT / "docs" / "llm-backends"
    )


def llm_profile_path(profile: str | Path) -> Path:
    """A profile file, given its path or the name of a shipped one.

    ``workers-ai`` resolves to ``workers-ai.yaml`` among the shipped profiles.

    Raises:
        FileNotFoundError: If neither a file nor a shipped profile has that name
    """
    path = Path(profile)
    if path.is_file():
        return path
    try:
        shipped = llm_profiles_dir()
    except FileNotFoundError:
        shipped = None
    if shipped is not None:
        candidate = shipped / f"{path.stem}.yaml"
        if candidate.is_file():
            return candidate
        names = ", ".join(sorted(f.stem for f in shipped.glob("*.yaml")))
        raise FileNotFoundError(
            f"LLM profile not found: {profile} (shipped profiles: {names})"
        )
    raise FileNotFoundError(f"LLM profile not found: {profile}")
