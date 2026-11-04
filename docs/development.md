# Development

## Architecture

```
src/borges/
├── cli.py                  # Click CLI: init | pipeline | report | favicon | config | version
├── config.py               # Pydantic config, ${ENV} interpolation, global get_config()/set_config()
├── pipeline/
│   ├── runner.py           # Pipeline: stage ordering, dependencies, checkpoints
│   └── stages.py           # the nine stages
├── analyzers/              # LLM sibling extraction, redirects, WHOIS, number validation, consolidation
├── scrapers/               # redirect, HTML and favicon scrapers
├── data/                   # loaders, processors, exporters
├── models/                 # AS network model and Pydantic schemas
└── utils/                  # LLM client (OpenAI-compatible), HTTP client, logging
scripts/                    # download_data.py (= borges download), evaluate_merge_guard.py, check_wheel.py
config.yaml                 # default configuration, prompts and blocklists (shipped in the wheel)
data/reference/             # negative favicon samples (framework defaults)
tests/                      # unit tests; no network, no API key
```

## Development

```bash
uv pip install -e ".[dev]"
ruff check src tests scripts --select E9,F63,F7,F82   # syntax errors and undefined names
black --check src tests                               # formatting (black 26)
mypy src/borges                                       # informational for now
pytest                                                # no network, no API key
git config blame.ignoreRevsFile .git-blame-ignore-revs   # hide the bulk reformat from blame
```

See [CONTRIBUTING.md](https://github.com/NU-AquaLab/borges/blob/main/CONTRIBUTING.md) for the full workflow.

## Documentation

This site is built with [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/) from `docs/` and deployed from `main`. To preview it:

```bash
uv pip install -e ".[docs]"
mkdocs serve
```

See [CONTRIBUTING.md](https://github.com/NU-AquaLab/borges/blob/main/CONTRIBUTING.md) for the contribution process and the [changelog](https://github.com/NU-AquaLab/borges/blob/main/CHANGELOG.md) for releases.
