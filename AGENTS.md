# AGENTS.md — Borges

Instructions for AI coding agents (Claude Code, Codex, Cursor, ...) working in this
repository. `CLAUDE.md` includes this file. Humans: see `CONTRIBUTING.md`.

## What this project is

Borges maps Autonomous Systems to organizations by finding sibling ASes. It is the code
of Selmo, Carisimo, Bustamante and Alvarez-Hamelin, *Learning AS-to-Organization
Mappings with Borges*, ACM IMC 2025 (doi:10.1145/3730567.3732918). A nine-stage pipeline
combines AS2Org (WHOIS), PeeringDB, LLM extraction from PeeringDB `notes`/`aka`, website
redirects and favicons.

## Layout

```
src/borges/
  cli.py                 Click CLI; the only place that prints for the user
  config.py              Pydantic Config; get_config()/set_config() global
  pipeline/runner.py     Pipeline: DEFAULT_STAGE_ORDER, dependencies, checkpoints
  pipeline/stages.py     the nine stages
  analyzers/             llm_analyzer, redirect, whois, number_validator, network_consolidator
  scrapers/              redirect, html, favicon
  data/                  loaders, processors (DataCleaner), exporters
  utils/llm_client.py    the only place an LLM client is created (OpenAI-compatible)
config.yaml              default config, LLM prompts, blocklists and exclusions
scripts/download_data.py fetch PeeringDB + AS2Org from CAIDA
tests/                   unit tests: no network, no API key
```

## Commands

```bash
uv pip install -e ".[dev]"
ruff check src tests scripts --select E9,F63,F7,F82
black --check src tests                 # black 26, pinned <27
mypy src/borges                         # informational in CI (many existing errors)
pytest
borges pipeline run --dry-run
```

## Conventions

- Python 3.10+; black line length 88.
- Get configuration through `get_config()`. The CLI calls `set_config()` so `--config`
  applies everywhere. Never re-read `config.yaml` in library code.
- Create LLM clients only through `borges.utils.llm_client.create_llm_client()`.
- American English everywhere.
- `CHANGELOG.md`: a bullet under `[Unreleased]` for every user-visible change. Changes
  to inferred mappings go under `Changed (results)`.

## Things that are easy to get wrong

- **No real LLM calls, API keys or network access in tests or CI.** The maintainer does
  not fund API usage for this project. Mock `LLMClient` or use a local fake server.
- **Changing results is a scientific change.** Prompts, blocklists, exclusions,
  URL cleaning and consolidation rules all change which ASes are grouped. Such changes
  need a reason, before/after examples in the PR and a `Changed (results)` entry. Do not
  rerun the full pipeline to validate refactors. Use small fixtures.
- Importing `borges` loads the configuration lazily through the logger. Code that runs
  at import time must tolerate a missing `config.yaml` (see `utils/logging.py`).
- Fix false groupings with the merge guard (`processing.merge_guard`, see
  `docs/merge-guard-evaluation.md`), not by adding favicon hashes to the blocklist:
  hashes change between runs. Evaluate changes with `scripts/evaluate_merge_guard.py`
  against a preserved run.
- AS4004 is excluded from PeeringDB on purpose (`peeringdb_asn_exclusions`): it would
  falsely bridge Sprint and Orange.
- Default favicons (WordPress, Bootstrap, nginx, …) in `data/reference/negative_samples/`
  prevent false groupings. Do not delete them.
- `.git-blame-ignore-revs` lists the bulk reformat commit. Merge PRs with merge
  commits, not squash, when a PR adds such a commit.
