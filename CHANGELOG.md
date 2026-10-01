# Changelog

All notable changes to this project are documented in this file.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning: [SemVer](https://semver.org).
Changes that alter the inferred mappings are listed separately under *Changed (results)*.

## [Unreleased]

## [1.1.0] - 2026-10-01

First release on PyPI (`pip install borges`).

### Added

- The default configuration (the paper's prompts, blocklists and PeeringDB exclusions)
  and the reference favicons now ship inside the package. `borges init` writes the full
  configuration, so a pip install behaves like a repository checkout.
- `borges download`: fetch PeeringDB and AS2Org snapshots from CAIDA (previously only
  `scripts/download_data.py`, which still works).
- Release workflow: a `vX.Y.Z` tag publishes to PyPI (trusted publishing) and creates the
  GitHub release. CI builds the package and smoke-tests the wheel in a clean environment.

- Merge guard for network consolidation (`processing.merge_guard`, **off by default**):
  favicon groups must share a website brand; websites of lookup services (RIR RDAP,
  bgp.tools, …) are dropped; an unverified weak link cannot join two established
  organizations on its own; one extracted relationship cannot join two large
  organizations; and the PeeringDB pass ignores single-ASN ties where WHOIS disagrees
  (the AS4004 pattern, optional and off by default). WHOIS and PeeringDB organizations
  count as corroborating links. Decisions go to `merge_guard_review_*.json`. On the
  2025-09-29 run, groups combining ≥ 20 WHOIS organizations drop from 6 to 0, and
  wrong-merge pairs in the changed groups drop by 96% while 92% of correct pairs are
  kept. On the 2025-08 replays, Level3 and Orange stay apart without hand blocklists
  (see `docs/merge-guard-evaluation.md`).
- `scripts/evaluate_merge_guard.py`: replay consolidation offline from a finished run
  (no scraping, no LLM calls), compare it with the original output, and sweep guard
  settings with `--set key=value`.

- `api.openai.base_url`: run the LLM stages against any OpenAI-compatible server (e.g.
  a local Ollama), so no paid API key is required.
- `CITATION.cff` (with the paper's DOI), `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`,
  `SECURITY.md`, `AGENTS.md`, issue and pull request templates, `CODEOWNERS` and
  Dependabot.
- `.git-blame-ignore-revs` for the bulk `black` reformat.

### Changed

- Python 3.10+ is required (3.9 is end-of-life). CI tests 3.10–3.13.
- README restructured. It now documents running without a paid key and the stage list
  (including `network_consolidation`).
- Code formatted with `black` 26, pinned to `<27` in the dev extras.

### Fixed

- `borges init` crashed when no `config.yaml` existed yet.
- `--config` / `BORGES_CONFIG` was ignored by the LLM client and analyzers, which
  re-read `./config.yaml`.
- CI had never passed: it called `flake8`, which was not installed; replaced by `ruff`.

### Changed (results)

- `DataCleaner.clean_url` now rejects website URLs whose host has no dot (e.g.
  `https://short`). Such URLs cannot resolve, so they could not produce redirects or
  favicons before either.

## [1.0] - 2025-08-20

Restructured from research scripts into a Python package with the `borges` CLI
(tagged `v1.0` on GitHub; `pyproject.toml` said 0.2.0 at the time).
