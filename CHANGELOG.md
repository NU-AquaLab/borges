# Changelog

All notable changes to this project are documented in this file.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning: [SemVer](https://semver.org).
Changes that alter the inferred mappings are listed separately under *Changed (results)*.

## [Unreleased]

### Added

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

## [0.2.0] - 2025-08-20

Restructured from research scripts into a Python package with the `borges` CLI. The
GitHub release for this code is tagged `v1.0`.
