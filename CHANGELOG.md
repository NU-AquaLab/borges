# Changelog

All notable changes to this project are documented in this file.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning: [SemVer](https://semver.org).
Changes that alter the inferred mappings are listed separately under *Changed (results)*.

## [Unreleased]

## [1.3.0] - 2026-11-18

### Added

- Cloudflare Workers AI as an LLM backend, with no GPU: profile
  `docs/llm-backends/workers-ai.yaml` (GLM-4.7-Flash for `as_detection`, Llama 4 Scout
  for `favicon_analysis`) and a section in `docs/local-llm.md`. The account ID and API
  token come from `CLOUDFLARE_ACCOUNT_ID` and `CLOUDFLARE_API_TOKEN`.
- `borges --llm PROFILE` (or `BORGES_LLM`) replaces `api.llm` with a profile for that
  command: a shipped one by name (`workers-ai`, `ollama`, `vllm-qwen3`, ...) or a YAML
  file. No need to copy a profile into `config.yaml` by hand. The shipped profiles are
  now included in the wheel.
- `scripts/check_llm_backend.py` sends a few real requests to the configured backend
  (or a profile given with `--llm`) and shows the answers, before a full run. It fails
  only when the backend does not work; siblings that differ from the expected ones are
  a warning about the model.
- `.env.template` and the `.env` written by `borges init` list the Workers AI
  variables.
- The README and getting-started guide list every LLM backend (OpenAI, Ollama, vLLM,
  Cloudflare Workers AI, other OpenAI-compatible servers, or skipping the LLM stages),
  with where each runs, how to select it and what it costs.

### Changed

- When the provider's daily quota is used up (Workers AI error 3036), the pipeline stops
  at once with a clear message instead of recording empty answers. The stage is not
  checkpointed; a rerun after the reset requests only the answers missing from
  `cache_dir`.
- A setting that uses an unset `${VAR}` in `api.llm.base_url`, or a missing Workers AI
  token, fails early and names the variable.

### Changed (results)

- `as_detection` accepts a bare list of ASNs (`[64497, 64498]`) or `null` as the
  model's answer, besides `{"ASs": [...]}`. The prompt's own examples show bare lists,
  and GLM-4.7-Flash on Workers AI answers that way. A bare list used to fail
  (`'list' object has no attribute 'get'`) and record no LLM siblings for that ASN; it
  now records them, after the usual check that each number appears in the text. A
  `null` answer was already read as no siblings. Answers in the requested format are
  read as before.

## [1.2.0] - 2026-11-03

### Added

- Open-weight models on your own GPU (vLLM, SGLang, llama.cpp, Ollama), documented in
  `docs/local-llm.md` with example profiles for Qwen3, GLM and Kimi in
  `docs/llm-backends/`. New `api.llm` settings, all off by default so the paper's
  `gpt-4o-mini` run is unchanged: `structured_output` (constrain `as_detection` to its
  JSON schema), `max_concurrency` (parallel requests in both LLM stages), `seed`,
  `max_tokens`, `extra_body` (server-specific options such as turning off thinking) and
  `cache_dir` (store every answer on disk; reruns send no requests).
- Documentation site built with MkDocs from `docs/`, at
  <https://nu-aqualab.github.io/borges/>. It replaces the previous website; the old URL
  redirects there and keeps hosting the artifacts and the paper.
- `docs/open-weight-evaluation.md`: a full run with Qwen3 on Ollama compared with the
  published September 2025 artifacts.
- `<think>` blocks of reasoning models are removed before an answer is parsed.
- `as_detection` logs how many LLM answers failed or could not be parsed; the error is
  kept on each `ASRelationship`.

### Changed

- The LLM settings moved from `api.openai` to `api.llm`. `api.openai` is still accepted
  as an alias. `api_key` is optional when `base_url` is set.

### Fixed

- `as_detection` failed with `TypeError` under pandas 3: the cleared `notes`/`aka` of
  blocklisted ASNs are NaN there, not None.
- `api.openai.request_delay` and `retry_delay` were silently ignored (the delay between
  requests was always 0 and the wait after a 429 error 60 s). They are now read; the
  default `config.yaml` sets `request_delay: 0`, so runs keep their speed.

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
