# Contributing

Thanks for your interest in Borges. Contributions are welcome when they:

- fix bugs in the pipeline, the CLI or the documentation;
- add tests, especially small fixtures that exercise a stage without network access;
- improve runtime, caching or the separation between stages;
- propose a new sibling signal, ideally with an evaluation like the ones in the paper.

For anything large, or anything that changes the inferred mappings, please open an issue
first.

## Development setup

```bash
git clone https://github.com/NU-AquaLab/borges.git
cd borges
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"            # or: pip install -e ".[dev]"
git config blame.ignoreRevsFile .git-blame-ignore-revs
```

Run the same checks as CI:

```bash
ruff check src tests scripts --select E9,F63,F7,F82
black --check src tests
mypy src/borges                        # informational until existing errors are fixed
pytest
uvx cffconvert --validate              # if you touched CITATION.cff
```

## No API keys in tests

Tests and CI **must not** call a real LLM, need an API key, or reach the network. Mock
`borges.utils.llm_client.LLMClient` or use a local fake. To try the LLM stages without
paying, point `api.openai.base_url` at a local OpenAI-compatible server (see the
README).

## Changes that affect results

Borges produces research data. A change that alters which ASes are grouped together is
a **scientific change**, not a refactor:

- explain it in the PR, with before/after examples (ASNs and the groups they land in);
- add a bullet under `[Unreleased]` → `Changed (results)` in `CHANGELOG.md`;
- do not rerun the full pipeline only to validate a cleanup. Use small fixtures and
  preserved outputs. Rerun only for a correctness bug, a claim that needs revalidation,
  missing artifacts, or a deliberate new experiment.

Prompt changes in `config.yaml` count as results changes.

## Conventions

- Python 3.10+, `black` formatting (line length 88), `ruff` for lint.
- `pathlib.Path`; logging through `borges.utils.get_logger`; output to the user only in
  `cli.py`.
- Type hints and docstrings on public functions.
- Configuration goes through `get_config()`, and the CLI sets it with `set_config()`.
  Do not read `config.yaml` directly.
- American English in code, docs, the changelog, commit messages and PR text.

## Pull requests

- Branch from `main` (`fix/…`, `feat/…`, `docs/…`, `chore/…`). `main` is protected:
  changes go through a pull request.
- CI must be green: lint, format, and tests on Python 3.10–3.13.
- Add tests for behavior changes, and update the README when user-facing behavior
  changes.
- Add an entry to `CHANGELOG.md` under `[Unreleased]` for any user-visible change.

## Releasing

1. Move the `[Unreleased]` entries under a new version and date in `CHANGELOG.md`.
2. Bump `__version__` in `src/borges/__init__.py` (the package version is read from
   it), and `version` and `date-released` in `CITATION.cff`, in the same pull request.
   Run `uvx cffconvert --validate`.
3. Optional dry run: run the **Release** workflow manually from the Actions tab. It
   publishes to TestPyPI.
4. After merging, tag `main` and push the tag. The **Release** workflow checks that the
   tag matches the version, builds the package, smoke-tests it, publishes to PyPI after
   approval in the `pypi` environment, and creates the GitHub release from the changelog:

   ```bash
   git tag -a vX.Y.Z -m "Borges X.Y.Z"
   git push origin vX.Y.Z
   ```
