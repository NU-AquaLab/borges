# Getting started

## Quick start

```bash
pip install borges                   # or: uv pip install borges
borges init                          # writes config.yaml (prompts, blocklists), .env and data/
borges download                      # latest PeeringDB + AS2Org snapshots into data/input/
borges pipeline run                  # needs an LLM key, or see below to skip it
```

From a clone (for development):

```bash
git clone https://github.com/NU-AquaLab/borges.git
cd borges
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
```

## Usage

### Pipeline stages

| # | Stage | What it does | LLM |
|---|---|---|---|
| 1 | `load_data` | Load PeeringDB and WHOIS (AS2Org) data | |
| 2 | `redirect_scraping` | Follow each PeeringDB website to its final URL (no HTML stored) | |
| 3 | `as_detection` | Extract sibling ASNs from `notes` and `aka` | 🤖 |
| 4 | `redirect_analysis` | Group networks that share a final URL | |
| 5 | `favicon_download` | Download website favicons | |
| 6 | `favicon_analysis` | Decide whether shared favicons mean a shared company | 🤖 |
| 7 | `whois_processing` | Group ASes by AS2Org organization | |
| 8 | `network_consolidation` | Merge groups from all sources | |
| 9 | `export_results` | Write Parquet/JSON/CSV and reports to `data/output/` | |

### Commands

```bash
borges pipeline list                                   # stages and their dependencies
borges pipeline run                                    # full run
borges pipeline run --stage redirect_scraping --stage as_detection   # selected stages
borges pipeline run --skip favicon_analysis            # skip stages
borges pipeline run --resume                           # continue from checkpoints
borges pipeline run --dry-run                          # show what would run
borges --config my-config.yaml pipeline run            # or: export BORGES_CONFIG=...
borges --llm workers-ai pipeline run                   # another LLM backend: see local-llm.md
borges download --peeringdb-date 2025-07-16 --as2org-date 2025-07-01
```

`borges --help` lists the rest (`report generate`, `favicon download`, `config`, `version`).

### LLM backends: where the AI inference runs

Only stages 3 (`as_detection`) and 6 (`favicon_analysis`) call an LLM. Borges talks to
any server that speaks the OpenAI chat completions API, so the same code runs on
OpenAI, on your own machine or GPU, or on a hosted open-model provider. Profiles in
`docs/llm-backends/` hold example settings for each, and `--llm` picks one for a
command without editing `config.yaml`.

| Backend | Where it runs | How | Cost and trade-off |
|---|---|---|---|
| **OpenAI** (default) | OpenAI's API | `OPENAI_API_KEY=...` in `.env` | About US$3 per full run. The only option that reproduces the paper (`gpt-4o-mini`) |
| **Ollama** | Your computer, a 12 GB GPU is enough | `borges --llm ollama pipeline run` | Free. A full run with Qwen3 agrees with `gpt-4o-mini` on 99.4% of sibling extractions ([evaluation](open-weight-evaluation.md)) |
| **vLLM** | Your GPU server | `--llm vllm-qwen3`, `vllm-glm` or `vllm-kimi` | Free, fastest with many requests in parallel. From one 24 GB GPU (Qwen3-8B) to a node of 8 × 80 GB (Kimi K2) |
| **Cloudflare Workers AI** | Cloudflare, no GPU | `borges --llm workers-ai pipeline run`, with `CLOUDFLARE_ACCOUNT_ID` and `CLOUDFLARE_API_TOKEN` set | Free plan of 10,000 neurons a day, so a run may span several days; Borges stops cleanly when the quota runs out and resumes from the cache. Smoke-tested with GLM-4.7-Flash and Llama 4 Scout |
| **Other OpenAI-compatible servers** | Your machine: SGLang, llama.cpp's `llama-server`, LM Studio | Set `base_url` and `model` in a profile or `config.yaml` | Free. No shipped profile; settings like `structured_output` depend on the server |
| **Skip the LLM stages** | Nowhere | `borges pipeline run --skip as_detection --skip favicon_analysis` | Free. No siblings from PeeringDB `notes`/`aka`, no favicon grouping |

Every backend other than OpenAI's `gpt-4o-mini` gives mappings that differ from the
paper's. `favicon_analysis` needs a model that accepts images (`vision_model`); with a
text-only model, skip that stage. Other hosted APIs that follow the OpenAI format
should work the same way (`base_url`, plus `api_key: ${YOUR_KEY_VARIABLE}`), but
they are not tested.

A profile is a YAML file with the keys of `api.llm`:

```bash
borges --llm ollama pipeline run                 # a shipped profile, by name
borges --llm my-server.yaml pipeline run         # your own file
python scripts/check_llm_backend.py --llm ollama  # a few real requests first
```

To keep the settings in `config.yaml` instead, copy the whole profile into `api.llm`,
including `timeout`. [Open-weight models](local-llm.md) explains each
setting, concurrency and the answer cache.

### Programmatic use

```python
from borges.config import load_config, set_config
from borges.pipeline import Pipeline

config = load_config("config.yaml")
set_config(config)
pipeline = Pipeline(config)
results = pipeline.run(skip_stages=["as_detection", "favicon_analysis"])
```

## Output

`export_results` writes timestamped files to `data/output/`:

| File | Contents |
|---|---|
| `autonomous_systems_*.parquet` | One row per AS |
| `organizations_*.parquet` | Organization groupings |
| `relationships_*.parquet` | Detected sibling relationships and their source |
| `network_groups_*.parquet` | Consolidated groups of sibling ASes |
| `redirect_analysis_*.parquet` | Final URL of each network's website |
| `network_report_*.json` | Full analysis report |
| `export_summary_*.json` | Export metadata and statistics |

The main settings in `config.yaml`:

| Key | Default | Purpose |
|---|---|---|
| `api.llm.model` / `vision_model` | `gpt-4o-mini` | Text and vision models |
| `api.llm.base_url` | *(unset: OpenAI)* | Any OpenAI-compatible endpoint |
| `api.llm.request_delay` / `retry_delay` | `0` / `30` s | Rate limiting |
| `scraping.html.max_workers` / `timeout` | `100` / `30` s | Redirect scraping parallelism |
| `scraping.favicon.max_workers` | `50` | Favicon download parallelism |
| `peeringdb_asn_exclusions`, blocklists | see file | Known false organizational bridges |
| `processing.merge_guard.enabled` | `false` | Guard against false merges (brand check, bridge guard); see [evaluation](merge-guard-evaluation.md) |
