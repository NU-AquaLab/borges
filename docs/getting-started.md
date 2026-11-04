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
borges download --peeringdb-date 2025-07-16 --as2org-date 2025-07-01
```

`borges --help` lists the rest (`report generate`, `favicon download`, `config`, `version`).

### Running without a paid API key

Only stages 3 and 6 call an LLM. Pick one option:

| Option | How | Trade-off |
|---|---|---|
| **Skip the LLM stages** | `borges pipeline run --skip as_detection --skip favicon_analysis` | No sibling extraction from `notes`/`aka`, no favicon grouping |
| **Free local model** | Any OpenAI-compatible server: [vLLM](https://docs.vllm.ai) on a GPU, [Ollama](https://ollama.com), LM Studio. Set `base_url` (below) | Results differ from the paper, which used `gpt-4o-mini` |
| **OpenAI** | `OPENAI_API_KEY=...` in `.env` | About US$3 per full run |

With Ollama (`ollama pull qwen3:8b` and `ollama pull qwen3-vl:8b-instruct`), set this in
`config.yaml`:

```yaml
api:
  llm:
    base_url: http://localhost:11434/v1
    model: qwen3:8b
    vision_model: qwen3-vl:8b-instruct
    structured_output: true              # force parseable JSON answers
    timeout: 300
    extra_body:
      reasoning_effort: none             # turn off thinking: much faster
```

For open-weight models on a GPU (Qwen3, GLM, Kimi on vLLM), concurrency and the
answer cache, see [docs/local-llm.md](local-llm.md). A full run with Qwen3 agrees with
`gpt-4o-mini` on 99.4% of sibling extractions; the
[evaluation](open-weight-evaluation.md) has the details and limitations.

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
