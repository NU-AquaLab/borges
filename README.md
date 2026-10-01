# 🗺️ Borges

**Borges** (*Better ORGanizations Entities mappingS*) maps Autonomous Systems (ASes) to the organizations that operate them. It finds **sibling ASes** run by the same company, even when WHOIS lists them under different organizations. It combines CAIDA's WHOIS-based AS2Org and PeeringDB with two new signals: **LLM extraction** of sibling ASNs from PeeringDB's free-text fields, and **website inference** from redirect chains, domain similarity and favicons. It is the code of the ACM IMC 2025 paper [*Learning AS-to-Organization Mappings with Borges*](https://doi.org/10.1145/3730567.3732918).

[![CI](https://github.com/NU-AquaLab/borges/actions/workflows/ci.yml/badge.svg)](https://github.com/NU-AquaLab/borges/actions/workflows/ci.yml)
[![Website](https://img.shields.io/badge/website-nu--aqualab.github.io%2Fborges--website-blue.svg)](https://nu-aqualab.github.io/borges-website/)
[![Paper](https://img.shields.io/badge/DOI-10.1145%2F3730567.3732918-informational.svg)](https://doi.org/10.1145/3730567.3732918)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

> [!NOTE]
> **Mappings go stale quickly.** Companies merge, rebrand and disappear. Edgio vanished
> between the paper's submission and its presentation. The [published artifacts](https://nu-aqualab.github.io/borges-website/)
> are a September 2025 snapshot. To get current mappings, run Borges on fresh PeeringDB
> and AS2Org data. A full run with `gpt-4o-mini` costs about **US$3**. You can also run it
> with **no paid API key** (see [below](#-running-without-a-paid-api-key)).

## ✨ Features

- 🏢 **Organization keys**: groups ASes by CAIDA AS2Org (WHOIS) organization IDs and PeeringDB organization IDs
- 🤖 **LLM sibling extraction**: few-shot prompting reads PeeringDB `notes` and `aka` fields and keeps only real sibling ASNs, not phone numbers, years or prefix limits
- 🔀 **Redirect analysis**: networks whose websites resolve to the same final URL are grouped together
- 🖼️ **Favicon analysis**: shared favicons plus similar domains reveal common branding; a vision LLM rejects framework defaults (WordPress, Bootstrap, …)
- 🧩 **Consolidation**: merges every signal into network groups, with blocklists for known false bridges
- 💾 **Robust pipeline**: nine stages with checkpoint/resume, parallel scraping with caching, and Parquet/JSON/CSV exports
- 💸 **Free to run**: tests and CI need no key. The LLM stages can be skipped or pointed at a local model

## 🚀 Quick Start

Install with [uv](https://docs.astral.sh/uv/) (or plain `pip`):

```bash
git clone https://github.com/NU-AquaLab/borges.git
cd borges
uv venv && source .venv/bin/activate
uv pip install -e .            # or: pip install -e .
```

Set up a project, fetch the inputs from CAIDA and run the pipeline:

```bash
borges init                          # writes config.yaml, .env and data/
python scripts/download_data.py      # latest PeeringDB + AS2Org snapshots into data/input/
borges pipeline run                  # needs an LLM key, or see below to skip it
```

The package is not on PyPI.

## 📖 Usage

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
python scripts/download_data.py --peeringdb-date 2025-07-16 --as2org-date 2025-07-01
```

`borges --help` lists the rest (`report generate`, `favicon download`, `config`, `version`).

### 💸 Running without a paid API key

Only stages 3 and 6 call an LLM. Pick one option:

| Option | How | Trade-off |
|---|---|---|
| **Skip the LLM stages** | `borges pipeline run --skip as_detection --skip favicon_analysis` | No sibling extraction from `notes`/`aka`, no favicon grouping |
| **Free local model** | Any OpenAI-compatible server: [Ollama](https://ollama.com), LM Studio, vLLM. Set `base_url` (below) | Results differ from the paper, which used `gpt-4o-mini` |
| **OpenAI** | `OPENAI_API_KEY=...` in `.env` | About US$3 per full run |

With Ollama (`ollama pull llama3.2` and `ollama pull llama3.2-vision`), set this in `config.yaml`:

```yaml
api:
  openai:
    api_key: unused                       # local servers ignore it, but it must be set
    base_url: http://localhost:11434/v1
    model: llama3.2
    vision_model: llama3.2-vision
```

### Programmatic use

```python
from borges.config import load_config, set_config
from borges.pipeline import Pipeline

config = load_config("config.yaml")
set_config(config)
pipeline = Pipeline(config)
results = pipeline.run(skip_stages=["as_detection", "favicon_analysis"])
```

## 🗃️ Output

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
| `api.openai.model` / `vision_model` | `gpt-4o-mini` | Text and vision models |
| `api.openai.base_url` | *(unset: OpenAI)* | Any OpenAI-compatible endpoint |
| `api.openai.request_delay` / `retry_delay` | `0.5` / `30` s | Rate limiting |
| `scraping.html.max_workers` / `timeout` | `100` / `30` s | Redirect scraping parallelism |
| `scraping.favicon.max_workers` | `50` | Favicon download parallelism |
| `peeringdb_asn_exclusions`, blocklists | see file | Known false organizational bridges |
| `processing.merge_guard.enabled` | `false` | Guard against false merges (brand check, bridge guard); see [evaluation](docs/merge-guard-evaluation.md) |

## 🔬 Methodology (brief)

The full method is in the [paper](https://doi.org/10.1145/3730567.3732918) ([PDF](https://estcarisimo.github.io/assets/pdf/papers/2025-IMC-borges.pdf)). In short:

- **Organization keys.** Start from organization IDs in CAIDA's AS2Org (WHOIS) and in PeeringDB.
- **Information extraction.** Few-shot prompting of `gpt-4o-mini` (temperature 0) recovers sibling ASNs written in PeeringDB's `notes` and `aka` fields. Earlier work used regular expressions, which confuse phone numbers, years and prefix limits with ASNs.
- **Website inference.**
  - *Redirects*: networks whose PeeringDB websites end at the same final URL are siblings. For example, Limelight (AS22822) and Edgecast (AS15133) both redirect to `www.edg.io`.
  - *Favicons*: websites with the same favicon and the same subdomain are grouped. Remaining shared favicons go to a vision-LLM classifier that separates company logos (Claro Chile and Claro Puerto Rico) from framework defaults (Bootstrap, WordPress).
- **Consolidation.** All signals are merged into organization groups.

Results on the July 2024 PeeringDB and AS2Org snapshots:

| Measure | Result |
|---|---|
| Sibling extraction from `notes`/`aka` (320 records checked by hand) | accuracy 0.947 · precision 0.974 · recall 0.94 |
| Favicon company classifier (449 favicons checked by hand) | accuracy 0.986 · precision 0.997 · recall 0.984 |
| Organization Factor (new metric, 0 to 1) | 0.3576: +7% over AS2Org, +3.3% over as2org+ |
| Users newly attributed to large conglomerates | ≈192 million (≈5% of the Internet population) |

### ⚠️ Known limitations

- **No website history.** There is no longitudinal archive of the websites listed in PeeringDB, so past runs cannot be reproduced exactly from the code alone. Keep your inputs and outputs.
- **PeeringDB coverage is partial.** Registration is voluntary, and entries can be incomplete or outdated. For example, some Microsoft ASNs are missing.
- **Errors in the input propagate.** If a PeeringDB record lists the wrong sibling, Borges faithfully extracts the wrong sibling (e.g. AS10026 listing AS2706).
- **Layered ownership is out of scope.** Groups spanning separate brands and regions are not linked, such as América Móvil's Claro and A1.
- **False merges through weak signals.** One favicon or website shared by unrelated networks can chain them into one group. The optional [merge guard](docs/merge-guard-evaluation.md) prevents most of these. It is off by default until its results are reviewed.
- **Known false bridges** are handled by configuration:
  - AS4004 appears under Orange in PeeringDB but under Sprint in WHOIS. It is listed in `peeringdb_asn_exclusions`.
  - Shared PeeringDB listings and default favicons can wrongly join small networks to large operators. They are covered by blocklists in `config.yaml`.
- **LLM output varies by model.** The published numbers are for `gpt-4o-mini`. Local or newer models need their own validation.

## 🏗️ Architecture

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
scripts/download_data.py    # fetch PeeringDB and AS2Org snapshots from CAIDA
config.yaml                 # default configuration, prompts and blocklists
data/reference/             # negative favicon samples (framework defaults)
tests/                      # unit tests; no network, no API key
```

## 🧪 Development

```bash
uv pip install -e ".[dev]"
ruff check src tests scripts --select E9,F63,F7,F82   # syntax errors and undefined names
black --check src tests                               # formatting (black 26)
mypy src/borges                                       # informational for now
pytest                                                # no network, no API key
git config blame.ignoreRevsFile .git-blame-ignore-revs   # hide the bulk reformat from blame
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full workflow.

## 📊 Example Output

```
$ borges pipeline run --skip as_detection --skip favicon_analysis --dry-run
Pipeline dry run - stages that would be executed:
  ✓ load_data: Load initial data from PeeringDB and WHOIS.
  ✓ redirect_scraping: Scrape redirect information from websites (no HTML content).
  ✓ redirect_analysis: Analyze URL redirects.
  ✓ favicon_download: Scrape favicons from websites.
  ✓ whois_processing: Process WHOIS data.
  ✓ network_consolidation: Consolidate network groups from different analysis sources.
  ✓ export_results: Export final results.
```

## 🤝 Contributing

Bug fixes, documentation, tests and new signals are welcome. Please open an issue before large changes. See [CONTRIBUTING.md](CONTRIBUTING.md), the [Code of Conduct](CODE_OF_CONDUCT.md), and [SECURITY.md](SECURITY.md) to report a vulnerability privately.

1. Fork the repository
2. Create a feature branch (`git checkout -b fix/my-fix`)
3. Commit your changes (`git commit -m 'Fix …'`)
4. Push to the branch (`git push origin fix/my-fix`)
5. Open a Pull Request

## 📄 License

[MIT](LICENSE).

## 📝 Citation

If you use Borges, please cite the paper. It is also in [CITATION.cff](CITATION.cff), which feeds GitHub's **Cite this repository** button:

```bibtex
@inproceedings{borges:imc,
  author    = {Selmo, Carlos and Carisimo, Esteban and Bustamante, Fabi{\'a}n E. and Alvarez-Hamelin, J. Ignacio},
  title     = {Learning AS-to-Organization Mappings with Borges},
  booktitle = {Proceedings of the 2025 ACM Internet Measurement Conference},
  series    = {IMC '25},
  year      = {2025},
  month     = {10},
  location  = {Madison, WI, USA},
  publisher = {Association for Computing Machinery},
  doi       = {10.1145/3730567.3732918},
  url       = {https://doi.org/10.1145/3730567.3732918}
}
```

## 🔗 Related Resources

- **Paper**: [ACM Digital Library](https://doi.org/10.1145/3730567.3732918) · [PDF](https://estcarisimo.github.io/assets/pdf/papers/2025-IMC-borges.pdf)
- **Website and artifacts**: [nu-aqualab.github.io/borges-website](https://nu-aqualab.github.io/borges-website/) (September 2025 mappings)
- **Press**: [Mapping Who Really Runs the Internet: Introducing Borges](https://pulse.internetsociety.org/blog/mapping-who-really-runs-the-internet-introducing-borges) (Internet Society Pulse)
- **CAIDA AS2Org**: [AS Organizations dataset](https://www.caida.org/catalog/datasets/as-organizations/) and the [PeeringDB archive](https://publicdata.caida.org/datasets/peeringdb/)
- **as2org+**: Arturi, Carisimo and Bustamante, *as2org+: Enriching AS-to-Organization Mappings with PeeringDB*, PAM 2023
- **State-Owned ASes**: [estcarisimo/state-owned-ases](https://github.com/estcarisimo/state-owned-ases), a hand-built dataset where Borges-style automation could help

## 🙏 Acknowledgements

- **Paper**: *Learning AS-to-Organization Mappings with Borges*
- **Authors**: Carlos Selmo, Esteban Carisimo, Fabián E. Bustamante, J. Ignacio Alvarez-Hamelin
- **Conference**: ACM Internet Measurement Conference (IMC) 2025, Madison, WI, USA
- **Funding**: NSF grant CNS-2107392 and UBACyT 20020220100053BA

Thanks to [CAIDA](https://www.caida.org/) for archiving PeeringDB snapshots and publishing AS2Org, to [PeeringDB](https://www.peeringdb.com), and to [@zhiyichenGT](https://github.com/zhiyichenGT) for the consolidator and WHOIS fixes.
