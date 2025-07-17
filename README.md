# Borges

Network AS-to-Organization mapping framework - A tool for inferring sibling Autonomous Systems (AS) under the same corporate structure using data from PeeringDB, WHOIS-based AS2Org, and web scraping with AI-powered analysis.

📄 **Research Paper**: [Learning AS-to-Organization Mappings with Borges (IMC 2025)](https://estcarisimo.github.io/assets/pdf/papers/2025-IMC-borges.pdf)

## Overview

Borges is a comprehensive pipeline for discovering and analyzing sibling relationships between Autonomous Systems. It combines traditional data sources (PeeringDB, WHOIS) with modern web scraping and AI analysis to identify organizations that operate multiple ASNs and discover hidden relationships.

### Key Features

- **Multi-source sibling AS detection**
  - PeeringDB notes and AKA field analysis for sibling AS identification
  - WHOIS-based AS2Org organization grouping to find sibling AS
  - Website redirect analysis to discover common ownership
  - Favicon similarity detection for corporate identification
  
- **AI-powered analysis**
  - LLM-based sibling AS relationship extraction from text
  - Computer vision for favicon company identification
  - Intelligent data aggregation to identify AS operated by the same organization
  
- **Robust data pipeline**
  - Parallel web scraping with caching
  - Checkpoint/resume capability
  - Multiple export formats (Parquet, JSON, CSV)
  - Comprehensive error handling

## Installation

### Using uv (recommended)

```bash
# Clone the repository
git clone https://github.com/NU-Aqualab/borges.git
cd borges

# Install with uv
uv pip install -e .

# Or install with development dependencies
uv pip install -e ".[dev]"
```

### Using pip

```bash
# Clone the repository
git clone https://github.com/NU-Aqualab/borges.git
cd borges

# Install with pip
pip install -e .
```

## Quick Start

### 1. Initialize a new project

```bash
borges init
```

This creates:
- `config.yaml` - Main configuration file
- `.env` - Environment variables (add your OpenAI API key here)
- `data/` - Directory structure for input/output files

### 2. Configure API access

Edit `.env` and add your OpenAI API key:

```bash
OPENAI_API_KEY=your-api-key-here
```

### 3. Download input data

Use the provided download script to fetch the latest data:

```bash
# Download latest PeeringDB and AS2Org data
python scripts/download_data.py

# Download specific dates
python scripts/download_data.py --peeringdb-date 2025-07-16 --as2org-date 2025-07-01
```

The script downloads:
- **PeeringDB dump** from [CAIDA PeeringDB datasets](https://publicdata.caida.org/datasets/peeringdb/)
- **AS2Org WHOIS data** from [CAIDA AS Organizations](https://publicdata.caida.org/datasets/as-organizations/)

Data files will be saved to `data/input/` with names like:
- `peeringdb_2_dump_2025_07_16.json`
- `20250701.as-org2info.txt`

### 4. Run the pipeline

```bash
# Run full pipeline
borges pipeline run

# Run specific stages
borges pipeline run --stage html_download --stage as_detection

# Skip stages
borges pipeline run --skip favicon_download --skip favicon_analysis

# Resume from checkpoint
borges pipeline run --resume
```

## Pipeline Stages

The analysis pipeline consists of the following stages:

1. **load_data** - Load PeeringDB and WHOIS data
2. **html_download** - Scrape HTML from AS websites
3. **as_detection** - Detect sibling AS relationships using LLM
4. **redirect_analysis** - Analyze URL redirects
5. **favicon_download** - Download website favicons
6. **favicon_analysis** - Analyze favicons using vision AI
7. **whois_processing** - Process WHOIS organization data to identify sibling AS
8. **export_results** - Export analysis results

View available stages:

```bash
borges pipeline list
```

## Configuration

The `config.yaml` file controls all aspects of the pipeline:

```yaml
# Data paths
paths:
  base_dir: ./data
  input_dir: ${paths.base_dir}/input
  output_dir: ${paths.base_dir}/output

# Input files
input_files:
  peeringdb: ${paths.input_dir}/peeringdb_dump.json
  whois: ${paths.input_dir}/whois.txt

# API configuration
api:
  openai:
    api_key: ${OPENAI_API_KEY}
    model: gpt-4o-mini
    temperature: 0

# Scraping settings
scraping:
  html:
    max_workers: 100
    timeout: 30
  favicon:
    max_workers: 50

# Pipeline settings
pipeline:
  stages:
    html_download: true
    favicon_analysis: false  # Disable specific stages
```

## Output

Results are exported to `data/output/` in multiple formats:

### Data Files

- `autonomous_systems_*.parquet` - AS information
- `organizations_*.parquet` - Organization groupings
- `relationships_*.parquet` - Detected sibling AS relationships
- `network_groups_*.parquet` - Network groupings of sibling AS
- `redirect_analysis_*.parquet` - URL redirect analysis for common ownership

### Reports

- `network_report_*.json` - Complete analysis report
- `export_summary_*.json` - Export metadata and statistics

## Advanced Usage

### Custom Configuration

```bash
# Use custom config file
borges --config my-config.yaml pipeline run

# Override with environment variable
export BORGES_CONFIG=production.yaml
borges pipeline run
```

### Programmatic Usage

```python
from borges import Pipeline, load_config

# Load configuration
config = load_config("config.yaml")

# Create and run pipeline
pipeline = Pipeline(config)
results = pipeline.run()

# Access AS network data
as_network = pipeline.context["as_network"]
print(f"Found {len(as_network.autonomous_systems)} ASNs")
```

### Data Analysis

```python
import pandas as pd

# Load results
as_df = pd.read_parquet("data/output/autonomous_systems_*.parquet")
rel_df = pd.read_parquet("data/output/relationships_*.parquet")

# Analyze sibling AS relationships
multi_as_orgs = rel_df.groupby("source_asn").size().sort_values(ascending=False)
print(f"Organizations with most sibling ASNs:")
print(multi_as_orgs.head(10))
```

## Performance Considerations

- **API Rate Limits**: The pipeline includes rate limiting for OpenAI API calls
- **Parallel Processing**: HTML and favicon scraping use configurable parallelism
- **Caching**: Web scraping results are cached to avoid redundant requests
- **Memory Usage**: Large PeeringDB dumps may require significant memory

### Optimization Tips

1. Start with a smaller dataset for testing
2. Disable expensive stages (favicon_analysis) for initial runs
3. Use checkpoint/resume for long-running pipelines
4. Adjust `max_workers` based on your system and network capacity

## Troubleshooting

### Common Issues

**OpenAI API errors**
- Verify API key in `.env` file
- Check API quota and billing
- Reduce `llm_batch_size` in config

**Memory errors**
- Process data in smaller batches
- Increase system memory
- Use `sample_size` in development config

**Network timeouts**
- Increase `timeout` values in config
- Reduce `max_workers` for scraping
- Check network connectivity

### Debug Mode

Run with debug logging:

```bash
export LOG_LEVEL=DEBUG
borges pipeline run
```

## Development

### Project Structure

```
borges/
├── src/borges/
│   ├── analyzers/      # Analysis modules (LLM, redirects, WHOIS)
│   ├── data/           # Data loading, processing, exporting
│   ├── models/         # Data models and schemas
│   ├── pipeline/       # Pipeline orchestration
│   ├── scrapers/       # Web scraping modules
│   ├── utils/          # Utilities (HTTP, LLM client, logging)
│   ├── cli.py          # CLI interface
│   └── config.py       # Configuration management
├── tests/              # Test suite
├── pyproject.toml      # Package configuration
└── config.yaml         # Default configuration
```

### Running Tests

```bash
# Install dev dependencies
uv pip install -e ".[dev]"

# Run tests
pytest

# Run with coverage
pytest --cov=borges
```

### Code Quality

```bash
# Format code
black src/

# Lint code
ruff check src/

# Type check
mypy src/
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [CAIDA (Center for Applied Internet Data Analysis)](https://www.caida.org/) for maintaining an archive of PeeringDB snapshots and creating WHOIS-based AS2Org mappings
- PeeringDB for providing comprehensive AS data
- OpenAI for LLM and vision capabilities
- The network research community

## Support

For issues and feature requests, please use the GitHub issue tracker.