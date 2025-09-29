"""Command-line interface for Borges."""

import json
import sys
from pathlib import Path
from typing import List, Optional

import click

from .config import load_config
from .pipeline import Pipeline
from .utils import setup_logging


@click.group()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    help="Path to configuration file",
    envvar="BORGES_CONFIG",
)
@click.pass_context
def cli(ctx, config):
    """Borges - AS Sibling Relationship Inference System.
    
    Infer sibling relationships between Autonomous Systems using data from
    PeeringDB, WHOIS, and web scraping with AI-powered analysis.
    """
    # Load configuration
    try:
        ctx.obj = load_config(config)
    except Exception as e:
        click.echo(f"Error loading configuration: {e}", err=True)
        sys.exit(1)


@cli.group()
@click.pass_context
def pipeline(ctx):
    """Pipeline management commands."""
    pass


@pipeline.command("run")
@click.option(
    "--stage",
    "-s",
    multiple=True,
    help="Specific stages to run (can be used multiple times)",
)
@click.option(
    "--skip",
    multiple=True,
    help="Stages to skip (can be used multiple times)",
)
@click.option(
    "--resume",
    is_flag=True,
    help="Resume from checkpoints",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be run without executing",
)
@click.option(
    "--peeringdb-file",
    type=click.Path(exists=True),
    help="Override PeeringDB input file path",
)
@click.option(
    "--whois-file", 
    type=click.Path(exists=True),
    help="Override WHOIS/AS2Org input file path",
)
@click.pass_context
def run_pipeline(ctx, stage, skip, resume, dry_run, peeringdb_file, whois_file):
    """Run the analysis pipeline.
    
    Examples:
        # Run full pipeline
        borges pipeline run
        
        # Run specific stages
        borges pipeline run --stage redirect_scraping --stage as_detection
        
        # Skip stages
        borges pipeline run --skip favicon_download --skip favicon_analysis
        
        # Resume from checkpoint
        borges pipeline run --resume
        
        # Use external input files
        borges pipeline run --peeringdb-file data/peeringdb_2025_08_01.json --whois-file external_whois.json
        
        # Use only WHOIS override (PeeringDB from config)
        borges pipeline run --whois-file /path/to/custom_whois.json
    """
    config = ctx.obj
    
    # Initialize pipeline
    pipeline_runner = Pipeline(config)
    
    # List stages if dry run
    if dry_run:
        stages_to_run = list(stage) if stage else None
        skip_stages = list(skip) if skip else None
        
        click.echo("Pipeline dry run - stages that would be executed:")
        for stage_info in pipeline_runner.list_stages():
            if skip_stages and stage_info["name"] in skip_stages:
                continue
            if stages_to_run and stage_info["name"] not in stages_to_run:
                continue
            if stage_info["enabled"]:
                click.echo(f"  ✓ {stage_info['name']}: {stage_info['description']}")
            else:
                click.echo(f"  ✗ {stage_info['name']}: {stage_info['description']} (disabled)")
        return
    
    # Run pipeline
    click.echo("Starting Borges pipeline...")
    
    # Prepare input overrides
    input_overrides = {}
    if peeringdb_file:
        input_overrides['peeringdb'] = peeringdb_file
        click.echo(f"Using PeeringDB file: {peeringdb_file}")
    if whois_file:
        input_overrides['whois'] = whois_file
        click.echo(f"Using WHOIS/AS2Org file: {whois_file}")
    
    try:
        results = pipeline_runner.run(
            stages=list(stage) if stage else None,
            skip_stages=list(skip) if skip else None,
            resume=resume,
            input_overrides=input_overrides
        )
        
        # Display summary
        click.echo("\nPipeline Summary:")
        click.echo(f"Total stages: {results['total_stages']}")
        click.echo(f"Successful: {results['successful_stages']}")
        click.echo(f"Partial: {results['partial_stages']}")
        click.echo(f"Failed: {results['failed_stages']}")
        click.echo(f"Total duration: {results['total_duration']:.2f} seconds")
        
        # Show stage details
        click.echo("\nStage Details:")
        for stage_result in results["stage_results"]:
            status_symbol = {
                "success": "✓",
                "partial": "⚠",
                "failed": "✗"
            }.get(stage_result["status"], "?")
            
            click.echo(
                f"{status_symbol} {stage_result['stage']}: "
                f"{stage_result['records_processed']} processed, "
                f"{stage_result['records_failed']} failed "
                f"({stage_result['duration']:.2f}s)"
            )
            
            if stage_result["errors"]:
                for error in stage_result["errors"]:
                    click.echo(f"    Error: {error}")
        
        # Show export info
        if "export_info" in results:
            click.echo(f"\nExport location: {config.paths.output_dir}")
            click.echo(f"Report ID: {results['export_info']['report_id']}")
        
    except Exception as e:
        click.echo(f"Pipeline failed: {e}", err=True)
        sys.exit(1)


@pipeline.command("list")
@click.pass_context
def list_stages(ctx):
    """List available pipeline stages."""
    config = ctx.obj
    pipeline_runner = Pipeline(config)
    
    click.echo("Available pipeline stages:")
    click.echo()
    
    stages = pipeline_runner.list_stages()
    dependencies = pipeline_runner.get_stage_dependencies()
    
    for stage_info in stages:
        status = "✓ Enabled" if stage_info["enabled"] else "✗ Disabled"
        click.echo(f"{stage_info['name']} ({status})")
        click.echo(f"  {stage_info['description']}")
        
        deps = dependencies.get(stage_info["name"], [])
        if deps:
            click.echo(f"  Dependencies: {', '.join(deps)}")
        click.echo()


@pipeline.command("resume")
@click.pass_context
def resume_pipeline(ctx):
    """Resume pipeline from last checkpoint."""
    config = ctx.obj
    pipeline_runner = Pipeline(config)
    
    click.echo("Resuming pipeline from checkpoint...")
    
    try:
        results = pipeline_runner.run(resume=True)
        
        # Display summary (same as run command)
        click.echo("\nPipeline Summary:")
        click.echo(f"Total stages: {results['total_stages']}")
        click.echo(f"Successful: {results['successful_stages']}")
        click.echo(f"Failed: {results['failed_stages']}")
        
    except Exception as e:
        click.echo(f"Resume failed: {e}", err=True)
        sys.exit(1)


@cli.group()
def report(ctx):
    """Report generation commands."""
    pass


@report.command("generate")
@click.option(
    "--format",
    "-f",
    type=click.Choice(["json", "parquet", "csv"]),
    multiple=True,
    default=["json"],
    help="Output format(s)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    help="Output directory",
)
@click.pass_context
def generate_report(ctx, format, output):
    """Generate analysis report from pipeline results.
    
    This command expects the pipeline to have been run already.
    """
    config = ctx.obj
    
    # TODO: Implement report generation from existing data
    click.echo("Report generation not yet implemented")
    click.echo("Please run the full pipeline with: borges pipeline run")


@cli.command("config")
@click.option(
    "--show",
    is_flag=True,
    help="Show current configuration",
)
@click.option(
    "--validate",
    is_flag=True,
    help="Validate configuration",
)
@click.pass_context
def config_cmd(ctx, show, validate):
    """Configuration management."""
    config = ctx.obj
    
    if show:
        # Show configuration (hide sensitive values)
        config_dict = config.model_dump()
        
        # Hide API keys
        if "api" in config_dict and "openai" in config_dict["api"]:
            if "api_key" in config_dict["api"]["openai"]:
                config_dict["api"]["openai"]["api_key"] = "***hidden***"
        
        click.echo(json.dumps(config_dict, indent=2, default=str))
    
    elif validate:
        click.echo("✓ Configuration is valid")
        click.echo(f"Environment: {config.environment}")
        click.echo(f"Data directory: {config.paths.base_dir}")
        click.echo(f"Input files:")
        for key, path in config.input_files.items():
            path_obj = Path(path)
            if path_obj.exists():
                click.echo(f"  ✓ {key}: {path}")
            else:
                click.echo(f"  ✗ {key}: {path} (not found)")
    
    else:
        click.echo("Use --show to display configuration or --validate to check it")


@cli.command("version")
def version():
    """Show version information."""
    import importlib.metadata
    
    try:
        version = importlib.metadata.version("borges")
    except:
        version = "0.2.0"
    
    click.echo(f"Borges version {version}")
    click.echo("AS Sibling Relationship Inference System")


@cli.group()
@click.pass_context
def favicon(ctx):
    """Favicon management commands."""
    pass


@favicon.command("download")
@click.option(
    "--fresh",
    is_flag=True,
    help="Force fresh download, bypass cache",
)
@click.option(
    "--companies",
    "-c",
    help="Comma-separated list of companies to target (e.g., claro,telmex,techtel)",
)
@click.option(
    "--urls",
    "-u",
    help="Comma-separated list of specific URLs to download",
)
@click.option(
    "--limit",
    "-l",
    type=int,
    help="Limit number of URLs to download (for testing)",
)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(),
    help="Output directory for downloaded favicons (defaults to data/raw/favicon_cache)",
)
@click.option(
    "--save-images",
    is_flag=True,
    help="Also save PNG images for visual inspection",
)
@click.option(
    "--workers",
    "-w",
    type=int,
    default=50,
    help="Number of parallel workers",
)
@click.pass_context
def download_favicons(ctx, fresh, companies, urls, limit, output_dir, save_images, workers):
    """Download fresh favicons for AS networks.
    
    Examples:
        # Download specific URLs
        borges favicon download --urls "https://www.claro.com.co,https://www.claro.com.do"
        
        # Download with fresh bypass and images
        borges favicon download --fresh --save-images --urls "https://www.claro.com.co,https://www.claro.com.do"
        
        # Use predefined company URLs
        borges favicon download --fresh --companies claro --limit 10
    """
    from .scrapers.favicon_scraper import FaviconScraper
    from PIL import Image
    from io import BytesIO
    
    all_urls = set()
    
    # Handle direct URLs
    if urls:
        url_list = [u.strip() for u in urls.split(',')]
        all_urls.update(url_list)
        click.echo(f"🔄 Using {len(url_list)} provided URLs")
    
    # Handle company-based URLs (predefined)
    elif companies:
        company_list = [c.strip().lower() for c in companies.split(',')]
        predefined_urls = {
            'claro': [
                'https://www.claro.com.co/personas/',
                'https://www.claro.com.do/personas/',
                'https://www.claro.com.ar/personas',
                'https://www.claro.com.pe/personas/',
                'https://www.clarochile.cl/personas/',
                'https://www.claro.com.br/personas/',
            ],
            'telmex': [
                'https://www.telmex.com/',
                'https://www.telmex.com.ar/',
                'https://www.telmex.com.pe/',
            ],
            'techtel': [
                'https://www.techtel.com.ar/',
                'https://techtel.com.ar/',
            ]
        }
        
        for company in company_list:
            if company in predefined_urls:
                all_urls.update(predefined_urls[company])
                click.echo(f"🔄 Added {len(predefined_urls[company])} URLs for {company}")
            else:
                click.echo(f"⚠️  Unknown company: {company}")
    
    else:
        click.echo("❌ Please provide either --urls or --companies")
        return
    
    # Apply limit if specified
    if limit and len(all_urls) > limit:
        all_urls = list(all_urls)[:limit]
        click.echo(f"Limited to first {limit} URLs")
    
    if not all_urls:
        click.echo("❌ No URLs found to download favicons for")
        return
    
    # Setup output directory
    if output_dir:
        output_path = Path(output_dir)
    else:
        output_path = Path("data/raw/favicon_cache")
    
    output_path.mkdir(parents=True, exist_ok=True)
    
    if save_images:
        images_path = output_path / "images"
        images_path.mkdir(exist_ok=True)
    
    # Initialize favicon scraper
    scraper = FaviconScraper(cache_dir=output_path)
    
    try:
        click.echo(f"🚀 Downloading favicons for {len(all_urls)} URLs...")
        if fresh:
            click.echo("   Using fresh download (bypassing cache)")
        click.echo(f"   Workers: {workers}")
        click.echo(f"   Output: {output_path}")
        
        # Progress tracking
        downloaded = 0
        failed = 0
        cached = 0
        
        def progress_callback(count):
            nonlocal downloaded
            downloaded += count
            if downloaded % 100 == 0:
                click.echo(f"   Progress: {downloaded}/{len(all_urls)} downloaded")
        
        # Download favicons
        favicon_data = scraper.scrape_favicons(
            list(all_urls),
            max_workers=workers,
            use_cache=not fresh,
            force_fresh=fresh,
            progress_callback=progress_callback
        )
        
        click.echo(f"\n📊 Download Results:")
        click.echo(f"   Total URLs: {len(all_urls)}")
        click.echo(f"   Successfully downloaded: {len(favicon_data)}")
        click.echo(f"   Failed: {len(all_urls) - len(favicon_data)}")
        
        # Save PNG images if requested
        if save_images and favicon_data:
            click.echo(f"\n🖼️  Saving PNG images...")
            
            for url, favicon_bytes in favicon_data.items():
                try:
                    # Create safe filename from URL
                    from urllib.parse import urlparse
                    domain = urlparse(url).netloc or "unknown"
                    safe_domain = domain.replace(".", "_").replace(":", "_")
                    
                    # Save PNG
                    img = Image.open(BytesIO(favicon_bytes))
                    png_path = images_path / f"{safe_domain}.png"
                    img.save(png_path)
                    
                except Exception as e:
                    click.echo(f"     Error saving PNG for {url}: {e}")
            
            click.echo(f"   PNG images saved to: {images_path}")
        
        # Group by hash to show duplicates
        if favicon_data:
            from .data.processors import FaviconProcessor
            
            click.echo(f"\n🔍 Analyzing favicon uniqueness...")
            
            favicon_groups = {}
            for url, data in favicon_data.items():
                hash_val = FaviconProcessor.hash_favicon(data)
                if hash_val not in favicon_groups:
                    favicon_groups[hash_val] = []
                favicon_groups[hash_val].append(url)
            
            common_favicons = {k: v for k, v in favicon_groups.items() if len(v) >= 2}
            
            click.echo(f"   Unique favicons: {len(favicon_groups)}")
            click.echo(f"   Common favicons (2+ URLs): {len(common_favicons)}")
            
            if common_favicons:
                click.echo(f"\n🔄 Common favicon groups:")
                for i, (hash_val, urls) in enumerate(common_favicons.items()):
                    click.echo(f"   Group {i+1} ({hash_val[:8]}...): {len(urls)} URLs")
                    for url in urls[:3]:  # Show first 3
                        click.echo(f"     - {url}")
                    if len(urls) > 3:
                        click.echo(f"     ... and {len(urls) - 3} more")
        
        click.echo(f"\n✅ Favicon download completed!")
        click.echo(f"   Cache directory: {output_path}")
        
    except Exception as e:
        click.echo(f"❌ Download failed: {e}", err=True)
        sys.exit(1)
    
    finally:
        scraper.close()


@cli.command("init")
@click.option(
    "--data-dir",
    type=click.Path(),
    default="./data",
    help="Data directory path",
)
@click.option(
    "--force",
    is_flag=True,
    help="Overwrite existing configuration",
)
def init(data_dir, force):
    """Initialize a new Borges project.
    
    This command creates:
    - Configuration file (config.yaml)
    - Environment file (.env)
    - Data directory structure
    """
    # Check if already initialized
    if Path("config.yaml").exists() and not force:
        click.echo("Project already initialized. Use --force to overwrite.")
        sys.exit(1)
    
    # Create config.yaml
    config_content = f"""# Borges Configuration
environment: development

paths:
  base_dir: {data_dir}

input_files:
  peeringdb: ${{paths.input_dir}}/peeringdb_dump.json
  whois: ${{paths.input_dir}}/whois.txt

# Add your OpenAI API key to .env file
api:
  openai:
    api_key: ${{OPENAI_API_KEY}}
    model: gpt-4o-mini
    temperature: 0.0
    max_retries: 3
    timeout: 30
    vision_model: gpt-4o-mini

scraping:
  html:
    max_workers: 100
    timeout: 30
    user_agent: "Borges AS Inference 1.0"
    retry_attempts: 3
    delay_between_requests: 0.1
  favicon:
    max_workers: 50
    google_favicon_api: "https://t3.gstatic.com/faviconV2"
    api_params:
      client: SOCIAL
      type: FAVICON
      fallback_opts: TYPE,SIZE,URL
      size: 16
    retry_attempts: 3

processing:
  batch_size: 1000
  llm_batch_size: 10
  prompts: {{}}

output:
  formats:
    default: parquet
    supported: [parquet, json, csv]
  file_patterns: {{}}

logging:
  level: INFO
  format: structured
  log_dir: ./logs
  modules: {{}}

pipeline:
  stages: {{}}
  checkpoint: {{}}
  error_handling: {{}}

performance: {{}}
development: {{}}
"""
    
    with open("config.yaml", "w") as f:
        f.write(config_content)
    
    # Create .env
    env_content = """# Borges Environment Variables
OPENAI_API_KEY=your-api-key-here
LOG_LEVEL=INFO
"""
    
    with open(".env", "w") as f:
        f.write(env_content)
    
    # Create directories
    data_path = Path(data_dir)
    for subdir in ["input", "raw", "processed", "output"]:
        (data_path / subdir).mkdir(parents=True, exist_ok=True)
    
    click.echo("✓ Created config.yaml")
    click.echo("✓ Created .env (add your OpenAI API key)")
    click.echo(f"✓ Created data directory structure at {data_dir}")
    click.echo()
    click.echo("Next steps:")
    click.echo("1. Add your OpenAI API key to .env")
    click.echo("2. Place PeeringDB dump in data/input/peeringdb_dump.json")
    click.echo("3. Place WHOIS data in data/input/whois.txt (optional)")
    click.echo("4. Run: borges pipeline run")


def main():
    """Main entry point."""
    cli(obj={})


if __name__ == "__main__":
    main()