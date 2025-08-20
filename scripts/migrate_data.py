#!/usr/bin/env python3
"""Migrate data from old Borges structure to new structure.

This script helps migrate existing data from the old file structure
to the new organized structure.
"""

import argparse
import json
import shutil
from pathlib import Path
from typing import Dict, List

import pandas as pd


class DataMigrator:
    """Migrate data from old to new structure."""

    def __init__(self, old_root: Path, new_root: Path):
        """Initialize migrator.

        Args:
            old_root: Root directory of old project
            new_root: Root directory of new project
        """
        self.old_root = Path(old_root)
        self.new_root = Path(new_root)

        # Create new directory structure
        self.data_dir = self.new_root / "data"
        self.input_dir = self.data_dir / "input"
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        self.output_dir = self.data_dir / "output"

        # Create directories
        for dir_path in [self.input_dir, self.raw_dir, self.processed_dir, self.output_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)

    def migrate_input_files(self) -> Dict[str, str]:
        """Migrate input files."""
        results = {}

        # Migrate PeeringDB file
        old_peeringdb_files = list(self.old_root.glob("input_files/peeringdb*.json"))
        if old_peeringdb_files:
            src = old_peeringdb_files[0]
            dst = self.input_dir / "peeringdb_dump.json"
            shutil.copy2(src, dst)
            results["peeringdb"] = f"Copied {src.name} -> {dst.name}"

        # Migrate WHOIS file
        old_whois_files = list(self.old_root.glob("input_files/*.as-org2info.txt"))
        if old_whois_files:
            src = old_whois_files[0]
            dst = self.input_dir / "whois.txt"
            shutil.copy2(src, dst)
            results["whois"] = f"Copied {src.name} -> {dst.name}"

        return results

    def migrate_raw_data(self) -> Dict[str, str]:
        """Migrate raw scraped data."""
        results = {}

        # Migrate HTML cache
        old_html_dir = self.old_root / "raw_htmls_2024"
        if old_html_dir.exists():
            new_html_dir = self.raw_dir / "html_cache"
            new_html_dir.mkdir(exist_ok=True)

            html_files = list(old_html_dir.glob("http*"))
            if html_files:
                print(f"Migrating {len(html_files)} HTML cache files...")
                for src in html_files:
                    dst = new_html_dir / src.name
                    shutil.copy2(src, dst)
                results["html_cache"] = f"Copied {len(html_files)} files"

        # Migrate favicon cache
        old_favicon_dir = self.old_root / "favicons_2024"
        if old_favicon_dir.exists():
            new_favicon_dir = self.raw_dir / "favicon_cache"
            new_favicon_dir.mkdir(exist_ok=True)

            favicon_files = list(old_favicon_dir.glob("*"))
            if favicon_files:
                print(f"Migrating {len(favicon_files)} favicon cache files...")
                for src in favicon_files:
                    dst = new_favicon_dir / src.name
                    shutil.copy2(src, dst)
                results["favicon_cache"] = f"Copied {len(favicon_files)} files"

        return results

    def migrate_output_files(self) -> Dict[str, str]:
        """Migrate output files and convert formats."""
        results = {}

        old_output_dir = self.old_root / "output_files"
        if not old_output_dir.exists():
            return results

        # Migrate and convert Feather files to Parquet
        feather_files = list(old_output_dir.glob("*.feather"))
        for src in feather_files:
            try:
                df = pd.read_feather(src)
                dst_name = src.stem + ".parquet"
                dst = self.output_dir / dst_name
                df.to_parquet(dst)
                results[src.name] = f"Converted to {dst_name}"
            except Exception as e:
                results[src.name] = f"Failed: {e}"

        # Migrate HDF files and convert to Parquet
        hdf_files = list(old_output_dir.glob("*.hdf"))
        for src in hdf_files:
            try:
                df = pd.read_hdf(src, key="data")
                dst_name = src.stem + ".parquet"
                dst = self.output_dir / dst_name
                df.to_parquet(dst)
                results[src.name] = f"Converted to {dst_name}"
            except Exception as e:
                results[src.name] = f"Failed: {e}"

        return results

    def create_config_file(self) -> None:
        """Create configuration file based on old data."""
        config = f"""# Borges Configuration - Migrated
environment: development

paths:
  base_dir: ./data

input_files:
  peeringdb: ${{paths.input_dir}}/peeringdb_dump.json
  whois: ${{paths.input_dir}}/whois.txt

# Add your OpenAI API key to .env file
api:
  openai:
    api_key: ${{OPENAI_API_KEY}}

# Adjusted for migrated data
scraping:
  html:
    max_workers: 50  # Reduced from original
  favicon:
    max_workers: 25  # Reduced from original
"""

        config_path = self.new_root / "config.yaml"
        if not config_path.exists():
            with open(config_path, "w") as f:
                f.write(config)
            print(f"Created config.yaml")

    def create_env_file(self) -> None:
        """Create .env template."""
        env_content = """# Borges Environment Variables
OPENAI_API_KEY=your-api-key-here
LOG_LEVEL=INFO

# Migrated from old project - update as needed
"""

        env_path = self.new_root / ".env"
        if not env_path.exists():
            with open(env_path, "w") as f:
                f.write(env_content)
            print(f"Created .env template")

    def update_claude_md(self) -> None:
        """Update CLAUDE.md file."""
        claude_md = f"""# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the refactored Borges project - a Python-based data processing pipeline for analyzing network infrastructure relationships.

## Migration Notes

This project was migrated from the old structure on {Path.cwd()}. The following changes were made:

- Restructured code into proper Python package under src/borges/
- Converted data formats from Feather/HDF to Parquet
- Added proper configuration management with YAML
- Implemented CLI interface
- Added comprehensive error handling and logging

## Running the Pipeline

After migration, run the pipeline with:

```bash
# Install dependencies
uv pip install -e .

# Run pipeline
borges pipeline run
```

## Data Locations

- Input files: data/input/
- Raw cache: data/raw/
- Output files: data/output/
"""

        claude_path = self.new_root / "CLAUDE.md"
        with open(claude_path, "w") as f:
            f.write(claude_md)
        print(f"Updated CLAUDE.md")

    def run(self) -> None:
        """Run the complete migration."""
        print(f"Migrating data from {self.old_root} to {self.new_root}")
        print("=" * 60)

        # Migrate input files
        print("\n1. Migrating input files...")
        input_results = self.migrate_input_files()
        for file_type, result in input_results.items():
            print(f"   {file_type}: {result}")

        # Migrate raw data
        print("\n2. Migrating raw data...")
        raw_results = self.migrate_raw_data()
        for data_type, result in raw_results.items():
            print(f"   {data_type}: {result}")

        # Migrate output files
        print("\n3. Migrating and converting output files...")
        output_results = self.migrate_output_files()
        for file_name, result in output_results.items():
            print(f"   {file_name}: {result}")

        # Create config files
        print("\n4. Creating configuration files...")
        self.create_config_file()
        self.create_env_file()
        self.update_claude_md()

        print("\n" + "=" * 60)
        print("Migration completed!")
        print("\nNext steps:")
        print("1. Add your OpenAI API key to .env")
        print("2. Review and adjust config.yaml as needed")
        print("3. Install the package: uv pip install -e .")
        print("4. Run the pipeline: borges pipeline run")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Migrate data from old Borges structure to new structure"
    )
    parser.add_argument(
        "--old-root",
        type=Path,
        default=Path.cwd(),
        help="Root directory of old project (default: current directory)",
    )
    parser.add_argument(
        "--new-root",
        type=Path,
        default=Path.cwd(),
        help="Root directory of new project (default: current directory)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be migrated without copying files",
    )

    args = parser.parse_args()

    if args.dry_run:
        print("DRY RUN MODE - No files will be copied")
        print(f"Would migrate from: {args.old_root}")
        print(f"Would migrate to: {args.new_root}")

        # Check what files exist
        old_root = args.old_root
        print("\nFound in old structure:")

        input_files = list(old_root.glob("input_files/*"))
        if input_files:
            print(f"  - Input files: {len(input_files)} files")
            for f in input_files[:5]:
                print(f"    - {f.name}")

        if (old_root / "raw_htmls_2024").exists():
            html_count = len(list((old_root / "raw_htmls_2024").glob("*")))
            print(f"  - HTML cache: {html_count} files")

        if (old_root / "favicons_2024").exists():
            favicon_count = len(list((old_root / "favicons_2024").glob("*")))
            print(f"  - Favicon cache: {favicon_count} files")

        if (old_root / "output_files").exists():
            output_files = list((old_root / "output_files").glob("*"))
            print(f"  - Output files: {len(output_files)} files")
            for f in output_files:
                print(f"    - {f.name}")

    else:
        # Run migration
        migrator = DataMigrator(args.old_root, args.new_root)
        migrator.run()


if __name__ == "__main__":
    main()