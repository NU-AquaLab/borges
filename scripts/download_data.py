#!/usr/bin/env python3
"""Download PeeringDB and AS2Org data from CAIDA.

This script downloads:
- PeeringDB dumps from https://publicdata.caida.org/datasets/peeringdb/
- AS2Org WHOIS data from https://publicdata.caida.org/datasets/as-organizations/
"""

import argparse
import gzip
import json
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Download PeeringDB and AS2Org data from CAIDA"
    )
    parser.add_argument(
        "--peeringdb-date",
        type=str,
        help="PeeringDB date in YYYY-MM-DD format (default: latest)",
    )
    parser.add_argument(
        "--as2org-date",
        type=str,
        help="AS2Org date in YYYY-MM-DD format (default: latest)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./data/input",
        help="Output directory for downloaded files (default: ./data/input)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force download even if files already exist",
    )
    return parser.parse_args()


def ensure_directory(path: str) -> Path:
    """Ensure directory exists."""
    directory = Path(path)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def download_file(url: str, output_path: Path, desc: str) -> None:
    """Download a file with progress bar."""
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    total_size = int(response.headers.get("content-length", 0))
    
    with open(output_path, "wb") as f:
        with tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            desc=desc,
            ncols=80
        ) as pbar:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                pbar.update(len(chunk))


def get_latest_peeringdb_url() -> Tuple[str, str]:
    """Get the latest PeeringDB dump URL."""
    base_url = "https://publicdata.caida.org/datasets/peeringdb/"
    
    # Get the main page
    response = requests.get(base_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    
    # Find year directories
    year_links = []
    for link in soup.find_all("a"):
        href = link.get("href", "")
        if re.match(r"^\d{4}/$", href):
            year_links.append(href.strip("/"))
    
    if not year_links:
        raise ValueError("No year directories found")
    
    # Get the latest year
    latest_year = max(year_links)
    year_url = urljoin(base_url, f"{latest_year}/")
    
    # Get month directories
    response = requests.get(year_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    
    month_links = []
    for link in soup.find_all("a"):
        href = link.get("href", "")
        if re.match(r"^\d{2}/$", href):
            month_links.append(href.strip("/"))
    
    if not month_links:
        raise ValueError("No month directories found")
    
    # Get the latest month
    latest_month = max(month_links)
    month_url = urljoin(year_url, f"{latest_month}/")
    
    # Get JSON files
    response = requests.get(month_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    
    json_files = []
    for link in soup.find_all("a"):
        href = link.get("href", "")
        if href.endswith(".json") and "peeringdb" in href:
            json_files.append(href)
    
    if not json_files:
        raise ValueError("No PeeringDB JSON files found")
    
    # Get the latest file
    latest_file = sorted(json_files)[-1]
    file_url = urljoin(month_url, latest_file)
    
    return file_url, latest_file


def get_peeringdb_url_for_date(date_str: str) -> Tuple[str, str]:
    """Get PeeringDB URL for a specific date."""
    date = datetime.strptime(date_str, "%Y-%m-%d")
    year = date.strftime("%Y")
    month = date.strftime("%m")
    day = date.strftime("%d")
    
    filename = f"peeringdb_2_dump_{year}_{month}_{day}.json"
    url = f"https://publicdata.caida.org/datasets/peeringdb/{year}/{month}/{filename}"
    
    return url, filename


def get_latest_as2org_url() -> Tuple[str, str]:
    """Get the latest AS2Org data URL."""
    base_url = "https://publicdata.caida.org/datasets/as-organizations/"
    
    response = requests.get(base_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    
    # Find all .as-org2info.txt.gz files
    gz_files = []
    for link in soup.find_all("a"):
        href = link.get("href", "")
        if href.endswith(".as-org2info.txt.gz"):
            # Extract date from filename (YYYYMMDD.as-org2info.txt.gz)
            match = re.match(r"^(\d{8})\.as-org2info\.txt\.gz$", href)
            if match:
                gz_files.append((match.group(1), href))
    
    if not gz_files:
        raise ValueError("No AS2Org files found")
    
    # Get the latest file
    latest_date, latest_file = max(gz_files)
    file_url = urljoin(base_url, latest_file)
    
    return file_url, latest_file


def get_as2org_url_for_date(date_str: str) -> Tuple[str, str]:
    """Get AS2Org URL for a specific date (finds closest available)."""
    target_date = datetime.strptime(date_str, "%Y-%m-%d")
    base_url = "https://publicdata.caida.org/datasets/as-organizations/"
    
    response = requests.get(base_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    
    # Find all .as-org2info.txt.gz files
    available_dates = []
    for link in soup.find_all("a"):
        href = link.get("href", "")
        if href.endswith(".as-org2info.txt.gz"):
            match = re.match(r"^(\d{8})\.as-org2info\.txt\.gz$", href)
            if match:
                file_date = datetime.strptime(match.group(1), "%Y%m%d")
                available_dates.append((file_date, href))
    
    if not available_dates:
        raise ValueError("No AS2Org files found")
    
    # Find the closest date that's not after the target date
    valid_dates = [(d, f) for d, f in available_dates if d <= target_date]
    
    if not valid_dates:
        # If no dates before target, use the earliest available
        closest_date, filename = min(available_dates)
        print(f"Warning: No AS2Org file available for {date_str}, using earliest available: {closest_date.strftime('%Y-%m-%d')}")
    else:
        # Use the latest date that's not after the target
        closest_date, filename = max(valid_dates)
        if closest_date.date() != target_date.date():
            print(f"Note: AS2Org file for exact date {date_str} not available, using closest: {closest_date.strftime('%Y-%m-%d')}")
    
    file_url = urljoin(base_url, filename)
    return file_url, filename


def download_peeringdb(date: Optional[str], output_dir: Path, force: bool) -> Path:
    """Download PeeringDB dump."""
    if date:
        url, filename = get_peeringdb_url_for_date(date)
        print(f"Downloading PeeringDB dump for {date}")
    else:
        url, filename = get_latest_peeringdb_url()
        print(f"Downloading latest PeeringDB dump")
    
    output_path = output_dir / filename
    
    if output_path.exists() and not force:
        print(f"File already exists: {output_path}")
        return output_path
    
    print(f"URL: {url}")
    print(f"Downloading to: {output_path}")
    
    try:
        download_file(url, output_path, "PeeringDB")
        print(f"Successfully downloaded: {filename}")
        return output_path
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            print(f"Error: PeeringDB dump not found for the specified date")
            print(f"The file might not be available yet or the date might be incorrect")
            sys.exit(1)
        raise


def download_as2org(date: Optional[str], output_dir: Path, force: bool) -> Path:
    """Download AS2Org data."""
    if date:
        url, filename = get_as2org_url_for_date(date)
        print(f"\nDownloading AS2Org data for {date}")
    else:
        url, filename = get_latest_as2org_url()
        print(f"\nDownloading latest AS2Org data")
    
    gz_path = output_dir / filename
    txt_filename = filename.replace(".gz", "")
    txt_path = output_dir / txt_filename
    
    if txt_path.exists() and not force:
        print(f"File already exists: {txt_path}")
        return txt_path
    
    print(f"URL: {url}")
    print(f"Downloading to: {gz_path}")
    
    try:
        download_file(url, gz_path, "AS2Org")
        print(f"Successfully downloaded: {filename}")
        
        # Decompress the file
        print(f"Decompressing to: {txt_path}")
        with gzip.open(gz_path, "rb") as gz_file:
            with open(txt_path, "wb") as txt_file:
                txt_file.write(gz_file.read())
        
        # Remove the compressed file
        gz_path.unlink()
        print(f"Successfully decompressed: {txt_filename}")
        
        return txt_path
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            print(f"Error: AS2Org data not found")
            sys.exit(1)
        raise


def main():
    """Main function."""
    args = parse_arguments()
    
    # Ensure output directory exists
    output_dir = ensure_directory(args.output_dir)
    print(f"Output directory: {output_dir.absolute()}")
    
    # Download PeeringDB
    peeringdb_path = download_peeringdb(
        args.peeringdb_date,
        output_dir,
        args.force
    )
    
    # Download AS2Org
    as2org_path = download_as2org(
        args.as2org_date,
        output_dir,
        args.force
    )
    
    # Print summary
    print("\n" + "="*60)
    print("Download Summary:")
    print("="*60)
    print(f"PeeringDB: {peeringdb_path.name}")
    print(f"AS2Org: {as2org_path.name}")
    print(f"\nFiles saved to: {output_dir.absolute()}")
    
    # Print config.yaml update suggestion
    print("\nUpdate your config.yaml with:")
    print(f"  peeringdb: ${{paths.input_dir}}/{peeringdb_path.name}")
    print(f"  whois: ${{paths.input_dir}}/{as2org_path.name}")


if __name__ == "__main__":
    main()