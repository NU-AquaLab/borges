"""Data processing utilities."""

import hashlib
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import tldextract

from ..config import get_config
from ..models import ASNetwork, NetworkGroup, WebsiteInfo


class URLProcessor:
    """Process URLs and extract domains."""
    
    @staticmethod
    def is_blocked_domain(fqdn: str) -> bool:
        """Check if FQDN is in the domain blocklist.
        
        Args:
            fqdn: Fully qualified domain name to check
            
        Returns:
            True if domain is blocked
        """
        config = get_config()
        blocked_domains = config.processing.domain_blocklist
        
        # Check if FQDN ends with any blocked domain
        for blocked_domain in blocked_domains:
            if fqdn.endswith(blocked_domain):
                return True
        return False
    
    @staticmethod
    def extract_fqdn(url: str) -> str:
        """Extract fully qualified domain name from URL.
        
        Args:
            url: URL to process
            
        Returns:
            Fully qualified domain name (FQDN)
        """
        extracted = tldextract.extract(url)
        return extracted.fqdn
    
    @staticmethod
    def extract_domain(url: str) -> str:
        """Extract domain from URL (deprecated - use extract_fqdn).
        
        Args:
            url: URL to process
            
        Returns:
            Extracted domain
        """
        # Keep for backward compatibility, but delegate to FQDN
        return URLProcessor.extract_fqdn(url)
    
    @staticmethod
    def process_redirects(df: pd.DataFrame) -> pd.DataFrame:
        """Process redirect data to group ASNs by final URL.
        
        Args:
            df: DataFrame with redirect information
            
        Returns:
            DataFrame with grouped ASNs (blocked domains filtered out)
        """
        # Group by final URL and aggregate ASNs
        grouped = df.groupby("final_url")["asn"].apply(
            lambda x: [item for sublist in x for item in (sublist if isinstance(sublist, list) else [sublist])]
        ).reset_index()
        
        # Add FQDN extraction
        grouped["domain"] = grouped["final_url"].apply(URLProcessor.extract_fqdn)
        
        # Filter out blocked domains
        grouped = grouped[~grouped["domain"].apply(URLProcessor.is_blocked_domain)]
        
        # Add count
        grouped["asn_count"] = grouped["asn"].apply(len)
        
        return grouped
    
    @staticmethod
    def group_by_domain(df: pd.DataFrame) -> pd.DataFrame:
        """Group ASNs by domain.
        
        Args:
            df: DataFrame with URL and ASN information
            
        Returns:
            DataFrame grouped by domain (blocked domains filtered out)
        """
        # Extract FQDNs if not already present
        if "domain" not in df.columns and "final_url" in df.columns:
            df["domain"] = df["final_url"].apply(URLProcessor.extract_fqdn)
        
        # Filter out blocked domains
        df = df[~df["domain"].apply(URLProcessor.is_blocked_domain)]
        
        # Group by domain
        grouped = df.groupby("domain")["asn"].apply(
            lambda x: list(set([item for sublist in x for item in (sublist if isinstance(sublist, list) else [sublist])]))
        ).reset_index()
        
        grouped["asn_count"] = grouped["asn"].apply(len)
        
        return grouped


class ASNProcessor:
    """Process ASN-related data."""
    
    @staticmethod
    def detect_related_asns(text: str, source_asn: int) -> List[int]:
        """Detect ASN numbers mentioned in text.
        
        Args:
            text: Text to search for ASNs
            source_asn: Source ASN to exclude
            
        Returns:
            List of detected ASNs
        """
        import re
        from ..analyzers.number_validator import is_bgp_community_pattern
        
        # Pre-filter: Remove PEER-AS patterns to prevent extracting numbers from placeholders
        cleaned_text = re.sub(r'PEER-AS\d+', 'PEER-ASX', text, flags=re.IGNORECASE)
        
        # Pattern to match AS numbers
        pattern = r'\bAS(\d+)\b'
        matches = re.findall(pattern, cleaned_text, re.IGNORECASE)
        
        # Convert to integers and filter
        asns = []
        for match in matches:
            try:
                asn = int(match)
                if asn != source_asn and 1 <= asn <= 4294967295:  # Valid ASN range
                    # Check if this ASN is not part of a BGP community pattern
                    if not is_bgp_community_pattern(text, match):
                        asns.append(asn)
            except ValueError:
                continue
        
        return list(set(asns))
    
    @staticmethod
    def has_asn_reference(text: str) -> bool:
        """Check if text contains ASN references.
        
        Args:
            text: Text to check
            
        Returns:
            True if ASN references found
        """
        if not text:
            return False
        
        import re
        return bool(re.search(r'\d+', text))
    
    @staticmethod
    def merge_asn_lists(lists: List[List[int]]) -> List[int]:
        """Merge multiple ASN lists removing duplicates.
        
        Args:
            lists: List of ASN lists
            
        Returns:
            Merged list of unique ASNs
        """
        all_asns = set()
        for asn_list in lists:
            if isinstance(asn_list, list):
                all_asns.update(asn_list)
        return sorted(all_asns)


class FaviconProcessor:
    """Process favicon data."""
    
    @staticmethod
    def hash_favicon(data: bytes) -> Optional[str]:
        """Generate hash for favicon image content only (ignoring metadata).
        
        Args:
            data: Favicon binary data
            
        Returns:
            SHA256 hash of normalized image content, or None if data is empty/invalid
        """
        # Check for empty or invalid data first
        if not data or len(data) == 0:
            return None
            
        try:
            from PIL import Image
            from io import BytesIO
            
            # Load image and normalize to remove metadata/format differences
            with BytesIO(data) as buffer:
                with Image.open(buffer) as img:
                    # Convert to RGBA to normalize format (handles transparency)
                    if img.mode != 'RGBA':
                        img = img.convert('RGBA')
                    
                    # Normalize size to 64x64 (Google favicon service standard)
                    if img.size != (64, 64):
                        img = img.resize((64, 64), Image.Resampling.LANCZOS)
                    
                    # Get raw pixel data (no metadata)
                    pixel_data = img.tobytes()
                    
                    # Hash the pure pixel content
                    return hashlib.sha256(pixel_data).hexdigest()
                    
        except Exception as e:
            # Fallback to raw bytes if image processing fails
            # But still return None if data is empty/invalid
            if not data or len(data) == 0:
                return None
            return hashlib.sha256(data).hexdigest()
    
    @staticmethod
    def favicons_identical(data1: bytes, data2: bytes) -> bool:
        """Check if two favicons are pixel-perfect identical using PIL comparison.
        
        This method provides a more sensitive comparison than hashing,
        similar to the POC approach using ImageChops.difference.
        
        Args:
            data1: First favicon binary data
            data2: Second favicon binary data
            
        Returns:
            True if favicons are pixel-perfect identical
        """
        try:
            from PIL import Image, ImageChops
            from io import BytesIO
            
            # Load both images
            with BytesIO(data1) as buffer1, BytesIO(data2) as buffer2:
                with Image.open(buffer1) as img1, Image.open(buffer2) as img2:
                    # Convert to same format and size
                    img1 = img1.convert('RGBA')
                    img2 = img2.convert('RGBA')
                    
                    # Normalize size to 64x64
                    if img1.size != (64, 64):
                        img1 = img1.resize((64, 64), Image.Resampling.LANCZOS)
                    if img2.size != (64, 64):
                        img2 = img2.resize((64, 64), Image.Resampling.LANCZOS)
                    
                    # Check size match
                    if img1.size != img2.size:
                        return False
                    
                    # Use ImageChops.difference like the POC
                    diff = ImageChops.difference(img1, img2)
                    return diff.getbbox() is None
                    
        except Exception:
            # Fallback to hash comparison
            return FaviconProcessor.hash_favicon(data1) == FaviconProcessor.hash_favicon(data2)
    
    @staticmethod
    def group_by_favicon(df: pd.DataFrame) -> pd.DataFrame:
        """Group URLs by favicon hash.
        
        Args:
            df: DataFrame with favicon data
            
        Returns:
            DataFrame grouped by favicon
        """
        # Calculate favicon hashes
        df["favicon_hash"] = df["favicon"].apply(
            lambda x: FaviconProcessor.hash_favicon(x) if x else None
        )
        
        # Group by favicon hash
        grouped = df[df["favicon_hash"].notna()].groupby("favicon_hash").agg({
            "final_url": list,
            "favicon": "first"  # Keep one copy of favicon data
        }).reset_index()
        
        grouped["url_count"] = grouped["final_url"].apply(len)
        
        return grouped
    
    @staticmethod
    def filter_common_favicons(df: pd.DataFrame, min_urls: int = 3) -> pd.DataFrame:
        """Filter favicons that appear on multiple URLs.
        
        Args:
            df: DataFrame with grouped favicon data
            min_urls: Minimum number of URLs to be considered common
            
        Returns:
            Filtered DataFrame
        """
        return df[df["url_count"] >= min_urls].copy()


class DataAggregator:
    """Aggregate data from multiple sources."""
    
    def __init__(self, as_network: ASNetwork):
        """Initialize aggregator.
        
        Args:
            as_network: AS network to aggregate data into
        """
        self.as_network = as_network
    
    def aggregate_redirect_data(self, redirect_df: pd.DataFrame) -> None:
        """Aggregate redirect data into AS network.
        
        Args:
            redirect_df: DataFrame with redirect data
        """
        for _, row in redirect_df.iterrows():
            domain = row.get("domain")
            asns = row.get("asn", [])
            
            if domain and asns:
                self.as_network.add_domain_mapping(domain, asns)
    
    def create_summary_report(self) -> Dict[str, pd.DataFrame]:
        """Create summary report of aggregated data.
        
        Returns:
            Dictionary of summary DataFrames
        """
        # Get base dataframes
        dfs = self.as_network.to_dataframes()
        
        # Add summary statistics
        summary_stats = pd.DataFrame([self.as_network.get_statistics()])
        dfs["summary_statistics"] = summary_stats
        
        # Add domain groupings
        domain_groups = []
        for domain, asns in self.as_network.domain_to_as.items():
            if len(asns) > 1:
                domain_groups.append({
                    "domain": domain,
                    "asn_count": len(asns),
                    "asns": sorted(asns)
                })
        
        if domain_groups:
            dfs["domain_groups"] = pd.DataFrame(domain_groups)
        
        return dfs


class DataCleaner:
    """Clean and validate data."""
    
    @staticmethod
    def clean_url(url: str) -> Optional[str]:
        """Clean and validate URL.
        
        Args:
            url: URL to clean
            
        Returns:
            Cleaned URL or None if invalid
        """
        if not url or not isinstance(url, str):
            return None
        
        url = url.strip()
        
        # Add protocol if missing
        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"
        
        # Basic validation
        if len(url) < 10 or " " in url:
            return None
        
        return url
    
    @staticmethod
    def clean_asn(asn: Any) -> Optional[int]:
        """Clean and validate ASN.
        
        Args:
            asn: ASN value to clean
            
        Returns:
            Cleaned ASN or None if invalid
        """
        if isinstance(asn, int):
            if 1 <= asn <= 4294967295:
                return asn
        elif isinstance(asn, str):
            # Remove "AS" prefix if present
            asn_str = asn.upper().replace("AS", "").strip()
            try:
                asn_int = int(asn_str)
                if 1 <= asn_int <= 4294967295:
                    return asn_int
            except ValueError:
                pass
        
        return None
    
    @staticmethod
    def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Clean common issues in DataFrames.
        
        Args:
            df: DataFrame to clean
            
        Returns:
            Cleaned DataFrame
        """
        df = df.copy()
        
        # Clean URLs
        for col in ["url", "website", "final_url", "original_url"]:
            if col in df.columns:
                df[col] = df[col].apply(DataCleaner.clean_url)
        
        # Clean ASNs
        if "asn" in df.columns:
            df["asn"] = df["asn"].apply(DataCleaner.clean_asn)
            df = df[df["asn"].notna()]
        
        # Remove duplicates
        df = df.drop_duplicates()
        
        return df