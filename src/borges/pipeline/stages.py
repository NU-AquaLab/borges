"""Pipeline stages for AS network analysis."""

import json
import glob
import pickle
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm import tqdm

from ..analyzers import (
    ASRelationshipAnalyzer,
    FaviconAnalyzer,
    NetworkGroupConsolidator,
    RedirectAnalyzer,
    WHOISAnalyzer,
)
from ..data import (
    DataAggregator,
    DataCleaner,
    FaviconProcessor,
    PeeringDBLoader,
    ReportExporter,
    URLProcessor,
    WHOISLoader,
)
from ..models import ASNetwork, ASNetworkReport, PipelineResult, WebsiteInfo
from ..scrapers import FaviconScraper, RedirectScraper
from ..utils import get_logger
from ..config import get_config

logger = get_logger(__name__)


def find_input_file(config_path: str, input_dir: Path) -> Optional[Path]:
    """Find input file with flexible pattern matching.
    
    Args:
        config_path: Configured file path (may contain generic name)
        input_dir: Input directory to search in
        
    Returns:
        Actual file path if found, None otherwise
    """
    config_path = Path(config_path)
    
    # If exact file exists, use it
    if config_path.exists():
        return config_path
    
    # Try pattern matching based on filename
    filename = config_path.name
    
    # PeeringDB patterns
    if "peeringdb" in filename.lower():
        patterns = [
            "peeringdb_*.json",
            "peeringdb_2_dump_*.json", 
            "*peeringdb*.json"
        ]
        for pattern in patterns:
            matches = list(input_dir.glob(pattern))
            if matches:
                # Return the most recent file
                return max(matches, key=lambda p: p.stat().st_mtime)
    
    # WHOIS/AS-org patterns  
    if "whois" in filename.lower() or "as-org" in filename.lower():
        patterns = [
            "*.as-org2info.txt",
            "*as-org*.txt",
            "whois*.txt",
            "*whois*.txt"
        ]
        for pattern in patterns:
            matches = list(input_dir.glob(pattern))
            if matches:
                # Return the most recent file
                return max(matches, key=lambda p: p.stat().st_mtime)
    
    return None


class PipelineStage:
    """Base class for pipeline stages."""

    def __init__(self, name: str, config: Dict[str, Any]):
        """Initialize pipeline stage.

        Args:
            name: Stage name
            config: Stage configuration
        """
        self.name = name
        self.config = config
        self.result = None

    def run(self, context: Dict[str, Any]) -> PipelineResult:
        """Run the pipeline stage.

        Args:
            context: Pipeline context with shared data

        Returns:
            Pipeline result
        """
        raise NotImplementedError

    def _create_result(
        self,
        status: str,
        records_processed: int,
        records_failed: int = 0,
        errors: List[str] = None,
        output_files: List[str] = None,
        start_time: datetime = None,
        end_time: datetime = None
    ) -> PipelineResult:
        """Create pipeline result.

        Args:
            status: Status (success, partial, failed)
            records_processed: Number of records processed
            records_failed: Number of records failed
            errors: List of errors
            output_files: List of output files
            start_time: Start time
            end_time: End time

        Returns:
            Pipeline result
        """
        return PipelineResult(
            stage_name=self.name,
            status=status,
            records_processed=records_processed,
            records_failed=records_failed,
            errors=errors or [],
            output_files=output_files or [],
            start_time=start_time or datetime.utcnow(),
            end_time=end_time or datetime.utcnow()
        )


class LoadDataStage(PipelineStage):
    """Load initial data from PeeringDB and WHOIS."""

    def run(self, context: Dict[str, Any]) -> PipelineResult:
        """Load data into AS network."""
        start_time = datetime.utcnow()
        logger.info(f"Starting {self.name}")

        try:
            # Initialize AS network
            as_network = ASNetwork()

            # Get input directory 
            input_dir = Path(self.config["paths"]["input_dir"])
            
            # Load PeeringDB data with flexible file finding
            peeringdb_config_path = self.config["input_files"]["peeringdb"]
            peeringdb_path = find_input_file(peeringdb_config_path, input_dir)
            
            if not peeringdb_path:
                raise FileNotFoundError(f"PeeringDB file not found. Searched for patterns matching: {peeringdb_config_path}")
            
            logger.info(f"Loading PeeringDB from {peeringdb_path}")
            pdb_loader = PeeringDBLoader(peeringdb_path)
            as_network = pdb_loader.load_to_as_network(as_network)
            
            # Load WHOIS data if available with flexible file finding
            whois_config_path = self.config["input_files"]["whois"]
            whois_path = find_input_file(whois_config_path, input_dir)
            
            if whois_path:
                logger.info(f"Loading WHOIS from {whois_path}")
                whois_loader = WHOISLoader(whois_path)
                as_network = whois_loader.load_to_as_network(as_network)
            else:
                logger.info(f"WHOIS file not found (searched for patterns matching: {whois_config_path}), skipping")

            # Store in context
            context["as_network"] = as_network
            peeringdb_df = pdb_loader.load_networks()
            context["peeringdb_df"] = peeringdb_df

            # Validate loaded data
            stats = as_network.get_statistics()
            logger.info(f"Loaded {stats['total_asns']} ASNs from {stats['total_organizations']} organizations")
            
            # Data validation checks
            if stats['total_asns'] == 0:
                logger.error("No ASNs loaded from PeeringDB - check input file format")
            if len(peeringdb_df) == 0:
                logger.error("PeeringDB DataFrame is empty - check input file")
            if "asn" not in peeringdb_df.columns:
                logger.error(f"PeeringDB DataFrame missing 'asn' column. Available columns: {list(peeringdb_df.columns)}")
            
            # Log data quality metrics
            websites_count = peeringdb_df["website"].notna().sum() if "website" in peeringdb_df.columns else 0
            logger.info(f"Data quality: {websites_count}/{len(peeringdb_df)} ASNs have websites ({websites_count/len(peeringdb_df)*100:.1f}%)")

            return self._create_result(
                status="success",
                records_processed=stats["total_asns"],
                start_time=start_time
            )

        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            return self._create_result(
                status="failed",
                records_processed=0,
                errors=[str(e)],
                start_time=start_time
            )


class HTMLScrapingStage(PipelineStage):
    """Scrape redirect information from websites (no HTML content)."""

    def run(self, context: Dict[str, Any]) -> PipelineResult:
        """Scrape redirect information only."""
        start_time = datetime.utcnow()
        logger.info(f"Starting {self.name}")

        try:
            df = context["peeringdb_df"]
            
            # Get blocklist from config to skip blocked ASN websites
            blocklist = set(self.config.get("processing", {}).get("asn_blocklist", []) if isinstance(self.config.get("processing"), dict) else getattr(self.config.get("processing", {}), "asn_blocklist", []))
            
            # Filter to non-empty websites AND exclude blocked ASNs
            website_filter = (
                df["website"].notna() & 
                (df["website"] != "")
            )
            
            if blocklist:
                # Exclude websites from blocked ASNs to prevent bridge creation
                blocked_filter = ~df["asn"].isin(blocklist)
                website_filter = website_filter & blocked_filter
                blocked_websites = df[df["asn"].isin(blocklist) & df["website"].notna()]
                if not blocked_websites.empty:
                    logger.info(f"Skipping {len(blocked_websites)} websites from {len(blocked_websites['asn'].unique())} blocked ASNs")
            
            df_websites = df[website_filter]
            urls = df_websites["website"].unique().tolist()
            
            logger.info(f"Found {len(df)} total ASNs, {len(df_websites)} with websites")
            logger.info(f"Scraping redirects from {len(urls)} unique websites")

            # Initialize redirect scraper
            scraper = RedirectScraper()
            
            # Scrape with progress bar (only redirects, no HTML content)
            pbar = tqdm(total=len(urls), desc="Scraping redirects")
            results = scraper.scrape_urls(
                urls,
                progress_callback=lambda x: pbar.update(x)
            )
            pbar.close()
            
            # Store results
            context["website_data"] = results
            
            # Count successes and analyze results
            successful = sum(1 for r in results if not r.error)
            failed = len(results) - successful
            with_redirects = sum(1 for r in results if not r.error and r.redirects)
            
            logger.info(f"Scraped redirects from {successful} websites successfully, {failed} failed")
            logger.info(f"Found {with_redirects} websites with redirect data")
            
            # Log some example results for debugging
            if results:
                logger.info(f"Example results: successful={any(not r.error for r in results[:3])}, "
                           f"has_final_url={any(r.final_url for r in results[:3] if not r.error)}")

            return self._create_result(
                status="success" if failed == 0 else "partial",
                records_processed=successful,
                records_failed=failed,
                start_time=start_time
            )

        except Exception as e:
            logger.error(f"Redirect scraping failed: {e}")
            return self._create_result(
                status="failed",
                records_processed=0,
                errors=[str(e)],
                start_time=start_time
            )


class ASDetectionStage(PipelineStage):
    """Detect AS relationships using LLM."""

    def run(self, context: Dict[str, Any]) -> PipelineResult:
        """Detect AS relationships."""
        start_time = datetime.utcnow()
        logger.info(f"Starting {self.name}")

        try:
            df = context["peeringdb_df"]
            as_network = context["as_network"]
            
            # Initialize analyzer
            analyzer = ASRelationshipAnalyzer()
            
            # Analyze relationships
            logger.info("Analyzing AS relationships with LLM")
            relationships = analyzer.analyze_dataframe(df)
            
            # Add to AS network
            for rel in relationships:
                as_network.add_relationship(rel)
            
            # Store results and API usage
            context["as_relationships"] = relationships
            context["api_usage"] = analyzer.get_api_usage()
            
            logger.info(f"Found {len(relationships)} AS relationships")
            logger.info(f"API usage: {analyzer.get_api_usage().total_requests} requests, "
                       f"estimated cost: ${analyzer.get_api_usage().estimated_cost_usd:.4f}")

            return self._create_result(
                status="success",
                records_processed=len(relationships),
                start_time=start_time
            )

        except Exception as e:
            logger.error(f"AS detection failed: {e}")
            return self._create_result(
                status="failed",
                records_processed=0,
                errors=[str(e)],
                start_time=start_time
            )


class RedirectAnalysisStage(PipelineStage):
    """Analyze URL redirects."""

    def run(self, context: Dict[str, Any]) -> PipelineResult:
        """Analyze redirects."""
        start_time = datetime.utcnow()
        logger.info(f"Starting {self.name}")

        try:
            website_data = context.get("website_data", [])
            as_network = context.get("as_network")
            df = context.get("peeringdb_df")
            
            # Handle missing context gracefully
            if as_network is None:
                logger.warning("as_network not found in context, skipping redirect analysis")
                return self._create_result(
                    status="success",
                    records_processed=0,
                    start_time=start_time
                )
            
            if df is None:
                logger.warning("peeringdb_df not found in context, skipping URL-to-ASN mapping")
                url_to_asn = {}
            else:
                # Create URL to ASN mapping
                url_to_asn = {}
                if "asn" in df.columns and "website" in df.columns:
                    for _, row in df.iterrows():
                        if pd.notna(row.get("website")):
                            url = row["website"]
                            asn = row["asn"]
                            if url not in url_to_asn:
                                url_to_asn[url] = []
                            url_to_asn[url].append(asn)
                else:
                    logger.warning(f"Required columns missing from dataframe. Available columns: {list(df.columns) if df is not None else 'None'}")
                    url_to_asn = {}
            
            # Initialize analyzer
            analyzer = RedirectAnalyzer(as_network)
            
            # Log website data stats
            if website_data:
                successful_websites = sum(1 for w in website_data if w.final_url and not w.error)
                failed_websites = len(website_data) - successful_websites
                logger.info(f"Website data: {len(website_data)} total, {successful_websites} successful, {failed_websites} failed")
            else:
                logger.warning("No website data available for redirect analysis")
            
            # Analyze redirects
            redirect_df = analyzer.analyze_redirects(website_data)
            logger.info(f"Redirect analysis produced {len(redirect_df)} redirect records")
            
            # Find domain relationships
            if not redirect_df.empty:
                groups = analyzer.find_domain_relationships(redirect_df, url_to_asn)
                as_network.network_groups.extend(groups)
                
                # Store redirect analysis results (no need for URLProcessor since we don't have ASN data)
                context["redirect_analysis"] = redirect_df
                
                logger.info(f"Found {len(groups)} redirect-based groups")
            
            return self._create_result(
                status="success",
                records_processed=len(redirect_df),
                start_time=start_time
            )

        except Exception as e:
            logger.error(f"Redirect analysis failed: {e}")
            return self._create_result(
                status="failed",
                records_processed=0,
                errors=[str(e)],
                start_time=start_time
            )


class FaviconScrapingStage(PipelineStage):
    """Scrape favicons from websites."""

    def run(self, context: Dict[str, Any]) -> PipelineResult:
        """Scrape favicons."""
        start_time = datetime.utcnow()
        logger.info(f"Starting {self.name}")

        try:
            website_data = context.get("website_data", [])
            
            if not website_data:
                logger.warning("No website data available for favicon scraping")
                return self._create_result(
                    status="success",
                    records_processed=0,
                    start_time=start_time
                )
            
            successful_websites = sum(1 for w in website_data if w.final_url and not w.error)
            logger.info(f"Favicon scraping: {len(website_data)} total websites, {successful_websites} successful")
            
            # Get unique final URLs
            final_urls = []
            for info in website_data:
                if info.final_url and not info.error:
                    final_urls.append(str(info.final_url))
            
            final_urls = list(set(final_urls))
            logger.info(f"Scraping favicons for {len(final_urls)} URLs")
            
            # Initialize scraper
            scraper = FaviconScraper()
            
            # Scrape favicons
            pbar = tqdm(total=len(final_urls), desc="Scraping favicons")
            favicon_data = scraper.scrape_favicons(
                final_urls,
                progress_callback=lambda x: pbar.update(x)
            )
            pbar.close()
            
            # Store results
            context["favicon_data"] = favicon_data
            
            logger.info(f"Scraped {len(favicon_data)} favicons")

            return self._create_result(
                status="success",
                records_processed=len(favicon_data),
                start_time=start_time
            )

        except Exception as e:
            logger.error(f"Favicon scraping failed: {e}")
            return self._create_result(
                status="failed",
                records_processed=0,
                errors=[str(e)],
                start_time=start_time
            )


class FaviconAnalysisStage(PipelineStage):
    """Analyze favicons using LLM."""

    def run(self, context: Dict[str, Any]) -> PipelineResult:
        """Analyze favicons."""
        start_time = datetime.utcnow()
        logger.info(f"Starting {self.name}")

        try:
            favicon_data = context.get("favicon_data", {})
            website_data = context.get("website_data", [])
            
            # If no favicon data in context, try to load from cached favicon files
            if not favicon_data:
                logger.info("No favicon data in context, attempting to load from cached favicon files")
                favicon_cache_dir = Path("data/raw/favicon_cache")
                if favicon_cache_dir.exists():
                    cached_favicons = {}
                    favicon_files = list(favicon_cache_dir.glob("*.favicon"))
                    logger.info(f"Found {len(favicon_files)} cached favicon files")
                    
                    for favicon_file in favicon_files:
                        try:
                            # Load favicon data - it's stored as pickle tuple (url, favicon_bytes)
                            with open(favicon_file, 'rb') as f:
                                data = pickle.load(f)
                                
                            if isinstance(data, tuple) and len(data) == 2:
                                url, favicon_bytes = data
                                if url and favicon_bytes:
                                    cached_favicons[url] = favicon_bytes
                            else:
                                logger.debug(f"Unexpected cached favicon data format in {favicon_file}")
                                
                        except Exception as e:
                            logger.debug(f"Error loading cached favicon {favicon_file}: {e}")
                    
                    if cached_favicons:
                        favicon_data = cached_favicons
                        logger.info(f"Loaded {len(favicon_data)} favicons from cache")
                    else:
                        logger.warning("No valid cached favicon data found")
            
            if not favicon_data:
                logger.warning("No favicon data available for analysis (neither in context nor cached)")
                return self._create_result(
                    status="success",
                    records_processed=0,
                    start_time=start_time
                )
            
            # Group favicons by hash
            favicon_groups = {}
            favicon_bytes = {}
            
            # FORENSIC DEBUGGING: Track favicon grouping for target companies
            # This forensic logging was added during mega-group investigation (Aug 2025)
            # to track how favicon analysis affects major telecoms (Sprint, Orange, Cogent)
            # Keep for future debugging of false groupings
            target_companies = ['claro', 'telmex', 'ams-ix', 'amsterdam', 'techtel']
            forensic_favicon_data = {}
            
            for url, data in favicon_data.items():
                if data:
                    hash_val = FaviconProcessor.hash_favicon(data)
                    if hash_val not in favicon_groups:
                        favicon_groups[hash_val] = []
                        favicon_bytes[hash_val] = data
                    favicon_groups[hash_val].append(url)
                    
                    # FORENSIC: Track target company favicons
                    url_lower = url.lower()
                    for company in target_companies:
                        if company in url_lower:
                            if company not in forensic_favicon_data:
                                forensic_favicon_data[company] = {}
                            forensic_favicon_data[company][url] = {
                                'hash': hash_val,
                                'size': len(data)
                            }
            
            logger.info(f"Found {len(favicon_data)} favicon entries, grouped into {len(favicon_groups)} unique favicons")
            
            # FORENSIC: Log target company favicon analysis
            if forensic_favicon_data:
                logger.warning("FORENSIC: Target company favicon analysis:")
                for company, company_data in forensic_favicon_data.items():
                    logger.warning(f"  {company.upper()}: {len(company_data)} favicons")
                    unique_hashes = set(data['hash'] for data in company_data.values())
                    logger.warning(f"    Unique hashes: {len(unique_hashes)}")
                    if len(unique_hashes) < len(company_data):
                        logger.warning(f"    🔄 POTENTIAL MERGING: {len(company_data) - len(unique_hashes)} duplicate hashes found")
                    else:
                        logger.warning(f"    ❌ NO MERGING: All favicons are unique")
                    for url, data in list(company_data.items())[:3]:  # Show first 3
                        logger.warning(f"      {url[:60]}... → {data['hash']}")
            
            # Filter to common favicons (data-driven: 2+ URLs minimum)
            common_favicons = {k: v for k, v in favicon_groups.items() if len(v) >= 2}
            
            logger.info(f"Analyzing {len(common_favicons)} common favicons (appearing 2+ times)")
            if len(common_favicons) == 0 and len(favicon_groups) > 0:
                logger.info("No common favicons found - all favicons appear only once")
                
            # FORENSIC: Check if target companies have common favicons
            target_common_favicons = 0
            for hash_val, urls in common_favicons.items():
                urls_lower = [u.lower() for u in urls]
                for company in target_companies:
                    company_urls = [u for u in urls_lower if company in u]
                    if len(company_urls) >= 2:
                        target_common_favicons += 1
                        logger.warning(f"FORENSIC: {company.upper()} common favicon {hash_val}: {company_urls}")
            
            if target_common_favicons == 0:
                logger.warning("FORENSIC: No target companies have common favicons (threshold: 2+ occurrences)")
            
            # Initialize analyzer
            analyzer = FaviconAnalyzer()
            
            # Analyze favicons
            analyses = analyzer.analyze_favicons(favicon_bytes, common_favicons)
            
            # Create NetworkGroups from favicon matches using LLM organizational analysis
            favicon_hash_to_asns = {}
            favicon_hash_to_urls = {}
            
            # Get AS network from context
            as_network = context.get("as_network")
            
            # Get domain blocklist from config 
            processing_config = self.config.get("processing", {})
            domain_blocklist = set(processing_config.get("domain_blocklist", []) if isinstance(processing_config, dict) else getattr(processing_config, "domain_blocklist", []))
            asn_blocklist = set(processing_config.get("asn_blocklist", []) if isinstance(processing_config, dict) else getattr(processing_config, "asn_blocklist", []))
            logger.info(f"Applying domain blocklist: {len(domain_blocklist)} blocked domains")
            
            if as_network and common_favicons:
                # Build complete ASN → Final URL → Favicon mapping structure
                # FORENSIC DEBUGGING: Building comprehensive ASN → Domain → Favicon mapping
                # Added during favicon hash investigation to trace how ASNs get grouped
                # Essential for debugging false positives in favicon-based grouping
                logger.info("FORENSIC: Building comprehensive ASN → Domain → Favicon mapping")
                
                # Step 1: Create ASN → Domain → Favicon mapping from website_data and favicon_data
                asn_domain_favicon_map = {}
                mapping_stats = {
                    'total_website_entries': len(website_data),
                    'successful_mappings': 0,
                    'missing_asn_mappings': 0,
                    'missing_favicon_data': 0,
                    'blocked_domains': 0
                }
                
                for website_info in website_data:
                    if not website_info.final_url or website_info.error:
                        continue
                        
                    original_url = str(website_info.original_url)
                    final_url = str(website_info.final_url)
                    final_domain = URLProcessor.extract_fqdn(final_url)
                    
                    # Apply domain blocklist
                    if URLProcessor.is_blocked_domain(final_domain):
                        mapping_stats['blocked_domains'] += 1
                        continue
                    
                    # Find ASNs associated with original URL
                    # First try direct URL match
                    asns = as_network.website_to_as.get(original_url, set())
                    if not asns:
                        # Try normalized domain match (how AS network actually stores URLs)
                        from ..models.as_network import normalize_website
                        normalized_original = normalize_website(original_url)
                        asns = as_network.website_to_as.get(normalized_original, set())
                    if not asns:
                        # Fallback: check domain_to_as mapping (populated by redirect analysis)
                        original_domain = URLProcessor.extract_fqdn(original_url)
                        asns = as_network.domain_to_as.get(original_domain, set())
                    
                    if not asns:
                        mapping_stats['missing_asn_mappings'] += 1
                        logger.debug(f"FORENSIC: No ASN mapping found for {original_url} → {final_url}")
                        continue
                    
                    # Get favicon hash for final URL
                    favicon_hash = None
                    if final_url in favicon_data and favicon_data[final_url]:
                        favicon_hash = FaviconProcessor.hash_favicon(favicon_data[final_url])
                        if not favicon_hash:  # Skip if hash is None (empty/invalid favicon)
                            mapping_stats['empty_favicon_data'] = mapping_stats.get('empty_favicon_data', 0) + 1
                            continue
                    else:
                        mapping_stats['missing_favicon_data'] += 1
                        continue
                    
                    # Store complete mapping for each ASN
                    for asn in asns:
                        if asn not in asn_blocklist:
                            asn_domain_favicon_map[asn] = {
                                'original_url': original_url,
                                'final_url': final_url,
                                'final_domain': final_domain,
                                'favicon_hash': favicon_hash
                            }
                            mapping_stats['successful_mappings'] += 1
                
                logger.warning(f"FORENSIC: ASN mapping stats: {mapping_stats}")
                if mapping_stats.get('empty_favicon_data', 0) > 0:
                    logger.warning(f"FORENSIC: ⚠️ {mapping_stats['empty_favicon_data']} ASNs had empty/invalid favicons (would cause false groupings if not filtered)")
                
                # Step 2: Group ASNs by identical favicons
                favicon_to_asn_mapping = {}
                for asn, data in asn_domain_favicon_map.items():
                    favicon_hash = data['favicon_hash']
                    if favicon_hash not in favicon_to_asn_mapping:
                        favicon_to_asn_mapping[favicon_hash] = []
                    favicon_to_asn_mapping[favicon_hash].append({
                        'asn': asn,
                        'domain': data['final_domain'],
                        'final_url': data['final_url'],
                        'original_url': data['original_url']
                    })
                
                # Step 2.5: Filter out blocked favicon hashes (framework/hosting defaults)
                config = get_config()
                favicon_blocklist = set(getattr(config.processing, 'favicon_blocklist', []))
                original_favicon_count = len(favicon_to_asn_mapping)
                blocked_favicon_count = 0
                blocked_asn_count = 0
                
                for favicon_hash in list(favicon_to_asn_mapping.keys()):
                    if favicon_hash in favicon_blocklist:
                        blocked_asns = len(favicon_to_asn_mapping[favicon_hash])
                        blocked_asn_count += blocked_asns
                        blocked_favicon_count += 1
                        logger.warning(f"FORENSIC: 🚫 Blocked favicon hash {favicon_hash[:16]}... affecting {blocked_asns} ASNs (framework/hosting default)")
                        del favicon_to_asn_mapping[favicon_hash]
                
                if blocked_favicon_count > 0:
                    logger.warning(f"FORENSIC: Filtered out {blocked_favicon_count} blocked favicon hashes affecting {blocked_asn_count} ASNs")
                
                logger.info(f"FORENSIC: Found {len(favicon_to_asn_mapping)} unique favicons across {len(asn_domain_favicon_map)} ASNs (after filtering {blocked_favicon_count} blocked hashes)")
                
                # Step 3: LLM analysis for ASN groups with identical favicons
                total_favicon_groups_processed = 0
                total_domains_analyzed = 0
                llm_organizational_matches = 0
                
                try:
                    for favicon_hash, asn_list in favicon_to_asn_mapping.items():
                        if len(asn_list) >= 2:  # Multiple ASNs with same favicon
                            total_favicon_groups_processed += 1
                            domains = [item['final_url'] for item in asn_list]
                            total_domains_analyzed += len(domains)
                            
                            logger.debug(f"FORENSIC: Analyzing {len(asn_list)} ASNs with identical favicon {favicon_hash[:8]}: domains {domains[:3]}...")
                            
                            # Log suspicious large groups that might indicate problems  
                            if len(asn_list) > 10:
                                asns_sample = [item['asn'] for item in asn_list[:10]]
                                logger.warning(f"SUSPICIOUS LARGE GROUP: {len(asn_list)} ASNs share favicon {favicon_hash[:16]}")
                                logger.warning(f"  → Sample ASNs: {asns_sample}")
                                logger.warning(f"  → Sample domains: {domains[:5]}")
                                
                                # SPRINT-ORANGE DEBUG: Check for specific problem favicon
                                sprint_orange_problem_hash = "abbd7aac078b0f7edf0778002e9d39cc7aa2eb9758b1af9d220779d324efcd03"
                                if favicon_hash.startswith(sprint_orange_problem_hash[:16]):
                                    logger.warning(f"🚨 SPRINT-ORANGE FAVICON: Found problematic favicon hash from forensic analysis!")
                                    logger.warning(f"🚨 Full hash: {favicon_hash}")  
                                    logger.warning(f"🚨 This favicon was identified as causing Sprint-Orange merger")
                                    
                                    # Check if Sprint/Orange ASNs are in this group
                                    sprint_orange_asns = {1239, 5511, 250, 215007, 62269, 211035, 46562, 200508, 41103, 35787, 147079}
                                    group_asns = set(item['asn'] for item in asn_list)
                                    overlap = group_asns & sprint_orange_asns
                                    
                                    if overlap:
                                        logger.warning(f"🚨 Contains Sprint-Orange target ASNs: {overlap}")
                                        logger.warning(f"🚨 This could be the source of the mega group merger!")
                                    else:
                                        logger.warning(f"🚨 No Sprint-Orange ASNs in this group - investigating separate issue")
                            
                            # LLM analysis: Do these domains belong to same organization?
                            # Validate URLs before LLM analysis
                            validated_urls = []
                            for url in domains[:10]:
                                if isinstance(url, str) and url.startswith(('http://', 'https://')):
                                    validated_urls.append(url)
                                elif isinstance(url, str) and not url.startswith(('http://', 'https://')):
                                    # Convert domain to full URL
                                    validated_urls.append(f"https://{url}")
                            
                            if not validated_urls:
                                logger.warning(f"No valid URLs for favicon {favicon_hash[:16]}...")
                                continue
                            
                            favicon_analysis = analyzer.analyze_favicon(
                                favicon_bytes[favicon_hash], 
                                validated_urls  # Limit to avoid token limits
                            )
                            
                            # Parse LLM response
                            if favicon_analysis and favicon_analysis.llm_response:
                                response_lower = favicon_analysis.llm_response.lower()
                                same_org_indicators = [
                                    "same organization", "same company", "same brand", 
                                    "belong to", "claro", "telmex", "related", "subsidiary"
                                ]
                                
                                if any(indicator in response_lower for indicator in same_org_indicators):
                                    llm_organizational_matches += 1
                                    asns = [item['asn'] for item in asn_list]
                                    
                                    # Create NetworkGroup with complete traceability
                                    favicon_hash_to_asns[favicon_hash] = asns
                                    favicon_hash_to_urls[favicon_hash] = domains
                                    
                                    logger.warning(f"FORENSIC: ✅ LLM confirmed organizational match for favicon {favicon_hash[:8]}")
                                    logger.warning(f"FORENSIC: → ASNs: {asns}")
                                    logger.warning(f"FORENSIC: → Domains: {domains}")
                                    logger.warning(f"FORENSIC: → Mappings: {[(item['asn'], item['original_url'], item['final_url']) for item in asn_list]}")
                                    
                                else:
                                    logger.debug(f"FORENSIC: ❌ LLM determined domains are NOT from same organization for favicon {favicon_hash[:8]}")
                            else:
                                logger.warning(f"FORENSIC: No LLM response for favicon analysis of hash {favicon_hash[:8]}")
                
                    # FORENSIC: Log final statistics
                    logger.warning(f"FORENSIC: LLM favicon analysis complete:")
                    logger.warning(f"  → {total_favicon_groups_processed} favicon groups processed")
                    logger.warning(f"  → {total_domains_analyzed} domains analyzed")
                    logger.warning(f"  → {llm_organizational_matches} organizational matches confirmed")
                    logger.warning(f"  → {len(favicon_hash_to_asns)} final ASN groups created")
                    
                except Exception as e:
                    logger.error(f"FORENSIC: Critical error in ASN-favicon mapping: {e}")
                    import traceback
                    logger.error(f"FORENSIC: Traceback: {traceback.format_exc()}")
                
                # Create NetworkGroups from LLM-confirmed organizational matches
                favicon_groups = analyzer.create_favicon_network_groups(
                    favicon_hash_to_asns,
                    favicon_hash_to_urls,
                    min_asns=2
                )
                
                # Add groups to AS network
                for group in favicon_groups:
                    as_network.network_groups.append(group)
                
                # FORENSIC DEBUGGING: Enhanced group creation logging
                # Added during AS147079/AS4004 bridge investigation to monitor group formation
                # This helps identify when legitimate network groups vs. false mega-groups are created
                if len(favicon_groups) > 0:
                    total_asns_in_groups = sum(len(g.asns) for g in favicon_groups)
                    largest_group_size = max(len(g.asns) for g in favicon_groups)
                    logger.warning(f"FORENSIC: Created {len(favicon_groups)} favicon-based network groups covering {total_asns_in_groups} ASNs, largest group: {largest_group_size} ASNs")
                    
                    # Log details of first few groups for debugging
                    for i, group in enumerate(favicon_groups[:3]):
                        sample_urls = group.metadata.get('sample_urls', [])[:2] if hasattr(group, 'metadata') else []
                        logger.warning(f"FORENSIC: Group {i+1}: {len(group.asns)} ASNs, hash: {group.common_attribute[:8]}, sample URLs: {sample_urls}")
                else:
                    logger.warning(f"FORENSIC: NO favicon groups created from {len(favicon_hash_to_asns)} favicon hashes")
                    if len(favicon_hash_to_asns) > 0:
                        # Log why no groups were created
                        for hash_val, asns in list(favicon_hash_to_asns.items())[:3]:
                            logger.warning(f"FORENSIC: Hash {hash_val[:8]} has {len(asns)} ASNs (min required: 2)")
                
                logger.info(f"Created {len(favicon_groups)} favicon-based network groups")
                
                # FORENSIC DEBUGGING: Log which target companies got favicon groups  
                # Critical for monitoring mega-group formation - helps detect when telecoms
                # get incorrectly grouped with small ASNs due to shared favicon hashes
                target_favicon_groups = 0
                for group in favicon_groups:
                    group_asns = group.asns if hasattr(group, 'asns') else []
                    # Check if this group contains ASNs from target companies
                    for asn in group_asns:
                        # Look up ASN in as_network to see if it's a target company
                        asn_info = as_network.autonomous_systems.get(asn)
                        if asn_info and hasattr(asn_info, 'name'):
                            name_lower = asn_info.name.lower()
                            for company in target_companies:
                                if company in name_lower:
                                    target_favicon_groups += 1
                                    logger.warning(f"FORENSIC: Created favicon group for {company.upper()} AS{asn} with {len(group_asns)} ASNs")
                                    break
                
                if target_favicon_groups == 0:
                    logger.warning("FORENSIC: No favicon groups created for target companies")
                context["favicon_network_groups"] = favicon_groups
            else:
                logger.warning("No AS network in context - cannot create favicon network groups")
            
            # Store results
            context["favicon_analyses"] = analyses
            
            logger.info(f"Completed {len(analyses)} favicon analyses")

            return self._create_result(
                status="success",
                records_processed=len(analyses),
                start_time=start_time
            )

        except Exception as e:
            logger.error(f"Favicon analysis failed: {e}")
            return self._create_result(
                status="failed",
                records_processed=0,
                errors=[str(e)],
                start_time=start_time
            )


class WHOISProcessingStage(PipelineStage):
    """Process WHOIS data."""

    def run(self, context: Dict[str, Any]) -> PipelineResult:
        """Process WHOIS data."""
        start_time = datetime.utcnow()
        logger.info(f"Starting {self.name}")

        try:
            # Get input directory and find WHOIS file flexibly
            input_dir = Path(self.config["paths"]["input_dir"])
            whois_config_path = self.config["input_files"]["whois"]
            whois_path = find_input_file(whois_config_path, input_dir)
            
            if not whois_path:
                logger.warning(f"WHOIS file not found (searched for patterns matching: {whois_config_path}), skipping")
                return self._create_result(
                    status="success",
                    records_processed=0,
                    start_time=start_time
                )
            
            as_network = context["as_network"]
            
            # Load WHOIS data
            loader = WHOISLoader(whois_path)
            df = loader.load()
            
            # Initialize analyzer
            analyzer = WHOISAnalyzer(as_network)
            
            # Analyze WHOIS data
            org_groups = analyzer.analyze_whois_data(df)
            
            # Create relationships
            relationships = analyzer.create_whois_relationships(org_groups)
            for rel in relationships:
                as_network.add_relationship(rel)
            
            # Create network groups
            groups = analyzer.find_multi_asn_organizations(org_groups)
            as_network.network_groups.extend(groups)
            
            # Add WHOIS-only ASNs (like AS721) that aren't in PeeringDB
            whois_only_count = loader.add_whois_only_asns(as_network)
            
            logger.info(f"Processed {len(org_groups)} organizations, found {len(groups)} multi-ASN orgs")
            logger.info(f"Added {whois_only_count} WHOIS-only ASNs not found in PeeringDB")

            return self._create_result(
                status="success",
                records_processed=len(org_groups),
                start_time=start_time
            )

        except Exception as e:
            logger.error(f"WHOIS processing failed: {e}")
            return self._create_result(
                status="failed",
                records_processed=0,
                errors=[str(e)],
                start_time=start_time
            )


class NetworkGroupConsolidationStage(PipelineStage):
    """Consolidate network groups from different analysis sources."""

    def _create_website_network_groups(self, as_network) -> None:
        """Create NetworkGroups from website mappings.
        
        Groups Autonomous Systems that share the same website domain, indicating
        potential organizational relationships. Applies both ASN and domain blocklists
        to prevent false groupings from shared hosting or common infrastructure.
        
        Parameters
        ----------
        as_network : ASNetwork
            The AS network containing website-to-ASN mappings to process
            
        Notes
        -----
        This method was enhanced during the mega-group investigation (Aug 2025) to
        properly apply domain blocklists, preventing false groupings from domains
        like peeringdb.com that are shared across unrelated organizations.
        """
        from ..models import NetworkGroup
        from ..data.processors import URLProcessor
        
        logger.info("Creating website-based network groups")
        
        # Get blocklists from config
        processing_config = self.config.get("processing", {})
        asn_blocklist = set(processing_config.get("asn_blocklist", []) if isinstance(processing_config, dict) else getattr(processing_config, "asn_blocklist", []))
        domain_blocklist = set(processing_config.get("domain_blocklist", []) if isinstance(processing_config, dict) else getattr(processing_config, "domain_blocklist", []))
        
        if asn_blocklist:
            logger.info(f"Applying ASN blocklist to website groups ({len(asn_blocklist)} blocked ASNs)")
        if domain_blocklist:
            logger.info(f"Applying domain blocklist to website groups ({len(domain_blocklist)} blocked domains)")
        
        # Create groups from website mappings
        website_groups_created = 0
        blocked_groups_skipped = 0
        domain_blocked_groups_skipped = 0
        
        for website, asns in as_network.website_to_as.items():
            # FORENSIC FIX: Check if the website domain is blocked
            # This fix was added Aug 17, 2025 during Sprint-Orange mega-group investigation
            # to prevent peeringdb.com domains from creating false ASN groupings
            # Critical for breaking bridges between unrelated small ASNs and major telecoms
            if URLProcessor.is_blocked_domain(website):
                domain_blocked_groups_skipped += 1
                logger.debug(f"Skipped website group for {website} - domain is blocked")
                continue
            
            # Filter out blocked ASNs
            filtered_asns = asns - asn_blocklist
            
            # Skip if all ASNs were blocked
            if not filtered_asns and asns:
                blocked_groups_skipped += 1
                logger.debug(f"Skipped website group for {website} - all ASNs blocked")
                continue
            
            if len(filtered_asns) >= 2:  # Only create groups with 2+ ASNs
                asn_list = sorted(list(filtered_asns))
                
                # Create NetworkGroup
                group = NetworkGroup(
                    group_id=f"website_{hash(website)}",
                    group_type="website",
                    asns=asn_list,
                    common_attribute=website
                )
                
                as_network.network_groups.append(group)
                website_groups_created += 1
        
        logger.info(f"Created {website_groups_created} website-based network groups")
        if blocked_groups_skipped > 0:
            logger.info(f"Skipped {blocked_groups_skipped} groups due to ASN blocklist filtering")
        if domain_blocked_groups_skipped > 0:
            logger.info(f"Skipped {domain_blocked_groups_skipped} groups due to domain blocklist filtering")

    def run(self, context: Dict[str, Any]) -> PipelineResult:
        """Consolidate network groups from multiple analysis sources.
        
        This stage combines ASN groupings from different analysis methods
        (PeeringDB organizations, website domains, favicon analysis, WHOIS data)
        into consolidated network groups using transitive closure.
        
        Parameters
        ----------
        context : Dict[str, Any]
            Pipeline context containing 'as_network' with populated groups
            
        Returns
        -------
        PipelineResult
            Result containing consolidated groups summary and detailed DataFrames
            
        Notes
        -----
        This stage applies ASN blocklists to prevent known problematic ASNs
        from creating false bridges between unrelated organizations.
        Critical fixes for mega-group prevention were added Aug 2025.
        """
        start_time = datetime.utcnow()
        logger.info(f"Starting {self.name}")

        try:
            as_network = context["as_network"]
            
            # Create website-based NetworkGroups before consolidation
            self._create_website_network_groups(as_network)
            
            # Get blocklist from config
            processing_config = self.config.get("processing", {})
            blocklist = set(processing_config.get("asn_blocklist", []) if isinstance(processing_config, dict) else getattr(processing_config, "asn_blocklist", []))
            
            # Initialize consolidator with blocklist
            consolidator = NetworkGroupConsolidator(as_network, blocklist=blocklist)
            
            # Create consolidated groups DataFrames
            summary_df = consolidator.create_summary_dataframe()
            detailed_df = consolidator.create_detailed_dataframe()
            
            # Store results in context
            context["consolidated_groups_summary"] = summary_df
            context["consolidated_groups_detailed"] = detailed_df
            
            # Validation and quality checks
            total_asns_in_groups = detailed_df['asn'].nunique() if not detailed_df.empty else 0
            total_asns_available = len(as_network.autonomous_systems)
            coverage_percent = (total_asns_in_groups / total_asns_available * 100) if total_asns_available > 0 else 0
            
            logger.info(f"Consolidated {len(summary_df)} network groups covering {len(detailed_df)} ASNs")
            logger.info(f"Group coverage: {total_asns_in_groups}/{total_asns_available} ASNs ({coverage_percent:.1f}%)")
            
            # Check for potential data quality issues
            if not summary_df.empty:
                large_groups = summary_df[summary_df['asn_count'] > 50]
                if len(large_groups) > 0:
                    logger.warning(f"Found {len(large_groups)} unusually large groups (>50 ASNs) - check for over-consolidation")
                
                single_asn_groups = summary_df[summary_df['asn_count'] == 1]
                single_asn_ratio = len(single_asn_groups) / len(summary_df) * 100
                logger.info(f"Single-ASN groups: {len(single_asn_groups)}/{len(summary_df)} ({single_asn_ratio:.1f}%)")

            return self._create_result(
                status="success",
                records_processed=len(summary_df),
                start_time=start_time
            )

        except Exception as e:
            import traceback
            logger.error(f"Network group consolidation failed: {e}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return self._create_result(
                status="failed",
                records_processed=0,
                errors=[str(e)],
                start_time=start_time
            )


class ExportResultsStage(PipelineStage):
    """Export final results."""

    def run(self, context: Dict[str, Any]) -> PipelineResult:
        """Export results."""
        start_time = datetime.utcnow()
        logger.info(f"Starting {self.name}")

        try:
            as_network = context["as_network"]
            output_dir = Path(self.config["paths"]["output_dir"])
            
            # Create report
            report = ASNetworkReport(
                report_id=f"borges_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                total_asns=len(as_network.autonomous_systems),
                total_organizations=len(as_network.organizations),
                as_relationships=as_network.as_relationships,
                network_groups=as_network.network_groups,
                favicon_analyses=context.get("favicon_analyses", []),
                api_usage=context.get("api_usage"),
                pipeline_results=[]  # Initialize empty for now
            )
            
            # Get dataframes
            dfs = as_network.to_dataframes()
            
            # Add additional analysis results
            if "redirect_analysis" in context:
                dfs["redirect_analysis"] = context["redirect_analysis"]
            
            # Add consolidated network groups
            if "consolidated_groups_summary" in context:
                dfs["consolidated_groups_summary"] = context["consolidated_groups_summary"]
            if "consolidated_groups_detailed" in context:
                dfs["consolidated_groups_detailed"] = context["consolidated_groups_detailed"]
            
            # Initialize exporter
            exporter = ReportExporter(output_dir, default_format="parquet")
            
            # Export everything
            export_result = exporter.export_full_report(
                report,
                dfs,
                formats=["parquet", "json"]
            )
            
            # Store export info
            context["export_result"] = export_result
            
            output_files = []
            for format_exports in export_result["exports"]["dataframes"].values():
                output_files.extend(str(p) for p in format_exports.values())
            
            logger.info(f"Exported {len(output_files)} files")
            
            # Log API usage statistics
            api_usage = context.get("api_usage")
            if api_usage:
                logger.info(f"API Usage Summary:")
                logger.info(f"  Requests: {api_usage.total_requests}")
                logger.info(f"  Input tokens: {api_usage.total_input_tokens:,}")
                logger.info(f"  Output tokens: {api_usage.total_output_tokens:,}")
                logger.info(f"  Estimated cost: ${api_usage.estimated_cost_usd:.4f}")

            return self._create_result(
                status="success",
                records_processed=len(dfs),
                output_files=output_files,
                start_time=start_time
            )

        except Exception as e:
            import traceback
            logger.error(f"Export failed: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return self._create_result(
                status="failed",
                records_processed=0,
                errors=[str(e)],
                start_time=start_time
            )