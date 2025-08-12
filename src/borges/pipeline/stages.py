"""Pipeline stages for AS network analysis."""

import json
import glob
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
            blocklist = set(self.config.get("processing", {}).get("asn_blocklist", []))
            
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
            
            if not favicon_data:
                logger.warning("No favicon data available for analysis")
                return self._create_result(
                    status="success",
                    records_processed=0,
                    start_time=start_time
                )
            
            # Group favicons by hash
            favicon_groups = {}
            favicon_bytes = {}
            
            # FORENSIC: Track favicon grouping for target companies
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
            
            # Filter to common favicons
            common_favicons = {k: v for k, v in favicon_groups.items() if len(v) >= 3}
            
            logger.info(f"Analyzing {len(common_favicons)} common favicons (appearing 3+ times)")
            if len(common_favicons) == 0 and len(favicon_groups) > 0:
                logger.info("No common favicons found - all favicons appear less than 3 times")
                
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
                logger.warning("FORENSIC: No target companies have common favicons (threshold: 3+ occurrences)")
            
            # Initialize analyzer
            analyzer = FaviconAnalyzer()
            
            # Analyze favicons
            analyses = analyzer.analyze_favicons(favicon_bytes, common_favicons)
            
            # Create NetworkGroups from favicon matches
            # First, map favicon hashes to ASNs
            favicon_hash_to_asns = {}
            favicon_hash_to_urls = {}
            
            # Get AS network from context
            as_network = context.get("as_network")
            
            # Get blocklist from config
            blocklist = set(self.config.get("processing", {}).get("asn_blocklist", []))
            if blocklist:
                logger.info(f"Applying blocklist filter to favicon groups ({len(blocklist)} blocked ASNs)")
            
            if as_network:
                for hash_val, urls in common_favicons.items():
                    asns = []
                    for url in urls:
                        # Find ASNs associated with this URL
                        # URLs in favicon data come from website scraping
                        # We need to find which ASN each URL belongs to
                        for asn, info in as_network.as_info.items():
                            if hasattr(info, 'website') and info.website == url:
                                # Skip blocked ASNs
                                if asn not in blocklist:
                                    asns.append(asn)
                    
                    if asns:
                        favicon_hash_to_asns[hash_val] = asns
                        favicon_hash_to_urls[hash_val] = urls
                
                # Create NetworkGroups
                favicon_groups = analyzer.create_favicon_network_groups(
                    favicon_hash_to_asns,
                    favicon_hash_to_urls,
                    min_asns=2
                )
                
                # Add groups to AS network
                for group in favicon_groups:
                    as_network.network_groups.append(group)
                
                logger.info(f"Created {len(favicon_groups)} favicon-based network groups")
                
                # FORENSIC: Log which target companies got favicon groups
                target_favicon_groups = 0
                for group in favicon_groups:
                    group_asns = group.asns if hasattr(group, 'asns') else []
                    # Check if this group contains ASNs from target companies
                    for asn in group_asns:
                        # Look up ASN in as_network to see if it's a target company
                        asn_info = as_network.as_info.get(asn)
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
        """Create NetworkGroups from website mappings."""
        from ..models import NetworkGroup
        
        logger.info("Creating website-based network groups")
        
        # Get blocklist from config
        blocklist = set(self.config.get("processing", {}).get("asn_blocklist", []))
        if blocklist:
            logger.info(f"Applying blocklist filter to website groups ({len(blocklist)} blocked ASNs)")
        
        # Create groups from website mappings
        website_groups_created = 0
        blocked_groups_skipped = 0
        
        for website, asns in as_network.website_to_as.items():
            # Filter out blocked ASNs
            filtered_asns = asns - blocklist
            
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
            logger.info(f"Skipped {blocked_groups_skipped} groups due to blocklist filtering")

    def run(self, context: Dict[str, Any]) -> PipelineResult:
        """Consolidate network groups."""
        start_time = datetime.utcnow()
        logger.info(f"Starting {self.name}")

        try:
            as_network = context["as_network"]
            
            # Create website-based NetworkGroups before consolidation
            self._create_website_network_groups(as_network)
            
            # Get blocklist from config
            blocklist = set(self.config.get("processing", {}).get("asn_blocklist", []))
            
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