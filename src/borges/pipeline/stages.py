"""Pipeline stages for AS network analysis."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm import tqdm

from ..analyzers import (
    ASRelationshipAnalyzer,
    FaviconAnalyzer,
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
from ..scrapers import FaviconScraper, HTMLScraper
from ..utils import get_logger

logger = get_logger(__name__)


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

            # Load PeeringDB data
            peeringdb_path = Path(self.config["input_files"]["peeringdb"])
            logger.info(f"Loading PeeringDB from {peeringdb_path}")
            
            pdb_loader = PeeringDBLoader(peeringdb_path)
            as_network = pdb_loader.load_to_as_network(as_network)
            
            # Load WHOIS data if available
            whois_path = Path(self.config["input_files"]["whois"])
            if whois_path.exists():
                logger.info(f"Loading WHOIS from {whois_path}")
                whois_loader = WHOISLoader(whois_path)
                as_network = whois_loader.load_to_as_network(as_network)

            # Store in context
            context["as_network"] = as_network
            context["peeringdb_df"] = pdb_loader.load_networks()

            stats = as_network.get_statistics()
            logger.info(f"Loaded {stats['total_asns']} ASNs from {stats['total_organizations']} organizations")

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
    """Scrape HTML from websites."""

    def run(self, context: Dict[str, Any]) -> PipelineResult:
        """Scrape HTML content."""
        start_time = datetime.utcnow()
        logger.info(f"Starting {self.name}")

        try:
            df = context["peeringdb_df"]
            
            # Filter to non-empty websites
            df_websites = df[df["website"].notna() & (df["website"] != "")]
            urls = df_websites["website"].unique().tolist()
            
            logger.info(f"Scraping {len(urls)} unique websites")

            # Initialize scraper
            scraper = HTMLScraper()
            
            # Scrape with progress bar
            pbar = tqdm(total=len(urls), desc="Scraping HTML")
            results = scraper.scrape_urls(
                urls,
                progress_callback=lambda x: pbar.update(x)
            )
            pbar.close()
            
            # Store results
            context["website_data"] = results
            
            # Count successes
            successful = sum(1 for r in results if not r.error)
            failed = len(results) - successful
            
            logger.info(f"Scraped {successful} websites successfully, {failed} failed")

            return self._create_result(
                status="success" if failed == 0 else "partial",
                records_processed=successful,
                records_failed=failed,
                start_time=start_time
            )

        except Exception as e:
            logger.error(f"HTML scraping failed: {e}")
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
            
            # Store results
            context["as_relationships"] = relationships
            
            logger.info(f"Found {len(relationships)} AS relationships")

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
            as_network = context["as_network"]
            df = context["peeringdb_df"]
            
            # Create URL to ASN mapping
            url_to_asn = {}
            for _, row in df.iterrows():
                if pd.notna(row.get("website")):
                    url = row["website"]
                    asn = row["asn"]
                    if url not in url_to_asn:
                        url_to_asn[url] = []
                    url_to_asn[url].append(asn)
            
            # Initialize analyzer
            analyzer = RedirectAnalyzer(as_network)
            
            # Analyze redirects
            redirect_df = analyzer.analyze_redirects(website_data)
            
            # Find domain relationships
            if not redirect_df.empty:
                groups = analyzer.find_domain_relationships(redirect_df, url_to_asn)
                as_network.network_groups.extend(groups)
                
                # Process domain data
                processed_df = URLProcessor.process_redirects(redirect_df)
                context["redirect_analysis"] = processed_df
                
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
            
            # Group favicons by hash
            favicon_groups = {}
            favicon_bytes = {}
            
            for url, data in favicon_data.items():
                if data:
                    hash_val = FaviconProcessor.hash_favicon(data)
                    if hash_val not in favicon_groups:
                        favicon_groups[hash_val] = []
                        favicon_bytes[hash_val] = data
                    favicon_groups[hash_val].append(url)
            
            # Filter to common favicons
            common_favicons = {k: v for k, v in favicon_groups.items() if len(v) >= 3}
            
            logger.info(f"Analyzing {len(common_favicons)} common favicons")
            
            # Initialize analyzer
            analyzer = FaviconAnalyzer()
            
            # Analyze favicons
            analyses = analyzer.analyze_favicons(favicon_bytes, common_favicons)
            
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
            whois_path = Path(self.config["input_files"]["whois"])
            
            if not whois_path.exists():
                logger.warning("WHOIS file not found, skipping")
                return self._create_result(
                    status="success",
                    records_processed=0,
                    start_time=start_time
                )
            
            as_network = context["as_network"]
            
            # Load WHOIS data
            loader = WHOISLoader(whois_path)
            df = loader.load(skiprows=95315)
            
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
            
            logger.info(f"Processed {len(org_groups)} organizations, found {len(groups)} multi-ASN orgs")

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
                pipeline_results=context.get("pipeline_results", [])
            )
            
            # Get dataframes
            dfs = as_network.to_dataframes()
            
            # Add additional analysis results
            if "redirect_analysis" in context:
                dfs["redirect_analysis"] = context["redirect_analysis"]
            
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

            return self._create_result(
                status="success",
                records_processed=len(dfs),
                output_files=output_files,
                start_time=start_time
            )

        except Exception as e:
            logger.error(f"Export failed: {e}")
            return self._create_result(
                status="failed",
                records_processed=0,
                errors=[str(e)],
                start_time=start_time
            )