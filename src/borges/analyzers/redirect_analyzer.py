"""Analyze URL redirects and domain relationships."""

from typing import Dict, List, Set

import pandas as pd
import tldextract

from ..data.processors import URLProcessor
from ..models import ASNetwork, NetworkGroup, WebsiteInfo


class RedirectAnalyzer:
    """Analyze URL redirects to find AS relationships."""

    def __init__(self, as_network: ASNetwork):
        """Initialize redirect analyzer.

        Args:
            as_network: AS network to update with findings
        """
        self.as_network = as_network

    def analyze_redirects(self, website_data: List[WebsiteInfo]) -> pd.DataFrame:
        """Analyze redirects from website scraping data.

        Args:
            website_data: List of website information

        Returns:
            DataFrame with redirect analysis
        """
        # Convert to DataFrame
        records = []
        for info in website_data:
            if info.final_url and not info.error:
                records.append({
                    "original_url": str(info.original_url),
                    "final_url": str(info.final_url),
                    "redirects": [str(r) for r in info.redirects],
                    "redirect_count": len(info.redirects)
                })

        df = pd.DataFrame(records)

        if df.empty:
            return df

        # Extract domains
        df["original_domain"] = df["original_url"].apply(URLProcessor.extract_domain)
        df["final_domain"] = df["final_url"].apply(URLProcessor.extract_domain)

        # Flag cross-domain redirects
        df["cross_domain"] = df["original_domain"] != df["final_domain"]

        return df

    def find_domain_relationships(
        self,
        redirect_df: pd.DataFrame,
        asn_mapping: Dict[str, List[int]]
    ) -> List[NetworkGroup]:
        """Find sibling relationships through shared domains.

        Args:
            redirect_df: DataFrame with redirect analysis
            asn_mapping: Mapping of URL to ASN list

        Returns:
            List of network groups
        """
        groups = []

        # Group by final URL
        url_groups = redirect_df.groupby("final_url")["original_url"].apply(list).reset_index()

        for _, row in url_groups.iterrows():
            final_url = row["final_url"]
            original_urls = row["original_url"]

            # Collect all ASNs that redirect to this final URL
            all_asns = set()
            for url in original_urls + [final_url]:
                if url in asn_mapping:
                    all_asns.update(asn_mapping[url])

            if len(all_asns) > 1:
                # Create a network group
                group = NetworkGroup(
                    group_id=f"redirect_{final_url[:50]}",
                    group_type="redirect_target",
                    asns=sorted(all_asns),
                    common_attribute=final_url,
                    metadata={
                        "final_url": final_url,
                        "original_count": len(original_urls)
                    }
                )
                groups.append(group)

                # Update AS network
                domain = URLProcessor.extract_domain(final_url)
                self.as_network.add_domain_mapping(domain, list(all_asns))

        return groups

    def analyze_domain_consolidation(self, redirect_df: pd.DataFrame) -> pd.DataFrame:
        """Analyze domain consolidation patterns.

        Args:
            redirect_df: DataFrame with redirect analysis

        Returns:
            DataFrame with domain consolidation analysis
        """
        # Group by final domain
        domain_groups = redirect_df.groupby("final_domain").agg({
            "original_domain": lambda x: list(set(x)),
            "original_url": "count",
            "cross_domain": "sum"
        }).reset_index()

        domain_groups.columns = ["final_domain", "source_domains", "total_redirects", "cross_domain_count"]

        # Calculate metrics
        domain_groups["source_domain_count"] = domain_groups["source_domains"].apply(len)
        domain_groups["consolidation_ratio"] = (
            domain_groups["cross_domain_count"] / domain_groups["total_redirects"]
        )

        # Sort by consolidation
        domain_groups = domain_groups.sort_values("source_domain_count", ascending=False)

        return domain_groups

    def find_redirect_chains(self, website_data: List[WebsiteInfo]) -> pd.DataFrame:
        """Find and analyze redirect chains.

        Args:
            website_data: List of website information

        Returns:
            DataFrame with redirect chain analysis
        """
        chains = []

        for info in website_data:
            if info.redirects and len(info.redirects) > 1:
                chain = {
                    "start_url": str(info.original_url),
                    "end_url": str(info.final_url),
                    "chain_length": len(info.redirects),
                    "chain": " -> ".join([str(info.original_url)] + [str(r) for r in info.redirects] + [str(info.final_url)])
                }
                chains.append(chain)

        df = pd.DataFrame(chains)

        if not df.empty:
            # Sort by chain length
            df = df.sort_values("chain_length", ascending=False)

        return df