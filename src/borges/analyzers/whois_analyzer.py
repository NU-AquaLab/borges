"""Analyze WHOIS data for sibling AS relationships."""

from collections import defaultdict
from typing import Dict, List, Set

import pandas as pd

from ..models import ASNetwork, ASRelationship, NetworkGroup


class WHOISAnalyzer:
    """Analyze WHOIS data to find sibling AS relationships."""

    def __init__(self, as_network: ASNetwork):
        """Initialize WHOIS analyzer.

        Args:
            as_network: AS network to update with findings
        """
        self.as_network = as_network

    def analyze_whois_data(self, whois_df: pd.DataFrame) -> Dict[str, List[int]]:
        """Analyze WHOIS data to group ASNs by organization (sibling AS relationships).

        Args:
            whois_df: DataFrame with WHOIS data

        Returns:
            Dictionary mapping org_id to ASN list
        """
        # Ensure ASN column is numeric
        if "ASN" in whois_df.columns:
            whois_df["ASN"] = pd.to_numeric(whois_df["ASN"], errors="coerce")
            whois_df = whois_df[whois_df["ASN"].notna()]

        # Group by organization ID
        if "org_id" in whois_df.columns:
            org_groups = whois_df.groupby("org_id")["ASN"].apply(
                lambda x: sorted(list(set(x.astype(int))))
            ).to_dict()
        else:
            org_groups = {}

        # Update AS network
        for org_id, asns in org_groups.items():
            for asn in asns:
                self.as_network.as_to_org[asn] = str(org_id)
                self.as_network.org_to_as[str(org_id)].update(asns)

        return org_groups

    def find_multi_asn_organizations(
        self,
        org_groups: Dict[str, List[int]],
        min_asns: int = 2
    ) -> List[NetworkGroup]:
        """Find organizations with multiple ASNs (sibling AS relationships).

        Args:
            org_groups: Dictionary mapping org_id to ASN list
            min_asns: Minimum ASNs to be considered multi-ASN

        Returns:
            List of network groups
        """
        groups = []

        for org_id, asns in org_groups.items():
            if len(asns) >= min_asns:
                group = NetworkGroup(
                    group_id=f"org_{org_id}",
                    group_type="sibling_as",
                    asns=sorted(asns),
                    common_attribute=org_id,
                    metadata={
                        "org_id": org_id,
                        "asn_count": len(asns)
                    }
                )
                groups.append(group)

        return groups

    def create_whois_relationships(
        self,
        org_groups: Dict[str, List[int]]
    ) -> List[ASRelationship]:
        """Create sibling AS relationships from WHOIS data.

        Args:
            org_groups: Dictionary mapping org_id to ASN list

        Returns:
            List of AS relationships
        """
        relationships = []

        for org_id, asns in org_groups.items():
            if len(asns) > 1:
                # Create sibling AS relationships between all ASNs in the organization
                # Use the first ASN as the source
                source_asn = asns[0]
                related_asns = asns[1:]

                relationship = ASRelationship(
                    source_asn=source_asn,
                    related_asns=related_asns,
                    relationship_type="sibling_as",
                    confidence=1.0,  # WHOIS data is authoritative
                    evidence=f"Sibling AS - Same organization ID: {org_id}",
                    detected_by="whois_analysis"
                )
                relationships.append(relationship)

        return relationships

    def analyze_organization_size(
        self,
        org_groups: Dict[str, List[int]]
    ) -> pd.DataFrame:
        """Analyze organization sizes by sibling AS count.

        Args:
            org_groups: Dictionary mapping org_id to ASN list

        Returns:
            DataFrame with organization size analysis
        """
        org_sizes = []

        for org_id, asns in org_groups.items():
            org_sizes.append({
                "org_id": org_id,
                "asn_count": len(asns),
                "asns": asns
            })

        df = pd.DataFrame(org_sizes)

        if not df.empty:
            # Add size categories
            df["size_category"] = pd.cut(
                df["asn_count"],
                bins=[0, 1, 5, 20, 100, float("inf")],
                labels=["single", "small", "medium", "large", "very_large"]
            )

            # Sort by ASN count
            df = df.sort_values("asn_count", ascending=False)

        return df

    def merge_organizations_by_name(
        self,
        whois_df: pd.DataFrame,
        similarity_threshold: float = 0.9
    ) -> Dict[str, Set[str]]:
        """Merge organizations with similar names to find additional sibling AS relationships.

        Args:
            whois_df: DataFrame with WHOIS data
            similarity_threshold: Similarity threshold for merging

        Returns:
            Dictionary mapping merged org_id to set of original org_ids
        """
        # This is a simplified version - in production you might use
        # fuzzy string matching or more sophisticated algorithms

        if "org_name" not in whois_df.columns:
            return {}

        # Group by normalized organization name
        org_name_groups = defaultdict(set)

        for _, row in whois_df[["org_id", "org_name"]].drop_duplicates().iterrows():
            if pd.notna(row["org_name"]):
                # Simple normalization
                normalized_name = row["org_name"].lower().strip()
                # Remove common suffixes
                for suffix in [" inc", " llc", " ltd", " corp", " sa", " gmbh"]:
                    normalized_name = normalized_name.replace(suffix, "")
                normalized_name = normalized_name.strip()

                org_name_groups[normalized_name].add(row["org_id"])

        # Create merge mapping
        merges = {}
        for norm_name, org_ids in org_name_groups.items():
            if len(org_ids) > 1:
                # Use the first org_id as the primary
                primary_org = sorted(org_ids)[0]
                merges[primary_org] = org_ids

        return merges