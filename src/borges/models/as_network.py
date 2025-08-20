"""AS Network data structures and operations."""

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import pandas as pd

from .schemas import ASRelationship, AutonomousSystem, NetworkGroup, Organization
from ..config import get_config


def normalize_website(url: str) -> str:
    """Normalize website URL for better matching.
    
    Args:
        url: Website URL
        
    Returns:
        Normalized URL
    """
    if not url:
        return url
        
    # Parse URL
    parsed = urlparse(url)
    
    # Get domain without www prefix
    domain = parsed.netloc.lower()
    if domain.startswith('www.'):
        domain = domain[4:]
    
    # Return normalized domain (ignore protocol, path, params)
    return domain


@dataclass
class ASNetwork:
    """Container for AS network data and relationships."""
    
    # Core data
    autonomous_systems: Dict[int, AutonomousSystem] = field(default_factory=dict)
    organizations: Dict[str, Organization] = field(default_factory=dict)
    
    # Relationships
    as_relationships: List[ASRelationship] = field(default_factory=list)
    as_to_org: Dict[int, str] = field(default_factory=dict)
    org_to_as: Dict[str, Set[int]] = field(default_factory=lambda: defaultdict(set))
    
    # Original PeeringDB organization mappings (preserved before WHOIS overwrites)
    as_to_peeringdb_org: Dict[int, str] = field(default_factory=dict)
    peeringdb_org_to_as: Dict[str, Set[int]] = field(default_factory=lambda: defaultdict(set))
    
    # Website data
    as_to_website: Dict[int, str] = field(default_factory=dict)
    website_to_as: Dict[str, Set[int]] = field(default_factory=lambda: defaultdict(set))
    domain_to_as: Dict[str, Set[int]] = field(default_factory=lambda: defaultdict(set))
    
    # Groups
    network_groups: List[NetworkGroup] = field(default_factory=list)
    
    def add_as(self, as_info: AutonomousSystem) -> None:
        """Add an Autonomous System to the network.
        
        Args:
            as_info: Autonomous System information
        """
        self.autonomous_systems[as_info.asn] = as_info
        
        # Update organization mapping
        if as_info.org_id:
            self.as_to_org[as_info.asn] = as_info.org_id
            self.org_to_as[as_info.org_id].add(as_info.asn)
            
            # Create or update organization - only create from PeeringDB data, not for every ASN
            # WHOIS data will handle proper organization grouping later
        
        # Update website mapping with normalization
        if as_info.website:
            website_str = str(as_info.website)
            normalized_website = normalize_website(website_str)
            if normalized_website:  # Only map if normalization succeeded
                self.as_to_website[as_info.asn] = normalized_website
                self.website_to_as[normalized_website].add(as_info.asn)
    
    def add_relationship(self, relationship: ASRelationship) -> None:
        """Add an AS relationship.
        
        Args:
            relationship: AS relationship information
        """
        self.as_relationships.append(relationship)
        
        # Auto-create network group if there are related ASNs
        if relationship.related_asns and len(relationship.related_asns) > 0:
            # Create a network group from the relationship
            all_asns = [relationship.source_asn] + relationship.related_asns
            group = NetworkGroup(
                group_id=f"llm_relationship_{relationship.source_asn}",
                group_type="llm_detected",
                asns=all_asns,
                common_attribute=f"LLM detected relationship from AS{relationship.source_asn}",
                metadata={
                    "source_asn": relationship.source_asn,
                    "confidence": relationship.confidence,
                    "detected_by": relationship.detected_by,
                    "relationship_type": relationship.relationship_type
                }
            )
            self.network_groups.append(group)
    
    def add_domain_mapping(self, domain: str, asns: List[int]) -> None:
        """Add domain to AS mapping.
        
        Args:
            domain: Domain name
            asns: List of AS numbers
        """
        self.domain_to_as[domain].update(asns)
    
    def get_related_asns(self, asn: int) -> Set[int]:
        """Get all ASNs related to a given ASN.
        
        Args:
            asn: Autonomous System Number
            
        Returns:
            Set of related ASNs
        """
        related = set()
        
        # Check direct relationships
        for rel in self.as_relationships:
            if rel.source_asn == asn:
                related.update(rel.related_asns)
            elif asn in rel.related_asns:
                related.add(rel.source_asn)
        
        # Check same organization
        org_id = self.as_to_org.get(asn)
        if org_id:
            related.update(self.org_to_as[org_id])
            related.discard(asn)  # Remove self
        
        # Check same website
        website = self.as_to_website.get(asn)
        if website:
            related.update(self.website_to_as[website])
            related.discard(asn)  # Remove self
        
        return related
    
    def get_organization_asns(self, org_id: str) -> Set[int]:
        """Get all ASNs for an organization.
        
        Args:
            org_id: Organization ID
            
        Returns:
            Set of ASNs
        """
        return self.org_to_as.get(org_id, set())
    
    def merge_organizations(self, org_id1: str, org_id2: str, new_org_id: Optional[str] = None) -> str:
        """Merge two organizations.
        
        Args:
            org_id1: First organization ID
            org_id2: Second organization ID
            new_org_id: New organization ID (uses org_id1 if not provided)
            
        Returns:
            Merged organization ID
        """
        if new_org_id is None:
            new_org_id = org_id1
        
        # Get all ASNs from both organizations
        asns1 = self.org_to_as.get(org_id1, set())
        asns2 = self.org_to_as.get(org_id2, set())
        all_asns = asns1 | asns2
        
        # Update mappings
        for asn in all_asns:
            self.as_to_org[asn] = new_org_id
        
        self.org_to_as[new_org_id] = all_asns
        
        # Remove old organizations if different
        if org_id1 != new_org_id and org_id1 in self.org_to_as:
            del self.org_to_as[org_id1]
        if org_id2 != new_org_id and org_id2 in self.org_to_as:
            del self.org_to_as[org_id2]
        
        # Update organization object
        if new_org_id not in self.organizations:
            self.organizations[new_org_id] = Organization(
                org_id=new_org_id,
                asns=sorted(all_asns)
            )
        else:
            self.organizations[new_org_id].asns = sorted(all_asns)
        
        return new_org_id
    
    def create_domain_groups(self) -> List[NetworkGroup]:
        """Create network groups based on shared domains.
        
        Returns:
            List of network groups
        """
        groups = []
        
        for domain, asns in self.domain_to_as.items():
            if len(asns) > 1:  # Only create groups with multiple ASNs
                group = NetworkGroup(
                    group_id=f"domain_{domain}",
                    group_type="shared_domain",
                    asns=sorted(asns),
                    common_attribute=domain,
                    metadata={"domain": domain, "asn_count": len(asns)}
                )
                groups.append(group)
        
        self.network_groups.extend(groups)
        return groups
    
    def to_dataframes(self) -> Dict[str, pd.DataFrame]:
        """Convert network data to pandas DataFrames.
        
        Returns:
            Dictionary of DataFrames
        """
        # AS DataFrame
        as_data = []
        for asn, as_info in self.autonomous_systems.items():
            as_data.append({
                "asn": asn,
                "org_id": as_info.org_id,
                "name": as_info.name,
                "website": str(as_info.website) if as_info.website else None,
                "notes": as_info.notes,
                "aka": as_info.aka
            })
        df_as = pd.DataFrame(as_data)
        
        # Organization DataFrame
        org_data = []
        for org_id, org in self.organizations.items():
            # Handle both dict and object organizations
            if hasattr(org, 'name'):
                # It's an Organization object
                org_name = org.name
                asns = org.asns
            elif isinstance(org, dict):
                # It's a dict
                org_name = org.get('name')
                asns = org.get('asns', [])
            else:
                # Fallback
                org_name = None
                asns = []
                
            org_data.append({
                "org_id": org_id,
                "name": org_name,
                "asn_count": len(asns),
                "asns": asns
            })
        df_org = pd.DataFrame(org_data)
        
        # Relationships DataFrame
        rel_data = []
        for rel in self.as_relationships:
            rel_data.append({
                "source_asn": rel.source_asn,
                "related_asns": rel.related_asns,
                "relationship_type": rel.relationship_type,
                "confidence": rel.confidence,
                "detected_by": rel.detected_by,
                "evidence": rel.evidence
            })
        df_rel = pd.DataFrame(rel_data) if rel_data else pd.DataFrame()
        
        # Groups DataFrame
        group_data = []
        for group in self.network_groups:
            group_data.append({
                "group_id": group.group_id,
                "group_type": group.group_type,
                "asn_count": len(group.asns),
                "asns": group.asns,
                "common_attribute": group.common_attribute
            })
        df_groups = pd.DataFrame(group_data) if group_data else pd.DataFrame()
        
        return {
            "autonomous_systems": df_as,
            "organizations": df_org,
            "relationships": df_rel,
            "network_groups": df_groups
        }
    
    def get_statistics(self) -> Dict[str, int]:
        """Get network statistics.
        
        Returns:
            Dictionary of statistics
        """
        return {
            "total_asns": len(self.autonomous_systems),
            "total_organizations": len(self.organizations),
            "total_relationships": len(self.as_relationships),
            "total_groups": len(self.network_groups),
            "asns_with_websites": len(self.as_to_website),
            "unique_websites": len(self.website_to_as),
            "unique_domains": len(self.domain_to_as)
        }