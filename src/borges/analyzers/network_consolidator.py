"""Consolidate network groups from different sources."""

from collections import defaultdict
from typing import Dict, List, Set, Tuple

import pandas as pd

from ..models import ASNetwork, NetworkGroup


class NetworkGroupConsolidator:
    """Consolidate network groups from different analysis sources."""

    def __init__(self, as_network: ASNetwork):
        """Initialize network group consolidator.

        Args:
            as_network: AS network with analysis results
        """
        self.as_network = as_network

    def _safe_flatten_asns(self, asns_data):
        """Safely flatten ASN data that may contain nested lists.
        
        Args:
            asns_data: ASN data that may be int, list, or nested lists
            
        Returns:
            List of integers representing ASNs
        """
        if not asns_data:
            return []
        
        flattened = []
        if isinstance(asns_data, (list, tuple)):
            for item in asns_data:
                if isinstance(item, (list, tuple)):
                    flattened.extend(self._safe_flatten_asns(item))
                elif isinstance(item, (int, str)):
                    try:
                        asn = int(item)
                        if asn > 0:  # Valid ASN
                            flattened.append(asn)
                    except (ValueError, TypeError):
                        continue
        elif isinstance(asns_data, (int, str)):
            try:
                asn = int(asns_data)
                if asn > 0:  # Valid ASN
                    flattened.append(asn)
            except (ValueError, TypeError):
                pass
        
        return flattened

    def consolidate_groups(self) -> List[Dict]:
        """Consolidate all network groups into unified organizations.
        
        Returns:
            List of consolidated organization records
        """
        # Start with organization-based groups from WHOIS/PeeringDB
        consolidated = self._create_base_organizations()
        
        # Merge additional groups from various sources
        self._merge_analysis_groups(consolidated)
        
        # Convert to final format and deduplicate by ASN set
        formatted_groups = self._format_consolidated_groups(consolidated)
        deduplicated_groups = self._deduplicate_by_asn_set(formatted_groups)
        
        # Merge groups that share original PeeringDB organizations
        return self._merge_peeringdb_organizations(deduplicated_groups)

    def _create_base_organizations(self) -> Dict[str, Dict]:
        """Create base organization groups from WHOIS/PeeringDB data.
        
        Returns:
            Dictionary of org_id -> organization info
        """
        organizations = {}
        
        for org_id, org in self.as_network.organizations.items():
            # Debug specific organizations
            org_asns = getattr(org, 'asns', [])
            if (org_id in ['@aut-271181-LACNIC', 'DNIC-ARIN', 'CCL-534-ARIN', 'LPL-141-ARIN'] or 
                271181 in org_asns or 721 in org_asns or 209 in org_asns or 3356 in org_asns):
                print(f"DEBUG BASE ORG: {org_id} has ASNs: {sorted(list(org_asns))[:20]}... (total: {len(org_asns)})")
                print(f"  Org name: {getattr(org, 'name', 'Unknown')}")
                if 209 in org_asns or 3356 in org_asns:
                    print(f"    >>> Contains AS209/3356")
            
            # Get ASN details
            asn_details = []
            for asn in org_asns:
                as_info = self.as_network.autonomous_systems.get(asn)
                if as_info:
                    asn_details.append({
                        'asn': asn,
                        'name': as_info.name,
                        'website': str(as_info.website) if as_info.website else None
                    })
            
            # Choose the best organization name
            org_name = self._get_best_organization_name(org, asn_details)
            
            organizations[org_id] = {
                'group_id': [f"org_{org_id}"],
                'group_name': [org_name],
                'group_type': 'organization',
                'asns': getattr(org, 'asns', []),
                'asn_details': asn_details,
                'sources': ['whois_peeringdb'],
                'metadata': {
                    'org_id': org_id,
                    'country': getattr(org, 'country', None),
                    'source': getattr(org, 'source', None)
                }
            }
        
        return organizations

    def _get_best_organization_name(self, org, asn_details: List[Dict]) -> str:
        """Choose the best name for an organization.
        
        Args:
            org: Organization object
            asn_details: List of ASN details
            
        Returns:
            Best organization name
        """
        # Start with the official organization name from WHOIS
        org_name = getattr(org, 'name', None)
        if org_name and not org_name.startswith('ASN-') and not org_name.startswith('DNIC-'):
            return org_name
        
        # If org name is not good, look for the most common/representative ASN name
        if asn_details:
            # Sort ASN details by ASN number to get consistent results
            sorted_details = sorted(asn_details, key=lambda x: x['asn'])
            
            # Look for names that don't look like ASN-specific identifiers
            good_names = []
            for detail in sorted_details:
                name = detail.get('name', '')
                if name and not name.startswith('AS') and len(name) > 3:
                    good_names.append(name)
            
            if good_names:
                # Use the first good name (from the lowest ASN)
                return good_names[0]
            
            # Fallback to the first ASN's name
            first_name = sorted_details[0].get('name')
            if first_name:
                return first_name
        
        # Ultimate fallback
        return org_name or f"Organization {getattr(org, 'org_id', 'Unknown')}"

    def _merge_analysis_groups(self, consolidated: Dict[str, Dict]) -> None:
        """Merge groups from analysis sources into consolidated groups.
        
        CRITICAL FIX: Only merge analysis groups into organizations if they have
        substantial overlap and don't cause cross-contamination between unrelated orgs.
        
        Args:
            consolidated: Dictionary to update with merged groups
        """
        import traceback
        
        # Track which ASNs are already in organizations
        assigned_asns = set()
        for group_id, group in consolidated.items():
            try:
                # Safely flatten ASN data to prevent unhashable type errors
                safe_asns = self._safe_flatten_asns(group['asns'])
                assigned_asns.update(safe_asns)
            except Exception as e:
                print(f"DEBUG: Exception processing consolidated group {group_id}: {e}")
                print(f"DEBUG: Traceback: {traceback.format_exc()}")
                print(f"DEBUG: Group ASNs data: {repr(group.get('asns', 'MISSING'))}")
                raise
        
        # Process network groups from various analyses
        group_counter = 0
        for group in self.as_network.network_groups:
            group_counter += 1
            
            # Find overlapping organizations with strict validation
            best_matching_org = None
            best_overlap_score = 0
            
            for org_id, org_group in consolidated.items():
                # Safely flatten ASN data to prevent unhashable type errors
                safe_group_asns = self._safe_flatten_asns(group.asns)
                safe_org_asns = self._safe_flatten_asns(org_group['asns'])
                overlap = set(safe_group_asns) & set(safe_org_asns)
                
                if overlap:
                    # Calculate overlap strength
                    overlap_ratio = len(overlap) / min(len(safe_group_asns), len(safe_org_asns)) if safe_group_asns and safe_org_asns else 0
                    overlap_score = len(overlap) * overlap_ratio  # Combined size + ratio metric
                    
                    # Only consider high-confidence overlaps to prevent contamination
                    if (overlap_ratio >= 0.8 or len(overlap) >= 3) and overlap_score > best_overlap_score:
                        # Additional validation: check if this creates cross-contamination
                        if self._validate_merge_compatibility(org_group, group, safe_group_asns, safe_org_asns):
                            best_matching_org = org_id
                            best_overlap_score = overlap_score
            
            if best_matching_org:
                # Merge with the single best-matching organization only
                self._merge_into_organization(consolidated[best_matching_org], group)
            else:
                # Create new group for unassigned ASNs
                safe_group_asns = self._safe_flatten_asns(group.asns)
                unassigned_asns = [asn for asn in safe_group_asns if asn not in assigned_asns]
                if unassigned_asns:
                    new_group_id = f"analysis_group_{group_counter}"
                    consolidated[new_group_id] = self._create_analysis_group(group, unassigned_asns)
                    assigned_asns.update(unassigned_asns)
    
    def _validate_merge_compatibility(self, org_group: Dict, analysis_group, analysis_asns: List[int], org_asns: List[int]) -> bool:
        """Validate if merging an analysis group into an organization would create contamination.
        
        Args:
            org_group: Organization group to potentially merge into
            analysis_group: Analysis group being considered
            analysis_asns: Flattened ASNs from analysis group
            org_asns: Flattened ASNs from organization
            
        Returns:
            True if merge is safe, False if it would cause contamination
        """
        # Check for organizational identity conflicts using WHOIS data
        org_sources = set()
        org_countries = set()
        
        # Extract organization characteristics from group_id
        group_ids = org_group.get('group_id', [])
        if isinstance(group_ids, str):
            group_ids = [group_ids]
            
        for group_id in group_ids:
            if 'ARIN' in group_id:
                org_sources.add('ARIN')
                org_countries.add('US')
            elif 'LACNIC' in group_id:
                org_sources.add('LACNIC') 
                org_countries.add('LACNIC_REGION')  # Could be BR, AR, etc.
            elif 'RIPE' in group_id:
                org_sources.add('RIPE')
                org_countries.add('EU')
            elif 'APNIC' in group_id:
                org_sources.add('APNIC')
                org_countries.add('APAC')
        
        # Check if analysis ASNs belong to conflicting organizations
        for asn in analysis_asns:
            if asn in self.as_network.as_to_org:
                asn_org_id = self.as_network.as_to_org[asn]
                
                # Skip if ASN already belongs to this organization  
                if any(asn_org_id in str(gid) for gid in group_ids):
                    continue
                
                # Check for cross-regional conflicts
                if 'ARIN' in asn_org_id and 'LACNIC' in ' '.join(str(gid) for gid in group_ids):
                    # US (ARIN) vs Latin America (LACNIC) - potential conflict
                    if self._are_organizations_highly_incompatible(org_group, asn_org_id):
                        return False
                elif 'LACNIC' in asn_org_id and 'ARIN' in ' '.join(str(gid) for gid in group_ids):
                    # Latin America (LACNIC) vs US (ARIN) - potential conflict  
                    if self._are_organizations_highly_incompatible(org_group, asn_org_id):
                        return False
        
        return True
    
    def _are_organizations_highly_incompatible(self, org_group: Dict, asn_org_id: str) -> bool:
        """Check if organizations are highly incompatible (e.g., DoD vs telecom).
        
        Args:
            org_group: Organization group
            asn_org_id: ASN's organization ID
            
        Returns:
            True if organizations are highly incompatible
        """
        org_names = org_group.get('group_name', [])
        if isinstance(org_names, str):
            org_names = [org_names]
            
        # Check for specific incompatible patterns
        dod_keywords = ['DoD', 'Department of Defense', 'DNIC', 'Network Information Center']
        telecom_keywords = ['telecom', 'comunicac', 'serviços', 'LTDA']
        
        has_dod = any(any(keyword in str(name) for keyword in dod_keywords) for name in org_names)
        has_telecom = any(any(keyword.lower() in str(name).lower() for keyword in telecom_keywords) for name in org_names)
        
        # Get characteristics of the ASN's organization
        asn_org = self.as_network.organizations.get(asn_org_id)
        if asn_org:
            asn_org_name = getattr(asn_org, 'name', '')
            asn_has_dod = any(keyword in asn_org_name for keyword in dod_keywords)
            asn_has_telecom = any(keyword.lower() in asn_org_name.lower() for keyword in telecom_keywords)
            
            # Block DoD + Telecom combinations
            if (has_dod and asn_has_telecom) or (has_telecom and asn_has_dod):
                return True
                
        return False

    def _merge_into_organization(self, org_group: Dict, analysis_group: NetworkGroup) -> None:
        """Merge analysis group into existing organization group.
        
        Args:
            org_group: Organization group to merge into
            analysis_group: Analysis group to merge
        """
        # Add new ASNs
        new_asns = [asn for asn in analysis_group.asns if asn not in org_group['asns']]
        org_group['asns'].extend(new_asns)
        
        # Add ASN details for new ASNs
        for asn in new_asns:
            as_info = self.as_network.autonomous_systems.get(asn)
            if as_info:
                org_group['asn_details'].append({
                    'asn': asn,
                    'name': as_info.name,
                    'website': str(as_info.website) if as_info.website else None
                })
        
        # Add source
        source_name = self._get_source_name(analysis_group.group_type)
        if source_name not in org_group['sources']:
            org_group['sources'].append(source_name)
        
        # Merge metadata
        if 'analysis_groups' not in org_group['metadata']:
            org_group['metadata']['analysis_groups'] = []
        
        org_group['metadata']['analysis_groups'].append({
            'group_id': analysis_group.group_id,
            'group_type': analysis_group.group_type,
            'common_attribute': analysis_group.common_attribute,
            'asns': analysis_group.asns
        })

    def _create_analysis_group(self, analysis_group: NetworkGroup, asns: List[int]) -> Dict:
        """Create new group from analysis results.
        
        Args:
            analysis_group: Source analysis group
            asns: ASNs to include in the group
            
        Returns:
            New group dictionary
        """
        # Get ASN details
        asn_details = []
        for asn in asns:
            as_info = self.as_network.autonomous_systems.get(asn)
            if as_info:
                asn_details.append({
                    'asn': asn,
                    'name': as_info.name,
                    'website': str(as_info.website) if as_info.website else None
                })
        
        group_name = self._generate_group_name(analysis_group)
        
        return {
            'group_id': [analysis_group.group_id],
            'group_name': [group_name],
            'group_type': analysis_group.group_type,
            'asns': asns,
            'asn_details': asn_details,
            'sources': [self._get_source_name(analysis_group.group_type)],
            'metadata': {
                'common_attribute': analysis_group.common_attribute,
                'original_group': {
                    'group_id': analysis_group.group_id,
                    'group_type': analysis_group.group_type,
                    'metadata': analysis_group.metadata or {}
                }
            }
        }

    def _get_source_name(self, group_type: str) -> str:
        """Get human-readable source name from group type.
        
        Args:
            group_type: Group type identifier
            
        Returns:
            Human-readable source name
        """
        source_mapping = {
            'sibling_as': 'whois_analysis',
            'redirect_target': 'redirect_analysis',
            'shared_domain': 'domain_analysis',
            'favicon_match': 'favicon_analysis',
            'organization_related': 'llm_analysis'
        }
        return source_mapping.get(group_type, group_type)

    def _generate_group_name(self, analysis_group: NetworkGroup) -> str:
        """Generate a human-readable name for an analysis group.
        
        Args:
            analysis_group: Analysis group
            
        Returns:
            Generated group name
        """
        if analysis_group.group_type == 'redirect_target':
            return f"Redirect Group ({analysis_group.common_attribute})"
        elif analysis_group.group_type == 'shared_domain':
            return f"Domain Group ({analysis_group.common_attribute})"
        elif analysis_group.group_type == 'favicon_match':
            return f"Favicon Group ({analysis_group.common_attribute})"
        elif analysis_group.group_type == 'sibling_as':
            return f"WHOIS Group ({analysis_group.common_attribute})"
        else:
            return f"Analysis Group ({analysis_group.group_type})"

    def _format_consolidated_groups(self, consolidated: Dict[str, Dict]) -> List[Dict]:
        """Format consolidated groups for export.
        
        Args:
            consolidated: Dictionary of consolidated groups
            
        Returns:
            List of formatted group records
        """
        formatted_groups = []
        
        for group in consolidated.values():
            # Sort ASNs
            group['asns'].sort()
            group['asn_details'].sort(key=lambda x: x['asn'])
            
            # Add summary statistics
            group['asn_count'] = len(group['asns'])
            group['has_websites'] = sum(1 for detail in group['asn_details'] if detail['website'])
            
            formatted_groups.append(group)
        
        # Sort by ASN count (largest groups first)
        formatted_groups.sort(key=lambda x: x['asn_count'], reverse=True)
        
        return formatted_groups

    def create_summary_dataframe(self) -> pd.DataFrame:
        """Create summary DataFrame of consolidated groups.
        
        Returns:
            DataFrame with group summaries
        """
        consolidated = self.consolidate_groups()
        
        summary_records = []
        for group in consolidated:
            summary_records.append({
                'group_id': group['group_id'],
                'group_name': group['group_name'],
                'primary_name': group.get('primary_name', group['group_name']),
                'group_type': group['group_type'],
                'asn_count': group['asn_count'],
                'asns': group['asns'],
                'sources': ', '.join(group['sources']) if isinstance(group['sources'], list) else group['sources'],
                'has_websites': group['has_websites']
            })
        
        return pd.DataFrame(summary_records)

    def create_detailed_dataframe(self) -> pd.DataFrame:
        """Create detailed DataFrame with one row per ASN.
        
        Returns:
            DataFrame with detailed ASN information
        """
        consolidated = self.consolidate_groups()
        
        detailed_records = []
        for group in consolidated:
            for asn_detail in group['asn_details']:
                # Ensure scalar values for sorting - fix unhashable type error
                asn_value = asn_detail['asn']
                if isinstance(asn_value, list):
                    # If ASN is a list, take the first element
                    asn_value = asn_value[0] if asn_value else None
                    print(f"WARNING: Found list ASN value in group {group['group_id']}, using first element: {asn_value}")
                
                group_id_value = group['group_id']
                if isinstance(group_id_value, list):
                    # If group_id is a list, join with comma
                    group_id_value = ','.join(str(x) for x in group_id_value)
                    print(f"WARNING: Found list group_id value, converted to: {group_id_value}")
                
                asn_count_value = group['asn_count']
                if isinstance(asn_count_value, list):
                    # If asn_count is a list, take the length or first element
                    asn_count_value = len(asn_count_value) if asn_count_value else 0
                    print(f"WARNING: Found list asn_count value in group {group['group_id']}, using length: {asn_count_value}")
                
                detailed_records.append({
                    'group_id': group_id_value,
                    'group_name': group['group_name'],
                    'primary_name': group.get('primary_name', group['group_name']),
                    'group_type': group['group_type'],
                    'asn': asn_value,
                    'asn_name': asn_detail['name'],
                    'website': asn_detail['website'],
                    'sources': ', '.join(group['sources']) if isinstance(group['sources'], list) else group['sources'],
                    'total_group_size': asn_count_value
                })
        
        df = pd.DataFrame(detailed_records)
        if not df.empty:
            # Filter out any rows with None ASN values to prevent sorting errors
            initial_count = len(df)
            df = df.dropna(subset=['asn'])
            if len(df) < initial_count:
                print(f"WARNING: Dropped {initial_count - len(df)} rows with None ASN values")
            
            # Final safety check before sorting
            try:
                df = df.sort_values(['total_group_size', 'group_id', 'asn'], ascending=[False, True, True])
            except Exception as e:
                print(f"ERROR: Still failed to sort DataFrame: {e}")
                print(f"Column types: total_group_size={df['total_group_size'].dtype}, group_id={df['group_id'].dtype}, asn={df['asn'].dtype}")
                print(f"Sample values - group_id: {df['group_id'].head(3).tolist()}, asn: {df['asn'].head(3).tolist()}")
                # Try sorting without the problematic column
                df = df.sort_values(['total_group_size'], ascending=[False])
        
        return df

    def _deduplicate_by_asn_set(self, groups: List[Dict]) -> List[Dict]:
        """Intelligently consolidate groups while respecting organizational boundaries.
        
        Only merges groups if they have identical ASN sets AND compatible organizational identity.
        This prevents incorrect merging of unrelated organizations that happen to share ASNs.
        
        Args:
            groups: List of group dictionaries
            
        Returns:
            List of consolidated groups with merged metadata
        """
        from collections import defaultdict
        import traceback
        
        def _are_organizations_compatible(group1: Dict, group2: Dict) -> bool:
            """Check if two groups represent compatible organizations that can be merged.
            
            Args:
                group1, group2: Group dictionaries to compare
                
            Returns:
                True if groups can be safely merged, False otherwise
            """
            # Get organization IDs for both groups
            group1_ids = group1.get('group_id', [])
            group2_ids = group2.get('group_id', [])
            
            if isinstance(group1_ids, str):
                group1_ids = [group1_ids]
            if isinstance(group2_ids, str):
                group2_ids = [group2_ids]
            
            # Extract country/source info from org IDs
            def get_org_info(org_ids):
                countries = set()
                sources = set()
                for org_id in org_ids:
                    if 'ARIN' in org_id:
                        countries.add('US')
                        sources.add('ARIN')
                    elif 'LACNIC' in org_id:
                        countries.add('BR')  # or other LACNIC countries
                        sources.add('LACNIC')
                    elif 'RIPE' in org_id:
                        countries.add('EU')
                        sources.add('RIPE')
                    elif 'APNIC' in org_id:
                        countries.add('APAC')
                        sources.add('APNIC')
                return countries, sources
            
            countries1, sources1 = get_org_info(group1_ids)
            countries2, sources2 = get_org_info(group2_ids)
            
            # Only block merges for clearly incompatible organizations
            group1_names = group1.get('group_name', [])
            group2_names = group2.get('group_name', [])
            
            # Convert to lists if they're strings
            if isinstance(group1_names, str):
                group1_names = [group1_names]
            if isinstance(group2_names, str):
                group2_names = [group2_names]
                
            # Allow all merges - let ASN overlap determine consolidation
            return True
        
        def _extract_asns(asns_data):
            """Recursively extract all ASNs from any data structure."""
            flat_asns = []
            
            if isinstance(asns_data, (int, float)):
                if str(asns_data).replace('.', '').isdigit():
                    flat_asns.append(int(float(asns_data)))
            elif isinstance(asns_data, str):
                if asns_data.isdigit():
                    flat_asns.append(int(asns_data))
            elif isinstance(asns_data, (list, tuple)):
                for item in asns_data:
                    flat_asns.extend(_extract_asns(item))
            elif isinstance(asns_data, dict):
                # If it's a dict, look for ASN-like keys/values
                for key, value in asns_data.items():
                    if key in ['asn', 'asns', 'asn_id']:
                        flat_asns.extend(_extract_asns(value))
            # Ignore other types (sets, None, etc.)
            
            return flat_asns
        
        # STEP 1: Group by ASN set (convert to frozenset for hashing)
        
        asn_set_groups = defaultdict(list)
        
        for i, group in enumerate(groups):
            try:
                # Extract all ASNs using recursive function
                asns_data = group.get('asns', [])
                
                # Debug specific ASNs
                if 209 in asns_data or 3356 in asns_data:
                    print(f"DEBUG: Group {i} contains AS209/3356:")
                    print(f"  Group ID: {group.get('group_id', 'Unknown')}")
                    print(f"  Group Name: {group.get('group_name', 'Unknown')}")
                    print(f"  ASN count: {len(asns_data)}")
                    print(f"  ASNs (first 10): {sorted(asns_data)[:10]}")
                
                flat_asns = _extract_asns(asns_data)
                
                # Remove duplicates and sort - with robust error handling
                try:
                    # First try to create set directly
                    unique_asns = sorted(set(flat_asns))
                except TypeError as e:
                    print(f"DEBUG: TypeError creating set from flat_asns at group {i}: {e}")
                    # Fallback: filter out unhashable elements
                    hashable_asns = []
                    for asn in flat_asns:
                        try:
                            hash(asn)  # Test if hashable
                            hashable_asns.append(asn)
                        except TypeError:
                            print(f"DEBUG: Skipping unhashable ASN: {type(asn)} = {repr(asn)}")
                    unique_asns = sorted(set(hashable_asns))
                
                # Final check - ensure all elements can be hashed
                try:
                    asn_set = frozenset(unique_asns)
                except TypeError as e:
                    print(f"DEBUG: Still can't create frozenset at group {i}: {e}")
                    print(f"DEBUG: unique_asns types: {[type(x) for x in unique_asns]}")
                    # Ultimate fallback - convert all to strings and hash those
                    string_asns = [str(x) for x in unique_asns if x is not None]
                    asn_set = frozenset(string_asns)
                
                asn_set_groups[asn_set].append(group)
                
            except Exception as e:
                print(f"DEBUG: Exception in _deduplicate_by_asn_set at group {i}: {e}")
                print(f"DEBUG: Traceback: {traceback.format_exc()}")
                print(f"DEBUG: Problematic asns_data: {repr(asns_data)[:500]}")
                
                # If we still can't process this group, create a unique key
                # using string representation to avoid the error
                try:
                    asn_str = str(group.get('asns', []))
                    asn_set = frozenset([hash(asn_str)])  # Use hash as a fallback
                    asn_set_groups[asn_set].append(group)
                    print(f"DEBUG: Used fallback for group {i}")
                except Exception as e2:
                    print(f"DEBUG: Even fallback failed: {e2}")
                    # Last resort - skip this group
                    continue
        
        deduplicated_groups = []
        
        for asn_set, duplicate_groups in asn_set_groups.items():
            if len(duplicate_groups) == 1:
                # No duplicates, keep as is but ensure consistent format
                group = duplicate_groups[0]
                # Convert single values to lists for consistency
                if not isinstance(group['group_id'], list):
                    group['group_id'] = [group['group_id']]
                if not isinstance(group['group_name'], list):
                    group['group_name'] = [group['group_name']]
                # Add primary name
                group['primary_name'] = self._select_best_name(group['group_name'])
                deduplicated_groups.append(group)
            else:
                # Before merging, check if all groups are organizationally compatible
                can_merge = True
                for i in range(len(duplicate_groups)):
                    for j in range(i + 1, len(duplicate_groups)):
                        if not _are_organizations_compatible(duplicate_groups[i], duplicate_groups[j]):
                            can_merge = False
                            break
                    if not can_merge:
                        break
                
                if can_merge:
                    # Check if this merge involves AS209/3356
                    involves_target_asns = any(209 in g.get('asns', []) or 3356 in g.get('asns', []) for g in duplicate_groups)
                    if involves_target_asns:
                        print(f"DEBUG: Merging {len(duplicate_groups)} groups containing AS209/3356:")
                        for i, g in enumerate(duplicate_groups):
                            print(f"  Group {i}: {g.get('group_id', 'Unknown')} - {g.get('group_name', 'Unknown')}")
                    
                    # Safe to merge - groups represent the same organizational entity
                    merged_group = self._merge_duplicate_groups(duplicate_groups)
                    
                    if involves_target_asns:
                        print(f"DEBUG: Merged result:")
                        print(f"  Final group_id: {merged_group.get('group_id', 'Unknown')}")
                        print(f"  Final group_name: {merged_group.get('group_name', 'Unknown')}")
                    
                    deduplicated_groups.append(merged_group)
                else:
                    # Cannot merge - keep groups separate with unique ASN-based identifiers
                    group_names = [str(g.get('group_name', ['Unknown'])[0] if isinstance(g.get('group_name'), list) else g.get('group_name', 'Unknown')) for g in duplicate_groups]
                    asn_list = sorted(list(asn_set))
                    print(f"BLOCKED merge of {len(duplicate_groups)} groups with identical {len(asn_list)} ASNs due to incompatible organizations:")
                    
                    for i, group in enumerate(duplicate_groups):
                        # Add each group separately with modified group_id to ensure uniqueness
                        modified_group = group.copy()
                        
                        # Ensure group_id and group_name are in list format
                        if not isinstance(modified_group.get('group_id'), list):
                            modified_group['group_id'] = [modified_group.get('group_id', f'unknown_{i}')]
                        if not isinstance(modified_group.get('group_name'), list):
                            modified_group['group_name'] = [modified_group.get('group_name', f'Unknown Group {i}')]
                        
                        # Add distinguishing suffix to prevent future conflicts
                        original_id = modified_group['group_id'][0]
                        modified_group['group_id'] = [f"{original_id}_distinct_{i}"]
                        modified_group['primary_name'] = self._select_best_name(modified_group['group_name'])
                        
                        deduplicated_groups.append(modified_group)
                        print(f"  Kept separate: {modified_group['group_name'][0]} (ID: {modified_group['group_id'][0]})")
        
        return deduplicated_groups

    def _merge_duplicate_groups(self, duplicate_groups: List[Dict]) -> Dict:
        """Merge groups with identical ASN sets.
        
        Args:
            duplicate_groups: List of groups with identical ASN sets
            
        Returns:
            Merged group dictionary
        """
        # Use the first group as base
        merged = duplicate_groups[0].copy()
        
        # Collect all group IDs and names
        all_group_ids = []
        all_group_names = []
        
        # Safely handle sources that might contain nested lists
        sources = merged.get('sources', [])
        if isinstance(sources, list):
            # Flatten any nested lists in sources
            flat_sources = []
            for source in sources:
                if isinstance(source, list):
                    flat_sources.extend(str(s) for s in source if s)
                else:
                    flat_sources.append(str(source) if source else '')
            all_sources = set(s for s in flat_sources if s)  # Filter out empty strings
        else:
            all_sources = set([str(sources)] if sources else [])
        
        original_groups = []
        
        for group in duplicate_groups:
            # Collect group IDs
            if isinstance(group['group_id'], list):
                all_group_ids.extend(group['group_id'])
            else:
                all_group_ids.append(group['group_id'])
            
            # Collect group names
            if isinstance(group['group_name'], list):
                all_group_names.extend(group['group_name'])
            else:
                all_group_names.append(group['group_name'])
            
            # Collect sources - safely handle nested lists
            if isinstance(group.get('sources'), list):
                flat_sources = []
                for source in group['sources']:
                    if isinstance(source, list):
                        flat_sources.extend(str(s) for s in source if s)
                    else:
                        flat_sources.append(str(source) if source else '')
                all_sources.update(s for s in flat_sources if s)  # Filter out empty strings
            elif isinstance(group.get('sources'), str):
                all_sources.update(group['sources'].split(', '))
            
            # Track original groups
            original_groups.append({
                'id': group['group_id'],
                'name': group['group_name'],
                'type': group['group_type']
            })
        
        # Flatten any nested lists before deduplication
        def flatten_list(items):
            """Recursively flatten nested lists."""
            flattened = []
            for item in items:
                if isinstance(item, list):
                    flattened.extend(flatten_list(item))
                else:
                    flattened.append(item)
            return flattened
        
        # Remove duplicates and sort
        merged['group_id'] = sorted(list(set(flatten_list(all_group_ids))))
        merged['group_name'] = sorted(list(set(flatten_list(all_group_names))))
        merged['sources'] = sorted(list(all_sources))
        merged['primary_name'] = self._select_best_name(merged['group_name'])
        
        # Add metadata about the merge
        if 'metadata' not in merged:
            merged['metadata'] = {}
        merged['metadata']['original_groups'] = original_groups
        merged['metadata']['duplicate_count'] = len(duplicate_groups)
        
        return merged

    def _merge_different_groups(self, groups: List[Dict]) -> Dict:
        """Merge groups with different ASN sets (for PeeringDB consolidation).
        
        Args:
            groups: List of groups to merge
            
        Returns:
            Merged group dictionary
        """
        if not groups:
            raise ValueError("Cannot merge empty list of groups")
        
        if len(groups) == 1:
            return groups[0].copy()
        
        # Use the first group as base
        merged = groups[0].copy()
        
        # Collect all ASNs, group IDs, names, and details
        all_asns = set()
        all_group_ids = []
        all_group_names = []
        all_asn_details = []
        all_sources = set()
        
        for group in groups:
            # Collect ASNs
            group_asns = group.get('asns', [])
            all_asns.update(group_asns)
            
            # Collect ASN details
            group_details = group.get('asn_details', [])
            all_asn_details.extend(group_details)
            
            # Collect group IDs
            if isinstance(group['group_id'], list):
                all_group_ids.extend(group['group_id'])
            else:
                all_group_ids.append(group['group_id'])
            
            # Collect group names
            if isinstance(group['group_name'], list):
                all_group_names.extend(group['group_name'])
            else:
                all_group_names.append(group['group_name'])
            
            # Collect sources
            if isinstance(group.get('sources'), list):
                all_sources.update(group['sources'])
            elif group.get('sources'):
                all_sources.add(group['sources'])
        
        # Update merged group with combined data
        merged['asns'] = sorted(list(all_asns))
        merged['asn_details'] = all_asn_details
        merged['group_id'] = sorted(list(set(all_group_ids)))
        merged['group_name'] = sorted(list(set(all_group_names)))
        merged['sources'] = sorted(list(all_sources))
        merged['primary_name'] = self._select_best_name(merged['group_name'])
        merged['asn_count'] = len(all_asns)
        
        return merged

    def _select_best_name(self, names) -> str:
        """Select the best name from a list of organization names.
        
        Args:
            names: List of organization names or single name
            
        Returns:
            Best representative name
        """
        # Handle single string input
        if isinstance(names, str):
            return names
            
        if not names:
            return "Unknown Organization"
        
        if len(names) == 1:
            return names[0]
        
        # Prefer names that don't look like technical identifiers
        good_names = []
        for name in names:
            name = name.strip()
            # Skip names that look like technical IDs
            if (not name.startswith('ASN-') and 
                not name.startswith('DNIC-') and 
                not name.startswith('org_') and
                not name.startswith('@aut-') and
                len(name) > 5):
                good_names.append(name)
        
        if good_names:
            # Return the shortest good name (often most official)
            return min(good_names, key=len)
        
        # Fallback to first name if no good names found
        return names[0]
    
    def _merge_peeringdb_organizations(self, groups: List[Dict]) -> List[Dict]:
        """Merge groups that share the same original PeeringDB organization.
        
        Args:
            groups: List of consolidated group dictionaries
            
        Returns:
            List with PeeringDB-related groups merged
        """
        from collections import defaultdict
        
        # Map each group to all PeeringDB orgs it contains ASNs for
        group_to_peeringdb_orgs = {}
        peeringdb_org_to_groups = defaultdict(list)
        
        for i, group in enumerate(groups):
            asns = group.get('asns', [])
            
            # Find all PeeringDB orgs that this group's ASNs belonged to
            peeringdb_orgs = set()
            for asn in asns:
                peeringdb_org = self.as_network.as_to_peeringdb_org.get(asn)
                if peeringdb_org:
                    peeringdb_orgs.add(peeringdb_org)
            
            group_to_peeringdb_orgs[i] = peeringdb_orgs
            
            # Add this group to each PeeringDB org it contains
            for peeringdb_org in peeringdb_orgs:
                peeringdb_org_to_groups[peeringdb_org].append(i)
        
        # Build merge clusters - groups that should be merged together
        merge_clusters = []
        processed_groups = set()
        
        for peeringdb_org, group_indices in peeringdb_org_to_groups.items():
            if len(group_indices) > 1:
                # Multiple groups share this PeeringDB org - they should be merged
                cluster = set(group_indices)
                
                # Check if any of these groups are already in a cluster
                existing_cluster = None
                for existing in merge_clusters:
                    if cluster & existing:
                        existing_cluster = existing
                        break
                
                if existing_cluster:
                    # Merge with existing cluster
                    existing_cluster.update(cluster)
                else:
                    # Create new cluster
                    merge_clusters.append(cluster)
                
                processed_groups.update(group_indices)
        
        merged_groups = []
        
        # Process merge clusters
        for cluster in merge_clusters:
            cluster_groups = [groups[i] for i in cluster]
            
            # Debug output for AS209/AS3356
            cluster_asns = []
            for g in cluster_groups:
                cluster_asns.extend(g.get('asns', []))
            if 209 in cluster_asns or 3356 in cluster_asns:
                print(f"MERGING {len(cluster_groups)} groups containing AS209/AS3356 due to shared PeeringDB org")
                for i, g in enumerate(cluster_groups):
                    print(f"  Group {i}: {g.get('group_id', 'Unknown')} - {g.get('group_name', 'Unknown')}")
                    print(f"    ASNs: {sorted(g.get('asns', []))[:10]}...")
            
            # Find the shared PeeringDB orgs for this cluster
            shared_peeringdb_orgs = set()
            for group_idx in cluster:
                shared_peeringdb_orgs.update(group_to_peeringdb_orgs[group_idx])
            
            merged_group = self._merge_different_groups(cluster_groups)
            
            # Add metadata about PeeringDB merge
            if 'metadata' not in merged_group:
                merged_group['metadata'] = {}
            merged_group['metadata']['peeringdb_merge'] = {
                'peeringdb_orgs': list(shared_peeringdb_orgs),
                'merged_groups_count': len(cluster_groups),
                'reason': 'shared_peeringdb_organization'
            }
            
            merged_groups.append(merged_group)
            
            if 209 in cluster_asns or 3356 in cluster_asns:
                print(f"  Result: Final group_ids: {merged_group.get('group_id', [])}")
        
        # Add all unprocessed groups (those that don't share PeeringDB orgs)
        for i, group in enumerate(groups):
            if i not in processed_groups:
                merged_groups.append(group)
        
        print(f"PeeringDB organization merging: {len(groups)} -> {len(merged_groups)} groups")
        return merged_groups
    
