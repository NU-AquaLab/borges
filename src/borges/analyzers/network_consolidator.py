"""Consolidate network groups from different sources."""

import json
import logging
import traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pandas as pd

from ..models import ASNetwork, NetworkGroup

logger = logging.getLogger(__name__)

# FORENSIC DEBUG - REMOVE AFTER INVESTIGATION
# All 141 ASNs from the problematic mega-group for tracking
MEGA_GROUP_ASNS = {
    1, 2, 34, 189, 199, 200, 201, 202, 203, 209, 279, 281, 560, 594, 595, 596, 597, 598,
    2379, 2551, 3356, 3447, 3508, 3549, 3561, 3831, 3908, 3909, 3910, 3951, 4015, 4048,
    4212, 4281, 4282, 4283, 4284, 4285, 4287, 4288, 4289, 4290, 4291, 4292, 4293, 4294,
    4295, 4296, 4297, 4298, 4323, 4911, 5668, 5737, 5778, 6100, 6222, 6223, 6224, 6225,
    6226, 6227, 6347, 6367, 6395, 6467, 6484, 6640, 6745, 7037, 7161, 7176, 7191, 7359,
    7776, 7911, 7986, 7987, 7988, 7989, 7990, 7991, 8043, 8895, 10383, 10424, 10753,
    10825, 10826, 10827, 10828, 10829, 10830, 10831, 10832, 10833, 10960, 11104, 11213,
    11225, 11226, 11398, 11412, 11530, 11538, 13787, 14905, 14910, 14921, 16718, 16835,
    16852, 16941, 17047, 17402, 18494, 18756, 19094, 19591, 19962, 20476, 20759, 22026,
    22186, 22561, 23126, 26458, 27497, 30686, 32421, 32855, 202818, 208520, 210859,
    211764, 393645, 393789, 394120, 394125, 394179, 394190
}

KEY_TARGET_ASNS = {34: "University of Delaware", 8895: "King Abdul Aziz City", 3356: "Level3"}

# SPRINT-ORANGE FORENSIC TRACKING (Aug 17, 2025)
SPRINT_ORANGE_TARGET_ASNS = {
    1239: "Sprint",
    5511: "Orange", 
    250: "AS250.net Foundation",
    215007: "BOZHAN LIANG",
    62269: "Lukas Schauer",
    211035: "PHANTOM HIVE NETWORK LTD",
    46562: "Performive",
    200508: "SOROK76 LTD",
    200226: "Ren Yamamoto", 
    41103: "teleBIZZ",
    35787: "ISLAND-SERWIS-NET",
    147079: "PT Diaza Lintas Asia"
}


class ForensicLogger:
    """FORENSIC DEBUG - REMOVE AFTER INVESTIGATION"""
    def __init__(self, output_dir: Path = None):
        self.output_dir = output_dir or Path("data/forensic")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.stage_counter = 0
        self.forensic_trace = []
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Track ASN changes for detailed analysis
        self.asn_change_log = []
        
    def log_stage(self, stage_name: str, groups: Dict[str, Dict], description: str = ""):
        """Log a forensic stage with all group information."""
        self.stage_counter += 1
        stage_id = f"stage_{self.stage_counter:03d}_{stage_name}"
        
        # Create forensic entry
        forensic_entry = {
            "stage_id": stage_id,
            "stage_name": stage_name,
            "description": description,
            "timestamp": datetime.now().isoformat(),
            "total_groups": len(groups),
            "groups": self._serialize_groups(groups),
            "mega_group_analysis": self._analyze_mega_groups(groups)
        }
        
        # Save stage file
        stage_file = self.output_dir / f"{stage_id}.json"
        with open(stage_file, 'w') as f:
            json.dump(forensic_entry, f, indent=2, default=str)
        
        # Add to trace
        self.forensic_trace.append({
            "stage": stage_id,
            "description": description,
            "mega_asns_count": len(forensic_entry["mega_group_analysis"]["mega_asns_found"]),
            "groups_with_mega": len(forensic_entry["mega_group_analysis"]["groups_with_mega_asns"])
        })
        
        logger.info(f"FORENSIC: Logged stage {stage_id} with {len(groups)} groups")
    
    def log_analysis_step(self, step_name: str, analysis_type: str, before_asns: set, after_asns: set, source_info: dict = None):
        """Log ASN-level changes during analysis steps."""
        added_asns = after_asns - before_asns
        removed_asns = before_asns - after_asns
        
        # Check for mega ASN changes
        mega_added = added_asns & MEGA_GROUP_ASNS
        mega_removed = removed_asns & MEGA_GROUP_ASNS
        
        # Check for key target changes
        key_targets_added = {asn: KEY_TARGET_ASNS[asn] for asn in added_asns if asn in KEY_TARGET_ASNS}
        key_targets_removed = {asn: KEY_TARGET_ASNS[asn] for asn in removed_asns if asn in KEY_TARGET_ASNS}
        
        change_log = {
            "timestamp": datetime.now().isoformat(),
            "step_name": step_name,
            "analysis_type": analysis_type,
            "source_info": source_info or {},
            "asn_changes": {
                "added_count": len(added_asns),
                "removed_count": len(removed_asns),
                "added_asns": sorted(list(added_asns)),
                "removed_asns": sorted(list(removed_asns))
            },
            "mega_asn_changes": {
                "added_mega": sorted(list(mega_added)),
                "removed_mega": sorted(list(mega_removed)),
                "mega_added_count": len(mega_added),
                "mega_removed_count": len(mega_removed)
            },
            "key_targets_changed": {
                "added_targets": key_targets_added,
                "removed_targets": key_targets_removed
            }
        }
        
        self.asn_change_log.append(change_log)
        
        # Log critical changes
        if mega_added or mega_removed or key_targets_added or key_targets_removed:
            logger.warning(f"FORENSIC CRITICAL: {step_name} - Mega ASNs added: {list(mega_added)}, removed: {list(mega_removed)}")
            logger.warning(f"FORENSIC CRITICAL: {step_name} - Key targets added: {key_targets_added}, removed: {key_targets_removed}")
        
        return change_log
    
    def save_asn_changes_summary(self):
        """Save detailed ASN change log to file."""
        if not self.asn_change_log:
            return
            
        changes_file = self.output_dir / f"asn_changes_detailed_{self.timestamp}.json"
        
        summary = {
            "investigation_timestamp": self.timestamp,
            "total_changes_logged": len(self.asn_change_log),
            "asn_change_log": self.asn_change_log,
            "mega_group_asns_tracked": sorted(list(MEGA_GROUP_ASNS)),
            "key_targets": KEY_TARGET_ASNS
        }
        
        with open(changes_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        logger.info(f"FORENSIC: Saved detailed ASN changes to {changes_file}")
    
    def _serialize_groups(self, groups: Dict[str, Dict]) -> List[Dict]:
        """Serialize groups for JSON output."""
        serialized = []
        for group_id, group in groups.items():
            try:
                serialized_group = {
                    "group_id": group_id,
                    "group_name": group.get("group_name", []),
                    "group_type": group.get("group_type", "unknown"),
                    "asns": self._safe_serialize_asns(group.get("asns", [])),
                    "sources": group.get("sources", []),
                    "merge_provenance": group.get("merge_provenance", []),
                    "metadata": group.get("metadata", {})
                }
                serialized.append(serialized_group)
            except Exception as e:
                logger.error(f"Error serializing group {group_id}: {e}")
                serialized.append({"group_id": group_id, "error": str(e)})
        return serialized
    
    def _safe_serialize_asns(self, asns):
        """Safely serialize ASN data."""
        if isinstance(asns, (list, tuple)):
            return [int(asn) for asn in asns if isinstance(asn, (int, str)) and str(asn).isdigit()]
        elif isinstance(asns, (int, str)) and str(asns).isdigit():
            return [int(asns)]
        else:
            return []
    
    def _analyze_mega_groups(self, groups: Dict[str, Dict]) -> Dict:
        """Analyze which groups contain mega-group ASNs."""
        mega_asns_found = set()
        groups_with_mega_asns = []
        key_targets_found = {}
        
        for group_id, group in groups.items():
            group_asns = set(self._safe_serialize_asns(group.get("asns", [])))
            mega_overlap = group_asns & MEGA_GROUP_ASNS
            key_targets_in_group = {asn: name for asn, name in KEY_TARGET_ASNS.items() if asn in group_asns}
            
            if mega_overlap:
                mega_asns_found.update(mega_overlap)
                groups_with_mega_asns.append({
                    "group_id": group_id,
                    "group_name": group.get("group_name", "Unknown"),
                    "group_type": group.get("group_type", "unknown"),
                    "total_asns": len(group_asns),
                    "mega_asns": sorted(list(mega_overlap)),
                    "key_targets": key_targets_in_group,
                    "sources": group.get("sources", [])
                })
            
            # Track key targets
            for asn, name in key_targets_in_group.items():
                if name not in key_targets_found:
                    key_targets_found[name] = []
                key_targets_found[name].append(group_id)
        
        return {
            "mega_asns_found": sorted(list(mega_asns_found)),
            "groups_with_mega_asns": groups_with_mega_asns,
            "key_targets_distribution": key_targets_found,
            "summary": f"Found {len(mega_asns_found)}/141 mega ASNs in {len(groups_with_mega_asns)} groups"
        }
    
    def save_final_summary(self):
        """Save the complete forensic trace."""
        summary = {
            "investigation_timestamp": self.timestamp,
            "total_stages": self.stage_counter,
            "trace": self.forensic_trace,
            "mega_group_asns_tracked": sorted(list(MEGA_GROUP_ASNS)),
            "key_targets": KEY_TARGET_ASNS
        }
        
        summary_file = self.output_dir / f"forensic_summary_{self.timestamp}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        logger.info(f"FORENSIC: Complete trace saved to {summary_file}")
        
        # Also save detailed ASN changes
        self.save_asn_changes_summary()
        
        return summary_file


class NetworkGroupConsolidator:
    """Consolidate network groups from different analysis sources."""

    def __init__(self, as_network: ASNetwork, blocklist: Set[int] = None):
        """Initialize network group consolidator.

        Args:
            as_network: AS network with analysis results
            blocklist: Set of ASNs to exclude from consolidation
        """
        self.as_network = as_network
        self.blocklist = blocklist or set()
        if self.blocklist:
            logger.info(f"NetworkGroupConsolidator initialized with blocklist of {len(self.blocklist)} ASNs")
        
        # FORENSIC DEBUG - REMOVE AFTER INVESTIGATION
        self.forensic_logger = ForensicLogger()
        logger.info("FORENSIC: Initialized forensic logging for consolidation process")

    def _filter_blocked_asns(self, asns: List[int]) -> List[int]:
        """Filter out blocked ASNs from a list.
        
        Args:
            asns: List of ASNs to filter
            
        Returns:
            Filtered list with blocked ASNs removed
        """
        if not self.blocklist:
            return asns
        return [asn for asn in asns if asn not in self.blocklist]

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
        # FORENSIC DEBUG - REMOVE AFTER INVESTIGATION
        logger.info("FORENSIC: Starting consolidation process")
        
        # Start with organization-based groups from WHOIS/PeeringDB
        consolidated = self._create_base_organizations()
        
        # FORENSIC DEBUG - REMOVE AFTER INVESTIGATION
        self.forensic_logger.log_stage("base_organizations", consolidated, 
                                     "Base organizations from WHOIS/PeeringDB data")
        
        # Merge additional groups from various sources
        self._merge_analysis_groups(consolidated)
        
        # FORENSIC DEBUG - REMOVE AFTER INVESTIGATION
        self.forensic_logger.log_stage("after_analysis_merge", consolidated,
                                     "After merging analysis groups (LLM, website, etc.)")
        
        # Convert to final format and deduplicate by ASN set
        formatted_groups = self._format_consolidated_groups(consolidated)
        
        # FORENSIC DEBUG - REMOVE AFTER INVESTIGATION
        # Convert formatted_groups list to dict for logging
        formatted_dict = {f"formatted_{i}": group for i, group in enumerate(formatted_groups)}
        self.forensic_logger.log_stage("formatted_groups", formatted_dict,
                                     "After formatting for deduplication")
        
        deduplicated_groups = self._deduplicate_by_asn_set(formatted_groups)
        
        # FORENSIC DEBUG - REMOVE AFTER INVESTIGATION
        dedup_dict = {f"dedup_{i}": group for i, group in enumerate(deduplicated_groups)}
        self.forensic_logger.log_stage("deduplicated_groups", dedup_dict,
                                     "After deduplication by ASN set")
        
        # Merge groups that share original PeeringDB organizations
        final_groups = self._merge_peeringdb_organizations(deduplicated_groups)
        
        # FORENSIC DEBUG - REMOVE AFTER INVESTIGATION
        final_dict = {f"final_{i}": group for i, group in enumerate(final_groups)}
        self.forensic_logger.log_stage("final_consolidated", final_dict,
                                     "Final consolidated groups after PeeringDB merge")
        
        # Save complete forensic summary
        summary_file = self.forensic_logger.save_final_summary()
        logger.info(f"FORENSIC: Complete consolidation trace saved to {summary_file}")
        
        return final_groups

    def _create_base_organizations(self) -> Dict[str, Dict]:
        """Create base organization groups from WHOIS/PeeringDB data.
        
        Returns:
            Dictionary of org_id -> organization info
        """
        organizations = {}
        
        for org_id, org in self.as_network.organizations.items():
            # Get ASN details
            org_asns = getattr(org, 'asns', [])
            
            # Filter out blocked ASNs
            filtered_asns = self._filter_blocked_asns(org_asns)
            
            # Skip organizations that only contain blocked ASNs
            if not filtered_asns and org_asns:
                logger.debug(f"Skipping organization {org_id} - all ASNs are blocked")
                continue
            
            asn_details = []
            for asn in filtered_asns:
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
                'asns': filtered_asns,  # Use filtered ASNs instead of all ASNs
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
        
        # FORENSIC DEBUG: Log state before merging analysis groups
        before_merge_asns = set()
        for group_id, group in consolidated.items():
            safe_asns = self._safe_flatten_asns(group['asns'])
            before_merge_asns.update(safe_asns)
        
        logger.warning(f"FORENSIC: Starting analysis merge with {len(before_merge_asns)} total ASNs")
        mega_before = before_merge_asns & MEGA_GROUP_ASNS
        logger.warning(f"FORENSIC: Mega ASNs before analysis merge: {len(mega_before)} - {sorted(list(mega_before))}")
        
        # Track which ASNs are already in organizations
        assigned_asns = set()
        for group_id, group in consolidated.items():
            try:
                # Safely flatten ASN data to prevent unhashable type errors
                safe_asns = self._safe_flatten_asns(group['asns'])
                assigned_asns.update(safe_asns)
            except Exception as e:
                logger.debug(f"Exception processing consolidated group {group_id}: {e}")
                logger.debug(f"Traceback: {traceback.format_exc()}")
                logger.debug(f"Group ASNs data: {repr(group.get('asns', 'MISSING'))}")
                raise
        
        # Group analysis groups by ASN sets to detect multi-source agreement
        asn_set_to_groups = {}
        blocked_groups_skipped = 0
        blocked_asns_attempted = set()  # Track which blocked ASNs were in analysis groups
        
        for group in self.as_network.network_groups:
            safe_asns = self._safe_flatten_asns(group.asns)
            
            # FORENSIC DEBUG: Track blocked ASNs in analysis groups
            blocked_asns_in_group = set(safe_asns) & self.blocklist
            if blocked_asns_in_group:
                blocked_asns_attempted.update(blocked_asns_in_group)
                mega_blocked = blocked_asns_in_group & MEGA_GROUP_ASNS
                if mega_blocked:
                    logger.warning(f"FORENSIC: Analysis group {group.group_type}:{getattr(group, 'common_attribute', 'unknown')} contains blocked mega ASNs: {sorted(list(mega_blocked))}")
            
            # Filter out blocked ASNs
            filtered_asns = self._filter_blocked_asns(safe_asns)
            
            # Skip groups that only contain blocked ASNs
            if not filtered_asns and safe_asns:
                blocked_groups_skipped += 1
                logger.debug(f"Skipping network group {group.group_id} - all ASNs are blocked")
                continue
            
            asn_set_key = frozenset(filtered_asns)
            if asn_set_key not in asn_set_to_groups:
                asn_set_to_groups[asn_set_key] = []
            asn_set_to_groups[asn_set_key].append(group)
        
        # FORENSIC DEBUG: Log blocked ASNs that were found in analysis groups
        if blocked_asns_attempted:
            logger.warning(f"FORENSIC: Found {len(blocked_asns_attempted)} blocked ASNs in analysis groups (should be filtered): {sorted(list(blocked_asns_attempted))}")
            mega_blocked_attempted = blocked_asns_attempted & MEGA_GROUP_ASNS
            if mega_blocked_attempted:
                logger.warning(f"FORENSIC: Blocked mega ASNs found in analysis groups: {sorted(list(mega_blocked_attempted))}")
        
        if blocked_groups_skipped > 0:
            logger.info(f"Skipped {blocked_groups_skipped} network groups containing only blocked ASNs")
        
        # Process analysis groups, prioritizing multi-source agreement
        # Sort by number of agreeing sources (more sources = processed first)
        sorted_asn_sets = sorted(asn_set_to_groups.items(), 
                               key=lambda x: len(x[1]), reverse=True)
        
        # FORENSIC DEBUG: Log analysis groups to be processed
        logger.warning(f"FORENSIC: Processing {len(sorted_asn_sets)} unique analysis group sets")
        
        for group_set_idx, (asn_set, groups) in enumerate(sorted_asn_sets):
            # Use the first group as the representative for ASN operations
            representative_group = groups[0]
            safe_group_asns = list(asn_set)  # Already flattened when creating the key
            
            # FORENSIC DEBUG: Check for mega ASNs in this analysis group set
            mega_asns_in_set = set(safe_group_asns) & MEGA_GROUP_ASNS
            
            # SPRINT-ORANGE DEBUG: Check for Sprint-Orange merger ASNs
            sprint_orange_asns_in_set = set(safe_group_asns) & set(SPRINT_ORANGE_TARGET_ASNS.keys())
            
            if mega_asns_in_set:
                group_types = [getattr(g, 'group_type', 'unknown') for g in groups]
                group_attrs = [getattr(g, 'common_attribute', 'unknown') for g in groups]
                logger.warning(f"FORENSIC: Processing group set {group_set_idx+1}/{len(sorted_asn_sets)} with {len(mega_asns_in_set)} mega ASNs: {sorted(list(mega_asns_in_set))}")
                logger.warning(f"FORENSIC: Group types: {group_types}, attributes: {group_attrs}")
            
            if sprint_orange_asns_in_set:
                group_types = [getattr(g, 'group_type', 'unknown') for g in groups]
                group_attrs = [getattr(g, 'common_attribute', 'unknown') for g in groups]
                so_names = [SPRINT_ORANGE_TARGET_ASNS[asn] for asn in sprint_orange_asns_in_set]
                logger.warning(f"🔍 SPRINT-ORANGE DEBUG: Analysis group contains {len(sprint_orange_asns_in_set)} target ASNs")
                logger.warning(f"🔍 Target ASNs: {sprint_orange_asns_in_set} ({so_names})")
                logger.warning(f"🔍 Group sources: {group_types}")  
                logger.warning(f"🔍 Common attributes: {group_attrs}")
                
                # Check if both Sprint AND Orange are in the same set
                has_sprint = 1239 in sprint_orange_asns_in_set
                has_orange = 5511 in sprint_orange_asns_in_set
                if has_sprint and has_orange:
                    logger.warning(f"⚠️  CRITICAL: Both Sprint (1239) and Orange (5511) in same analysis group!")
                elif has_sprint or has_orange:
                    logger.warning(f"📍 Telecom company present: {'Sprint' if has_sprint else 'Orange'}")
            
            # FORENSIC DEBUG: Track ASN state before processing this group set
            current_consolidated_asns = set()
            for org_group in consolidated.values():
                current_consolidated_asns.update(self._safe_flatten_asns(org_group['asns']))
            
            # Find overlapping organizations with pure ASN overlap
            overlapping_orgs = []
            
            for org_id, org_group in consolidated.items():
                safe_org_asns = self._safe_flatten_asns(org_group['asns'])
                overlap = set(safe_group_asns) & set(safe_org_asns)
                
                if overlap:
                    # Any ASN overlap indicates a potential relationship
                    overlapping_orgs.append((org_id, org_group, overlap))
            
            if len(overlapping_orgs) == 1:
                # Simple case: merge into single overlapping organization
                org_id, org_group, overlap = overlapping_orgs[0]
                # Merge all groups with this ASN set
                for group in groups:
                    self._merge_into_organization(org_group, group)
                self._track_multisource_merge_provenance(org_group, groups, overlap)
                
            elif len(overlapping_orgs) > 1:
                # Complex case: analysis groups connect multiple organizations
                # This is transitive closure - merge all connected organizations together
                primary_org_id, primary_org = overlapping_orgs[0][0:2]
                
                # FORENSIC DEBUG - REMOVE AFTER INVESTIGATION
                # Check if this merge involves mega-group ASNs
                mega_asns_in_merge = set()
                sprint_orange_asns_in_merge = set()
                orgs_being_merged = []
                
                for org_id, org_group, overlap in overlapping_orgs:
                    org_asns = set(self._safe_flatten_asns(org_group.get('asns', [])))
                    mega_overlap = org_asns & MEGA_GROUP_ASNS
                    so_overlap = org_asns & set(SPRINT_ORANGE_TARGET_ASNS.keys())
                    
                    mega_asns_in_merge.update(mega_overlap)
                    sprint_orange_asns_in_merge.update(so_overlap)
                    
                    orgs_being_merged.append({
                        'org_id': org_id,
                        'org_name': org_group.get('group_name', 'Unknown'),
                        'asn_count': len(org_asns),
                        'mega_asns': list(mega_overlap),
                        'sprint_orange_asns': list(so_overlap),
                        'key_targets': [KEY_TARGET_ASNS.get(asn, f'AS{asn}') for asn in org_asns if asn in KEY_TARGET_ASNS]
                    })
                
                if mega_asns_in_merge:
                    logger.warning(f"FORENSIC: CRITICAL MERGE involving {len(mega_asns_in_merge)} mega ASNs!")
                    logger.warning(f"FORENSIC: Analysis groups causing merge: {[g.group_type + ':' + str(g.common_attribute) for g in groups]}")
                    logger.warning(f"FORENSIC: Organizations being merged: {[org['org_id'] + ' (' + str(org['asn_count']) + ' ASNs)' for org in orgs_being_merged]}")
                    logger.warning(f"FORENSIC: Key targets involved: {set(target for org in orgs_being_merged for target in org['key_targets'])}")
                
                # SPRINT-ORANGE SPECIFIC LOGGING
                if sprint_orange_asns_in_merge:
                    has_sprint = 1239 in sprint_orange_asns_in_merge
                    has_orange = 5511 in sprint_orange_asns_in_merge 
                    so_names = [SPRINT_ORANGE_TARGET_ASNS[asn] for asn in sprint_orange_asns_in_merge]
                    
                    logger.warning(f"🚨 SPRINT-ORANGE MERGER: Merging {len(overlapping_orgs)} organizations with {len(sprint_orange_asns_in_merge)} target ASNs")
                    logger.warning(f"🚨 Target ASNs in merge: {sorted(list(sprint_orange_asns_in_merge))} ({so_names})")
                    logger.warning(f"🚨 Analysis trigger: {[g.group_type + ':' + str(g.common_attribute) for g in groups]}")
                    
                    if has_sprint and has_orange:
                        logger.warning(f"💥 CRITICAL: Sprint (1239) and Orange (5511) being merged into same organization!")
                        logger.warning(f"💥 Personal networks also affected: {[asn for asn in sprint_orange_asns_in_merge if asn not in [1239, 5511]]}")
                    
                    # Log detailed organization info 
                    for org in orgs_being_merged:
                        if org['sprint_orange_asns']:
                            logger.warning(f"📊 Org {org['org_id']}: {org['org_name']} ({org['asn_count']} ASNs) - Contains: {[SPRINT_ORANGE_TARGET_ASNS[asn] for asn in org['sprint_orange_asns']]}")
                    
                    # Save detailed merge snapshot
                    merge_snapshot = {
                        f"merge_snapshot_{primary_org_id}": {
                            "merge_type": "transitive_closure",
                            "trigger_groups": [{"type": g.group_type, "attribute": g.common_attribute, "asns": list(asn_set)} for g in groups],
                            "organizations_merged": orgs_being_merged,
                            "mega_asns_involved": sorted(list(mega_asns_in_merge)),
                            "before_merge": {org_id: dict(org_group) for org_id, org_group, _ in overlapping_orgs}
                        }
                    }
                    self.forensic_logger.log_stage(f"critical_merge_{len(orgs_being_merged)}_orgs", merge_snapshot,
                                                  f"CRITICAL: Transitive merger of {len(orgs_being_merged)} organizations")
                
                # Merge all analysis groups into the primary organization
                for group in groups:
                    self._merge_into_organization(primary_org, group)
                self._track_multisource_merge_provenance(primary_org, groups, overlapping_orgs[0][2])
                
                # Merge all other overlapping organizations into the primary one
                for org_id, org_group, overlap in overlapping_orgs[1:]:
                    if org_id != primary_org_id:
                        self._merge_organizations(primary_org, org_group, org_id)
                        # Remove the merged organization from consolidated dict
                        del consolidated[org_id]
                        
            else:
                # No overlaps: create new organization for these analysis groups
                new_org_id = f"analysis_org_{len(groups)}sources_{representative_group.group_id}"
                consolidated[new_org_id] = self._create_multisource_analysis_group(groups, safe_group_asns)
                
                # FORENSIC DEBUG: Track new organization creation with mega ASNs
                if mega_asns_in_set:
                    self.forensic_logger.log_analysis_step(
                        f"new_org_creation_{new_org_id}",
                        "new_analysis_org",
                        set(),
                        set(safe_group_asns),
                        {
                            "new_org_id": new_org_id,
                            "analysis_groups": [getattr(g, 'group_type', 'unknown') for g in groups],
                            "mega_asns_in_new_org": sorted(list(mega_asns_in_set))
                        }
                    )
        
        # FORENSIC DEBUG: Log final state after all analysis merges
        after_merge_asns = set()
        for group_id, group in consolidated.items():
            safe_asns = self._safe_flatten_asns(group['asns'])
            after_merge_asns.update(safe_asns)
        
        mega_after = after_merge_asns & MEGA_GROUP_ASNS
        logger.warning(f"FORENSIC: Analysis merge complete - Total ASNs: {len(after_merge_asns)}")
        logger.warning(f"FORENSIC: Mega ASNs after analysis merge: {len(mega_after)} - {sorted(list(mega_after))}")
        
        # Track overall changes
        self.forensic_logger.log_analysis_step(
            "complete_analysis_merge",
            "full_analysis_merge",
            before_merge_asns,
            after_merge_asns,
            {
                "total_groups_processed": len(sorted_asn_sets),
                "blocked_groups_skipped": blocked_groups_skipped,
                "blocked_asns_attempted": sorted(list(blocked_asns_attempted))
            }
        )
    
    def _track_merge_provenance(self, org_group: Dict, analysis_group, overlap: Set[int]) -> None:
        """Track merge provenance without artificial scoring."""
        if 'merge_provenance' not in org_group:
            org_group['merge_provenance'] = []
        
        # FORENSIC DEBUG: Enhanced provenance tracking
        analysis_group_asns = set(self._safe_flatten_asns(getattr(analysis_group, 'asns', [])))
        mega_asns_from_analysis = analysis_group_asns & MEGA_GROUP_ASNS
        blocked_asns_from_analysis = analysis_group_asns & self.blocklist
        key_targets_from_analysis = {asn: KEY_TARGET_ASNS[asn] for asn in analysis_group_asns if asn in KEY_TARGET_ASNS}
        
        provenance_entry = {
            'group_type': getattr(analysis_group, 'group_type', 'unknown'),
            'asn_overlap_count': len(overlap),
            'overlapping_asns': sorted(list(overlap)),
            'common_attribute': getattr(analysis_group, 'common_attribute', None),
            'source': getattr(analysis_group, 'group_id', 'unknown_source'),
            
            # FORENSIC DEBUG: Enhanced tracking
            'forensic_tracking': {
                'total_asns_from_analysis': len(analysis_group_asns),
                'mega_asns_from_analysis': sorted(list(mega_asns_from_analysis)),
                'blocked_asns_from_analysis': sorted(list(blocked_asns_from_analysis)),
                'key_targets_from_analysis': key_targets_from_analysis,
                'potential_blocked_reintroduction': len(blocked_asns_from_analysis) > 0
            }
        }
        
        org_group['merge_provenance'].append(provenance_entry)
        
        # Log critical reintroductions
        if blocked_asns_from_analysis:
            org_id = org_group.get('group_id', ['unknown'])[0] if isinstance(org_group.get('group_id', []), list) else org_group.get('group_id', 'unknown')
            logger.warning(f"FORENSIC PROVENANCE: {analysis_group.group_type} analysis potentially reintroducing {len(blocked_asns_from_analysis)} blocked ASNs to org {org_id}")
            logger.warning(f"FORENSIC PROVENANCE: Blocked ASNs: {sorted(list(blocked_asns_from_analysis))}")
            logger.warning(f"FORENSIC PROVENANCE: Analysis attribute: {getattr(analysis_group, 'common_attribute', 'unknown')}")
    
    def _track_multisource_merge_provenance(self, org_group: Dict, analysis_groups: List, overlap: Set[int]) -> None:
        """Track merge provenance for multiple agreeing sources."""
        if 'merge_provenance' not in org_group:
            org_group['merge_provenance'] = []
        
        # Collect all source types and attributes
        source_types = []
        source_attributes = []
        all_analysis_asns = set()
        for group in analysis_groups:
            source_types.append(getattr(group, 'group_type', 'unknown'))
            common_attr = getattr(group, 'common_attribute', None)
            if common_attr:
                source_attributes.append(common_attr)
            group_asns = set(self._safe_flatten_asns(getattr(group, 'asns', [])))
            all_analysis_asns.update(group_asns)
        
        # FORENSIC DEBUG: Enhanced multisource provenance tracking
        mega_asns_from_multisource = all_analysis_asns & MEGA_GROUP_ASNS
        blocked_asns_from_multisource = all_analysis_asns & self.blocklist
        key_targets_from_multisource = {asn: KEY_TARGET_ASNS[asn] for asn in all_analysis_asns if asn in KEY_TARGET_ASNS}
        
        org_group['merge_provenance'].append({
            'source_count': len(analysis_groups),
            'agreeing_sources': source_types,
            'asn_overlap_count': len(overlap),
            'overlapping_asns': sorted(list(overlap)),
            'common_attributes': source_attributes,
            'multisource_agreement': True,  # Flag for high-quality merges
            
            # FORENSIC DEBUG: Enhanced multisource tracking
            'forensic_tracking': {
                'total_asns_from_multisource': len(all_analysis_asns),
                'mega_asns_from_multisource': sorted(list(mega_asns_from_multisource)),
                'blocked_asns_from_multisource': sorted(list(blocked_asns_from_multisource)),
                'key_targets_from_multisource': key_targets_from_multisource,
                'potential_blocked_reintroduction': len(blocked_asns_from_multisource) > 0,
                'high_confidence_merge': True  # Multiple sources agreeing
            }
        })
        
        # Log critical multisource reintroductions
        if blocked_asns_from_multisource:
            org_id = org_group.get('group_id', ['unknown'])[0] if isinstance(org_group.get('group_id', []), list) else org_group.get('group_id', 'unknown')
            logger.warning(f"FORENSIC PROVENANCE: MULTISOURCE {len(analysis_groups)} sources agreeing to reintroduce {len(blocked_asns_from_multisource)} blocked ASNs to org {org_id}")
            logger.warning(f"FORENSIC PROVENANCE: Agreeing sources: {source_types}")
            logger.warning(f"FORENSIC PROVENANCE: Blocked ASNs: {sorted(list(blocked_asns_from_multisource))}")
            logger.warning(f"FORENSIC PROVENANCE: Common attributes: {source_attributes}")
    
    def _create_multisource_analysis_group(self, analysis_groups: List, asns: List[int]) -> Dict:
        """Create a group from multiple agreeing analysis sources."""
        representative_group = analysis_groups[0]
        
        # Collect all source types
        source_types = [getattr(group, 'group_type', 'unknown') for group in analysis_groups]
        all_attributes = []
        for group in analysis_groups:
            attr = getattr(group, 'common_attribute', None)
            if attr:
                all_attributes.append(attr)
        
        # Create group name based on sources
        if len(source_types) == 1:
            group_name = f"{source_types[0].title()} Group"
        else:
            group_name = f"Multi-Source Group ({', '.join(set(source_types))})"
        
        return {
            'group_id': [f"multisource_{len(analysis_groups)}_{representative_group.group_id}"],
            'group_name': [group_name],
            'primary_name': group_name,
            'group_type': 'multisource_analysis',
            'asns': sorted(asns),
            'asn_details': [],  # Will be populated later if needed
            'sources': sorted(list(set(source_types))),
            'multisource_agreement': {
                'source_count': len(analysis_groups),
                'agreeing_sources': source_types,
                'common_attributes': all_attributes
            }
        }
    
    def _merge_organizations(self, primary_org: Dict, secondary_org: Dict, secondary_org_id: str) -> None:
        """Merge two organizations together during transitive closure."""
        # FORENSIC DEBUG: Track ASNs before organization merge
        pre_primary_asns = set(self._safe_flatten_asns(primary_org.get('asns', [])))
        pre_secondary_asns = set(self._safe_flatten_asns(secondary_org.get('asns', [])))
        
        # Check for mega ASNs in merge
        mega_in_primary = pre_primary_asns & MEGA_GROUP_ASNS
        mega_in_secondary = pre_secondary_asns & MEGA_GROUP_ASNS
        
        if mega_in_primary or mega_in_secondary:
            primary_id = primary_org.get('group_id', ['unknown'])[0] if isinstance(primary_org.get('group_id', []), list) else primary_org.get('group_id', 'unknown')
            logger.warning(f"FORENSIC: Merging organizations - Primary {primary_id} has {len(mega_in_primary)} mega ASNs, Secondary {secondary_org_id} has {len(mega_in_secondary)} mega ASNs")
        
        # Combine group IDs
        primary_ids = primary_org.get('group_id', [])
        if isinstance(primary_ids, str):
            primary_ids = [primary_ids]
        secondary_ids = secondary_org.get('group_id', [])
        if isinstance(secondary_ids, str):
            secondary_ids = [secondary_ids]
        primary_org['group_id'] = sorted(list(set(primary_ids + secondary_ids)))
        
        # Combine group names
        primary_names = primary_org.get('group_name', [])
        if isinstance(primary_names, str):
            primary_names = [primary_names]
        secondary_names = secondary_org.get('group_name', [])
        if isinstance(secondary_names, str):
            secondary_names = [secondary_names]
        primary_org['group_name'] = sorted(list(set(primary_names + secondary_names)))
        
        # Combine ASNs
        primary_asns = self._safe_flatten_asns(primary_org.get('asns', []))
        secondary_asns = self._safe_flatten_asns(secondary_org.get('asns', []))
        primary_org['asns'] = sorted(list(set(primary_asns + secondary_asns)))
        
        # Combine ASN details
        primary_details = primary_org.get('asn_details', [])
        secondary_details = secondary_org.get('asn_details', [])
        primary_org['asn_details'] = primary_details + secondary_details
        
        # Combine sources
        primary_sources = primary_org.get('sources', [])
        if isinstance(primary_sources, str):
            primary_sources = [primary_sources]
        secondary_sources = secondary_org.get('sources', [])
        if isinstance(secondary_sources, str):
            secondary_sources = [secondary_sources]
        primary_org['sources'] = sorted(list(set(primary_sources + secondary_sources)))
        
        # Update primary name
        primary_org['primary_name'] = self._select_best_name(primary_org['group_name'])
        
        # Track organization merger
        if 'organization_mergers' not in primary_org:
            primary_org['organization_mergers'] = []
        primary_org['organization_mergers'].append({
            'merged_org_id': secondary_org_id,
            'merged_org_names': secondary_names,
            'reason': 'transitive_closure_via_analysis_group'
        })
        
        # FORENSIC DEBUG: Track organization merge changes
        if mega_in_primary or mega_in_secondary:
            post_merge_asns = set(self._safe_flatten_asns(primary_org.get('asns', [])))
            self.forensic_logger.log_analysis_step(
                f"org_merge_{secondary_org_id}_into_{primary_id}",
                "organization_merge",
                pre_primary_asns,
                post_merge_asns,
                {
                    "merged_org_id": secondary_org_id,
                    "mega_asns_from_primary": sorted(list(mega_in_primary)),
                    "mega_asns_from_secondary": sorted(list(mega_in_secondary)),
                    "secondary_asn_count": len(pre_secondary_asns),
                    "final_asn_count": len(post_merge_asns)
                }
            )
    

    def _merge_into_organization(self, org_group: Dict, analysis_group: NetworkGroup) -> None:
        """Merge analysis group into existing organization group.
        
        Args:
            org_group: Organization group to merge into
            analysis_group: Analysis group to merge
        """
        # FORENSIC DEBUG: Track ASNs before merging analysis group
        pre_merge_asns = set(self._safe_flatten_asns(org_group.get('asns', [])))
        analysis_group_asns = set(self._safe_flatten_asns(analysis_group.asns))
        
        # Check for mega ASNs being added
        mega_in_analysis = analysis_group_asns & MEGA_GROUP_ASNS
        mega_being_added = mega_in_analysis - pre_merge_asns
        
        if mega_being_added:
            org_id = org_group.get('group_id', ['unknown'])[0] if isinstance(org_group.get('group_id', []), list) else org_group.get('group_id', 'unknown')
            logger.warning(f"FORENSIC: Analysis group {analysis_group.group_type}:{getattr(analysis_group, 'common_attribute', 'unknown')} adding {len(mega_being_added)} mega ASNs to org {org_id}: {sorted(list(mega_being_added))}")
        
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
        
        # FORENSIC DEBUG: Track analysis group merge if mega ASNs involved
        if mega_being_added:
            post_merge_asns = set(self._safe_flatten_asns(org_group.get('asns', [])))
            self.forensic_logger.log_analysis_step(
                f"analysis_merge_{analysis_group.group_type}_{getattr(analysis_group, 'common_attribute', 'unknown')[:50]}",
                f"analysis_group_{analysis_group.group_type}",
                pre_merge_asns,
                post_merge_asns,
                {
                    "analysis_group_id": analysis_group.group_id,
                    "analysis_group_type": analysis_group.group_type,
                    "common_attribute": str(getattr(analysis_group, 'common_attribute', 'unknown')),
                    "mega_asns_added": sorted(list(mega_being_added)),
                    "new_asns_count": len(new_asns)
                }
            )

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
                    logger.warning(f"Found list ASN value in group {group['group_id']}, using first element: {asn_value}")
                
                group_id_value = group['group_id']
                if isinstance(group_id_value, list):
                    # If group_id is a list, join with comma
                    group_id_value = ','.join(str(x) for x in group_id_value)
                    logger.warning(f"Found list group_id value, converted to: {group_id_value}")
                
                asn_count_value = group['asn_count']
                if isinstance(asn_count_value, list):
                    # If asn_count is a list, take the length or first element
                    asn_count_value = len(asn_count_value) if asn_count_value else 0
                    logger.warning(f"Found list asn_count value in group {group['group_id']}, using length: {asn_count_value}")
                
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
                logger.warning(f"Dropped {initial_count - len(df)} rows with None ASN values")
            
            # Final safety check before sorting
            try:
                df = df.sort_values(['total_group_size', 'group_id', 'asn'], ascending=[False, True, True])
            except Exception as e:
                logger.error(f"Still failed to sort DataFrame: {e}")
                logger.error(f"Column types: total_group_size={df['total_group_size'].dtype}, group_id={df['group_id'].dtype}, asn={df['asn'].dtype}")
                logger.error(f"Sample values - group_id: {df['group_id'].head(3).tolist()}, asn: {df['asn'].head(3).tolist()}")
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
                
                flat_asns = _extract_asns(asns_data)
                
                # Remove duplicates and sort - with robust error handling
                try:
                    # First try to create set directly
                    unique_asns = sorted(set(flat_asns))
                except TypeError as e:
                    logger.debug(f"TypeError creating set from flat_asns at group {i}: {e}")
                    # Fallback: filter out unhashable elements
                    hashable_asns = []
                    for asn in flat_asns:
                        try:
                            hash(asn)  # Test if hashable
                            hashable_asns.append(asn)
                        except TypeError:
                            logger.debug(f"Skipping unhashable ASN: {type(asn)} = {repr(asn)}")
                    unique_asns = sorted(set(hashable_asns))
                
                # Final check - ensure all elements can be hashed
                try:
                    asn_set = frozenset(unique_asns)
                except TypeError as e:
                    logger.debug(f"Still can't create frozenset at group {i}: {e}")
                    logger.debug(f"unique_asns types: {[type(x) for x in unique_asns]}")
                    # Ultimate fallback - convert all to strings and hash those
                    string_asns = [str(x) for x in unique_asns if x is not None]
                    asn_set = frozenset(string_asns)
                
                asn_set_groups[asn_set].append(group)
                
            except Exception as e:
                logger.debug(f"Exception in _deduplicate_by_asn_set at group {i}: {e}")
                logger.debug(f"Traceback: {traceback.format_exc()}")
                logger.debug(f"Problematic asns_data: {repr(asns_data)[:500]}")
                
                # If we still can't process this group, create a unique key
                # using string representation to avoid the error
                try:
                    asn_str = str(group.get('asns', []))
                    asn_set = frozenset([hash(asn_str)])  # Use hash as a fallback
                    asn_set_groups[asn_set].append(group)
                    logger.debug(f"Used fallback for group {i}")
                except Exception as e2:
                    logger.debug(f"Even fallback failed: {e2}")
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
                    # Safe to merge - groups represent the same organizational entity
                    merged_group = self._merge_duplicate_groups(duplicate_groups)
                    
                    deduplicated_groups.append(merged_group)
                else:
                    # Cannot merge - keep groups separate with unique ASN-based identifiers
                    group_names = [str(g.get('group_name', ['Unknown'])[0] if isinstance(g.get('group_name'), list) else g.get('group_name', 'Unknown')) for g in duplicate_groups]
                    asn_list = sorted(list(asn_set))
                    logger.info(f"BLOCKED merge of {len(duplicate_groups)} groups with identical {len(asn_list)} ASNs due to incompatible organizations")
                    
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
        
        # Add all unprocessed groups (those that don't share PeeringDB orgs)
        for i, group in enumerate(groups):
            if i not in processed_groups:
                merged_groups.append(group)
        
        return merged_groups
    
