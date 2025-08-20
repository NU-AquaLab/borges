"""Data loading utilities for various file formats."""

import json
import pickle
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from ..models import ASNetwork, AutonomousSystem


class PeeringDBLoader:
    """Load and parse PeeringDB data."""
    
    def __init__(self, file_path: Union[str, Path]):
        """Initialize PeeringDB loader.
        
        Args:
            file_path: Path to PeeringDB JSON dump file
        """
        self.file_path = Path(file_path)
        if not self.file_path.exists():
            raise FileNotFoundError(f"PeeringDB file not found: {file_path}")
    
    def load_raw(self) -> Dict[str, Any]:
        """Load raw PeeringDB data.
        
        Returns:
            Dictionary containing PeeringDB data
        """
        with open(self.file_path, "r") as f:
            return json.load(f)
    
    def load_networks(self) -> pd.DataFrame:
        """Load network data from PeeringDB.
        
        Returns:
            DataFrame with network information
        """
        from ..config import get_config
        
        data = self.load_raw()
        df = pd.DataFrame(data["net"]["data"])
        
        # Select relevant columns
        columns = ["asn", "org_id", "name", "website", "notes", "aka", "info_traffic", 
                   "info_ratio", "info_scope", "info_type", "info_prefixes4", "info_prefixes6"]
        
        # Keep only columns that exist
        columns = [col for col in columns if col in df.columns]
        df = df[columns]
        
        # Filter content from blocked ASNs to prevent bridge creation
        try:
            config = get_config()
            content_blocklist = set(config.processing.asn_blocklist)
            exclusion_list = set(config.processing.peeringdb_asn_exclusions)
            
            # FORENSIC FIX: Completely remove excluded ASNs from PeeringDB dataset
            # This mechanism was added Aug 17, 2025 to solve AS4004 dual-identity problem
            # AS4004 appears in Orange PeeringDB org but Sprint WHOIS org, creating false bridge
            # Complete exclusion prevents it from participating in any PeeringDB-based grouping
            if exclusion_list and not df.empty:
                excluded_mask = df["asn"].isin(exclusion_list)
                excluded_count = excluded_mask.sum()
                if excluded_count > 0:
                    print(f"Completely excluded {excluded_count} ASNs from PeeringDB dataset to prevent organization bridges")
                df = df[~excluded_mask]
            
            # Nullify content for blocked ASNs (preserves org structure)
            if content_blocklist and not df.empty:
                blocked_mask = df["asn"].isin(content_blocklist)
                
                # Nullify content fields for blocked ASNs (preserves org structure)
                if "notes" in df.columns:
                    df.loc[blocked_mask, "notes"] = None
                if "aka" in df.columns:
                    df.loc[blocked_mask, "aka"] = None
                if "website" in df.columns:
                    df.loc[blocked_mask, "website"] = None
                    
                blocked_count = blocked_mask.sum()
                if blocked_count > 0:
                    print(f"Filtered content from {blocked_count} blocked ASNs to prevent bridge creation")
        except Exception as e:
            # Don't fail loading if blocklist processing fails
            print(f"Warning: Could not apply content filtering: {e}")
        
        # Clean website URLs (for non-blocked ASNs)
        if "website" in df.columns:
            df["website"] = df["website"].str.strip()
            df.loc[df["website"] == "", "website"] = None
        
        return df
    
    def load_to_as_network(self, as_network: Optional[ASNetwork] = None) -> ASNetwork:
        """Load PeeringDB data into AS network structure.
        
        Args:
            as_network: Existing AS network to add to (creates new if None)
            
        Returns:
            AS network with loaded data
        """
        if as_network is None:
            as_network = ASNetwork()
        
        df = self.load_networks()
        
        for _, row in df.iterrows():
            as_info = AutonomousSystem(
                asn=row["asn"],
                org_id=str(row["org_id"]) if pd.notna(row.get("org_id")) else None,
                name=row.get("name"),
                website=row.get("website") if pd.notna(row.get("website")) else None,
                notes=row.get("notes") if pd.notna(row.get("notes")) else None,
                aka=row.get("aka") if pd.notna(row.get("aka")) else None
            )
            as_network.add_as(as_info)
            
            # Store original PeeringDB organization mapping
            if pd.notna(row.get("org_id")):
                peeringdb_org_id = f"peeringdb_{row['org_id']}"
                as_network.as_to_peeringdb_org[row["asn"]] = peeringdb_org_id
                as_network.peeringdb_org_to_as[peeringdb_org_id].add(row["asn"])
        
        return as_network


class WHOISLoader:
    """Load and parse WHOIS AS2Org data."""
    
    def __init__(self, file_path: Union[str, Path]):
        """Initialize WHOIS loader.
        
        Args:
            file_path: Path to WHOIS AS2Org data file
        """
        self.file_path = Path(file_path)
        if not self.file_path.exists():
            raise FileNotFoundError(f"WHOIS file not found: {file_path}")
    
    def parse_as2org_file(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Parse AS2Org file with two sections.
        
        The AS2Org files contain two different types of entries:
        - AS numbers and their organization mappings
        - Organization details
        
        The two data types are divided by lines that start with '# format:'
        
        Returns:
            Tuple of (as_df, org_df) DataFrames
        """
        with open(self.file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        
        # Find the section dividers
        as_section_start = None
        org_section_start = None
        
        for i, line in enumerate(lines):
            if line.startswith("# format:") and "org_id|changed|org_name|country|source" in line:
                org_section_start = i + 1
            elif line.startswith("# format:") and "aut|changed|aut_name|org_id|opaque_id|source" in line:
                as_section_start = i + 1
        
        if as_section_start is None or org_section_start is None:
            raise ValueError("Could not find section headers in AS2Org file")
        
        # Parse organization section (comes first)
        org_lines = lines[org_section_start:as_section_start - 1]
        org_data = []
        for line in org_lines:
            if line.strip() and not line.startswith("#"):
                parts = line.strip().split("|")
                if len(parts) >= 5:
                    org_data.append({
                        "org_id": parts[0],
                        "changed": parts[1],
                        "org_name": parts[2],
                        "country": parts[3],
                        "source": parts[4]
                    })
        
        # Parse AS section (comes second)
        as_lines = lines[as_section_start:]
        as_data = []
        for line in as_lines:
            if line.strip() and not line.startswith("#"):
                parts = line.strip().split("|")
                if len(parts) >= 6:
                    as_data.append({
                        "asn": parts[0],
                        "changed": parts[1],
                        "aut_name": parts[2],
                        "org_id": parts[3],
                        "opaque_id": parts[4],
                        "source": parts[5]
                    })
        
        as_df = pd.DataFrame(as_data)
        org_df = pd.DataFrame(org_data)
        
        # Convert ASN to integer
        if not as_df.empty and "asn" in as_df.columns:
            as_df["asn"] = pd.to_numeric(as_df["asn"], errors="coerce")
            as_df = as_df[as_df["asn"].notna()]
            as_df["asn"] = as_df["asn"].astype(int)
        
        return as_df, org_df
    
    def load(self, skiprows: int = 0) -> pd.DataFrame:
        """Load WHOIS AS2Org data.
        
        Args:
            skiprows: Ignored for compatibility
            
        Returns:
            DataFrame with AS to organization mappings
        """
        as_df, org_df = self.parse_as2org_file()
        
        # Merge AS and organization data
        if not as_df.empty and not org_df.empty:
            merged_df = as_df.merge(org_df, on="org_id", how="left", suffixes=("_as", "_org"))
            # Rename for backward compatibility
            merged_df = merged_df.rename(columns={"asn": "ASN"})
            return merged_df
        elif not as_df.empty:
            # Return just AS data if org data is empty
            return as_df.rename(columns={"asn": "ASN"})
        else:
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=["ASN", "org_id"])
    
    def load_to_as_network(self, as_network: Optional[ASNetwork] = None) -> ASNetwork:
        """Load WHOIS AS2Org data into AS network structure.
        
        Args:
            as_network: Existing AS network to add to (creates new if None)
            
        Returns:
            AS network with loaded data
        """
        if as_network is None:
            as_network = ASNetwork()
        
        as_df, org_df = self.parse_as2org_file()
        
        # Process AS to organization mappings
        if not as_df.empty and "asn" in as_df.columns and "org_id" in as_df.columns:
            for _, row in as_df.iterrows():
                asn = row["asn"]
                org_id = str(row["org_id"])
                
                if pd.notna(asn) and pd.notna(org_id):
                    as_network.as_to_org[int(asn)] = org_id
                    as_network.org_to_as[org_id].add(int(asn))
        
        # Add organization details from WHOIS data
        if not org_df.empty:
            from ..models import Organization
            for _, row in org_df.iterrows():
                org_id = str(row["org_id"])
                # Get ASNs for this organization
                asns = list(as_network.org_to_as.get(org_id, set()))
                if asns:  # Only create organizations that have ASNs
                    as_network.organizations[org_id] = Organization(
                        org_id=org_id,
                        name=row.get("org_name", ""),
                        country=row.get("country", ""),
                        source=row.get("source", ""),
                        asns=sorted(asns)
                    )
        
        return as_network
    
    def add_whois_only_asns(self, as_network: ASNetwork) -> int:
        """Add ASNs that exist in WHOIS but not in PeeringDB.
        
        Args:
            as_network: AS network to add WHOIS-only ASNs to
            
        Returns:
            Number of WHOIS-only ASNs added
        """
        as_df, org_df = self.parse_as2org_file()
        added_count = 0
        
        if not as_df.empty:
            from ..models import AutonomousSystem
            for _, row in as_df.iterrows():
                asn = int(row["asn"])
                if asn not in as_network.autonomous_systems:
                    # This ASN is in WHOIS but not in PeeringDB, add it
                    as_info = AutonomousSystem(
                        asn=asn,
                        org_id=str(row["org_id"]) if pd.notna(row.get("org_id")) else None,
                        name=row.get("aut_name", f"AS{asn}"),
                        website=None,  # WHOIS doesn't have website info
                        notes=f"WHOIS-only ASN (not in PeeringDB)",
                        aka=None
                    )
                    as_network.add_as(as_info)
                    added_count += 1
        
        return added_count


class PickleLoader:
    """Load data from pickle files."""
    
    @staticmethod
    def load(file_path: Union[str, Path]) -> Any:
        """Load object from pickle file.
        
        Args:
            file_path: Path to pickle file
            
        Returns:
            Unpickled object
        """
        with open(file_path, "rb") as f:
            return pickle.load(f)
    
    @staticmethod
    def load_html_results(directory: Union[str, Path]) -> List[tuple]:
        """Load HTML scraping results from pickle files.
        
        Args:
            directory: Directory containing pickle files
            
        Returns:
            List of (url, redirects, final_url, html) tuples
        """
        directory = Path(directory)
        results = []
        
        for file_path in directory.glob("http*"):
            try:
                data = PickleLoader.load(file_path)
                if isinstance(data, tuple) and len(data) == 4:
                    results.append(data)
            except Exception:
                # Skip corrupted files
                continue
        
        return results


class ParquetLoader:
    """Load data from Parquet files."""
    
    @staticmethod
    def load(file_path: Union[str, Path]) -> pd.DataFrame:
        """Load DataFrame from Parquet file.
        
        Args:
            file_path: Path to Parquet file
            
        Returns:
            Loaded DataFrame
        """
        return pd.read_parquet(file_path)
    
    @staticmethod
    def load_multiple(pattern: str, directory: Union[str, Path] = ".") -> pd.DataFrame:
        """Load and concatenate multiple Parquet files.
        
        Args:
            pattern: Glob pattern for files
            directory: Directory to search in
            
        Returns:
            Concatenated DataFrame
        """
        directory = Path(directory)
        files = list(directory.glob(pattern))
        
        if not files:
            raise FileNotFoundError(f"No files matching pattern: {pattern}")
        
        dfs = [pd.read_parquet(f) for f in files]
        return pd.concat(dfs, ignore_index=True)


class FeatherLoader:
    """Load data from Feather files (for backward compatibility)."""
    
    @staticmethod
    def load(file_path: Union[str, Path]) -> pd.DataFrame:
        """Load DataFrame from Feather file.
        
        Args:
            file_path: Path to Feather file
            
        Returns:
            Loaded DataFrame
        """
        return pd.read_feather(file_path)


class HDFLoader:
    """Load data from HDF5 files (for backward compatibility)."""
    
    @staticmethod
    def load(file_path: Union[str, Path], key: str = "data") -> pd.DataFrame:
        """Load DataFrame from HDF5 file.
        
        Args:
            file_path: Path to HDF5 file
            key: HDF5 key to read
            
        Returns:
            Loaded DataFrame
        """
        return pd.read_hdf(file_path, key=key)