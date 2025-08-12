"""Data export utilities for various formats."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from ..models import ASNetworkReport


class DataExporter:
    """Base class for data exporters."""
    
    def __init__(self, output_dir: Union[str, Path]):
        """Initialize exporter.
        
        Args:
            output_dir: Output directory for exported files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def get_filename(self, base_name: str, extension: str) -> Path:
        """Generate filename with timestamp.
        
        Args:
            base_name: Base name for file
            extension: File extension
            
        Returns:
            Full file path
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{base_name}_{timestamp}.{extension}"
        return self.output_dir / filename


class ParquetExporter(DataExporter):
    """Export data to Parquet format."""
    
    def export_dataframe(self, df: pd.DataFrame, name: str) -> Path:
        """Export DataFrame to Parquet.
        
        Args:
            df: DataFrame to export
            name: Base name for file
            
        Returns:
            Path to exported file
        """
        file_path = self.get_filename(name, "parquet")
        df.to_parquet(file_path, index=False)
        return file_path
    
    def export_multiple(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, Path]:
        """Export multiple DataFrames.
        
        Args:
            dataframes: Dictionary of name -> DataFrame
            
        Returns:
            Dictionary of name -> file path
        """
        paths = {}
        for name, df in dataframes.items():
            if not df.empty:
                paths[name] = self.export_dataframe(df, name)
        return paths


class JSONExporter(DataExporter):
    """Export data to JSON format."""
    
    def export_dataframe(self, df: pd.DataFrame, name: str, orient: str = "records") -> Path:
        """Export DataFrame to JSON.
        
        Args:
            df: DataFrame to export
            name: Base name for file
            orient: Pandas JSON orientation
            
        Returns:
            Path to exported file
        """
        file_path = self.get_filename(name, "json")
        df.to_json(file_path, orient=orient, indent=2)
        return file_path
    
    def export_report(self, report: ASNetworkReport) -> Path:
        """Export full network report to JSON.
        
        Args:
            report: Network analysis report
            
        Returns:
            Path to exported file
        """
        file_path = self.get_filename("network_report", "json")
        
        # Convert report to dictionary
        report_dict = report.model_dump(mode="json")
        
        # Write with pretty formatting
        with open(file_path, "w") as f:
            json.dump(report_dict, f, indent=2, default=str)
        
        return file_path
    
    def export_dict(self, data: Dict[str, Any], name: str) -> Path:
        """Export dictionary to JSON.
        
        Args:
            data: Dictionary to export
            name: Base name for file
            
        Returns:
            Path to exported file
        """
        file_path = self.get_filename(name, "json")
        
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        
        return file_path


class CSVExporter(DataExporter):
    """Export data to CSV format."""
    
    def export_dataframe(self, df: pd.DataFrame, name: str) -> Path:
        """Export DataFrame to CSV.
        
        Args:
            df: DataFrame to export
            name: Base name for file
            
        Returns:
            Path to exported file
        """
        file_path = self.get_filename(name, "csv")
        
        # Handle list columns by converting to string
        df_export = df.copy()
        for col in df_export.columns:
            if df_export[col].dtype == object:
                # Check if column contains lists
                if any(isinstance(val, list) for val in df_export[col].dropna().head()):
                    df_export[col] = df_export[col].apply(
                        lambda x: ",".join(map(str, x)) if isinstance(x, list) else x
                    )
        
        df_export.to_csv(file_path, index=False)
        return file_path
    
    def export_multiple(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, Path]:
        """Export multiple DataFrames to CSV.
        
        Args:
            dataframes: Dictionary of name -> DataFrame
            
        Returns:
            Dictionary of name -> file path
        """
        paths = {}
        for name, df in dataframes.items():
            if not df.empty:
                paths[name] = self.export_dataframe(df, name)
        return paths


class ReportExporter:
    """Export comprehensive network analysis reports."""
    
    def __init__(self, output_dir: Union[str, Path], default_format: str = "parquet"):
        """Initialize report exporter.
        
        Args:
            output_dir: Output directory
            default_format: Default export format
        """
        self.output_dir = Path(output_dir)
        self.default_format = default_format
        
        # Initialize format-specific exporters
        self.exporters = {
            "parquet": ParquetExporter(output_dir),
            "json": JSONExporter(output_dir),
            "csv": CSVExporter(output_dir)
        }
    
    def export_dataframes(
        self, 
        dataframes: Dict[str, pd.DataFrame], 
        formats: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, Path]]:
        """Export DataFrames in multiple formats.
        
        Args:
            dataframes: Dictionary of DataFrames to export
            formats: List of formats to export (uses default if None)
            
        Returns:
            Nested dictionary of format -> name -> file path
        """
        if formats is None:
            formats = [self.default_format]
        
        results = {}
        
        for format_name in formats:
            if format_name in self.exporters:
                exporter = self.exporters[format_name]
                if format_name == "parquet":
                    results[format_name] = exporter.export_multiple(dataframes)
                elif format_name == "csv":
                    results[format_name] = exporter.export_multiple(dataframes)
                elif format_name == "json":
                    results[format_name] = {}
                    for name, df in dataframes.items():
                        if not df.empty:
                            results[format_name][name] = exporter.export_dataframe(df, name)
        
        return results
    
    def export_full_report(
        self, 
        report: ASNetworkReport,
        dataframes: Dict[str, pd.DataFrame],
        formats: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Export full analysis report with all data.
        
        Args:
            report: Network analysis report
            dataframes: Dictionary of DataFrames
            formats: Export formats
            
        Returns:
            Dictionary with export paths and metadata
        """
        if formats is None:
            formats = ["parquet", "json"]
        
        result = {
            "report_id": report.report_id,
            "generated_at": report.generated_at.isoformat(),
            "exports": {}
        }
        
        # Export dataframes
        df_exports = self.export_dataframes(dataframes, formats)
        result["exports"]["dataframes"] = df_exports
        
        # Export full report as JSON
        if "json" in formats:
            json_exporter = self.exporters["json"]
            report_path = json_exporter.export_report(report)
            result["exports"]["full_report"] = str(report_path)
        
        # Create summary file
        summary_path = self._create_summary(report, result)
        result["exports"]["summary"] = str(summary_path)
        
        return result
    
    def _create_summary(self, report: ASNetworkReport, export_result: Dict[str, Any]) -> Path:
        """Create a summary file with export information.
        
        Args:
            report: Network analysis report
            export_result: Export results
            
        Returns:
            Path to summary file
        """
        summary = {
            "report_id": report.report_id,
            "generated_at": report.generated_at.isoformat(),
            "statistics": {
                "total_asns": report.total_asns,
                "total_organizations": report.total_organizations,
                "total_relationships": len(report.as_relationships),
                "total_groups": len(report.network_groups),
                "total_favicon_analyses": len(report.favicon_analyses)
            },
            "api_usage": {
                "total_requests": report.api_usage.total_requests if report.api_usage else 0,
                "total_input_tokens": report.api_usage.total_input_tokens if report.api_usage else 0,
                "total_output_tokens": report.api_usage.total_output_tokens if report.api_usage else 0,
                "estimated_cost_usd": report.api_usage.estimated_cost_usd if report.api_usage else 0.0
            },
            "pipeline_results": [
                {
                    "stage": result.stage_name,
                    "status": result.status,
                    "records_processed": result.records_processed,
                    "duration_seconds": result.duration_seconds
                }
                for result in report.pipeline_results
            ],
            "exported_files": export_result["exports"]
        }
        
        json_exporter = self.exporters["json"]
        return json_exporter.export_dict(summary, "export_summary")