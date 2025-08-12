"""Pydantic schemas for data validation and serialization."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl, validator


class AutonomousSystem(BaseModel):
    """Autonomous System (AS) information."""
    
    asn: int = Field(..., description="Autonomous System Number", ge=1)
    org_id: Optional[str] = Field(None, description="Organization ID")
    name: Optional[str] = Field(None, description="AS name")
    website: Optional[HttpUrl] = Field(None, description="AS website URL")
    notes: Optional[str] = Field(None, description="PeeringDB notes")
    aka: Optional[str] = Field(None, description="Also known as")
    
    class Config:
        """Pydantic config."""
        
        json_schema_extra = {
            "example": {
                "asn": 13335,
                "org_id": "ORG-1234",
                "name": "Example AS",
                "website": "https://example.com",
                "notes": "Example network",
                "aka": "Example aka"
            }
        }


class Organization(BaseModel):
    """Organization information."""
    
    org_id: str = Field(..., description="Organization ID")
    name: Optional[str] = Field(None, description="Organization name")
    asns: List[int] = Field(default_factory=list, description="List of ASNs")
    
    @validator("asns")
    def validate_asns(cls, v):
        """Ensure ASNs are unique."""
        return list(set(v))


class WebsiteInfo(BaseModel):
    """Website information from scraping."""
    
    original_url: HttpUrl = Field(..., description="Original URL")
    final_url: Optional[HttpUrl] = Field(None, description="Final URL after redirects")
    redirects: List[HttpUrl] = Field(default_factory=list, description="Redirect chain")
    domain: Optional[str] = Field(None, description="Extracted domain (FQDN)")
    html_content: Optional[str] = Field(None, description="Raw HTML content")
    favicon_url: Optional[HttpUrl] = Field(None, description="Favicon URL")
    favicon_data: Optional[bytes] = Field(None, description="Favicon binary data")
    scrape_timestamp: datetime = Field(default_factory=datetime.utcnow)
    error: Optional[str] = Field(None, description="Error message if scraping failed")
    is_blocked_domain: bool = Field(False, description="Whether domain is in blocklist")
    blocked_reason: Optional[str] = Field(None, description="Reason domain was blocked")
    
    class Config:
        """Pydantic config."""
        
        arbitrary_types_allowed = True
        json_encoders = {
            bytes: lambda v: None  # Don't serialize bytes in JSON
        }


class ASRelationship(BaseModel):
    """Relationship between Autonomous Systems."""
    
    source_asn: int = Field(..., description="Source AS number")
    related_asns: List[int] = Field(default_factory=list, description="Related AS numbers")
    relationship_type: str = Field(..., description="Type of relationship")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score")
    evidence: Optional[str] = Field(None, description="Evidence for relationship")
    detected_by: str = Field(..., description="Detection method")
    
    @validator("related_asns")
    def validate_related_asns(cls, v, values):
        """Ensure related ASNs don't include source ASN."""
        source = values.get("source_asn")
        if source:
            return [asn for asn in v if asn != source]
        return v


class FaviconAnalysis(BaseModel):
    """Favicon analysis results."""
    
    favicon_hash: str = Field(..., description="Hash of favicon data")
    urls: List[HttpUrl] = Field(..., description="URLs using this favicon")
    company_name: Optional[str] = Field(None, description="Detected company name")
    is_telecom: bool = Field(False, description="Is telecom company")
    is_hosting: bool = Field(False, description="Is hosting provider")
    confidence: float = Field(0.0, ge=0.0, le=1.0, description="Analysis confidence")
    llm_response: Optional[str] = Field(None, description="Raw LLM response")
    
    class Config:
        """Pydantic config."""
        
        json_schema_extra = {
            "example": {
                "favicon_hash": "abc123",
                "urls": ["https://example.com"],
                "company_name": "Example Corp",
                "is_telecom": True,
                "is_hosting": False,
                "confidence": 0.95,
                "llm_response": "Example Corp"
            }
        }


class NetworkGroup(BaseModel):
    """Group of related networks."""
    
    group_id: str = Field(..., description="Group identifier")
    group_type: str = Field(..., description="Type of grouping")
    asns: List[int] = Field(..., description="ASNs in group")
    common_attribute: str = Field(..., description="Common attribute")
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @validator("asns")
    def validate_asns(cls, v):
        """Ensure ASNs are unique and sorted."""
        return sorted(set(v))


class PipelineResult(BaseModel):
    """Result from a pipeline stage."""
    
    stage_name: str = Field(..., description="Pipeline stage name")
    status: str = Field(..., description="Status: success, partial, failed")
    records_processed: int = Field(0, ge=0)
    records_failed: int = Field(0, ge=0)
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    errors: List[str] = Field(default_factory=list)
    output_files: List[str] = Field(default_factory=list)
    
    @validator("duration_seconds", always=True)
    def calculate_duration(cls, v, values):
        """Calculate duration from start and end times."""
        if v is not None:
            return v
        start = values.get("start_time")
        end = values.get("end_time")
        if start and end:
            return (end - start).total_seconds()
        return None


class APIUsageStats(BaseModel):
    """API usage and cost statistics."""
    
    total_requests: int = Field(0, description="Total API requests made")
    total_input_tokens: int = Field(0, description="Total input tokens")
    total_output_tokens: int = Field(0, description="Total output tokens")
    estimated_cost_usd: float = Field(0.0, description="Estimated cost in USD")
    
    class Config:
        """Pydantic config."""
        
        json_schema_extra = {
            "example": {
                "total_requests": 100,
                "total_input_tokens": 50000,
                "total_output_tokens": 10000,
                "estimated_cost_usd": 2.50
            }
        }


class ASNetworkReport(BaseModel):
    """Complete AS network analysis report."""
    
    report_id: str = Field(..., description="Unique report ID")
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    total_asns: int = Field(..., ge=0)
    total_organizations: int = Field(..., ge=0)
    
    # Analysis results
    as_relationships: List[ASRelationship] = Field(default_factory=list)
    network_groups: List[NetworkGroup] = Field(default_factory=list)
    favicon_analyses: List[FaviconAnalysis] = Field(default_factory=list)
    
    # API usage and costs
    api_usage: Optional[APIUsageStats] = Field(None, description="API usage statistics")
    
    # Pipeline metadata
    pipeline_results: List[PipelineResult] = Field(default_factory=list)
    configuration: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        """Pydantic config."""
        
        json_schema_extra = {
            "example": {
                "report_id": "report-123",
                "generated_at": "2024-01-01T00:00:00Z",
                "total_asns": 1000,
                "total_organizations": 500,
                "as_relationships": [],
                "network_groups": [],
                "favicon_analyses": [],
                "pipeline_results": []
            }
        }