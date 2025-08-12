"""LLM-based analysis for sibling relationships and favicon identification."""

import base64
import time
from typing import Dict, List, Optional

import pandas as pd
from langchain_core.messages import HumanMessage
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from pydantic import BaseModel, Field
from langchain_openai import ChatOpenAI
from PIL import Image
from io import BytesIO

from ..config import get_config
from ..data.processors import ASNProcessor
from ..models import APIUsageStats, ASRelationship, FaviconAnalysis, NetworkGroup
from .number_validator import validate_llm_output


class ASList(BaseModel):
    """Model for LLM AS detection output."""
    ASs: Optional[List[int]] = Field(None, description="List of AS numbers")


class ASRelationshipAnalyzer:
    """Analyze AS relationships using LLM."""

    def __init__(self):
        """Initialize AS relationship analyzer."""
        config = get_config()
        self.config = config.api.openai
        self.prompt_template = config.processing.prompts.get("as_detection")
        self.asn_blocklist = set(config.processing.asn_blocklist)

        # Initialize LLM with rate limiting
        from ..utils.llm_client import create_llm_client
        self.llm_client = create_llm_client()
        self.llm = self.llm_client.llm

        # Initialize parser
        self.parser = JsonOutputParser(pydantic_object=ASList)

        # Create prompt
        self.prompt = PromptTemplate(
            template=self.prompt_template,
            input_variables=["aka", "notes", "asn"],
            partial_variables={"format_instructions": self.parser.get_format_instructions()},
        )

        # Create chain
        self.chain = self.prompt | self.llm | self.parser
        
        # Initialize cost tracking
        self.api_usage = APIUsageStats()

    def analyze_as(self, asn: int, notes: str, aka: str) -> ASRelationship:
        """Analyze AS relationships from notes and AKA fields.

        Args:
            asn: Source AS number
            notes: PeeringDB notes
            aka: AKA field

        Returns:
            AS relationship
        """
        # Skip analysis for blocklisted ASNs to prevent upstream pollution
        if asn in self.asn_blocklist:
            return ASRelationship(
                source_asn=asn,
                related_asns=[],
                relationship_type="sibling",
                confidence=0.0,
                sources=[],
                detected_by="llm_analysis"
            )
        
        try:
            # Run LLM analysis with proper rate limiting
            prompt_input = {
                "asn": asn,
                "notes": notes or "",
                "aka": aka or ""
            }
            
            # Format the prompt
            formatted_prompt = self.prompt.format(**prompt_input)
            messages = [{"role": "user", "content": formatted_prompt}]
            
            # Use LLM client with rate limiting
            response = self.llm_client.invoke(messages)
            
            # Parse the response
            result = self.parser.parse(response.content)
            
            # Track API usage (estimate tokens and cost) 
            self._track_api_usage(notes or "", aka or "")

            # Extract ASNs from LLM
            llm_asns = result.get("ASs", []) if result else []

            # Validate LLM output - filter out hallucinated ASNs
            input_text = f"{notes or ''} {aka or ''}".strip()
            validated_llm_asns = validate_llm_output(input_text, llm_asns, source_asn=asn, blocklist=self.asn_blocklist)

            # Also extract ASNs using regex (keep existing functionality)
            text_asns = []
            if notes:
                text_asns.extend(ASNProcessor.detect_related_asns(notes, asn))
            if aka:
                text_asns.extend(ASNProcessor.detect_related_asns(aka, asn))

            # Combine validated LLM results with regex results
            all_asns = list(set(validated_llm_asns + text_asns))
            all_asns = [asn_num for asn_num in all_asns if asn_num != asn]
            
            # Filter out blocklisted ASNs from related_asns to prevent pollution
            all_asns = [asn_num for asn_num in all_asns if asn_num not in self.asn_blocklist]

            # Calculate dynamic confidence score
            confidence = self._calculate_confidence(
                llm_asns=validated_llm_asns,
                text_asns=text_asns,
                notes=notes,
                aka=aka
            )

            return ASRelationship(
                source_asn=asn,
                related_asns=all_asns,
                relationship_type="organization_related",
                confidence=confidence,
                evidence=f"Notes: {notes}" if notes else f"AKA: {aka}" if aka else None,
                detected_by="llm_analysis"
            )

        except Exception as e:
            # Return empty relationship on error
            return ASRelationship(
                source_asn=asn,
                related_asns=[],
                relationship_type="organization_related",
                confidence=0.0,
                error=str(e),
                detected_by="llm_analysis"
            )

    def analyze_dataframe(self, df: pd.DataFrame) -> List[ASRelationship]:
        """Analyze AS relationships from a DataFrame.

        Args:
            df: DataFrame with asn, notes, and aka columns

        Returns:
            List of AS relationships
        """
        relationships = []

        # Filter to rows that might have AS references
        filtered_df = df[
            (df["notes"].notna() & df["notes"].apply(ASNProcessor.has_asn_reference)) |
            (df["aka"].notna() & df["aka"].apply(ASNProcessor.has_asn_reference))
        ]

        for _, row in filtered_df.iterrows():
            relationship = self.analyze_as(
                asn=row["asn"],
                notes=row.get("notes", ""),
                aka=row.get("aka", "")
            )
            if relationship.related_asns:
                relationships.append(relationship)

        return relationships

    def _calculate_confidence(
        self, 
        llm_asns: List[int], 
        text_asns: List[int], 
        notes: str, 
        aka: str
    ) -> float:
        """Calculate confidence score for AS relationship detection.
        
        Args:
            llm_asns: ASNs found by LLM analysis
            text_asns: ASNs found by text processing
            notes: Notes field content
            aka: AKA field content
            
        Returns:
            Confidence score between 0.0 and 1.0
        """
        if not llm_asns and not text_asns:
            return 0.0
        
        confidence = 0.0
        
        # Base confidence for finding any ASNs
        if llm_asns or text_asns:
            confidence += 0.3
        
        # Higher confidence if both LLM and text processing agree
        if llm_asns and text_asns:
            overlap = set(llm_asns) & set(text_asns)
            if overlap:
                confidence += 0.4  # Strong agreement
            else:
                confidence += 0.2  # Both found ASNs, but different ones
        
        # Confidence boost based on evidence quality
        evidence_text = (notes or "") + " " + (aka or "")
        evidence_length = len(evidence_text.strip())
        
        if evidence_length > 100:
            confidence += 0.2  # Rich evidence
        elif evidence_length > 20:
            confidence += 0.1  # Some evidence
        
        # Confidence based on number of ASNs found
        total_asns = len(set(llm_asns + text_asns))
        if total_asns == 1:
            confidence += 0.1  # Single ASN is more reliable
        elif total_asns <= 3:
            confidence += 0.05  # Small group is reasonable
        # No bonus for large groups (might be noisy)
        
        # Evidence source preference (notes are more reliable than aka)
        if notes and llm_asns:
            confidence += 0.05
        
        return min(confidence, 1.0)  # Cap at 1.0

    def _track_api_usage(self, notes: str, aka: str) -> None:
        """Track API usage for cost estimation.
        
        Args:
            notes: Notes text processed
            aka: AKA text processed
        """
        # Rough token estimation (1 token ≈ 4 characters for English text)
        input_text = f"{self.prompt_template} {notes} {aka}"
        estimated_input_tokens = len(input_text) // 4
        estimated_output_tokens = 50  # Estimated JSON response size
        
        # Update usage stats
        self.api_usage.total_requests += 1
        self.api_usage.total_input_tokens += estimated_input_tokens
        self.api_usage.total_output_tokens += estimated_output_tokens
        
        # Calculate cost based on gpt-4o-mini pricing (as of 2024)
        # Input: $0.15 per 1M tokens, Output: $0.60 per 1M tokens
        input_cost = (estimated_input_tokens / 1_000_000) * 0.15
        output_cost = (estimated_output_tokens / 1_000_000) * 0.60
        self.api_usage.estimated_cost_usd += input_cost + output_cost

    def get_api_usage(self) -> APIUsageStats:
        """Get current API usage statistics.
        
        Returns:
            API usage statistics
        """
        return self.api_usage


class FaviconAnalyzer:
    """Analyze favicons using LLM vision capabilities."""

    def __init__(self):
        """Initialize favicon analyzer."""
        config = get_config()
        self.config = config.api.openai
        self.prompt_template = config.processing.prompts.get("favicon_analysis")

        # Initialize vision LLM with rate limiting
        from ..utils.llm_client import create_llm_client
        self.llm_client = create_llm_client(use_vision=True)
        self.llm = self.llm_client.llm

    def _encode_image(self, image_bytes: bytes) -> str:
        """Encode image to base64.

        Args:
            image_bytes: Image binary data

        Returns:
            Base64 encoded string
        """
        return base64.b64encode(image_bytes).decode("utf-8")

    def analyze_favicon(self, favicon_bytes: bytes, urls: List[str]) -> FaviconAnalysis:
        """Analyze a favicon to identify company.

        Args:
            favicon_bytes: Favicon binary data
            urls: URLs using this favicon

        Returns:
            Favicon analysis result
        """
        try:
            # Encode image
            image_data = self._encode_image(favicon_bytes)

            # Create message
            message = HumanMessage(
                content=[
                    {
                        "type": "text",
                        "text": self.prompt_template.format(urls=", ".join(urls[:5]))
                    },
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{image_data}"},
                    },
                ],
            )

            # Get LLM response with rate limiting
            response = self.llm_client.invoke([message])
            content = response.content.lower()

            # Parse response
            company_name = None
            is_telecom = False
            is_hosting = False
            confidence = 0.0

            if "no sé" not in content and "no se" not in content:
                company_name = response.content.strip()
                confidence = 0.8

                # Check for telecom keywords
                telecom_keywords = ["telecom", "telco", "communications", "móvil", "mobile", "fiber"]
                if any(keyword in content for keyword in telecom_keywords):
                    is_telecom = True

                # Check for hosting keywords
                hosting_keywords = ["hosting", "cloud", "server", "data center", "cdn"]
                if any(keyword in content for keyword in hosting_keywords):
                    is_hosting = True

            # Generate hash
            from hashlib import sha256
            favicon_hash = sha256(favicon_bytes).hexdigest()

            return FaviconAnalysis(
                favicon_hash=favicon_hash,
                urls=urls,
                company_name=company_name,
                is_telecom=is_telecom,
                is_hosting=is_hosting,
                confidence=confidence,
                llm_response=response.content
            )

        except Exception as e:
            # Return empty analysis on error
            from hashlib import sha256
            favicon_hash = sha256(favicon_bytes).hexdigest()

            return FaviconAnalysis(
                favicon_hash=favicon_hash,
                urls=urls,
                confidence=0.0,
                llm_response=f"Error: {str(e)}"
            )

    def analyze_favicons(
        self,
        favicon_data: Dict[str, bytes],
        url_groups: Dict[str, List[str]]
    ) -> List[FaviconAnalysis]:
        """Analyze multiple favicons.

        Args:
            favicon_data: Dictionary of favicon_hash -> favicon_bytes
            url_groups: Dictionary of favicon_hash -> list of URLs

        Returns:
            List of favicon analyses
        """
        analyses = []

        for favicon_hash, favicon_bytes in favicon_data.items():
            urls = url_groups.get(favicon_hash, [])
            if urls and len(urls) >= 3:  # Only analyze if used by multiple sites
                analysis = self.analyze_favicon(favicon_bytes, urls)
                analyses.append(analysis)

        return analyses
    
    def create_favicon_network_groups(
        self,
        favicon_hash_to_asns: Dict[str, List[int]],
        favicon_hash_to_urls: Dict[str, List[str]],
        min_asns: int = 2
    ) -> List[NetworkGroup]:
        """Create NetworkGroups from favicon matches.
        
        Args:
            favicon_hash_to_asns: Mapping of favicon hash to ASNs using it
            favicon_hash_to_urls: Mapping of favicon hash to URLs using it
            min_asns: Minimum number of ASNs to form a group
            
        Returns:
            List of NetworkGroups based on shared favicons
        """
        groups = []
        
        for favicon_hash, asns in favicon_hash_to_asns.items():
            # Only create group if multiple ASNs share the favicon
            if len(asns) >= min_asns:
                # Get URLs for metadata
                urls = favicon_hash_to_urls.get(favicon_hash, [])
                
                group = NetworkGroup(
                    group_id=f"favicon_{favicon_hash[:16]}",
                    group_type="favicon_match",
                    asns=sorted(set(asns)),
                    common_attribute=favicon_hash,
                    metadata={
                        "favicon_hash": favicon_hash,
                        "url_count": len(urls),
                        "sample_urls": urls[:5],  # Keep first 5 URLs as examples
                        "confidence": 0.6  # Medium confidence for favicon matches
                    }
                )
                groups.append(group)
        
        return groups