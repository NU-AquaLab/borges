"""Number validation module to prevent LLM hallucination of ASNs."""

import re
import logging
from typing import List, Set, Tuple

logger = logging.getLogger(__name__)


def is_bgp_community_pattern(text: str, number: str) -> bool:
    """Check if a number appears ONLY in BGP community patterns (not as standalone ASN).
    
    This function returns True only if the number appears exclusively in BGP community 
    patterns and never as a standalone ASN mention.
    
    Args:
        text: The text to search in
        number: The number to check
        
    Returns:
        True if the number appears ONLY in BGP community patterns
    """
    escaped_num = re.escape(number)
    
    # Check for legitimate ASN mentions first - if found, don't filter
    legitimate_asn_patterns = [
        rf'\bAS{escaped_num}\b',                      # AS12345
        rf'\bASN\s*{escaped_num}\b',                  # ASN 12345
        rf'autonomous\s+system\s+{escaped_num}\b',    # autonomous system 12345
        rf'we\s+operate\s+AS{escaped_num}',           # we operate AS12345
        rf'our\s+network\s+AS{escaped_num}',          # our network AS12345
        rf'network\s+AS{escaped_num}',                # network AS12345
        rf'peer\s+with\s+.*AS{escaped_num}',          # peer with ... AS12345
    ]
    
    # If it appears as a legitimate ASN mention, don't filter it
    has_legitimate_mention = False
    for pattern in legitimate_asn_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            has_legitimate_mention = True
            break
    
    if has_legitimate_mention:
        logger.debug(f"Number {number} found as legitimate ASN mention, not filtering")
        return False
    
    # Now check if it appears in BGP community patterns
    bgp_community_patterns = [
        # Parenthesized patterns
        rf'\({escaped_num}:\d+\)',                    # (ASN:value)
        rf'\(\d+:{escaped_num}\)',                    # (value:ASN)
        rf'\({escaped_num}:\d+:\d+\)',                # (ASN:value:value)
        rf'\(\d+:{escaped_num}:\d+\)',                # (value:ASN:value)
        rf'\(\d+:\d+:{escaped_num}\)',                # (value:value:ASN)
        rf'\(0:{escaped_num}\)',                      # (0:ASN)
        rf'\({escaped_num}:0\)',                      # (ASN:0)
        rf'\({escaped_num}:0:\d+\)',                  # (ASN:0:value)
        rf'\({escaped_num}:0:0\)',                    # (ASN:0:0)
        
        # Non-parenthesized patterns with word boundaries
        rf'\b{escaped_num}:\d+(?=\s|,|;|$|\))',       # ASN:value
        rf'\b\d+:{escaped_num}(?=\s|,|;|$|\))',       # value:ASN
        rf'\b{escaped_num}:\d+:\d+(?=\s|,|;|$|\))',   # ASN:value:value
        rf'\b\d+:{escaped_num}:\d+(?=\s|,|;|$|\))',   # value:ASN:value
        rf'\b\d+:\d+:{escaped_num}(?=\s|,|;|$|\))',   # value:value:ASN
        rf'\b0:{escaped_num}(?=\s|,|;|$|\))',         # 0:ASN
        rf'\b{escaped_num}:0(?=\s|,|;|$|\))',         # ASN:0
        rf'\b{escaped_num}:0:\d+(?=\s|,|;|$|\))',     # ASN:0:value
        rf'\b{escaped_num}:0:0(?=\s|,|;|$|\))',       # ASN:0:0
        
        # ASN:ASN patterns (very common in BGP communities)
        rf'\b{escaped_num}:{escaped_num}(?=\s|,|;|$|\))',  # ASN:ASN (same number twice)
        rf'\({escaped_num}:{escaped_num}\)',          # (ASN:ASN)
        
        # PEER-AS patterns (handle placeholders in documentation)
        rf'{escaped_num}:0:PEER-AS\d*',               # ASN:0:PEER-AS or ASN:0:PEER-AS2
        rf'{escaped_num}:\d+:PEER-AS\d*',             # ASN:value:PEER-AS
        rf'\({escaped_num}:.*?PEER-AS.*?\)',          # (ASN:...PEER-AS...)
        rf'PEER-AS{escaped_num}(?!\d)',               # PEER-AS followed by number (not part of larger number)
        rf'\bPEER-AS{escaped_num}\b',                 # PEER-AS with word boundaries
    ]
    
    # Check if it appears in BGP community patterns
    for pattern in bgp_community_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            logger.debug(f"Number {number} found only in BGP community pattern")
            return True
    
    return False


def extract_numbers_from_text(text: str) -> Set[int]:
    """Extract all numbers from text that could potentially be ASNs.
    
    Args:
        text: Input text to search for numbers
        
    Returns:
        Set of numbers in valid ASN range (1-400000)
    """
    if not text:
        return set()
    
    # Pre-filter: Remove PEER-AS patterns to prevent extracting numbers from placeholders
    # Replace patterns like "PEER-AS1", "PEER-AS2" with "PEER-ASX" to avoid number extraction
    cleaned_text = re.sub(r'PEER-AS\d+', 'PEER-ASX', text, flags=re.IGNORECASE)
    
    # Find all potential ASN numbers using multiple patterns:
    # 1. AS12345 format (most reliable)
    # 2. ASN 12345 format (also reliable)
    # 3. Autonomous System 12345 format
    # 4. Standalone numbers that could be ASNs (very conservative)
    
    all_numbers = set()
    
    # Pattern 1: AS followed by digits (most common and reliable)
    as_pattern = re.findall(r'AS(\d+)', cleaned_text, re.IGNORECASE)
    for num in as_pattern:
        if not is_bgp_community_pattern(text, num):
            all_numbers.add(num)
    
    # Pattern 2: ASN followed by digits
    asn_pattern = re.findall(r'ASN\s*(\d+)', cleaned_text, re.IGNORECASE)
    for num in asn_pattern:
        if not is_bgp_community_pattern(text, num):
            all_numbers.add(num)
    
    # Pattern 3: "Autonomous System" followed by digits
    autonomous_system_pattern = re.findall(r'autonomous\s+system\s+(\d+)', cleaned_text, re.IGNORECASE)
    for num in autonomous_system_pattern:
        if not is_bgp_community_pattern(text, num):
            all_numbers.add(num)
    
    # Pattern 4: Standalone numbers in ASN range (very conservative)
    # Only include if they appear in contexts that suggest they're ASNs
    asn_context_keywords = ['peer', 'transit', 'upstream', 'downstream', 'bgp', 'routing', 'announce', 'prefix']
    if any(keyword in text.lower() for keyword in asn_context_keywords):
        standalone_pattern = re.findall(r'(?:^|\s)(\d{1,6})(?=\s|$|[^-\d])', cleaned_text)
        for num_str in standalone_pattern:
            num = int(num_str)
            # Very restrictive range and avoid common non-ASN numbers
            if 1000 <= num <= 400000 and num not in {1000, 2000, 3000, 4000, 5000, 10000}:
                # Check if it's not in a BGP community pattern
                if not is_bgp_community_pattern(text, num_str):
                    all_numbers.add(num_str)
    
    # Convert to int and filter to valid ASN range
    valid_asns = set()
    for num_str in all_numbers:
        try:
            num = int(num_str)
            # ASN range: 1 to 400000 (approximate upper bound for valid ASNs)
            if 1 <= num <= 400000:
                valid_asns.add(num)
        except ValueError:
            # Skip invalid numbers
            continue
    
    return valid_asns


def validate_llm_output(input_text: str, detected_asns: List[int], source_asn: int = None, blocklist: set = None) -> List[int]:
    """Validate LLM output to ensure no hallucinated ASNs.
    
    Args:
        input_text: Original input text (notes + aka)
        detected_asns: ASNs detected by LLM
        source_asn: Source ASN to exclude from validation
        blocklist: Set of ASNs to filter out regardless of text presence
        
    Returns:
        Filtered list of ASNs that actually appear in input text and are not blocklisted
    """
    if not detected_asns:
        return []
    
    # Extract all valid numbers from input text
    valid_numbers = extract_numbers_from_text(input_text)
    
    # Exclude source ASN from validation (it's not expected to appear in its own description)
    if source_asn and source_asn in valid_numbers:
        valid_numbers.discard(source_asn)
    
    # Filter LLM output to only include numbers that appear in input
    validated_asns = []
    hallucinated_asns = []
    company_mentions = []  # Track cases where company names appear without ASNs
    blocklisted_asns = []  # Track blocklisted ASNs
    
    for asn in detected_asns:
        # Check blocklist first
        if blocklist and asn in blocklist:
            blocklisted_asns.append(asn)
            continue
            
        if asn in valid_numbers:
            validated_asns.append(asn)
        else:
            hallucinated_asns.append(asn)
            # Check if this might be a company name inference
            if _is_likely_company_inference(input_text, asn):
                company_mentions.append(asn)
    
    # Log blocklisted ASNs  
    if blocklisted_asns:
        logger.info(f"Filtered blocklisted ASNs: {blocklisted_asns} for source ASN {source_asn}")
    
    # Enhanced logging for different types of hallucinations
    if hallucinated_asns:
        if company_mentions:
            logger.warning(
                f"LLM inferred ASNs from company mentions: {company_mentions}. "
                f"Source ASN: {source_asn}. "
                f"Company names found in text but ASN numbers missing. "
                f"Input text: '{input_text[:150]}...'"
            )
        
        other_hallucinations = [asn for asn in hallucinated_asns if asn not in company_mentions]
        if other_hallucinations:
            logger.warning(
                f"LLM hallucinated ASNs with no textual basis: {other_hallucinations}. "
                f"Source ASN: {source_asn}. "
                f"Input text: '{input_text[:100]}...'. "
                f"Valid numbers in text: {sorted(valid_numbers)}"
            )
    
    if validated_asns:
        logger.debug(f"Validated ASNs: {validated_asns} for source ASN {source_asn}")
    
    return validated_asns


def _is_likely_company_inference(text: str, asn: int) -> bool:
    """Check if an ASN was likely inferred from company name mentions.
    
    Args:
        text: Input text to check
        asn: ASN that was hallucinated
        
    Returns:
        True if the ASN appears to be inferred from company names
    """
    # Common company-to-ASN inferences that should be caught
    company_asn_map = {
        # Google ASNs
        15169: ['google', 'youtube'],
        36040: ['google', 'youtube'],
        36384: ['google', 'youtube'],
        36385: ['google', 'youtube'],
        
        # Amazon ASNs  
        16509: ['amazon', 'aws'],
        14618: ['amazon', 'aws'],
        
        # Microsoft ASNs
        8075: ['microsoft', 'azure'],
        3598: ['microsoft', 'azure'],
        
        # AMS-IX ASNs
        1200: ['ams-ix', 'amsterdam internet exchange'],
        
        # Equinix ASNs (note: these are not actually Equinix's ASNs)
        20940: ['equinix'],  # Actually Akamai
        26415: ['equinix'],  # Actually VeriSign
        
        # Cloudflare
        13335: ['cloudflare'],
        
        # Cogent
        174: ['cogent'],
    }
    
    text_lower = text.lower()
    if asn in company_asn_map:
        for company_name in company_asn_map[asn]:
            if company_name in text_lower:
                return True
    
    return False


def check_input_has_numbers(text: str) -> bool:
    """Check if input text contains any numbers that could be ASNs.
    
    Args:
        text: Input text to check
        
    Returns:
        True if text contains potential ASN numbers
    """
    return len(extract_numbers_from_text(text)) > 0