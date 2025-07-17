"""Tests for data processors."""

import pandas as pd
import pytest

from borges.data.processors import (
    ASNProcessor,
    DataCleaner,
    FaviconProcessor,
    URLProcessor,
)


class TestURLProcessor:
    """Test URL processing functionality."""

    def test_extract_domain(self):
        """Test domain extraction from URLs."""
        assert URLProcessor.extract_domain("https://example.com/path") == "example"
        assert URLProcessor.extract_domain("http://sub.example.co.uk") == "example"
        assert URLProcessor.extract_domain("https://test.org") == "test"

    def test_process_redirects(self):
        """Test redirect data processing."""
        df = pd.DataFrame([
            {
                "final_url": "https://example.com",
                "asn": [100, 200]
            },
            {
                "final_url": "https://example.com",
                "asn": [300]
            },
            {
                "final_url": "https://test.org",
                "asn": [400]
            }
        ])
        
        result = URLProcessor.process_redirects(df)
        
        # Should group by final URL
        assert len(result) == 2
        
        # Check example.com group
        example_row = result[result["final_url"] == "https://example.com"].iloc[0]
        assert set(example_row["asn"]) == {100, 200, 300}
        assert example_row["domain"] == "example"
        assert example_row["asn_count"] == 3

    def test_group_by_domain(self):
        """Test grouping by domain."""
        df = pd.DataFrame([
            {"final_url": "https://example.com/page1", "asn": [100, 200]},
            {"final_url": "https://example.com/page2", "asn": [200, 300]},
            {"final_url": "https://test.org", "asn": [400, 500]},
        ])
        
        result = URLProcessor.group_by_domain(df)
        
        assert len(result) == 2
        
        example_row = result[result["domain"] == "example"].iloc[0]
        assert set(example_row["asn"]) == {100, 200, 300}


class TestASNProcessor:
    """Test ASN processing functionality."""

    def test_detect_related_asns(self):
        """Test ASN detection in text."""
        text = "This network is connected to AS12345 and AS67890. Also see AS100."
        source_asn = 12345
        
        asns = ASNProcessor.detect_related_asns(text, source_asn)
        
        assert 12345 not in asns  # Source ASN excluded
        assert 67890 in asns
        assert 100 in asns

    def test_has_asn_reference(self):
        """Test checking for ASN references."""
        assert ASNProcessor.has_asn_reference("Connected to AS12345")
        assert ASNProcessor.has_asn_reference("See as100 for details")
        assert not ASNProcessor.has_asn_reference("No ASN references here")
        assert not ASNProcessor.has_asn_reference("")

    def test_merge_asn_lists(self):
        """Test merging ASN lists."""
        lists = [
            [100, 200, 300],
            [200, 400],
            [100, 500]
        ]
        
        result = ASNProcessor.merge_asn_lists(lists)
        
        assert result == [100, 200, 300, 400, 500]


class TestFaviconProcessor:
    """Test favicon processing functionality."""

    def test_hash_favicon(self):
        """Test favicon hashing."""
        data1 = b"favicon data 1"
        data2 = b"favicon data 2"
        
        hash1 = FaviconProcessor.hash_favicon(data1)
        hash2 = FaviconProcessor.hash_favicon(data2)
        
        # Different data should have different hashes
        assert hash1 != hash2
        
        # Same data should have same hash
        assert hash1 == FaviconProcessor.hash_favicon(data1)

    def test_group_by_favicon(self):
        """Test grouping by favicon."""
        df = pd.DataFrame([
            {"final_url": "https://site1.com", "favicon": b"icon1"},
            {"final_url": "https://site2.com", "favicon": b"icon1"},
            {"final_url": "https://site3.com", "favicon": b"icon2"},
            {"final_url": "https://site4.com", "favicon": None},
        ])
        
        result = FaviconProcessor.group_by_favicon(df)
        
        # Should have 2 groups (icon1 and icon2, not None)
        assert len(result) == 2
        
        # Check grouping
        icon1_hash = FaviconProcessor.hash_favicon(b"icon1")
        icon1_row = result[result["favicon_hash"] == icon1_hash].iloc[0]
        assert len(icon1_row["final_url"]) == 2
        assert icon1_row["url_count"] == 2

    def test_filter_common_favicons(self):
        """Test filtering common favicons."""
        df = pd.DataFrame([
            {"favicon_hash": "hash1", "url_count": 5},
            {"favicon_hash": "hash2", "url_count": 2},
            {"favicon_hash": "hash3", "url_count": 10},
        ])
        
        result = FaviconProcessor.filter_common_favicons(df, min_urls=3)
        
        assert len(result) == 2
        assert "hash2" not in result["favicon_hash"].values


class TestDataCleaner:
    """Test data cleaning functionality."""

    def test_clean_url(self):
        """Test URL cleaning."""
        assert DataCleaner.clean_url("https://example.com") == "https://example.com"
        assert DataCleaner.clean_url("example.com") == "https://example.com"
        assert DataCleaner.clean_url("  http://example.com  ") == "http://example.com"
        assert DataCleaner.clean_url("") is None
        assert DataCleaner.clean_url("bad url with spaces") is None
        assert DataCleaner.clean_url("short") is None

    def test_clean_asn(self):
        """Test ASN cleaning."""
        assert DataCleaner.clean_asn(12345) == 12345
        assert DataCleaner.clean_asn("12345") == 12345
        assert DataCleaner.clean_asn("AS12345") == 12345
        assert DataCleaner.clean_asn("as12345") == 12345
        assert DataCleaner.clean_asn(0) is None
        assert DataCleaner.clean_asn(4294967296) is None  # Too large
        assert DataCleaner.clean_asn("invalid") is None

    def test_clean_dataframe(self):
        """Test DataFrame cleaning."""
        df = pd.DataFrame([
            {"asn": "AS100", "website": "example.com"},
            {"asn": 200, "website": "https://test.org"},
            {"asn": "invalid", "website": "bad url"},
            {"asn": 0, "website": ""},
        ])
        
        result = DataCleaner.clean_dataframe(df)
        
        # Should have cleaned ASNs and URLs
        assert len(result) == 2  # Two valid rows
        assert result.iloc[0]["asn"] == 100
        assert result.iloc[0]["website"] == "https://example.com"
        assert result.iloc[1]["asn"] == 200