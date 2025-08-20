"""Tests for data models."""

import pytest

from borges.models import (
    ASNetwork,
    ASRelationship,
    AutonomousSystem,
    FaviconAnalysis,
    NetworkGroup,
    Organization,
)


class TestASNetwork:
    """Test AS network functionality."""

    def test_add_as(self):
        """Test adding AS to network."""
        network = ASNetwork()
        
        as_info = AutonomousSystem(
            asn=12345,
            org_id="ORG-TEST",
            name="Test AS",
            website="https://example.com"
        )
        
        network.add_as(as_info)
        
        assert 12345 in network.autonomous_systems
        assert network.autonomous_systems[12345] == as_info
        assert network.as_to_org[12345] == "ORG-TEST"
        assert 12345 in network.org_to_as["ORG-TEST"]

    def test_add_relationship(self):
        """Test adding AS relationship."""
        network = ASNetwork()
        
        relationship = ASRelationship(
            source_asn=12345,
            related_asns=[67890, 11111],
            relationship_type="same_organization",
            confidence=0.9,
            detected_by="test"
        )
        
        network.add_relationship(relationship)
        
        assert len(network.as_relationships) == 1
        assert network.as_relationships[0] == relationship

    def test_get_related_asns(self):
        """Test getting related ASNs."""
        network = ASNetwork()
        
        # Add ASs
        network.add_as(AutonomousSystem(asn=100, org_id="ORG-A"))
        network.add_as(AutonomousSystem(asn=200, org_id="ORG-A"))
        network.add_as(AutonomousSystem(asn=300, org_id="ORG-B"))
        
        # Add relationship
        network.add_relationship(ASRelationship(
            source_asn=100,
            related_asns=[300],
            relationship_type="partner",
            confidence=0.8,
            detected_by="test"
        ))
        
        # Test related ASNs
        related = network.get_related_asns(100)
        assert 200 in related  # Same org
        assert 300 in related  # Direct relationship
        assert 100 not in related  # Not self

    def test_merge_organizations(self):
        """Test merging organizations."""
        network = ASNetwork()
        
        # Add ASs to different orgs
        network.add_as(AutonomousSystem(asn=100, org_id="ORG-A"))
        network.add_as(AutonomousSystem(asn=200, org_id="ORG-A"))
        network.add_as(AutonomousSystem(asn=300, org_id="ORG-B"))
        network.add_as(AutonomousSystem(asn=400, org_id="ORG-B"))
        
        # Merge organizations
        merged_id = network.merge_organizations("ORG-A", "ORG-B")
        
        assert merged_id == "ORG-A"
        assert network.as_to_org[100] == "ORG-A"
        assert network.as_to_org[300] == "ORG-A"
        assert len(network.org_to_as["ORG-A"]) == 4
        assert "ORG-B" not in network.org_to_as

    def test_create_domain_groups(self):
        """Test creating domain-based groups."""
        network = ASNetwork()
        
        # Add domain mappings
        network.add_domain_mapping("example", [100, 200])
        network.add_domain_mapping("test", [300, 400, 500])
        network.add_domain_mapping("single", [600])  # Should not create group
        
        # Create groups
        groups = network.create_domain_groups()
        
        assert len(groups) == 2
        assert any(g.common_attribute == "example" for g in groups)
        assert any(g.common_attribute == "test" for g in groups)
        assert not any(g.common_attribute == "single" for g in groups)


class TestSchemas:
    """Test Pydantic schemas."""

    def test_autonomous_system(self):
        """Test AutonomousSystem schema."""
        as_data = {
            "asn": 12345,
            "org_id": "ORG-TEST",
            "name": "Test AS",
            "website": "https://example.com",
            "notes": "Test notes",
            "aka": "Test AKA"
        }
        
        as_obj = AutonomousSystem(**as_data)
        
        assert as_obj.asn == 12345
        assert as_obj.org_id == "ORG-TEST"
        assert str(as_obj.website) == "https://example.com/"

    def test_as_relationship_validation(self):
        """Test ASRelationship validation."""
        # Should remove source ASN from related ASNs
        relationship = ASRelationship(
            source_asn=100,
            related_asns=[100, 200, 300],  # Includes source
            relationship_type="test",
            confidence=0.5,
            detected_by="test"
        )
        
        assert 100 not in relationship.related_asns
        assert 200 in relationship.related_asns
        assert 300 in relationship.related_asns

    def test_network_group_validation(self):
        """Test NetworkGroup validation."""
        group = NetworkGroup(
            group_id="test_group",
            group_type="domain",
            asns=[300, 100, 200, 100],  # Duplicates and unsorted
            common_attribute="example.com"
        )
        
        # Should be unique and sorted
        assert group.asns == [100, 200, 300]


@pytest.fixture
def sample_as_network():
    """Create a sample AS network for testing."""
    network = ASNetwork()
    
    # Add some ASs
    network.add_as(AutonomousSystem(asn=100, org_id="ORG-A", name="AS 100"))
    network.add_as(AutonomousSystem(asn=200, org_id="ORG-A", name="AS 200"))
    network.add_as(AutonomousSystem(asn=300, org_id="ORG-B", name="AS 300"))
    
    return network