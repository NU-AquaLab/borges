"""Tests for the consolidation merge guard on a tiny synthetic network.

No network access and no LLM calls: the analysis groups are built by hand.
"""

import pytest

from borges.analyzers.network_consolidator import NetworkGroupConsolidator
from borges.config import MergeGuardConfig
from borges.models import ASNetwork, AutonomousSystem, NetworkGroup, Organization

# asn: (WHOIS org, website)
ASES = {
    # Claro: different legal entities, one brand, one favicon
    1: ("ORG-CLARO-CL", "https://www.clarochile.cl"),
    2: ("ORG-CLARO-PR", "https://www.claropr.com"),
    # Unrelated ISPs sharing a WordPress-style default favicon
    11: ("ORG-ALPHA", "https://alphanet.com.br"),
    12: ("ORG-BRAVO", "https://bravoisp.net"),
    13: ("ORG-CHARLIE", "https://charlietel.com"),
    14: ("ORG-DELTA", "https://deltanet.io"),
    # Two established organizations (2 ASNs each)
    21: ("ORG-ECHO", "https://echonet.com"),
    22: ("ORG-ECHO", "https://echonet.com"),
    31: ("ORG-FOXTROT", "https://foxtrot.net"),
    32: ("ORG-FOXTROT", "https://foxtrot.net"),
    # Unrelated networks listing an RIR lookup page as their website
    41: ("ORG-GOLF", None),
    42: ("ORG-HOTEL", None),
    43: ("ORG-INDIA", None),
}


def make_network(groups):
    net = ASNetwork()
    for asn, (org_id, website) in ASES.items():
        net.add_as(
            AutonomousSystem(asn=asn, org_id=org_id, name=f"AS{asn}", website=website)
        )
    for org_id, asns in net.org_to_as.items():
        net.organizations[org_id] = Organization(
            org_id=org_id, name=org_id, asns=sorted(asns)
        )
    net.network_groups = groups
    return net


def group(group_id, group_type, asns, attribute="x"):
    return NetworkGroup(
        group_id=group_id,
        group_type=group_type,
        asns=asns,
        common_attribute=attribute,
    )


GROUPS = [
    group("fav_claro", "favicon_match", [1, 2], "claro-logo"),
    group("fav_wordpress", "favicon_match", [11, 12, 13, 14], "wordpress-w"),
    group("redirect_bridge", "redirect_target", [21, 31], "https://parkingpage.com"),
    group("site_rdap", "website", [41, 42, 43], "rdap.apnic.net"),
]


def consolidate(groups, enabled):
    consolidator = NetworkGroupConsolidator(
        make_network(groups), merge_guard=MergeGuardConfig(enabled=enabled)
    )
    result = consolidator.consolidate_groups()
    return {frozenset(g["asns"]) for g in result}, consolidator.merge_guard_review


def together(partition, asns):
    return any(set(asns) <= g for g in partition)


@pytest.fixture(params=[False, True], ids=["guard_off", "guard_on"])
def run(request):
    return request.param, *consolidate(GROUPS, request.param)


def test_brand_favicon_merges_either_way(run):
    _, partition, _ = run
    assert together(partition, [1, 2])


def test_generic_favicon_only_merges_without_guard(run):
    enabled, partition, _ = run
    assert together(partition, [11, 12, 13, 14]) is not enabled


def test_single_redirect_bridge_only_merges_without_guard(run):
    enabled, partition, _ = run
    assert together(partition, [21, 22, 31, 32]) is not enabled
    assert together(partition, [21, 22]) and together(partition, [31, 32])


def test_lookup_service_website_only_merges_without_guard(run):
    enabled, partition, _ = run
    assert together(partition, [41, 42, 43]) is not enabled


def test_review_records_every_decision(run):
    enabled, _, review = run
    decisions = {(r["group_id"], r["decision"]) for r in review}
    if not enabled:
        assert not review
        return
    assert decisions == {
        ("fav_wordpress", "rejected"),
        ("redirect_bridge", "blocked_bridge"),
        ("site_rdap", "rejected"),
    }


def test_corroborated_bridge_is_allowed():
    corroboration = group("llm", "llm_detected", [22, 32], "notes of AS22")
    partition, review = consolidate(GROUPS + [corroboration], enabled=True)
    assert together(partition, [21, 22, 31, 32])
    assert "redirect_bridge" not in {r["group_id"] for r in review}


def test_mixed_favicon_keeps_brand_pairs():
    mixed = group("fav_mixed", "favicon_match", [1, 2, 11, 12], "shared")
    partition, review = consolidate([mixed], enabled=True)
    assert together(partition, [1, 2])
    assert not together(partition, [11, 12])
    (record,) = review
    assert record["decision"] == "split"
    assert record["kept"] == {"claro": [1, 2]}
    assert record["dropped"] == [11, 12]


def test_disabled_guard_matches_legacy_behavior():
    """With the guard off, the consolidator must not touch any group."""
    consolidator = NetworkGroupConsolidator(make_network(GROUPS))
    assert consolidator.merge_guard.enabled is False
    assert consolidator._guarded_analysis_groups() == GROUPS
