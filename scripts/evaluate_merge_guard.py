#!/usr/bin/env python3
"""Replay network consolidation offline and compare it with a reference run.

Rebuilds the consolidator's input from a finished pipeline run, without scraping
or LLM calls: the ``load_data`` stage is replayed on the run's PeeringDB and
WHOIS snapshots, and the analysis groups are read back from the run's exported
``network_groups`` file. Consolidation then runs with the merge guard on or off.

Example (on the machine holding the run)::

    python scripts/evaluate_merge_guard.py \\
        --config config.yaml \\
        --peeringdb data/input/peeringdb_2_dump_2025_08_01.json \\
        --whois data/input/20250901.as-org2info.txt \\
        --network-groups data/output/network_groups_20250929_013117.parquet \\
        --reference data/output/consolidated_groups_summary_20250929_013118.parquet \\
        --guard off --out /tmp/eval-baseline
"""

import argparse
import json
import sys
import time
from collections import Counter
from pathlib import Path

import pandas as pd

from borges.analyzers.network_consolidator import NetworkGroupConsolidator
from borges.config import load_config, set_config
from borges.models import NetworkGroup
from borges.pipeline.stages import LoadDataStage

ORG_SPAN_THRESHOLDS = [2, 5, 10, 20, 50]


def build_as_network(config, peeringdb: Path, whois: Path, network_groups: Path):
    """Replay load_data and attach the run's exported analysis groups."""
    context = {
        "config": config,
        "input_overrides": {"peeringdb": str(peeringdb), "whois": str(whois)},
    }
    result = LoadDataStage("load_data", config.model_dump()).run(context)
    if result.status != "success":
        sys.exit(f"load_data failed: {result.errors}")
    as_network = context["as_network"]

    df = pd.read_parquet(network_groups)
    as_network.network_groups = [
        NetworkGroup(
            group_id=str(row.group_id),
            group_type=str(row.group_type),
            asns=[int(a) for a in row.asns],
            common_attribute=str(row.common_attribute),
        )
        for row in df.itertuples(index=False)
    ]
    return as_network


def org_span(asns, asn_to_org) -> int:
    """Number of distinct WHOIS organizations among ASNs."""
    return len({asn_to_org[a] for a in asns if a in asn_to_org})


def summarize(groups, asn_to_org) -> dict:
    """Size and organization-span statistics of a consolidation result."""
    spans = [org_span(g["asns"], asn_to_org) for g in groups]
    sizes = [len(g["asns"]) for g in groups]
    return {
        "groups": len(groups),
        "asns": sum(sizes),
        "largest_group": max(sizes, default=0),
        "groups_spanning_orgs": {
            f">={k}": {
                "groups": sum(1 for s in spans if s >= k),
                "asns": sum(n for n, s in zip(sizes, spans) if s >= k),
            }
            for k in ORG_SPAN_THRESHOLDS
        },
    }


def partition(groups) -> set:
    return {frozenset(int(a) for a in g["asns"]) for g in groups}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--peeringdb", required=True, type=Path)
    parser.add_argument("--whois", required=True, type=Path)
    parser.add_argument("--network-groups", required=True, type=Path)
    parser.add_argument(
        "--reference",
        type=Path,
        help="consolidated_groups_summary parquet of the original run",
    )
    parser.add_argument("--guard", choices=["on", "off"], default="off")
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    config = load_config(args.config)
    config.processing.merge_guard.enabled = args.guard == "on"
    set_config(config)

    start = time.time()
    as_network = build_as_network(
        config, args.peeringdb, args.whois, args.network_groups
    )
    asn_to_org = dict(as_network.as_to_org)  # WHOIS organization of each ASN

    consolidator = NetworkGroupConsolidator(
        as_network,
        blocklist=set(config.processing.asn_blocklist),
        merge_guard=config.processing.merge_guard,
    )
    groups = consolidator.consolidate_groups()

    report = {
        "guard": args.guard,
        "inputs": {k: str(v) for k, v in vars(args).items()},
        "base_organizations": len(as_network.organizations),
        "autonomous_systems": len(as_network.autonomous_systems),
        "analysis_groups": dict(
            Counter(g.group_type for g in as_network.network_groups)
        ),
        "result": summarize(groups, asn_to_org),
        "seconds": round(time.time() - start, 1),
    }
    if consolidator.merge_guard_review:
        report["merge_guard"] = Counter(
            r["decision"] for r in consolidator.merge_guard_review
        )

    if args.reference:
        ref = pd.read_parquet(args.reference)
        ref_groups = [{"asns": list(a)} for a in ref["asns"]]
        mine, theirs = partition(groups), partition(ref_groups)
        report["reference"] = {
            "result": summarize(ref_groups, asn_to_org),
            "identical_groups": len(mine & theirs),
            "only_in_reference": len(theirs - mine),
            "only_in_this_run": len(mine - theirs),
            "partition_identical": mine == theirs,
        }

    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "report.json").write_text(json.dumps(report, indent=2, default=str))
    pd.DataFrame(
        [
            {
                "primary_name": g.get("primary_name"),
                "asn_count": len(g["asns"]),
                "org_span": org_span(g["asns"], asn_to_org),
                "asns": sorted(int(a) for a in g["asns"]),
                "sources": ", ".join(g.get("sources", [])),
            }
            for g in groups
        ]
    ).to_parquet(args.out / "groups.parquet")
    if consolidator.merge_guard_review:
        (args.out / "merge_guard_review.json").write_text(
            json.dumps(consolidator.merge_guard_review, indent=2, default=str)
        )
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
