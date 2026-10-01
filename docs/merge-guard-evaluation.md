# Merge guard: evaluation on the 2025-09-29 run

The merge guard (`processing.merge_guard` in `config.yaml`) stops weak signals from
joining unrelated organizations during consolidation. It is **off by default**. This page
records how it was evaluated, so the numbers can be reproduced and challenged.

## Why it exists

`NetworkGroupConsolidator._merge_analysis_groups` merges every organization that an
analysis group touches, and the merges chain. A single favicon shared by unrelated
websites therefore produced groups such as "Power Media" (54 ASNs, including the
University of Montenegro) and "United SA" (105 ASNs: DE-CIX, Digital Realty, Pakistan
Telecom). Until now, each case was fixed by adding a favicon hash or an ASN to a
blocklist. That cannot keep up: the same default favicons came back under new hashes in
the next run.

## What it does

| Signal | Rule |
|---|---|
| `favicon_match` spanning ≥ 3 WHOIS organizations | Split by **brand**, the first 5 letters of the registrable domain name, or the full name when it starts with a generic word (`inter`, `telec`, …). Brand pieces with ≥ 2 ASNs are kept. This is the paper's "same favicon and same domain" rule. |
| `favicon_match` with one brand | Kept, marked verified |
| `website` | Kept and marked verified, unless the site is a lookup service (`rdap.`, `whois.`, `bgp.tools`, …). Those groups are dropped. |
| `redirect_target`, small multi-brand favicon pairs | Unverified: they may not join **two established organizations** (≥ 2 ASNs each) on their own. They need a second signal of a different type connecting the same organizations. |

LLM and WHOIS signals are never filtered. Every decision is written to
`data/output/merge_guard_review_<timestamp>.json`.

## Method

All runs are offline: no scraping and no LLM calls. `scripts/evaluate_merge_guard.py`
replays `load_data` on the run's input snapshots and attaches the run's exported
`network_groups`. It then consolidates with the guard off and on. The run used here is
the 2025-09-29 one, with PeeringDB 2025-08-01 and AS2Org 2025-09-01, and the run's own
`config.yaml`.

Replay fidelity: with the guard off, 88,297 groups are identical to the original run's
output. All differences come from input drift. The local snapshot files contain 5,253
ASNs that the original run did not have, and lack 250 that it did. No group differs for
any other reason.

## Results (same inputs, guard off → on)

| Final groups combining… | Off | On |
|---|---|---|
| ≥ 20 WHOIS organizations | 6 groups, 277 ASNs | 1 group, 61 ASNs |
| ≥ 10 | 33 groups, 1,147 ASNs | 16 groups, 550 ASNs |
| ≥ 5 | 90 groups, 2,188 ASNs | 58 groups, 1,595 ASNs |
| ≥ 2 | 1,227 groups, 9,118 ASNs | 1,179 groups, 8,350 ASNs |

Guard decisions: 63 favicon groups split, 55 groups rejected, 24 bridges blocked. 117
groups of the guard-off result changed:

- **53 look like artifacts**: they have nearly one WHOIS organization per ASN. Examples:
  "Deepnet" (34 ASNs, 31 organizations → 30 pieces), "Ydio", "LEV Telecom", "ROS
  Telecom", and hobbyist networks sharing a template favicon. "United SA" splits into
  Digital Realty, IX operators and the rest.
- **64 need a look.** Some are probably real companies with several brands that lost
  members: A1 Telekom Austria, Claranet, Vodafone (3 ASNs), Dell, Fujitsu, IBM Cloud,
  Apogee and DataPipe. A favicon shared across different domains is ambiguous. The paper
  resolved it with the vision-LLM classifier, which this guard overrides.

Known sibling sets that stay together with the guard on: Claro (América Móvil), Leaseweb,
Deutsche Telekom, Orange, Telstra.

## Before enabling it by default

1. Label the 64 "needs a look" groups (hand-check CSV produced by the evaluation).
2. If real multi-brand companies dominate, relax the bridge rule for favicon pairs
   (for example, require only that one of the two organizations is established).
3. Record the decision and the final numbers here and in `CHANGELOG.md` under
   *Changed (results)*.
