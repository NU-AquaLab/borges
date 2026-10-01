# Merge guard: evaluation

The merge guard (`processing.merge_guard` in `config.yaml`) stops individual links from
joining unrelated organizations during consolidation. It is **off by default**. This page
records how it was evaluated, so the numbers can be reproduced and challenged.

## Why it exists

`NetworkGroupConsolidator` merges every organization that a link touches, and merges
chain. One wrong link anywhere in a chain therefore welds two companies together. Each
case used to be fixed by hand: an ASN blocklist, a PeeringDB exclusion, or a favicon hash.
Traced examples:

| Run | Result | Chain |
|---|---|---|
| 2025-08-05 | Level3 (AS3356) and Orange (AS5511) inside a 1,294-ASN group | Lumen → Arelion → Orange, through two networks' PeeringDB notes that list their **upstream providers** |
| 2025-08-07 | Level3 and Orange inside "Sprint" (341 ASNs) | notes listing upstreams (Cherry Servers → Lumen, → Cogent) → `cogentco.com` → Sprint → **AS4004** (Sprint in WHOIS, Orange Business Services in PeeringDB) → Orange |
| 2025-09-29 | "Power Media" (54 ASNs), "United SA" (105 ASNs) | one favicon shared by unrelated websites; the same favicons came back under new hashes in later runs |

## Rules

| Link | Rule |
|---|---|
| `favicon_match` spanning ≥ 3 WHOIS organizations | Split by **brand**, the first 5 letters of the registrable domain name, or the full name when it starts with a generic word (`inter`, `telec`, …). This is the paper's "same favicon and same domain" rule. |
| `website` | Dropped if it is a lookup service (`rdap.`, `whois.`, `bgp.tools`, …) |
| Unverified weak link (redirect, multi-brand favicon pair) | May not join two **established** organizations (≥ 2 ASNs) alone |
| Extracted relationship from notes (`llm_detected`) | May not join two **large** organizations (≥ 10 ASNs) alone |
| PeeringDB-organization pass | A large group may not be pulled in by **one** ASN whose WHOIS organization disagrees (the AS4004 pattern) |

"Alone" means no link of a *different* type connects the same organizations. Every
decision is written to `data/output/merge_guard_review_<timestamp>.json`. The extraction
step itself is unchanged; only how its output is merged is constrained.

## Method

All runs are offline: no scraping and no LLM calls. `scripts/evaluate_merge_guard.py`
replays `load_data` on a run's input snapshots, attaches the run's exported
`network_groups`, and consolidates with the guard off and on.

Replay fidelity (2025-09-29 run, PeeringDB 2025-08-01, AS2Org 2025-09-01): with the guard
off, 88,297 groups are identical to the original output. All differences come from input
drift (5,253 ASNs only in the local snapshots, 250 only in the original run). The
2025-08-05 and 2025-08-07 replays (July snapshots) reproduce the original Level3+Orange
groups exactly (1,294 and 341 ASNs).

## Results

**2025-09-29 run** (same inputs, guard off → on, current hand blocklists kept):

| Final groups combining… | Off | On |
|---|---|---|
| ≥ 20 WHOIS organizations | 6 groups, 277 ASNs | 0 |
| ≥ 10 | 33 groups, 1,147 ASNs | 14 groups, 397 ASNs |
| ≥ 5 | 90 groups, 2,188 ASNs | 54 groups, 1,298 ASNs |
| ≥ 2 | 1,227 groups, 9,118 ASNs | 1,178 groups, 7,882 ASNs |

- **Decisions:** 63 favicon groups split, 55 groups rejected, 48 merges blocked.
- **Stay together:** Claro (América Móvil), Leaseweb, Orange, Telstra, Lumen (Level3 + CenturyLink + AS3549), Verizon, AT&T.
- **No longer merged, needs review:**
  - **Deutsche Telekom:** AS3320's notes link two DT subsidiaries of 12 ASNs each, with no second link type.
  - **Cogent + Sprint wireline.**

**August runs with all hand fixes removed** (no ASN blocklist, no PeeringDB exclusions):

| Run | Off | On |
|---|---|---|
| 2025-08-05 | Level3 + Orange in a 1,294-ASN group | separate; largest group 976 (US DoD) |
| 2025-08-07 | Level3 + Orange in "Sprint" (341) | separate |

The guard alone does not fully replace the hand fixes yet:

- **Sprint + Orange.** Without the AS4004 exclusion they still merge, because AS4004 also lists `orange-business.com` as its website. A shared website looks the same whether it's real (Sprint's AS3646 lists `cogentco.com`) or comes from a stale WHOIS record (AS4004).
- **Lumen absorbed by a large hosting group** in the 2025-08-05 replay: large organizations can still grow by absorbing small ones one at a time.

## Before enabling it by default

1. Label the groups that change (hand-check CSV produced by the evaluation), in
   particular the multi-brand companies: A1, Claranet, Vodafone, Dell, Fujitsu, IBM Cloud,
   Deutsche Telekom, Cogent + Sprint.
2. Decide the thresholds (`large_org_size`, `brand_check_min_orgs`) from those labels.
3. Keep `peeringdb_asn_exclusions` (AS4004) until the shared-website case above is handled.
4. Record the decision and final numbers here and in `CHANGELOG.md` under
   *Changed (results)*.
