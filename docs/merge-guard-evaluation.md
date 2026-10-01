# Merge guard: evaluation

The merge guard (`processing.merge_guard` in `config.yaml`) stops individual links from
joining unrelated organizations during consolidation. It is deterministic and **off by
default**. This page records how it was evaluated and how its defaults were chosen, so
the numbers can be reproduced and challenged.

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

| Link | Rule (default) |
|---|---|
| `favicon_match` spanning ≥ 3 WHOIS organizations | Split by **brand**, the first 5 letters of the registrable domain name, or the full name when it starts with a generic word (`inter`, `telec`, …). This is the paper's "same favicon and same domain" rule. |
| `website` | Dropped if it is a lookup service (`rdap.`, `whois.`, `bgp.tools`, …) |
| Unverified weak link (redirect, multi-brand favicon pair) | May not join two **established** organizations (≥ 2 ASNs) alone |
| Extracted relationship from notes (`llm_detected`) | May not join two **large** organizations (≥ 10 ASNs) alone |
| PeeringDB-organization pass | Optional (`peeringdb_tie_guard`, **off**): ignore single-ASN ties where WHOIS disagrees (the AS4004 pattern) |

"Alone" means no link of a *different* type connects the same organizations. WHOIS
organizations and PeeringDB organizations (the paper's OID_W and OID_P) count as such
links (`corroborate_with_peeringdb`, on). Every decision is written to
`data/output/merge_guard_review_<timestamp>.json`. The extraction step itself is
unchanged; only how its output is merged is constrained.

## Method

All runs are offline: no scraping and no LLM calls. `scripts/evaluate_merge_guard.py`
replays `load_data` on a run's input snapshots, attaches the run's exported
`network_groups`, and consolidates with the guard off or on. `--set key=value` overrides
any `merge_guard` setting for threshold sweeps.

Replay fidelity:

- **2025-09-29 run** (PeeringDB 2025-08-01, AS2Org 2025-09-01): with the guard off, 88,297 groups are identical to the original output. All differences come from input drift (5,253 ASNs only in the local snapshots, 250 only in the original run).
- **2025-08-05 and 2025-08-07 runs** (July snapshots): the replays reproduce the original Level3+Orange groups exactly (1,294 and 341 ASNs).

**Labels.** Every group of the 2025-09-29 result that the strictest setting changes (131
groups, 574 pieces) was labeled: which pieces belong to the same company. Settings are
scored on ASN pairs inside these groups:

- **wrong merges:** pairs kept together that belong to different companies;
- **lost merges:** pairs separated that belong to the same company.

The labels are a **draft pending maintainer confirmation**. The sweep only uses looser
settings, so every change is inside the labeled groups.

## Choosing the defaults

| Setting | Wrong merges (pairs / groups) | Lost merges (pairs / groups) |
|---|---|---|
| Guard off | 16,543 / 94 | 0 / 0 |
| All rules, PeeringDB tie rule on | 0 / 0 | 7,196 / 42 |
| + PeeringDB organizations corroborate | 0 / 0 | 5,333 / 41 |
| **+ PeeringDB tie rule off (default)** | **598 / 2** | **2,788 / 34** |

- **The notes rule (large organizations) is kept.** Measured with the tie rule on, removing it lets 4,735 wrong-merge pairs back in to recover 1,799 lost ones. It alone keeps Level3 and Orange apart in the August replays.
- **Counting PeeringDB organizations as corroboration is a pure gain:** fewer lost merges, no new wrong ones.
- **The PeeringDB tie rule is off by default.** On the September run it costs 2,545 real pairs (for example Equinix, HP, Telefônica Brasil) to prevent 598 wrong ones (Vocus + Servers Australia, Lanet + Pitline). It no longer changes the August outcome.
- **Looser thresholds don't help:** `brand_check_min_orgs` 5 or 10, and `established_org_size` 3 or 5, recover few real merges and let wrong ones back in.

## Results with the defaults

**2025-09-29 run** (same inputs, guard off → on, current hand blocklists kept):

| Final groups combining… | Off | On |
|---|---|---|
| ≥ 20 WHOIS organizations | 6 groups, 277 ASNs | 0 |
| ≥ 10 | 33 groups, 1,147 ASNs | 15 groups, 430 ASNs |
| ≥ 5 | 90 groups, 2,188 ASNs | 55 groups, 1,334 ASNs |
| ≥ 2 | 1,227 groups, 9,118 ASNs | 1,184 groups, 8,240 ASNs |

- **Labeled groups:** wrong-merge pairs fall by 96% (16,543 → 598), and 92% of correct pairs are kept.
- **Stay together:** Claro (América Móvil), Leaseweb, Orange, Telstra, Lumen, Verizon, AT&T, Cogent + Sprint wireline.
- **Still lost:**
  - Deutsche Telekom subsidiaries, linked only by AS3320's notes;
  - multi-brand companies linked by one favicon pair: Claranet FR/UK, Rackspace/Datapipe.

**August runs with all hand fixes removed** (no ASN blocklist, no PeeringDB exclusions):
Level3 and Orange stay separate in both. The 1,294-ASN group disappears.

**Remaining gaps:**

- **Sprint + Orange** still merge through AS4004's website (`orange-business.com`). A shared website looks the same whether it's real (Sprint's AS3646 lists `cogentco.com`) or comes from a stale WHOIS record. Keep `peeringdb_asn_exclusions` (AS4004).
- **Lumen absorbed by a large hosting group** in the 2025-08-05 replay: large organizations can still grow by absorbing small ones one at a time.

## Before enabling it by default

1. Confirm the draft labels.
2. Re-score with `score`-style pairwise counts if labels change.
3. Record the decision and final numbers here and in `CHANGELOG.md` under *Changed (results)*.
