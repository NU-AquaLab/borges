# Open-weight models: evaluation

Open-weight models provide long-term usability for Borges: the pipeline keeps working
with models whose weights are public and can be pinned, whatever happens to a hosted
model. This page records one full run on open-weight models and how it compares with
the published September 2025 artifacts, which used `gpt-4o-mini`, so the numbers can be
reproduced and challenged. Setup instructions are in [local-llm.md](local-llm.md).

**Summary.** On sibling extraction from PeeringDB `notes` and `aka`, Qwen3 8B gives the
same answer as `gpt-4o-mini` on 99.4% of the rows. The large differences between the
two mappings come from favicons, whose hashes changed over the 14 months between the
runs. They do not appear to depend on the model. With the merge guard on, the two
mappings keep 99.92% and 99.83% of each other's sibling pairs.

There is no ground truth here. These numbers measure consistency with the published
mapping, not correctness.

## Setup

| | Published artifacts | This run |
|---|---|---|
| Date | 2025-09-29 | 2026-11-04 |
| Text model | `gpt-4o-mini` | `qwen3:8b` |
| Vision model | `gpt-4o-mini` | `qwen3-vl:8b-instruct` |
| Server | OpenAI API | Ollama 0.40.14, 4 parallel requests |
| Hardware | | NVIDIA TITAN Xp, 12 GB (Pascal) |
| PeeringDB, AS2Org | 2025-08-01, 2025-09-01 | the same files |
| Websites and favicons | scraped September 2025 | scraped November 2026 |

The exported `autonomous_systems` tables are identical: the same 119,440 ASNs with the
same organization, name, website, notes and aka. Merge guard off in both runs.

```yaml
api:
  llm:
    base_url: http://127.0.0.1:11434/v1
    model: qwen3:8b
    vision_model: qwen3-vl:8b-instruct
    temperature: 0.0
    seed: 0
    timeout: 300
    max_retries: 2
    max_concurrency: 4
    structured_output: true
    extra_body:
      reasoning_effort: none   # turn off thinking
    cache_dir: ./data/raw/llm_cache
```

The run also set `max_tokens: 256`, which had no effect (see [Limitations](#limitations)).

## Method

Consolidation code changed after the artifacts were published, so a direct comparison
mixes code changes with signal changes. Replaying the published `network_groups` with
the current code gives 88,297 groups identical to the published ones, 227 only in the
published output and 5,298 only in the replay. The replay assigns 119,260 ASNs to groups;
the published output has 112,350, with 4,953 additional repeated memberships.

So every comparison below is between replays with the same code, using
`scripts/evaluate_merge_guard.py` (offline: no scraping, no LLM calls). The baseline is
the published `network_groups`. Each row replaces only the named group types with those
from this run. Replaying all of this run's groups reproduces its exported partition
exactly (93,443 of 93,443 groups).

## Results

| Replaced in the baseline | Published pairs kept | Run pairs also in published | ASNs in an identical group |
|---|---:|---:|---:|
| `llm_detected` (text) | 100.00% | 99.99% | 99.92% |
| `redirect_target` | 100.00% | 100.00% | 100.00% |
| `favicon_match` | 99.12% | 96.03% | 97.10% |
| **All** | **99.12%** | **96.01%** | **97.05%** |
| **All, merge guard on in both** | **99.92%** | **99.83%** | **98.88%** |

A pair is two ASNs in the same group. Pair counts weigh large groups heavily, which is
why the last column is included.

| Groups spanning WHOIS organizations | Published groups, replayed | This run |
|---|---:|---:|
| ≥ 20 organizations | 6 (277 ASNs) | 9 (674 ASNs) |
| ≥ 50 organizations | 0 | 2 (276 ASNs) |
| ≥ 20 organizations, merge guard on | 0 | 0 |

## Sibling extraction

Qwen3 and `gpt-4o-mini` agree on 2,977 of the 2,994 rows sent to the LLM. Over all
rows, 1,222 directed pairs are shared, 12 appear only with `gpt-4o-mini` and 28 only
with Qwen3. Reading the notes of the 17 differing rows by hand:

| Answer only from | Pairs | Rows | Reading | Examples |
|---|---:|---:|---|---|
| `gpt-4o-mini` | 12 | 6 | All look correct: Qwen3 misses | AS6871 Plusnet → AS2856 BT; AS19834 and AS26762 Conversant |
| Qwen3 | 26 | 9 | Look correct: `gpt-4o-mini` misses | AS2828 XO → AS701 Verizon; AS40027 → AS2906 Netflix; AS28263 Ensite and AS262272 FIT ("We manage ASN ...") |
| Qwen3 | 2 | 2 | Doubtful | AS17036 Fiberdash → AS20055 ("downstream of"); AS34135 Renesys → AS64597 (a private ASN) |

This reading is a manual judgment on a small sample, not an accuracy measurement, and
the variation between two `gpt-4o-mini` runs was not measured.

One request failed after its retries timed out, very likely AS57706, whose note has
110,583 characters. That ASN has no extracted relationships in the published output
either.

With thinking on, requests took 15 to 30 seconds and some ran until the 5-minute server
limit. With thinking off, a 40-row test took 0.87 seconds per row with no errors.

## Favicons

**The vision answer rarely removes a group.** `FaviconAnalysisStage` keeps a favicon
group if the answer contains any of a list of phrases, including "same organization"
and "belong to". An answer such as "REJECT — NOT THE SAME ORGANIZATION" therefore
keeps the group. In this run, 1,904 of 1,915 cached vision answers pass the check, and
about 1,250 of those also contain rejection language (keyword count, approximate). In
the published artifacts, 99.6% of the 479 stored favicon analyses pass the same check.
This is existing behavior, not specific to Qwen3.

**Favicon hashes changed.** Only 532 of the 1,663 `favicon_match` hashes in the published
artifacts are also in this run. Website changes are the likely cause, but this run cannot
separate them from the different scraping location.

**New false groups appear.** One favicon shared by 123 ASNs, including a biotech company,
a university system and LINX route collectors, produces a 180-ASN group spanning 123
WHOIS organizations that joins LINX, TELUS, Ziply and Zenlayer. Hash blocklists cannot
anticipate this, which is the case for the merge guard (see
[merge-guard-evaluation.md](merge-guard-evaluation.md)).

## Runtime

| Stage | Time | Requests |
|---|---:|---:|
| `redirect_scraping` | 2 h 13 min | 25,953 websites |
| `as_detection` | 37 min | 2,994 |
| `favicon_download` | 32 min | |
| `favicon_analysis` | 6 h 8 min | 1,915 vision answers cached |
| Other stages | under 1 min | |
| **Total** | **9 h 32 min** | |

## Limitations

- One model on one snapshot. GLM and Kimi do not fit in 12 GB and were not run.
- The published mapping is the reference, not the truth.
- Text and favicon effects are separated, but website change and the vision model are
  not: both are inside the `favicon_match` row.
- `max_tokens` is not enforced on this setup. The client sends `max_completion_tokens`,
  and with a limit of 256, vision answers reached 30,592 characters. Rely on `timeout`
  with Ollama.
- The run used commit `0d7422c` plus the pandas 3 fix for `has_asn_reference`.

## Reproducing

1. Start Ollama with `OLLAMA_NUM_PARALLEL=4` and `OLLAMA_CONTEXT_LENGTH=8192`, and pull
   both models.
2. Download the inputs and run the pipeline with the configuration above:

   ```bash
   borges download --peeringdb-date 2025-08-01 --as2org-date 2025-09-01
   borges pipeline run
   ```

3. Build a `network_groups` parquet that takes the chosen `group_type` rows from this
   run and the others from the
   [published artifacts](https://nu-aqualab.github.io/borges-website/assets/borges-artifacts-sept25.zip),
   then replay it, with `--guard on` for the guarded row:

   ```bash
   python scripts/evaluate_merge_guard.py \
       --config config.yaml \
       --peeringdb data/input/peeringdb_2_dump_2025_08_01.json \
       --whois data/input/20250901.as-org2info.txt \
       --network-groups hybrid_network_groups.parquet \
       --guard off --out /tmp/eval
   ```

4. Compare the `groups.parquet` of two replays.
