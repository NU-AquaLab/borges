# Methodology

The full method is in the [paper](https://doi.org/10.1145/3730567.3732918) ([PDF](https://estcarisimo.github.io/assets/pdf/papers/2025-IMC-borges.pdf)). In short:

- **Organization keys.** Start from organization IDs in CAIDA's AS2Org (WHOIS) and in PeeringDB.
- **Information extraction.** Few-shot prompting of `gpt-4o-mini` (temperature 0) recovers sibling ASNs written in PeeringDB's `notes` and `aka` fields. Earlier work used regular expressions, which confuse phone numbers, years and prefix limits with ASNs.
- **Website inference.**
  - *Redirects*: networks whose PeeringDB websites end at the same final URL are siblings. For example, Limelight (AS22822) and Edgecast (AS15133) both redirect to `www.edg.io`.
  - *Favicons*: websites with the same favicon and the same subdomain are grouped. Remaining shared favicons go to a vision-LLM classifier that separates company logos (Claro Chile and Claro Puerto Rico) from framework defaults (Bootstrap, WordPress).
- **Consolidation.** All signals are merged into organization groups.

Results on the July 2024 PeeringDB and AS2Org snapshots:

| Measure | Result |
|---|---|
| Sibling extraction from `notes`/`aka` (320 records checked by hand) | accuracy 0.947 · precision 0.974 · recall 0.94 |
| Favicon company classifier (449 favicons checked by hand) | accuracy 0.986 · precision 0.997 · recall 0.984 |
| Organization Factor (new metric, 0 to 1) | 0.3576: +7% over AS2Org, +3.3% over as2org+ |
| Users newly attributed to large conglomerates | ≈192 million (≈5% of the Internet population) |

## Known limitations

- **No website history.** There is no longitudinal archive of the websites listed in PeeringDB, so past runs cannot be reproduced exactly from the code alone. Keep your inputs and outputs.
- **PeeringDB coverage is partial.** Registration is voluntary, and entries can be incomplete or outdated. For example, some Microsoft ASNs are missing.
- **Errors in the input propagate.** If a PeeringDB record lists the wrong sibling, Borges faithfully extracts the wrong sibling (e.g. AS10026 listing AS2706).
- **Layered ownership is out of scope.** Groups spanning separate brands and regions are not linked, such as América Móvil's Claro and A1.
- **False merges through weak signals.** One favicon or website shared by unrelated networks can chain them into one group. The optional [merge guard](merge-guard-evaluation.md) prevents most of these. It is off by default until its results are reviewed.
- **Known false bridges** are handled by configuration:
  - AS4004 appears under Orange in PeeringDB but under Sprint in WHOIS. It is listed in `peeringdb_asn_exclusions`.
  - Shared PeeringDB listings and default favicons can wrongly join small networks to large operators. They are covered by blocklists in `config.yaml`.
- **LLM output varies by model.** The published numbers are for `gpt-4o-mini`. Local or newer models need their own validation.
