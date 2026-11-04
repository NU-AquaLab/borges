# Learning AS-to-Organization Mappings with Borges

> ...the Cartographers Guilds made a Map of the Empire whose size was that of the Empire, and which coincided point for point with it.
>
> — J.L. Borges. *On Rigor in Science.* (1946)

**Carlos Selmo**, **Esteban Carisimo**, **Fabián E. Bustamante**, **J. Ignacio Alvarez-Hamelin**

!!! warning "Mappings go stale quickly"
    The artifacts below were generated in September 2025. Internet infrastructure changes rapidly: Edgio disappeared between the paper's submission and its presentation. We recommend regenerating the data with the tool. A full run with `gpt-4o-mini` costs about US$3.

## Abstract

Borges is a Python package and CLI tool that maps Autonomous Systems (ASNs) to their parent organizations, revealing the true corporate structure of the Internet. By combining traditional WHOIS data with AI-powered analysis of websites, favicons, and unstructured text, Borges uncovers hidden relationships that existing methods miss.

Our approach achieves 0.947 accuracy in extracting sibling AS relationships and outperforms prior systems by 7% on the Organization Factor metric. This improved mapping reveals approximately 192 million previously uncounted Internet users and enables better understanding of Internet resilience, incident response, and market concentration.

## Key features

- **Multi-source AS detection**: combines PeeringDB, WHOIS, website analysis, and favicon similarity.
- **AI-powered analysis**: uses LLMs for text extraction and computer vision for favicon comparison.
- **Lightweight and repeatable**: designed for regular, inexpensive updates as Internet infrastructure evolves.

Start with [Getting started](getting-started.md), or read the [methodology](methodology.md).

## Downloads

- [Paper PDF](https://nu-aqualab.github.io/borges-website/assets/borges-paper.pdf): full paper as presented at ACM IMC 2025
- [Artifacts (September 2025)](https://nu-aqualab.github.io/borges-website/assets/borges-artifacts-sept25.zip): dataset and supplementary materials
- [GitHub repository](https://github.com/NU-AquaLab/borges): source code

## Long-term usability

To provide long-term usability, Borges also runs on open-weight models on your own GPU, so it keeps working whatever happens to a hosted model. In a full run with Qwen3 8B on a 12 GB GPU, sibling extraction from PeeringDB notes matched `gpt-4o-mini` on 99.4% of rows, using the same PeeringDB and AS2Org snapshots as the artifacts above. The mappings differ mainly in favicon groups, likely because websites changed between runs. With the [merge guard](merge-guard-evaluation.md) on, no group spans 20 or more WHOIS organizations in either mapping.

These numbers measure consistency with the published mapping, not correctness. Read the [evaluation](open-weight-evaluation.md) and the [setup guide](local-llm.md).

## Citation

```bibtex
@inproceedings{borges:imc,
    author = {Carlos Selmo and Esteban Carisimo and
              Fabián E. Bustamante and J. Ignacio Alvarez-Hamelin},
    title = {Learning AS-to-Organization Mappings with Borges},
    booktitle = {Proc. of ACM IMC},
    year = {2025},
    month = {10}
}
```

## Press coverage

- [Mapping Who Really Runs the Internet: Introducing Borges](https://pulse.internetsociety.org/blog/mapping-who-really-runs-the-internet-introducing-borges), *Internet Society Pulse*: how Borges reveals the hidden corporate structure of the Internet and its implications for policy and resilience.
- [Announcement on X](https://twitter.com/estcarisimo/status/1958054727242985660) by Esteban Carisimo.

## About the name

The project is named after Jorge Luis Borges, the Argentine writer whose story "On Rigor in Science" describes cartographers creating a map so detailed it becomes as large as the territory itself. Like Borges's fictional map, our tool aims to create a comprehensive representation of Internet ownership, though in a more manageable format.
