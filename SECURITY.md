# Security Policy

## Supported versions

Borges is a research tool maintained on a best-effort basis. Only the latest code on
`main` receives fixes.

| Version | Supported |
| ------- | --------- |
| `main`  | ✅        |
| older tags | ❌     |

## Reporting a vulnerability

Please **do not open a public issue** for security problems.

Use GitHub's private reporting:
[Report a vulnerability](https://github.com/NU-AquaLab/borges/security/advisories/new).

Include a description, reproduction steps, and the commit you used. Expect an
acknowledgment within about two weeks. This is an academic project, so response times
are best-effort. Confirmed issues are fixed in a pull request and disclosed in
`CHANGELOG.md` under *Security* and in a GitHub security advisory that credits the
reporter (unless they prefer otherwise). Please keep details private until the fix is
merged.

## What Borges does

Running the pipeline means Borges will:

- **fetch URLs taken from PeeringDB**: it follows the redirects of thousands of
  third-party websites and downloads their favicons;
- **parse untrusted content**: HTTP responses, and favicon images decoded with Pillow;
- **send data to an LLM provider**: PeeringDB `notes`/`aka` text and favicon images go
  to the configured OpenAI-compatible endpoint (`api.openai.base_url`), using the API
  key in `.env`;
- **write files** under the configured `data/` directories (caches, checkpoints,
  exports).

Run it from a network location where outbound requests to arbitrary hosts are
acceptable, and keep `.env` out of version control. It is already in `.gitignore`.

## Scope

In scope:

- Requests to internal or link-local addresses (SSRF), triggered by crafted PeeringDB
  website fields or redirects.
- Crashes or code execution caused by malicious HTTP responses or images.
- File writes outside the configured data directories (path traversal through URLs or
  cache keys).
- Leaking the API key, for example in logs, exports or error messages.
- Dependency vulnerabilities with a plausible exploitation path in Borges.

Out of scope:

- The content or availability of third-party websites listed in PeeringDB.
- Wrong sibling inferences. These are accuracy issues: please open a regular issue.
- Prompt injection that only changes the LLM's sibling answer for the injecting
  network's own record.
- Findings from automated scanners with no demonstrated impact.

## Automated scanning

Dependabot proposes updates for Python dependencies and GitHub Actions.
