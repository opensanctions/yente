---
description: Plan for producing per-release SBOMs and CRA-adjacent supply-chain artifacts for yente, scanning the built image (no lockfile prerequisite)
date: 2026-05-22
tags: [security, sbom, cra, release-engineering, supply-chain]
---

# SBOM and CRA-adjacent artifacts for yente

## 1. Why

GitHub's auto-generated dependency-graph SBOM has two gaps for our purposes:

1. **Incomplete scope.** It captures direct Python deps from `pyproject.toml` but misses the OS packages we install on top of `python:3.12-slim` (`libicu76`, `ca-certificates`, `curl`, plus the build stage's `build-essential`, `pkg-config`, `locales`, `libicu-dev`) and the contents of the base image itself.
2. **Not frozen per release.** It reflects whatever is on `main` at scan time, not the `ghcr.io/opensanctions/yente:5.4.0` image that customers actually pull. CRA Annex I expects "the SBOM for the version you are running" — not a moving target.

The EU Cyber Resilience Act (in force, with the bulk of manufacturer obligations applying from December 2027) requires an SBOM "at least covering the top-level dependencies", coordinated vulnerability handling, and security/risk documentation. This plan is about being CRA-conformant by the time those obligations bite, and making life easier for downstream customers asking the same questions today.

## 2. Scope

In scope:

- yente Python package as published to PyPI (wheel + sdist)
- The container image at `ghcr.io/opensanctions/yente`, both `linux/amd64` and `linux/arm64`
- Build-time and runtime OS packages inside that image
- Direct and transitive Python dependencies, including first-party OpenSanctions deps (`nomenklatura`, `followthemoney`, `rigour`, `normality`, `fingerprints`, `countrynames`)
- Native code linked via `pyicu`, `cryptography`, `aiohttp[speedups]`, `httpx[http2]`, `orjson`

Out of scope:

- The Elasticsearch / OpenSearch / S3 backends in `docker-compose.yml`. Operator-supplied infrastructure, not part of the yente product. Document supported versions; don't claim SBOM coverage.
- Customer-supplied data and manifests.
- **Build reproducibility / lockfiles.** Explored and shelved as a separate concern — see §8. The SBOM describes what shipped regardless of whether the resolution was reproducible.

## 3. Artifact set

Per release, attached to the GitHub Release and pushed to the registry as OCI referrers.

### 3.1 SBOM (primary)

Generate the SBOM from the **built artifact**, not from `pyproject.toml`. The installed venv inside the image already has exact pinned versions for every transitive dep — that's the source of truth. The SBOM describes what shipped; we don't need a lockfile to know what shipped.

- **Format:** CycloneDX 1.6 JSON as the primary format; also emit SPDX 2.3 JSON for procurement / ISO-preferring downstream users. Both are CRA-acceptable.
- **Tooling:** `syft <image-digest> -o cyclonedx-json -o spdx-json` for the container image. Multi-ecosystem in one pass — catches apt + the installed Python venv + base-image layers.
- **Per-platform:** generate one SBOM per arch (amd64 + arm64). Two SBOMs describe two different sets of bytes; merging them would be a lie.
- **Wheel/sdist SBOM:** murky territory. A Python wheel is a building block, not a runtime — its real dep set depends on the resolver and environment at `pip install` time. Two options:
  - (a) Don't publish a wheel-level SBOM. Let downstream users generate their own per-environment SBOM after install. The image SBOM is the high-value artifact.
  - (b) Publish a "top-level only" SBOM listing the deps declared in `pyproject.toml`, with an explicit note that transitive coverage is environment-dependent.
  - Recommendation: start with (a). Revisit if procurement asks for (b).
- Scans run against the *built* artifact, not the source tree. What we ship is what we describe.

### 3.2 VEX (Vulnerability Exploitability eXchange)

A published, version-controlled answer sheet of "yes / no / not yet decided" for CVEs flagged against our SBOM. VEX does not detect anything — the scanner detects, the VEX suppresses noise the scanner would otherwise produce.

- **Format:** OpenVEX (single `security/vex/openvex.json`) or CycloneDX VEX. Functionally equivalent at our scale. OpenVEX has a dedicated `vexctl` CLI; CycloneDX VEX pairs natively with our SBOM document family. Lean OpenVEX unless we hit a tooling wall.
- **Producer-side detection loop:** weekly CI cron runs `grype ghcr.io/opensanctions/yente:latest --vex security/vex/openvex.json`. New findings (not covered by VEX) open a GitHub issue. Triage decides one of:
  - `not_affected` + standard justification code (most commonly `vulnerable_code_not_in_execute_path`; others include `inline_mitigation_already_exists`, `vulnerable_code_cannot_be_controlled_by_adversary`, `vulnerable_code_not_present`)
  - `affected` + planned response (typically: bump the dep)
  - `under_investigation` if undecided today (acceptable short-term state; scanners will keep reporting)
- **Distribution to customers:** attach the VEX as an OCI referrer alongside the SBOM via `cosign attest --type openvex --predicate openvex.json <image-digest>`. Customer scanners that respect attached VEX (grype, trivy) auto-discover it and apply the same suppression we do — they see the same noise-filtered view, and stop emailing us about CVEs we've already triaged.
- **Triage cadence:** realistically zero entries most weeks; 1–3 when something fires; 10+ during a news-cycle event (log4shell-style). Steady-state well under an hour per month, dominated by the news-cycle events.
- **Version scoping is the failure mode to watch.** A VEX entry saying "yente 5.4 doesn't reach the vulnerable path" can become wrong in 5.5 if we start calling that function. Scope entries with version ranges (the `affects` blocks support this) and re-triage existing entries on each minor release.

### 3.3 Build provenance (SLSA)

- `actions/attest@v4` (not `actions/attest-build-provenance` — that's now a wrapper around `actions/attest@v4`; new code should call the wrapped action directly) produces SLSA v1 provenance.
  - Wheel: `subject-path: dist/*.whl`
  - Image: `subject-name: ghcr.io/opensanctions/yente` + `subject-digest: ${{ steps.push.outputs.digest }}` from the existing `docker/build-push-action@v7` step, `push-to-registry: true`.
- **New workflow permissions required** in `build.yml`: currently `packages: write` + `id-token: write`; `actions/attest@v4` also needs `attestations: write` + `artifact-metadata: write`.
- Provenance answers "was this artifact built from this commit by this workflow?" — CRA's "secure by design" expectation, and protection against compromised release pipelines.

### 3.4 Signing

- Container images and SBOMs signed via `cosign` (keyless / Sigstore — OIDC against the GitHub Actions identity, no key management).
  - Image: `cosign sign <image-digest>`
  - SBOM-as-attestation: `cosign attest --type cyclonedx --predicate sbom.cdx.json <image-digest>` (and `--type spdx`, `--type openvex` for the other artifacts). Cosign's built-in `--type` values cover `cyclonedx`, `spdx`, `spdxjson`, `slsaprovenance1`, `vuln`, `openvex` — so each artifact gets a correctly-typed attestation.
- **PyPI / PEP 740 attestations are already engaged.** `pypa/gh-action-pypi-publish` ≥ v1.11.0 produces and uploads PEP 740 attestations by default for any project using Trusted Publishing. The existing `build.yml` publish step is tokenless (Trusted Publishing), so attestations are already happening with no workflow change required. Verify the attestation badge on the next tag release.

### 3.5 License inventory

A `LICENSES.md` derived from the SBOM, attached per release. CRA doesn't mandate this, but customer procurement will ask. Cheap to generate from `syft` output (small `jq` filter, or `cyclonedx-py` license report).

### 3.6 CRA companion documentation

Not SBOM artifacts but the documents CRA assumes you have alongside. Same workstream because the audience overlaps:

- **Expanded vulnerability disclosure policy.** Current `SECURITY.md` is two sentences. CRA wants a coordinated disclosure policy with response timelines.
- **Support / update window.** Declare which yente major versions receive security updates and for how long. CRA's "expected product lifetime".
- **CSAF advisories.** When we issue a security fix, publish a CSAF 2.0 advisory alongside the GitHub Security Advisory. CSAF is the format the EU CSIRT network speaks under CRA's coordinated-disclosure machinery.
- **Risk assessment / threat model.** A one-pager covering trust boundaries (the API, the index backend, the delivery token, manifest loading from S3). CRA Annex I requires the manufacturer to have performed one; publishing is not required, but having it written down means we can hand it to an auditor.
- **EU Declaration of Conformity.** Required at point of CE-marking. Drafted closer to the 2027 deadline against final CRA implementing acts.

## 4. Pipeline integration

All of this runs on **tag pushes only**. PR / `main` pushes keep today's behaviour — we don't publish supply-chain artifacts for every commit.

Changes to `.github/workflows/build.yml`:

1. After `docker/build-push-action@v7` pushes the image, run `syft <image-digest> -o cyclonedx-json=sbom.cdx.json -o spdx-json=sbom.spdx.json` per platform.
2. `cosign sign <image-digest>` and `cosign attest --type cyclonedx --predicate sbom.cdx.json <image-digest>` (also for SPDX, and for VEX). All keyless via Sigstore.
3. `actions/attest@v4` for both the wheel digest and the image digest. Add the `attestations: write` + `artifact-metadata: write` permissions to the workflow.
4. Generate `LICENSES.md` from the SBOM (small `jq` over `sbom.cdx.json`).
5. Verify PEP 740 PyPI attestations engage on next tag release. No workflow change required.
6. `softprops/action-gh-release@v2` creates the GitHub Release on tag push and attaches the artifact bundle (wheel, sdist, SBOMs per arch, VEX file, `LICENSES.md`, provenance bundle, plus a single `yente-<version>-supply-chain.zip` for one-click download). Use `generate_release_notes: true` to preserve the auto-changelog. **This replaces the current manual Release-creation flow.**

Separate weekly workflow (not gated on tags) for VEX maintenance: cron runs `grype ghcr.io/opensanctions/yente:latest --vex security/vex/openvex.json --fail-on high`. Non-zero exit → open or update a GitHub issue listing un-VEX'd findings.

## 5. Storage and distribution

- **Primary:** GitHub Release page, created and populated by CI on tag push (`softprops/action-gh-release@v2`). Individual artifact files attached directly so they're greppable / linkable; a single zip bundle alongside for one-click procurement download.
- **Secondary:** OCI registry — SBOMs, VEX, provenance, and signatures attached as referrers to the image digest via `cosign`. This is what `trivy` / `grype` / customer scanners look for automatically.
- **PyPI:** PEP 740 attestation only. Don't try to push SBOMs into PyPI metadata.
- Discoverability: link the latest release's artifacts from `SECURITY.md` and from the docs site.

## 6. Open questions

- **OpenVEX vs CycloneDX VEX.** Lean OpenVEX for simplicity (`vexctl` workflow, smaller schema) unless we hit a tooling wall.
- **Wheel SBOM strategy** (per §3.1) — start with no wheel SBOM and revisit if customers ask.
- **First-party deps (`nomenklatura` etc.) and their own SBOMs.** For CRA purposes yente is the product placed on the EU market, so yente's SBOM is what matters. Upstream packages need clean SPDX license metadata; per-release SBOMs would be nice but aren't blocking. Separate workstream.
- **Container hardening adjacent to CRA "secure by default"** — distroless base, drop `curl` from runtime, pin base image by digest. Not in this workstream.

## 7. Suggested phasing

1. **SBOM emission (medium):** add `syft` to the release workflow, emit CycloneDX + SPDX per platform, attach to GitHub Release and OCI referrers. **Foundation of everything else.**
2. **Signing + provenance (small):** `cosign sign` + `cosign attest` + `actions/attest@v4`. Cheap once the workflow already has the right permissions.
3. **GitHub Release automation (small):** `softprops/action-gh-release@v2` replaces manual Release creation; bundles the artifacts.
4. **VEX scaffolding (small):** empty `security/vex/openvex.json`, weekly grype cron, document the triage process. Populate as findings come in.
5. **Documentation pass (medium, mostly writing):** expand `SECURITY.md`, write the support window statement, draft the threat model, set up CSAF publishing.
6. **EU DoC + conformity assessment:** revisit in 2027 against final CRA implementing acts.

Steps 1–3 give us a defensible "yes, we publish per-release SBOMs, signed, with provenance" answer to procurement and to CRA market-surveillance authorities. Step 4 reduces ongoing customer-question burden. Step 5 covers CRA companion docs.

## 8. Related but out-of-scope: build reproducibility / lockfiles

Initially treated as a prerequisite for the SBOM; revisited and dropped. The SBOM describes what shipped (via scanning the built image's installed venv) regardless of whether the resolution was reproducible — the lockfile and the SBOM solve different problems.

A lockfile (PEP 751 `pylock.toml`) is still genuinely useful for other reasons:

- Reproducible rebuilds for forensics
- Stable security-review surface (review once, ship many times)
- Faster CI installs
- Dependabot signal on transitive bumps

…but it's expensive to get right end-to-end at yente's scope. The exploratory work on the `sbom-cra-artifacts` branch found three real costs that aren't worth paying just to enable SBOMs:

- **Multi-arch lockfiles.** PEP 751 is single-platform per file. The amd64 + arm64 buildx flow needs per-arch lockfiles, picked via `TARGETPLATFORM` in the Dockerfile, regenerated together on every dep bump (arm64 regen requires qemu emulation, ~10× slowdown — or use uv as the resolver).
- **Dependency group splitting.** `pip install -r pylock.toml` doesn't yet support extras / dep groups. We'd need `pylock.toml` (runtime) + `pylock.dev.toml` (CI extras) per the PEP's split-lockfile convention.
- **Scanner-tooling gaps.** Neither `syft` nor `cyclonedx-py` parse `pylock.toml` today (only `pip-audit` does). The natural workaround — scan the installed venv instead of the lockfile — is what we're doing for the SBOM anyway, which obviates the lockfile's main SBOM rationale.

If build reproducibility becomes a priority on its own merits, pick this up as a standalone effort with the costs understood up front. Nothing in the SBOM workstream depends on it.
