---
description: SBOM and CRA-adjacent artifacts for yente — current state plus remaining work
date: 2026-05-22
tags: [security, sbom, cra, release-engineering, supply-chain]
---

# SBOM and CRA-adjacent artifacts for yente

## 1. Why

GitHub's auto-generated dependency-graph SBOM has two gaps for our purposes:

1. **Incomplete scope.** It captures direct Python deps from `pyproject.toml` but misses the OS packages we install on top of `python:3.12-slim` and the contents of the base image itself.
2. **Not frozen per release.** It reflects whatever is on `main` at scan time, not the `ghcr.io/opensanctions/yente:5.4.0` image customers actually pull. CRA Annex I expects "the SBOM for the version you are running" — not a moving target.

The EU Cyber Resilience Act (in force, with the bulk of manufacturer obligations applying from December 2027) requires an SBOM "at least covering the top-level dependencies", coordinated vulnerability handling, and security/risk documentation. This plan is about being CRA-conformant by the time those obligations bite, and reducing the customer-question burden today.

## 2. Scope

In scope:

- The container image at `ghcr.io/opensanctions/yente` (both arches) and the PyPI wheel + sdist
- Build-time and runtime OS packages inside the image
- Direct and transitive Python dependencies, native code linked via `pyicu`, `cryptography`, `aiohttp[speedups]`, `httpx[http2]`, `orjson`

Out of scope:

- Elasticsearch / OpenSearch / S3 backends in `docker-compose.yml` — operator-supplied infrastructure
- Customer-supplied data and manifests
- **Build reproducibility / lockfiles** — explored separately, deprioritised; see §8

## 3. Artifact set

### 3.1 SBOMs — implemented

`syft` scans the published image per platform on tag push, emitting CycloneDX 1.6 + SPDX 2.3 JSON to a workflow artifact and as cosign attestations on each platform's image digest.

- **Why scan the built image, not the lockfile or pyproject.** The installed venv inside the image is the bit-exact record of what shipped. Lockfiles describe what *would* be installed; the SBOM should describe what *was* installed.
- **Why per-platform.** A multi-arch image is two different sets of bytes; merging into one SBOM would lie about which file ships where.
- **Why both CycloneDX and SPDX.** Customer procurement asks for one or the other (occasionally both). Emitting both is free.
- **Wheel SBOM deliberately skipped.** A Python wheel's transitive set depends on the install-time environment, so a per-release wheel SBOM is a snapshot of one moment rather than a property of the wheel. Revisit if customers ask.

### 3.2 VEX — implemented

`security/vex/openvex.json` (OpenVEX 0.2.0, empty statements) is committed. A weekly grype cron (`.github/workflows/security-scan.yml`) scans the latest published image with `--vex` applied and opens or updates a single tracking issue on un-suppressed high-severity findings. The release workflow `cosign attest`s the VEX at the manifest-list digest.

- **Why publish a VEX at all.** Without it, every customer scanner re-reports the same false positives, and every customer security team independently asks "are you affected by CVE-X?". The VEX is the published, version-controlled answer — for both their scanner and ours.
- **Why OpenVEX over CycloneDX VEX.** Simpler schema, dedicated `vexctl` CLI, no functional cost.
- **Why a cron rather than per-PR scan.** Vulnerability databases update continuously; new CVEs land independently of our commits. A cron catches drift that the PR loop wouldn't.
- **Why attach at manifest-list level (not per-platform).** VEX statements are scoped by package + CVE, which are the same across arches.
- **Operational note.** Triage is realistically zero entries most weeks, 1–3 when something fires, 10+ during a news-cycle event. Scope statements with version ranges and re-triage on each minor release; the failure mode is "still says not_affected after 5.5 starts reaching the vulnerable path".

### 3.3 Build provenance (SLSA) — implemented

`actions/attest-build-provenance@v4` emits SLSA v1 provenance for the wheel before PyPI publication and for the image manifest-list digest (pushed to GHCR as an OCI referrer).

- **Why both surfaces (GitHub attestations + OCI referrer).** The GitHub-side attestation store powers `gh attestation verify` and the repo's Attestations tab; OCI referrers power container scanners. Different consumers, both needed.
- **Why the wrapper action, not raw `actions/attest`.** `attest-build-provenance` auto-generates the SLSA predicate body from the workflow context; `actions/attest` would require hand-constructing it.

### 3.4 Signing — implemented

`cosign sign --recursive` over the manifest list (covers list + both per-platform images), `cosign attest` for each SBOM and the VEX. All keyless via Sigstore against the GitHub OIDC identity. The wheel carries PEP 740 attestations automatically via the existing tokenless `pypa/gh-action-pypi-publish` step.

- **Why keyless / Sigstore.** No long-lived signing keys to rotate, lose, or compromise. The trust root is "built by this GitHub Actions workflow at this commit", which is what we actually want to attest.
- **Why `--recursive`.** A multi-arch tag's manifest list and per-platform images each need their own signature; `--recursive` does both in one call.

### 3.5 License inventory — TBD

`LICENSES.md` derived from the SBOM, attached per release. Cheap to generate (`jq` over `sbom.cdx.json`); deferred until it can land alongside the GitHub Release automation that bundles it.

### 3.6 CRA companion documentation — TBD

Not SBOM artifacts but documents CRA assumes you have alongside:

- **Expanded `SECURITY.md`** — current file is two sentences; CRA expects coordinated disclosure timelines.
- **Support / update window statement** — which yente major versions get security updates and for how long.
- **CSAF 2.0 advisories** — the format the EU CSIRT network speaks under CRA's coordinated-disclosure machinery; publish alongside the existing GitHub Security Advisory when issuing security fixes.
- **Risk assessment / threat model** — one-pager covering trust boundaries (API, index backend, delivery token, manifest loading from S3). CRA Annex I requires having one; publishing it isn't required.
- **EU Declaration of Conformity** — drafted closer to the 2027 deadline against final CRA implementing acts.

## 4. Remaining pipeline work

Done: SBOM emission, signing, cosign + GitHub attestations on tag push; weekly VEX-aware grype cron.

Open:

1. **GitHub Release automation.** Replace manual `gh release create` with `softprops/action-gh-release@v2` on tag push. Attach the artifact bundle (wheel, sdist, both SBOMs per arch, VEX, `LICENSES.md`, provenance). `generate_release_notes: true` preserves the auto-changelog from manual creation.
2. **`LICENSES.md` generation.** Small step in the release workflow before the Release-creation step; derive from `sbom.cdx.json`.
3. **PEP 740 attestation verification on the first real tag release.** No workflow change — open the published PyPI page and confirm the attestation badge appears.

## 5. Storage and distribution

- **OCI registry** — SBOMs, VEX, provenance, and signatures attached as referrers to image digests via `cosign`. Customer scanners auto-discover.
- **GitHub Release page** — currently manual; once §4.1 lands, CI-populated with the artifact bundle plus a one-click `yente-<version>-supply-chain.zip`.
- **PyPI** — PEP 740 attestation only.

## 6. Open questions

- **Wheel SBOM strategy** (§3.1) — currently not publishing one. Revisit if customers ask.
- **Container hardening** — distroless base, drop `curl` from runtime, pin base image by digest. Not on this plan's critical path.
- **First-party OpenSanctions deps' own SBOMs** — yente's SBOM is what matters for CRA, but `nomenklatura`, `followthemoney`, etc. would benefit from per-release SBOMs of their own. Separate workstream.

## 7. Remaining phases

1. **GitHub Release automation + `LICENSES.md`** (small) — finishes the release-artifact pipeline.
2. **Documentation pass** (medium, mostly writing) — expand `SECURITY.md`, support-window statement, CSAF process, threat model.
3. **EU Declaration of Conformity** — revisit in 2027 against final CRA implementing acts.

Steps 1–2 close the loop on producer-side burden reduction. Step 3 is on the calendar, not the to-do list.

## 8. Build reproducibility / lockfiles — out of scope

Initially treated as a prerequisite, revisited and dropped. The SBOM describes what shipped (via scanning the built image's installed venv) regardless of whether the resolution was reproducible. Lockfile and SBOM solve different problems.

A lockfile (PEP 751 `pylock.toml`) is still useful for forensic rebuilds, stable security-review surfaces, faster CI, and Dependabot transitive-bump signal — but it's expensive to get right end-to-end at yente's scope. The exploratory work on the `sbom-cra-artifacts` branch (#1147) found three real costs:

- **Multi-arch lockfiles.** PEP 751 is single-platform per file. The amd64 + arm64 buildx flow needs per-arch lockfiles, regenerated together on every dep bump (arm64 regen requires qemu, ~10× slowdown — unless we use uv as the resolver).
- **Dependency-group splitting.** `pip install -r pylock.toml` doesn't support extras / dep groups, so the dev/docs sets need separate `pylock.dev.toml` files per the PEP's convention.
- **Scanner-tooling gaps.** Neither `syft` nor `cyclonedx-py` parses `pylock.toml` today. The workaround — scan the installed venv — is what we do for the SBOM anyway, which obviates the lockfile's main SBOM rationale.

Pick this up as a standalone effort if build reproducibility becomes a priority on its own merits. Nothing in the SBOM workstream depends on it.
