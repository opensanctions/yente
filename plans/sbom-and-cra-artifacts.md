---
description: Plan for producing a per-release SBOM and CRA-adjacent supply-chain artifacts for yente
date: 2026-05-22
tags: [security, sbom, cra, release-engineering, supply-chain]
---

# SBOM and CRA-adjacent artifacts for yente

## 1. Why

GitHub's auto-generated dependency graph SBOM gives us a rough view of direct Python dependencies on `main`, but it is insufficient for two reasons:

1. **Incomplete scope.** It misses the OS packages we install on top of `python:3.12-slim` (`libicu76`, `ca-certificates`, `curl`, plus build-stage `build-essential`, `pkg-config`, `locales`, `libicu-dev`), the contents of the base image itself, and transitive Python dependencies that aren't captured because we have no lockfile.
2. **Not frozen per release.** It reflects whatever is on `main` at scan time, not what shipped as `ghcr.io/opensanctions/yente:5.4.0` or `yente==5.4.0` on PyPI. CRA Article 13 / Annex I expects the manufacturer to be able to point a user at "the SBOM for the version you are running" — not a moving target.

The EU Cyber Resilience Act (in force, with the bulk of manufacturer obligations applying from December 2027) requires, among other things, an SBOM "at least covering the top-level dependencies", coordinated vulnerability handling, and security/risk documentation. We want yente to be CRA-conformant when those obligations bite, and to make life easier for downstream customers who will be asked the same questions sooner.

## 2. Scope

In scope for the artifact set:

- yente Python package as published to PyPI (the wheel and sdist).
- The container image published to `ghcr.io/opensanctions/yente` for both `linux/amd64` and `linux/arm64`.
- Build-time and runtime OS packages inside that image.
- Direct and transitive Python dependencies, including first-party OpenSanctions deps (`nomenklatura`, `followthemoney`, `rigour`, `normality`, `fingerprints`, `countrynames`).
- Native code linked via `pyicu`, `cryptography`, `aiohttp[speedups]`, `httpx[http2]`, `orjson`.

Out of scope (or: scope of a different SBOM):

- The Elasticsearch / OpenSearch / S3 backends referenced in `docker-compose.yml`. These are operator-supplied infrastructure, not part of the yente product. We should *document* the supported versions but not claim to ship an SBOM for them.
- Customer-supplied data and manifests.

## 3. Artifact set

Per release, attached to the GitHub release and (where it makes sense) pushed to the registry as OCI referrers.

### 3.1 SBOM (primary)

- **Format:** CycloneDX 1.6 JSON as the primary format; also emit SPDX 2.3 JSON for procurement/ISO-preferring downstream users. Both are CRA-acceptable.
- **Scope:** one SBOM per artifact (wheel, sdist, amd64 image, arm64 image). Don't try to merge — different artifacts, different bill.
- **Tooling — important gap to design around:** neither `syft` nor `cyclonedx-py` parses `pylock.toml` as of May 2026. `syft`'s Python catalogers cover `pip` METADATA, `poetry.lock`, `Pipfile.lock`, and (since v1.29.0) `uv.lock`. `cyclonedx-py` supports `environment` / `requirements` / `pipenv` / `poetry` subcommands. Only `pip-audit` ≥ 2.9.0 reads `pylock.toml` natively, and it's a vuln scanner, not an SBOM generator.
- **Workaround for the lockfile gap:** generate the SBOM from the *installed environment*, not from `pylock.toml`. Since we install from the lockfile, the venv contents already reflect it bit-for-bit.
  - Container image: `syft <image-ref> -o cyclonedx-json -o spdx-json` (multi-ecosystem — catches apt + the installed Python venv + base image in one pass).
  - Wheel/sdist: install into a throwaway venv from `pylock.toml`, then `cyclonedx-py environment <venv-python> -o sbom.cdx.json`. Or use `uv export --format cyclonedx` against the same lockfile if uv's native exporter gives cleaner output.
  - All scans run against the *built* artifact, not the source tree — so what we ship is what we describe.
- **Revisit once tooling catches up.** Both `syft` and `cyclonedx-py` will almost certainly add `pylock.toml` catalogers given pip-audit already has one. Track and switch when it lands.
- **Lockfile prerequisite.** To get reliable transitive coverage we need a lockfile. Use **PEP 751 `pylock.toml`** — it's the standardised format (accepted March 2025), tool-neutral, and consumable by pip directly. Concretely:
  - Generate with `pip lock` (pip ≥ 25.1, April 2025). Install with `pip install -r pylock.toml` (pip ≥ 26.1, April 2026). Both sides are still marked **experimental** in pip, so pin pip's version explicitly in CI and Docker — don't ride `pip install -U pip`.
  - **Known constraints we have to design around:**
    - The lockfile is **single-platform / single-Python**: valid only for the interpreter and OS that produced it. We run `linux/amd64` and `linux/arm64`; either lock for the target platform inside a multi-platform build, or accept that the lockfile pins one arch and verify the other arch via SBOM diffing.
    - `pip install -r pylock.toml` does **not yet support extras or dependency groups**. Our `dev` and `docs` groups can't be installed from a single `pylock.toml`. PEP 751 anticipates this — additional lockfiles are named `pylock.<name>.toml` (e.g. `pylock.dev.toml`). Commit `pylock.toml` for runtime and `pylock.dev.toml` for the test/CI set.
  - The Dockerfile switches from `pip install --no-cache-dir -e /app` to `pip install --no-cache-dir -r pylock.toml` followed by a separate `pip install --no-deps -e /app` to install yente itself without re-resolving. CI's `test-*` jobs install both `pylock.toml` and `pylock.dev.toml`.
  - Pinning pip itself (and the base image by digest) closes the last "what produced this set of bytes" gap.
  - `uv export --format pylock.toml` is the fallback resolver if pip's experimental resolver gives us trouble. The lockfile format is the same either way.

### 3.2 VEX (Vulnerability Exploitability eXchange)

Once we have an SBOM, downstream scanners *will* report CVEs against transitive dependencies that don't actually affect yente (classic example: a CVE in a code path we never call). A VEX document lets us declare, per CVE, whether the product is `affected`, `not_affected`, `fixed`, or `under_investigation`, with a justification.

- **Format:** CycloneDX VEX (lives alongside the CycloneDX SBOM) or OpenVEX. CycloneDX VEX is the lighter integration since we're already on CycloneDX.
- **Process:** start empty. Populate as scanners flag noise. Maintain in-repo as `security/vex/` so history is visible.
- This is the artifact that most directly reduces support burden — without it, every customer security team re-asks the same question.

### 3.3 Build provenance (SLSA)

- Use `actions/attest@v4` directly (not `actions/attest-build-provenance` — v4+ of that is now a thin wrapper) to produce SLSA v1 provenance for the wheel and the container image.
- **Permissions to add to `build.yml`:** currently `packages: write` and `id-token: write` are granted; `actions/attest@v4` also needs `attestations: write` and `artifact-metadata: write`.
- Wheel: `subject-path: dist/*.whl`. Image: `subject-name: ghcr.io/opensanctions/yente`, `subject-digest: ${{ steps.push.outputs.digest }}`, `push-to-registry: true` — uses the digest output from the existing `docker/build-push-action@v7` step.
- Provenance answers "was this artifact built from this commit, by this workflow?" which CRA expects under the "secure by design" pillar and which protects against compromised release pipelines.

### 3.4 Artifact signing

- Sign container images and SBOMs with `cosign` (keyless / Sigstore — OIDC against the GitHub Actions identity, no key management). For SBOMs as attestations: `cosign attest --type cyclonedx --predicate sbom.cdx.json <image-digest>`. `--type` accepts `cyclonedx`, `spdx`, `spdxjson`, `slsaprovenance1`, `vuln`, `openvex` as built-ins, so each artifact gets a properly-typed attestation.
- **PyPI / PEP 740 already engages automatically.** `pypa/gh-action-pypi-publish` ≥ v1.11.0 produces and uploads PEP 740 attestations by default for any project using Trusted Publishing. The existing `build.yml` step (`pypa/gh-action-pypi-publish@release/v1`, no token configured) is tokenless / Trusted Publishing, so once the floating tag resolves to ≥ v1.11.0 (it already does as of late 2024), attestations are published with no workflow change. Worth one explicit verification on the next release that PyPI shows the attestation badge.

### 3.5 License inventory

- Derived from the SBOM, but worth emitting as a standalone, human-readable artifact (`LICENSES.md` or similar) per release. CRA itself doesn't mandate this, but customer procurement does, and we'll get the question.

### 3.6 Security / risk documentation (CRA Annex I + Annex VII)

These aren't SBOM artifacts but are the *companion* documents CRA assumes you have. Worth scoping in the same workstream because the audience overlaps:

- **Vulnerability disclosure policy** — `SECURITY.md` currently exists but is two sentences. CRA expects a coordinated disclosure policy with response timelines. Expand it.
- **Support / update window statement** — declare which yente major versions receive security updates and for how long. CRA expects this in the "expected product lifetime" sense.
- **CSAF advisories** — once we issue a security fix, publish a CSAF 2.0 advisory rather than only a GitHub Security Advisory. CSAF is the format the EU CSIRT network and CRA's coordinated disclosure machinery speak.
- **Risk assessment / threat model** — a one-pager covering trust boundaries (the API, the index backend, the delivery token, manifest loading from S3). CRA Annex I requires the manufacturer to have performed one; it does not require publishing it, but having it written down means we can hand it to an auditor.
- **EU Declaration of Conformity** — required at point of CE-marking. Drafted closer to the 2027 deadline; mention here so we don't forget it exists.

## 4. Pipeline integration

Proposed shape (changes to `.github/workflows/build.yml`):

1. Add `pylock.toml` (runtime) and `pylock.dev.toml` (test/CI extras) to the repo via `pip lock`. Switch `Dockerfile` to install runtime deps from `pylock.toml`, then `pip install --no-deps -e /app` for yente itself. Switch CI `test-*` jobs to install both lockfiles. Pin pip explicitly in both. Add a `make lock` target.
2. After `python -m build --wheel`, create a throwaway venv, install from `pylock.toml`, run `cyclonedx-py environment <venv-python>` and (for SPDX) `syft <venv-dir>`. Upload as workflow artifacts; on tag builds, attach to the corresponding GitHub Release (see step 7).
3. In `package-docker`, after the build/push, run `syft <image-digest> -o cyclonedx-json -o spdx-json` for each platform, then `cosign attest --type cyclonedx --predicate <file> <digest>` to attach the SBOM as an OCI referrer. Also `cosign sign` the image digest itself.
4. Add `actions/attest@v4` (not `attest-build-provenance` — that's now a wrapper) for both wheel (`subject-path`) and image (`subject-name` + `subject-digest` from the build-push step). Grant the additional `attestations: write` and `artifact-metadata: write` workflow permissions.
5. Verify PEP 740 PyPI attestations engage on the next tag release (no code change required — the existing Trusted Publishing setup + ≥ v1.11.0 of `gh-action-pypi-publish` does this by default).
6. Keep VEX maintenance manual for now — `security/vex/*.cdx.json` checked into the repo, copied into the release on tag builds.
7. **Automate GitHub Release creation.** Today Releases are created by hand after `bump2version` pushes a tag. Move that into CI: on tag push, after wheel + image + SBOMs + provenance are built, use `softprops/action-gh-release@v2` to create the Release and attach the artifact bundle (wheel, sdist, `pylock.toml`, `pylock.dev.toml`, both SBOM formats, VEX files, a `LICENSES.md`). Generate notes via `generate_release_notes: true` so we don't lose the auto-changelog we'd get from manual creation. This step replaces the manual Release-creation flow entirely.

All of this runs only on tag pushes (i.e. real releases). On PR / `main` pushes we keep today's behaviour. We don't want to publish SBOMs for every commit — they should pin to a versioned, immutable artifact.

## 5. Storage and distribution

- **Primary:** GitHub Release page, created and populated by CI on tag push (via `softprops/action-gh-release@v2`). Individual artifact files attached directly so they're greppable / linkable; a single `yente-<version>-supply-chain.zip` bundle alongside for one-click download by procurement.
- **Secondary:** OCI registry — SBOM, VEX, provenance, and signature attached as referrers to the image digest via `cosign`. This is what `trivy` / `grype` / customer scanners look for automatically.
- **PyPI:** PEP 740 attestation only. Don't try to push SBOMs into PyPI metadata.
- Discoverability: link the latest release's artifacts from `SECURITY.md` and from the docs site.

## 6. Open questions

- **Scanner support for `pylock.toml` is not there yet (May 2026).** Confirmed: `syft` and `cyclonedx-py` don't have `pylock.toml` catalogers. Only `pip-audit` ≥ 2.9.0 does. We work around by scanning the installed venv / image, not the lockfile — which is actually more honest about what shipped. Revisit when catalogers land; at that point we'd add a "lockfile vs installed venv" diff check to catch resolver drift.
- **pip's PEP 751 implementation is still experimental.** Both `pip lock` and `pip install -r pylock.toml` carry experimental warnings, and the install side doesn't yet support extras / dependency groups (hence the split into `pylock.toml` + `pylock.dev.toml`). If pip changes the CLI before stabilising, our `make lock` target absorbs that — but worth watching pip release notes.
- **Single-platform locking + multi-arch builds.** PEP 751 lockfiles are platform-specific. We build `linux/amd64` and `linux/arm64`. Cleanest path: lock per-arch inside the multi-platform build (each platform-leg of buildx resolves against its own pylock during the build) and produce one SBOM per arch. Need to verify buildx + pip cooperate cleanly here; if not, lock for amd64 and accept that arm64's SBOM is built from the installed env, not the lockfile.
- **Do we publish the SBOM for the base image (`python:3.12-slim`) ourselves, or rely on the upstream one?** Probably ourselves — `syft` will scan the image regardless, and customers will scan whatever we publish. Means we own the noise from base-image CVEs (which is where VEX earns its keep).
- **First-party deps (`nomenklatura` etc.) — do they need their own SBOMs to be useful here, or do we let yente's SBOM absorb them?** For CRA purposes, yente is the product placed on the EU market, so yente's SBOM is what matters. But if we want clean attribution and the ability to issue advisories at the right granularity, the upstream packages should at least carry SPDX license metadata and ideally their own per-release SBOMs. Worth a follow-up plan.
- **Reproducible builds** — nice to have, not required by CRA. Probably defer.
- **Container hardening adjacent to CRA "secure by default"** — distroless base, drop `curl` from runtime (used only for healthcheck — switch to a Python one-liner?), pin base image by digest. Separate workstream but mention.

## 7. Suggested phasing

1. **Foundation (small):** commit `pylock.toml` (PEP 751), pin pip in CI and Docker, switch both install paths to consume the lockfile. Unblocks everything else.
2. **SBOM emission (medium):** add `syft` + `cyclonedx-py` to the release workflow, attach to GitHub release and OCI referrers. This is the headline CRA artifact.
3. **Signing + provenance (small, given Sigstore):** `cosign` + `actions/attest-build-provenance`. Cheap once the workflow already has `id-token: write`.
4. **VEX scaffolding (small):** empty `security/vex/`, document the process, wire it into the release bundle. Populate as findings come in.
5. **Documentation pass (medium, mostly writing):** expand `SECURITY.md`, write the support window statement, draft the threat model, set up CSAF publishing.
6. **EU DoC + conformity assessment:** revisit in 2027 against the then-final CRA implementing acts.

Steps 1–3 give us a defensible "yes, we publish SBOMs per release, signed, with provenance" answer to procurement and to CRA's eventual market-surveillance authorities. Steps 4–5 reduce ongoing burden.
