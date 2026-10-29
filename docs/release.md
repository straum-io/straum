# Releasing Straum

Draft. How a version of `straum-server` is built, checked, tagged and
published. The workflow is `.github/workflows/release.yml`; the helpers are
in `scripts/release/`.

## Who does what

| Step | Owner | CI | Contributors |
|---|---|---|---|
| version bump PR | merges | runs the PR checks | prepare it |
| pre-release checklist | runs the bare-metal power-loss run, signs off | runs the PR-level checks | run `scripts/validate.sh pr` |
| tag | creates and pushes it | | never |
| build, image push, attestations, draft release | | on the tag push | |
| verify the draft | runs `scripts/release/verify.sh` | | |
| publish the release | `gh release edit --draft=false` | never | never |
| rollback and remediation | decides and acts | | prepare the fix version |

CI publishes no release. A tag push makes it push the image to the
registries and create a draft; only the owner makes the draft public.

## What a release contains

| Artifact | Targets |
|---|---|
| image `ghcr.io/straum-io/straum:X.Y.Z` and `<DOCKERHUB_NAMESPACE>/straum:X.Y.Z` | `linux/amd64`, `linux/arm64`, one manifest list |
| Debian package `straum_X.Y.Z_<arch>.deb` | `amd64`, `arm64`; Debian 12, Ubuntu 22.04 and newer |
| archive `straum-server-X.Y.Z-<target>.tar.gz` (binary, `LICENSE`, `README.md`) | `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu` (glibc 2.34 or newer), `aarch64-apple-darwin`, `x86_64-apple-darwin` |
| `SHA256SUMS` | over the packages and archives |
| GitHub artifact attestations | each package and archive, and the GHCR manifest list |
| buildx provenance (`mode=max`) and SBOM | attached to each platform image |

Production targets are the two Linux ones (`rules.md` §13). The macOS
archives are for development; the `x86_64-apple-darwin` one is
cross-compiled on an arm64 runner and run there only if Rosetta is present.

Linux binaries and images are built natively on amd64 and arm64 runners,
not cross-compiled: `ring` compiles C, and a native build needs no cross
linker or sysroot. Every build uses `--locked` and Rust 1.95.0.

## Versioning

Semantic Versioning (Constitution §12.3), with Cargo's rule for `0.y.z`:
while the major version is 0, a change of `y` marks an incompatible change
and a change of `z` a compatible one. Incompatible means a change to the
wire protocol, the on-disk format, the configuration or the command line
that an existing client, data directory or configuration file does not
survive.

A pre-release is `vX.Y.Z-<pre>`, for example `v0.2.0-rc.1`. The Debian
version turns `-` into `~` (`0.2.0~rc.1`), so it sorts before `0.2.0`.

The tag must equal the `version` of `crates/straum-broker/Cargo.toml` at
the tagged commit, `v` aside. The version bump is an ordinary PR to `main`.

## Pre-release checklist

At the commit to be tagged, on `main`:

1. CI is green: `ci` (`check`, `coverage`, `power-loss`) and
   `pr-contract`, and the `release` dry run (step 5).
2. `scripts/validate.sh pr` exits 0 at that commit.
3. The full power-loss run on the owner's bare-metal Linux computer exits
   0 (D-242 A): `scripts/power-loss/run.sh --out ~/power-loss/vX.Y.Z`,
   200 iterations per scenario and write mode. Its `env.txt` and
   `summary.txt` are recorded in `straum-dev-plan` and named in the
   release notes. The `--quick` job in CI does not replace it.
4. Guarantees evidence: `docs/guarantees.md` describes the code at that
   commit. Its power-loss
   statement changes only after the bare-metal run of step 3 passes
   (D-243 A). The release notes link the section that changed since the
   last release, or say none did.
5. Dry run: `gh workflow run release.yml --ref main`, then
   `gh run watch`. It builds every artifact and `SHA256SUMS` and publishes
   nothing.
6. `scripts/release/check-tag.sh vX.Y.Z <commit>` exits 0.
7. Release notes: what changed, compatibility and upgrade notes (protocol,
   on-disk format, configuration), known limits, the power-loss run.

## Tagging

```bash
git fetch origin
git tag -a vX.Y.Z -m vX.Y.Z <commit>
scripts/release/check-tag.sh vX.Y.Z
git push origin vX.Y.Z
```

The push starts `release`:

1. `tag` runs `check-tag.sh`: the tag names a version, is on `main` and
   matches the crate, and `DOCKERHUB_NAMESPACE` is set.
2. `image` builds each platform on its own runner and pushes it by digest;
   `image-merge` tags one manifest list `X.Y.Z`, `X.Y` and, without a `-`
   suffix, `latest` in both registries and attests the GHCR one.
3. `deb` and `macos` build the packages and archives; `checksums` writes
   `SHA256SUMS` and checks it.
4. `github-release` checks `SHA256SUMS` again, attests every package and
   archive, and creates the draft release (a pre-release for a `-` tag).

A published tag is never moved or rebuilt, and a version is never reused,
even one whose draft was deleted.

## Verifying the draft

```bash
scripts/release/verify.sh vX.Y.Z
```

It downloads the draft's assets, runs `sha256sum -c SHA256SUMS`, and
`gh attestation verify` on each package, each archive and
`oci://ghcr.io/straum-io/straum:X.Y.Z`. By hand, also:

```bash
docker buildx imagetools inspect ghcr.io/straum-io/straum:X.Y.Z
```

shows `linux/amd64` and `linux/arm64`, and
`docker buildx imagetools inspect ghcr.io/straum-io/straum:X.Y.Z --format '{{json .Image}}'`
shows the `org.opencontainers.image.revision` label of the tagged commit.
A user verifies a downloaded file the same way:
`gh attestation verify <file> -R straum-io/straum`.

## Publishing

```bash
gh release edit vX.Y.Z --draft=false            # a -pre tag stays a pre-release
```

For the first release to GHCR, the owner makes the `straum` package public
in the organization's package settings; GHCR creates it private.

## Rollback and remediation

Nothing is overwritten: a defect is fixed by a new version.

- **Draft is wrong, not yet published.** Delete the draft
  (`gh release delete vX.Y.Z`). The image tags `X.Y.Z`, `X.Y` and `latest`
  were already pushed: point `X.Y` and `latest` back to the previous
  release (`docker buildx imagetools create -t ghcr.io/straum-io/straum:latest ghcr.io/straum-io/straum:<previous>`,
  the same for Docker Hub) and delete the `X.Y.Z` image tag in both
  registries. The tag stays; the fix is the next version.
- **Published release with a defect.** Release the fix as a new version.
  Until then, point `latest` and `X.Y` back to the previous release, and
  add a warning to the defective release's notes. Users downgrade with
  `sudo apt install ./straum_<previous>_<arch>.deb --allow-downgrades` or
  the previous image; a downgrade across an on-disk format change works
  only where that release's notes say so.
- **Compromised artifact or leaked secret** (Constitution §13). Delete the
  affected release assets and image tags, revoke the secret
  (`DOCKERHUB_TOKEN`, `PPA_GPG_KEY`), publish a GitHub security advisory
  naming the affected versions and digests, and release a fixed version
  from a clean build. The attestations show which run built each artifact.

## Pins

Every action in `release.yml` is pinned to a commit, with its tag in a
comment; `rust:1.95-bookworm` and `gcr.io/distroless/cc-debian12:nonroot`
are pinned by the digest of their multi-platform index, in the
`Dockerfile` and in the `deb` job. To move a pin, resolve the new commit
(`git ls-remote --tags https://github.com/<owner>/<action>`) or index
digest (`docker buildx imagetools inspect <image>:<tag>`), choose a version
at least two weeks old, and change it in a PR, which runs the dry run.

## One-time setup

In the repository settings, by the owner:

- variable `DOCKERHUB_NAMESPACE` and secrets `DOCKERHUB_USERNAME`,
  `DOCKERHUB_TOKEN` (a Docker Hub access token with write scope);
- GHCR needs no secret: the workflow's `GITHUB_TOKEN` with
  `packages: write`;
- the PPA settings (`LAUNCHPAD_PPA`, `PPA_GPG_KEY`) are not needed while
  the PPA is paused (D-253 A).

Agents never read or print the secrets.
