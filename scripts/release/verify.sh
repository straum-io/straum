#!/bin/sh
# scripts/release/verify.sh TAG [REPO]
#
# Downloads the assets of release TAG (a draft too, for a user with write
# access) and checks them: SHA256SUMS, and for every package and archive
# and for the GHCR manifest list named in the release notes, a GitHub build
# attestation made by release.yml for refs/tags/TAG. Needs gh, logged in.
# REPO defaults to straum-io/straum.
set -eu
tag="${1:?usage: verify.sh TAG [REPO]}"
repo="${2:-straum-io/straum}"
dir="$(mktemp -d)"
trap 'rm -rf -- "${dir:?}"' EXIT
attest() {
    gh attestation verify "$1" --repo "$repo" \
        --signer-workflow "$repo/.github/workflows/release.yml" \
        --source-ref "refs/tags/$tag" >/dev/null
    echo "verify: $1: attestation OK"
}

digest="$(gh release view "$tag" --repo "$repo" --json body --jq .body \
    | grep -oE 'manifest list `sha256:[0-9a-f]{64}`' | grep -oE 'sha256:[0-9a-f]{64}' | head -n 1)"
[ -n "$digest" ] || { echo "verify: no manifest list digest in the notes of $tag" >&2; exit 1; }

gh release download "$tag" --repo "$repo" --dir "$dir"
cd "$dir"
if command -v sha256sum >/dev/null; then sha256sum -c SHA256SUMS; else shasum -a 256 -c SHA256SUMS; fi
n=0
for f in *.deb *.tar.gz; do
    [ -e "$f" ] || continue
    attest "$f"
    n=$((n + 1))
done
# Two packages and four archives (release.yml, job checksums).
[ "$n" -eq 6 ] || { echo "verify: expected 6 packages and archives, checked $n" >&2; exit 1; }
attest "oci://ghcr.io/$repo@$digest"
echo "verify: the tag ${tag#v} must resolve to $digest:"
echo "  docker buildx imagetools inspect ghcr.io/$repo:${tag#v}"
