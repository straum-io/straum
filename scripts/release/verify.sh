#!/bin/sh
# scripts/release/verify.sh TAG [REPO]
#
# Downloads the assets of release TAG (a draft too, for a user with write
# access) and checks them: SHA256SUMS, a GitHub build attestation for every
# package and archive, and one for the GHCR image X.Y.Z. Needs gh, logged in.
# REPO defaults to straum-io/straum.
set -eu
tag="${1:?usage: verify.sh TAG [REPO]}"
repo="${2:-straum-io/straum}"
dir="$(mktemp -d)"
trap 'rm -rf -- "${dir:?}"' EXIT

gh release download "$tag" --repo "$repo" --dir "$dir"
cd "$dir"
if command -v sha256sum >/dev/null; then sha256sum -c SHA256SUMS; else shasum -a 256 -c SHA256SUMS; fi
n=0
for f in *.deb *.tar.gz; do
    [ -e "$f" ] || continue
    gh attestation verify "$f" --repo "$repo" >/dev/null
    echo "verify: $f: attestation OK"
    n=$((n + 1))
done
[ "$n" -eq 6 ] || { echo "verify: expected 6 packages and archives, checked $n" >&2; exit 1; }
image="ghcr.io/$repo:${tag#v}"
gh attestation verify "oci://$image" --repo "$repo" >/dev/null
echo "verify: $image: attestation OK"
