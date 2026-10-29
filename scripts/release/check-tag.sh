#!/bin/sh
# scripts/release/check-tag.sh TAG [COMMIT [MAIN]]
#
# Refuses a release tag unless it names a version (vX.Y.Z or vX.Y.Z-pre),
# COMMIT (default: the tag) is on MAIN (default: origin/main), and the
# straum-broker version at COMMIT equals the tag without its "v". The
# release workflow runs it on every pushed tag; run it before tagging too.
set -eu
tag="${1:?usage: check-tag.sh TAG [COMMIT [MAIN]]}"
commit="${2:-$tag}"
main="${3:-origin/main}"
fail() { echo "check-tag: $*" >&2; exit 1; }

printf '%s\n' "$tag" | grep -Eqx 'v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?' \
    || fail "$tag is not vX.Y.Z or vX.Y.Z-pre"
git merge-base --is-ancestor "$commit" "$main" || fail "$commit is not on $main"
crate="$(git show "$commit:crates/straum-broker/Cargo.toml" | sed -n 's/^version = "\(.*\)"$/\1/p' | head -n 1)"
[ "$crate" = "${tag#v}" ] || fail "straum-broker is version '$crate' at $commit, the tag says ${tag#v}"
echo "check-tag: $tag at $(git rev-parse --short "$commit^{commit}") is on $main and matches straum-broker $crate"
