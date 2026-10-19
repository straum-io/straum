#!/bin/sh
# Builds the Launchpad source packages of a release in target/ppa/: one orig
# tarball with every crate vendored (Launchpad builders have no network) and
# one source package per Ubuntu series. Upload with
#   dput ppa:OWNER/NAME target/ppa/*_source.changes
#
#   scripts/ppa.sh VERSION SERIES...    e.g. scripts/ppa.sh 0.1.0~rc.2 jammy noble resolute
#
# Signs with the first secret key in GnuPG, whose identity also becomes the
# maintainer; PPA_UNSIGNED=1 builds unsigned packages (pull-request check).
# Needs git, cargo, dpkg-dev and xz; runs on Linux.
set -eu

version=$1
shift
root=$(git rev-parse --show-toplevel)
out=$root/target/ppa
src=$out/straum-$version

if [ "${PPA_UNSIGNED:-0}" = 1 ]; then
    maintainer="Straum CI <noreply@github.com>"
    sign="--no-sign"
else
    maintainer=$(gpg --batch --list-secret-keys --with-colons | awk -F: '$1 == "uid" { print $10; exit }')
    [ -n "$maintainer" ] || { echo "ppa: no secret key in GnuPG" >&2; exit 1; }
    sign=""
fi

rm -rf "$out"
mkdir -p "$src"
git -C "$root" archive HEAD | tar -x -C "$src"
(cd "$src" && cargo vendor --locked --versioned-dirs vendor >/dev/null)
# dpkg-source leaves out files such as *.orig, so drop the per-file checksums
# and keep each crate's package checksum, as Debian's Rust packages do.
find "$src/vendor" -name .cargo-checksum.json -print0 | xargs -0 sed -i 's/"files":{[^}]*}/"files":{}/'
tar -C "$out" --exclude="straum-$version/debian" -cJf "$out/straum_$version.orig.tar.xz" "straum-$version"
sed -i "s|@MAINTAINER@|$maintainer|" "$src/debian/control"

# The orig tarball goes up with the first series only; Launchpad keeps one
# copy per PPA.
include_orig=-sa
for series in "$@"; do
    case $series in
        jammy) release=22.04 ;;
        noble) release=24.04 ;;
        resolute) release=26.04 ;;
        *) echo "ppa: no Rust 1.91 toolchain known for series $series" >&2; exit 1 ;;
    esac
    # ~ubuntuYY.MM sorts the series in upgrade order.
    cat > "$src/debian/changelog" <<CHANGELOG
straum ($version-0ppa1~ubuntu$release) $series; urgency=medium

  * Release $version: https://github.com/straum-io/straum/releases

 -- $maintainer  $(date -R)
CHANGELOG
    (cd "$src" && dpkg-buildpackage -S -d -nc $include_orig $sign)
    include_orig=-sd
done
ls -l "$out"
