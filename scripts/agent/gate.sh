#!/usr/bin/env sh
# Stop gate: the agent cannot call a change done until the crates it touched
# are formatted, lint-clean and green. Source of truth: CLAUDE.md §17.
#
# Only the touched crates are checked; the full workspace, the Linux container
# run and the benchmarks stay in CI, because a gate nobody waits for is a gate
# somebody disables.
set -eu

cd "$(dirname "$0")/../.."

# Claude Code re-runs Stop hooks after the agent reacts. Skipping that re-run
# unconditionally also skipped checking the fix the agent made in response,
# so the skip is taken only when nothing changed since the last failure: an
# unchanged tree fails the same way, and that repetition is the loop the skip
# exists to break (D-5). Same pattern as require-closeout.sh, spaces allowed.
failed=".claude/state/gate-failed.sha"
current="$(.claude/hooks/diff-hash.sh 2>/dev/null || true)"
if cat | grep -Eq '"stop_hook_active"[[:space:]]*:[[:space:]]*true'; then
    if [ -f "$failed" ] && [ "$(cat "$failed")" = "$current" ]; then
        exit 0
    fi
fi

changed="$(git status --porcelain | awk '{print $NF}')"
crates="$(printf '%s\n' "$changed" | sed -n 's|^crates/\([^/]*\)/.*|\1|p' | sort -u)"
# A deleted crate shows up as changed too; there is nothing left to check
# in it (step 8 removed six at once, D-111).
crates="$(for c in $crates; do [ -f "crates/$c/Cargo.toml" ] && echo "$c"; done)"

[ -n "$crates" ] || exit 0

out="$(mktemp)"
trap 'rm -f "$out"' EXIT
fail=0

run() {
    if ! "$@" >>"$out" 2>&1; then
        echo "gate: failed: $*" >>"$out"
        fail=1
    fi
}

run cargo fmt --check
for c in $crates; do
    run cargo clippy -p "$c" --all-targets --all-features -- -D warnings
    # Benches are built, not run (CLAUDE.md §17, D-4).
    run cargo test -p "$c" --all-targets --no-run
    run cargo test -p "$c" --tests
done
case "$changed" in *Cargo.toml*) run scripts/check-boundaries.sh ;; esac

if [ "$fail" -eq 0 ]; then
    rm -f "$failed"
    exit 0
fi

mkdir -p "$(dirname "$failed")"
printf '%s' "$current" >"$failed"
echo "Stop gate failed for: $crates. Fix this before reporting the change as done." >&2
# What failed first, then where. A tail alone shows whichever crate ran last,
# and a failure in an earlier one scrolled out of it.
grep -E '^gate: failed:|^test .* FAILED$|panicked at|^error(\[|:)' "$out" | head -40 >&2
echo "--- last lines of the output:" >&2
tail -30 "$out" >&2
exit 2
