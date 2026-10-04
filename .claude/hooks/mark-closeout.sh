#!/usr/bin/env bash
# Last command of task-closeout: records that this exact change was closed out.
set -euo pipefail
root="$(git rev-parse --show-toplevel)"
mkdir -p "$root/.claude/state"
"$root/.claude/hooks/diff-hash.sh" > "$root/.claude/state/closeout.sha"
echo "closeout recorded"
