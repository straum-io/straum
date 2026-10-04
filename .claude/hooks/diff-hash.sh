#!/usr/bin/env bash
# Prints a hash of the task's current change, or nothing when there is none.
# The change is everything since the upstream merge base (committed or not)
# plus untracked files; .claude/state is excluded so the marker itself
# does not reopen the task.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

if git rev-parse -q --verify '@{u}' >/dev/null 2>&1; then
  base="$(git merge-base HEAD '@{u}')"
elif git rev-parse -q --verify HEAD >/dev/null 2>&1; then
  base="HEAD"
else
  base="$(git hash-object -t tree /dev/null)"   # repository without commits
fi

changes="$(
  git diff --binary "$base" -- . ':(exclude).claude/state'
  git ls-files --others --exclude-standard -- . ':(exclude).claude/state' |
    while IFS= read -r f; do printf '%s %s\n' "$f" "$(git hash-object -- "$f")"; done
)"

[ -z "$changes" ] && exit 0

if command -v shasum >/dev/null 2>&1; then
  printf '%s' "$changes" | shasum -a 256 | cut -d' ' -f1
else
  printf '%s' "$changes" | sha256sum | cut -d' ' -f1
fi
