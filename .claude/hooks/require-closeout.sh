#!/usr/bin/env bash
# Stop hook: refuses to let the agent finish while its change has not been
# through task-closeout. Exit 2 sends stderr back to the agent.
set -uo pipefail
input="$(cat)"

# Already continuing because of this hook: do not loop.
if printf '%s' "$input" | grep -Eq '"stop_hook_active"[[:space:]]*:[[:space:]]*true'; then
  exit 0
fi

root="$(git rev-parse --show-toplevel 2>/dev/null)" || exit 0
current="$("$root/.claude/hooks/diff-hash.sh" 2>/dev/null)" || exit 0
[ -z "$current" ] && exit 0                     # nothing changed

marker="$root/.claude/state/closeout.sha"
if [ -f "$marker" ] && [ "$(cat "$marker")" = "$current" ]; then
  exit 0
fi

cat >&2 <<'MSG'
The working tree has changes that have not been closed out.
Run the task-closeout skill: validation, vision-guardian review, decision
cards in docs/decisions/pending.md, then .claude/hooks/mark-closeout.sh as
the last command. If you only answered a question and changed nothing of the
task, the changes are the user's: run mark-closeout.sh and say so.
MSG
exit 2
