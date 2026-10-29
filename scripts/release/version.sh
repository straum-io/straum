#!/bin/sh
# The version the release workflow builds: the tag without its "v" for a
# pushed tag, 0.0.0-prN for a pull request, 0.0.0-dev for a manual run.
# Reads GITHUB_EVENT_NAME, REF (the ref name) and PR (the PR number).
set -eu
case "${GITHUB_EVENT_NAME:-}" in
    push) printf '%s\n' "${REF#v}" ;;
    pull_request) printf '0.0.0-pr%s\n' "$PR" ;;
    *) printf '0.0.0-dev\n' ;;
esac
