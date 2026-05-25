#!/usr/bin/env bash
# Pre-push hook: scan commits being pushed for infra-specific strings
# that should not leak to the public repo.
#
# Patterns target the user-stated scrub policy:
#   - Tailscale IPv4 (100.64.0.0/10) and IPv6 (fd7a:)
#   - Tailscale magic-DNS (.ts.net)
#   - LAN 192.168.x
#   - Specific local hostnames
#
# Wired in via .pre-commit-config.yaml with stages: [pre-push].
# Install hook: pre-commit install --hook-type pre-push
# Override (not recommended): git push --no-verify

set -euo pipefail

PATTERN='100\.(6[4-9]|[7-9][0-9]|1[01][0-9]|12[0-7])\.[0-9]+\.[0-9]+|fd7a:[0-9a-f:]|192\.168\.[0-9]+\.[0-9]+|<host>|\.ts\.net'

branch=$(git rev-parse --abbrev-ref HEAD)
if upstream=$(git rev-parse --abbrev-ref "${branch}@{upstream}" 2>/dev/null); then
    range="${upstream}..HEAD"
else
    range="HEAD~20..HEAD"
fi

if ! git rev-parse "$range" >/dev/null 2>&1; then
    exit 0
fi

found=0

msg_hits=$(git log --format='%h %s%n%b' "$range" 2>/dev/null | grep -niE --color=always "$PATTERN" || true)
if [ -n "$msg_hits" ]; then
    echo "ERROR: infra-specific strings found in commit messages of commits being pushed:" >&2
    echo "$msg_hits" >&2
    echo "" >&2
    found=1
fi

diff_hits=$(git log -p "$range" -- ':!scripts/check-infra-leaks.sh' 2>/dev/null | grep -nE --color=always "^\+.*($PATTERN)" || true)
if [ -n "$diff_hits" ]; then
    echo "ERROR: infra-specific strings found in committed diff content being pushed:" >&2
    echo "$diff_hits" >&2
    echo "" >&2
    found=1
fi

if [ "$found" -eq 1 ]; then
    echo "Fix the leaks above and re-push." >&2
    echo "Override (not recommended): git push --no-verify" >&2
    exit 1
fi

exit 0
