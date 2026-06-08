#!/usr/bin/env bash
#
# Provision the Agentforce agent run-as user (idempotent), then publish + activate
# the Fleet_Anomaly_Triage authoring bundle. CLI-only; safe to re-run.
#
# WHY a SOQL-query-first guard for the agent user:
#   `sf org create agent-user` is NOT idempotent — it appends a globally-unique GUID
#   to --base-username on every call, so a naive re-run creates a SECOND agent user.
#   There is no native --if-not-exists. So we query for an existing agent user first
#   and only create one if absent (research-confirmed pattern; see 07_agentforce.md).
#
# Each `sf agent publish` creates a NEW BotVersion; `sf agent activate` makes exactly
# one version active (the rest go Inactive). There is no CLI to prune old versions;
# for a single org the version clutter is harmless, so we don't try to clean it up.
#
# Prereqs in the target org: Einstein + Agentforce enabled, agent licenses available,
# and the AFM_Triage_Automation permission set deployed (it grants the agent user the
# Apex-class access AFM_TriageCase needs). On afm-dev these already hold.
#
# Usage:
#   scripts/publish_agent.sh [target-org-alias]   # default: afm-dev
#
set -euo pipefail

ORG="${1:-${SF_TARGET_ORG:-afm-dev}}"
AGENT="Fleet_Anomaly_Triage"
PERMSET="AFM_Triage_Automation"
BASE_USERNAME="fleet-triage-agent"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SF_DIR="$REPO_ROOT/salesforce"
AGENT_FILE="$SF_DIR/force-app/main/default/aiAuthoringBundles/${AGENT}/${AGENT}.agent"

export SF_AUTOUPDATE_DISABLE=true

# `sf agent publish/activate` require the working dir to be inside the SFDX project
# (RequiresProjectError otherwise). AGENT_FILE is absolute, so the in-place patch below
# is unaffected by the cd. The other commands (data query, org display/create/assign)
# resolve the org via --target-org and are happy from here too.
cd "$SF_DIR"

# Return one field of the first record of a SOQL result (empty string if no rows).
# NB: --json prints to stdout; the CLI's "update available" notice prints to stderr,
# so discard stderr (2>/dev/null) to keep the JSON parseable. The field name is passed
# as argv to python (not interpolated into the source) to avoid quoting issues.
soql_one() {
  sf data query --target-org "$ORG" --query "$1" --json 2>/dev/null \
    | python3 -c 'import sys, json
try:
    recs = json.load(sys.stdin).get("result", {}).get("records", [])
    print((recs[0].get(sys.argv[1]) or "") if recs else "")
except Exception:
    print("")' "$2"
}

# Fail fast on a dead/expired auth rather than misreading an empty query as "absent".
sf org display --target-org "$ORG" >/dev/null 2>&1 \
  || { echo "ERROR: cannot reach org '$ORG' — not authenticated? Run 'make sf-auth' (or JWT login in CI)." >&2; exit 1; }

echo "→ [1/5] Ensuring exactly one active agent run-as user (base: ${BASE_USERNAME})…"
AGENT_USER="$(soql_one "SELECT Username FROM User WHERE Username LIKE '${BASE_USERNAME}%' AND IsActive = true ORDER BY CreatedDate DESC LIMIT 1" Username)"
if [[ -z "$AGENT_USER" ]]; then
  echo "  none found — creating one (sf org create agent-user)…"
  sf org create agent-user --target-org "$ORG" \
    --base-username "$BASE_USERNAME" --first-name "Fleet" --last-name "Triage Agent"
  AGENT_USER="$(soql_one "SELECT Username FROM User WHERE Username LIKE '${BASE_USERNAME}%' AND IsActive = true ORDER BY CreatedDate DESC LIMIT 1" Username)"
  [[ -z "$AGENT_USER" ]] && { echo "ERROR: agent user not found after creation." >&2; exit 1; }
fi
echo "  agent user: ${AGENT_USER}"

echo "→ [2/5] Ensuring ${PERMSET} is assigned to the agent user…"
ASSIGNED="$(soql_one "SELECT Id FROM PermissionSetAssignment WHERE Assignee.Username = '${AGENT_USER}' AND PermissionSet.Name = '${PERMSET}'" Id)"
if [[ -z "$ASSIGNED" ]]; then
  sf org assign permset --target-org "$ORG" --name "$PERMSET" --on-behalf-of "$AGENT_USER"
  echo "  assigned."
else
  echo "  already assigned."
fi

echo "→ [3/5] Binding ${AGENT}.agent default_agent_user → ${AGENT_USER}…"
CURRENT="$(sed -n 's/.*default_agent_user:[[:space:]]*"\([^"]*\)".*/\1/p' "$AGENT_FILE" | head -1)"
if [[ "$CURRENT" != "$AGENT_USER" ]]; then
  # Patch in place for THIS publish only; restore the committed source on exit so the
  # working tree stays clean. (On afm-dev the committed literal already matches, so this
  # branch is a no-op; it only fires on a fresh org whose agent user has a new GUID.)
  cp "$AGENT_FILE" "${AGENT_FILE}.orig"
  trap 'mv -f "${AGENT_FILE}.orig" "$AGENT_FILE" 2>/dev/null || true' EXIT
  sed -i "s|\(default_agent_user:[[:space:]]*\"\)[^\"]*\"|\1${AGENT_USER}\"|" "$AGENT_FILE"
  echo "  patched for this publish (committed source had: ${CURRENT:-<none>})"
else
  echo "  already correct — no patch needed."
fi

echo "→ [4/5] Publishing authoring bundle ${AGENT} (creates a new BotVersion)…"
sf agent publish authoring-bundle --api-name "$AGENT" --target-org "$ORG" --skip-retrieve
BOT_USER_ID="$(soql_one "SELECT BotUserId FROM BotDefinition WHERE DeveloperName = '${AGENT}'" BotUserId)"
[[ -z "$BOT_USER_ID" ]] && { echo "ERROR: BotDefinition.BotUserId is null after publish — agent user binding failed." >&2; exit 1; }
echo "  published; BotUserId = ${BOT_USER_ID}"

echo "→ [5/5] Activating the latest BotVersion…"
LATEST="$(soql_one "SELECT VersionNumber FROM BotVersion WHERE BotDefinition.DeveloperName = '${AGENT}' ORDER BY VersionNumber DESC LIMIT 1" VersionNumber)"
[[ -z "$LATEST" ]] && { echo "ERROR: no BotVersion found to activate." >&2; exit 1; }
sf agent activate --api-name "$AGENT" --version "$LATEST" --target-org "$ORG"
echo "✔ ${AGENT} published + activated (v${LATEST}, BotUserId ${BOT_USER_ID}) on ${ORG}."
