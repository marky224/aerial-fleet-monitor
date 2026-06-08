#!/usr/bin/env bash
#
# Seed / refresh the AFM_Runbook__mdt records into the Salesforce org via the
# Apex Metadata API (Metadata.Operations.enqueueDeployment).
#
# WHY NOT `sf project deploy`:
#   afm-dev hits a known Salesforce platform bug (GACK -315522575) that fails
#   ANY CustomMetadata *record* deploy through the Metadata API with
#   "UNKNOWN_EXCEPTION: An unexpected error occurred" — org-wide and
#   content-independent (a one-field record fails; SOAP and REST; API 60-66).
#   The CMDT *type* + *fields* and all non-CMDT metadata deploy fine. The Apex
#   Metadata API uses a different deploy path that is unaffected.
#   See _private/docs/build/07_agentforce.md (Decisions log) and the memory
#   note afm-dev-cmdt-mdapi-deploy-broken for the full diagnosis.
#
# The customMetadata/AFM_Runbook.*.md-meta.xml files remain the canonical,
# version-controlled source; this script materialises them faithfully (it parses
# the same XML), so it stays DRY and correct if a runbook changes.
#
# Idempotent: a CMDT deploy is an upsert keyed by fullName. NOTE the Apex
# Metadata API can create/update but NOT delete records — retire a record via
# the Setup UI or MDAPI destructiveChanges. The ~1,488-records-per-call limit
# is far above the 8 runbooks here.
#
# Usage:
#   scripts/seed_runbook_cmdt.sh [target-org-alias]    # default: afm-dev
#
set -euo pipefail

ORG="${1:-${SF_TARGET_ORG:-afm-dev}}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APEX_FILE="$(mktemp --suffix=.apex)"
trap 'rm -f "$APEX_FILE"' EXIT

# Generate the anonymous Apex from the source customMetadata XML (faithful + DRY).
python3 - "$REPO_ROOT" "$APEX_FILE" <<'PYEOF'
import glob, os, sys, xml.etree.ElementTree as ET
repo_root, apex_file = sys.argv[1], sys.argv[2]
NS = '{http://soap.sforce.com/2006/04/metadata}'
XSI_NIL = '{http://www.w3.org/2001/XMLSchema-instance}nil'
def esc(s):
    return s.replace('\\', '\\\\').replace("'", "\\'").replace('\r', '').replace('\n', '\\n')
pattern = os.path.join(repo_root, 'salesforce/force-app/main/default/customMetadata/AFM_Runbook.*.md-meta.xml')
files = sorted(glob.glob(pattern))
if not files:
    sys.stderr.write('error: no AFM_Runbook customMetadata files found at %s\n' % pattern)
    sys.exit(1)
out = ['Metadata.DeployContainer c = new Metadata.DeployContainer();']
for i, f in enumerate(files):
    root = ET.parse(f).getroot()
    base = os.path.basename(f)
    fullname = 'AFM_Runbook.' + base[len('AFM_Runbook.'):-len('.md-meta.xml')]
    label = root.find(NS + 'label').text
    out += ['Metadata.CustomMetadata cm%d = new Metadata.CustomMetadata();' % i,
            "cm%d.fullName = '%s';" % (i, esc(fullname)),
            "cm%d.label = '%s';" % (i, esc(label))]
    j = 0
    for values in root.findall(NS + 'values'):
        field = values.find(NS + 'field').text
        valel = values.find(NS + 'value')
        if valel.get(XSI_NIL) == 'true' or valel.text is None:
            continue  # leave the field null
        out += ['Metadata.CustomMetadataValue mv%d_%d = new Metadata.CustomMetadataValue();' % (i, j),
                "mv%d_%d.field = '%s'; mv%d_%d.value = '%s';" % (i, j, esc(field), i, j, esc(valel.text)),
                'cm%d.values.add(mv%d_%d);' % (i, i, j)]
        j += 1
    out.append('c.addMetadata(cm%d);' % i)
out += ['Id jobId = Metadata.Operations.enqueueDeployment(c, null);',
        "System.debug(LoggingLevel.INFO, 'ENQUEUED_JOB=' + jobId);"]
open(apex_file, 'w').write('\n'.join(out) + '\n')
sys.stderr.write('generated anonymous Apex for %d AFM_Runbook__mdt records\n' % len(files))
PYEOF

RECORD_COUNT="$(grep -c 'c.addMetadata' "$APEX_FILE" || true)"
echo "Seeding ${RECORD_COUNT} AFM_Runbook__mdt records into org '${ORG}' via the Apex Metadata API..."

RUN_OUT="$(sf apex run --target-org "$ORG" --file "$APEX_FILE" 2>&1)"
JOB_ID="$(printf '%s' "$RUN_OUT" | grep -oE 'ENQUEUED_JOB=[a-zA-Z0-9]+' | head -1 | cut -d= -f2)"
if [[ -z "$JOB_ID" ]]; then
  echo "ERROR: enqueue failed — no job id in Apex output:" >&2
  printf '%s\n' "$RUN_OUT" | tail -25 >&2
  exit 1
fi

echo "Enqueued metadata deployment ${JOB_ID}; waiting for completion..."
REPORT="$(sf project deploy report --job-id "$JOB_ID" --target-org "$ORG" --wait 10 --json 2>/dev/null || true)"
STATUS="$(printf '%s' "$REPORT" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("result",{}).get("status","Unknown"))' 2>/dev/null || echo Unknown)"
echo "Deploy status: ${STATUS}"
if [[ "$STATUS" != "Succeeded" ]]; then
  echo "ERROR: CMDT record deploy did not succeed (status=${STATUS}, job=${JOB_ID})." >&2
  exit 1
fi
echo "Done — ${RECORD_COUNT} AFM_Runbook__mdt records deployed to '${ORG}'."
