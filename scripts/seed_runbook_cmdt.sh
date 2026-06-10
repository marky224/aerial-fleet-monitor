#!/usr/bin/env bash
#
# Deploy / refresh the AFM_Runbook__mdt CustomMetadata records into a Salesforce
# org — ORG-ADAPTIVE, with a SOQL read-back to verify the result.
#
# WHY ADAPTIVE (and not just `sf project deploy`):
#   CustomMetadata records are normally deployed via the Metadata API. That is
#   the supported, production path; it works on a healthy org and supports
#   deletion (via destructiveChanges), which the Apex path below does NOT.
#
#   The afm-dev demo org, however, GACKs on ANY CustomMetadata *record* deploy:
#   `UNKNOWN_EXCEPTION` / internal code -315522575, org-wide, with ZERO
#   component-level errors. Verified hands-on this is NOT our records (a brand-new
#   clean CMDT type fails identically), NOT transport (SOAP + REST), NOT API
#   version (60-66), and NOT subscriber licensing (licensing the org's only
#   package, `devedapp`, did not clear it). The trigger is platform-managed
#   CustomMetadata that this org's deploy path can't resolve — an org/feature-level
#   anomaly, not anything our metadata carries, and possibly present on other
#   Agentforce orgs too. See _private/docs/build/07_agentforce.md (Decisions log),
#   the drafted support case in _private/docs/build/, and the memory note
#   afm-dev-cmdt-mdapi-deploy-broken.
#
#   So this script TRIES the standard path first and falls back to the Apex
#   Metadata API (Metadata.Operations.enqueueDeployment — a different, unaffected
#   in-org deploy path) ONLY on that exact GACK signature. Any OTHER failure is
#   surfaced, never masked. Either way the result is verified by a SOQL read-back
#   of all the records (don't trust a deploy's RUN_SUCCESS — verify the state).
#
#   The standard attempt deploys a metadata-format package built in a temp dir
#   OUTSIDE force-app, so salesforce/.forceignore's `customMetadata/**` exclusion
#   (which keeps the records out of the bulk `make sf-deploy`) does not apply.
#
# The customMetadata/AFM_Runbook.*.md-meta.xml files remain the canonical,
# version-controlled source; this script materialises them faithfully (it reads
# the same XML), so it stays DRY and correct if a runbook changes.
#
# Idempotent: the standard deploy is an upsert; the Apex path is an upsert keyed
# by fullName. NOTE the Apex Metadata API can create/update but NOT delete
# records — retire a record via the Setup UI, or via an MDAPI destructiveChanges
# deploy on a healthy (non-GACK) org. The ~1,488-records-per-call Apex limit is
# far above the handful of runbooks here.
#
# Env:
#   AFM_CMDT_SEED_MODE = auto (default) | apex | standard
#     auto     — try the standard deploy, fall back to Apex on the GACK
#     apex     — skip straight to the Apex path (use on a known-GACK org like
#                afm-dev to save the ~15s doomed standard attempt)
#     standard — standard deploy only; fail (no fallback) if it errors
#                (use to verify a healthy org needs no workaround)
#
# Usage:
#   scripts/seed_runbook_cmdt.sh [target-org-alias]    # default: afm-dev
#
set -euo pipefail

ORG="${1:-${SF_TARGET_ORG:-afm-dev}}"
MODE="${AFM_CMDT_SEED_MODE:-auto}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC_DIR="$REPO_ROOT/salesforce/force-app/main/default/customMetadata"
API_VERSION="$(python3 -c "import json;print(json.load(open('$REPO_ROOT/salesforce/sfdx-project.json')).get('sourceApiVersion','66.0'))" 2>/dev/null || echo 66.0)"
export SF_AUTOUPDATE_DISABLE=true

TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

shopt -s nullglob
SRC_FILES=( "$SRC_DIR"/AFM_Runbook.*.md-meta.xml )
shopt -u nullglob
RECORD_COUNT=${#SRC_FILES[@]}
if [[ "$RECORD_COUNT" -eq 0 ]]; then
  echo "ERROR: no AFM_Runbook custom metadata records under $SRC_DIR" >&2
  exit 1
fi

# --- standard MDAPI deploy ---------------------------------------------------
# Builds a metadata-format package in $TMP_ROOT and deploys it. Leaves the JSON
# result at $TMP_ROOT/deploy.json and echoes one classification token:
#   SUCCESS | GACK | OTHER
deploy_standard() {
  python3 - "$SRC_DIR" "$TMP_ROOT" "$API_VERSION" <<'PYEOF'
import glob, os, shutil, sys
src, tmp, ver = sys.argv[1], sys.argv[2], sys.argv[3]
cmdt = os.path.join(tmp, 'customMetadata'); os.makedirs(cmdt, exist_ok=True)
members = []
for f in sorted(glob.glob(os.path.join(src, 'AFM_Runbook.*.md-meta.xml'))):
    member = os.path.basename(f)[:-len('.md-meta.xml')]   # AFM_Runbook.<Name>
    members.append(member)
    shutil.copyfile(f, os.path.join(cmdt, member + '.md'))
pkg = ['<?xml version="1.0" encoding="UTF-8"?>',
       '<Package xmlns="http://soap.sforce.com/2006/04/metadata">', '    <types>']
pkg += ['        <members>%s</members>' % m for m in members]
pkg += ['        <name>CustomMetadata</name>', '    </types>',
        '    <version>%s</version>' % ver, '</Package>']
open(os.path.join(tmp, 'package.xml'), 'w').write('\n'.join(pkg) + '\n')
PYEOF
  sf project deploy start --metadata-dir "$TMP_ROOT" --target-org "$ORG" \
    --wait 10 --json > "$TMP_ROOT/deploy.json" 2>"$TMP_ROOT/deploy.err" || true
  python3 - "$TMP_ROOT/deploy.json" <<'PYEOF'
import json, sys
try:
    raw = open(sys.argv[1]).read(); d = json.loads(raw)
except Exception:
    print('OTHER'); sys.exit()
r = d.get('result', d)
if r.get('status') == 'Succeeded' or r.get('success') is True:
    print('SUCCESS'); sys.exit()
is_gack = (r.get('errorStatusCode') == 'UNKNOWN_EXCEPTION') or ('-315522575' in raw)
zero_comp = r.get('numberComponentErrors') in (0, None)
print('GACK' if (is_gack and zero_comp) else 'OTHER')
PYEOF
}

# --- Apex Metadata API fallback ----------------------------------------------
seed_apex() {
  local apex_file="$TMP_ROOT/seed.apex"
  python3 - "$SRC_DIR" "$apex_file" <<'PYEOF'
import glob, os, sys, xml.etree.ElementTree as ET
src_dir, apex_file = sys.argv[1], sys.argv[2]
NS = '{http://soap.sforce.com/2006/04/metadata}'
XSI_NIL = '{http://www.w3.org/2001/XMLSchema-instance}nil'
def esc(s):
    return s.replace('\\', '\\\\').replace("'", "\\'").replace('\r', '').replace('\n', '\\n')
files = sorted(glob.glob(os.path.join(src_dir, 'AFM_Runbook.*.md-meta.xml')))
if not files:
    sys.stderr.write('error: no AFM_Runbook customMetadata files found in %s\n' % src_dir)
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
PYEOF
  local run_out job_id report status
  run_out="$(sf apex run --target-org "$ORG" --file "$apex_file" 2>&1)"
  job_id="$(printf '%s' "$run_out" | grep -oE 'ENQUEUED_JOB=[a-zA-Z0-9]+' | head -1 | cut -d= -f2)"
  if [[ -z "$job_id" ]]; then
    echo "ERROR: Apex enqueue failed — no job id in output:" >&2
    printf '%s\n' "$run_out" | tail -25 >&2
    exit 1
  fi
  echo "  enqueued Apex metadata deployment ${job_id}; waiting..."
  report="$(sf project deploy report --job-id "$job_id" --target-org "$ORG" --wait 10 --json 2>/dev/null || true)"
  status="$(printf '%s' "$report" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("result",{}).get("status","Unknown"))' 2>/dev/null || echo Unknown)"
  if [[ "$status" != "Succeeded" ]]; then
    echo "ERROR: Apex CMDT deploy did not succeed (status=${status}, job=${job_id})." >&2
    exit 1
  fi
}

# --- SOQL verification (both paths) ------------------------------------------
verify_soql() {
  sf data query --target-org "$ORG" \
    -q "SELECT DeveloperName FROM AFM_Runbook__mdt" --json \
    > "$TMP_ROOT/verify.json" 2>/dev/null || true
  python3 - "$SRC_DIR" "$TMP_ROOT/verify.json" <<'PYEOF'
import glob, json, os, sys
src_dir, qfile = sys.argv[1], sys.argv[2]
expected = {os.path.basename(f)[len('AFM_Runbook.'):-len('.md-meta.xml')]
            for f in glob.glob(os.path.join(src_dir, 'AFM_Runbook.*.md-meta.xml'))}
try:
    recs = json.load(open(qfile)).get('result', {}).get('records', [])
except Exception as e:
    sys.stderr.write('ERROR: could not read the verification query result: %s\n' % e)
    sys.exit(2)
got = {r.get('DeveloperName') for r in recs}
missing = sorted(expected - got)
if missing:
    sys.stderr.write('ERROR: verification failed — %d/%d records present; MISSING: %s\n'
                     % (len(expected & got), len(expected), ', '.join(missing)))
    sys.exit(2)
print('  verified %d/%d AFM_Runbook__mdt records present.' % (len(expected), len(expected)))
PYEOF
}

# --- dispatch ----------------------------------------------------------------
echo "Seeding ${RECORD_COUNT} AFM_Runbook__mdt records into '${ORG}' (mode=${MODE})..."
METHOD=""
case "$MODE" in
  apex)
    echo "→ Apex Metadata API path (forced)."
    seed_apex
    METHOD="Apex Metadata API (forced)"
    ;;
  standard)
    echo "→ Standard MDAPI deploy (no fallback)."
    cls="$(deploy_standard)"
    if [[ "$cls" != "SUCCESS" ]]; then
      echo "ERROR: standard deploy failed (classification=${cls})." >&2
      python3 -c "import json;r=json.load(open('$TMP_ROOT/deploy.json')).get('result',{});print('  status=',r.get('status'),'errorStatusCode=',r.get('errorStatusCode'),'numCompErr=',r.get('numberComponentErrors'),'\n  msg=',r.get('errorMessage'))" 2>/dev/null || cat "$TMP_ROOT/deploy.err" >&2
      exit 1
    fi
    METHOD="standard MDAPI deploy"
    ;;
  auto)
    echo "→ Trying the standard MDAPI deploy..."
    cls="$(deploy_standard)"
    case "$cls" in
      SUCCESS)
        METHOD="standard MDAPI deploy" ;;
      GACK)
        echo "  standard deploy hit the known GACK (-315522575 / UNKNOWN_EXCEPTION, 0 component errors)."
        echo "  → falling back to the Apex Metadata API..."
        seed_apex
        METHOD="Apex Metadata API (GACK fallback)" ;;
      *)
        echo "ERROR: standard deploy failed with a NON-GACK error — not falling back (would mask a real failure):" >&2
        python3 -c "import json;r=json.load(open('$TMP_ROOT/deploy.json')).get('result',{});print('  status=',r.get('status'),'errorStatusCode=',r.get('errorStatusCode'),'numCompErr=',r.get('numberComponentErrors'),'\n  msg=',r.get('errorMessage'))" 2>/dev/null || cat "$TMP_ROOT/deploy.err" >&2
        exit 1 ;;
    esac
    ;;
  *)
    echo "ERROR: invalid AFM_CMDT_SEED_MODE='${MODE}' (use: auto | apex | standard)." >&2
    exit 1 ;;
esac

echo "Verifying via SOQL read-back..."
verify_soql
echo "Done — ${RECORD_COUNT} AFM_Runbook__mdt records in '${ORG}' via ${METHOD}."
