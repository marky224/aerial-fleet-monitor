# Route 53 change-batch templates

Why this directory exists: `markandrewmarquez.com` is authoritative on AWS
Route 53 (hosted zone `Z04568022MZ21HXK15I1D`), but AFM's public API is
proxied through a **Cloudflare Tunnel** (see
`infra/cloudflared/config.yml.example`). That split means
`cloudflared tunnel route dns ...` — Cloudflare's normal one-liner for
creating the DNS hostname — does not work, because it can only manage
zones that are on Cloudflare DNS.

The workaround is a hand-maintained CNAME in Route 53 pointing at the
tunnel's `<uuid>.cfargotunnel.com` endpoint. These JSON files capture
those CNAMEs in the format the AWS CLI expects.

## Files

| File | Phase | Purpose |
|---|---|---|
| `api-cname.json.example` | 00 | `api.aerial-fleet-monitor` CNAME for the backend API tunnel |

Future phases will add `grafana.aerial-fleet-monitor` and
`dagster.aerial-fleet-monitor` records (Phase 09); same shape, different
hostnames, all pointing at the same tunnel UUID.

## Applying a change

1. Find the tunnel UUID in the Cloudflare Zero Trust dashboard
   (Networks → Tunnels → click the tunnel → URL or detail panel).
2. Copy the template:
   ```bash
   cp infra/route53/api-cname.json.example infra/route53/api-cname.json
   ```
   The non-`.example` file is gitignored (DNS targets shouldn't drift
   from the template — keep the template authoritative and treat the
   substituted file as a local artifact).
3. Replace the placeholder:
   ```bash
   sed -i "s/<TUNNEL_UUID>/00000000-0000-0000-0000-000000000000/" \
     infra/route53/api-cname.json
   ```
4. Apply:
   ```bash
   aws route53 change-resource-record-sets \
     --hosted-zone-id Z04568022MZ21HXK15I1D \
     --change-batch file://infra/route53/api-cname.json
   ```
5. Verify (give DNS a minute to propagate):
   ```bash
   dig +short api.aerial-fleet-monitor.markandrewmarquez.com CNAME
   ```
   Should return `<TUNNEL_UUID>.cfargotunnel.com.`.

`UPSERT` is idempotent — re-running the same change is safe.
