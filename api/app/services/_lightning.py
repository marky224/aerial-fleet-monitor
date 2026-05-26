"""Salesforce Lightning URL helpers.

Pure derivation — takes the org's instance URL and a Case Id, returns the
Lightning UI deeplink. Lives next to the services that compose case
response models (`QueryService`, `CaseSyncService`) so both share one
formatter; kept out of `models.cases` to keep model imports settings-free.
"""

from __future__ import annotations


def case_lightning_url(instance_url: str | None, salesforce_id: str | None) -> str | None:
    """Lightning Case deeplink, or None when SF id or instance URL is unset.

    Appends the standard Lightning record-view path directly to the My
    Domain instance URL — Salesforce auto-redirects
    `<domain>.my.salesforce.com/lightning/...` to the Lightning host.
    Avoids the `.my.salesforce.com` → `.lightning.force.com` swap because
    developer-edition My Domains (`.develop.my.salesforce.com`) don't
    expose a `.develop.lightning.force.com` counterpart.
    """
    if not salesforce_id or not instance_url:
        return None
    return f"{instance_url.rstrip('/')}/lightning/r/Case/{salesforce_id}/view"
