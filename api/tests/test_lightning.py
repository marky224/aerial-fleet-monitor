"""Unit tests for `app.services._lightning.case_lightning_url`."""

from __future__ import annotations

from app.services._lightning import case_lightning_url


def test_composes_lightning_url_for_orgfarm_my_domain() -> None:
    """Standard My Domain + record-view path → Lightning deeplink."""
    url = case_lightning_url("https://orgfarm-12345.my.salesforce.com", "500dL00003F3ER6QAN")
    assert url == (
        "https://orgfarm-12345.my.salesforce.com/lightning/r/Case/500dL00003F3ER6QAN/view"
    )


def test_composes_lightning_url_for_dev_ed_my_domain() -> None:
    """Developer-edition `.develop.my.salesforce.com` doesn't get host-swapped."""
    url = case_lightning_url(
        "https://orgfarm-ad58b0bbec-dev-ed.develop.my.salesforce.com",
        "500dL00003F3ER6QAN",
    )
    assert url == (
        "https://orgfarm-ad58b0bbec-dev-ed.develop.my.salesforce.com"
        "/lightning/r/Case/500dL00003F3ER6QAN/view"
    )


def test_strips_trailing_slash_on_instance_url() -> None:
    """Instance URL with trailing slash composes cleanly."""
    url = case_lightning_url("https://orgfarm-12345.my.salesforce.com/", "500dL00003F3ER6QAN")
    assert url == (
        "https://orgfarm-12345.my.salesforce.com/lightning/r/Case/500dL00003F3ER6QAN/view"
    )


def test_returns_none_when_salesforce_id_missing() -> None:
    """Pending push (no SF Id yet) → no deeplink."""
    assert case_lightning_url("https://orgfarm-12345.my.salesforce.com", None) is None


def test_returns_none_when_instance_url_unset() -> None:
    """`SALESFORCE_INSTANCE_URL` not configured → no deeplink."""
    assert case_lightning_url(None, "500dL00003F3ER6QAN") is None


def test_returns_none_when_both_unset() -> None:
    assert case_lightning_url(None, None) is None
