"""GET /v1/auth/me — auth stub for Phase 02 + 03.

Returns a hardcoded internal-ops session envelope. Real Salesforce
OAuth + JWT-derived identity lands in Phase 04; replacing this router
is part of that phase's work.

The Scope object behind the response comes from `dependencies.get_scope`,
which Phase 04 replaces with JWT-derived scope. This router's job is
purely presentational — projecting Scope's four canonical fields plus
the three wire-format fields (read_only, expires_at, salesforce_user_id)
into MeResponse.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app.dependencies import get_scope
from app.models.common import Region, Scope

router = APIRouter(prefix="/v1/auth", tags=["auth"])

_STUB_SESSION_LIFETIME = timedelta(hours=24)


class MeResponse(BaseModel):
    """Current session identity + scope (API.md §2.1)."""

    user_handle: str = Field(description="Caller identity, e.g. 'internal-ops'.")
    salesforce_user_id: str | None = Field(
        description="Salesforce User Id. Null until Phase 04 OAuth lands."
    )
    region: Region = Field(description="Region scope: 'west', 'east', or 'all'.")
    custom_perms: list[str] = Field(
        description="Salesforce custom permissions on the user. Empty in Phase 02."
    )
    read_only: bool = Field(
        description=(
            "True for the internal-ops stub. Blocks state-changing endpoints "
            "once they exist (Phase 04+)."
        )
    )
    expires_at: datetime = Field(description="Session expiry timestamp. Stub: now + 24h.")
    sites_in_scope: list[str] = Field(
        description="ICAO codes the caller may read. Sourced from the live watched-airports list."
    )


@router.get("/me", response_model=MeResponse)
def me(scope: Annotated[Scope, Depends(get_scope)]) -> MeResponse:
    """Return the current session identity and scope.

    Phase 02 stub — every caller gets the same internal-ops envelope.
    """
    return MeResponse(
        user_handle=scope.user_handle,
        salesforce_user_id=None,
        region=scope.region,
        custom_perms=scope.custom_perms,
        read_only=True,
        expires_at=datetime.now(UTC) + _STUB_SESSION_LIFETIME,
        sites_in_scope=scope.sites_in_scope,
    )
