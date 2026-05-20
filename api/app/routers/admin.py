"""Dev-only admin endpoints.

This router is mounted **only when `environment == 'dev'`** (see
`app.main`) — in any other environment the routes simply do not exist
(404), which is the intended access control for these helpers.

`POST /v1/admin/sf-test-case` is the Phase-04 acceptance #9 SF write
smoke: it creates a Case populating every `AFM_*__c` custom field +
the Fleet_Operations record type, then deletes it, and returns the
exact (region/format-translated) field map that was sent.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated
from uuid import uuid4

from fastapi import APIRouter, Depends

from app.dependencies import get_salesforce_service
from app.exceptions import AFMException
from app.logging import get_logger
from app.models.salesforce import CaseCreateInput, SfTestCaseResult
from app.services.salesforce import SalesforceService

router = APIRouter(prefix="/v1/admin", tags=["admin"])
log = get_logger(__name__)


@router.post("/sf-test-case", response_model=SfTestCaseResult)
async def sf_test_case(
    sf: Annotated[SalesforceService, Depends(get_salesforce_service)],
) -> SfTestCaseResult:
    """Round-trip a fully-populated synthetic Case (create → delete)."""
    external_id = f"CASE-TEST-{uuid4().hex[:12]}"
    payload = CaseCreateInput(
        external_id=external_id,
        subject=f"AFM SF write smoke — lost signal during cruise — TEST near KSFO ({external_id})",
        status="New",
        priority="Medium",
        flight_id="abc123",
        site_icao="KSFO",
        customer_region="west",
        case_type="lost_signal",
        detection_facts={
            "rule": "lost_signal",
            "smoke": True,
            "generated_at": datetime.now(UTC).isoformat(),
        },
        severity_justification="Synthetic Phase-04 smoke-test case; safe to delete.",
        runbook_refs=["lost-signal-cruise", "diversion-divert"],
        internal_url=f"https://example.invalid/afm/cases/{external_id}",
    )

    # Resolve the exact SF field map first (also warms the record-type
    # cache so create_case doesn't re-query). Surfaces the §10.1
    # region/format translation in the response for verification.
    sf_fields_sent = await sf_to_fields(sf, payload)

    ref = await sf.create_case(payload)
    log.info("admin.sf_test_case.created", salesforce_id=ref.salesforce_id)

    deleted = True
    try:
        await sf.delete_case(ref.salesforce_id)
        log.info("admin.sf_test_case.deleted", salesforce_id=ref.salesforce_id)
    except AFMException as exc:
        deleted = False
        log.warning(
            "admin.sf_test_case.delete_failed",
            salesforce_id=ref.salesforce_id,
            error=str(exc),
        )

    return SfTestCaseResult(created=ref, deleted=deleted, sf_fields_sent=sf_fields_sent)


async def sf_to_fields(sf: SalesforceService, payload: CaseCreateInput) -> dict[str, object]:
    """Run the (sync) field mapper off the event loop."""
    import asyncio

    return await asyncio.to_thread(sf.to_sf_fields, payload)
