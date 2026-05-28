"""REST endpoints for KG rebuild preflight + confirm (KG-02.1 + KG-02.2).

Lifecycle (val_d0da4a75 rework):

    POST /api/v1/kg/rebuild/preflight  →  RebuildPreflightResponse
                                           includes manifest_ref + source_set_hash
                                           (manifest is PERSISTED here, immutably)
    POST /api/v1/kg/rebuild/confirm    →  RebuildConfirmResponse
                                           LOADS the manifest_ref + validates
                                           preflight_hash matches; does NOT
                                           re-enumerate or build a new manifest.

This closes the lifecycle gap: every preflight result lands in a
durable, immutable manifest, and every confirm token is bound to the
same manifest_ref the operator saw on screen.

TR13 invariant still holds: /preflight is READ-ONLY against KG storage
— writing the manifest JSON to the rebuild dir is the only side effect,
and it never touches graph.lbug or discovery.lbug.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.kg.rebuild_audit import default_rebuild_base_dir

router = APIRouter()


# Shared storage root for rebuild manifests + confirmations.
# Resolved through ``default_rebuild_base_dir()`` so the REST layer and
# the MCP layer agree on the on-disk location of the cognitive pending
# ledger (KG-03.2). Overridable via ``OKTO_PULSE_REBUILD_BASE_DIR``.
_REBUILD_BASE_DIR = default_rebuild_base_dir()


def _resolve_pulse_db_path() -> Path:
    """Return the path to the SQLite file the SQLAlchemy engine targets.

    Used by BoardSourceStore + BoardRebuildIngestionAdapter — both bypass
    the async engine because the rebuild service's adapters are sync
    callables. Reads the URL from the engine that's already initialised.
    """
    try:
        from okto_pulse.core.infra.database import get_engine

        url = str(get_engine().url)
    except Exception:
        # Fallback to default path so preflight never crashes if the
        # engine hasn't been initialised yet (e.g. during unit tests).
        return Path.home() / ".okto-pulse" / "data" / "pulse.db"
    # url is e.g. ``sqlite+aiosqlite:///C:/Users/.../pulse.db`` or
    # ``sqlite+aiosqlite:///./dashboard.db``.
    marker = ":///"
    idx = url.rfind(marker)
    if idx < 0:
        return Path.home() / ".okto-pulse" / "data" / "pulse.db"
    raw = url[idx + len(marker):]
    return Path(raw)


def _build_source_store():
    """Production source store reading non-cancelled specs/refinements/
    ideations from the SQLAlchemy SQLite file. Replaces the
    ``_empty_source_store`` stub that caused preflight to always report
    eligible_source_count=0 (bug b4c6920c)."""

    from okto_pulse.core.kg.board_source_store import BoardSourceStore

    store = BoardSourceStore(db_path=_resolve_pulse_db_path())
    return store.fetch


def _empty_source_store(_board_id: str) -> list[dict]:
    """Legacy alias retained for test seams that import it directly.

    Production code MUST use ``_build_source_store()`` instead — calling
    this would silently return an empty source set and the rebuild would
    not ingest anything (bug b4c6920c repro)."""
    return []


class RebuildPreflightResponse(BaseModel):
    """Frozen response shape exposed by /api/v1/kg/rebuild/preflight.

    val_d0da4a75 rework: now includes ``manifest_ref`` and
    ``source_set_hash`` — preflight is the manifest-issuance point.
    The frontend MUST echo these back to /confirm unchanged.
    """

    board_id: str
    outcome: str
    action_required: str
    reason: str | None = None
    base_state: str
    metric_status: str
    current_kg_generation_id: str | None = None
    eligible_source_count: int
    skipped_cancelled_count: int
    has_non_deterministic_inputs: bool
    preflight_hash: str
    generated_at: str
    rebuild_status: str = "idle"
    operational_substatus: str = ""
    # val_d0da4a75 #1: manifest is built and persisted at preflight time.
    manifest_ref: str
    source_set_hash: str


@router.post("/kg/rebuild/preflight", response_model=RebuildPreflightResponse)
async def post_rebuild_preflight(
    board_id: str = Query(..., description="Board ID (uuid)"),
    _: str = Depends(require_user),
) -> RebuildPreflightResponse:
    """Run preflight + persist the immutable source manifest. READ-ONLY (TR13).

    The manifest_ref returned here is the same one /confirm consumes —
    /confirm NEVER recomputes a manifest, so the operator's preflight
    view is the source of truth bound to the confirmation token.
    """
    from okto_pulse.core.kg.rebuild_preflight import (
        RebuildPreflightService,
        RebuildHealthSummary,
        RebuildSourceSummary,
    )
    from okto_pulse.core.kg.rebuild_sources import (
        KGRebuildSourceManifest,
        RebuildSourceEnumerator,
    )

    if not board_id:
        raise HTTPException(status_code=400, detail="board_id is required")

    # 1. Read base health from KG-01.1. Stub stays healthy until
    # KG-01 wiring lands at the runtime layer.
    def health_probe(_board_id: str) -> RebuildHealthSummary:
        return RebuildHealthSummary(
            base_state="healthy",
            metric_status="available",
            current_kg_generation_id=None,
        )

    # 2. Enumerate real sources via the injected source store. /preflight
    # now drives the SAME enumerator the manifest is built from — no
    # more divergence with /confirm. The preflight service consumes a
    # RebuildSourceSummary (slim view); the full source_set is kept
    # for the manifest builder below.
    # bug b4c6920c fix: real SQLite-backed source store (was empty stub).
    enumerator = RebuildSourceEnumerator(source_store=_build_source_store())
    source_set = enumerator.enumerate(board_id=board_id)

    def source_probe(_board_id: str) -> RebuildSourceSummary:
        return RebuildSourceSummary(
            eligible_count=source_set.eligible_count,
            skipped_cancelled_count=source_set.skipped_cancelled_count,
            has_non_deterministic_inputs=source_set.has_non_deterministic_inputs,
        )

    service = RebuildPreflightService(
        source_probe=source_probe,
        health_probe=health_probe,
    )
    result = service.run(board_id=board_id)

    # 3. Persist the immutable manifest bound to this preflight_hash.
    manifest_store = KGRebuildSourceManifest(base_dir=_REBUILD_BASE_DIR)
    manifest = manifest_store.build(
        source_set=source_set,
        preflight_hash=result.preflight_hash,
    )

    payload = result.to_dict()
    payload["manifest_ref"] = manifest.manifest_ref
    payload["source_set_hash"] = manifest.source_set_hash
    return RebuildPreflightResponse(**payload)


# --- KG-02.2 confirm endpoint ------------------------------------------------


class RebuildConfirmRequest(BaseModel):
    """Body of POST /api/v1/kg/rebuild/confirm.

    Bound to the manifest_ref + preflight_hash returned by /preflight.
    The server LOADS the manifest by ref; it does NOT enumerate fresh
    sources or build a new manifest.
    """

    board_id: str = Field(..., min_length=1)
    operation: str = Field(..., min_length=1)
    preflight_hash: str = Field(..., min_length=64, max_length=64)
    manifest_ref: str = Field(..., min_length=8)


class RebuildConfirmResponse(BaseModel):
    confirmation_id: str
    manifest_ref: str
    source_set_hash: str
    expires_at: str


@router.post("/kg/rebuild/confirm", response_model=RebuildConfirmResponse)
async def post_rebuild_confirm(
    body: RebuildConfirmRequest,
    user_id: str = Depends(require_user),
) -> RebuildConfirmResponse:
    """Bind the operator's confirmation to an existing manifest.

    val_d0da4a75 rework: loads the manifest by ref (never re-enumerates),
    verifies the preflight_hash matches, then issues the single-use
    token bound to the original manifest. Mismatch → HTTP 400.
    """
    from okto_pulse.core.kg.rebuild_confirmation import (
        CANONICAL_OPERATIONS,
        RebuildConfirmationStore,
    )
    from okto_pulse.core.kg.rebuild_sources import (
        KGRebuildSourceManifest,
        validate_preflight_hash,
    )

    if body.operation not in CANONICAL_OPERATIONS:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "unsupported_operation",
                "reason": "operation not in canonical set",
            },
        )

    # val_dfdff0b8 fail-closed (defence in depth): KG-02.3 wired the
    # rebuild path; other destructive operations land in KG-02.4+.
    # Reject confirmation issuance for non-rebuild ops so the operator
    # never sees a token that the runner will refuse to consume.
    from okto_pulse.core.kg.rebuild_service import (
        SUPPORTED_REBUILD_OPERATIONS,
    )

    if body.operation not in SUPPORTED_REBUILD_OPERATIONS:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "operation_pending_implementation",
                "reason": (
                    f"operation={body.operation!r} not implemented yet; "
                    f"KG-02.3 only supports {sorted(SUPPORTED_REBUILD_OPERATIONS)}"
                ),
            },
        )

    try:
        validate_preflight_hash(body.preflight_hash)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_preflight_hash",
                "reason": str(exc),
            },
        )

    # val_d0da4a75 #1: LOAD the existing manifest. NO re-enumeration.
    manifest_store = KGRebuildSourceManifest(base_dir=_REBUILD_BASE_DIR)
    manifest = manifest_store.load(body.manifest_ref)
    if manifest is None:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "manifest_not_found",
                "reason": "manifest_ref does not exist or is invalid",
            },
        )

    if manifest.board_id != body.board_id:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "manifest_board_mismatch",
                "reason": "manifest_ref belongs to a different board",
            },
        )

    if manifest.preflight_hash != body.preflight_hash:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "preflight_hash_mismatch",
                "reason": "preflight_hash does not match manifest binding",
            },
        )

    confirmation_store = RebuildConfirmationStore(base_dir=_REBUILD_BASE_DIR)
    token = confirmation_store.issue(
        board_id=body.board_id,
        actor_id=user_id,
        operation=body.operation,
        preflight_hash=body.preflight_hash,
        manifest_ref=manifest.manifest_ref,
    )

    return RebuildConfirmResponse(
        confirmation_id=token.confirmation_id,
        manifest_ref=manifest.manifest_ref,
        source_set_hash=manifest.source_set_hash,
        expires_at=token.expires_at,
    )


# --- KG-02.3 run endpoint ----------------------------------------------------


class RebuildRunRequest(BaseModel):
    confirmation_id: str = Field(..., min_length=8)
    board_id: str = Field(..., min_length=1)
    operation: str = Field(..., min_length=1)
    preflight_hash: str = Field(..., min_length=64, max_length=64)
    manifest_ref: str = Field(..., min_length=8)
    reason: str = Field(..., min_length=1, max_length=512)


class RebuildRunResponse(BaseModel):
    run_id: str
    outcome: str
    reason: str
    audit_ref: str
    previous_kg_generation_id: str | None = None
    current_kg_generation_id: str | None = None
    started_at: str
    finished_at: str
    affected_files: list[str] = Field(default_factory=list)
    # KG-02.4 — report-first surfaces.
    report_ref: str | None = None
    report_id: str | None = None
    publishable_status: str | None = None
    promotion_outcome: str | None = None
    operator_action: str | None = None
    event_emitted: bool = False


@router.post("/kg/rebuild/run", response_model=RebuildRunResponse)
async def post_rebuild_run(
    body: RebuildRunRequest,
    user_id: str = Depends(require_user),
) -> RebuildRunResponse:
    """Execute the KG rebuild under the KG-01 admin lane.

    Consumes the confirmation token issued by /confirm. NEVER mutates
    if confirmation is invalid, manifest drifted, or the admin lane
    can't take the lock exclusively. Returns the run audit ref and
    outcome.
    """
    from okto_pulse.core.kg.quarantine import KGQuarantineService
    from okto_pulse.core.kg.rebuild_confirmation import (
        RebuildConfirmationStore,
    )
    from okto_pulse.core.kg.rebuild_generation import (
        KGGenerationPromotionGuard,
        KGGenerationRepository,
    )
    from okto_pulse.core.kg.rebuild_report import (
        RebuildReportStore,
        RebuildReportTerminalStateGuard,
    )
    from okto_pulse.core.kg.rebuild_service import (
        KGRebuildService,
        RebuildOutcome,
    )
    from okto_pulse.core.kg.rebuild_sources import (
        KGRebuildSourceManifest,
        RebuildSourceEnumerator,
    )
    from okto_pulse.core.kg.safe_write_lifecycle import (
        HealthProbe,
        KGSafeWriteLifecycle,
        LockOwnerProbe,
    )
    from okto_pulse.core.kg.schema import apply_ladybug_lifecycle_step
    from okto_pulse.core.kg.single_writer_lock import KGSingleWriterLock
    from okto_pulse.core.kg.board_rebuild_adapter import (
        BoardRebuildIngestionAdapter,
    )
    from okto_pulse.core.kg.rebuild_audit import (
        CognitivePendingMarker,
        ConfirmationConsumptionAuditRecorder,
        KGRebuiltEventPublisher,
        build_kg_rebuilt_event_handler,
    )

    # Build KG-01 primitives.
    lock = KGSingleWriterLock(base_dir=_REBUILD_BASE_DIR / "locks")

    def _always_owner(board_id: str, owner_token: str) -> bool:
        manifest = lock.inspect(board_id=board_id)
        return manifest is not None and manifest.owner_token == owner_token

    safe_lifecycle = KGSafeWriteLifecycle(
        step_adapter=apply_ladybug_lifecycle_step,
        owner_probe=LockOwnerProbe(is_active_owner=_always_owner),
        health_probe=HealthProbe(
            classify=lambda b, g, status, step: "at_risk"
        ),
    )

    # bug b4c6920c fix: real source store (was empty stub).
    source_store_fetch = _build_source_store()
    enumerator = RebuildSourceEnumerator(source_store=source_store_fetch)
    manifest_store_obj = KGRebuildSourceManifest(base_dir=_REBUILD_BASE_DIR)

    # bug b4c6920c fix: real step adapter that enqueues sources for the
    # existing consolidation worker (which actually mutates graph.lbug).
    # The KG-02.5 DeterministicStructuralRebuilder still owns the
    # structural_hash + source_hash so KG-02.4 promotion gets a real
    # receipt. Closure resolver loads sources from the manifest the
    # rebuild_service just validated (KG-02.2 lifecycle).
    ingestion = BoardRebuildIngestionAdapter(db_path=_resolve_pulse_db_path())

    def _step_source_resolver(req):
        manifest = manifest_store_obj.load(req.manifest_ref)
        if manifest is None:
            return ()
        return tuple(row.to_dict() for row in manifest.sources)

    _step_adapter_with_sources = ingestion.build_step_adapter(
        source_resolver=_step_source_resolver,
    )

    # bug b4c6920c fix: real event_emitter composing publisher + marker
    # so kg.rebuilt is published AND cognitive pending is marked for the
    # new generation (KG-02.7 wiring that was missing).
    audit_recorder = ConfirmationConsumptionAuditRecorder(
        base_dir=_REBUILD_BASE_DIR
    )
    event_publisher = KGRebuiltEventPublisher(base_dir=_REBUILD_BASE_DIR)
    cognitive_marker = CognitivePendingMarker(base_dir=_REBUILD_BASE_DIR)

    def _source_resolver(event_payload):
        manifest = manifest_store_obj.load(event_payload.get("manifest_ref", ""))
        if manifest is None:
            return ()
        return tuple(row.to_dict() for row in manifest.sources)

    event_handler = build_kg_rebuilt_event_handler(
        publisher=event_publisher,
        cognitive_marker=cognitive_marker,
        source_resolver=_source_resolver,
    )

    service = KGRebuildService(
        base_dir=_REBUILD_BASE_DIR,
        single_writer_lock=lock,
        safe_write_lifecycle=safe_lifecycle,
        quarantine_service=None,  # wired by KG-02.4 reset path
        confirmation_store=RebuildConfirmationStore(
            base_dir=_REBUILD_BASE_DIR, audit_recorder=audit_recorder,
        ),
        manifest_store=manifest_store_obj,
        source_enumerator=enumerator,
        # bug b4c6920c: real step adapter (was _default_step_adapter stub).
        rebuild_step_adapter=_step_adapter_with_sources,
        # KG-02.4 — report-first terminal gate + generation promotion.
        generation_repository=KGGenerationRepository(base_dir=_REBUILD_BASE_DIR),
        promotion_guard=KGGenerationPromotionGuard,
        report_store=RebuildReportStore(base_dir=_REBUILD_BASE_DIR),
        terminal_state_guard=RebuildReportTerminalStateGuard,
        # bug b4c6920c: real event handler (was no-op default).
        event_emitter=event_handler,
    )

    # `KGRebuildService.run()` is synchronous and the rebuild step now waits
    # for the async consolidation worker to drain the board queue. Running it
    # directly inside this async endpoint would block the event loop and starve
    # the very worker we are waiting for.
    result = await run_in_threadpool(
        service.run,
        confirmation_id=body.confirmation_id,
        board_id=body.board_id,
        actor_id=user_id,
        operation=body.operation,
        preflight_hash=body.preflight_hash,
        manifest_ref=body.manifest_ref,
        reason=body.reason,
    )

    return RebuildRunResponse(
        run_id=result.run_id,
        outcome=result.outcome,
        reason=result.reason,
        audit_ref=result.audit_ref,
        previous_kg_generation_id=result.previous_kg_generation_id,
        current_kg_generation_id=result.current_kg_generation_id,
        started_at=result.started_at,
        finished_at=result.finished_at,
        affected_files=list(result.affected_files),
        report_ref=result.report_ref,
        report_id=result.report_id,
        publishable_status=result.publishable_status,
        promotion_outcome=result.promotion_outcome,
        operator_action=result.operator_action,
        event_emitted=result.event_emitted,
    )
