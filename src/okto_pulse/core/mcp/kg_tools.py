"""MCP tool wrappers for the 7 consolidation primitives.

Tools are registered via `register_kg_tools(mcp)` which is called from
`server.py` after the Core command catalog is constructed. This avoids the
circular import that would happen if `kg_tools` imported `mcp` at module load.

Each tool:
1. Resolves the authenticated agent (shared helper from server.py)
2. Validates the request payload against the Pydantic schema
3. Delegates to the corresponding primitive function
4. Serializes the response (or error) as JSON string
"""

from __future__ import annotations

import json
import logging
from functools import partial
from typing import Any

from pydantic import ValidationError

from okto_pulse.core.kg.guarded_write import (
    GuardedWriteError,
    guarded_board_write,
)
from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.mcp.kg_authorization import (
    kg_permission_error,
    principal_id,
)
from okto_pulse.core.kg.primitives import (
    KGPrimitiveError,
    abort_deferred_consolidation,
    _require_open_session,
    abort_consolidation,
    add_edge_candidate,
    add_node_candidate,
    finalize_deferred_consolidation,
    get_similar_nodes,
    run_cancellation_atomic,
)
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitiveItemListOutcome,
    CognitiveItemListReasonCode,
    CognitiveItemListSurface,
    CognitiveItemStatus,
    CognitiveItemUpdateOutcome,
    CognitiveItemUpdateReasonCode,
    CognitivePendingOutcomeType,
    CognitiveUnsafePayloadReason,
    CognitiveUnsafePayloadSurface,
    _emit_list_sample,
    _emit_unsafe_payload_sample,
    _emit_update_sample,
    compute_status_counts,
    detect_unsafe_update_payload,
    emit_operational_inspection_sample,
    empty_status_counts,
    project_item_for_api,
    project_item_for_update_api,
    record_cognitive_working_only_hold,
    require_rebuild_audit_artifact_store,
)
from okto_pulse.core.kg.cognitive_readiness import R7_HOLD_REASON_CODES

from okto_pulse.core.kg.schemas import (
    AbortConsolidationRequest,
    AddEdgeCandidateRequest,
    AddNodeCandidateRequest,
    BeginConsolidationRequest,
    CommitConsolidationRequest,
    GetSimilarNodesRequest,
    ProposeReconciliationRequest,
)

_VALID_OUTCOME_TYPES: frozenset[str] = frozenset(
    o.value for o in CognitivePendingOutcomeType
)

_VALID_LIST_STATUS_FILTERS: frozenset[str] = frozenset(
    s.value for s in CognitiveItemStatus
)


def _err(code: str, message: str, **extra: Any) -> str:
    payload: dict[str, Any] = {"error": {"code": code, "message": message}}
    if extra:
        payload["error"].update(extra)
    return json.dumps(payload, default=str)


def _ok(response) -> str:
    return response.model_dump_json()


def _validation_message(error: ValidationError) -> str:
    """Return a stable, input-safe summary of a Pydantic validation failure."""

    issues: list[str] = []
    for item in error.errors(
        include_url=False,
        include_context=False,
        include_input=False,
    ):
        location = ".".join(str(part) for part in item.get("loc", ())) or "request"
        message = str(item.get("msg") or "invalid value")
        issues.append(f"{location}: {message}")
    return "validation failed: " + "; ".join(issues)


logger = logging.getLogger("okto_pulse.mcp.kg_tools")


def _maybe_record_r7_cognitive_hold(
    *, board_id: str, error: KGPrimitiveError, actor_id: str
) -> None:
    """Persist an R7 working-only canonical Learning go-forward hold when a
    commit is rejected with the bounded hold payload.

    Non-blocking: a persistence failure must NEVER mask the structured error
    that the agent needs to see. The hold lands in the cognitive pending
    ledger only (never CanonicalDebt / DLQ)."""
    details = getattr(error, "details", None)
    if not isinstance(details, dict):
        return
    payload = details.get("r7_cognitive_hold_candidate")
    if not isinstance(payload, dict):
        return
    try:
        record_cognitive_working_only_hold(
            board_id=board_id,
            hold_payload=payload,
            actor_id=actor_id,
        )
    except Exception:
        logger.warning(
            "kg.r7_hold.persist_failed board=%s", board_id, exc_info=True
        )


def register_kg_tools(
    mcp,
    *,
    get_agent,
    get_uow,
    get_board_agent=None,
) -> None:
    """Register the 7 KG primitive tools on the given command catalog.

    Args:
        mcp: Core command catalog from okto_pulse.core.mcp.server
        get_agent: async callable returning the authenticated agent, or None
                   on auth failure (shared helper from server.py)
        get_uow: callable returning the MCP UnitOfWorkFactory (spec R01A MCP-FU1);
                 the consolidation write tools obtain a PulseUnitOfWork from it
                 instead of opening a raw AsyncSession.
        get_board_agent: async callable returning a board-scoped authenticated
                         context, or None when authentication/ACL resolution
                         fails. Omitting the callback fails closed.
    """

    async def _authorize_board(
        agent,
        board_id: str,
        required_permission: str,
    ):
        if get_board_agent is None:
            return None, _err(
                "unauthorized",
                "authentication failed or board access denied",
            )
        try:
            board_agent = await get_board_agent(board_id)
        except Exception:
            logger.warning(
                "kg.mcp.board_acl_resolution_failed board=%s",
                board_id,
                exc_info=True,
            )
            return None, _err(
                "unauthorized",
                "authentication failed or board access denied",
            )
        if board_agent is None:
            return None, _err(
                "unauthorized",
                "authentication failed or board access denied",
            )
        if principal_id(agent) != principal_id(board_agent):
            return None, _err(
                "unauthorized",
                "authentication failed or board access denied",
            )
        permission_error = kg_permission_error(
            board_agent,
            required_permission,
        )
        if permission_error is not None:
            return None, _err(
                "permission_denied",
                permission_error,
                required_permission=required_permission,
            )
        return board_agent, None

    async def _authorized_session(
        session_id: str,
        agent,
        *,
        allow_pending_commit: bool = False,
        required_permission: str,
    ):
        try:
            session = await _require_open_session(
                session_id,
                agent.id,
                allow_pending_commit=allow_pending_commit,
            )
        except KGPrimitiveError as exc:
            return None, _err(
                exc.code,
                exc.message,
                session_id=exc.session_id,
                details=exc.details,
            )
        _board_agent, access_error = await _authorize_board(
            agent,
            session.board_id,
            required_permission,
        )
        if access_error is not None:
            return None, access_error
        return session, None

    @mcp.tool()
    async def okto_pulse_kg_begin_consolidation(
        board_id: str,
        artifact_type: str,
        artifact_id: str,
        raw_content: str,
        deterministic_candidates: list[dict] | None = None,
    ) -> str:
        """Open a transactional consolidation session against a board.

        Computes SHA256(board + artifact + content) for nothing-changed detection
        (pass the full artifact content as raw_content). Returns the session_id
        used by all subsequent consolidation primitives, plus content_hash,
        nothing_changed and expires_at. The session has a TTL (default 1h,
        configurable via kg_session_ttl_seconds) and is owned exclusively by the
        authenticated agent.
        """
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        _board_agent, access_error = await _authorize_board(
            agent,
            board_id,
            "kg.session.begin",
        )
        if access_error is not None:
            return access_error
        try:
            req = BeginConsolidationRequest(
                board_id=board_id,
                artifact_type=artifact_type,
                artifact_id=artifact_id,
                raw_content=raw_content,
                deterministic_candidates=deterministic_candidates or [],
            )
        except ValidationError as e:
            return _err("invalid_candidate", _validation_message(e))
        # Spec R01A MCP-FU1 (MCP strangler): route through the transport-free use
        # case + injected MCP UnitOfWorkFactory (get_uow) instead of a raw get_db()
        # session. The commit primitive's internal persistence runs on the same
        # session, so write behaviour is unchanged.
        from okto_pulse.core.application.use_cases import (
            BeginConsolidationCommand,
            BeginConsolidationUseCase,
        )
        from okto_pulse.core.application.use_cases.base import ActorContext

        actor = ActorContext(agent.id, "mcp")
        try:
            async with get_uow()(actor=actor) as uow:
                result = await BeginConsolidationUseCase().execute(
                    BeginConsolidationCommand(req), actor=actor, uow=uow
                )
            return _ok(result.resp)
        except KGPrimitiveError as e:
            return _err(e.code, e.message, session_id=e.session_id,
                        details=e.details)

    @mcp.tool()
    async def okto_pulse_kg_add_node_candidate(
        session_id: str,
        candidate: dict,
    ) -> str:
        """Add a node candidate to an open consolidation session. The candidate dict
        (candidate_id, node_type, title, content, ...) stays in-memory until
        commit_consolidation or session expiry; candidate_id must be unique within
        the session.
        """
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        try:
            req = AddNodeCandidateRequest(session_id=session_id, candidate=candidate)
        except ValidationError as e:
            return _err("invalid_candidate", _validation_message(e))
        _session, access_error = await _authorized_session(
            req.session_id,
            agent,
            required_permission="kg.session.add_node",
        )
        if access_error is not None:
            return access_error
        try:
            resp = await add_node_candidate(req, agent_id=agent.id)
            return _ok(resp)
        except KGPrimitiveError as e:
            return _err(e.code, e.message, session_id=e.session_id, details=e.details)

    @mcp.tool()
    async def okto_pulse_kg_add_edge_candidate(
        session_id: str,
        candidate: dict,
    ) -> str:
        """Add an edge candidate to an open consolidation session.

        Endpoints (from_candidate_id / to_candidate_id) must reference either
        another in-session node candidate OR an existing persisted KG node via the
        'kg:' prefix (kg:decision_abc123).

        Cognitive agents may only propose judgement edges: supersedes,
        contradicts, depends_on, relates_to, validates. Deterministic edges
        (implements, tests, belongs_to, mentions, violates, derives_from) are
        reserved for the Layer 1 worker. Endpoint pairs are strict:
        Decision->Decision for supersedes/contradicts/depends_on,
        Decision->Alternative for relates_to, Learning->Bug for validates.
        Errors: layer_violation.
        """
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        try:
            req = AddEdgeCandidateRequest(session_id=session_id, candidate=candidate)
        except ValidationError as e:
            return _err("invalid_candidate", _validation_message(e))
        _session, access_error = await _authorized_session(
            req.session_id,
            agent,
            required_permission="kg.session.add_edge",
        )
        if access_error is not None:
            return access_error
        try:
            resp = await add_edge_candidate(req, agent_id=agent.id)
            return _ok(resp)
        except KGPrimitiveError as e:
            return _err(e.code, e.message, session_id=e.session_id, details=e.details)

    @mcp.tool()
    async def okto_pulse_kg_get_similar_nodes(
        session_id: str,
        candidate_id: str,
        top_k: int = 5,
        min_similarity: float = 0.3,
    ) -> str:
        """Fetch existing persisted KG nodes similar to an in-session candidate
        (top_k 1-50, default 5; min_similarity 0.0-1.0, default 0.3). MVP uses
        title-prefix match as a deterministic fallback; production replaces it
        with HNSW k-NN via the vector index (card 00dae72a).
        """
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        try:
            req = GetSimilarNodesRequest(
                session_id=session_id,
                candidate_id=candidate_id,
                top_k=top_k,
                min_similarity=min_similarity,
            )
        except ValidationError as e:
            return _err("invalid_candidate", _validation_message(e))
        _session, access_error = await _authorized_session(
            req.session_id,
            agent,
            required_permission="kg.session.get_similar",
        )
        if access_error is not None:
            return access_error
        try:
            resp = await get_similar_nodes(req, agent_id=agent.id)
            return _ok(resp)
        except KGPrimitiveError as e:
            return _err(e.code, e.message, session_id=e.session_id, details=e.details)

    @mcp.tool()
    async def okto_pulse_kg_propose_reconciliation(
        session_id: str,
    ) -> str:
        """Compute deterministic ADD/UPDATE/SUPERSEDE/NOOP hints for every candidate
        in the session. If the content SHA256 matches the last commit every hint
        is NOOP; otherwise ADD with the candidate's self-assessed confidence.
        UPDATE/SUPERSEDE hints will land once the HNSW index is in place.
        """
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        try:
            req = ProposeReconciliationRequest(session_id=session_id)
        except ValidationError as e:
            return _err("invalid_candidate", _validation_message(e))
        _session, access_error = await _authorized_session(
            req.session_id,
            agent,
            required_permission="kg.session.propose",
        )
        if access_error is not None:
            return access_error
        # Spec R01A MCP-FU1 (MCP strangler): transport-free use case + injected MCP
        # UnitOfWorkFactory (get_uow) instead of a raw get_db() session.
        from okto_pulse.core.application.use_cases import (
            ProposeReconciliationCommand,
            ProposeReconciliationUseCase,
        )
        from okto_pulse.core.application.use_cases.base import ActorContext

        actor = ActorContext(agent.id, "mcp")
        try:
            async with get_uow()(actor=actor) as uow:
                result = await ProposeReconciliationUseCase().execute(
                    ProposeReconciliationCommand(req), actor=actor, uow=uow
                )
            return _ok(result.resp)
        except KGPrimitiveError as e:
            return _err(e.code, e.message, session_id=e.session_id,
                        details=e.details)

    @mcp.tool()
    async def okto_pulse_kg_commit_consolidation(
        session_id: str,
        summary_text: str = "",
        agent_overrides: dict[str, dict] | None = None,
    ) -> str:
        """Atomically commit the session: graph-store writes + audit row + outbox
        event. agent_overrides map candidate_id -> ReconciliationHint when the
        agent's semantic reasoning differs from the server's deterministic
        default; summary_text is surfaced in the dashboard.
        """
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        try:
            req = CommitConsolidationRequest(
                session_id=session_id,
                summary_text=summary_text or None,
                agent_overrides=agent_overrides or {},
            )
        except ValidationError as e:
            return _err("invalid_candidate", _validation_message(e))
        # Resolve the session before taking the commit lock so we can key the
        # lock on the correct board_id. _require_open_session also enforces
        # ownership — if it raises here the agent hears the same error they
        # would have heard from commit_consolidation directly.
        session, access_error = await _authorized_session(
            req.session_id,
            agent,
            allow_pending_commit=True,
            required_permission="kg.session.commit",
        )
        if access_error is not None:
            return access_error
        assert session is not None
        # Spec R01A MCP-FU1 (MCP strangler): transport-free use case + injected MCP
        # UnitOfWorkFactory. The outer guarded boundary owns the real per-board
        # writer fence and keeps it through graph durability, relational commit,
        # finalization, or compensation.
        from okto_pulse.core.application.use_cases import (
            CommitConsolidationCommand,
            CommitConsolidationUseCase,
        )
        from okto_pulse.core.application.use_cases.base import ActorContext

        actor = ActorContext(agent.id, "mcp")
        try:
            release_after_rollback = False
            relational_commit_confirmed = False
            with guarded_board_write(
                session.board_id,
                operation="mcp_commit_consolidation",
                owner_id=str(agent.id),
                mutation_ref=f"consolidation:{req.session_id}",
            ) as write_lease:
                try:
                    async with get_uow()(actor=actor) as uow:
                        try:
                            result = await CommitConsolidationUseCase().execute(
                                CommitConsolidationCommand(
                                    req,
                                    board_id=session.board_id,
                                ),
                                actor=actor,
                                uow=uow,
                            )
                            # execute() returned with this caller's deferred
                            # graph snapshot installed. Mark ownership before
                            # durability: if that lifecycle fails, the catch
                            # below must still compensate the graph while this
                            # same board fence is held.
                            release_after_rollback = True
                        finally:
                            # A primitive failure may follow an embedded
                            # auto-commit and its compensation. Drain both
                            # possibilities before this fence can be released.
                            await run_blocking_graph_io(
                                write_lease.ensure_durable,
                                task_name=(
                                    "core.kg.mcp_commit_graph_durability"
                                ),
                            )
                        async def _commit_and_finalize() -> None:
                            nonlocal relational_commit_confirmed
                            write_lease.ensure_owned(
                                failure_phase="before_relational_ack",
                            )
                            await uow.commit()
                            relational_commit_confirmed = True
                            await finalize_deferred_consolidation(
                                req.session_id,
                                agent_id=agent.id,
                            )
                            write_lease.ensure_owned(
                                failure_phase="after_relational_ack",
                            )

                        await run_cancellation_atomic(
                            _commit_and_finalize(),
                            task_name="core.kg.mcp_commit_and_finalize",
                        )
                except BaseException:
                    # Only this caller's graph-applied deferred snapshot is
                    # compensated. Execute-time contention never owns another
                    # invocation's pending claim.
                    if release_after_rollback:
                        try:

                            async def _cleanup_deferred_commit() -> None:
                                if relational_commit_confirmed:
                                    # Relational durability is already
                                    # established: retry only idempotent
                                    # terminal cleanup.
                                    await finalize_deferred_consolidation(
                                        req.session_id,
                                        agent_id=agent.id,
                                    )
                                else:
                                    # A client may never retry after this
                                    # response. Compensate graph auto-commits
                                    # while the same writer fence is still held.
                                    await abort_deferred_consolidation(
                                        req.session_id,
                                        agent_id=agent.id,
                                    )

                            await run_cancellation_atomic(
                                _cleanup_deferred_commit(),
                                task_name="core.kg.mcp_deferred_cleanup",
                            )
                            if not relational_commit_confirmed:
                                # The compensation is itself a graph mutation;
                                # best-effort durability must run under the same
                                # fence without replacing the original error.
                                try:
                                    await run_blocking_graph_io(
                                        partial(
                                            write_lease.ensure_durable,
                                            mutation_ref=(
                                                "consolidation-abort:"
                                                f"{req.session_id}"
                                            ),
                                        ),
                                        task_name=(
                                            "core.kg.mcp_abort_graph_durability"
                                        ),
                                    )
                                except GuardedWriteError:
                                    logger.warning(
                                        "kg.deferred_commit.compensation_"
                                        "lifecycle_failed session=%s",
                                        req.session_id,
                                        exc_info=True,
                                    )
                        except BaseException:
                            logger.warning(
                                "kg.deferred_commit.cleanup_failed session=%s",
                                req.session_id,
                                exc_info=True,
                            )
                    raise
            return _ok(result.resp)
        except GuardedWriteError as e:
            return _err(
                e.code,
                e.message,
                session_id=req.session_id,
                retryable=e.retryable,
                details=e.details,
            )
        except KGPrimitiveError as e:
            # R7: a working-only canonical Learning bug-derived commit is an
            # EXPECTED semantic hold — materialize the go-forward hold in the
            # cognitive pending ledger (never CanonicalDebt/DLQ) before
            # surfacing the structured error back to the agent.
            _maybe_record_r7_cognitive_hold(
                board_id=session.board_id, error=e, actor_id=agent.id
            )
            return _err(e.code, e.message, session_id=e.session_id,
                        details=e.details)

    @mcp.tool()
    async def okto_pulse_kg_list_cognitive_pending_items(
        board_id: str,
        kg_generation_id: str | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
        status_filter: str | None = None,
    ) -> str:
        """KG-03.2 — List cognitive pending items by board + generation (api_ae3a932a).
Resolves to the latest generation when kg_generation_id is omitted; an explicit
missing generation returns generation_not_found. Items use a strict API projection
(contract fields only; storage-only fields never echoed). Filters: status from
{pending,in_progress,consolidated,skipped,failed}; limit 1..200 (default 100);
offset >= 0. Full args: okto-pulse://reference/tool-docs/kg."""
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")

        # ``status`` is the contract parameter; ``status_filter`` is kept
        # only as a compatibility alias and ignored when ``status`` is set.
        effective_status = status if status is not None else status_filter
        status_present = effective_status is not None

        if not isinstance(board_id, str) or not board_id:
            _emit_list_sample(
                surface=CognitiveItemListSurface.MCP.value,
                board_id="",
                outcome=CognitiveItemListOutcome.VALIDATION_ERROR.value,
                status_filter_present=status_present,
                reason_code=CognitiveItemListReasonCode.MISSING_BOARD_ID.value,
                item_count=0,
            )
            return _err(
                "missing_board_id",
                "board_id must be a non-empty string",
                reason_code=CognitiveItemListReasonCode.MISSING_BOARD_ID.value,
            )

        _board_agent, access_error = await _authorize_board(
            agent,
            board_id,
            "board.read",
        )
        if access_error is not None:
            return access_error

        if status_present and effective_status not in _VALID_LIST_STATUS_FILTERS:
            _emit_list_sample(
                surface=CognitiveItemListSurface.MCP.value,
                board_id=board_id,
                outcome=CognitiveItemListOutcome.VALIDATION_ERROR.value,
                status_filter_present=True,
                reason_code=CognitiveItemListReasonCode.INVALID_STATUS.value,
                item_count=0,
            )
            return _err(
                "invalid_status",
                (
                    "status must be one of "
                    f"{sorted(_VALID_LIST_STATUS_FILTERS)}"
                ),
                provided=effective_status,
                reason_code=CognitiveItemListReasonCode.INVALID_STATUS.value,
            )

        # Bounded limit/offset per api_ae3a932a.
        if not isinstance(limit, int) or not 1 <= limit <= 200:
            return _err(
                "invalid_limit",
                "limit must be an integer in [1, 200]",
                provided=limit,
            )
        if not isinstance(offset, int) or offset < 0:
            return _err(
                "invalid_offset",
                "offset must be a non-negative integer",
                provided=offset,
            )

        store = CognitiveConsolidationItemStore(
            artifact_store=require_rebuild_audit_artifact_store()
        )

        explicit_generation = bool(kg_generation_id)
        resolved_generation = (
            kg_generation_id
            if explicit_generation
            else store.latest_generation(board_id)
        )

        if explicit_generation and not store.record_exists(
            board_id, resolved_generation
        ):
            # Codex audit val_ead80fbd: explicit gen + missing record =
            # typed generation_not_found, not silent empty success.
            _emit_list_sample(
                surface=CognitiveItemListSurface.MCP.value,
                board_id=board_id,
                outcome=CognitiveItemListOutcome.NOT_FOUND.value,
                status_filter_present=status_present,
                reason_code=CognitiveItemListReasonCode.NO_GENERATION_FOUND.value,
                item_count=0,
            )
            return _err(
                "generation_not_found",
                "no cognitive pending record exists for the requested generation",
                board_id=board_id,
                kg_generation_id=resolved_generation,
                reason_code=CognitiveItemListReasonCode.NO_GENERATION_FOUND.value,
            )

        if resolved_generation is None:
            # No explicit gen + no latest = empty board (safe empty per
            # Codex audit: keep the friendly response for THIS case).
            _emit_list_sample(
                surface=CognitiveItemListSurface.MCP.value,
                board_id=board_id,
                outcome=CognitiveItemListOutcome.NOT_FOUND.value,
                status_filter_present=status_present,
                reason_code=CognitiveItemListReasonCode.NO_GENERATION_FOUND.value,
                item_count=0,
            )
            return json.dumps({
                "board_id": board_id,
                "selected_kg_generation_id": None,
                "legacy_mode": False,
                "counts": empty_status_counts(),
                "items": [],
            }, default=str)

        legacy_mode = store.is_legacy_record(board_id, resolved_generation)
        page_items = store.list_items(
            board_id,
            resolved_generation,
            status_filter=effective_status,
            limit=limit,
            offset=offset,
        )

        counts = compute_status_counts(page_items)
        item_count = counts["total"]

        _emit_list_sample(
            surface=CognitiveItemListSurface.MCP.value,
            board_id=board_id,
            outcome=CognitiveItemListOutcome.SUCCESS.value,
            status_filter_present=status_present,
            reason_code=CognitiveItemListReasonCode.NONE.value,
            item_count=item_count,
        )
        # or_b8ff0cc2: count this operational-inspection listing (cognitive
        # pending domain) so absence of drill-down usage is diagnosable.
        emit_operational_inspection_sample(
            signal="cognitive_pending", surface="mcp", outcome="success",
            board_id=board_id, item_count=item_count,
        )

        return json.dumps({
            "board_id": board_id,
            "selected_kg_generation_id": resolved_generation,
            "legacy_mode": legacy_mode,
            "counts": counts,
            "items": [project_item_for_api(item) for item in page_items],
        }, default=str)

    @mcp.tool()
    async def okto_pulse_kg_update_cognitive_pending_item(
        board_id: str,
        kg_generation_id: str,
        item_id: str,
        status: str,
        consolidation_session_id: str | None = None,
        reason: str | None = None,
        summary_text: str | None = None,
        outcome_type: str | None = None,
        evidence_refs: list[str] | None = None,
        generated_candidate_decision_ids: list[str] | None = None,
        promoted_formal_decision_ids: list[str] | None = None,
    ) -> str:
        """KG-03.3 — Mutate exactly one cognitive consolidation item (api_525a25f1).
Invariants: status=consolidated needs a consolidation_session_id from a prior
commit_consolidation; status=skipped/failed need a reason; token shapes and
oversized narrative are rejected as unsafe_payload so raw bodies never enter the
ledger. Atomic single-item update; aggregate counts recomputed. Emits
kg_cognitive_item_update_total with bounded labels (free-text reason never
labelled). Full args/contract/invariants: okto-pulse://reference/tool-docs/kg."""
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")

        # Compute the bounded target_status label up front so the counter
        # observes a finite label set even on invalid input.
        target_status_label = (
            status
            if isinstance(status, str) and status in _VALID_LIST_STATUS_FILTERS
            else "invalid"
        )
        safe_board_id = board_id if isinstance(board_id, str) and board_id else ""

        def _reject(
            code: str,
            message: str,
            *,
            outcome: str,
            reason_code: str,
            **extra: Any,
        ) -> str:
            _emit_update_sample(
                board_id=safe_board_id,
                target_status=target_status_label,
                outcome=outcome,
                reason_code=reason_code,
            )
            return _err(code, message, reason_code=reason_code, **extra)

        if not isinstance(board_id, str) or not board_id:
            return _reject(
                "missing_board_id",
                "board_id must be a non-empty string",
                outcome=CognitiveItemUpdateOutcome.VALIDATION_ERROR.value,
                reason_code=CognitiveItemUpdateReasonCode.MISSING_BOARD_ID.value,
            )
        _board_agent, access_error = await _authorize_board(
            agent,
            board_id,
            "kg.session.commit",
        )
        if access_error is not None:
            return access_error
        if not isinstance(kg_generation_id, str) or not kg_generation_id:
            return _reject(
                "missing_kg_generation_id",
                "kg_generation_id must be a non-empty string",
                outcome=CognitiveItemUpdateOutcome.VALIDATION_ERROR.value,
                reason_code=CognitiveItemUpdateReasonCode.MISSING_GENERATION_ID.value,
            )
        if not isinstance(item_id, str) or not item_id:
            return _reject(
                "missing_item_id",
                "item_id must be a non-empty string",
                outcome=CognitiveItemUpdateOutcome.VALIDATION_ERROR.value,
                reason_code=CognitiveItemUpdateReasonCode.MISSING_ITEM_ID.value,
            )

        if status not in _VALID_LIST_STATUS_FILTERS:
            return _reject(
                "invalid_status",
                (
                    "status must be one of "
                    f"{sorted(_VALID_LIST_STATUS_FILTERS)}"
                ),
                outcome=CognitiveItemUpdateOutcome.VALIDATION_ERROR.value,
                reason_code=CognitiveItemUpdateReasonCode.INVALID_STATUS.value,
                provided=status,
            )

        # br_858a0859 — reject unsafe narrative payloads BEFORE any
        # content validation so security checks fail fast even when the
        # consolidated/skipped/failed gates would otherwise reject first.
        # KG-03A.3 rework: outcome metadata lists are also validated
        # (Codex audit val_44b86726).
        unsafe, unsafe_field = detect_unsafe_update_payload(
            consolidation_session_id=consolidation_session_id,
            reason=reason,
            summary_text=summary_text,
            evidence_refs=evidence_refs,
            generated_candidate_decision_ids=generated_candidate_decision_ids,
            promoted_formal_decision_ids=promoted_formal_decision_ids,
        )
        if unsafe:
            # or_03222a4f — alert metric: any non-zero value in
            # production must be investigated. The label set is bounded
            # (surface + board_id + reason enum).
            _emit_unsafe_payload_sample(
                surface=CognitiveUnsafePayloadSurface.MCP_UPDATE.value,
                board_id=safe_board_id,
                reason=(
                    CognitiveUnsafePayloadReason(unsafe_field).value
                    if unsafe_field in {r.value for r in CognitiveUnsafePayloadReason}
                    else CognitiveUnsafePayloadReason.OTHER.value
                ),
            )
            return _reject(
                "unsafe_payload",
                "request contains disallowed raw or sensitive content",
                outcome=CognitiveItemUpdateOutcome.VALIDATION_ERROR.value,
                reason_code=CognitiveItemUpdateReasonCode.UNSAFE_PAYLOAD.value,
                unsafe_field=unsafe_field,
            )

        # br_689bdf14 — consolidated requires consolidation_session_id.
        if status == CognitiveItemStatus.CONSOLIDATED.value and (
            not isinstance(consolidation_session_id, str)
            or not consolidation_session_id.strip()
        ):
            return _reject(
                "consolidation_session_required",
                "consolidated status requires consolidation_session_id",
                outcome=CognitiveItemUpdateOutcome.VALIDATION_ERROR.value,
                reason_code=CognitiveItemUpdateReasonCode.CONSOLIDATION_SESSION_REQUIRED.value,
            )

        # KG-03A.3 — outcome_type validation (br_7500e5f9 + tr_16ec917c).
        # When consolidated, the agent MUST attribute one of the bounded
        # outcome types. ``no_action_required`` is accepted but also
        # requires a justifying ``reason`` (br_f9823bad-style).
        if status == CognitiveItemStatus.CONSOLIDATED.value:
            if outcome_type is None or (
                isinstance(outcome_type, str) and not outcome_type.strip()
            ):
                return _reject(
                    "outcome_required",
                    (
                        "consolidated status requires outcome_type from "
                        f"{sorted(_VALID_OUTCOME_TYPES)}"
                    ),
                    outcome=CognitiveItemUpdateOutcome.VALIDATION_ERROR.value,
                    reason_code=CognitiveItemUpdateReasonCode.OUTCOME_REQUIRED.value,
                )
            if outcome_type not in _VALID_OUTCOME_TYPES:
                return _reject(
                    "invalid_outcome_type",
                    (
                        "outcome_type must be one of "
                        f"{sorted(_VALID_OUTCOME_TYPES)}"
                    ),
                    outcome=CognitiveItemUpdateOutcome.VALIDATION_ERROR.value,
                    reason_code=CognitiveItemUpdateReasonCode.INVALID_OUTCOME_TYPE.value,
                    provided=outcome_type,
                )
            # no_action_required outcome demands an auditable reason
            # (no silent empty consolidations — br_7500e5f9).
            if outcome_type == CognitivePendingOutcomeType.NO_ACTION_REQUIRED.value and (
                not isinstance(reason, str) or not reason.strip()
            ):
                return _reject(
                    "reason_required",
                    (
                        "outcome_type=no_action_required requires a "
                        "non-empty reason justifying the empty consolidation"
                    ),
                    outcome=CognitiveItemUpdateOutcome.VALIDATION_ERROR.value,
                    reason_code=CognitiveItemUpdateReasonCode.REASON_REQUIRED.value,
                )

        # br_f9823bad — skipped/failed require non-empty reason.
        if status in (
            CognitiveItemStatus.SKIPPED.value,
            CognitiveItemStatus.FAILED.value,
        ) and (not isinstance(reason, str) or not reason.strip()):
            return _reject(
                "reason_required",
                "skipped or failed status requires a non-empty reason",
                outcome=CognitiveItemUpdateOutcome.VALIDATION_ERROR.value,
                reason_code=CognitiveItemUpdateReasonCode.REASON_REQUIRED.value,
            )

        store = CognitiveConsolidationItemStore(
            artifact_store=require_rebuild_audit_artifact_store()
        )

        # AC9 / IR3 — this MCP tool is AGENT-facing (get_agent). An R7 canonical
        # Learning partition-integrity HOLD/debt item (item.reason_code in
        # R7_HOLD_REASON_CODES) may ONLY be skipped/cleared by an explicit human
        # action via the REST surface; an agent must never mutate it here and
        # mask the hold. Fail-closed BEFORE the ledger write.
        _r7_current = next(
            (
                it
                for it in store.list_items(board_id, kg_generation_id)
                if it.item_id == item_id
            ),
            None,
        )
        if (
            _r7_current is not None
            and str(_r7_current.reason_code or "") in R7_HOLD_REASON_CODES
        ):
            return _reject(
                "human_only_reason_code",
                (
                    "this item is an R7 canonical Learning partition-integrity "
                    "hold/debt; only an explicit human action may skip or clear it"
                ),
                outcome=CognitiveItemUpdateOutcome.VALIDATION_ERROR.value,
                reason_code=CognitiveItemUpdateReasonCode.NONE.value,
            )

        try:
            updated_item = store.update_item(
                board_id=board_id,
                kg_generation_id=kg_generation_id,
                item_id=item_id,
                new_status=status,
                updated_by_agent_id=str(agent.id),
                consolidation_session_id=consolidation_session_id,
                reason=reason,
                outcome_type=outcome_type,
                evidence_refs=evidence_refs,
                generated_candidate_decision_ids=generated_candidate_decision_ids,
                promoted_formal_decision_ids=promoted_formal_decision_ids,
            )
        except Exception as exc:
            _emit_update_sample(
                board_id=safe_board_id,
                target_status=target_status_label,
                outcome=CognitiveItemUpdateOutcome.STORE_ERROR.value,
                reason_code=CognitiveItemUpdateReasonCode.NONE.value,
            )
            return _err(
                "store_error",
                f"ledger update failed: {type(exc).__name__}",
                reason_code=CognitiveItemUpdateReasonCode.NONE.value,
            )

        if updated_item is None:
            return _reject(
                "item_not_found",
                "item_id was not found in the requested generation",
                outcome=CognitiveItemUpdateOutcome.ITEM_NOT_FOUND.value,
                reason_code=CognitiveItemUpdateReasonCode.ITEM_NOT_FOUND.value,
            )

        # Recompute generation counts from the post-update ledger.
        full_items = store.list_items(board_id, kg_generation_id)
        counts = compute_status_counts(full_items)

        _emit_update_sample(
            board_id=safe_board_id,
            target_status=target_status_label,
            outcome=CognitiveItemUpdateOutcome.UPDATED.value,
            reason_code=CognitiveItemUpdateReasonCode.NONE.value,
        )

        return json.dumps({
            "board_id": board_id,
            "kg_generation_id": kg_generation_id,
            "updated": True,
            "item": project_item_for_update_api(updated_item),
            "counts": counts,
        }, default=str)

    @mcp.tool()
    async def okto_pulse_kg_abort_consolidation(
        session_id: str,
        reason: str = "",
    ) -> str:
        """Drop an in-flight consolidation session without committing. No
        compensating delete is applied — commit never ran, so the graph store has
        no partial writes. The session is marked aborted (reason is logged for
        audit) and removed from the in-memory registry.
        """
        agent = await get_agent()
        if agent is None:
            return _err("unauthorized", "authentication required")
        try:
            req = AbortConsolidationRequest(
                session_id=session_id, reason=reason or None
            )
        except ValidationError as e:
            return _err("invalid_candidate", _validation_message(e))
        _session, access_error = await _authorized_session(
            req.session_id,
            agent,
            required_permission="kg.session.abort",
        )
        if access_error is not None:
            return access_error
        try:
            resp = await abort_consolidation(req, agent_id=agent.id)
            return _ok(resp)
        except KGPrimitiveError as e:
            return _err(e.code, e.message, session_id=e.session_id, details=e.details)
