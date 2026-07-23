"""Transaction-scoped Core KG operation catalog."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import Any

from okto_pulse.core.kg.curation_policy import CurationPolicyError
from okto_pulse.core.kg.graph_export import GraphExportError


logger = logging.getLogger(__name__)


async def _takedown_e2e_health(
    snapshot: object,
    *,
    relational_context: object,
) -> dict[str, object]:
    """Evaluate the Card 9 health predicate from independent evidence.

    Delivery truth comes from the durable telemetry timeline.  Parity truth is
    read independently from the board graph/source census; an unavailable or
    malformed probe is explicit and can never be interpreted as healthy.
    """

    from okto_pulse.core.kg.stale_canonical_parity import (
        GD_EVALUATED,
        list_stale_canonical_parity,
    )
    from okto_pulse.core.ports.takedown_telemetry import (
        TAKEDOWN_STATE_RANK,
        TakedownState,
        TakedownTelemetrySnapshot,
    )

    if not isinstance(snapshot, TakedownTelemetrySnapshot):
        raise RuntimeError("takedown_telemetry_snapshot_invalid")

    attempt_states = {
        TakedownState.OUTBOX_PERSISTED,
        TakedownState.DELIVERY_DEBT,
        TakedownState.DELIVERED,
    }
    delivery_transitions = tuple(
        transition
        for transition in snapshot.states
        if transition.state in attempt_states
    )
    final_delivery = (
        max(
            delivery_transitions,
            key=lambda transition: (
                transition.attempt if transition.attempt is not None else -1,
                transition.occurred_at,
                TAKEDOWN_STATE_RANK[transition.state],
                transition.transition_key,
            ),
        )
        if delivery_transitions
        else None
    )
    delivered = (
        final_delivery is not None
        and final_delivery.state is TakedownState.DELIVERED
    )
    final_delivery_state = (
        final_delivery.state.value if final_delivery is not None else None
    )
    final_delivery_attempt = (
        final_delivery.attempt if final_delivery is not None else None
    )

    matching_stale_count: int | None = None
    board_stale_count: int | None = None
    digest_stale_count: int | None = None
    global_discovery_evaluation: str | None = None
    parity_evaluable = False
    parity_clean: bool | None = None
    board_parity_clean: bool | None = None
    global_discovery_parity_clean: bool | None = None
    probe_error_class: str | None = None
    try:
        probe = await list_stale_canonical_parity(
            relational_context,
            board_id=snapshot.board_id,
            limit=200,
            offset=0,
        )
        if (
            not isinstance(probe, Mapping)
            or probe.get("board_id") != snapshot.board_id
            or probe.get("mutation_allowed") is not False
        ):
            raise RuntimeError("takedown_parity_probe_result_invalid")
        stale_items = probe.get("items")
        raw_board_stale_count = probe.get("count")
        raw_digest_stale_count = probe.get(
            "global_discovery_stale_digest_count"
        )
        if (
            not isinstance(stale_items, list)
            or any(not isinstance(item, Mapping) for item in stale_items)
            or isinstance(raw_board_stale_count, bool)
            or not isinstance(raw_board_stale_count, int)
            or raw_board_stale_count < 0
            or isinstance(raw_digest_stale_count, bool)
            or not isinstance(raw_digest_stale_count, int)
            or raw_digest_stale_count < 0
        ):
            raise RuntimeError("takedown_parity_probe_result_invalid")
        board_stale_count = raw_board_stale_count
        digest_stale_count = raw_digest_stale_count
        global_discovery_evaluation = str(
            probe.get("global_discovery_evaluation") or ""
        )
        source_ref = f"{snapshot.artifact_type}:{snapshot.artifact_id}"
        matching_stale_count = sum(
            1
            for item in stale_items
            if item.get("source_artifact_ref") == source_ref
        )
        board_parity_clean = board_stale_count == 0
        if global_discovery_evaluation == GD_EVALUATED:
            global_discovery_parity_clean = digest_stale_count == 0
            parity_evaluable = True
            parity_clean = (
                board_parity_clean and global_discovery_parity_clean
            )
    except Exception as exc:
        probe_error_class = type(exc).__name__
        logger.warning(
            "kg.takedown.parity_probe_unavailable board_id=%s "
            "delete_event_id=%s delivery_key=%s error_class=%s",
            snapshot.board_id,
            snapshot.delete_event_id,
            snapshot.delivery_key,
            probe_error_class,
            extra={
                "event": "kg.takedown.parity_probe_unavailable",
                "board_id": snapshot.board_id,
                "delete_event_id": snapshot.delete_event_id,
                "delivery_key": snapshot.delivery_key,
                "artifact_type": snapshot.artifact_type,
                "artifact_id": snapshot.artifact_id,
                "generation": snapshot.generation,
                "probe_error_class": probe_error_class,
            },
        )

    failure_reasons: list[str] = []
    if not delivered:
        failure_reasons.append("delivered_state_not_observed")
    if not parity_evaluable:
        failure_reasons.append("parity_probe_not_evaluable")
    else:
        if not board_parity_clean:
            failure_reasons.append("stale_canonical_parity_detected")
        if not global_discovery_parity_clean:
            failure_reasons.append(
                "global_discovery_parity_mismatch_detected"
            )

    return {
        "predicate": "delivered_state_and_evaluable_parity_probe",
        "healthy": not failure_reasons,
        "delivered": delivered,
        "final_delivery_state": final_delivery_state,
        "final_delivery_attempt": final_delivery_attempt,
        "parity_probe_evaluable": parity_evaluable,
        "parity_clean": parity_clean,
        "board_parity_clean": board_parity_clean,
        "global_discovery_parity_clean": global_discovery_parity_clean,
        "board_stale_node_count": board_stale_count,
        "global_discovery_stale_digest_count": digest_stale_count,
        "global_discovery_evaluation": global_discovery_evaluation,
        "matching_stale_node_count": matching_stale_count,
        "failure_reasons": failure_reasons,
        "probe_error_class": probe_error_class,
    }


class CoreKnowledgeGraphOperations:
    def __init__(
        self,
        relational_context: object,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.__relational_context = relational_context
        self.__clock = clock or (lambda: datetime.now(timezone.utc))

    async def dispatch_manual_tick(
        self,
        *,
        tick_id: str,
        board_id: str | None,
        force_full_rebuild: bool,
        scheduled_at: str | None = None,
    ) -> list[str]:
        from okto_pulse.core.application.kg_tick import dispatch_manual_tick

        return await dispatch_manual_tick(
            tick_id=tick_id,
            board_id=board_id,
            force_full_rebuild=force_full_rebuild,
            relational_context=self.__relational_context,
            scheduled_at=scheduled_at,
        )

    async def evaluate_bug_cognitive_closure(
        self, readiness_service: object, **request: object
    ) -> dict[str, object]:
        from okto_pulse.core.services.application_kg import (
            evaluate_bug_cognitive_closure,
        )
        from okto_pulse.core.ports.bug_cognitive_context import (
            BugCognitiveContext,
            resolve_bug_cognitive_context_assembler,
        )

        # The edition owns all relational/graph reads.  Both REST and MCP enter
        # through this operation, so they receive the exact same immutable
        # snapshot.  Direct Core policy tests may inject ``bug_context``.
        if "bug_context" not in request:
            assembler = resolve_bug_cognitive_context_assembler()
            if assembler is not None:
                board_id = str(request.get("board_id") or "")
                bug_id = str(request.get("bug_id") or "")
                try:
                    request["bug_context"] = await assembler.assemble(
                        self.__relational_context,
                        board_id=board_id,
                        bug_id=bug_id,
                    )
                except Exception as exc:
                    request["bug_context"] = BugCognitiveContext.unavailable(
                        board_id=board_id,
                        bug_id=bug_id,
                        error=f"context_assembly_failed:{type(exc).__name__}",
                    )

        return await evaluate_bug_cognitive_closure(
            readiness_service,
            self.__relational_context,
            **request,
        )

    async def list_cognitive_signals(
        self, readiness_service: object, **query: object
    ) -> dict[str, object]:
        from okto_pulse.core.services.application_kg import (
            build_cognitive_action_center_read_model,
        )

        read_model = build_cognitive_action_center_read_model(readiness_service)
        return await read_model.list_signals(self.__relational_context, **query)

    async def evaluate_cognitive_readiness(
        self, readiness_service: object, **request: object
    ):  # noqa: ANN201
        return await readiness_service.evaluate_artifact(
            self.__relational_context,
            **request,
        )

    async def cognitive_enforcement_active(self, board_id: str) -> bool:
        from okto_pulse.core.services.main import cognitive_enforcement_active

        return await cognitive_enforcement_active(self.__relational_context, board_id)

    async def record_cognitive_skip(
        self, readiness_service: object, **request: object
    ):  # noqa: ANN201
        return await readiness_service.record_cognitive_skip(
            self.__relational_context,
            **request,
        )

    async def clear_cognitive_skip(
        self, readiness_service: object, **request: object
    ):  # noqa: ANN201
        return await readiness_service.clear_cognitive_skip(
            self.__relational_context,
            **request,
        )

    async def cognitive_readiness_metrics(
        self,
        readiness_service: object,
        *,
        board_id: str,
        kg_generation_id: str | None,
    ) -> dict[str, object]:
        from okto_pulse.core.services.application_kg import (
            build_cognitive_action_center_read_model,
        )

        read_model = build_cognitive_action_center_read_model(readiness_service)
        return await read_model.metrics(
            self.__relational_context,
            board_id=board_id,
            kg_generation_id=kg_generation_id,
        )

    async def cognitive_effectiveness_inventory(
        self,
        board_id: str,
        *,
        artifact_id: str | None,
        include_candidate_logs: bool,
        graph_layer: str,
        metric_status: object,
    ) -> dict[str, object]:
        from okto_pulse.core.services.cognitive_effectiveness_service import (
            build_cognitive_effectiveness_inventory,
        )

        return await build_cognitive_effectiveness_inventory(
            self.__relational_context,
            board_id,
            artifact_id=artifact_id,
            include_candidate_logs=include_candidate_logs,
            graph_layer=graph_layer,
            metric_status=metric_status,
        )

    async def invoke_health_reader(
        self,
        reader: object,
        board_id: str,
        *,
        scheduler_control: object | None,
    ) -> dict[str, object]:
        if not isinstance(reader, Callable):
            raise TypeError("health reader must be callable")
        return await reader(
            board_id,
            self.__relational_context,
            scheduler_control=scheduler_control,
        )

    async def begin_consolidation(self, request: object, *, agent_id: str):  # noqa: ANN201
        from okto_pulse.core.services.application_kg import begin_consolidation

        return await begin_consolidation(
            request,
            agent_id=agent_id,
            db=self.__relational_context,
        )

    async def propose_reconciliation(
        self, request: object, *, agent_id: str
    ):  # noqa: ANN201
        from okto_pulse.core.services.application_kg import propose_reconciliation

        return await propose_reconciliation(
            request,
            agent_id=agent_id,
            db=self.__relational_context,
        )

    async def commit_consolidation(
        self,
        request: object,
        *,
        board_id: str,
        agent_id: str,
        defer_session_finalization: bool = False,
    ):  # noqa: ANN201
        from okto_pulse.core.services.application_kg import (
            commit_consolidation,
            run_with_commit_lock_and_retry,
        )

        return await run_with_commit_lock_and_retry(
            board_id,
            lambda: commit_consolidation(
                request,
                agent_id=agent_id,
                db=self.__relational_context,
                defer_session_finalization=defer_session_finalization,
            ),
        )

    async def health(
        self, board_id: str, *, scheduler_control: object | None = None
    ) -> dict[str, object]:
        from okto_pulse.core.services.kg_health_service import get_kg_health

        return await get_kg_health(
            board_id,
            self.__relational_context,
            scheduler_control=scheduler_control,
        )

    async def health_readiness(
        self,
        board_id: str,
        *,
        profile: str,
        surface: str,
        artifact_ref: str | None,
        scheduler_control: object | None = None,
    ) -> dict[str, object]:
        from okto_pulse.core.services.kg_health_readiness_service import (
            build_health_readiness,
        )

        return await build_health_readiness(
            board_id,
            self.__relational_context,
            profile=profile,
            surface=surface,
            artifact_ref=artifact_ref,
            scheduler_control=scheduler_control,
        )

    async def list_consolidation_audit(
        self, board_id: str, *, limit: int
    ):  # noqa: ANN201
        from okto_pulse.core.services.application_kg import list_consolidation_audit

        return await list_consolidation_audit(
            self.__relational_context,
            board_id,
            limit=limit,
        )

    async def start_historical_consolidation(self, board_id: str):  # noqa: ANN201
        from okto_pulse.core.services.application_kg import start_historical_consolidation

        return await start_historical_consolidation(self.__relational_context, board_id)

    async def cancel_historical(self, board_id: str):  # noqa: ANN201
        from okto_pulse.core.services.application_kg import cancel_historical

        return await cancel_historical(self.__relational_context, board_id)

    async def get_historical_progress(self, board_id: str):  # noqa: ANN201
        from okto_pulse.core.services.application_kg import get_historical_progress

        return await get_historical_progress(self.__relational_context, board_id)

    async def right_to_erasure(self, board_id: str):  # noqa: ANN201
        from okto_pulse.core.services.application_kg import right_to_erasure

        return await right_to_erasure(self.__relational_context, board_id)

    async def list_pending_entries(self, board_id: str):  # noqa: ANN201
        from okto_pulse.core.services.application_kg import list_pending_entries

        return await list_pending_entries(self.__relational_context, board_id)

    async def build_pending_tree(self, board_id: str, *, depth: int):  # noqa: ANN201
        from okto_pulse.core.services.application_kg import build_pending_tree

        return await build_pending_tree(
            self.__relational_context,
            board_id,
            depth=depth,
        )

    async def retry_pending_entry(
        self, board_id: str, queue_entry_id: str, *, recursive: bool
    ):  # noqa: ANN201
        from okto_pulse.core.services.application_kg import retry_pending_entry

        return await retry_pending_entry(
            self.__relational_context,
            board_id,
            queue_entry_id,
            recursive=recursive,
        )

    async def boost_node(
        self, board_id: str, node_id: str, *, actor_id: str
    ):  # noqa: ANN201
        from okto_pulse.core.services.application_kg import boost_node

        return await boost_node(
            self.__relational_context,
            board_id,
            node_id,
            actor_id=actor_id,
        )

    async def reprocess_dead_letter_rows(
        self,
        board_id: str,
        *,
        dead_letter_ids: list[str] | None,
        limit: int,
    ) -> dict[str, object]:
        from okto_pulse.core.services.dead_letter_inspector_service import (
            reprocess_dead_letter_rows,
        )

        return await reprocess_dead_letter_rows(
            self.__relational_context,
            board_id,
            dead_letter_ids=dead_letter_ids,
            limit=limit,
        )

    async def diagnose_connectivity_guard_dlq(
        self, board_id: str
    ) -> dict[str, object]:
        from okto_pulse.core.services.connectivity_dlq_reprocess_service import (
            diagnose_connectivity_guard_dlq,
        )

        return await diagnose_connectivity_guard_dlq(
            self.__relational_context,
            board_id,
        )

    async def reprocess_connectivity_guard_dlq(
        self, board_id: str, dead_letter_ids: list[str]
    ) -> dict[str, object]:
        from okto_pulse.core.services.connectivity_dlq_reprocess_service import (
            reprocess_connectivity_guard_dlq,
        )

        return await reprocess_connectivity_guard_dlq(
            self.__relational_context,
            board_id,
            dead_letter_ids,
        )

    async def verify_connectivity_class_cleared(
        self, board_id: str, *, artifact_refs: list[str] | None
    ) -> dict[str, object]:
        from okto_pulse.core.services.connectivity_dlq_reprocess_service import (
            verify_connectivity_class_cleared,
        )

        return await verify_connectivity_class_cleared(
            self.__relational_context,
            board_id,
            artifact_refs=artifact_refs,
        )

    async def list_cognitive_dlq_rows(
        self, board_id: str, *, limit: int, offset: int
    ):  # noqa: ANN201
        from okto_pulse.core.services.dead_letter_inspector_service import (
            list_cognitive_dlq_rows,
        )

        return await list_cognitive_dlq_rows(
            self.__relational_context,
            board_id,
            limit=limit,
            offset=offset,
        )

    async def list_dead_letter_rows(
        self, board_id: str, *, limit: int, offset: int
    ) -> dict[str, object]:
        from okto_pulse.core.services.dead_letter_inspector_service import (
            list_dead_letter_rows,
        )

        return await list_dead_letter_rows(
            self.__relational_context,
            board_id,
            limit=limit,
            offset=offset,
        )

    async def list_stale_canonical_parity(
        self, board_id: str, *, limit: int, offset: int
    ):  # noqa: ANN201
        from okto_pulse.core.services.application_kg import list_stale_canonical_parity

        return await list_stale_canonical_parity(
            self.__relational_context,
            board_id=board_id,
            limit=limit,
            offset=offset,
        )

    async def query_takedown_telemetry(
        self,
        *,
        board_id: str,
        delete_event_id: str | None = None,
        delivery_key: str | None = None,
    ) -> dict[str, object]:
        """Return one governed-takedown timeline through the edition read port.

        The query DTO owns exactly-one selector validation.  A missing snapshot
        is an explicit not-found result with no empty states/aggregates that
        could be mistaken for healthy delivery.  The injected clock keeps SLO
        aggregation deterministic without widening the transport surface.
        """

        from okto_pulse.core.ports.takedown_telemetry import (
            TakedownTelemetryQuery,
            TakedownTelemetrySnapshot,
            get_takedown_telemetry_read_port,
        )

        query = TakedownTelemetryQuery(
            now=self.__clock(),
            board_id=board_id,
            delete_event_id=delete_event_id,
            delivery_key=delivery_key,
        )
        selector = (
            {"delete_event_id": query.delete_event_id}
            if query.delete_event_id is not None
            else {"delivery_key": query.delivery_key}
        )
        snapshot = await get_takedown_telemetry_read_port().query_takedown_telemetry(
            self.__relational_context,
            query,
        )
        if snapshot is None:
            return {
                "found": False,
                "error": "takedown_telemetry_not_found",
                "selector": selector,
                "observed_at": query.now.isoformat(),
            }
        if not isinstance(snapshot, TakedownTelemetrySnapshot):
            raise RuntimeError("takedown_telemetry_snapshot_invalid")
        if snapshot.board_id != query.board_id:
            raise RuntimeError("takedown_telemetry_board_mismatch")
        if (
            query.delete_event_id is not None
            and snapshot.delete_event_id != query.delete_event_id
        ) or (
            query.delivery_key is not None
            and snapshot.delivery_key != query.delivery_key
        ):
            raise RuntimeError("takedown_telemetry_selector_mismatch")
        e2e_health = await _takedown_e2e_health(
            snapshot,
            relational_context=self.__relational_context,
        )
        return {
            "found": True,
            "selector": selector,
            "observed_at": query.now.isoformat(),
            **snapshot.to_dict(),
            "e2e_health": e2e_health,
        }

    async def list_canonical_debt(
        self,
        *,
        board_id: str,
        artifact_type: str | None,
        state: str | None,
        limit: int,
        offset: int,
    ):  # noqa: ANN201
        from okto_pulse.core.services.canonical_debt_service import list_canonical_debt

        return await list_canonical_debt(
            self.__relational_context,
            board_id=board_id,
            artifact_type=artifact_type,
            state=state,
            limit=limit,
            offset=offset,
        )

    async def schedule_canonical_debt_retry(
        self,
        *,
        board_id: str,
        debt_id: str,
        actor_id: str,
        kg_health_state: str,
    ) -> dict[str, object]:
        from okto_pulse.core.services.canonical_debt_service import (
            schedule_canonical_debt_retry,
        )

        return await schedule_canonical_debt_retry(
            self.__relational_context,
            board_id=board_id,
            debt_id=debt_id,
            actor_id=actor_id,
            kg_health_state=kg_health_state,
        )

    async def list_canonical_partition_integrity(
        self, *, board_id: str, **filters: object
    ):  # noqa: ANN201
        from okto_pulse.core.services.application_kg import (
            list_canonical_partition_integrity,
        )

        return await list_canonical_partition_integrity(
            self.__relational_context,
            board_id=board_id,
            **filters,
        )

    async def canonical_partition_integrity_detail(
        self, *, board_id: str, node_id: str
    ):  # noqa: ANN201
        from okto_pulse.core.services.application_kg import (
            get_canonical_partition_integrity_detail,
        )

        return await get_canonical_partition_integrity_detail(
            self.__relational_context,
            board_id=board_id,
            node_id=node_id,
        )

    async def list_digest_layer_mismatches(
        self, *, board_id: str, limit: int, offset: int
    ):  # noqa: ANN201
        from okto_pulse.core.services.application_kg import list_digest_layer_mismatches

        return await list_digest_layer_mismatches(
            self.__relational_context,
            board_id=board_id,
            limit=limit,
            offset=offset,
        )

    async def enqueue_digest_layer_reconciliation(
        self, *, board_id: str, reason: str
    ) -> dict[str, object]:
        """Request board/digest layer convergence through the durable outbox.

        The relational context stays private to this transaction-scoped service;
        the application use case depends only on this Core-owned capability.
        """
        from okto_pulse.core.kg.canonical_demotion_global_sync import (
            enqueue_digest_layer_reconciliation,
        )

        return await enqueue_digest_layer_reconciliation(
            self.__relational_context,
            board_id=board_id,
            reason=reason,
        )

    async def capture_global_discovery_recovery_seed_inputs(
        self,
        *,
        boards: list[tuple[str, str, str]],
        captured_cognitive_pending_exclusions: Mapping[
            str, Mapping[str, str]
        ],
    ) -> tuple[object, ...]:
        """Capture UoW-bound overlays before graph-only seed materialization."""

        from okto_pulse.core.ports.global_discovery_recovery_control import (
            GlobalDiscoveryRecoveryBoardSeedInputService,
        )

        board_rows = sorted(boards)
        board_ids = [str(row[0]) for row in board_rows]
        if len(set(board_ids)) != len(board_ids):
            raise ValueError("recovery boards must be unique")
        if set(captured_cognitive_pending_exclusions) != set(board_ids):
            raise ValueError(
                "captured cognitive exclusions must cover every recovery board"
            )
        input_service = GlobalDiscoveryRecoveryBoardSeedInputService()
        rows = []
        for board_id, board_name, board_summary in board_rows:
            rows.append(
                await input_service.capture_board_seed_input(
                    self.__relational_context,
                    board_id=board_id,
                    board_name=board_name,
                    board_summary=board_summary,
                    captured_cognitive_pending_exclusions=(
                        captured_cognitive_pending_exclusions[board_id]
                    ),
                )
            )
        return tuple(rows)

    async def list_global_outbox_dead_letters(
        self,
        *,
        limit: int,
        cursor: str | None,
        classification: str | None,
    ) -> dict[str, object]:
        from okto_pulse.core.application.global_outbox_dead_letter import (
            GlobalOutboxDeadLetterOperations,
        )
        from okto_pulse.core.ports.global_outbox import get_global_outbox_store

        return await GlobalOutboxDeadLetterOperations(
            store=get_global_outbox_store()
        ).list(
            context=self.__relational_context,
            limit=limit,
            cursor=cursor,
            classification=classification,
        )

    async def reprocess_global_outbox_dead_letters(
        self,
        *,
        dead_letter_ids: list[str],
        reason: str,
    ) -> dict[str, object]:
        from okto_pulse.core.application.global_outbox_dead_letter import (
            GlobalOutboxDeadLetterOperations,
        )
        from okto_pulse.core.ports.global_outbox import get_global_outbox_store

        return await GlobalOutboxDeadLetterOperations(
            store=get_global_outbox_store()
        ).reprocess(
            context=self.__relational_context,
            dead_letter_ids=dead_letter_ids,
            reason=reason,
        )

    async def verify_global_outbox_dead_letters(
        self,
        *,
        dead_letter_ids: list[str],
    ) -> dict[str, object]:
        from okto_pulse.core.application.global_outbox_dead_letter import (
            GlobalOutboxDeadLetterOperations,
        )
        from okto_pulse.core.ports.global_outbox import get_global_outbox_store

        return await GlobalOutboxDeadLetterOperations(
            store=get_global_outbox_store()
        ).verify(
            context=self.__relational_context,
            dead_letter_ids=dead_letter_ids,
        )

    async def recover_global_discovery_delivery(
        self, *, run_id: str, board_ids: list[str], dead_letter_limit: int
    ) -> dict[str, object]:
        """Requeue only global-open DLQ rows and reconcile every manifest board."""

        from okto_pulse.core.application.processors.global_outbox import (
            GLOBAL_OPEN_ERROR_CODES,
            _is_retryable_global_open_error,
        )
        from okto_pulse.core.kg.canonical_demotion_global_sync import (
            enqueue_digest_layer_reconciliation,
        )
        from okto_pulse.core.ports.delivery_ledger import (
            is_governed_delivery_attempt,
        )
        from okto_pulse.core.ports.global_outbox import (
            GlobalOutboxDeadLetterCursor,
            get_global_outbox_store,
        )

        limit = max(1, min(int(dead_letter_limit), 5000))
        store = get_global_outbox_store()
        cursor = None
        selected = []
        governed_deferred = 0
        while len(selected) < limit:
            page_limit = min(100, limit - len(selected))
            rows = list(
                await store.list_dead_letters(
                    self.__relational_context,
                    limit=page_limit,
                    error_markers=GLOBAL_OPEN_ERROR_CODES,
                    after=cursor,
                )
            )
            if not rows:
                break
            for row in rows:
                if not _is_retryable_global_open_error(row.last_error):
                    continue
                if is_governed_delivery_attempt(
                    event_id=row.event_id,
                    payload=row.payload,
                ):
                    governed_deferred += 1
                    continue
                selected.append(row)
            cursor = GlobalOutboxDeadLetterCursor(
                created_at=rows[-1].created_at,
                id=rows[-1].id,
            )
            if len(rows) < page_limit:
                break
        selected = selected[:limit]
        for row in selected:
            row.retry_count = 0
            row.last_error = None
        if selected:
            await store.save_events(self.__relational_context, selected)

        enqueued = 0
        for board_id in sorted(set(board_ids)):
            await enqueue_digest_layer_reconciliation(
                self.__relational_context,
                board_id=board_id,
                reason="global_discovery_recovery",
                idempotency_key=f"{run_id}:{board_id}",
            )
            enqueued += 1
        return {
            "run_id": run_id,
            "dead_letters_requeued": len(selected),
            "governed_delivery_attempts_deferred": governed_deferred,
            "board_reconciliations_enqueued": enqueued,
            "selective_error_class": "global_open",
        }

    async def queue_health(self) -> dict[str, object]:
        from okto_pulse.core.services.queue_health_service import get_queue_health

        return await get_queue_health(self.__relational_context)

    async def queue_drilldown(self, board_id: str | None) -> dict[str, object]:
        from okto_pulse.core.services.queue_health_service import (
            get_active_queue_drilldown,
        )

        return await get_active_queue_drilldown(self.__relational_context, board_id)

    async def invoke_rebuild_admission(
        self,
        refusal_check: object,
        board_id: str,
        *,
        scheduler_control: object | None,
    ):  # noqa: ANN201
        if not isinstance(refusal_check, Callable):
            raise TypeError("refusal_check must be callable")

        async def health_probe(
            probe_board_id: str,
            _context: object,
            *,
            scheduler_control: object | None = None,
        ) -> dict[str, object]:
            return await self.health(
                probe_board_id,
                scheduler_control=scheduler_control,
            )

        if scheduler_control is None:
            return await refusal_check(
                board_id,
                self.__relational_context,
                health_probe=health_probe,
            )
        return await refusal_check(
            board_id,
            self.__relational_context,
            scheduler_control=scheduler_control,
            health_probe=health_probe,
        )


def export_board_jsonld(board_id: str) -> dict[str, Any]:
    from okto_pulse.core.kg.graph_export import export_board_jsonld as _export

    return _export(board_id)


__all__ = [
    "CoreKnowledgeGraphOperations",
    "CurationPolicyError",
    "GraphExportError",
    "export_board_jsonld",
]
