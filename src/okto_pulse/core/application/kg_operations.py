"""Transaction-scoped Core KG operation catalog."""

from __future__ import annotations

from collections.abc import Callable


class CoreKnowledgeGraphOperations:
    def __init__(self, relational_context: object) -> None:
        self.__relational_context = relational_context

    async def dispatch_manual_tick(
        self,
        *,
        tick_id: str,
        board_id: str | None,
        force_full_rebuild: bool,
    ) -> None:
        from okto_pulse.core.application.kg_tick import dispatch_manual_tick

        await dispatch_manual_tick(
            tick_id=tick_id,
            board_id=board_id,
            force_full_rebuild=force_full_rebuild,
            session=self.__relational_context,
        )

    async def evaluate_bug_cognitive_closure(
        self, readiness_service: object, **request: object
    ) -> dict[str, object]:
        from okto_pulse.core.services.application_kg import (
            evaluate_bug_cognitive_closure,
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
        self, request: object, *, board_id: str, agent_id: str
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
        if scheduler_control is None:
            return await refusal_check(board_id, self.__relational_context)
        return await refusal_check(
            board_id,
            self.__relational_context,
            scheduler_control=scheduler_control,
        )


__all__ = ["CoreKnowledgeGraphOperations"]
