"""B14 closed event-delivery and policy-constraint projection contracts."""

from __future__ import annotations

from datetime import datetime, timezone
import uuid

import pytest
from pydantic import ValidationError

from okto_pulse.core.application.domain_event_delivery import event_from_stored
from okto_pulse.core.events.handlers.policy_constraint_projection import (
    PolicyConstraintProjectionHandler,
)
from okto_pulse.core.events.registry import registered_handlers
from okto_pulse.core.events.types import (
    PolicyAdoptionChanged,
    PolicyBindingMaterialized,
    PolicyRetirementChanged,
    SemanticGuidelineProjectionChanged,
    resolve_event_class,
)
from okto_pulse.core.ports.domain_event_delivery import StoredDomainEvent
from okto_pulse.core.ports.policy_constraint_projection import (
    PolicyConstraintProjectionResult,
    get_policy_constraint_projection_port,
    register_policy_constraint_projection_port,
    reset_policy_constraint_projection_port_for_tests,
)


_SHA = "a" * 64
_NOW = datetime(2026, 7, 29, 12, 0, tzinfo=timezone.utc)
_RETIREMENT_EVENT_NAMESPACE = uuid.UUID(
    "d79ddf58-c1f8-520a-9cf2-50cd36157abc"
)


def _adoption(*, operation: str = "adopt") -> PolicyAdoptionChanged:
    adopt = operation == "adopt"
    return PolicyAdoptionChanged(
        event_schema_version="guideline-impact/v2",
        event_id=f"event-{operation}",
        board_id="board-1",
        actor_id="agent-1",
        actor_type="agent",
        occurred_at=_NOW,
        operation=operation,
        guideline_id="guideline-1",
        binding_id="binding-1",
        previous_binding_revision=None if adopt else 1,
        binding_revision=1 if adopt else 2,
        from_revision_id=None if adopt else "revision-1",
        from_semantic_version=None if adopt else "1.0.0",
        from_revision_digest=None if adopt else _SHA,
        to_revision_id="revision-1" if adopt else None,
        to_semantic_version="1.0.0" if adopt else None,
        to_revision_digest=_SHA if adopt else None,
        impact_receipt_id="impact-1" if adopt else None,
        impact_digest=_SHA if adopt else None,
        binding_digest_before=_SHA,
        binding_head_digest_before=_SHA,
        binding_head_digest_after=_SHA,
        policy_set_digest_before=_SHA,
        policy_set_digest_after=_SHA,
        policy_set_digest=_SHA,
        added_metric_ids=("metric-1",) if adopt else (),
        changed_metric_ids=(),
        removed_metric_ids=() if adopt else ("metric-1",),
    )


def _retirement() -> PolicyRetirementChanged:
    retirement_id = "retirement-1"
    board_id = "board-1"
    return PolicyRetirementChanged(
        event_schema_version="guideline-impact/v2",
        event_id=str(
            uuid.uuid5(
                _RETIREMENT_EVENT_NAMESPACE,
                f"{retirement_id}:{board_id}",
            )
        ),
        board_id=board_id,
        actor_id="agent-1",
        actor_type="agent",
        occurred_at=_NOW,
        operation="retire",
        guideline_id="guideline-1",
        retirement_id=retirement_id,
        retirement_status="retired",
        superseded_by_guideline_id=None,
        binding_id="binding-1",
        binding_revision=2,
        revision_id="revision-1",
        revision_number=1,
        semantic_version="1.0.0",
        revision_digest=_SHA,
        binding_digest_before=_SHA,
        binding_head_digest_before=_SHA,
        binding_head_digest_after=_SHA,
        policy_set_digest_before=_SHA,
        policy_set_digest_after=_SHA,
        policy_set_digest=_SHA,
        removed_metric_ids=("metric-1",),
        request_digest=_SHA,
    )


def _materialized(
    *,
    source_kind: str = "native",
    actor_type: str = "agent",
) -> PolicyBindingMaterialized:
    return PolicyBindingMaterialized(
        event_schema_version="policy-binding-materialized/v2",
        event_id="event-materialized-1",
        board_id="board-1",
        actor_id="agent-1",
        actor_type=actor_type,
        occurred_at=_NOW,
        operation="adopt",
        guideline_id="guideline-1",
        binding_id="binding-1",
        binding_revision=1,
        revision_id="revision-1",
        semantic_version="1.0.0",
        revision_digest=_SHA,
        source_kind=source_kind,
        enforcement="advisory",
        minimum_confidence=70,
        metric_threshold_overrides={"quality.require_review": 85},
        priority=3,
    )


def _semantic_projection() -> SemanticGuidelineProjectionChanged:
    return SemanticGuidelineProjectionChanged(
        event_id="semantic-event-1",
        board_id="board-1",
        actor_id="agent-1",
        actor_type="agent",
        occurred_at=_NOW,
        event_schema_version="semantic-guideline-kg-projection/v1",
        entity_kind="assessment_receipt",
        causation_id="receipt-1",
        entity_id="receipt-1",
        entity_digest=_SHA,
        operation="upsert",
    )


@pytest.mark.parametrize(
    ("event", "revision_id"),
    (
        (_adoption(), "revision-1"),
        (_adoption(operation="unlink"), "revision-1"),
        (_retirement(), "revision-1"),
        (_materialized(), "revision-1"),
        (
            _materialized(
                source_kind="default_materialization",
                actor_type="system",
            ),
            "revision-1",
        ),
    ),
)
def test_companions_are_closed_normalized_and_resolvable(
    event: (
        PolicyAdoptionChanged
        | PolicyBindingMaterialized
        | PolicyRetirementChanged
    ),
    revision_id: str,
) -> None:
    assert event.exact_revision_id == revision_id
    assert resolve_event_class(event.event_type) is type(event)
    assert PolicyConstraintProjectionHandler in registered_handlers(
        event.event_type
    )
    with pytest.raises(ValidationError):
        type(event).model_validate(
            {
                **event.model_dump(mode="python"),
                "unexpected": "authority",
            }
        )


def test_adoption_companion_rejects_digest_alias_drift() -> None:
    payload = _adoption().model_dump(mode="python")
    payload["policy_set_digest"] = "b" * 64
    with pytest.raises(ValidationError, match="policy_adoption_event_evidence_invalid"):
        PolicyAdoptionChanged.model_validate(payload)


def test_semantic_projection_event_is_closed_registered_and_resolvable() -> None:
    event = _semantic_projection()

    assert resolve_event_class(event.event_type) is type(event)
    assert PolicyConstraintProjectionHandler in registered_handlers(
        event.event_type
    )
    restored = event_from_stored(
        StoredDomainEvent(
            event_id=event.event_id,
            event_type=event.event_type,
            board_id=event.board_id,
            actor_id=event.actor_id,
            actor_type=event.actor_type,
            occurred_at=event.occurred_at,
            payload=event.model_dump(mode="json"),
        )
    )
    assert restored == event
    with pytest.raises(ValidationError):
        SemanticGuidelineProjectionChanged.model_validate(
            {**event.model_dump(mode="python"), "unexpected": "authority"}
        )


@pytest.mark.asyncio
async def test_handler_projects_semantic_event_as_sync() -> None:
    event = _semantic_projection()

    class Projection:
        async def apply(self, received_context, *, event):
            assert received_context == "caller-session"
            return PolicyConstraintProjectionResult(
                board_id=event.board_id,
                operation="sync",
                event_id=event.event_id,
                activated_count=1,
                ended_count=0,
                active_count=1,
                unadopted_active_count=0,
                node_ids=("semantic-guideline:assessment_receipt:receipt-1",),
                replayed=False,
            )

        async def rebuild_board(self, received_context, *, board_id):
            raise AssertionError((received_context, board_id))

    register_policy_constraint_projection_port(Projection())
    result = await PolicyConstraintProjectionHandler().handle(
        event,
        "caller-session",
    )

    assert result.operation == "sync"
    reset_policy_constraint_projection_port_for_tests()


@pytest.mark.parametrize(
    "field_name",
    ("event_id", "board_id", "actor_id", "actor_type", "occurred_at"),
)
def test_stored_full_payload_requires_top_level_column_equality(
    field_name: str,
) -> None:
    event = _adoption()
    payload = {
        **event.model_dump(mode="json"),
        "event_schema_version": "guideline-impact/v2",
    }
    row = StoredDomainEvent(
        event_id=event.event_id,
        event_type=event.event_type,
        board_id=event.board_id,
        actor_id=event.actor_id,
        actor_type=event.actor_type,
        occurred_at=event.occurred_at,
        payload=payload,
    )
    restored = event_from_stored(row)
    assert restored == event

    payload[field_name] = (
        "2026-07-30T12:00:00Z"
        if field_name == "occurred_at"
        else f"different-{field_name}"
    )
    with pytest.raises(
        ValueError,
        match=rf"stored_event_top_level_mismatch:{field_name}",
    ):
        event_from_stored(
            StoredDomainEvent(
                event_id=row.event_id,
                event_type=row.event_type,
                board_id=row.board_id,
                actor_id=row.actor_id,
                actor_type=row.actor_type,
                occurred_at=row.occurred_at,
                payload=payload,
            )
        )


def test_materialized_companion_reconstructs_from_full_stored_payload() -> None:
    event = _materialized()
    restored = event_from_stored(
        StoredDomainEvent(
            event_id=event.event_id,
            event_type=event.event_type,
            board_id=event.board_id,
            actor_id=event.actor_id,
            actor_type=event.actor_type,
            occurred_at=event.occurred_at,
            payload=event.model_dump(mode="json"),
        )
    )

    assert restored == event
    assert restored.exact_revision_id == "revision-1"
    assert restored.source_kind == "native"


def test_projection_registry_and_handler_fail_closed() -> None:
    reset_policy_constraint_projection_port_for_tests()
    with pytest.raises(
        RuntimeError,
        match="policy_constraint_projection_port_not_configured",
    ):
        get_policy_constraint_projection_port()
    with pytest.raises(TypeError, match="policy_constraint_projection_port_invalid"):
        register_policy_constraint_projection_port(object())  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_handler_delegates_in_caller_context_and_validates_result() -> None:
    event = _adoption()
    context = object()

    class Projection:
        seen: tuple[object, object] | None = None

        async def apply(self, received_context, *, event):
            self.seen = (received_context, event)
            return PolicyConstraintProjectionResult(
                board_id=event.board_id,
                operation=event.operation,
                event_id=event.event_id,
                activated_count=1,
                ended_count=0,
                active_count=2,
                unadopted_active_count=0,
                node_ids=("constraint-2", "constraint-1"),
                replayed=False,
            )

        async def rebuild_board(self, received_context, *, board_id):
            raise AssertionError((received_context, board_id))

    projection = Projection()
    register_policy_constraint_projection_port(projection)
    result = await PolicyConstraintProjectionHandler().handle(event, context)

    assert projection.seen == (context, event)
    assert result.node_ids == ("constraint-1", "constraint-2")
    reset_policy_constraint_projection_port_for_tests()


@pytest.mark.asyncio
async def test_handler_projects_materialized_binding_as_public_adopt() -> None:
    event = _materialized(
        source_kind="default_materialization",
        actor_type="system",
    )

    class Projection:
        async def apply(self, received_context, *, event):
            assert received_context == "caller-session"
            assert event.source_kind == "default_materialization"
            return PolicyConstraintProjectionResult(
                board_id=event.board_id,
                operation="adopt",
                event_id=event.event_id,
                activated_count=1,
                ended_count=0,
                active_count=1,
                unadopted_active_count=0,
                node_ids=("constraint-1",),
                replayed=False,
            )

        async def rebuild_board(self, received_context, *, board_id):
            raise AssertionError((received_context, board_id))

    register_policy_constraint_projection_port(Projection())
    result = await PolicyConstraintProjectionHandler().handle(
        event,
        "caller-session",
    )

    assert result.operation == "adopt"
    assert result.event_id == event.event_id
    reset_policy_constraint_projection_port_for_tests()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "expected"),
    (
        (
            RuntimeError(
                "secret=Bearer abc path=C:\\private\\db.sqlite query=SELECT *"
            ),
            "policy_constraint_projection_failed:RuntimeError",
        ),
        (
            type(
                "ClosedProjectionFailure",
                (RuntimeError,),
                {"code": "policy_constraint_adapter_unavailable"},
            )("secret body"),
            "policy_constraint_adapter_unavailable",
        ),
        (
            type(
                "ClosedSemanticProjectionFailure",
                (RuntimeError,),
                {"code": "semantic_guideline_authority_mismatch"},
            )("secret body"),
            "semantic_guideline_authority_mismatch",
        ),
    ),
)
async def test_handler_normalizes_port_failures_before_delivery_ledger(
    failure: Exception,
    expected: str,
) -> None:
    class FailingProjection:
        async def apply(self, received_context, *, event):
            del received_context, event
            raise failure

        async def rebuild_board(self, received_context, *, board_id):
            raise AssertionError((received_context, board_id))

    register_policy_constraint_projection_port(FailingProjection())
    with pytest.raises(RuntimeError) as captured:
        await PolicyConstraintProjectionHandler().handle(_adoption(), object())

    assert str(captured.value) == expected
    assert captured.value.__cause__ is None
    assert "secret" not in str(captured.value).lower()
    assert "private" not in str(captured.value).lower()
    assert "select" not in str(captured.value).lower()
    reset_policy_constraint_projection_port_for_tests()


def test_projection_result_closes_operation_event_and_count_invariants() -> None:
    with pytest.raises(
        ValueError,
        match="policy_constraint_projection_event_id_invalid",
    ):
        PolicyConstraintProjectionResult(
            board_id="board-1",
            operation="rebuild",
            event_id="event-not-allowed",
            activated_count=0,
            ended_count=0,
            active_count=0,
            unadopted_active_count=0,
            node_ids=(),
            replayed=False,
        )

    with pytest.raises(
        ValueError,
        match="policy_constraint_projection_active_count_mismatch",
    ):
        PolicyConstraintProjectionResult(
            board_id="board-1",
            operation="adopt",
            event_id="event-1",
            activated_count=1,
            ended_count=0,
            active_count=2,
            unadopted_active_count=0,
            node_ids=("constraint-1",),
            replayed=False,
        )

    with pytest.raises(
        ValueError,
        match="policy_constraint_projection_node_ids_duplicate",
    ):
        PolicyConstraintProjectionResult(
            board_id="board-1",
            operation="rebuild",
            event_id=None,
            activated_count=0,
            ended_count=0,
            active_count=1,
            unadopted_active_count=0,
            node_ids=("constraint-1", " constraint-1 "),
            replayed=False,
        )

    with pytest.raises(
        ValueError,
        match="policy_constraint_projection_unadopted_count_invalid",
    ):
        PolicyConstraintProjectionResult(
            board_id="board-1",
            operation="rebuild",
            event_id=None,
            activated_count=0,
            ended_count=0,
            active_count=1,
            unadopted_active_count=1,
            node_ids=("constraint-1",),
            replayed=False,
        )
