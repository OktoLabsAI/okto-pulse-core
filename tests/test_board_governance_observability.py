"""BG-01.5 — Governance audit and safe metric payloads."""

from __future__ import annotations

import uuid

import pytest


def test_governance_metric_rejects_unsafe_extra_payload_keys():
    from okto_pulse.core.services.governance_observability import (
        GovernanceAuditPayloadError,
        emit_governance_metric,
        get_governance_metric_samples,
        reset_governance_metric_samples,
    )

    reset_governance_metric_samples()
    with pytest.raises(GovernanceAuditPayloadError) as exc_info:
        emit_governance_metric(
            {
                "metric_name": "qa_self_answer_denied_total",
                "board_id": "board-1",
                "actor_id": "actor-1",
                "entity_type": "card",
                "question_id": "question-1",
                "reason": "self_answering_not_allowed",
                "surface": "service",
                "outcome": "deny",
                "answer_text": "This free-form answer must not become a metric label.",
            }
        )
    assert "extra" in str(exc_info.value) or "extra_keys" in str(exc_info.value)
    samples = get_governance_metric_samples()
    assert samples == [
        {
            "metric_name": "governance_audit_safe_label_violation_total",
            "value": 1,
            "labels": {
                "event_type": "qa_self_answer_denied_total",
                "violation_kind": "governance_metric_payload_extra_keys",
                "surface": "service",
                "outcome": "rejected",
            },
        }
    ]


def test_governance_metric_rejects_free_form_label_values():
    from okto_pulse.core.services.governance_observability import (
        GovernanceAuditPayloadError,
        GovernanceMetricEvent,
        reset_governance_metric_samples,
        sanitize_governance_metric_event,
    )

    reset_governance_metric_samples()
    with pytest.raises(GovernanceAuditPayloadError) as exc_info:
        sanitize_governance_metric_event(
            GovernanceMetricEvent(
                "board_missing_context_warning_total",
                labels={
                    "board_id": "board-1",
                    "warning_code": "guidelines_missing because the board has no text",
                    "surface": "menu_board",
                    "outcome": "warning",
                },
            )
        )
    assert "unsafe_governance_label_value" in str(exc_info.value)


def test_governance_metric_emit_can_swallow_violation_after_counting_it():
    from okto_pulse.core.services.governance_observability import (
        emit_governance_metric,
        get_governance_metric_samples,
        reset_governance_metric_samples,
    )

    reset_governance_metric_samples()
    emit_governance_metric(
        {
            "metric_name": "board_missing_context_warning_total",
            "board_id": "board-1",
            "warning_code": "guideline text leaked into label",
            "surface": "menu_board",
            "outcome": "warning",
        },
        raise_on_violation=False,
    )

    samples = get_governance_metric_samples()
    assert samples == [
        {
            "metric_name": "governance_audit_safe_label_violation_total",
            "value": 1,
            "labels": {
                "event_type": "board_missing_context_warning_total",
                "violation_kind": "unsafe_governance_label_value",
                "surface": "menu_board",
                "outcome": "rejected",
            },
        }
    ]


def test_context_fingerprint_is_audit_metadata_not_metric_label():
    from okto_pulse.core.services.critical_context_guard import (
        CriticalAction,
        CriticalContextDecision,
    )
    from okto_pulse.core.services.governance_observability import (
        emit_governance_metric,
        get_governance_metric_samples,
        reset_governance_metric_samples,
    )

    decision = CriticalContextDecision(
        board_id="board-1",
        actor_id="actor-1",
        entity_type="card",
        entity_id="card-1",
        critical_action=CriticalAction.CARD_MOVE_STATUS,
        surface="service",
        outcome="allow",
        reason="full_context_resolved",
        context_profile="full",
        context_fingerprint="ctx_sha256_v1:abcdef123456",
        context_resolved_at="2026-06-07T00:00:00+00:00",
        latency_ms=12.5,
    )

    audit = decision.audit_details()
    assert audit["context_fingerprint"] == "ctx_sha256_v1:abcdef123456"
    assert "context_fingerprint" not in decision.metric_labels()

    reset_governance_metric_samples()
    emit_governance_metric(audit)
    samples = get_governance_metric_samples()
    assert len(samples) == 1
    assert samples[0]["metric_name"] == "critical_context_guard_decision_total"
    assert "context_fingerprint" not in samples[0]["labels"]
    assert "entity_id" not in samples[0]["labels"]


def test_full_context_failure_and_latency_metric_labels_are_safe():
    from okto_pulse.core.services.critical_context_guard import (
        CriticalAction,
        CriticalContextDecision,
    )
    from okto_pulse.core.services.governance_observability import (
        emit_governance_metric,
        get_governance_metric_samples,
        reset_governance_metric_samples,
    )

    decision = CriticalContextDecision(
        board_id="board-1",
        actor_id="actor-1",
        entity_type="card",
        entity_id="card-1",
        critical_action=CriticalAction.CARD_MOVE_STATUS,
        surface="service",
        outcome="deny",
        reason="full_context_unavailable",
        context_profile="unavailable",
        context_fingerprint=None,
        context_resolved_at=None,
        latency_ms=7.25,
    )

    reset_governance_metric_samples()
    emit_governance_metric(decision.metric_labels())
    emit_governance_metric(decision.latency_metric_labels(), value=decision.latency_ms)
    emit_governance_metric(decision.resolution_failure_metric_labels())

    samples = get_governance_metric_samples()
    assert [item["metric_name"] for item in samples] == [
        "critical_context_guard_decision_total",
        "critical_context_resolution_latency_ms",
        "critical_context_resolution_failure_total",
    ]
    for item in samples:
        assert all(key not in item["labels"] for key in ("context_fingerprint", "entity_id"))


@pytest.mark.asyncio
async def test_board_setting_change_records_safe_metric_sample(db_factory):
    from sqlalchemy_test_models import Board
    from okto_pulse.core.models.schemas import BoardUpdate
    from okto_pulse.core.services.governance_observability import (
        get_governance_metric_samples,
        reset_governance_metric_samples,
    )
    from okto_pulse.core.services.main import BoardService

    board_id = f"bg-observability-board-{uuid.uuid4().hex[:8]}"
    user_id = "bg-observability-user"
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="BG Observability Board",
                owner_id=user_id,
                settings={},
            )
        )
        await db.commit()

    reset_governance_metric_samples()
    async with db_factory() as db:
        updated = await BoardService(db).update_board(
            board_id,
            user_id,
            BoardUpdate.model_validate(
                {"settings": {"allow_agent_self_answering": True}}
            ),
        )
        assert updated is not None
        await db.commit()

    samples = get_governance_metric_samples()
    assert samples == [
        {
            "metric_name": "board_governance_setting_changed_total",
            "value": 1,
            "labels": {
                "board_id": board_id,
                "actor_id": user_id,
                "setting_key": "allow_agent_self_answering",
                "old_effective_value": False,
                "new_effective_value": True,
                "surface": "board_patch",
                "outcome": "changed",
            },
        }
    ]


@pytest.mark.asyncio
async def test_empty_board_guidelines_emit_missing_context_warning_metric(db_factory):
    from sqlalchemy_test_models import Board
    from okto_pulse.core.services.governance_observability import (
        get_governance_metric_samples,
        reset_governance_metric_samples,
    )
    from okto_pulse.core.services.main import GuidelineService

    board_id = f"bg-board-{uuid.uuid4().hex[:8]}"
    user_id = "bg-observability-user"
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Missing Guidelines Board",
                owner_id=user_id,
                settings={},
            )
        )
        await db.commit()

    reset_governance_metric_samples()
    async with db_factory() as db:
        items = await GuidelineService(db).get_board_guidelines(
            board_id,
            surface="menu_board",
        )
        await db.commit()

    assert items == []
    assert get_governance_metric_samples() == [
        {
            "metric_name": "board_missing_context_warning_total",
            "value": 1,
            "labels": {
                "board_id": board_id,
                "warning_code": "board_rules_missing",
                "surface": "menu_board",
                "outcome": "warning",
            },
        }
    ]


@pytest.mark.asyncio
async def test_empty_board_description_update_emits_missing_context_warning_metric(db_factory):
    from sqlalchemy_test_models import Board
    from okto_pulse.core.models.schemas import BoardUpdate
    from okto_pulse.core.services.governance_observability import (
        get_governance_metric_samples,
        reset_governance_metric_samples,
    )
    from okto_pulse.core.services.main import BoardService

    board_id = f"bg-board-{uuid.uuid4().hex[:8]}"
    user_id = "bg-observability-user"
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Missing Description Board",
                description="Existing context",
                owner_id=user_id,
                settings={},
            )
        )
        await db.commit()

    reset_governance_metric_samples()
    async with db_factory() as db:
        updated = await BoardService(db).update_board(
            board_id,
            user_id,
            BoardUpdate.model_validate({"description": "   "}),
        )
        assert updated is not None
        await db.commit()

    assert get_governance_metric_samples() == [
        {
            "metric_name": "board_missing_context_warning_total",
            "value": 1,
            "labels": {
                "board_id": board_id,
                "warning_code": "board_summary_missing",
                "surface": "board_patch",
                "outcome": "warning",
            },
        }
    ]
