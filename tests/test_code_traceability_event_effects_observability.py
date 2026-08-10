"""Outbox, downstream effect, and metric safety contracts."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from okto_pulse.core.events import (
    CODE_TRACEABILITY_EVENT_TYPES,
    CodeEvidenceCreated,
    CodeInvestigationReceiptRevoked,
    code_traceability_event_digest,
    make_code_traceability_event,
    publish_code_traceability_mutation,
)
from okto_pulse.core.events.handlers.code_traceability_effects import (
    CodeTraceabilityEventEffectsHandler,
)
from okto_pulse.core.events.registry import registered_handlers
from okto_pulse.core.events.types import DomainEvent
from okto_pulse.core.ports.code_traceability_event_effects import (
    CodeTraceabilityEventEffectsProviderMissing,
    code_traceability_event_effect_plan,
    register_code_traceability_event_effects_port,
    reset_code_traceability_event_effects_port_for_tests,
)
from okto_pulse.core.services.code_traceability_observability import (
    CODE_TRACEABILITY_METRIC_LABELS,
    CODE_TRACEABILITY_METRIC_NAMES,
    METRIC_CODE_EVIDENCE_ATTESTATION_TOTAL,
    METRIC_CODE_EVIDENCE_DISPOSITION_TOTAL,
    METRIC_CODE_EVIDENCE_SUBMISSION_TOTAL,
    METRIC_CODE_INVESTIGATION_RECEIPT_AGE_SECONDS,
    METRIC_CODE_INVESTIGATION_RECEIPT_REJECTED_TOTAL,
    METRIC_CODE_INVESTIGATION_RECEIPT_TOTAL,
    METRIC_CODE_TRACEABILITY_GATE_BLOCKER_TOTAL,
    METRIC_CODE_TRACEABILITY_GATE_TOTAL,
    METRIC_IMPLEMENTATION_OVERLAP_ACKNOWLEDGED_TOTAL,
    METRIC_IMPLEMENTATION_OVERLAP_TOTAL,
    METRIC_IMPLEMENTATION_TARGET_CREATED_TOTAL,
    METRIC_IMPLEMENTATION_TARGET_RESOLUTION_RECEIPT_TOTAL,
    METRIC_IMPLEMENTATION_TARGET_RESOLUTION_SUBMISSION_DURATION_SECONDS,
    CodeTraceabilityMetricEvent,
    get_code_traceability_metric_samples,
    observe_code_traceability_metric,
    reset_code_traceability_observability_for_tests,
    sanitize_code_traceability_metric_event,
)


def _revocation_event() -> CodeInvestigationReceiptRevoked:
    return make_code_traceability_event(
        CodeInvestigationReceiptRevoked,
        board_id="board-1",
        actor_id="agent-1",
        actor_type="agent",
        investigation_receipt_id="receipt-1",
        revocation_id="revocation-1",
        reason_code="operator_revoked",
        head_state="revoked",
    )


def test_event_factory_is_closed_and_digest_only_for_free_text():
    event = _revocation_event()
    assert event.event_type == "code_investigation.receipt_revoked"
    assert code_traceability_event_digest("operator reason") == (
        "e0441cf31fa78b4b7439237f91fb3ff96887e1cb440cb0621f67fd302c491a50"
    )
    with pytest.raises(ValidationError):
        make_code_traceability_event(
            CodeInvestigationReceiptRevoked,
            board_id="board-1",
            actor_id="agent-1",
            actor_type="agent",
            investigation_receipt_id="receipt-1",
            revocation_id="revocation-1",
            reason_code="operator_revoked",
            head_state="revoked",
            excerpt="must-not-be-published",
        )
    with pytest.raises(TypeError, match="event_class_invalid"):
        make_code_traceability_event(
            DomainEvent,
            board_id="board-1",
            actor_id="agent-1",
            actor_type="agent",
        )


@pytest.mark.asyncio
async def test_publish_stages_in_caller_uow_and_suppresses_replays():
    published: list[object] = []

    async def publish_domain_event(event: object) -> None:
        published.append(event)

    uow = SimpleNamespace(
        services=SimpleNamespace(publish_domain_event=publish_domain_event)
    )
    event = _revocation_event()

    assert await publish_code_traceability_mutation(uow, event) is True
    assert await publish_code_traceability_mutation(
        uow,
        event,
        replayed=True,
    ) is False
    assert published == [event]


def test_every_traceability_event_has_consolidation_and_effect_handlers():
    for event_type in CODE_TRACEABILITY_EVENT_TYPES:
        names = {handler.__name__ for handler in registered_handlers(event_type)}
        assert "ConsolidationEnqueuer" in names
        assert "CodeTraceabilityEventEffectsHandler" in names


def test_effect_plan_invalidates_validation_for_links_currentness_and_waivers():
    expected = {
        "code_investigation.receipt_submitted",
        "code_investigation.receipt_revoked",
        "code_evidence.superseded",
        "code_evidence.revoked",
        "code_evidence.linked",
        "code_evidence.unlinked",
        "code_evidence.disposition_changed",
        "code_traceability.waiver_created",
        "code_traceability.waiver_cleared",
    }
    for event_type in CODE_TRACEABILITY_EVENT_TYPES:
        plan = code_traceability_event_effect_plan(event_type)
        assert plan.invalidate_read_models is True
        assert plan.record_activity is True
        assert plan.invalidate_spec_validation is (event_type in expected)


@pytest.mark.asyncio
async def test_effect_handler_is_fail_closed_and_calls_edition_port():
    reset_code_traceability_event_effects_port_for_tests()
    handler = CodeTraceabilityEventEffectsHandler()
    event = _revocation_event()
    with pytest.raises(CodeTraceabilityEventEffectsProviderMissing):
        await handler.handle(event, object())

    calls: list[tuple[object, object]] = []

    class FakePort:
        async def apply(self, session: object, received: object) -> None:
            calls.append((session, received))

    session = object()
    register_code_traceability_event_effects_port(FakePort())
    try:
        await handler.handle(event, session)
    finally:
        reset_code_traceability_event_effects_port_for_tests()
    assert calls == [(session, event)]


def test_metric_catalog_and_labels_match_section_22_exactly():
    assert CODE_TRACEABILITY_METRIC_NAMES == frozenset(
        {
            METRIC_CODE_INVESTIGATION_RECEIPT_TOTAL,
            METRIC_CODE_INVESTIGATION_RECEIPT_REJECTED_TOTAL,
            METRIC_CODE_INVESTIGATION_RECEIPT_AGE_SECONDS,
            METRIC_CODE_EVIDENCE_SUBMISSION_TOTAL,
            METRIC_CODE_EVIDENCE_ATTESTATION_TOTAL,
            METRIC_CODE_EVIDENCE_DISPOSITION_TOTAL,
            METRIC_IMPLEMENTATION_TARGET_CREATED_TOTAL,
            METRIC_IMPLEMENTATION_TARGET_RESOLUTION_RECEIPT_TOTAL,
            METRIC_IMPLEMENTATION_TARGET_RESOLUTION_SUBMISSION_DURATION_SECONDS,
            METRIC_IMPLEMENTATION_OVERLAP_TOTAL,
            METRIC_IMPLEMENTATION_OVERLAP_ACKNOWLEDGED_TOTAL,
            METRIC_CODE_TRACEABILITY_GATE_TOTAL,
            METRIC_CODE_TRACEABILITY_GATE_BLOCKER_TOTAL,
        }
    )
    assert CODE_TRACEABILITY_METRIC_LABELS == frozenset(
        {
            "outcome",
            "state",
            "evidence_type",
            "selector_kind",
            "role",
            "tooling_family",
            "trust_level",
            "language",
            "gate",
            "reason_code",
            "overlap_severity",
        }
    )


def test_metric_sink_retains_only_safe_bounded_samples():
    reset_code_traceability_observability_for_tests()
    observe_code_traceability_metric(
        METRIC_CODE_EVIDENCE_SUBMISSION_TOTAL,
        labels={
            "outcome": "accepted",
            "evidence_type": "behavior",
            "selector_kind": "symbol",
            "language": "python",
        },
    )
    assert get_code_traceability_metric_samples() == [
        {
            "metric_name": METRIC_CODE_EVIDENCE_SUBMISSION_TOTAL,
            "value": 1,
            "labels": {
                "outcome": "accepted",
                "evidence_type": "behavior",
                "selector_kind": "symbol",
                "language": "python",
            },
        }
    ]


@pytest.mark.parametrize(
    "forbidden_label",
    (
        "path",
        "symbol",
        "claim",
        "excerpt",
        "remote_url",
        "workspace_locator",
        "board_id",
    ),
)
def test_metric_contract_rejects_high_cardinality_or_sensitive_labels(
    forbidden_label: str,
):
    with pytest.raises(ValueError, match="metric_label_invalid"):
        sanitize_code_traceability_metric_event(
            CodeTraceabilityMetricEvent(
                METRIC_CODE_EVIDENCE_SUBMISSION_TOTAL,
                labels={forbidden_label: "value"},
            )
        )


@pytest.mark.parametrize(
    "unsafe_value",
    (
        pytest.param("src/orders/service.py", id="relative-path"),
        pytest.param(r"C:\work\source.py", id="absolute-path"),
        pytest.param("https://provider.example/repo", id="remote-url"),
        pytest.param("line\nbreak", id="control-character"),
    ),
)
def test_metric_contract_rejects_locator_shaped_allowed_label_values(
    unsafe_value: str,
):
    with pytest.raises(ValueError, match="metric_label_value_invalid"):
        sanitize_code_traceability_metric_event(
            CodeTraceabilityMetricEvent(
                METRIC_CODE_EVIDENCE_SUBMISSION_TOTAL,
                labels={"reason_code": unsafe_value},
            )
        )


def test_metric_duration_requires_finite_non_negative_value():
    with pytest.raises(ValueError, match="metric_value_invalid"):
        sanitize_code_traceability_metric_event(
            CodeTraceabilityMetricEvent(
                METRIC_IMPLEMENTATION_TARGET_RESOLUTION_SUBMISSION_DURATION_SECONDS,
                value=float("nan"),
            )
        )
    with pytest.raises(ValueError, match="metric_value_invalid"):
        sanitize_code_traceability_metric_event(
            CodeTraceabilityMetricEvent(
                METRIC_IMPLEMENTATION_TARGET_RESOLUTION_SUBMISSION_DURATION_SECONDS,
                value=-0.01,
            )
        )


def test_created_event_factory_accepts_only_ids_states_counts_and_hashes():
    event = make_code_traceability_event(
        CodeEvidenceCreated,
        board_id="board-1",
        actor_id="agent-1",
        actor_type="agent",
        evidence_id="evidence-1",
        investigation_receipt_id="receipt-1",
        parent_type="spec",
        parent_id="spec-1",
        lifecycle_status="active",
        attestation_state="agent_attested",
        payload_sha256="a" * 64,
    )
    assert set(event.payload_for_storage()) == {
        "evidence_id",
        "investigation_receipt_id",
        "parent_type",
        "parent_id",
        "lifecycle_status",
        "attestation_state",
        "payload_sha256",
    }
