from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone

import pytest
from pydantic import TypeAdapter, ValidationError

from okto_pulse.core.domain.enums import IdeationStatus
from okto_pulse.core.domain.quality_assessment import QualityPageCursor
from okto_pulse.core.models.quality_assessment import (
    QualityFindingInput,
    decode_quality_cursor,
    encode_quality_cursor,
)
from okto_pulse.core.models.schemas import (
    IdeationPageItem,
    QualityAssessmentSummary,
)
from okto_pulse.core.services.ska_observability import (
    METRIC_PROJECTION_QUERIES_TOTAL,
    METRIC_VALIDATION_EXTERNAL_COGNITION_UOW_DURATION_SECONDS,
    SkaMetricEvent,
    observe_validation_uow_factory,
    observe_ska_projection_queries,
    reset_ska_metric_samples_for_tests,
    sanitize_ska_metric,
    ska_metric_counters,
    ska_metric_samples,
)


def _ideation(**extra: object) -> IdeationPageItem:
    now = datetime(2026, 7, 27, tzinfo=timezone.utc)
    return IdeationPageItem(
        id="ideation-1",
        board_id="board-1",
        title="Quality",
        status=IdeationStatus.DRAFT,
        version=3,
        created_by="actor-1",
        created_at=now,
        updated_at=now,
        **extra,
    )


def test_parent_quality_summary_is_permission_omittable_and_closed() -> None:
    denied = _ideation().model_dump(mode="json")
    assert "quality_summaries" not in denied

    allowed = _ideation(
        quality_summaries={
            "ambiguity": {
                "edition": 3,
                "state": "current",
                "previous_count": 2,
                "current_result": {
                    "score": 0,
                    "scale": {
                        "kind": "ambiguity_score",
                        "min": 0,
                        "max": 5,
                        "direction": "lower_better",
                    },
                },
            }
        }
    ).model_dump(mode="json")
    assert set(allowed["quality_summaries"]["ambiguity"]) == {
        "edition",
        "state",
        "previous_count",
        "current_result",
    }


@pytest.mark.parametrize(
    "payload",
    (
        {
            "edition": 4,
            "state": "not_started",
            "previous_count": 3,
            "current_result": None,
        },
        {
            "edition": 4,
            "state": "current",
            "previous_count": 1,
            "current_result": {
                "score": 0,
                "scale": {
                    "kind": "ambiguity_score",
                    "min": 0,
                    "max": 5,
                    "direction": "lower_better",
                },
            },
        },
    ),
)
def test_quality_summary_response_schema_accepts_canonical_states(payload) -> None:
    result = TypeAdapter(QualityAssessmentSummary).validate_python(payload)
    assert result.model_dump(mode="json") == payload


@pytest.mark.parametrize(
    "payload",
    (
        {
            "edition": 4,
            "state": "current",
            "previous_count": 0,
            "current_result": None,
        },
        {
            "edition": 4,
            "state": "not_started",
            "previous_count": 0,
            "current_result": {
                "score": 0,
                "scale": {
                    "kind": "ambiguity_score",
                    "min": 0,
                    "max": 5,
                    "direction": "lower_better",
                },
            },
        },
    ),
)
def test_quality_summary_response_schema_rejects_incoherent_state(payload) -> None:
    with pytest.raises(ValidationError):
        TypeAdapter(QualityAssessmentSummary).validate_python(payload)


def test_quality_finding_input_rejects_server_owned_fields() -> None:
    with pytest.raises(ValidationError) as captured:
        QualityFindingInput.model_validate(
            {
                "finding_key": "f-1",
                "category_code": "functional_scope_behavior",
                "severity": "medium",
                "confidence": 0.9,
                "deterministic": False,
                "blocking_eligible": True,
                "title": "Missing actor",
                "detail": "The initiating actor is not stated.",
                "anchor": {"anchor_type": "whole_artifact"},
            }
        )
    assert captured.value.errors()[0]["type"] == "extra_forbidden"


def test_quality_cursor_round_trip_preserves_real_keyset_boundary() -> None:
    source = QualityPageCursor(
        created_at=datetime(2026, 7, 27, 12, 30, tzinfo=timezone.utc),
        item_id="qar-9",
        offset=50,
    )
    assert decode_quality_cursor(encode_quality_cursor(source)) == source
    with pytest.raises(ValueError, match="quality_cursor_invalid"):
        decode_quality_cursor("not-a-cursor")


def test_ska_metrics_retain_numeric_measurements_and_bounded_labels() -> None:
    reset_ska_metric_samples_for_tests()
    observe_ska_projection_queries(
        surface="quality_assessments",
        subject_type="spec",
        query_count=4,
        duration_ms=12.5,
        payload_bytes=256,
    )
    assert ska_metric_samples() == (
        {
            "metric_name": METRIC_PROJECTION_QUERIES_TOTAL,
            "value": 4,
            "surface": "quality_assessments",
            "subject_type": "spec",
            "outcome": "success",
            "duration_ms": 12.5,
            "payload_bytes": 256,
        },
    )
    counter = ska_metric_counters()[0]
    assert counter["count"] == 1
    assert counter["sums"] == {
        "value": 4,
        "duration_ms": 12,
        "payload_bytes": 256,
    }


def test_ska_metrics_reject_ids_and_free_form_labels() -> None:
    with pytest.raises(ValueError, match="ska_metric_labels_invalid"):
        sanitize_ska_metric(
            SkaMetricEvent(
                metric_name=METRIC_PROJECTION_QUERIES_TOTAL,
                labels={
                    "surface": "quality_findings",
                    "subject_type": "ideation",
                    "outcome": "success",
                    "board_id": "board-1",
                },
            )
        )
    with pytest.raises(
        ValueError,
        match="ska_metric_label_value_invalid:surface",
    ):
        sanitize_ska_metric(
            SkaMetricEvent(
                metric_name=METRIC_PROJECTION_QUERIES_TOTAL,
                labels={
                    "surface": "user-defined-surface",
                    "subject_type": "ideation",
                    "outcome": "success",
                },
            )
        )


@pytest.mark.asyncio
async def test_external_cognition_metric_wraps_only_the_short_uow() -> None:
    reset_ska_metric_samples_for_tests()
    phases: list[str] = []

    async def external_preflight() -> None:
        phases.append("preflight")

    @asynccontextmanager
    async def raw_uow(**_kwargs):
        phases.append("uow_enter")
        try:
            yield object()
        finally:
            phases.append("uow_exit")

    await external_preflight()
    assert ska_metric_samples() == ()

    observed = observe_validation_uow_factory(
        raw_uow,
        assessment_kind="ambiguity",
        subject_type="ideation",
    )
    async with observed(actor=object()):
        phases.append("submit")

    assert phases == ["preflight", "uow_enter", "submit", "uow_exit"]
    samples = tuple(
        item
        for item in ska_metric_samples()
        if item["metric_name"]
        == METRIC_VALIDATION_EXTERNAL_COGNITION_UOW_DURATION_SECONDS
    )
    assert len(samples) == 1
    assert samples[0]["assessment_kind"] == "ambiguity"
    assert samples[0]["subject_type"] == "ideation"
    assert samples[0]["outcome"] == "success"
