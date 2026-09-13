from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.kg.cognitive_action_center import CognitiveActionCenterReadModel, _RawSignal

NOW = datetime(2026, 9, 13, tzinfo=timezone.utc)


def row(name, signal, revisit=None):
    return _RawSignal(
        artifact_id=f"spec:{name}", source_ref_original=f"spec:{name}",
        artifact_type="spec", signal=signal, signal_source="cognitive_item",
        status="skipped" if signal in {"skipped", "revisit_required"} else "pending",
        outcome_type=None, reason_code=None, error_cause=None, revisit_at=revisit,
        justification="Reviewed source; explanation is audit text", actor="human",
    )


def filtered(rows, signal):
    return CognitiveActionCenterReadModel._apply_filters(
        rows, signal=signal, artifact_id=None, source_ref=None, reason_code=None,
        status=None, search=None, now=NOW,
    )


def test_sections_partition_current_records_and_expired_or_invalid_reviews():
    rows = [row("pending", "cognitive_pending"), row("error", "dlq"),
            row("debt", "open_canonical_debt"), row("old", "terminal_history"),
            row("waived", "skipped"), row("future", "revisit_required", "2027-01-01T00:00:00Z"),
            row("due", "revisit_required", NOW.isoformat()),
            row("invalid", "revisit_required", "bad"), row("missing", "revisit_required")]
    attention = filtered(rows, "attention")
    assert [r.artifact_id for r in attention] == [f"spec:{v}" for v in ("pending", "error", "debt", "due", "invalid", "missing")]
    assert [r.artifact_id for r in filtered(rows, "deferred")] == ["spec:waived", "spec:future"]
    assert filtered(rows, "terminal_history") == [rows[3]]
    assert filtered(rows, "all") == rows
    assert filtered(rows, "dlq") == [rows[1]]


@pytest.mark.asyncio
async def test_section_filter_precedes_pagination_and_preserves_service_verdict_and_audit():
    verdict = SimpleNamespace(readiness_effect="advisory", blocking=False, precedence_explanation={"tier": "fixture"})
    service = SimpleNamespace(_now=lambda: NOW, evaluate_artifact=AsyncMock(return_value=verdict))
    model = CognitiveActionCenterReadModel(service)
    rows = [row("history", "terminal_history")] + [row(str(i), "cognitive_pending") for i in range(230)]
    model._gather = AsyncMock(return_value=rows)
    response = await model.list_signals(None, board_id="board", signal="attention", limit=25, offset=200)
    assert response["summary"]["total"] == 230
    assert len(response["items"]) == 25
    assert response["items"][0]["artifact_id"] == "spec:200"
    assert response["items"][-1]["artifact_id"] == "spec:224"
    assert service.evaluate_artifact.await_count == 25
    assert all(i["readiness_effect"] == "advisory" and not i["blocking"] for i in response["items"])
    assert response["items"][0]["actor"] == "human"
    assert "explanation" in response["items"][0]["justification"]
