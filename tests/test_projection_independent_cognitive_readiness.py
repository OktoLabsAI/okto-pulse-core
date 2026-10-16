"""F6E: technical precedence must not hide a substantive cognitive obligation."""
from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.kg.cognitive_readiness import (
    compose_cognitive_readiness,
    compose_readiness,
)
from okto_pulse.core.kg.rebuild_audit import CognitiveConsolidationItem


NOW = datetime(2026, 9, 24, tzinfo=timezone.utc)


def _item(status, *, reason=None, revisit=None):
    return CognitiveConsolidationItem(
        item_id="item", board_id="board", kg_generation_id="generation",
        source_ref="bug:bug-id", artifact_type="bug", status=status,
        recorded_at=NOW.isoformat(), reason_code=reason, revisit_at=revisit,
    )


@pytest.mark.parametrize("technical_dlq,canonical_debt_open", [(True, False), (False, True), (True, True)])
@pytest.mark.parametrize("items,tier,blocking", [
    ([], "ready", False),
    ([_item("pending")], "cognitive_active", True),
    ([_item("failed")], "cognitive_active", True),
    ([_item("skipped", reason="evidence_insufficient", revisit=(NOW - timedelta(seconds=1)).isoformat())], "skip_expired", True),
    ([_item("skipped", reason="evidence_insufficient")], "skip_expired", True),
    ([_item("skipped", reason="evidence_insufficient", revisit=(NOW + timedelta(seconds=1)).isoformat())], "skip_valid", False),
    ([_item("skipped", reason="no_reusable_learning")], "skip_valid", False),
    ([_item("consolidated")], "terminal_history", False),
])
def test_cognitive_verdict_survives_coexisting_projection_debt(
    items, tier, blocking, technical_dlq, canonical_debt_open,
):
    original_records = tuple(item.to_dict() for item in items)
    pipeline = compose_readiness(
        artifact_id="card:bug-id", cognitive_items=items, now=NOW,
        technical_dlq=technical_dlq, canonical_debt_open=canonical_debt_open,
    )
    assert pipeline.tier in {"technical_dlq", "canonical_debt_open"}
    cognitive = compose_cognitive_readiness(
        artifact_id="card:bug-id", cognitive_items=items, now=NOW,
    )
    assert cognitive.tier == tier
    assert cognitive.blocking is blocking
    assert tuple(item.to_dict() for item in items) == original_records


def test_task_advisory_does_not_hide_a_real_expired_obligation():
    empty = compose_cognitive_readiness(
        artifact_id="card:task-id", cognitive_items=[],
        has_reusable_cognition=False, now=NOW,
    )
    assert empty.tier == "advisory_no_cognition" and not empty.blocking
    expired = compose_cognitive_readiness(
        artifact_id="card:task-id",
        cognitive_items=[_item("skipped", reason="path_b_pending")],
        has_reusable_cognition=False, now=NOW,
    )
    assert expired.tier == "skip_expired" and expired.blocking
