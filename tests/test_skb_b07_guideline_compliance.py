"""Native policy keysets preserve filter, projection and signature boundaries."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from okto_pulse.core.domain.guideline_compliance import PolicyCursorCodec
from okto_pulse.core.domain.guideline_semantic_projection import (
    SEMANTIC_FINDING_ORDERING,
    SEMANTIC_GUIDELINE_KEYSET_CONTRACT_VERSION,
    SEMANTIC_ASSESSMENT_ORDERING,
    SemanticFindingPageCursor,
    SemanticGuidelineProjection,
    SemanticAssessmentPageCursor,
)
from okto_pulse.core.domain.guideline_policy import PolicyEntityType, GuidelinePolicyContractError
from okto_pulse.core.ports.guideline_policy import (
    GuidelinePolicyCursorConflict,
    SemanticFindingListQuery,
    SemanticAssessmentListQuery,
)


NOW = datetime(2026, 7, 29, 14, tzinfo=timezone.utc)


def test_policy_keyset_cursor_is_bound_to_filter_and_projection() -> None:
    first = SemanticAssessmentListQuery(
        board_id="board-1",
        entity_type=PolicyEntityType.SPEC,
        projection=SemanticGuidelineProjection.SUMMARY,
    )
    cursor = SemanticAssessmentPageCursor(
        at=NOW,
        item_id="receipt-1",
        filter_digest=first.filter_digest,
        projection_digest=first.projection_digest,
    )
    second = SemanticAssessmentListQuery(
        board_id="board-1",
        entity_type=PolicyEntityType.SPEC,
        projection=SemanticGuidelineProjection.SUMMARY,
        cursor=cursor,
    )

    assert second.cursor is cursor
    assert cursor.schema_version == SEMANTIC_GUIDELINE_KEYSET_CONTRACT_VERSION
    assert cursor.ordering == SEMANTIC_ASSESSMENT_ORDERING
    with pytest.raises(
        GuidelinePolicyCursorConflict,
        match="semantic_assessment_cursor_context_mismatch",
    ):
        SemanticAssessmentListQuery(
            board_id="board-1",
            entity_type=PolicyEntityType.SPEC,
            projection=SemanticGuidelineProjection.DETAIL,
            cursor=cursor,
        )


def test_finding_keyset_cursor_rejects_filter_drift() -> None:
    first = SemanticFindingListQuery(
        board_id="board-1",
        guideline_id="guideline-1",
        projection=SemanticGuidelineProjection.SUMMARY,
    )
    cursor = SemanticFindingPageCursor(
        at=NOW,
        item_id="finding-1",
        filter_digest=first.filter_digest,
        projection_digest=first.projection_digest,
    )

    assert cursor.ordering == SEMANTIC_FINDING_ORDERING
    with pytest.raises(
        GuidelinePolicyCursorConflict,
        match="semantic_finding_cursor_context_mismatch",
    ):
        SemanticFindingListQuery(
            board_id="board-1",
            guideline_id="guideline-2",
            projection=SemanticGuidelineProjection.SUMMARY,
            cursor=cursor,
        )


def test_policy_cursor_codec_is_opaque_tamper_evident_and_kind_bound() -> None:
    query = SemanticAssessmentListQuery(board_id="board-1")
    cursor = SemanticAssessmentPageCursor(
        at=NOW,
        item_id="receipt-1",
        filter_digest=query.filter_digest,
        projection_digest=query.projection_digest,
    )
    codec = PolicyCursorCodec(b"policy-cursor-test-key-32-bytes!!")
    token = codec.encode(cursor)

    assert "receipt-1" not in token
    assert codec.decode(token, expected_kind="semantic_assessment") == cursor
    with pytest.raises(GuidelinePolicyContractError, match="invalid_cursor"):
        codec.decode(token, expected_kind="semantic_finding")
    replacement = "A" if token[-1] != "A" else "B"
    with pytest.raises(GuidelinePolicyContractError, match="invalid_cursor"):
        codec.decode(token[:-1] + replacement, expected_kind="semantic_assessment")
    payload, signature = token.split(".")
    with pytest.raises(GuidelinePolicyContractError, match="invalid_cursor"):
        codec.decode(f"{payload}!!!!.{signature}", expected_kind="semantic_assessment")
    with pytest.raises(GuidelinePolicyContractError, match="invalid_cursor"):
        codec.decode(f"{payload}.{signature}!!!!", expected_kind="semantic_assessment")
