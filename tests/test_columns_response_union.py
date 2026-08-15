"""C6 columns response schema: canonical card and exclusive oneOf shapes."""

from __future__ import annotations

from copy import deepcopy

import jsonschema
import pytest
from pydantic import ValidationError

from okto_pulse.core.models import ColumnsResponseUnion
from okto_pulse.core.models.schemas import CardSummary


CARD = {
    "id": "card-1",
    "board_id": "board-1",
    "spec_id": None,
    "title": "Card",
    "description": None,
    "status": "not_started",
    "priority": "medium",
    "position": 0,
    "assignee_id": None,
    "created_by": "agent-1",
    "created_at": "2026-07-20T00:00:00Z",
    "updated_at": "2026-07-20T00:00:00Z",
    "due_date": None,
    "labels": [],
    "test_scenario_ids": None,
    "conclusions": None,
    "card_type": "normal",
    "origin_task_id": None,
    "severity": None,
    "linked_test_task_ids": None,
    "archived": False,
    "open_qa_count": 0,
    "current_rejection_kind": None,
    "current_rejection_id": None,
    "current_rejection_code": None,
    "current_rejection_summary": None,
}

OPTIONAL_CARD_FIELDS = {
    "open_qa_count",
    "current_rejection_kind",
    "current_rejection_id",
    "current_rejection_code",
    "current_rejection_summary",
}

COLUMN_META = {
    "total_filtered": 1,
    "total_overall": 1,
    "has_more": False,
    "facets": {"card_type": {"normal": 1}},
}

LEGACY = {
    "board_id": "board-1",
    "columns": {"not_started": [CARD]},
}

OPT_IN = {
    **LEGACY,
    "columns_meta": {
        "columns": {"not_started": COLUMN_META},
        "facets": {"assignee": [{"value": None, "count": 1}]},
    },
}

PAGE = {
    "board_id": "board-1",
    "column": "not_started",
    "items": [CARD],
    "meta": COLUMN_META,
    "offset": 0,
    "limit": 25,
    "next_offset": None,
}

SCHEMA = ColumnsResponseUnion.model_json_schema()


def _matching_variants(payload: dict) -> int:
    return sum(
        jsonschema.Draft202012Validator({"$defs": SCHEMA["$defs"], **variant}).is_valid(
            payload
        )
        for variant in SCHEMA["oneOf"]
    )


def test_card_summary_is_the_canonical_projection() -> None:
    schema = CardSummary.model_json_schema()
    expected = set(CARD)

    assert set(schema["properties"]) == expected
    assert set(schema["required"]) == expected - OPTIONAL_CARD_FIELDS


@pytest.mark.parametrize(
    ("payload", "runtime_type"),
    [
        (LEGACY, "ColumnsLegacyResponse"),
        (OPT_IN, "ColumnsOptInResponse"),
        (PAGE, "ColumnPageResponse"),
    ],
)
def test_valid_shape_matches_exactly_one_variant(
    payload: dict,
    runtime_type: str,
) -> None:
    assert "anyOf" not in SCHEMA
    assert len(SCHEMA["oneOf"]) == 3
    assert _matching_variants(payload) == 1
    assert (
        type(ColumnsResponseUnion.model_validate(payload).root).__name__ == runtime_type
    )
    jsonschema.Draft202012Validator(SCHEMA).validate(payload)


@pytest.mark.parametrize(
    "payload",
    [
        {**OPT_IN, **PAGE},
        {**LEGACY, "column": "not_started"},
        {**OPT_IN, "items": []},
        {**PAGE, "columns": {}},
    ],
)
def test_hybrid_shapes_match_no_variant(payload: dict) -> None:
    assert _matching_variants(payload) == 0
    assert not jsonschema.Draft202012Validator(SCHEMA).is_valid(payload)
    with pytest.raises(ValidationError):
        ColumnsResponseUnion.model_validate(payload)


def test_shapes_are_open_to_unrelated_future_fields() -> None:
    payload = deepcopy(OPT_IN)
    payload["future_top_level"] = {"version": 2}
    payload["columns_meta"]["future_meta"] = True

    parsed = ColumnsResponseUnion.model_validate(payload).root

    assert parsed.model_extra == {"future_top_level": {"version": 2}}
    assert parsed.columns_meta.model_extra == {"future_meta": True}
    jsonschema.Draft202012Validator(SCHEMA).validate(payload)
