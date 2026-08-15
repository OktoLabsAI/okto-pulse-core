"""Canonical open-Q&A badge contract for bounded list projections."""

from __future__ import annotations

import pytest
from pydantic import TypeAdapter, ValidationError

from okto_pulse.core.application.use_cases.entity_pagination import SURFACES
from okto_pulse.core.models.schemas import (
    CardPageItem,
    CardSummary,
    IdeationPageItem,
    IdeationSummary,
    RefinementPageItem,
    RefinementSummary,
    SpecPageItem,
    SpecSummary,
    SprintPageItem,
    SprintSummary,
)


@pytest.mark.parametrize(
    "projection",
    (
        IdeationPageItem,
        IdeationSummary,
        RefinementPageItem,
        RefinementSummary,
        SpecPageItem,
        SpecSummary,
        SprintPageItem,
        SprintSummary,
        CardPageItem,
        CardSummary,
    ),
)
def test_open_qa_count_is_optional_omittable_and_non_negative(
    projection: type,
) -> None:
    field = projection.model_fields["open_qa_count"]

    assert not field.is_required()
    assert field.default is None
    assert "open_qa_count" not in projection.model_construct().model_dump()
    assert (
        projection.model_construct(open_qa_count=2).model_dump()["open_qa_count"] == 2
    )
    with pytest.raises(ValidationError):
        TypeAdapter(field.rebuild_annotation()).validate_python(-1)


@pytest.mark.parametrize(
    "surface",
    (
        "ideation_list",
        "refinement_list",
        "refinement_board",
        "spec_list",
        "sprint_list",
        "card_list",
        "kanban_column",
    ),
)
def test_open_qa_count_is_selected_by_every_badge_list_surface(surface: str) -> None:
    assert "open_qa_count" in SURFACES[surface].select_fields
