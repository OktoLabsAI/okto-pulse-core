"""SK-A coverage for the closed FR/TR/AC canonicalization boundary."""

from __future__ import annotations

import copy

import pytest

from okto_pulse.core.services.spec_entity_canonicalization import (
    DuplicateSpecChildIdError,
    canonicalize_spec_requirement_fields,
)


@pytest.mark.parametrize(
    ("field_name", "prefix"),
    [
        ("functional_requirements", "fr_"),
        ("technical_requirements", "tr_"),
        ("acceptance_criteria", "ac_"),
    ],
)
def test_requirement_collection_is_stable_and_idempotent_for_every_closed_type(
    field_name: str,
    prefix: str,
) -> None:
    authored = {
        field_name: [
            "Repeated authored text",
            "Repeated authored text",
            {"text": "Structured child", "locale": "pt"},
        ]
    }

    first = canonicalize_spec_requirement_fields(authored)
    replay = canonicalize_spec_requirement_fields(
        copy.deepcopy(first),
        existing_fields=copy.deepcopy(first),
    )

    assert replay == first
    children = first[field_name]
    assert children is not None
    assert [child["text"] for child in children] == [
        "Repeated authored text",
        "Repeated authored text",
        "Structured child",
    ]
    assert [child["status"] for child in children] == [
        "active",
        "active",
        "active",
    ]
    assert all(child["id"].startswith(prefix) for child in children)
    assert len({child["id"] for child in children}) == len(children)
    assert children[1]["id"] == f"{children[0]['id']}_1"
    assert children[2]["locale"] == "pt"


def test_whole_spec_reorder_reuses_existing_ids_across_fr_tr_and_ac() -> None:
    original = canonicalize_spec_requirement_fields(
        {
            "functional_requirements": ["FR A", "FR B"],
            "technical_requirements": ["TR A", "TR B"],
            "acceptance_criteria": ["AC A", "AC B"],
        }
    )
    reordered = canonicalize_spec_requirement_fields(
        {
            "functional_requirements": ["FR B", "FR A"],
            "technical_requirements": ["TR B", "TR A"],
            "acceptance_criteria": ["AC B", "AC A"],
        },
        existing_fields=original,
    )

    for field_name in (
        "functional_requirements",
        "technical_requirements",
        "acceptance_criteria",
    ):
        original_by_text = {
            child["text"]: child["id"] for child in original[field_name] or []
        }
        reordered_by_text = {
            child["text"]: child["id"] for child in reordered[field_name] or []
        }
        assert reordered_by_text == original_by_text


@pytest.mark.parametrize(
    "fields",
    [
        {
            "functional_requirements": [
                {"id": "requirement_duplicate", "text": "FR A"},
                {"id": "requirement_duplicate", "text": "FR B"},
            ]
        },
        {
            "functional_requirements": [
                {"id": "requirement_duplicate", "text": "FR"}
            ],
            "technical_requirements": [
                {"id": "requirement_duplicate", "text": "TR"}
            ],
            "acceptance_criteria": [],
        },
        {
            "functional_requirements": [],
            "technical_requirements": [
                {"id": "requirement_duplicate", "text": "TR"}
            ],
            "acceptance_criteria": [
                {"id": "requirement_duplicate", "text": "AC"}
            ],
        },
    ],
)
def test_duplicate_explicit_ids_fail_closed_within_or_across_collections(
    fields: dict[str, list[dict[str, str]]],
) -> None:
    with pytest.raises(
        DuplicateSpecChildIdError,
        match="duplicate_spec_child_id",
    ):
        canonicalize_spec_requirement_fields(fields)


def test_generated_id_cannot_silently_collide_with_a_later_explicit_id() -> None:
    generated = canonicalize_spec_requirement_fields(
        {"functional_requirements": ["Collision candidate"]}
    )
    generated_id = generated["functional_requirements"][0]["id"]  # type: ignore[index]

    with pytest.raises(
        DuplicateSpecChildIdError,
        match="duplicate_spec_child_id",
    ):
        canonicalize_spec_requirement_fields(
            {
                "functional_requirements": [
                    "Collision candidate",
                    {"id": generated_id, "text": "Explicit collision"},
                ]
            }
        )
