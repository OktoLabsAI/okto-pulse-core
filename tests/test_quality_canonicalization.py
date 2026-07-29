from __future__ import annotations

import hashlib

import pytest

from okto_pulse.core.domain.quality_canonicalization import (
    CanonicalizationError,
    authority_digest_v1,
    canonical_json_bytes,
    canonical_sha256,
    clarification_digest_v1,
    clarification_snapshot_v1,
    policy_digest_v1,
    ruleset_digest_v1,
    semantic_content_digest_v1,
    semantic_snapshot_v1,
    taxonomy_digest_v1,
)


def test_canonical_json_v1_normalizes_unicode_line_endings_and_object_order() -> None:
    composed = {"z": "café\r\nok", "a": [{"b": 2, "a": 1}]}
    decomposed = {"a": [{"a": 1, "b": 2}], "z": "cafe\u0301\nok"}

    assert canonical_json_bytes(composed) == canonical_json_bytes(decomposed)
    assert canonical_json_bytes(composed) == (
        '{"a":[{"a":1,"b":2}],"z":"café\\nok"}'.encode()
    )
    assert canonical_sha256(composed) == hashlib.sha256(
        canonical_json_bytes(composed)
    ).hexdigest()


def test_canonical_json_v1_preserves_semantic_list_order() -> None:
    assert canonical_sha256({"items": ["a", "b"]}) != canonical_sha256(
        {"items": ["b", "a"]}
    )


@pytest.mark.parametrize(
    "invalid",
    [
        {1: "non-string key"},
        {"value": {1, 2}},
        {"value": float("nan")},
        {"value": b"bytes"},
    ],
)
def test_canonical_json_v1_fails_closed_for_unsupported_values(invalid) -> None:
    with pytest.raises(CanonicalizationError):
        canonical_json_bytes(invalid)


def test_semantic_manifests_exclude_status_scope_quality_and_mockups() -> None:
    payload = {
        "title": "R",
        "description": "D",
        "in_scope": ["in"],
        "out_of_scope": ["out"],
        "analysis": "A",
        "decisions": ["D1"],
        "scope": "legacy ghost",
        "status": "done",
        "quality_summaries": [{"score": 1}],
        "screen_mockups": ["<html>"],
    }

    assert semantic_snapshot_v1("refinement", payload) == {
        "title": "R",
        "description": "D",
        "in_scope": ["in"],
        "out_of_scope": ["out"],
        "analysis": "A",
        "decisions": ["D1"],
    }
    baseline = semantic_content_digest_v1("refinement", payload)
    assert baseline == semantic_content_digest_v1(
        "refinement",
        {
            **payload,
            "status": "approved",
            "scope": "changed",
            "screen_mockups": ["other"],
        },
    )


def test_spec_semantic_snapshot_excludes_volatile_nested_metadata() -> None:
    payload = {
        "title": "Spec",
        "functional_requirements": [
            {
                "id": "fr_1",
                "text": "The system shall respond in 100 ms.",
                "notes": "Authored rationale",
                "status": "active",
                "linked_task_ids": ["card-1"],
                "created_at": "2026-01-01T00:00:00Z",
                "created_by": "actor-1",
            }
        ],
        "test_scenarios": [
            {
                "id": "ts_1",
                "title": "Latency",
                "scenario_type": "integration",
                "status": "passed",
                "evidence": {"runner": "local"},
                "latest_evidence": {"status": "passed"},
                "linked_task_ids": ["test-card-1"],
            }
        ],
    }
    changed_metadata = {
        **payload,
        "functional_requirements": [
            {
                **payload["functional_requirements"][0],
                "status": "deprecated",
                "linked_task_ids": ["card-2"],
                "created_at": "2026-02-01T00:00:00Z",
                "created_by": "actor-2",
            }
        ],
        "test_scenarios": [
            {
                **payload["test_scenarios"][0],
                "status": "failed",
                "evidence": {"runner": "remote"},
                "latest_evidence": {"status": "failed"},
                "linked_task_ids": ["test-card-2"],
            }
        ],
    }

    assert semantic_content_digest_v1(
        "spec",
        payload,
    ) == semantic_content_digest_v1("spec", changed_metadata)
    snapshot = semantic_snapshot_v1("spec", payload)
    assert snapshot["functional_requirements"] == [
        {
            "id": "fr_1",
            "text": "The system shall respond in 100 ms.",
            "notes": "Authored rationale",
        }
    ]
    assert snapshot["test_scenarios"] == [
        {
            "id": "ts_1",
            "title": "Latency",
            "scenario_type": "integration",
        }
    ]


def test_nested_api_contract_property_named_status_remains_semantic() -> None:
    original = {
        "title": "Spec",
        "api_contracts": [
            {
                "id": "api_1",
                "path": "/items",
                "response_success_json": {
                    "status": {"type": "string", "enum": ["ready"]},
                },
                "status": "active",
            }
        ],
    }
    changed = {
        **original,
        "api_contracts": [
            {
                **original["api_contracts"][0],
                "response_success_json": {
                    "status": {"type": "string", "enum": ["done"]},
                },
                "status": "deprecated",
            }
        ],
    }
    metadata_only = {
        **original,
        "api_contracts": [
            {
                **original["api_contracts"][0],
                "status": "deprecated",
            }
        ],
    }

    assert semantic_content_digest_v1(
        "spec",
        original,
    ) != semantic_content_digest_v1("spec", changed)
    assert semantic_content_digest_v1(
        "spec",
        original,
    ) == semantic_content_digest_v1("spec", metadata_only)


def test_clarification_digest_ignores_unanswered_proposals_and_metadata() -> None:
    answered = {
        "id": "qa_2",
        "revision": 3,
        "question": "Which timeout?",
        "answer": "30 seconds",
        "answered_at": "2026-07-27T10:00:00Z",
        "asked_by": "agent-a",
    }
    unanswered = {
        "id": "qa_1",
        "revision": 1,
        "question": "What is the fallback?",
        "answer": None,
        "created_at": "2026-07-27T09:00:00Z",
    }
    baseline = clarification_digest_v1([answered])
    with_proposal = clarification_digest_v1([unanswered, answered])
    metadata_changed = clarification_digest_v1(
        [
            {
                **answered,
                "answered_at": "2026-07-28T11:00:00Z",
                "asked_by": "agent-b",
            }
        ]
    )

    assert baseline == with_proposal == metadata_changed
    assert clarification_snapshot_v1([unanswered])["answered"] == []


def test_answering_or_revising_a_question_changes_clarification_digest() -> None:
    unanswered = [{"id": "qa_1", "revision": 1, "question": "Timeout?", "answer": None}]
    answered = [
        {
            "id": "qa_1",
            "revision": 2,
            "question": "Timeout?",
            "answer": "30 seconds",
        }
    ]
    revised = [{**answered[0], "revision": 3, "answer": "60 seconds"}]

    assert clarification_digest_v1(unanswered) != clarification_digest_v1(answered)
    assert clarification_digest_v1(answered) != clarification_digest_v1(revised)


def test_choice_only_answer_is_included_and_selected_aliases_are_canonical() -> None:
    persisted = {
        "id": "qa_choice",
        "revision": 2,
        "question": "Which targets?",
        "question_type": "multi_choice",
        "choices": ["api", "mcp", "ui"],
        "allow_free_text": False,
        "answer": None,
        "selected": ["mcp", "api"],
        "answered_at": "2026-07-27T10:00:00Z",
    }
    boundary = {
        **persisted,
        "selected": None,
        "selected_choices": ["api", "mcp"],
        "answered_at": "2026-07-28T10:00:00Z",
    }

    assert clarification_digest_v1([persisted]) == clarification_digest_v1(
        [boundary]
    )
    answered = clarification_snapshot_v1([persisted])["answered"]
    assert answered[0]["answer"] is None
    assert answered[0]["selected"] == ["api", "mcp"]


def test_answered_at_without_text_or_selection_still_marks_answered() -> None:
    snapshot = clarification_snapshot_v1(
        [
            {
                "id": "qa_empty_choice",
                "question": "No applicable target?",
                "question_type": "choice",
                "choices": ["none"],
                "answer": None,
                "selected": [],
                "answered_at": "2026-07-27T10:00:00Z",
            }
        ]
    )

    assert [item["qa_id"] for item in snapshot["answered"]] == [
        "qa_empty_choice"
    ]


def test_tombstoning_an_unanswered_question_changes_the_digest() -> None:
    untouched = {
        "id": "qa_1",
        "revision": 1,
        "question": "Which fallback?",
        "answer": None,
    }
    tombstoned = {
        **untouched,
        "revision": 2,
        "tombstoned": True,
    }

    assert clarification_snapshot_v1([untouched])["answered"] == []
    assert clarification_digest_v1([untouched]) != clarification_digest_v1(
        [tombstoned]
    )


def test_choice_contract_and_lifecycle_are_clarification_digest_inputs() -> None:
    answered = {
        "id": "qa_1",
        "question": "Which mode?",
        "question_type": "choice",
        "choices": ["safe", "fast"],
        "allow_free_text": False,
        "selected_choice": "safe",
    }
    baseline = clarification_digest_v1([answered])

    assert baseline != clarification_digest_v1(
        [{**answered, "choices": ["fast", "safe"]}]
    )
    assert baseline != clarification_digest_v1(
        [{**answered, "allow_free_text": True}]
    )
    assert baseline != clarification_digest_v1(
        [{**answered, "tombstoned": True}]
    )


def test_conflicting_selection_aliases_fail_closed() -> None:
    with pytest.raises(
        CanonicalizationError,
        match="clarification_selection_alias_conflict",
    ):
        clarification_snapshot_v1(
            [
                {
                    "id": "qa_1",
                    "question": "Which mode?",
                    "question_type": "choice",
                    "selected": ["safe"],
                    "selected_choice": "fast",
                }
            ]
        )


def test_named_normative_digest_builders_are_versioned_and_namespaced() -> None:
    manifest = {"rules": ["r1"]}
    assert ruleset_digest_v1("v1", manifest) == ruleset_digest_v1("v1", manifest)
    assert ruleset_digest_v1("v1", manifest) != ruleset_digest_v1("v2", manifest)
    assert len(
        {
            ruleset_digest_v1("v1", manifest),
            taxonomy_digest_v1("v1", manifest),
            policy_digest_v1("v1", manifest),
            authority_digest_v1("v1", manifest),
        }
    ) == 4


def test_semantic_manifest_distinguishes_absent_from_empty() -> None:
    absent = semantic_content_digest_v1("ideation", {"title": "I"})
    empty = semantic_content_digest_v1(
        "ideation",
        {
            "title": "I",
            "description": "",
            "problem_statement": "",
            "proposed_approach": "",
        },
    )
    assert absent != empty


def test_semantic_manifest_rejects_unsupported_subject_type() -> None:
    with pytest.raises(CanonicalizationError, match="unsupported_subject_type"):
        semantic_snapshot_v1("card", {})
