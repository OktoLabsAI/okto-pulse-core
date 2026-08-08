from __future__ import annotations

from copy import deepcopy

import pytest

from okto_pulse.core.domain.guideline_policy import PolicyEntityType
from okto_pulse.core.domain.guideline_semantic_snapshot import (
    SEMANTIC_POLICY_ARTIFACT_FIELD_MANIFEST_V1,
    SEMANTIC_POLICY_SUBJECT_SNAPSHOT_VERSION,
    SemanticPolicySubjectSnapshotError,
    semantic_policy_subject_content_digest_v1,
    semantic_policy_subject_snapshot_v1,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256


_DIGEST_A = "a" * 64
_DIGEST_B = "b" * 64


def _qa(item_id: str = "qa-1") -> dict[str, object]:
    return {
        "id": item_id,
        "revision": 2,
        "question": "Which boundary owns persistence?",
        "question_type": "choice",
        "choices": ["Core", "Community"],
        "allow_free_text": True,
        "answer": "Community",
        "selected": ["Community"],
        "lifecycle": "active",
        "tombstoned": False,
    }


def _resource(
    resource_type: str = "knowledge",
    resource_id: str = "kb-1",
    resource_version: int = 1,
    *,
    content_digest: str = _DIGEST_A,
    source_ref: str | None = None,
) -> dict[str, object]:
    return {
        "resource_type": resource_type,
        "resource_id": resource_id,
        "resource_version": resource_version,
        "content_digest": content_digest,
        "source_ref": source_ref,
    }


@pytest.mark.parametrize(
    ("subject_type", "artifact", "changed_field", "changed_value"),
    (
        (
            PolicyEntityType.IDEATION,
            {
                "title": "Semantic guidelines",
                "description": "Cognitive deterministic gates.",
                "problem_statement": "Predicates duplicate native gates.",
                "proposed_approach": "Use bounded cognitive metrics.",
            },
            "problem_statement",
            "A different problem.",
        ),
        (
            PolicyEntityType.REFINEMENT,
            {
                "title": "Refine semantic metrics",
                "description": "Define exact assessment inputs.",
                "in_scope": ["Q&A", "resources"],
                "out_of_scope": ["runtime predicates"],
                "analysis": "A closed snapshot prevents accidental drift.",
                "decisions": [{"id": "rd-1", "text": "Include all Q&A."}],
            },
            "analysis",
            "Changed analysis.",
        ),
        (
            PolicyEntityType.SPEC,
            {
                "title": "Semantic assessment contract",
                "description": "Seal cognitive metric receipts.",
                "functional_requirements": [
                    {"id": "fr-1", "text": "Scores shall be bounded."}
                ],
            },
            "description",
            "Changed contract.",
        ),
        (
            PolicyEntityType.CARD,
            {
                "title": "Implement snapshot",
                "description": "Create the Core helper.",
                "details": "Use a closed field manifest.",
                "card_type": "normal",
            },
            "details",
            "Use an open payload.",
        ),
        (
            PolicyEntityType.SPRINT,
            {
                "title": "Semantic delivery",
                "description": "Ship SK-B3.",
                "objective": "Replace executable predicates.",
                "expected_outcome": "Cognitive deterministic governance.",
            },
            "objective",
            "Changed objective.",
        ),
        (
            PolicyEntityType.TEST_SCENARIO,
            {
                "title": "Reject malformed evidence",
                "linked_criteria": ["ac-1"],
                "scenario_type": "negative",
                "given": "An invalid resource reference",
                "when": "The snapshot is created",
                "then": "The input is rejected",
                "notes": "No partial snapshot.",
            },
            "then",
            "The input is accepted.",
        ),
    ),
)
def test_snapshot_supports_every_closed_policy_subject_type(
    subject_type: PolicyEntityType,
    artifact: dict[str, object],
    changed_field: str,
    changed_value: object,
) -> None:
    baseline = semantic_policy_subject_content_digest_v1(
        subject_type=subject_type,
        artifact=artifact,
    )
    assert baseline != semantic_policy_subject_content_digest_v1(
        subject_type=subject_type,
        artifact={**artifact, changed_field: changed_value},
    )
    assert baseline == semantic_policy_subject_content_digest_v1(
        subject_type=subject_type,
        artifact={
            **artifact,
            "status": "done",
            "updated_at": "2026-07-30T12:00:00Z",
            "created_by": "another-actor",
            "unknown_future_metadata": {"volatile": True},
        },
    )

    snapshot = semantic_policy_subject_snapshot_v1(
        subject_type=subject_type,
        artifact=artifact,
    )
    assert tuple(snapshot["artifact"]) == (
        SEMANTIC_POLICY_ARTIFACT_FIELD_MANIFEST_V1[subject_type]
    )


def test_snapshot_has_an_explicit_contract_and_digest_of_the_whole_input() -> None:
    snapshot = semantic_policy_subject_snapshot_v1(
        subject_type=PolicyEntityType.SPEC,
        artifact={"title": "Spec"},
        q_and_a=(_qa(),),
        resource_refs=(_resource(),),
    )

    assert snapshot["contract"] == SEMANTIC_POLICY_SUBJECT_SNAPSHOT_VERSION
    assert snapshot["subject_type"] == "spec"
    assert semantic_policy_subject_content_digest_v1(
        subject_type=PolicyEntityType.SPEC,
        artifact={"title": "Spec"},
        q_and_a=(_qa(),),
        resource_refs=(_resource(),),
    ) == canonical_sha256(snapshot)


def test_artifact_projection_removes_only_structured_child_metadata() -> None:
    artifact = {
        "title": "HTTP contract",
        "functional_requirements": [
            {
                "id": "fr-1",
                "text": "Return the current status.",
                "status": "active",
                "created_at": "2026-07-30T10:00:00Z",
            }
        ],
        "api_contracts": [
            {
                "id": "api-1",
                "status": "active",
                "response_schema": {
                    "status": {"type": "string", "enum": ["ready"]}
                },
            }
        ],
    }
    changed_metadata = deepcopy(artifact)
    changed_metadata["functional_requirements"][0]["status"] = "deprecated"
    changed_metadata["api_contracts"][0]["status"] = "deprecated"
    changed_nested_schema = deepcopy(artifact)
    changed_nested_schema["api_contracts"][0]["response_schema"]["status"][
        "enum"
    ] = ["done"]

    baseline = semantic_policy_subject_content_digest_v1(
        subject_type=PolicyEntityType.SPEC,
        artifact=artifact,
    )
    assert baseline == semantic_policy_subject_content_digest_v1(
        subject_type=PolicyEntityType.SPEC,
        artifact=changed_metadata,
    )
    assert baseline != semantic_policy_subject_content_digest_v1(
        subject_type=PolicyEntityType.SPEC,
        artifact=changed_nested_schema,
    )


def test_q_and_a_is_exact_sorted_and_semantic() -> None:
    first = _qa("qa-a")
    second = {**_qa("qa-b"), "answer": None, "selected": []}
    snapshot = semantic_policy_subject_snapshot_v1(
        subject_type=PolicyEntityType.IDEATION,
        artifact={"title": "Idea"},
        q_and_a=(second, first),
    )

    assert [item["id"] for item in snapshot["q_and_a"]] == ["qa-a", "qa-b"]
    assert set(snapshot["q_and_a"][0]) == set(_qa())
    assert semantic_policy_subject_content_digest_v1(
        subject_type=PolicyEntityType.IDEATION,
        artifact={"title": "Idea"},
        q_and_a=(first,),
    ) != semantic_policy_subject_content_digest_v1(
        subject_type=PolicyEntityType.IDEATION,
        artifact={"title": "Idea"},
        q_and_a=({**first, "answer": "Core"},),
    )


@pytest.mark.parametrize(
    "q_and_a",
    (
        ({**_qa(), "created_at": "2026-07-30T10:00:00Z"},),
        (_qa(), _qa()),
        ({key: value for key, value in _qa().items() if key != "answer"},),
        ({**_qa(), "revision": 0},),
        ({**_qa(), "allow_free_text": 1},),
        ({**_qa(), "selected": "Community"},),
    ),
)
def test_q_and_a_rejects_extras_duplicates_missing_fields_and_invalid_types(
    q_and_a: tuple[dict[str, object], ...],
) -> None:
    with pytest.raises(SemanticPolicySubjectSnapshotError):
        semantic_policy_subject_snapshot_v1(
            subject_type=PolicyEntityType.SPEC,
            artifact={"title": "Spec"},
            q_and_a=q_and_a,
        )


def test_resource_refs_are_exact_sorted_and_normalized() -> None:
    refs = (
        _resource("mockup", "mock-2", 3, content_digest=_DIGEST_B.upper()),
        _resource("knowledge", "kb-1", 2),
        _resource("architecture", "arch-1", 1),
    )
    snapshot = semantic_policy_subject_snapshot_v1(
        subject_type=PolicyEntityType.SPEC,
        artifact={"title": "Spec"},
        resource_refs=refs,
    )

    assert [
        (
            item["resource_type"],
            item["resource_id"],
            item["resource_version"],
        )
        for item in snapshot["resource_refs"]
    ] == [
        ("architecture", "arch-1", 1),
        ("knowledge", "kb-1", 2),
        ("mockup", "mock-2", 3),
    ]
    assert snapshot["resource_refs"][-1]["content_digest"] == _DIGEST_B
    assert snapshot["resource_refs"][0]["source_ref"] is None


def test_different_versions_of_one_resource_are_distinct_references() -> None:
    snapshot = semantic_policy_subject_snapshot_v1(
        subject_type=PolicyEntityType.SPEC,
        artifact={"title": "Spec"},
        resource_refs=(
            _resource("knowledge", "kb-1", 2),
            _resource("knowledge", "kb-1", 1),
        ),
    )

    assert [
        item["resource_version"] for item in snapshot["resource_refs"]
    ] == [1, 2]


@pytest.mark.parametrize(
    "resource_refs",
    (
        ({**_resource(), "updated_at": "2026-07-30T10:00:00Z"},),
        (_resource(), _resource()),
        ({**_resource(), "resource_type": "file"},),
        ({**_resource(), "resource_version": 0},),
        ({**_resource(), "resource_version": True},),
        ({**_resource(), "content_digest": "not-a-sha256"},),
        ({key: value for key, value in _resource().items() if key != "resource_id"},),
    ),
)
def test_resource_refs_reject_invalid_closed_contracts(
    resource_refs: tuple[dict[str, object], ...],
) -> None:
    with pytest.raises(SemanticPolicySubjectSnapshotError):
        semantic_policy_subject_snapshot_v1(
            subject_type=PolicyEntityType.SPEC,
            artifact={"title": "Spec"},
            resource_refs=resource_refs,
        )


def test_subject_and_collection_boundaries_fail_closed() -> None:
    with pytest.raises(SemanticPolicySubjectSnapshotError):
        semantic_policy_subject_snapshot_v1(
            subject_type="spec",  # type: ignore[arg-type]
            artifact={"title": "Spec"},
        )
    with pytest.raises(SemanticPolicySubjectSnapshotError):
        semantic_policy_subject_snapshot_v1(
            subject_type=PolicyEntityType.SPEC,
            artifact="Spec",  # type: ignore[arg-type]
        )
    with pytest.raises(SemanticPolicySubjectSnapshotError):
        semantic_policy_subject_snapshot_v1(
            subject_type=PolicyEntityType.SPEC,
            artifact={"title": "Spec"},
            q_and_a="qa",  # type: ignore[arg-type]
        )
    with pytest.raises(SemanticPolicySubjectSnapshotError):
        semantic_policy_subject_snapshot_v1(
            subject_type=PolicyEntityType.SPEC,
            artifact={"title": "Spec"},
            resource_refs="kb",  # type: ignore[arg-type]
        )
