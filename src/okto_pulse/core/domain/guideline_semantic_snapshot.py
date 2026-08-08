"""Canonical semantic subject snapshots for guideline assessments.

This module is a pure, versioned boundary between persisted Pulse artifacts
and cognitive guideline assessment.  It deliberately does not reuse the SK-A
content digest: guideline assessments also consume exact Q&A and governed
resource references, while the existing quality-assessment contract must
remain unchanged.

Raw artifacts are projected through a closed authorial manifest.  Lifecycle,
ownership, execution, linkage, and derived-quality metadata therefore cannot
make a semantic guideline receipt stale merely because those fields happen to
be present on an ORM or transport payload.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Any, Final

from okto_pulse.core.domain.guideline_policy import PolicyEntityType
from okto_pulse.core.domain.quality_canonicalization import (
    CanonicalizationError,
    canonical_sha256,
    normalize_canonical_value,
)


SEMANTIC_POLICY_SUBJECT_SNAPSHOT_VERSION: Final = (
    "semantic-policy-subject-snapshot/v1"
)

SEMANTIC_POLICY_ARTIFACT_FIELD_MANIFEST_V1: Mapping[
    PolicyEntityType, tuple[str, ...]
] = MappingProxyType(
    {
        PolicyEntityType.IDEATION: (
            "title",
            "description",
            "problem_statement",
            "proposed_approach",
        ),
        PolicyEntityType.REFINEMENT: (
            "title",
            "description",
            "in_scope",
            "out_of_scope",
            "analysis",
            "decisions",
        ),
        PolicyEntityType.SPEC: (
            "title",
            "description",
            "context",
            "functional_requirements",
            "technical_requirements",
            "acceptance_criteria",
            "test_scenarios",
            "business_rules",
            "api_contracts",
            "integration_requirements",
            "observability_requirements",
            "decisions",
        ),
        PolicyEntityType.CARD: (
            "title",
            "description",
            "details",
            "card_type",
            "severity",
            "expected_behavior",
            "observed_behavior",
            "steps_to_reproduce",
            "action_plan",
            "test_scenario_ids",
            "linked_test_task_ids",
            "conclusions",
        ),
        PolicyEntityType.SPRINT: (
            "title",
            "description",
            "objective",
            "expected_outcome",
            "lane_type",
            "origin_sprint_id",
            "origin_bug_id",
            "test_scenario_ids",
            "business_rule_ids",
        ),
        PolicyEntityType.TEST_SCENARIO: (
            "title",
            "linked_criteria",
            "scenario_type",
            "given",
            "when",
            "then",
            "notes",
        ),
    }
)

SEMANTIC_POLICY_QA_FIELDS_V1: Final[frozenset[str]] = frozenset(
    {
        "id",
        "revision",
        "question",
        "question_type",
        "choices",
        "allow_free_text",
        "answer",
        "selected",
        "lifecycle",
        "tombstoned",
    }
)
SEMANTIC_POLICY_RESOURCE_REQUIRED_FIELDS_V1: Final[frozenset[str]] = frozenset(
    {
        "resource_type",
        "resource_id",
        "resource_version",
        "content_digest",
    }
)
SEMANTIC_POLICY_RESOURCE_OPTIONAL_FIELDS_V1: Final[frozenset[str]] = frozenset(
    {"source_ref"}
)
SEMANTIC_POLICY_RESOURCE_TYPES_V1: Final[frozenset[str]] = frozenset(
    {"knowledge", "architecture", "mockup"}
)

# Only the root object of an authored structured child is filtered.  A nested
# API schema property literally named ``status`` remains authorial content.
_VOLATILE_STRUCTURED_CHILD_KEYS_V1: Final[frozenset[str]] = frozenset(
    {
        "archived",
        "assignee_id",
        "assigned_to",
        "author_id",
        "created_at",
        "created_by",
        "evidence",
        "execution_attestation",
        "execution_receipt",
        "labels",
        "latest_evidence",
        "linked_task_ids",
        "pre_archive_status",
        "status",
        "updated_at",
        "updated_by",
    }
)

_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")


class SemanticPolicySubjectSnapshotError(CanonicalizationError):
    """A semantic policy subject cannot satisfy the closed v1 contract."""


def _error(code: str) -> SemanticPolicySubjectSnapshotError:
    return SemanticPolicySubjectSnapshotError(code)


def _sequence(value: object, code: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(
        value, str | bytes | bytearray
    ):
        raise _error(code)
    return value


def _normalized_text(
    value: object,
    code: str,
    *,
    strip: bool,
    optional: bool = False,
) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str):
        raise _error(code)
    normalized = normalize_canonical_value(value)
    if not isinstance(normalized, str):  # defensive: normalization is closed
        raise _error(code)
    if strip:
        normalized = normalized.strip()
    if not normalized:
        raise _error(code)
    return normalized


def _positive_int(value: object, code: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise _error(code)
    return value


def _closed_keys(
    value: object,
    *,
    required: frozenset[str],
    optional: frozenset[str] = frozenset(),
    code: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _error(f"{code}_invalid")
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in value.items():
        if not isinstance(raw_key, str):
            raise _error(f"{code}_key_invalid")
        key = normalize_canonical_value(raw_key)
        if not isinstance(key, str):  # defensive: normalization is closed
            raise _error(f"{code}_key_invalid")
        if key in normalized:
            raise _error(f"{code}_key_duplicate")
        normalized[key] = raw_value
    actual = frozenset(normalized)
    if missing := required - actual:
        raise _error(f"{code}_field_required:{sorted(missing)[0]}")
    if extras := actual - required - optional:
        raise _error(f"{code}_field_unsupported:{sorted(extras)[0]}")
    return normalized


def _is_volatile_child_key(key: str) -> bool:
    return (
        key in _VOLATILE_STRUCTURED_CHILD_KEYS_V1
        or key.startswith("skip_")
        or key.endswith("_actor_id")
        or key.endswith("_timestamp")
    )


def _artifact_value(
    value: object,
    *,
    strip_child_metadata: bool = False,
) -> Any:
    if isinstance(value, Mapping):
        projected: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            if not isinstance(raw_key, str):
                raise _error("semantic_policy_artifact_child_key_invalid")
            key = normalize_canonical_value(raw_key)
            if not isinstance(key, str):
                raise _error("semantic_policy_artifact_child_key_invalid")
            if strip_child_metadata and _is_volatile_child_key(key):
                continue
            if key in projected:
                raise _error("semantic_policy_artifact_child_key_duplicate")
            projected[key] = _artifact_value(raw_value)
        return projected
    if isinstance(value, Sequence) and not isinstance(
        value, str | bytes | bytearray
    ):
        return [
            _artifact_value(
                item,
                strip_child_metadata=isinstance(item, Mapping),
            )
            for item in value
        ]
    try:
        return normalize_canonical_value(value)
    except CanonicalizationError as exc:
        raise _error(f"semantic_policy_artifact_value_invalid:{exc}") from exc


def _project_artifact(
    subject_type: PolicyEntityType,
    artifact: Mapping[str, Any],
) -> dict[str, Any]:
    fields = SEMANTIC_POLICY_ARTIFACT_FIELD_MANIFEST_V1[subject_type]
    return {
        field: _artifact_value(
            artifact.get(field),
            strip_child_metadata=isinstance(artifact.get(field), Mapping),
        )
        for field in fields
    }


def _project_q_and_a(
    q_and_a: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    projected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for raw_item in _sequence(
        q_and_a,
        "semantic_policy_q_and_a_invalid",
    ):
        item = _closed_keys(
            raw_item,
            required=SEMANTIC_POLICY_QA_FIELDS_V1,
            code="semantic_policy_qa",
        )
        item_id = _normalized_text(
            item["id"],
            "semantic_policy_qa_id_invalid",
            strip=True,
        )
        assert item_id is not None
        if item_id in seen_ids:
            raise _error("semantic_policy_qa_id_duplicate")
        seen_ids.add(item_id)

        revision = _positive_int(
            item["revision"],
            "semantic_policy_qa_revision_invalid",
        )
        question = _normalized_text(
            item["question"],
            "semantic_policy_qa_question_invalid",
            strip=False,
        )
        question_type = _normalized_text(
            item["question_type"],
            "semantic_policy_qa_question_type_invalid",
            strip=True,
        )
        lifecycle = _normalized_text(
            item["lifecycle"],
            "semantic_policy_qa_lifecycle_invalid",
            strip=True,
        )
        if not isinstance(item["allow_free_text"], bool):
            raise _error("semantic_policy_qa_allow_free_text_invalid")
        if not isinstance(item["tombstoned"], bool):
            raise _error("semantic_policy_qa_tombstoned_invalid")
        choices = [
            normalize_canonical_value(value)
            for value in _sequence(
                item["choices"],
                "semantic_policy_qa_choices_invalid",
            )
        ]
        selected = [
            normalize_canonical_value(value)
            for value in _sequence(
                item["selected"],
                "semantic_policy_qa_selected_invalid",
            )
        ]
        try:
            answer = normalize_canonical_value(item["answer"])
        except CanonicalizationError as exc:
            raise _error("semantic_policy_qa_answer_invalid") from exc
        projected.append(
            {
                "id": item_id,
                "revision": revision,
                "question": question,
                "question_type": question_type,
                "choices": choices,
                "allow_free_text": item["allow_free_text"],
                "answer": answer,
                "selected": selected,
                "lifecycle": lifecycle,
                "tombstoned": item["tombstoned"],
            }
        )
    projected.sort(key=lambda item: item["id"])
    return projected


def _project_resource_refs(
    resource_refs: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    projected: list[dict[str, Any]] = []
    seen_identities: set[tuple[str, str, int]] = set()
    for raw_item in _sequence(
        resource_refs,
        "semantic_policy_resource_refs_invalid",
    ):
        item = _closed_keys(
            raw_item,
            required=SEMANTIC_POLICY_RESOURCE_REQUIRED_FIELDS_V1,
            optional=SEMANTIC_POLICY_RESOURCE_OPTIONAL_FIELDS_V1,
            code="semantic_policy_resource_ref",
        )
        resource_type = _normalized_text(
            item["resource_type"],
            "semantic_policy_resource_type_invalid",
            strip=True,
        )
        assert resource_type is not None
        if resource_type not in SEMANTIC_POLICY_RESOURCE_TYPES_V1:
            raise _error("semantic_policy_resource_type_invalid")
        resource_id = _normalized_text(
            item["resource_id"],
            "semantic_policy_resource_id_invalid",
            strip=True,
        )
        assert resource_id is not None
        resource_version = _positive_int(
            item["resource_version"],
            "semantic_policy_resource_version_invalid",
        )
        content_digest = _normalized_text(
            item["content_digest"],
            "semantic_policy_resource_content_digest_invalid",
            strip=True,
        )
        assert content_digest is not None
        content_digest = content_digest.lower()
        if not _SHA256_RE.fullmatch(content_digest):
            raise _error("semantic_policy_resource_content_digest_invalid")
        source_ref = _normalized_text(
            item.get("source_ref"),
            "semantic_policy_resource_source_ref_invalid",
            strip=True,
            optional=True,
        )
        identity = (resource_type, resource_id, resource_version)
        if identity in seen_identities:
            raise _error("semantic_policy_resource_ref_duplicate")
        seen_identities.add(identity)
        projected.append(
            {
                "resource_type": resource_type,
                "resource_id": resource_id,
                "resource_version": resource_version,
                "content_digest": content_digest,
                "source_ref": source_ref,
            }
        )
    projected.sort(
        key=lambda item: (
            item["resource_type"],
            item["resource_id"],
            item["resource_version"],
        )
    )
    return projected


def semantic_policy_subject_snapshot_v1(
    *,
    subject_type: PolicyEntityType,
    artifact: Mapping[str, Any],
    q_and_a: Sequence[Mapping[str, Any]] = (),
    resource_refs: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Return the complete canonical cognitive input for one policy subject."""

    if not isinstance(subject_type, PolicyEntityType):
        raise _error("semantic_policy_subject_type_invalid")
    if not isinstance(artifact, Mapping):
        raise _error("semantic_policy_artifact_invalid")
    return {
        "contract": SEMANTIC_POLICY_SUBJECT_SNAPSHOT_VERSION,
        "subject_type": subject_type.value,
        "artifact": _project_artifact(subject_type, artifact),
        "q_and_a": _project_q_and_a(q_and_a),
        "resource_refs": _project_resource_refs(resource_refs),
    }


def semantic_policy_subject_content_digest_v1(
    *,
    subject_type: PolicyEntityType,
    artifact: Mapping[str, Any],
    q_and_a: Sequence[Mapping[str, Any]] = (),
    resource_refs: Sequence[Mapping[str, Any]] = (),
) -> str:
    """Hash one exact semantic policy subject snapshot using canonical JSON."""

    return canonical_sha256(
        semantic_policy_subject_snapshot_v1(
            subject_type=subject_type,
            artifact=artifact,
            q_and_a=q_and_a,
            resource_refs=resource_refs,
        )
    )


__all__ = [
    "SEMANTIC_POLICY_ARTIFACT_FIELD_MANIFEST_V1",
    "SEMANTIC_POLICY_QA_FIELDS_V1",
    "SEMANTIC_POLICY_RESOURCE_OPTIONAL_FIELDS_V1",
    "SEMANTIC_POLICY_RESOURCE_REQUIRED_FIELDS_V1",
    "SEMANTIC_POLICY_RESOURCE_TYPES_V1",
    "SEMANTIC_POLICY_SUBJECT_SNAPSHOT_VERSION",
    "SemanticPolicySubjectSnapshotError",
    "semantic_policy_subject_content_digest_v1",
    "semantic_policy_subject_snapshot_v1",
]
