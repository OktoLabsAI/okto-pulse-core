"""Canonical JSON and semantic snapshots for SK-A quality assessments.

This module is deliberately a pure leaf: it imports no service, adapter, ORM,
or transport type.  The exact byte representation is part of the persisted
assessment contract, so changes require a new canonicalization version.
"""

from __future__ import annotations

import hashlib
import json
import unicodedata
from collections.abc import Mapping, Sequence
from enum import Enum
from types import MappingProxyType
from typing import Any

CANONICAL_JSON_VERSION = "canonical-json/v1"
CLARIFICATION_SNAPSHOT_VERSION = "clarification-snapshot/v1"
NORMATIVE_MANIFEST_DIGEST_VERSION = "normative-manifest-digest/v1"

SEMANTIC_FIELD_MANIFEST_V1: Mapping[str, tuple[str, ...]] = MappingProxyType({
    "ideation": (
        "title",
        "description",
        "problem_statement",
        "proposed_approach",
    ),
    "refinement": (
        "title",
        "description",
        "in_scope",
        "out_of_scope",
        "analysis",
        "decisions",
    ),
    "spec": (
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
})

# These keys are lifecycle, ownership, execution, linkage, or derived-quality
# metadata even when they occur inside one of the semantic collections above.
# They must not make a receipt stale.  Stable child IDs and authored fields are
# deliberately preserved so anchors and actual requirement edits remain
# observable.
SEMANTIC_NESTED_EXCLUDED_KEYS_V1: frozenset[str] = frozenset(
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


class CanonicalizationError(ValueError):
    """A value cannot be represented by the canonical JSON v1 contract."""


def _normalize_text(value: str) -> str:
    return unicodedata.normalize("NFC", value.replace("\r\n", "\n").replace("\r", "\n"))


def normalize_canonical_value(value: Any) -> Any:
    """Return the JSON-compatible canonical form of ``value``.

    Object keys are normalized and sorted by the JSON encoder.  Sequence order
    is intentionally preserved because list order is semantic in Pulse
    artifacts.  Sets, bytes, non-finite floats, and non-string object keys are
    rejected rather than guessed.
    """

    if value is None or isinstance(value, bool | int):
        return value
    if isinstance(value, float):
        if value != value or value in {float("inf"), float("-inf")}:
            raise CanonicalizationError("non_finite_number")
        return value
    if isinstance(value, str):
        return _normalize_text(value)
    if isinstance(value, Enum):
        return normalize_canonical_value(value.value)
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            if not isinstance(raw_key, str):
                raise CanonicalizationError("object_key_must_be_string")
            key = _normalize_text(raw_key)
            if key in normalized:
                raise CanonicalizationError("normalized_object_key_collision")
            normalized[key] = normalize_canonical_value(raw_value)
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, bytes | bytearray):
        return [normalize_canonical_value(item) for item in value]
    raise CanonicalizationError(
        f"unsupported_canonical_value:{type(value).__name__}"
    )


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize ``value`` as canonical UTF-8 JSON v1."""

    normalized = normalize_canonical_value(value)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    """Return the lowercase SHA-256 of canonical JSON v1 bytes."""

    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _is_non_semantic_nested_key(key: str) -> bool:
    return (
        key in SEMANTIC_NESTED_EXCLUDED_KEYS_V1
        or key.startswith("skip_")
        or key.endswith("_actor_id")
        or key.endswith("_timestamp")
    )


def _semantic_value_v1(
    value: Any,
    *,
    strip_child_metadata: bool = False,
) -> Any:
    """Normalize an authored value with path-aware child metadata removal.

    Metadata names are stripped only from the root object of a structured
    entity in a top-level semantic collection.  Nested authored schemas remain
    opaque: for example an API response property literally named ``status`` is
    contractual content and must affect the digest.
    """
    if isinstance(value, Mapping):
        projected: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            if not isinstance(raw_key, str):
                raise CanonicalizationError("object_key_must_be_string")
            key = _normalize_text(raw_key)
            if strip_child_metadata and _is_non_semantic_nested_key(key):
                continue
            if key in projected:
                raise CanonicalizationError("normalized_object_key_collision")
            projected[key] = _semantic_value_v1(raw_value)
        return projected
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_semantic_value_v1(item) for item in value]
    return normalize_canonical_value(value)


def _semantic_field_value_v1(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _semantic_value_v1(value, strip_child_metadata=True)
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [
            _semantic_value_v1(
                item,
                strip_child_metadata=isinstance(item, Mapping),
            )
            for item in value
        ]
    return _semantic_value_v1(value)


def semantic_snapshot_v1(subject_type: str | Enum, payload: Mapping[str, Any]) -> dict[str, Any]:
    """Project only the normative semantic fields for one subject.

    Missing fields are represented as ``None``.  This makes an absent field
    distinguishable from an empty collection while excluding status, actors,
    timestamps, labels, skips, quality outputs, architecture, and mockup HTML.
    """

    raw_subject_type = (
        subject_type.value if isinstance(subject_type, Enum) else str(subject_type)
    )
    fields = SEMANTIC_FIELD_MANIFEST_V1.get(raw_subject_type)
    if fields is None:
        raise CanonicalizationError(f"unsupported_subject_type:{raw_subject_type}")
    return {
        field: _semantic_field_value_v1(payload.get(field))
        for field in fields
    }


def semantic_content_digest_v1(
    subject_type: str | Enum,
    payload: Mapping[str, Any],
) -> str:
    """Hash the canonical semantic snapshot for ``subject_type``."""

    return canonical_sha256(semantic_snapshot_v1(subject_type, payload))


def clarification_snapshot_v1(
    qa_items: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Project answered clarifications only, independent of adapter metadata.

    Newly proposed/unanswered questions are deliberately excluded so an
    assessment that atomically materializes proposals does not make itself
    stale. Once answered, stable identity, revision, authored question/answer,
    choice contract, normalized answer selection and lifecycle become semantic.
    ``answered_at`` is used only as an answered marker; actor/timestamp metadata
    is excluded and entries are sorted by stable QA ID.

    Persisted adapters historically expose the answer selection as
    ``selected``, while boundary payloads may call it ``selected_choice`` or
    ``selected_choices``. The aliases collapse to one canonical ``selected``
    field and conflicting aliases fail closed. Multi-choice selection order is
    non-semantic and is therefore sorted; authored choice presentation order
    remains semantic.
    """

    if not isinstance(qa_items, Sequence) or isinstance(
        qa_items,
        str | bytes | bytearray,
    ):
        raise CanonicalizationError("clarification_items_invalid")
    projected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for item in qa_items:
        if not isinstance(item, Mapping):
            raise CanonicalizationError("clarification_item_invalid")
        raw_id = item.get("id", item.get("qa_id"))
        if not isinstance(raw_id, str) or not raw_id.strip():
            raise CanonicalizationError("clarification_id_required")
        qa_id = _normalize_text(raw_id.strip())
        if qa_id in seen_ids:
            raise CanonicalizationError("clarification_id_duplicate")
        seen_ids.add(qa_id)
        answer = item.get("answer")
        question_type = item.get("question_type", "text")
        selection_aliases: list[tuple[str, Any]] = [
            (key, item.get(key))
            for key in ("selected", "selected_choices", "selected_choice")
            if item.get(key) is not None
        ]
        normalized_selections: list[tuple[str, list[Any]]] = []
        for key, raw_selection in selection_aliases:
            if isinstance(raw_selection, Sequence) and not isinstance(
                raw_selection,
                str | bytes | bytearray,
            ):
                selection = [
                    _semantic_value_v1(value) for value in raw_selection
                ]
            else:
                selection = [_semantic_value_v1(raw_selection)]
            if question_type == "multi_choice":
                selection = sorted(
                    selection,
                    key=lambda value: canonical_json_bytes(value),
                )
            normalized_selections.append((key, selection))
        if normalized_selections:
            first_selection = normalized_selections[0][1]
            if any(
                selection != first_selection
                for _, selection in normalized_selections[1:]
            ):
                raise CanonicalizationError(
                    "clarification_selection_alias_conflict"
                )
            selected = first_selection
        else:
            selected = []

        tombstoned = item.get("tombstoned", False)
        if not isinstance(tombstoned, bool):
            raise CanonicalizationError("clarification_tombstone_invalid")
        answered_at = item.get("answered_at")
        is_answered = (
            answered_at is not None
            or answer is not None
            or bool(selected)
            or tombstoned
        )
        if not is_answered:
            continue
        revision = item.get("revision", 1)
        if (
            not isinstance(revision, int)
            or isinstance(revision, bool)
            or revision < 1
        ):
            raise CanonicalizationError("clarification_revision_invalid")
        lifecycle = item.get(
            "lifecycle",
            "tombstoned" if tombstoned else "active",
        )
        projected.append(
            {
                "qa_id": qa_id,
                "revision": revision,
                "question": _semantic_value_v1(item.get("question")),
                "question_type": _semantic_value_v1(question_type),
                "choices": _semantic_value_v1(item.get("choices") or []),
                "allow_free_text": _semantic_value_v1(
                    item.get("allow_free_text", True)
                ),
                "answer": _semantic_value_v1(answer),
                "selected": selected,
                "lifecycle": _semantic_value_v1(lifecycle),
                "tombstoned": tombstoned,
            }
        )
    projected.sort(key=lambda item: item["qa_id"])
    return {
        "version": CLARIFICATION_SNAPSHOT_VERSION,
        "answered": projected,
    }


def clarification_digest_v1(
    qa_items: Sequence[Mapping[str, Any]],
) -> str:
    return canonical_sha256(clarification_snapshot_v1(qa_items))


def normative_manifest_digest_v1(
    *,
    namespace: str,
    version: str,
    manifest: Mapping[str, Any] | Sequence[Any],
) -> str:
    """Hash a named/versioned ruleset, taxonomy, policy, or authority manifest."""

    if not isinstance(namespace, str) or not namespace.strip():
        raise CanonicalizationError("normative_manifest_namespace_required")
    if not isinstance(version, str) or not version.strip():
        raise CanonicalizationError("normative_manifest_version_required")
    if not isinstance(manifest, Mapping | Sequence) or isinstance(
        manifest,
        str | bytes | bytearray,
    ):
        raise CanonicalizationError("normative_manifest_invalid")
    return canonical_sha256(
        {
            "digest_contract": NORMATIVE_MANIFEST_DIGEST_VERSION,
            "namespace": namespace.strip(),
            "version": version.strip(),
            "manifest": manifest,
        }
    )


def ruleset_digest_v1(version: str, manifest: Mapping[str, Any] | Sequence[Any]) -> str:
    return normative_manifest_digest_v1(
        namespace="ruleset",
        version=version,
        manifest=manifest,
    )


def taxonomy_digest_v1(version: str, manifest: Mapping[str, Any] | Sequence[Any]) -> str:
    return normative_manifest_digest_v1(
        namespace="taxonomy",
        version=version,
        manifest=manifest,
    )


def policy_digest_v1(version: str, manifest: Mapping[str, Any] | Sequence[Any]) -> str:
    return normative_manifest_digest_v1(
        namespace="calculation_policy",
        version=version,
        manifest=manifest,
    )


def authority_digest_v1(
    version: str,
    manifest: Mapping[str, Any] | Sequence[Any],
) -> str:
    return normative_manifest_digest_v1(
        namespace="authority",
        version=version,
        manifest=manifest,
    )


def assessment_input_digest_v1(
    *,
    content_digest: str,
    clarification_digest: str,
    ruleset_digest: str,
    taxonomy_digest: str,
    policy_digest: str,
) -> str:
    """Hash the five calculation identities that define an assessment input."""

    return canonical_sha256(
        {
            "canonicalization_version": CANONICAL_JSON_VERSION,
            "content_digest": content_digest,
            "clarification_digest": clarification_digest,
            "ruleset_digest": ruleset_digest,
            "taxonomy_digest": taxonomy_digest,
            "policy_digest": policy_digest,
        }
    )


__all__ = [
    "CANONICAL_JSON_VERSION",
    "CLARIFICATION_SNAPSHOT_VERSION",
    "NORMATIVE_MANIFEST_DIGEST_VERSION",
    "SEMANTIC_FIELD_MANIFEST_V1",
    "SEMANTIC_NESTED_EXCLUDED_KEYS_V1",
    "CanonicalizationError",
    "assessment_input_digest_v1",
    "authority_digest_v1",
    "canonical_json_bytes",
    "canonical_sha256",
    "clarification_digest_v1",
    "clarification_snapshot_v1",
    "normative_manifest_digest_v1",
    "policy_digest_v1",
    "ruleset_digest_v1",
    "normalize_canonical_value",
    "semantic_content_digest_v1",
    "semantic_snapshot_v1",
    "taxonomy_digest_v1",
]
