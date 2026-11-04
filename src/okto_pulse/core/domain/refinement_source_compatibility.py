"""Bounded, explicit proof that a lifecycle move preserved source context.

This proof only connects adjacent semantic versions. It never changes a receipt
or substitutes for source-head, expiry, trust, revocation or execution checks.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

CONTRACT = "refinement-source-compatibility/v1"
FIELD = "source_context_compatibility"
MAX_TRANSITIONS = 50
FORWARD_MOVES = frozenset(
    {("draft", "review"), ("review", "approved"), ("approved", "done")}
)
_CONTENT_FIELDS = (
    "id",
    "board_id",
    "ideation_id",
    "edition",
    "title",
    "description",
    "in_scope",
    "out_of_scope",
    "analysis",
    "decisions",
    "labels",
    "screen_mockups",
    "delivery_context",
)


def _field(value: object, name: str, default=None):
    return (
        value.get(name, default)
        if isinstance(value, Mapping)
        else getattr(value, name, default)
    )


def _status(value: object) -> str:
    return str(getattr(value, "value", value))


def is_legacy_approved_to_done_history(rows: Sequence[object]) -> bool:
    """Caller supplies at most two rows at the exact subject/snapshot version."""
    changes = _field(rows[0], "changes") if len(rows) == 1 else None
    return bool(
        isinstance(changes, list)
        and len(changes) == 1
        and isinstance(changes[0], dict)
        and changes[0].get("field") == "status"
        and changes[0].get("old") == "approved"
        and changes[0].get("new") == "done"
    )


def refinement_source_digest(subject: object) -> str:
    from okto_pulse.core.domain.code_traceability import (
        canonical_code_traceability_sha256,
    )

    # Child Q&A, knowledge and architecture writes independently advance the
    # subject version. The contiguous history chain below cannot cross a child
    # edit, even if all root fields happen to be unchanged.
    return canonical_code_traceability_sha256(
        {
            "contract": CONTRACT,
            "content": {name: _field(subject, name) for name in _CONTENT_FIELDS},
        }
    )


def compatibility_change(
    *, subject: object, before_version: int, before_status: object, before_digest: str
) -> dict | None:
    after_version = _field(subject, "version")
    move = (_status(before_status), _status(_field(subject, "status")))
    after_digest = refinement_source_digest(subject)
    if (
        type(before_version) is not int
        or type(after_version) is not int
        or after_version != before_version + 1
        or move not in FORWARD_MOVES
        or before_digest != after_digest
    ):
        return None
    return {
        "field": FIELD,
        "old": None,
        "new": {
            "contract_version": CONTRACT,
            "board_id": _field(subject, "board_id"),
            "refinement_id": _field(subject, "id"),
            "edition": _field(subject, "edition"),
            "before_version": before_version,
            "after_version": after_version,
            "from_status": move[0],
            "to_status": move[1],
            "before_sha256": before_digest,
            "after_sha256": after_digest,
        },
    }


@dataclass(frozen=True)
class RefinementSourceCompatibility:
    current_version: int
    semantic_sha256: str
    compatible_versions: tuple[int, ...]
    history_ids: tuple[str, ...]

    def __post_init__(self):
        versions = self.compatible_versions
        if (
            type(self.current_version) is not int
            or self.current_version < 1
            or not versions
            or versions[0] != self.current_version
            or len(versions) > MAX_TRANSITIONS + 1
            or any(type(v) is not int or v < 1 for v in versions)
            or any(a - b != 1 for a, b in zip(versions, versions[1:]))
            or len(self.history_ids) != len(versions) - 1
            or len(set(self.history_ids)) != len(self.history_ids)
            or any(not isinstance(v, str) or not v for v in self.history_ids)
            or not isinstance(self.semantic_sha256, str)
            or len(self.semantic_sha256) != 64
            or any(c not in "0123456789abcdef" for c in self.semantic_sha256)
        ):
            raise ValueError("refinement_source_compatibility_invalid")


@dataclass(frozen=True)
class RefinementSourceSnapshotBasis:
    snapshot_id: str
    snapshot_version: int
    manifest_sha256: str
    kind: str = "historical_snapshot"

    def __post_init__(self):
        if (
            not isinstance(self.snapshot_id, str)
            or not self.snapshot_id
            or type(self.snapshot_version) is not int
            or self.snapshot_version < 1
            or self.kind != "historical_snapshot"
            or not isinstance(self.manifest_sha256, str)
            or len(self.manifest_sha256) != 64
            or any(c not in "0123456789abcdef" for c in self.manifest_sha256)
        ):
            raise ValueError("refinement_source_snapshot_basis_invalid")


def resolve_source_compatibility(
    subject: object, history: Sequence[object]
) -> RefinementSourceCompatibility | None:
    """Resolve only a contiguous, unambiguous chain of recorded equal digests.

    Legacy status history without the proof is not upgraded. A missing version,
    extra content change, duplicate edge or changed digest terminates the chain.
    """
    current = _field(subject, "version")
    digest = refinement_source_digest(subject)
    version, status = current, _status(_field(subject, "status"))
    versions, ids = [current], []
    for _ in range(MAX_TRANSITIONS):
        rows = []
        for row in history:
            if _field(row, "version") != version:
                continue
            # Native assessment admission writes a projection-only audit at
            # the assessed version before the operational Approved bump. It
            # neither edits source content nor represents another status edge.
            if _field(row, "action") == "quality_assessment_lifecycle" and _field(
                row, "changes"
            ) == [
                {
                    "field": "quality_assessment_projection",
                    "old": "review",
                    "new": "approved",
                }
            ]:
                continue
            rows.append(row)
        if len(rows) != 1:
            break
        row = rows[0]
        changes = _field(row, "changes")
        if (
            _field(row, "refinement_id") != _field(subject, "id")
            or _field(row, "action") != "status_changed"
            or not isinstance(changes, list)
            or len(changes) != 2
            or any(not isinstance(item, dict) for item in changes)
        ):
            break
        fields = {item.get("field"): item for item in changes}
        proof = fields.get(FIELD, {}).get("new")
        transition = fields.get("status", {})
        if not isinstance(proof, dict):
            break
        if (
            proof.get("contract_version") != CONTRACT
            or proof.get("board_id") != _field(subject, "board_id")
            or proof.get("refinement_id") != _field(subject, "id")
            or proof.get("edition") != _field(subject, "edition")
            or type(proof.get("before_version")) is not int
            or type(proof.get("after_version")) is not int
            or proof["before_version"] != version - 1
            or version <= 1
            or proof["after_version"] != version
            or proof.get("to_status") != status
            or (proof.get("from_status"), status) not in FORWARD_MOVES
            or transition.get("old") != proof.get("from_status")
            or transition.get("new") != status
            or proof.get("before_sha256") != digest
            or proof.get("after_sha256") != digest
            or not isinstance(_field(row, "id"), str)
            or not _field(row, "id")
        ):
            break
        version, status = version - 1, proof["from_status"]
        versions.append(version)
        ids.append(_field(row, "id"))
    if not ids:
        return None
    return RefinementSourceCompatibility(current, digest, tuple(versions), tuple(ids))
