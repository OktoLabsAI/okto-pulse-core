"""Deterministic Spec Evidence rebase planning over persisted Pulse records."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Any, Callable

from okto_pulse.core.domain.code_traceability import (
    CodeEvidence,
    CodeEvidenceLinkInvalid,
    CodeTraceabilityLifecycleStatus,
    canonical_code_traceability_sha256,
)
from okto_pulse.core.ports.code_traceability import CodeTraceabilityStore


Clock = Callable[[], datetime]
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _clock() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class SpecCodeEvidenceRebasePlan:
    board_id: str
    spec_id: str
    expected_spec_version: int
    resulting_spec_version: int
    current_refinement_snapshot_id: str
    current_refinement_version: int
    target_refinement_snapshot_id: str
    target_refinement_version: int
    added_evidence_ids: tuple[str, ...]
    removed_evidence_ids: tuple[str, ...]
    superseded_evidence_pairs: tuple[tuple[str, str], ...]
    stale_link_ids: tuple[str, ...]
    invalid_disposition_ids: tuple[str, ...]

    @property
    def preview_sha256(self) -> str:
        return canonical_code_traceability_sha256(self._digest_payload())

    def _digest_payload(self) -> dict[str, object]:
        return {
            "operation": "spec_code_evidence_rebase",
            "board_id": self.board_id,
            "spec_id": self.spec_id,
            "expected_spec_version": self.expected_spec_version,
            "resulting_spec_version": self.resulting_spec_version,
            "current_refinement_snapshot_id": self.current_refinement_snapshot_id,
            "current_refinement_version": self.current_refinement_version,
            "target_refinement_snapshot_id": self.target_refinement_snapshot_id,
            "target_refinement_version": self.target_refinement_version,
            "added_evidence_ids": self.added_evidence_ids,
            "removed_evidence_ids": self.removed_evidence_ids,
            "superseded_evidence_pairs": self.superseded_evidence_pairs,
            "stale_link_ids": self.stale_link_ids,
            "invalid_disposition_ids": self.invalid_disposition_ids,
        }

    def as_dict(self) -> dict[str, object]:
        return {
            **self._digest_payload(),
            "superseded_evidence": [
                {"predecessor_id": predecessor, "replacement_id": replacement}
                for predecessor, replacement in self.superseded_evidence_pairs
            ],
            "preview_sha256": self.preview_sha256,
        }


@dataclass(frozen=True, slots=True)
class SpecCodeEvidenceRebaseResult:
    plan: SpecCodeEvidenceRebasePlan
    spec_version: int

    def as_dict(self) -> dict[str, object]:
        return {**self.plan.as_dict(), "spec_version": self.spec_version}


class SpecCodeEvidenceRebaseService:
    def __init__(self, *, clock: Clock = _clock) -> None:
        self._clock = clock

    @staticmethod
    def _snapshot_manifest(snapshot: object) -> dict[str, str]:
        raw = getattr(snapshot, "code_evidence_manifest", None)
        if raw is None and isinstance(snapshot, Mapping):
            raw = snapshot.get("code_evidence_manifest")
        if isinstance(raw, str | bytes) or not isinstance(raw, Sequence):
            raise CodeEvidenceLinkInvalid(
                details={"reason": "refinement_snapshot_manifest_invalid"}
            )
        result: dict[str, str] = {}
        for entry in raw:
            if not isinstance(entry, Mapping):
                raise CodeEvidenceLinkInvalid(
                    details={"reason": "refinement_snapshot_manifest_invalid"}
                )
            evidence_id = entry.get("evidence_id")
            content_sha256 = entry.get("content_sha256")
            lifecycle = entry.get("lifecycle_status")
            if (
                not isinstance(evidence_id, str)
                or not evidence_id
                or evidence_id in result
                or not isinstance(content_sha256, str)
                or _SHA256_RE.fullmatch(content_sha256.casefold()) is None
                or lifecycle != CodeTraceabilityLifecycleStatus.ACTIVE.value
            ):
                raise CodeEvidenceLinkInvalid(
                    details={"reason": "refinement_snapshot_manifest_invalid"}
                )
            result[evidence_id] = content_sha256.casefold()
        return result

    @staticmethod
    def _field(value: object, name: str) -> Any:
        if isinstance(value, Mapping):
            return value.get(name)
        return getattr(value, name, None)

    async def preview(
        self,
        *,
        board_id: str,
        spec: object,
        current_snapshot: object,
        target_snapshot: object,
        target_refinement_version: int,
        expected_spec_version: int,
        store: CodeTraceabilityStore,
    ) -> SpecCodeEvidenceRebasePlan:
        spec_id = self._field(spec, "id")
        refinement_id = self._field(spec, "refinement_id")
        current_snapshot_id = self._field(spec, "source_refinement_snapshot_id")
        current_version = self._field(spec, "source_refinement_version")
        spec_version = self._field(spec, "version")
        if (
            not isinstance(spec_id, str)
            or not isinstance(refinement_id, str)
            or not isinstance(current_snapshot_id, str)
            or type(current_version) is not int
            or type(spec_version) is not int
            or spec_version != expected_spec_version
        ):
            raise CodeEvidenceLinkInvalid(details={"reason": "spec_rebase_scope_invalid"})
        if target_refinement_version <= current_version:
            raise CodeEvidenceLinkInvalid(
                details={"reason": "newer_refinement_snapshot_required"}
            )
        if (
            self._field(current_snapshot, "id") != current_snapshot_id
            or self._field(current_snapshot, "refinement_id") != refinement_id
            or self._field(current_snapshot, "version") != current_version
            or self._field(target_snapshot, "refinement_id") != refinement_id
            or self._field(target_snapshot, "version") != target_refinement_version
            or not isinstance(self._field(target_snapshot, "id"), str)
        ):
            raise CodeEvidenceLinkInvalid(
                details={"reason": "refinement_snapshot_scope_mismatch"}
            )
        old_manifest = self._snapshot_manifest(current_snapshot)
        new_manifest = self._snapshot_manifest(target_snapshot)
        evidence_by_id: dict[str, CodeEvidence] = {}
        for evidence_id, content_sha256 in sorted(new_manifest.items()):
            evidence = await store.get_evidence(
                board_id=board_id,
                evidence_id=evidence_id,
            )
            if (
                evidence is None
                or evidence.parent_id != refinement_id
                or evidence.parent_version != target_refinement_version
                or evidence.lifecycle_status
                is not CodeTraceabilityLifecycleStatus.ACTIVE
                or evidence.content_sha256 != content_sha256
            ):
                raise CodeEvidenceLinkInvalid(
                    details={
                        "reason": "target_snapshot_evidence_mismatch",
                        "evidence_id": evidence_id,
                    }
                )
            evidence_by_id[evidence_id] = evidence

        old_ids = set(old_manifest)
        new_ids = set(new_manifest)
        added = tuple(sorted(new_ids - old_ids))
        removed = tuple(sorted(old_ids - new_ids))
        removed_set = set(removed)
        superseded = tuple(
            sorted(
                (evidence.supersedes_evidence_id, evidence.id)
                for evidence in evidence_by_id.values()
                if evidence.supersedes_evidence_id in removed_set
            )
        )
        links = await store.list_spec_links(board_id=board_id, spec_id=spec_id)
        stale_link_ids = tuple(
            sorted(
                link.id
                for link in links
                if link.source_refinement_version == current_version
                and (
                    link.evidence_id not in new_manifest
                    or link.evidence_content_sha256
                    != new_manifest[link.evidence_id]
                )
            )
        )
        dispositions = await store.list_spec_dispositions(
            board_id=board_id,
            spec_id=spec_id,
            active_only=True,
        )
        invalid_disposition_ids = tuple(
            sorted(
                item.id
                for item in dispositions
                if item.evidence_id in removed_set
            )
        )
        return SpecCodeEvidenceRebasePlan(
            board_id=board_id,
            spec_id=spec_id,
            expected_spec_version=expected_spec_version,
            resulting_spec_version=expected_spec_version + 1,
            current_refinement_snapshot_id=current_snapshot_id,
            current_refinement_version=current_version,
            target_refinement_snapshot_id=self._field(target_snapshot, "id"),
            target_refinement_version=target_refinement_version,
            added_evidence_ids=added,
            removed_evidence_ids=removed,
            superseded_evidence_pairs=superseded,
            stale_link_ids=stale_link_ids,
            invalid_disposition_ids=invalid_disposition_ids,
        )

    async def apply(
        self,
        plan: SpecCodeEvidenceRebasePlan,
        *,
        expected_preview_sha256: str,
        actor_id: str,
        store: CodeTraceabilityStore,
    ) -> SpecCodeEvidenceRebaseResult:
        if plan.preview_sha256 != expected_preview_sha256.casefold():
            raise CodeEvidenceLinkInvalid(details={"reason": "rebase_preview_stale"})
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise CodeEvidenceLinkInvalid(details={"reason": "rebase_clock_invalid"})
        spec_version = await store.apply_spec_evidence_rebase(
            board_id=plan.board_id,
            spec_id=plan.spec_id,
            current_refinement_snapshot_id=plan.current_refinement_snapshot_id,
            current_refinement_version=plan.current_refinement_version,
            target_refinement_snapshot_id=plan.target_refinement_snapshot_id,
            target_refinement_version=plan.target_refinement_version,
            stale_link_ids=plan.stale_link_ids,
            invalid_disposition_ids=plan.invalid_disposition_ids,
            cleared_by=actor_id,
            cleared_at=now.astimezone(timezone.utc),
            expected_spec_version=plan.expected_spec_version,
            next_spec_version=plan.resulting_spec_version,
        )
        if spec_version != plan.resulting_spec_version:
            raise CodeEvidenceLinkInvalid(details={"reason": "spec_version_conflict"})
        return SpecCodeEvidenceRebaseResult(plan=plan, spec_version=spec_version)


__all__ = [
    "SpecCodeEvidenceRebasePlan",
    "SpecCodeEvidenceRebaseResult",
    "SpecCodeEvidenceRebaseService",
]
