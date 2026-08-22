"""Human-governed append-only context overlays for legacy Code Evidence."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

from okto_pulse.core.domain.code_traceability import (
    CodeEvidenceBaselinePresence,
    CodeEvidenceLegacyClassification,
    CodeEvidenceLegacyClassificationBatchReceipt,
    CodeEvidenceLegacyClassificationEvidenceNotFound,
    CodeEvidenceLegacyClassificationIdempotencyConflict,
    CodeEvidenceLegacyClassificationLegacyRequired,
    CodeEvidenceLegacyClassificationPayloadConflict,
    CodeEvidenceLegacyClassificationPersistenceConflict,
    CodeEvidenceLegacyClassificationRevisionConflict,
    CodeEvidenceSourceRole,
    canonical_code_traceability_sha256,
)
from okto_pulse.core.models.code_traceability import (
    LegacyEvidenceClassificationBatchInput,
)
from okto_pulse.core.ports.code_traceability import (
    CodeTraceabilityStore,
    LegacyEvidenceClassificationIdempotencyConflict as StoreIdempotencyConflict,
    LegacyEvidenceClassificationPersistenceConflict as StorePersistenceConflict,
    LegacyEvidenceClassificationRevisionConflict as StoreRevisionConflict,
)


Clock = Callable[[], datetime]
IdFactory = Callable[[str], str]


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


def _default_id_factory(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise CodeEvidenceLegacyClassificationPersistenceConflict(
            details={"reason": "clock_invalid"}
        )
    return value.astimezone(timezone.utc)


class LegacyCodeEvidenceClassificationService:
    """Build and atomically append explicit authorized classifications."""

    def __init__(
        self,
        *,
        clock: Clock = _default_clock,
        id_factory: IdFactory = _default_id_factory,
    ) -> None:
        self._clock = clock
        self._id_factory = id_factory

    @staticmethod
    def _request_sha256(
        command: LegacyEvidenceClassificationBatchInput,
        *,
        actor_id: str,
    ) -> str:
        return canonical_code_traceability_sha256(
            {
                "operation": "classify_legacy_code_evidence_batch",
                "contract_version": 1,
                "actor_id": actor_id,
                "board_id": command.board_id,
                "items": [
                    item.model_dump(mode="python") for item in command.items
                ],
                "justification": command.justification,
                "idempotency_key": command.idempotency_key,
            }
        )

    async def classify(
        self,
        command: LegacyEvidenceClassificationBatchInput,
        *,
        actor_id: str,
        store: CodeTraceabilityStore,
    ) -> CodeEvidenceLegacyClassificationBatchReceipt:
        request_sha256 = self._request_sha256(command, actor_id=actor_id)
        try:
            replay = await store.resolve_legacy_classification_batch_replay(
                board_id=command.board_id,
                classified_by=actor_id,
                idempotency_key=command.idempotency_key,
            )
        except StoreIdempotencyConflict as exc:
            raise CodeEvidenceLegacyClassificationIdempotencyConflict() from exc
        except StorePersistenceConflict as exc:
            raise CodeEvidenceLegacyClassificationPersistenceConflict() from exc
        if replay is not None:
            if (
                not isinstance(
                    replay,
                    CodeEvidenceLegacyClassificationBatchReceipt,
                )
                or replay.request_sha256 != request_sha256
            ):
                raise CodeEvidenceLegacyClassificationIdempotencyConflict()
            replay_items = {
                item.evidence_id: item for item in replay.classifications
            }
            if (
                replay.board_id != command.board_id
                or replay.classified_by != actor_id
                or replay.idempotency_key != command.idempotency_key
                or len(replay_items) != len(command.items)
                or any(
                    (
                        (persisted := replay_items.get(item.evidence_id)) is None
                        or persisted.evidence_payload_sha256
                        != item.expected_evidence_payload_sha256
                        or persisted.revision
                        != item.expected_classification_revision + 1
                        or persisted.source_role is not item.source_role
                        or persisted.relevance_summary != item.relevance_summary
                        or persisted.scope_relation != item.scope_relation
                        or persisted.source_origin != item.source_origin
                        or persisted.interpretation_limit
                        != item.interpretation_limit
                        or persisted.baseline_provenance
                        != item.baseline_provenance
                        or persisted.justification != command.justification
                    )
                    for item in command.items
                )
            ):
                raise CodeEvidenceLegacyClassificationPersistenceConflict(
                    details={"reason": "classification_replay_incoherent"}
                )
            return replay if replay.replayed else replace(replay, replayed=True)

        evidence_by_id = {}
        for item in command.items:
            evidence = await store.get_evidence(
                board_id=command.board_id,
                evidence_id=item.evidence_id,
            )
            if evidence is None:
                raise CodeEvidenceLegacyClassificationEvidenceNotFound(
                    details={"evidence_id": item.evidence_id}
                )
            if evidence.source_role is not CodeEvidenceSourceRole.UNCATEGORIZED_LEGACY:
                raise CodeEvidenceLegacyClassificationLegacyRequired(
                    details={"evidence_id": item.evidence_id}
                )
            if evidence.payload_sha256 != item.expected_evidence_payload_sha256:
                raise CodeEvidenceLegacyClassificationPayloadConflict(
                    details={"evidence_id": item.evidence_id}
                )
            baseline = item.baseline_provenance
            workspace = evidence.workspace_state
            baseline_is_worktree = (
                baseline.presence
                is CodeEvidenceBaselinePresence.PREEXISTING_WORKTREE
            )
            if (
                baseline.workspace_state_id != workspace.workspace_state_id
                or baseline_is_worktree is not workspace.declared_dirty
            ):
                raise CodeEvidenceLegacyClassificationPayloadConflict(
                    details={
                        "evidence_id": item.evidence_id,
                        "field": "baseline_provenance",
                    }
                )
            evidence_by_id[item.evidence_id] = evidence

        try:
            current_items = await store.list_latest_evidence_classifications(
                board_id=command.board_id,
                evidence_ids=tuple(item.evidence_id for item in command.items),
            )
        except StorePersistenceConflict as exc:
            raise CodeEvidenceLegacyClassificationPersistenceConflict() from exc
        current_by_id = {}
        for current in current_items:
            if (
                not isinstance(current, CodeEvidenceLegacyClassification)
                or current.board_id != command.board_id
                or current.evidence_id not in evidence_by_id
                or current.evidence_id in current_by_id
                or current.evidence_payload_sha256
                != evidence_by_id[current.evidence_id].payload_sha256
            ):
                raise CodeEvidenceLegacyClassificationPersistenceConflict(
                    details={"reason": "classification_head_invalid"}
                )
            current_by_id[current.evidence_id] = current

        expected_revisions: dict[str, int] = {}
        for item in command.items:
            current = current_by_id.get(item.evidence_id)
            current_revision = 0 if current is None else current.revision
            if current_revision != item.expected_classification_revision:
                raise CodeEvidenceLegacyClassificationRevisionConflict(
                    details={
                        "evidence_id": item.evidence_id,
                        "expected_revision": item.expected_classification_revision,
                        "current_revision": current_revision,
                    }
                )
            expected_revisions[item.evidence_id] = current_revision

        classified_at = _aware_utc(self._clock())
        batch_id = self._id_factory("code_evidence_classification_batch")
        item_count = len(command.items)
        classifications = tuple(
            CodeEvidenceLegacyClassification(
                id=self._id_factory("code_evidence_classification"),
                batch_id=batch_id,
                board_id=command.board_id,
                evidence_id=item.evidence_id,
                evidence_payload_sha256=item.expected_evidence_payload_sha256,
                revision=item.expected_classification_revision + 1,
                predecessor_classification_id=(
                    None
                    if current_by_id.get(item.evidence_id) is None
                    else current_by_id[item.evidence_id].id
                ),
                source_role=item.source_role,
                relevance_summary=item.relevance_summary,
                scope_relation=item.scope_relation,
                source_origin=item.source_origin,
                interpretation_limit=item.interpretation_limit,
                baseline_provenance=item.baseline_provenance,
                classified_by=actor_id,
                classified_at=classified_at,
                justification=command.justification,
                idempotency_key=command.idempotency_key,
                request_sha256=request_sha256,
                batch_item_count=item_count,
                batch_item_index=index,
            )
            for index, item in enumerate(command.items, start=1)
        )
        receipt = CodeEvidenceLegacyClassificationBatchReceipt(
            batch_id=batch_id,
            board_id=command.board_id,
            classified_by=actor_id,
            classified_at=classified_at,
            idempotency_key=command.idempotency_key,
            request_sha256=request_sha256,
            classifications=classifications,
        )
        try:
            persisted = await store.append_legacy_evidence_classification_batch(
                receipt=receipt,
                expected_revisions=expected_revisions,
            )
        except StoreRevisionConflict as exc:
            raise CodeEvidenceLegacyClassificationRevisionConflict() from exc
        except StoreIdempotencyConflict as exc:
            raise CodeEvidenceLegacyClassificationIdempotencyConflict() from exc
        except StorePersistenceConflict as exc:
            raise CodeEvidenceLegacyClassificationPersistenceConflict() from exc
        if persisted != receipt:
            raise CodeEvidenceLegacyClassificationPersistenceConflict(
                details={"reason": "commit_result_mismatch"}
            )
        return persisted


__all__ = ["LegacyCodeEvidenceClassificationService"]
