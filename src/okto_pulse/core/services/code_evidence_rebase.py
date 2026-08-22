"""Deterministic Spec Evidence rebase planning over persisted Pulse records."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Any, Callable

from okto_pulse.core.domain.code_traceability import (
    CodeDeliveryContextRequired,
    CodeEvidence,
    CodeEvidenceLinkInvalid,
    CodeEvidenceSourceRole,
    CodeTraceabilityContractError,
    CodeTraceabilityLifecycleStatus,
    ContextualInvestigationOutcomeV2,
    DeliveryContext,
    RefinementSourceContextManifestV2,
    SourceContextClassificationFenceV2,
    SourceContextRoleCountsV2,
    SpecDeliveryContextProvenance,
    build_source_context_summary_v2,
    canonical_code_traceability_sha256,
    parse_refinement_source_context_manifest_v2,
    source_context_classification_fence_v2,
    source_context_evidence_item_v2,
    source_context_evidence_payload_v2,
)
from okto_pulse.core.ports.code_traceability import CodeTraceabilityStore


Clock = Callable[[], datetime]
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _clock() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class SpecDeliveryContextRebaseDelta:
    """Bounded, human-readable delivery-context effects of one rebase."""

    effective_value_changed: bool
    inherited_value_changed: bool
    override_state_changed: bool
    override_reason_changed: bool

    def as_dict(self) -> dict[str, bool]:
        return {
            "effective_value_changed": self.effective_value_changed,
            "inherited_value_changed": self.inherited_value_changed,
            "override_state_changed": self.override_state_changed,
            "override_reason_changed": self.override_reason_changed,
        }


@dataclass(frozen=True, slots=True)
class SourceContextInvestigationRebaseDelta:
    """Authored investigation changes, separated by head, receipt and outcome."""

    head_changed_source_refs: tuple[str, ...]
    current_receipt_changed_source_refs: tuple[str, ...]
    outcome_changed: bool
    previous_outcome: ContextualInvestigationOutcomeV2 | None
    next_outcome: ContextualInvestigationOutcomeV2 | None

    @property
    def changed(self) -> bool:
        return bool(
            self.head_changed_source_refs
            or self.current_receipt_changed_source_refs
            or self.outcome_changed
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "changed": self.changed,
            "head_changed_source_refs": self.head_changed_source_refs,
            "current_receipt_changed_source_refs": (
                self.current_receipt_changed_source_refs
            ),
            "outcome_changed": self.outcome_changed,
            "previous_outcome": (
                None if self.previous_outcome is None else self.previous_outcome.value
            ),
            "next_outcome": (
                None if self.next_outcome is None else self.next_outcome.value
            ),
        }


@dataclass(frozen=True, slots=True)
class SourceContextEvidenceRebaseDelta:
    """Contextual Evidence digest and factual role-count changes."""

    context_sha256_changed_evidence_ids: tuple[str, ...]
    role_counts_changed: bool
    previous_role_counts: SourceContextRoleCountsV2
    next_role_counts: SourceContextRoleCountsV2

    @property
    def changed(self) -> bool:
        return bool(self.context_sha256_changed_evidence_ids or self.role_counts_changed)

    @staticmethod
    def _role_counts_payload(value: SourceContextRoleCountsV2) -> dict[str, int]:
        return {
            "current_implementation_count": value.current_implementation_count,
            "existing_scaffold_count": value.existing_scaffold_count,
            "existing_constraint_count": value.existing_constraint_count,
            "reference_pattern_count": value.reference_pattern_count,
            "uncategorized_legacy_count": value.uncategorized_legacy_count,
        }

    def as_dict(self) -> dict[str, object]:
        return {
            "changed": self.changed,
            "context_sha256_changed_evidence_ids": (
                self.context_sha256_changed_evidence_ids
            ),
            "role_counts_changed": self.role_counts_changed,
            "previous_role_counts": self._role_counts_payload(
                self.previous_role_counts
            ),
            "next_role_counts": self._role_counts_payload(self.next_role_counts),
        }


@dataclass(frozen=True, slots=True)
class SourceContextClassificationRebaseDelta:
    """Human overlay changes remain distinct from authored Evidence context."""

    overlay_changed_evidence_ids: tuple[str, ...]
    revision_changed_evidence_ids: tuple[str, ...]
    digest_changed_evidence_ids: tuple[str, ...]
    fence_revision_changed: bool
    fence_digest_changed: bool
    previous_fence: SourceContextClassificationFenceV2
    next_fence: SourceContextClassificationFenceV2

    @property
    def changed(self) -> bool:
        return bool(
            self.overlay_changed_evidence_ids
            or self.fence_revision_changed
            or self.fence_digest_changed
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "changed": self.changed,
            "overlay_changed_evidence_ids": self.overlay_changed_evidence_ids,
            "revision_changed_evidence_ids": self.revision_changed_evidence_ids,
            "digest_changed_evidence_ids": self.digest_changed_evidence_ids,
            "fence_revision_changed": self.fence_revision_changed,
            "fence_digest_changed": self.fence_digest_changed,
            "previous_fence": self.previous_fence.as_dict(),
            "next_fence": self.next_fence.as_dict(),
        }


@dataclass(frozen=True, slots=True)
class SourceContextRebaseDelta:
    investigation: SourceContextInvestigationRebaseDelta
    evidence: SourceContextEvidenceRebaseDelta
    classification: SourceContextClassificationRebaseDelta

    @property
    def changed(self) -> bool:
        return bool(
            self.investigation.changed
            or self.evidence.changed
            or self.classification.changed
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "changed": self.changed,
            "investigation": self.investigation.as_dict(),
            "evidence": self.evidence.as_dict(),
            "classification": self.classification.as_dict(),
        }


def _provenance_payload(
    provenance: SpecDeliveryContextProvenance,
) -> dict[str, str | int | None]:
    return {
        "value": provenance.value.value,
        "inherited_value": provenance.inherited_value.value,
        "source_refinement_id": provenance.source_refinement_id,
        "source_refinement_version": provenance.source_refinement_version,
        "override_reason": provenance.override_reason,
    }


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
    current_delivery_context_provenance: SpecDeliveryContextProvenance
    target_delivery_context_provenance: SpecDeliveryContextProvenance
    resulting_delivery_context_provenance: SpecDeliveryContextProvenance
    delivery_context_delta: SpecDeliveryContextRebaseDelta
    current_source_context_manifest: dict[str, object]
    current_source_context_sha256: str
    target_source_context_manifest: dict[str, object]
    target_source_context_sha256: str
    source_context_delta: SourceContextRebaseDelta
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
            "current_delivery_context_provenance": _provenance_payload(
                self.current_delivery_context_provenance
            ),
            "target_delivery_context_provenance": _provenance_payload(
                self.target_delivery_context_provenance
            ),
            "resulting_delivery_context_provenance": _provenance_payload(
                self.resulting_delivery_context_provenance
            ),
            "delivery_context_delta": self.delivery_context_delta.as_dict(),
            "current_source_context_manifest": self.current_source_context_manifest,
            "current_source_context_sha256": self.current_source_context_sha256,
            "target_source_context_manifest": self.target_source_context_manifest,
            "target_source_context_sha256": self.target_source_context_sha256,
            "source_context_delta": self.source_context_delta.as_dict(),
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


@dataclass(frozen=True, slots=True)
class _ContextualEvidenceManifestEntry:
    content_sha256: str
    context_sha256: str
    classification_revision: int | None
    classification_sha256: str | None
    context_contract_version: int | None
    context_origin: str | None


class SpecCodeEvidenceRebaseService:
    def __init__(self, *, clock: Clock = _clock) -> None:
        self._clock = clock

    @classmethod
    def _snapshot_manifest(
        cls,
        snapshot: object,
    ) -> dict[str, _ContextualEvidenceManifestEntry]:
        raw = getattr(snapshot, "code_evidence_manifest", None)
        if raw is None and isinstance(snapshot, Mapping):
            raw = snapshot.get("code_evidence_manifest")
        if isinstance(raw, str | bytes) or not isinstance(raw, Sequence):
            raise CodeEvidenceLinkInvalid(
                details={"reason": "refinement_snapshot_manifest_invalid"}
            )
        result: dict[str, _ContextualEvidenceManifestEntry] = {}
        for entry in raw:
            if not isinstance(entry, Mapping):
                raise CodeEvidenceLinkInvalid(
                    details={"reason": "refinement_snapshot_manifest_invalid"}
                )
            evidence_id = entry.get("evidence_id")
            content_sha256 = entry.get("content_sha256")
            context_sha256 = entry.get("context_sha256")
            lifecycle = entry.get("lifecycle_status")
            classification_revision = entry.get("classification_revision")
            classification_sha256 = entry.get("classification_sha256")
            context_contract_version = entry.get("context_contract_version")
            context_origin = entry.get("context_origin")
            legacy_keys = {
                "evidence_id",
                "content_sha256",
                "lifecycle_status",
                "context_sha256",
                "classification_revision",
                "classification_sha256",
            }
            contextual_keys = {
                *legacy_keys,
                "context_contract_version",
                "context_origin",
            }
            keys = frozenset(entry)
            if (
                keys not in {frozenset(legacy_keys), frozenset(contextual_keys)}
                or not isinstance(evidence_id, str)
                or not evidence_id
                or evidence_id in result
                or not isinstance(content_sha256, str)
                or _SHA256_RE.fullmatch(content_sha256.casefold()) is None
                or not isinstance(context_sha256, str)
                or _SHA256_RE.fullmatch(context_sha256.casefold()) is None
                or lifecycle != CodeTraceabilityLifecycleStatus.ACTIVE.value
                or (
                    (classification_revision is None)
                    != (classification_sha256 is None)
                )
                or (
                    classification_revision is not None
                    and (
                        type(classification_revision) is not int
                        or classification_revision < 1
                        or not isinstance(classification_sha256, str)
                        or _SHA256_RE.fullmatch(
                            classification_sha256.casefold()
                        )
                        is None
                    )
                )
                or (
                    keys == frozenset(legacy_keys)
                    and classification_revision is not None
                )
                or (
                    keys == contextual_keys
                    and (
                        context_contract_version not in {None, 2}
                        or context_origin
                        not in {
                            "authored",
                            "human_legacy_classification",
                            "unclassified_legacy",
                        }
                        or (context_contract_version is None)
                        != (context_origin == "unclassified_legacy")
                        or (classification_revision is None)
                        != (context_origin != "human_legacy_classification")
                    )
                )
            ):
                raise CodeEvidenceLinkInvalid(
                    details={"reason": "refinement_snapshot_manifest_invalid"}
                )
            result[evidence_id] = _ContextualEvidenceManifestEntry(
                content_sha256=content_sha256.casefold(),
                context_sha256=context_sha256.casefold(),
                classification_revision=classification_revision,
                classification_sha256=(
                    None
                    if classification_sha256 is None
                    else classification_sha256.casefold()
                ),
                context_contract_version=context_contract_version,
                context_origin=context_origin,
            )
        return result

    @staticmethod
    def _field(value: object, name: str) -> Any:
        if isinstance(value, Mapping):
            return value.get(name)
        return getattr(value, name, None)

    @classmethod
    def _delivery_context(
        cls,
        value: object,
        *,
        reason: str,
    ) -> DeliveryContext:
        try:
            return DeliveryContext(value)
        except (TypeError, ValueError) as exc:
            raise CodeDeliveryContextRequired(details={"reason": reason}) from exc

    @classmethod
    def _delivery_context_provenance(
        cls,
        value: object,
        *,
        reason: str,
    ) -> SpecDeliveryContextProvenance:
        if value is None:
            raise CodeDeliveryContextRequired(details={"reason": reason})
        if isinstance(value, SpecDeliveryContextProvenance):
            return value
        try:
            return SpecDeliveryContextProvenance(
                value=cls._field(value, "value"),
                inherited_value=cls._field(value, "inherited_value"),
                source_refinement_id=cls._field(value, "source_refinement_id"),
                source_refinement_version=cls._field(
                    value,
                    "source_refinement_version",
                ),
                override_reason=cls._field(value, "override_reason"),
            )
        except CodeTraceabilityContractError:
            raise
        except (AttributeError, TypeError, ValueError) as exc:
            raise CodeDeliveryContextRequired(details={"reason": reason}) from exc

    @classmethod
    def _source_context_manifest(
        cls,
        value: object,
        sha256: object,
        *,
        refinement_id: str,
        refinement_version: int,
        missing_reason: str,
        invalid_reason: str,
    ) -> tuple[
        RefinementSourceContextManifestV2,
        dict[str, object],
        str,
    ]:
        if value is None or sha256 is None:
            raise CodeEvidenceLinkInvalid(details={"reason": missing_reason})
        if (
            not isinstance(value, Mapping)
            or not isinstance(sha256, str)
            or _SHA256_RE.fullmatch(sha256.casefold()) is None
        ):
            raise CodeEvidenceLinkInvalid(details={"reason": invalid_reason})
        try:
            manifest = parse_refinement_source_context_manifest_v2(value)
        except (CodeTraceabilityContractError, TypeError, ValueError) as exc:
            raise CodeEvidenceLinkInvalid(details={"reason": invalid_reason}) from exc
        canonical = manifest.as_dict()
        if (
            manifest.refinement_id != refinement_id
            or manifest.refinement_version != refinement_version
            or manifest.payload_sha256 != sha256.casefold()
        ):
            raise CodeEvidenceLinkInvalid(details={"reason": invalid_reason})
        return manifest, canonical, manifest.payload_sha256

    @classmethod
    def _evidence_context_sha256(cls, evidence: object) -> str:
        source_role = cls._field(evidence, "source_role")
        if source_role is None:
            source_role = CodeEvidenceSourceRole.UNCATEGORIZED_LEGACY
        try:
            source_role = CodeEvidenceSourceRole(source_role)
        except (TypeError, ValueError) as exc:
            raise CodeEvidenceLinkInvalid(
                details={"reason": "target_snapshot_evidence_context_invalid"}
            ) from exc
        return canonical_code_traceability_sha256(
            {
                "source_role": source_role.value,
                "relevance_summary": cls._field(evidence, "relevance_summary"),
                "scope_relation": cls._field(evidence, "scope_relation"),
                "source_origin": cls._field(evidence, "source_origin"),
                "interpretation_limit": cls._field(
                    evidence,
                    "interpretation_limit",
                ),
                "baseline_provenance": cls._field(
                    evidence,
                    "baseline_provenance",
                ),
            }
        )

    @staticmethod
    def _source_context_delta(
        current: RefinementSourceContextManifestV2,
        target: RefinementSourceContextManifestV2,
        *,
        current_evidence: Mapping[str, _ContextualEvidenceManifestEntry],
        target_evidence: Mapping[str, _ContextualEvidenceManifestEntry],
    ) -> SourceContextRebaseDelta:
        current_receipts = {item.source_ref: item for item in current.current_receipts}
        target_receipts = {item.source_ref: item for item in target.current_receipts}
        source_refs = set(current_receipts) | set(target_receipts)
        head_changed = tuple(
            sorted(
                source_ref
                for source_ref in source_refs
                if (
                    current_receipts.get(source_ref) is None
                    or target_receipts.get(source_ref) is None
                    or (
                        current_receipts[source_ref].generation,
                        current_receipts[source_ref].head_revision,
                    )
                    != (
                        target_receipts[source_ref].generation,
                        target_receipts[source_ref].head_revision,
                    )
                )
            )
        )
        receipt_changed = tuple(
            sorted(
                source_ref
                for source_ref in source_refs
                if current_receipts.get(source_ref) != target_receipts.get(source_ref)
            )
        )
        evidence_ids = set(current_evidence) | set(target_evidence)
        context_changed = tuple(
            sorted(
                evidence_id
                for evidence_id in evidence_ids
                if (
                    current_evidence.get(evidence_id) is None
                    or target_evidence.get(evidence_id) is None
                    or current_evidence[evidence_id].context_sha256
                    != target_evidence[evidence_id].context_sha256
                )
            )
        )
        classification_ids = set(current_evidence) & set(target_evidence)
        revision_changed = tuple(
            sorted(
                evidence_id
                for evidence_id in classification_ids
                if current_evidence[evidence_id].classification_revision
                    != target_evidence[evidence_id].classification_revision
            )
        )
        digest_changed = tuple(
            sorted(
                evidence_id
                for evidence_id in classification_ids
                if current_evidence[evidence_id].classification_sha256
                    != target_evidence[evidence_id].classification_sha256
            )
        )
        return SourceContextRebaseDelta(
            investigation=SourceContextInvestigationRebaseDelta(
                head_changed_source_refs=head_changed,
                current_receipt_changed_source_refs=receipt_changed,
                outcome_changed=(
                    current.summary.investigation_outcome
                    is not target.summary.investigation_outcome
                ),
                previous_outcome=current.summary.investigation_outcome,
                next_outcome=target.summary.investigation_outcome,
            ),
            evidence=SourceContextEvidenceRebaseDelta(
                context_sha256_changed_evidence_ids=context_changed,
                role_counts_changed=(
                    current.summary.role_counts != target.summary.role_counts
                ),
                previous_role_counts=current.summary.role_counts,
                next_role_counts=target.summary.role_counts,
            ),
            classification=SourceContextClassificationRebaseDelta(
                overlay_changed_evidence_ids=tuple(
                    sorted(set(revision_changed) | set(digest_changed))
                ),
                revision_changed_evidence_ids=revision_changed,
                digest_changed_evidence_ids=digest_changed,
                fence_revision_changed=(
                    current.classification_fence.revision
                    != target.classification_fence.revision
                ),
                fence_digest_changed=(
                    current.classification_fence.payload_sha256
                    != target.classification_fence.payload_sha256
                ),
                previous_fence=current.classification_fence,
                next_fence=target.classification_fence,
            ),
        )

    @staticmethod
    def _resulting_delivery_context_provenance(
        *,
        current: SpecDeliveryContextProvenance,
        target: SpecDeliveryContextProvenance,
    ) -> SpecDeliveryContextProvenance:
        if current.overridden and current.value is not target.inherited_value:
            return SpecDeliveryContextProvenance(
                value=current.value,
                inherited_value=target.inherited_value,
                source_refinement_id=target.source_refinement_id,
                source_refinement_version=target.source_refinement_version,
                override_reason=current.override_reason,
            )
        # An inherited Spec follows the target baseline. An override that now
        # equals that baseline is normalized so consumers never see a phantom
        # override reason.
        return target

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
            or not spec_id
            or not isinstance(refinement_id, str)
            or not refinement_id
            or not isinstance(current_snapshot_id, str)
            or not current_snapshot_id
            or type(current_version) is not int
            or type(spec_version) is not int
            or spec_version != expected_spec_version
        ):
            raise CodeEvidenceLinkInvalid(
                details={"reason": "spec_rebase_scope_invalid"}
            )
        spec_delivery_context = self._delivery_context(
            self._field(spec, "delivery_context"),
            reason="spec_delivery_context_required",
        )
        current_provenance = self._delivery_context_provenance(
            self._field(spec, "delivery_context_provenance"),
            reason="spec_delivery_context_provenance_required",
        )
        if target_refinement_version <= current_version:
            raise CodeEvidenceLinkInvalid(
                details={"reason": "newer_refinement_snapshot_required"}
            )
        target_snapshot_id = self._field(target_snapshot, "id")
        if (
            self._field(current_snapshot, "id") != current_snapshot_id
            or self._field(current_snapshot, "refinement_id") != refinement_id
            or self._field(current_snapshot, "version") != current_version
            or self._field(target_snapshot, "refinement_id") != refinement_id
            or self._field(target_snapshot, "version") != target_refinement_version
            or not isinstance(target_snapshot_id, str)
            or not target_snapshot_id
            or target_snapshot_id == current_snapshot_id
        ):
            raise CodeEvidenceLinkInvalid(
                details={"reason": "refinement_snapshot_scope_mismatch"}
            )
        current_snapshot_context = self._delivery_context(
            self._field(current_snapshot, "delivery_context"),
            reason="current_refinement_snapshot_delivery_context_required",
        )
        target_snapshot_context = self._delivery_context(
            self._field(target_snapshot, "delivery_context"),
            reason="target_refinement_snapshot_delivery_context_required",
        )
        if (
            current_provenance.value is not spec_delivery_context
            or current_provenance.inherited_value is not current_snapshot_context
            or current_provenance.source_refinement_id != refinement_id
            or current_provenance.source_refinement_version != current_version
        ):
            raise CodeEvidenceLinkInvalid(
                details={"reason": "spec_delivery_context_provenance_mismatch"}
            )
        target_provenance = SpecDeliveryContextProvenance(
            value=target_snapshot_context,
            inherited_value=target_snapshot_context,
            source_refinement_id=refinement_id,
            source_refinement_version=target_refinement_version,
            override_reason=None,
        )
        resulting_provenance = self._resulting_delivery_context_provenance(
            current=current_provenance,
            target=target_provenance,
        )
        context_delta = SpecDeliveryContextRebaseDelta(
            effective_value_changed=(
                current_provenance.value is not resulting_provenance.value
            ),
            inherited_value_changed=(
                current_provenance.inherited_value
                is not resulting_provenance.inherited_value
            ),
            override_state_changed=(
                current_provenance.overridden is not resulting_provenance.overridden
            ),
            override_reason_changed=(
                current_provenance.override_reason
                != resulting_provenance.override_reason
            ),
        )
        current_source_context, current_source_context_payload, current_context_sha = (
            self._source_context_manifest(
                self._field(current_snapshot, "source_context_manifest"),
                self._field(current_snapshot, "source_context_sha256"),
                refinement_id=refinement_id,
                refinement_version=current_version,
                missing_reason=(
                    "current_refinement_snapshot_source_context_manifest_required"
                ),
                invalid_reason=(
                    "current_refinement_snapshot_source_context_manifest_invalid"
                ),
            )
        )
        target_source_context, target_source_context_payload, target_context_sha = (
            self._source_context_manifest(
                self._field(target_snapshot, "source_context_manifest"),
                self._field(target_snapshot, "source_context_sha256"),
                refinement_id=refinement_id,
                refinement_version=target_refinement_version,
                missing_reason=(
                    "target_refinement_snapshot_source_context_manifest_required"
                ),
                invalid_reason=(
                    "target_refinement_snapshot_source_context_manifest_invalid"
                ),
            )
        )
        spec_source_context, spec_source_context_payload, spec_context_sha = (
            self._source_context_manifest(
                self._field(spec, "source_context_manifest"),
                self._field(spec, "source_context_sha256"),
                refinement_id=refinement_id,
                refinement_version=current_version,
                missing_reason="spec_source_context_manifest_required",
                invalid_reason="spec_source_context_manifest_invalid",
            )
        )
        if (
            spec_context_sha != current_context_sha
            or spec_source_context_payload != current_source_context_payload
            or spec_source_context != current_source_context
        ):
            raise CodeEvidenceLinkInvalid(
                details={"reason": "spec_source_context_snapshot_mismatch"}
            )
        old_manifest = self._snapshot_manifest(current_snapshot)
        new_manifest = self._snapshot_manifest(target_snapshot)
        evidence_by_id: dict[str, CodeEvidence] = {}
        verified_classifications = []
        for evidence_id, manifest_entry in sorted(new_manifest.items()):
            evidence = await store.get_evidence(
                board_id=board_id,
                evidence_id=evidence_id,
            )
            actual_context_sha256 = None
            if evidence is not None:
                if manifest_entry.context_origin == "human_legacy_classification":
                    classification_reader = getattr(
                        store,
                        "get_evidence_classification",
                        None,
                    )
                    if not callable(classification_reader):
                        raise CodeEvidenceLinkInvalid(
                            details={
                                "reason": (
                                    "target_snapshot_classification_unavailable"
                                ),
                                "evidence_id": evidence_id,
                            }
                        )
                    classification = await classification_reader(
                        board_id=board_id,
                        evidence_id=evidence_id,
                        revision=manifest_entry.classification_revision,
                    )
                    if (
                        classification is None
                        or classification.classification_sha256
                        != manifest_entry.classification_sha256
                    ):
                        raise CodeEvidenceLinkInvalid(
                            details={
                                "reason": (
                                    "target_snapshot_classification_mismatch"
                                ),
                                "evidence_id": evidence_id,
                            }
                        )
                    effective_context = source_context_evidence_item_v2(
                        evidence,
                        classification,
                    )
                    verified_classifications.append(classification)
                    actual_context_sha256 = canonical_code_traceability_sha256(
                        source_context_evidence_payload_v2(effective_context)
                    )
                elif manifest_entry.context_origin is None:
                    actual_context_sha256 = self._evidence_context_sha256(
                        evidence
                    )
                else:
                    effective_context = source_context_evidence_item_v2(
                        evidence
                    )
                    actual_context_sha256 = canonical_code_traceability_sha256(
                        source_context_evidence_payload_v2(effective_context)
                    )
                if (
                    manifest_entry.context_origin is not None
                    and (
                        effective_context.context_origin.value
                        != manifest_entry.context_origin
                        or effective_context.context_contract_version
                        != manifest_entry.context_contract_version
                    )
                ):
                    actual_context_sha256 = None
            if (
                evidence is None
                or evidence.parent_id != refinement_id
                or type(evidence.parent_version) is not int
                or evidence.parent_version < 1
                or evidence.parent_version > target_refinement_version
                or evidence.lifecycle_status
                is not CodeTraceabilityLifecycleStatus.ACTIVE
                or evidence.content_sha256 != manifest_entry.content_sha256
                or actual_context_sha256 != manifest_entry.context_sha256
            ):
                raise CodeEvidenceLinkInvalid(
                    details={
                        "reason": "target_snapshot_evidence_mismatch",
                        "evidence_id": evidence_id,
                    }
                )
            evidence_by_id[evidence_id] = evidence

        expected_classification_fence = (
            source_context_classification_fence_v2(verified_classifications)
        )
        if (
            target_source_context.classification_fence
            != expected_classification_fence
        ):
            raise CodeEvidenceLinkInvalid(
                details={
                    "reason": "target_snapshot_classification_fence_mismatch"
                }
            )
        expected_summary = build_source_context_summary_v2(
            delivery_context=target_source_context.summary.delivery_context,
            delivery_context_provenance=(
                target_source_context.summary.delivery_context_provenance
            ),
            current_investigation_outcomes=tuple(
                item.contextual_outcome
                for item in target_source_context.current_receipts
            ),
            evidence=tuple(evidence_by_id.values()),
            classifications=tuple(verified_classifications),
        )
        if target_source_context.summary != expected_summary:
            raise CodeEvidenceLinkInvalid(
                details={"reason": "target_snapshot_source_context_mismatch"}
            )

        old_ids = set(old_manifest)
        new_ids = set(new_manifest)
        added = tuple(sorted(new_ids - old_ids))
        removed = tuple(sorted(old_ids - new_ids))
        removed_set = set(removed)
        source_context_delta = self._source_context_delta(
            current_source_context,
            target_source_context,
            current_evidence=old_manifest,
            target_evidence=new_manifest,
        )
        affected_evidence_ids = {
            evidence_id for evidence_id in old_manifest
            if (
                evidence_id not in new_manifest
                or new_manifest[evidence_id].content_sha256
                != old_manifest[evidence_id].content_sha256
            )
        }
        affected_evidence_ids.update(
            source_context_delta.evidence.context_sha256_changed_evidence_ids
        )
        affected_evidence_ids.update(
            source_context_delta.classification.overlay_changed_evidence_ids
        )
        changed_source_refs = set(
            source_context_delta.investigation.head_changed_source_refs
        ) | set(
            source_context_delta.investigation.current_receipt_changed_source_refs
        )
        changed_receipt_ids = {
            item.receipt_id
            for item in (
                *current_source_context.current_receipts,
                *target_source_context.current_receipts,
            )
            if item.source_ref in changed_source_refs
        }
        for evidence_id in old_ids & new_ids:
            evidence = evidence_by_id[evidence_id]
            if (
                source_context_delta.investigation.outcome_changed
                or self._field(evidence, "source_ref") in changed_source_refs
                or self._field(evidence, "investigation_receipt_id")
                in changed_receipt_ids
            ):
                affected_evidence_ids.add(evidence_id)
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
                if link.evidence_id in affected_evidence_ids
                and link.evidence_id in old_manifest
                and link.evidence_content_sha256
                == old_manifest[link.evidence_id].content_sha256
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
                if item.evidence_id in affected_evidence_ids
            )
        )
        return SpecCodeEvidenceRebasePlan(
            board_id=board_id,
            spec_id=spec_id,
            expected_spec_version=expected_spec_version,
            resulting_spec_version=expected_spec_version + 1,
            current_refinement_snapshot_id=current_snapshot_id,
            current_refinement_version=current_version,
            target_refinement_snapshot_id=target_snapshot_id,
            target_refinement_version=target_refinement_version,
            current_delivery_context_provenance=current_provenance,
            target_delivery_context_provenance=target_provenance,
            resulting_delivery_context_provenance=resulting_provenance,
            delivery_context_delta=context_delta,
            current_source_context_manifest=current_source_context_payload,
            current_source_context_sha256=current_context_sha,
            target_source_context_manifest=target_source_context_payload,
            target_source_context_sha256=target_context_sha,
            source_context_delta=source_context_delta,
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
            expected_delivery_context=(plan.current_delivery_context_provenance.value),
            expected_delivery_context_provenance=(
                plan.current_delivery_context_provenance
            ),
            next_delivery_context=(plan.resulting_delivery_context_provenance.value),
            next_delivery_context_provenance=(
                plan.resulting_delivery_context_provenance
            ),
            expected_source_context_manifest=plan.current_source_context_manifest,
            expected_source_context_sha256=plan.current_source_context_sha256,
            next_source_context_manifest=plan.target_source_context_manifest,
            next_source_context_sha256=plan.target_source_context_sha256,
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
    "SourceContextClassificationRebaseDelta",
    "SourceContextEvidenceRebaseDelta",
    "SourceContextInvestigationRebaseDelta",
    "SourceContextRebaseDelta",
    "SpecCodeEvidenceRebasePlan",
    "SpecCodeEvidenceRebaseResult",
    "SpecCodeEvidenceRebaseService",
    "SpecDeliveryContextRebaseDelta",
]
