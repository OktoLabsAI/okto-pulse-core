"""Pure, reconstructible delivery readiness projection.

The source ledger and the native completion gates remain authoritative.  This
module only turns one immutable source snapshot into rows that a storage adapter
can stage and publish as a single generation.  In particular, it never infers
proof from a Card's workflow status.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from hashlib import sha256
import json

from okto_pulse.core.domain.delivery_assessment import (
    AssessmentCompleteness,
    DeliveryAssessment,
    FactualReason,
    ReadinessReason,
    UnitPhaseAssessment,
    UnitState,
    aggregate_delivery_metrics,
)
from okto_pulse.core.domain.effective_delivery_coverage import EvaluatedDeliverySnapshot
from okto_pulse.core.domain.enums import CardType
from okto_pulse.core.models.delivery_readiness import DeliveryMetric


EVALUATOR_VERSION = "delivery-readiness-evaluator/v1"


def readiness_manifest_digest(*, board_id: str, spec_id: str, edition: int,
                              captured_revision: int, source_snapshot_id: str,
                              evaluator_version: str, summaries: list[dict],
                              units: list[dict]) -> str:
    """Canonical digest of exactly the rows a reader will receive."""
    manifest = {
        "scope": [board_id, spec_id, edition, captured_revision],
        "source_snapshot_id": source_snapshot_id,
        "evaluator_version": evaluator_version,
        "summaries": sorted(summaries, key=lambda row: row["card_id"] or ""),
        "units": sorted(units, key=lambda row: (row["card_id"] or "", row["kind"],
            row["ref"], row["semantic_sha256"])),
    }
    return sha256(json.dumps(manifest, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class ProjectedUnit:
    card_id: str | None
    kind: str
    ref: str
    semantic_sha256: str
    spec_edition: int
    contribution_card_ids: tuple[str, ...]
    implementation_state: str
    verification_state: str
    implementation_reason_codes: tuple[str, ...]
    verification_reason_codes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ProjectedSummary:
    card_id: str | None
    card_type: str | None
    card_version: int | None
    implementation: DeliveryMetric
    verification: DeliveryMetric
    population_known: bool
    evaluation_complete: bool

    def payload(self) -> dict:
        return {
            "card_id": self.card_id,
            "card_type": self.card_type,
            "card_version": self.card_version,
            "implementation": self.implementation.model_dump(mode="json"),
            "verification": self.verification.model_dump(mode="json"),
            "population_known": self.population_known,
            "evaluation_complete": self.evaluation_complete,
        }


@dataclass(frozen=True, slots=True)
class ReadinessGeneration:
    board_id: str
    spec_id: str
    spec_edition: int
    spec_version: int
    captured_revision: int
    source_snapshot_id: str
    evaluated_at: datetime
    evaluator_version: str
    summaries: tuple[ProjectedSummary, ...]
    units: tuple[ProjectedUnit, ...]
    manifest_sha256: str


def _codes(phase: UnitPhaseAssessment) -> tuple[str, ...]:
    return tuple(sorted({reason.code.value for reason in phase.reasons}))


def _unit_rows(assessment: DeliveryAssessment) -> tuple[ProjectedUnit, ...]:
    if assessment.card_type == CardType.TEST:
        return tuple(ProjectedUnit(
            assessment.card_id, "scenario", row.scenario_id, row.semantic_sha256,
            row.spec_edition, (row.card_id,), UnitState.NOT_APPLICABLE.value,
            row.verification.state.value, (), _codes(row.verification),
        ) for row in assessment.scenarios)
    return tuple(ProjectedUnit(
        assessment.card_id, "obligation", row.binding.obligation_ref,
        row.binding.semantic_sha256, row.spec_edition, row.contribution_card_ids,
        row.implementation.state.value, row.verification.state.value,
        _codes(row.implementation), _codes(row.verification),
    ) for row in assessment.obligations)


def _summary(assessment: DeliveryAssessment, *, card_version: int | None) -> ProjectedSummary:
    metrics = aggregate_delivery_metrics(assessment)
    return ProjectedSummary(
        assessment.card_id,
        (assessment.card_type.value if isinstance(assessment.card_type, CardType)
            else str(assessment.card_type)) if assessment.card_type is not None else None,
        card_version,
        DeliveryMetric.model_validate(asdict(metrics.implementation)),
        DeliveryMetric.model_validate(asdict(metrics.verification)),
        assessment.completeness.population_known and assessment.completeness.scope_resolved,
        assessment.completeness.evaluation_complete,
    )


def build_readiness_generation(snapshot, *, captured_revision: int) -> ReadinessGeneration:
    """Evaluate exactly one source revision with the existing canonical engine.

    The caller must close its DB snapshot before invoking this CPU-bound
    function.  A storage adapter must stage all returned rows and CAS-publish
    the manifest; this object by itself has no authority to publish.
    """
    if captured_revision < 0:
        raise ValueError("readiness_revision_invalid")
    if snapshot.unavailable_reason_codes:
        reason = FactualReason(ReadinessReason.ASSESSMENT_UNAVAILABLE,
            native_reason_codes=snapshot.unavailable_reason_codes)
        def unknown(card=None):
            return DeliveryAssessment(snapshot.facts.scope,
                AssessmentCompleteness(False, False, False, True), (),
                reasons=(reason,), card_id=card.id if card else None,
                card_type=card.card_type if card else None)
        assessments = [(None, unknown())] + [(card, unknown(card)) for card in snapshot.cards]
    else:
        evaluation = EvaluatedDeliverySnapshot(snapshot.facts)
        assessments = [(None, evaluation.obligations())]
        for card in snapshot.cards:
            assessment = (evaluation.scenarios(card_id=card.id, scenario_ids=card.scenario_ids)
                if card.card_type == CardType.TEST else
                evaluation.obligations(card_id=card.id, card_type=card.card_type))
            assessments.append((card, assessment))

    summaries = tuple(_summary(assessment, card_version=card.version if card else None)
        for card, assessment in assessments)
    unique_units = {}
    for _, assessment in assessments:
        for unit in _unit_rows(assessment):
            key = (unit.card_id, unit.kind, unit.ref, unit.semantic_sha256)
            existing = unique_units.setdefault(key, unit)
            if existing != unit:
                raise ValueError("readiness_unit_conflict")
    units = tuple(unique_units[key] for key in sorted(unique_units,
        key=lambda key: (key[0] or "", key[1], key[2], key[3])))
    digest = readiness_manifest_digest(board_id=snapshot.facts.scope.board_id,
        spec_id=snapshot.facts.scope.spec_id, edition=snapshot.facts.scope.edition,
        captured_revision=captured_revision, source_snapshot_id=snapshot.snapshot_id,
        evaluator_version=EVALUATOR_VERSION,
        summaries=[summary.payload() for summary in summaries],
        units=[asdict(unit) for unit in units])
    return ReadinessGeneration(snapshot.facts.scope.board_id, snapshot.facts.scope.spec_id,
        snapshot.facts.scope.edition, snapshot.spec_version, captured_revision,
        snapshot.snapshot_id, snapshot.evaluated_at, EVALUATOR_VERSION, summaries, units, digest)
