"""Pure, reconstructible delivery readiness projection.

The source ledger and the native completion gates remain authoritative.  This
module only turns one immutable source snapshot into rows that a storage adapter
can stage and publish as a single generation.  In particular, it never infers
proof from a Card's workflow status.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime
from hashlib import sha256
from time import monotonic
import orjson
from pydantic import TypeAdapter

from okto_pulse.core.domain.delivery_assessment import (
    AssessmentCompleteness,
    DeliveryAssessment,
    FactualReason,
    ReadinessReason,
    UnitPhaseAssessment,
    UnitState,
)
from okto_pulse.core.domain.effective_delivery_coverage import (
    EffectiveDeliveryContext, EvaluatedDeliverySnapshot,
)
from okto_pulse.core.domain.enums import CardType
from okto_pulse.core.models.delivery_readiness import DeliveryReadinessContext, DeliveryMetric
from okto_pulse.core.ports.delivery_readiness import READINESS_PERMISSION_OPERATIONS, ReadinessAccess
from okto_pulse.core.services.delivery_readiness import EvaluatedDeliveryReadiness
from okto_pulse.core.services.delivery_readiness_diagnostics import NativeDeliveryDiagnostics


EVALUATOR_VERSION = "delivery-readiness-evaluator/v4"
_DIAGNOSTICS = TypeAdapter(NativeDeliveryDiagnostics)
_PROGRESS_LOCAL_BLOCKERS = frozenset({
    "delivery_implementation_missing", "delivery_test_result_missing",
    "decision_verification_incomplete", "delivery_verification_run_conflict",
})


class IncrementalReadinessUnavailable(ValueError):
    """A source delta cannot safely reuse the previous generation."""


def _progress_affected_card_ids(snapshot, evaluation: EvaluatedDeliverySnapshot,
                                changed_card_id: str) -> frozenset[str | None] | None:
    """Conservatively close one Card progress delta over its proof dependants.

    The caller must separately prove that this insert is the sole source
    mutation since the predecessor. A changed Spec, Card declaration, test run,
    receipt or exception needs a full generation.
    """
    context = snapshot.facts.effective_context
    if (not isinstance(context, EffectiveDeliveryContext)
            or not context.inventory.complete
            or set(evaluation.proof.blockers) - _PROGRESS_LOCAL_BLOCKERS
            or set(evaluation.lifecycle.blockers) - _PROGRESS_LOCAL_BLOCKERS):
        return None
    cards = {card.id: card for card in snapshot.cards}
    changed = cards.get(changed_card_id)
    if changed is None or changed.card_type not in {CardType.NORMAL, CardType.BUG}:
        return None
    allocated = context.inventory.card_obligations(changed_card_id)
    if not allocated:
        return None
    bindings = {row.binding for row in allocated}
    bindings.update(binding for fact in snapshot.facts.implementations
        if fact.card_id == changed_card_id for binding in fact.bindings)
    bindings.update(binding for fact in snapshot.facts.rejected_records
        if fact.card_id == changed_card_id and fact.kind == "implementation"
        for binding in fact.bindings)
    by_binding = {row.binding: row for row in context.inventory.rows}
    if (len(by_binding) != len(context.inventory.rows)
            or any(binding not in by_binding or by_binding[binding].family == "decision"
                for binding in bindings)):
        return None
    affected: set[str | None] = {None, changed_card_id}
    for binding in bindings:
        row = by_binding[binding]
        affected.update(row.declared_card_ids)
        affected.update(contribution.card_id for contribution in row.contributions)
    scenarios = {check.scenario_id for check in evaluation.proof.scenario_checks
        if check.binding in bindings}
    scenarios.update(fact.scenario_id for fact in snapshot.facts.tests
        if set(fact.bindings) & bindings)
    affected.update(fact.card_id for fact in snapshot.facts.tests
        if set(fact.bindings) & bindings)
    for plan in context.required_scenarios or ():
        if plan.scenario_id in scenarios:
            affected.update(plan.test_card_ids)
    affected.update(card.id for card in snapshot.cards
        if card.card_type == CardType.TEST and set(card.scenario_ids or ()) & scenarios)
    if affected - {None} - cards.keys():
        return None
    return frozenset(affected)


def restore_projected_diagnostics(payload: dict) -> NativeDeliveryDiagnostics:
    return _DIAGNOSTICS.validate_python(payload)


def readiness_manifest_digest(*, board_id: str, spec_id: str, edition: int,
                              captured_revision: int, source_snapshot_id: str,
                              evaluator_version: str, summaries: list[dict],
                              units: list[dict], diagnostics: list[dict]) -> str:
    """Canonical digest of exactly the rows a reader will receive."""
    manifest = {
        "scope": [board_id, spec_id, edition, captured_revision],
        "source_snapshot_id": source_snapshot_id,
        "evaluator_version": evaluator_version,
        "summaries": sorted(summaries, key=lambda row: row["card_id"] or ""),
        "units": sorted(units, key=lambda row: (row["card_id"] or "", row["kind"],
            row["ref"], row["semantic_sha256"])),
        "diagnostics": sorted(diagnostics, key=lambda row: row["card_id"] or ""),
    }
    # The version above fences generations written by the previous encoder.
    # Both creation and read-back verification hash these exact JSON bytes.
    return sha256(orjson.dumps(manifest, option=orjson.OPT_SORT_KEYS)).hexdigest()


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
    context: dict

    def payload(self) -> dict:
        return {
            "card_id": self.card_id,
            "card_type": self.card_type,
            "card_version": self.card_version,
            "implementation": self.implementation.model_dump(mode="json"),
            "verification": self.verification.model_dump(mode="json"),
            "population_known": self.population_known,
            "evaluation_complete": self.evaluation_complete,
            "context": self.context,
        }


@dataclass(frozen=True, slots=True)
class ProjectedDiagnostic:
    card_id: str | None
    payload: dict

    def row(self) -> dict:
        return {"card_id": self.card_id, "payload": self.payload}


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
    diagnostics: tuple[ProjectedDiagnostic, ...]
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


def _summary(assessment: DeliveryAssessment, *, card_version: int | None,
             context: DeliveryReadinessContext) -> ProjectedSummary:
    return ProjectedSummary(
        assessment.card_id,
        (assessment.card_type.value if isinstance(assessment.card_type, CardType)
            else str(assessment.card_type)) if assessment.card_type is not None else None,
        card_version,
        context.implementation,
        context.verification,
        assessment.completeness.population_known and assessment.completeness.scope_resolved,
        assessment.completeness.evaluation_complete,
        context.model_dump(mode='json'),
    )


def _verified_predecessor(snapshot, *, captured_revision: int,
                          predecessor: ReadinessGeneration,
                          affected_card_ids: frozenset[str | None]) -> tuple[
                              dict[str | None, ProjectedSummary],
                              dict[str | None, ProjectedDiagnostic],
                              dict[str | None, tuple[ProjectedUnit, ...]],
                          ]:
    """Refuse reuse unless the predecessor is complete and adjacent to this source."""
    scope = snapshot.facts.scope
    cards = {card.id: card for card in snapshot.cards}
    expected_ids = {None, *cards}
    if (snapshot.unavailable_reason_codes
            or not isinstance(snapshot.facts.effective_context, EffectiveDeliveryContext)
            or not snapshot.facts.effective_context.inventory.complete
            or affected_card_ids is None or None not in affected_card_ids
            or not affected_card_ids <= expected_ids
            or predecessor.board_id != scope.board_id
            or predecessor.spec_id != scope.spec_id
            or predecessor.spec_edition != scope.edition
            or predecessor.spec_version != snapshot.spec_version
            or predecessor.evaluator_version != EVALUATOR_VERSION
            or predecessor.captured_revision + 1 != captured_revision
            or predecessor.source_snapshot_id == snapshot.snapshot_id):
        raise IncrementalReadinessUnavailable("readiness_incremental_predecessor_ineligible")
    summaries = {row.card_id: row for row in predecessor.summaries}
    diagnostics = {row.card_id: row for row in predecessor.diagnostics}
    if (len(summaries) != len(predecessor.summaries)
            or len(diagnostics) != len(predecessor.diagnostics)
            or set(summaries) != expected_ids or set(diagnostics) != expected_ids):
        raise IncrementalReadinessUnavailable("readiness_incremental_predecessor_incomplete")
    for card_id, summary in summaries.items():
        card = cards.get(card_id)
        status = card.status if card is not None else snapshot.spec_status
        status = status.value if hasattr(status, "value") else status
        if (summary.card_version != (card.version if card else None)
                or summary.card_type != ((card.card_type.value
                    if isinstance(card.card_type, CardType) else str(card.card_type)) if card else None)
                or summary.context.get("snapshot_id") != predecessor.source_snapshot_id
                or summary.context.get("lifecycle", {}).get("stored_status") != status):
            raise IncrementalReadinessUnavailable("readiness_incremental_predecessor_scope_changed")
    units: dict[str | None, list[ProjectedUnit]] = {}
    for row in predecessor.units:
        if row.card_id not in expected_ids:
            raise IncrementalReadinessUnavailable("readiness_incremental_predecessor_incomplete")
        units.setdefault(row.card_id, []).append(row)
    digest = readiness_manifest_digest(board_id=predecessor.board_id,
        spec_id=predecessor.spec_id, edition=predecessor.spec_edition,
        captured_revision=predecessor.captured_revision,
        source_snapshot_id=predecessor.source_snapshot_id,
        evaluator_version=predecessor.evaluator_version,
        summaries=[row.payload() for row in predecessor.summaries],
        units=[asdict(row) for row in predecessor.units],
        diagnostics=[row.row() for row in predecessor.diagnostics])
    if digest != predecessor.manifest_sha256:
        raise IncrementalReadinessUnavailable("readiness_incremental_predecessor_manifest_mismatch")
    return summaries, diagnostics, {key: tuple(rows) for key, rows in units.items()}


def build_readiness_generation(snapshot, *, captured_revision: int,
                               deadline: float | None = None,
                               predecessor: ReadinessGeneration | None = None,
                               affected_card_ids: frozenset[str | None] | None = None,
                               progress_card_id: str | None = None,
                               ) -> ReadinessGeneration:
    """Evaluate exactly one source revision with the existing canonical engine.

    The caller must close its DB snapshot before invoking this CPU-bound
    function.  A storage adapter must stage all returned rows and CAS-publish
    the manifest; this object by itself has no authority to publish.
    Reuse requires a verified adjacent predecessor and an affected Card set
    proven from the same transactional source mutation. If that proof is absent,
    callers must use the full calculation.
    """
    if captured_revision < 0:
        raise ValueError("readiness_revision_invalid")
    if ((predecessor is None and (affected_card_ids is not None or progress_card_id is not None))
            or (predecessor is not None
                and (affected_card_ids is None) == (progress_card_id is None))):
        raise ValueError("readiness_incremental_arguments_incomplete")
    def checkpoint() -> None:
        if deadline is not None and monotonic() >= deadline:
            raise TimeoutError("readiness_calculation_budget_exceeded")

    checkpoint()
    prior_summaries = prior_diagnostics = prior_units = None
    if predecessor is not None:
        prior_summaries, prior_diagnostics, prior_units = _verified_predecessor(snapshot,
            captured_revision=captured_revision, predecessor=predecessor,
            affected_card_ids=affected_card_ids if affected_card_ids is not None
                else frozenset({None}))
        checkpoint()
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
        checkpoint()
        if progress_card_id is not None:
            affected_card_ids = _progress_affected_card_ids(snapshot, evaluation,
                progress_card_id)
            if affected_card_ids is None:
                raise IncrementalReadinessUnavailable("readiness_progress_impact_unavailable")
        assessments = [(None, evaluation.obligations())]
        for card in snapshot.cards:
            checkpoint()
            assessment = (evaluation.scenarios(card_id=card.id, scenario_ids=card.scenario_ids)
                if card.card_type == CardType.TEST else
                evaluation.obligations(card_id=card.id, card_type=card.card_type))
            assessments.append((card, assessment))

    checkpoint()
    # This temporary read authority exists only inside the pure projector. It
    # lets the canonical context composer calculate actor-neutral counts and
    # lifecycle wording; no access token or permission grant is persisted.
    projection_actor = 'readiness-projection-builder'
    projection_batch = replace(snapshot, access=ReadinessAccess(
        projection_actor, snapshot.facts.scope.board_id, 'local',
        READINESS_PERMISSION_OPERATIONS))
    contextual = EvaluatedDeliveryReadiness(projection_batch,
        actor_id=projection_actor, precomputed_assessments={
            card.id if card else None: assessment for card, assessment in assessments})
    spec_context = contextual.context(None) if predecessor is not None else None
    provenance_time = spec_context.model_dump(mode='json')["evaluated_at"] if spec_context else None
    summaries_list = []
    diagnostics_list = []
    for index, (card, assessment) in enumerate(assessments):
        if index % 25 == 0:
            checkpoint()
        card_id = card.id if card else None
        if predecessor is not None and card_id not in affected_card_ids:
            previous = prior_summaries[card_id]
            summaries_list.append(replace(previous, context={**previous.context,
                "snapshot_id": snapshot.snapshot_id, "evaluated_at": provenance_time}))
            diagnostics_list.append(prior_diagnostics[card_id])
        else:
            summaries_list.append(_summary(assessment,
                card_version=card.version if card else None,
                context=spec_context if spec_context is not None and card_id is None
                    else contextual.context(card_id)))
            diagnostics_list.append(ProjectedDiagnostic(card_id,
                _DIAGNOSTICS.dump_python(contextual.diagnostics(card_id), mode='json')))
    summaries = tuple(summaries_list)
    diagnostics = tuple(diagnostics_list)
    unique_units = {}
    processed_units = 0
    for card, assessment in assessments:
        card_id = card.id if card else None
        rows = (prior_units.get(card_id, ()) if predecessor is not None
            and card_id not in affected_card_ids else _unit_rows(assessment))
        for unit in rows:
            processed_units += 1
            if processed_units % 250 == 0:
                checkpoint()
            key = (unit.card_id, unit.kind, unit.ref, unit.semantic_sha256)
            existing = unique_units.setdefault(key, unit)
            if existing != unit:
                raise ValueError("readiness_unit_conflict")
    units = tuple(unique_units[key] for key in sorted(unique_units,
        key=lambda key: (key[0] or "", key[1], key[2], key[3])))
    checkpoint()
    digest = readiness_manifest_digest(board_id=snapshot.facts.scope.board_id,
        spec_id=snapshot.facts.scope.spec_id, edition=snapshot.facts.scope.edition,
        captured_revision=captured_revision, source_snapshot_id=snapshot.snapshot_id,
        evaluator_version=EVALUATOR_VERSION,
        summaries=[summary.payload() for summary in summaries],
        units=[asdict(unit) for unit in units],
        diagnostics=[row.row() for row in diagnostics])
    return ReadinessGeneration(snapshot.facts.scope.board_id, snapshot.facts.scope.spec_id,
        snapshot.facts.scope.edition, snapshot.spec_version, captured_revision,
        snapshot.snapshot_id, snapshot.evaluated_at, EVALUATOR_VERSION,
        summaries, units, diagnostics, digest)
