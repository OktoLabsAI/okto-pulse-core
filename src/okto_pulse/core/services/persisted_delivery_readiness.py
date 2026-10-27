"""Pure, reconstructible delivery readiness projection.

The source ledger and the native completion gates remain authoritative.  This
module only turns one immutable source snapshot into rows that a storage adapter
can stage and publish as a single generation.  In particular, it never infers
proof from a Card's workflow status.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
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


@dataclass(frozen=True, slots=True)
class ReadinessSourceDelta:
    """One source revision persisted in the writer's transaction."""

    revision: int
    kind: str
    card_id: str | None


def _verified_progress_span(predecessor: ReadinessGeneration, captured_revision: int,
                            deltas: tuple[ReadinessSourceDelta, ...]) -> tuple[str, ...]:
    """Require a complete, ordered chain of progress inserts since the predecessor."""
    expected = predecessor.captured_revision + 1
    cards = []
    for delta in deltas:
        if (delta.revision != expected or delta.kind != "card_progress_insert"
                or not delta.card_id):
            raise IncrementalReadinessUnavailable("readiness_progress_delta_span_invalid")
        cards.append(delta.card_id)
        expected += 1
    if not cards or expected != captured_revision + 1:
        raise IncrementalReadinessUnavailable("readiness_progress_delta_span_invalid")
    return tuple(dict.fromkeys(cards))


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


def readiness_manifest_digest_stored(*, board_id: str, spec_id: str, edition: int,
                                     captured_revision: int, source_snapshot_id: str,
                                     evaluator_version: str,
                                     rows: list[tuple[int, str, str, str, str, str]]) -> str:
    """Hash canonical JSON rows read back from storage without decoding bodies.

    ``rows`` must be ordered by category, card key, kind, ref and semantic hash.
    The storage adapter checks the typed keys against JSON before calling this.
    This byte stream is exactly the object encoded by ``readiness_manifest_digest``.
    """
    digest = sha256()
    digest.update(b'{"diagnostics":[')
    first = True
    for category, card_key, _kind, _ref, _semantic, payload in rows:
        if category != 1:
            continue
        if not first:
            digest.update(b',')
        first = False
        digest.update(b'{"card_id":')
        digest.update(orjson.dumps(card_key or None))
        digest.update(b',"payload":')
        digest.update(payload.encode('utf-8'))
        digest.update(b'}')
    digest.update(b'],"evaluator_version":')
    digest.update(orjson.dumps(evaluator_version))
    digest.update(b',"scope":')
    digest.update(orjson.dumps([board_id, spec_id, edition, captured_revision]))
    digest.update(b',"source_snapshot_id":')
    digest.update(orjson.dumps(source_snapshot_id))
    digest.update(b',"summaries":[')
    first = True
    for category, _card_key, _kind, _ref, _semantic, payload in rows:
        if category != 0:
            continue
        if not first:
            digest.update(b',')
        first = False
        digest.update(payload.encode('utf-8'))
    digest.update(b'],"units":[')
    first = True
    for category, _card_key, _kind, _ref, _semantic, payload in rows:
        if category != 2:
            continue
        if not first:
            digest.update(b',')
        first = False
        digest.update(payload.encode('utf-8'))
    digest.update(b']}')
    return digest.hexdigest()


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

    def row(self) -> dict:
        """Canonical storage row without recursively copying immutable fields."""
        return {
            'card_id': self.card_id,
            'kind': self.kind,
            'ref': self.ref,
            'semantic_sha256': self.semantic_sha256,
            'spec_edition': self.spec_edition,
            'contribution_card_ids': self.contribution_card_ids,
            'implementation_state': self.implementation_state,
            'verification_state': self.verification_state,
            'implementation_reason_codes': self.implementation_reason_codes,
            'verification_reason_codes': self.verification_reason_codes,
        }


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
                          affected_card_ids: frozenset[str | None],
                          allow_revision_span: bool = False) -> tuple[
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
            or (not allow_revision_span and predecessor.captured_revision + 1 != captured_revision)
            or (allow_revision_span and predecessor.captured_revision >= captured_revision)
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
        units=[row.row() for row in predecessor.units],
        diagnostics=[row.row() for row in predecessor.diagnostics])
    if digest != predecessor.manifest_sha256:
        raise IncrementalReadinessUnavailable("readiness_incremental_predecessor_manifest_mismatch")
    return summaries, diagnostics, {key: tuple(rows) for key, rows in units.items()}


def build_readiness_generation(snapshot, *, captured_revision: int,
                               deadline: float | None = None,
                               predecessor: ReadinessGeneration | None = None,
                               affected_card_ids: frozenset[str | None] | None = None,
                               progress_card_id: str | None = None,
                               progress_deltas: tuple[ReadinessSourceDelta, ...] | None = None,
                                phase_metrics: dict[str, float] | None = None,
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
    supplied = sum(value is not None for value in (
        affected_card_ids, progress_card_id, progress_deltas))
    if (predecessor is None and supplied or predecessor is not None and supplied != 1):
        raise ValueError("readiness_incremental_arguments_incomplete")
    def checkpoint() -> None:
        if deadline is not None and monotonic() >= deadline:
            raise TimeoutError("readiness_calculation_budget_exceeded")

    checkpoint()
    phase_now = monotonic if phase_metrics is not None else lambda: 0.0
    phase_started = phase_now()
    prior_summaries = prior_diagnostics = prior_units = None
    changed_progress_cards = (_verified_progress_span(predecessor,
        captured_revision, progress_deltas) if progress_deltas is not None else ())
    if predecessor is not None:
        prior_summaries, prior_diagnostics, prior_units = _verified_predecessor(snapshot,
            captured_revision=captured_revision, predecessor=predecessor,
            affected_card_ids=affected_card_ids if affected_card_ids is not None
                else frozenset({None}),
            allow_revision_span=progress_deltas is not None)
        checkpoint()
    if phase_metrics is not None:
        phase_metrics['readiness_compute_predecessor_ms'] = (monotonic() - phase_started) * 1000
    phase_started = phase_now()
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
        if phase_metrics is not None:
            phase_metrics['readiness_compute_evaluation_ms'] = (monotonic() - phase_started) * 1000
        phase_started = phase_now()
        checkpoint()
        if progress_card_id is not None:
            affected_card_ids = _progress_affected_card_ids(snapshot, evaluation,
                progress_card_id)
            if affected_card_ids is None:
                raise IncrementalReadinessUnavailable("readiness_progress_impact_unavailable")
        elif progress_deltas is not None:
            affected = set()
            for changed_card_id in changed_progress_cards:
                impact = _progress_affected_card_ids(snapshot, evaluation, changed_card_id)
                if impact is None:
                    raise IncrementalReadinessUnavailable("readiness_progress_impact_unavailable")
                affected.update(impact)
            affected_card_ids = frozenset(affected)
        if phase_metrics is not None:
            phase_metrics['readiness_compute_impact_ms'] = (monotonic() - phase_started) * 1000
        phase_started = phase_now()
        assessments = [(None, evaluation.obligations())]
        for card in snapshot.cards:
            checkpoint()
            if predecessor is not None and card.id not in affected_card_ids:
                assessment = None  # Its complete rows come from the verified predecessor.
            elif card.card_type == CardType.TEST:
                assessment = evaluation.scenarios(card_id=card.id,
                    scenario_ids=card.scenario_ids)
            else:
                assessment = evaluation.obligations(card_id=card.id,
                    card_type=card.card_type)
            assessments.append((card, assessment))
    if phase_metrics is not None:
        phase_metrics['readiness_compute_assessments_ms'] = (monotonic() - phase_started) * 1000

    checkpoint()
    phase_started = phase_now()
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
    if phase_metrics is not None:
        phase_metrics['readiness_compute_context_setup_ms'] = (monotonic() - phase_started) * 1000
    spec_context_started = phase_now()
    spec_context = contextual.context(None) if predecessor is not None else None
    if phase_metrics is not None:
        phase_metrics['readiness_compute_context_spec_ms'] = (monotonic() - spec_context_started) * 1000
        phase_metrics['readiness_compute_context_ms'] = (monotonic() - phase_started) * 1000
    phase_started = phase_now()
    provenance_time = spec_context.model_dump(mode='json')["evaluated_at"] if spec_context else None
    summaries_list = []
    diagnostics_list = []
    changed_context_ms = changed_diagnostic_ms = 0.0
    reused_rows = 0
    for index, (card, assessment) in enumerate(assessments):
        if index % 25 == 0:
            checkpoint()
        card_id = card.id if card else None
        if predecessor is not None and card_id not in affected_card_ids:
            previous = prior_summaries[card_id]
            summaries_list.append(replace(previous, context={**previous.context,
                "snapshot_id": snapshot.snapshot_id, "evaluated_at": provenance_time}))
            diagnostics_list.append(prior_diagnostics[card_id])
            reused_rows += 1
        else:
            context_started = phase_now()
            context = spec_context if spec_context is not None and card_id is None else contextual.context(card_id)
            changed_context_ms += (phase_now() - context_started) * 1000
            summaries_list.append(_summary(assessment,
                card_version=card.version if card else None,
                context=context))
            diagnostic_started = phase_now()
            diagnostics_list.append(ProjectedDiagnostic(card_id,
                _DIAGNOSTICS.dump_python(contextual.diagnostics(card_id), mode='json')))
            changed_diagnostic_ms += (phase_now() - diagnostic_started) * 1000
    summaries = tuple(summaries_list)
    diagnostics = tuple(diagnostics_list)
    if phase_metrics is not None:
        phase_metrics['readiness_compute_rows_ms'] = (monotonic() - phase_started) * 1000
        phase_metrics['readiness_compute_rows_changed_context_ms'] = changed_context_ms
        phase_metrics['readiness_compute_rows_changed_diagnostic_ms'] = changed_diagnostic_ms
        phase_metrics['readiness_compute_rows_reused_count'] = float(reused_rows)
    phase_started = phase_now()
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
    if phase_metrics is not None:
        phase_metrics['readiness_compute_units_ms'] = (monotonic() - phase_started) * 1000
    phase_started = phase_now()
    checkpoint()
    digest = readiness_manifest_digest(board_id=snapshot.facts.scope.board_id,
        spec_id=snapshot.facts.scope.spec_id, edition=snapshot.facts.scope.edition,
        captured_revision=captured_revision, source_snapshot_id=snapshot.snapshot_id,
        evaluator_version=EVALUATOR_VERSION,
        summaries=[summary.payload() for summary in summaries],
        units=[unit.row() for unit in units],
        diagnostics=[row.row() for row in diagnostics])
    if phase_metrics is not None:
        phase_metrics['readiness_compute_digest_ms'] = (monotonic() - phase_started) * 1000
    return ReadinessGeneration(snapshot.facts.scope.board_id, snapshot.facts.scope.spec_id,
        snapshot.facts.scope.edition, snapshot.spec_version, captured_revision,
        snapshot.snapshot_id, snapshot.evaluated_at, EVALUATOR_VERSION,
        summaries, units, diagnostics, digest)
