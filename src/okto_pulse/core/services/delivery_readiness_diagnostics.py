"""Read-only diagnostic composition over one authorized delivery snapshot.

No persistence, receipt authentication, source inspection or proof evaluation
occurs here. Concrete loading/permissions belong to the edition adapter.
"""
from collections import defaultdict
from dataclasses import dataclass, replace
import json

from okto_pulse.core.domain.delivery_assessment import ReadinessPhase, ReadinessReason, UnitPhaseAssessment, UnitState
from okto_pulse.core.domain.delivery_diagnostics import (
    ACTION_REGISTRY, ActionCandidate, ActionKind, CauseOccurrence, CauseOrigin,
    DiagnosticGraph, DiagnosticIntegrityError, assessment_diagnostic_units,
    build_diagnostic_graph, cause_identity, reason_definition,
)
from okto_pulse.core.domain.delivery_evidence import DeliveryBinding, DeliveryScope
from okto_pulse.core.domain.effective_delivery_coverage import EffectiveDeliveryContext
from okto_pulse.core.models.delivery_readiness import (
    ReadinessAction, ReadinessDetailQuery, ReadinessEvidenceRef, ReadinessFact,
    ReadinessIssue, ReadinessPinpoint, ReadinessUnitRef,
)


@dataclass(frozen=True, slots=True)
class DiagnosticOwner:
    entity_type: str
    entity_id: str
    owner_id: str | None
    visible: bool


@dataclass(frozen=True, slots=True)
class DiagnosticDetail:
    issue_id: str
    expected: tuple[ReadinessFact, ...]
    observed: tuple[ReadinessFact, ...]
    evidence_refs: tuple[ReadinessEvidenceRef, ...]
    pinpoints: tuple[ReadinessPinpoint, ...]
    implementation_card_ids: tuple[str, ...]
    test_card_ids: tuple[str, ...]
    owner: str | None = None
    owner_state: str = 'restricted'
    detail_state: str = 'available'


@dataclass(frozen=True, slots=True)
class NativeDeliveryDiagnostics:
    graph: DiagnosticGraph
    details: tuple[DiagnosticDetail, ...]
    scope: DeliveryScope
    card_id: str | None = None


def _unique(values):
    """DTO values are immutable; canonical serialization gives stable ordering."""
    by_key = {value.model_dump_json(): value for value in values}
    return tuple(by_key[key] for key in sorted(by_key))


def _index(values, identity):
    result = {}
    for value in values:
        key = identity(value)
        if key in result and result[key] != value:
            raise DiagnosticIntegrityError('readiness_diagnostic_identity_ambiguous')
        result[key] = value
    return result


def _digest(value):
    value = value.removeprefix('sha256:')
    if len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
        raise DiagnosticIntegrityError('readiness_diagnostic_digest_invalid')
    return value


def _pinpoint(entity_type, entity_id, **values):
    fields = dict(kind='entity', entity_type=entity_type, entity_id=entity_id,
        source_ref=None, revision=None, relative_path=None, qualified_symbol=None,
        line_start=None, line_end=None, target_id=None, execution_id=None,
        record_id=None, observation_id=None, precision='entity_only', origin='native_entity')
    return ReadinessPinpoint(**(fields | values))


class _SnapshotIndex:
    def __init__(self, snapshot):
        self.scope = snapshot.scope
        self.context = snapshot.effective_context
        self.implementations = _index((f for f in snapshot.implementations if f.scope == self.scope), lambda f: f.id)
        self.rejections = _index((f for f in snapshot.rejected_records if f.scope == self.scope), lambda f: f.record_id)
        self.waivers = _index((f for f in snapshot.waivers if f.scope == self.scope), lambda f: f.id)
        context = self.context if isinstance(self.context, EffectiveDeliveryContext) else None
        self.obligations = _index(context.inventory.rows if context else (), lambda r: r.binding)
        self.scoped_implementations = _index(context.implementations if context else (), lambda r: r.fact.id)
        self.plans = _index(context.required_scenarios or () if context else (), lambda r: r.scenario_id)
        self.inspections = _index(context.decision_inspections if context else (), lambda r: r.decision_id)
        self.runs = defaultdict(list)
        native = context.native_verification if context else None
        for run in native.runs if native else ():
            if run.scope == self.scope:
                self.runs[run.scenario_id].append(run)
        self.cards = {c.card_id for row in self.obligations.values() for c in row.contributions}
        self.cards.update(f.card_id for f in self.implementations.values())
        self.source_ids = self.cards | set(self.implementations) | set(self.rejections) | set(self.waivers)
        self.source_ids.update(run.receipt_id for values in self.runs.values() for run in values if run.receipt_id)
        self.source_ids.update(identity for row in self.inspections.values() for identity in row.record_ids)
        self.source_ids.update(identity for fact in snapshot.tests if fact.scope == self.scope
            for identity in (fact.id, fact.receipt_id))

    def details(self, unit, phase, reason):
        if set(reason.source_ids) - self.source_ids:
            raise DiagnosticIntegrityError('readiness_diagnostic_reference_unresolved')
        bindings = reason.bindings or ((DeliveryBinding(unit.ref, unit.semantic_sha256),)
            if unit and unit.unit == 'obligation' else ())
        if any(binding not in self.obligations for binding in bindings):
            raise DiagnosticIntegrityError('readiness_diagnostic_snapshot_mismatch')
        selected_ids = set(reason.implementation_ids)
        if phase == ReadinessPhase.IMPLEMENTATION:
            selected_ids.update(identity for identity in reason.source_ids if identity in self.implementations)
        if set(reason.implementation_ids) - set(self.implementations):
            raise DiagnosticIntegrityError('readiness_diagnostic_reference_unresolved')
        facts = tuple(self.implementations[identity] for identity in sorted(selected_ids))
        if reason.code != ReadinessReason.SCOPE_CONTRACT_CHANGED and any(
            not set(bindings) & set(fact.bindings) for fact in facts):
            raise DiagnosticIntegrityError('readiness_diagnostic_snapshot_mismatch')
        expected, observed, evidence, pinpoints = [], [], [], []
        implementation_cards = {identity for identity in reason.source_ids if identity in self.cards}
        implementation_cards.update(f.card_id for f in facts)
        if unit:
            entity_type, _, identity = unit.ref.partition(':') if unit.unit == 'obligation' else ('test_scenario', '', unit.ref)
            identity = identity or unit.ref
            pinpoints.append(_pinpoint(entity_type, identity,
                kind='scenario' if unit.unit == 'scenario' else 'entity'))
        else:
            pinpoints.append(_pinpoint('spec', self.scope.spec_id))
        for binding in bindings:
            expected.append(ReadinessFact(obligation_ref=binding.obligation_ref, semantic_sha256=binding.semantic_sha256,
                outcome='complete' if phase == ReadinessPhase.IMPLEMENTATION else 'passed'))
        proofs = []
        for fact in facts:
            evidence.append(ReadinessEvidenceRef(kind='record', id=fact.id, source_ref=None, revision=None))
            if reason.code == ReadinessReason.SCOPE_CONTRACT_CHANGED:
                refs = {binding.obligation_ref for binding in bindings}
                for binding in bindings:
                    expected.extend(ReadinessFact(obligation_ref=binding.obligation_ref,
                        semantic_sha256=binding.semantic_sha256, scope_sha256=c.scope_sha256, card_id=c.card_id)
                        for c in self.obligations[binding].contributions if c.card_id == fact.card_id)
                scoped = self.scoped_implementations.get(fact.id)
                observed.extend(ReadinessFact(record_id=fact.id, card_id=fact.card_id,
                    obligation_ref=b.obligation_ref, semantic_sha256=b.semantic_sha256) for b in fact.bindings
                    if b.obligation_ref in refs)
                if scoped:
                    observed.extend(ReadinessFact(record_id=fact.id, card_id=fact.card_id,
                        obligation_ref=s.binding.obligation_ref, semantic_sha256=s.binding.semantic_sha256,
                        scope_sha256=s.scope_sha256) for s in scoped.scopes if s.binding.obligation_ref in refs)
            selected = {identity for c in fact.contributions if c.binding in bindings for identity in c.execution_ids}
            if reason.code == ReadinessReason.SCOPE_CONTRACT_CHANGED:
                selected.update(identity for c in fact.contributions if any(
                    b.obligation_ref == c.binding.obligation_ref for b in bindings) for identity in c.execution_ids)
            for proof in fact.executions:
                if proof.execution_id not in selected:
                    continue
                proofs.append(proof)
                if phase == ReadinessPhase.VERIFICATION:
                    expected.append(ReadinessFact(source_ref=proof.source_ref or None, revision=proof.result_revision or None,
                        execution_id=proof.execution_id, target_id=proof.target_id, record_id=fact.id, card_id=fact.card_id,
                        outcome='passed'))
                else:
                    observed.append(ReadinessFact(source_ref=proof.source_ref or None, revision=proof.result_revision or None,
                        execution_id=proof.execution_id, target_id=proof.target_id, record_id=fact.id, card_id=fact.card_id,
                        native_reason_codes=proof.rejection_reasons))
                evidence.append(ReadinessEvidenceRef(kind='execution', id=proof.execution_id,
                    source_ref=proof.source_ref or None, revision=proof.result_revision or None))
                location = dict(kind='symbol' if proof.symbol else 'target', target_id=proof.target_id,
                    execution_id=proof.execution_id, record_id=fact.id, source_ref=proof.source_ref or None,
                    revision=proof.result_revision or None, relative_path=proof.relative_path or None,
                    qualified_symbol=proof.symbol, precision='symbol' if proof.symbol else 'target', origin='execution_receipt')
                try:
                    pinpoints.append(_pinpoint('card', fact.card_id, **location))
                except ValueError:
                    # A malformed/host-local path is not exposed as a portable
                    # code location. The native entity and evidence stay usable.
                    pinpoints.append(_pinpoint('card', fact.card_id, target_id=proof.target_id,
                        execution_id=proof.execution_id, record_id=fact.id))
        scenarios = reason.scenario_ids or ((unit.ref,) if unit and unit.unit == 'scenario' else ())
        runs, test_cards = [], set()
        for scenario in scenarios:
            plan = self.plans.get(scenario)
            test_cards.update(plan.test_card_ids if plan else ())
            expected.append(ReadinessFact(scenario_id=scenario, verification_method=plan.method if plan else None, outcome='passed'))
            pinpoints.append(_pinpoint('test_scenario', scenario, kind='scenario'))
            for run in self.runs.get(scenario, ()):
                if reason.source_ids and run.receipt_id not in reason.source_ids:
                    continue
                runs.append(run)
                observed.append(ReadinessFact(scenario_id=scenario, receipt_id=run.receipt_id,
                    verification_method=run.method, outcome=run.outcome, native_reason_codes=run.native_reason_codes))
                if run.receipt_id:
                    evidence.append(ReadinessEvidenceRef(kind='receipt', id=run.receipt_id, source_ref=None, revision=None))
                for source in run.sources:
                    observed.append(ReadinessFact(source_ref=source.source_ref, revision=source.revision,
                        scenario_id=scenario, receipt_id=run.receipt_id, verification_method=run.method, outcome=run.outcome))
                for observation in run.observations:
                    if reason.criterion_ids and observation.criterion_id not in reason.criterion_ids:
                        continue
                    expected.append(ReadinessFact(scenario_id=scenario, criterion_id=observation.criterion_id,
                        observation_id=observation.observation_id, text=observation.expected))
                    observed.append(ReadinessFact(scenario_id=scenario, criterion_id=observation.criterion_id,
                        observation_id=observation.observation_id, text=observation.observed, outcome=observation.outcome))
                    pinpoints.append(_pinpoint('test_scenario', scenario, kind='observation',
                        record_id=run.receipt_id, observation_id=observation.observation_id, origin='verification_report'))
        for identity in reason.source_ids:
            if identity in self.rejections:
                evidence.append(ReadinessEvidenceRef(kind='record', id=identity, source_ref=None, revision=None))
                observed.append(ReadinessFact(record_id=identity, native_reason_codes=self.rejections[identity].reason_codes))
            if identity in self.waivers:
                evidence.append(ReadinessEvidenceRef(kind='exception', id=identity, source_ref=None, revision=None))
        decision = next((self.obligations[b] for b in bindings if self.obligations[b].family == 'decision'), None)
        if decision:
            identity = decision.binding.obligation_ref.removeprefix('decision:')
            inspection = self.inspections.get(identity)
            if inspection:
                observed.append(ReadinessFact(obligation_ref=decision.binding.obligation_ref,
                    scope_sha256=inspection.scope_sha256, outcome=inspection.status))
                evidence.extend(ReadinessEvidenceRef(kind='inspection', id=key, source_ref=None, revision=None)
                    for key in inspection.record_ids if not reason.source_ids or key in reason.source_ids)
            plan = decision.decision_plan
            if plan and plan.verification and plan.verification.inspection:
                expected.append(ReadinessFact(obligation_ref=decision.binding.obligation_ref,
                    text=plan.verification.inspection.condition))
        if not observed:
            observed.append(ReadinessFact(native_reason_codes=reason.native_reason_codes,
                outcome='unavailable' if reason.code in {ReadinessReason.ASSESSMENT_UNAVAILABLE, ReadinessReason.SCOPE_UNAVAILABLE} else 'missing'))
        expected_bases = tuple(sorted({(p.source_ref, p.result_revision) for p in proofs if p.source_ref and p.result_revision}))
        observed_bases = tuple(sorted({(s.source_ref, s.revision) for run in runs for s in run.sources}))
        if phase == ReadinessPhase.IMPLEMENTATION:
            expected_bases, observed_bases = (), expected_bases
        matching = {p.execution_id for p in proofs if set(p.rejection_reasons) & set(reason.native_reason_codes)}
        if scenarios:
            origin = CauseOrigin('scenario', scenarios[0] if len(scenarios) == 1 else unit.ref, expected_bases, observed_bases)
        elif len(matching) == 1:
            origin = CauseOrigin('execution', next(iter(matching)), expected_bases, observed_bases)
        elif len(reason.source_ids) == 1:
            origin = CauseOrigin('native_fact', reason.source_ids[0], expected_bases, observed_bases)
        else:
            origin = CauseOrigin(unit.unit if unit else 'spec', unit.ref if unit else self.scope.spec_id,
                expected_bases, observed_bases)
        detail = DiagnosticDetail('', _unique(expected), _unique(observed), _unique(evidence), _unique(pinpoints),
            tuple(sorted(implementation_cards)), tuple(sorted(test_cards)))
        return detail, origin, bindings, tuple(proofs), tuple(runs), decision


def _verification_procedure_arguments(index, scenario_ids, procedures, proofs):
    """Return authored procedures and exact evaluated bases, never commands.

    Procedures are authorized metadata from the same read snapshot. Missing
    text remains missing; an observation or another scenario cannot replace it.
    """
    scenarios, missing = [], []
    for identity in sorted(set(scenario_ids)):
        procedure, plan = procedures.get(identity), index.plans.get(identity)
        if procedure is None:
            missing.append(f'scenarios.{identity}')
            continue
        if (plan is None or procedure.verification_method != plan.method
                or set(procedure.criterion_ids) != set(plan.criterion_ids)):
            raise DiagnosticIntegrityError('readiness_diagnostic_snapshot_mismatch')
        item = dict(scenario_id=identity, verification_method=plan.method,
            criterion_ids=sorted(set(plan.criterion_ids)))
        for field in ('title', 'given', 'when', 'then', 'notes'):
            value = getattr(procedure, field)
            if value is not None:
                item[field] = value
            if field in {'given', 'when', 'then'} and (not isinstance(value, str) or not value.strip()):
                missing.append(f'scenarios.{identity}.{field}')
        scenarios.append(item)
    bases = _index(proofs, lambda p: p.execution_id)
    return dict(spec_edition=index.scope.edition, scenarios=scenarios,
        implementation_bases=[dict(execution_id=p.execution_id, target_id=p.target_id,
            target_revision=p.target_revision, source_ref=p.source_ref, revision=p.result_revision)
            for _, p in sorted(bases.items())]), missing


def _actions(index, assessment, unit, phase, reason, detail, origin, bindings, proofs, runs, decision,
             permissions, card_versions, procedures):
    definition = reason_definition(reason.code)
    if not definition.actions:
        return ()
    if decision and ActionKind.REVIEW_DECISION in definition.actions:
        kind = ActionKind.REVIEW_DECISION
    else:
        kind = {
            ReadinessReason.VERIFICATION_BASE_MISMATCH: ActionKind.EXECUTE_VERIFICATION,
            ReadinessReason.VERIFICATION_FAILED: ActionKind.IMPLEMENT_CONTRIBUTION,
            ReadinessReason.VERIFICATION_METHOD_UNSUPPORTED: ActionKind.RESTORE_EVALUATION,
        }.get(reason.code, definition.actions[0])
    action = ACTION_REGISTRY[kind]
    scenario_ids = reason.scenario_ids or ((unit.ref,) if unit and unit.unit == 'scenario' else ())
    plans = tuple(index.plans[s] for s in scenario_ids if s in index.plans)
    preconditions = set()
    if bindings and all(index.obligations[b].contributions for b in bindings):
        preconditions.add('native_contribution')
    if plans and len(plans) == len(scenario_ids) and all(p.complete for p in plans):
        preconditions.add('native_scenario')
    if reason.implementation_ids and proofs and reason.code != ReadinessReason.VERIFICATION_DEPENDENCY_PENDING:
        # The implementation IDs came from the evaluator's exact join. This
        # does not admit a candidate merely because a receipt looks plausible.
        preconditions.add('implementation_base')
    if reason.code == ReadinessReason.VERIFICATION_ASSOCIATION_MISSING and runs:
        preconditions.add('authenticated_current_result')
    if decision and decision.decision_plan and decision.decision_plan.complete:
        preconditions.add('native_decision_plan')
    verification = kind in {ActionKind.EXECUTE_VERIFICATION, ActionKind.ASSOCIATE_VERIFICATION}
    responsible = detail.test_card_ids if verification else detail.implementation_card_ids
    card_id = assessment.card_id if assessment.card_id in responsible else responsible[0] if len(responsible) == 1 else None
    known = {'board_id': index.scope.board_id, 'spec_id': index.scope.spec_id}
    if kind == ActionKind.RESTORE_EVALUATION:
        known['view'] = 'readiness'
        card_id = assessment.card_id
    if card_id:
        known['card_id'] = card_id
    missing = []
    if kind == ActionKind.EXECUTE_VERIFICATION:
        execution_context, missing = _verification_procedure_arguments(index, scenario_ids, procedures, proofs)
        known.update(execution_context)
    if kind == ActionKind.ASSOCIATE_VERIFICATION:
        known['evidence'] = {'kind': 'test', 'expected_spec_edition': index.scope.edition,
            'obligation_refs': sorted({b.obligation_ref for b in bindings}),
            'implementation_ids': sorted(set(reason.implementation_ids))}
        if len(scenario_ids) == 1:
            known['evidence']['scenario_id'] = scenario_ids[0]
        else:
            missing.append('evidence.scenario_id')
        if type(card_versions.get(card_id)) is int and card_versions[card_id] >= 1:
            known['evidence']['expected_card_version'] = card_versions[card_id]
        else:
            missing.append('evidence.expected_card_version')
        missing.extend(('evidence.justification', 'evidence.idempotency_key'))
        if not bindings:
            missing.append('evidence.obligation_refs')
        if not reason.implementation_ids:
            missing.append('evidence.implementation_ids')
    context = {origin.identity, *reason.source_ids, *reason.implementation_ids, *scenario_ids, *reason.criterion_ids}
    if kind == ActionKind.REFRESH_IMPLEMENTATION_PROOF and origin.kind == 'execution':
        # One reobservation repairs the same execution used by several record
        # associations. The issue detail still preserves every record ID.
        context.difference_update(reason.source_ids)
        context.add(origin.identity)
    context.update(p.execution_id for p in proofs)
    context.update(p.target_id for p in proofs)
    context.update(p.source_ref for p in proofs if p.source_ref)
    context.update(p.result_revision for p in proofs if p.result_revision)
    if kind in {ActionKind.IMPLEMENT_CONTRIBUTION, ActionKind.COMPLETE_CONTRIBUTION, ActionKind.RESOLVE_SCOPE}:
        context.update(b.obligation_ref for b in bindings)
    if reason.code == ReadinessReason.SCOPE_CONTRACT_CHANGED:
        context.update(digest for item in (*detail.expected, *detail.observed)
            for digest in (item.semantic_sha256, item.scope_sha256) if digest)
    context.update(p.method for p in plans if p.method)
    supported = index.context.admitted_methods if isinstance(index.context, EffectiveDeliveryContext) else None
    capable = kind != ActionKind.EXECUTE_VERIFICATION or (
        supported is not None and bool(plans) and all(p.method in supported for p in plans))
    return (ActionCandidate(kind, arguments_json=json.dumps({k: v for k, v in known.items() if k in action.argument_fields}),
        context_refs=tuple(sorted(context)), responsible_card_ids=responsible,
        missing_inputs=tuple(missing), permission_allowed=permissions.get(action.permission) if action.permission else None,
        capability_available=capable, satisfied_preconditions=tuple(sorted(preconditions))),)


def compose_delivery_diagnostics(assessment, snapshot, *, spec_assessment=None, permissions=None, owners=(),
                                 card_versions=None, procedures=(), native_lifecycle=None):
    """Compose already evaluated native facts, without per-unit queries/evaluation.

    Test views can receive the shared Spec assessment to explain implementation
    predecessors. The adapter must load all arguments in the same authorized
    read snapshot; native mutation authorization is always checked again.
    """
    if assessment.scope != snapshot.scope or spec_assessment is not None and spec_assessment.scope != snapshot.scope:
        raise DiagnosticIntegrityError('readiness_diagnostic_scope_invalid')
    index = _SnapshotIndex(snapshot)
    owner_index = _index(owners, lambda o: (o.entity_type, o.entity_id))
    procedure_index = _index(procedures, lambda p: p.id)
    permissions, card_versions = permissions or {}, card_versions or {}
    occurrences, details, relations, required = [], defaultdict(list), [], set()

    def add(unit, phase, result, reason):
        detail, origin, bindings, proofs, runs, decision = index.details(unit, phase, reason)
        actions = _actions(index, assessment, unit, phase, reason, detail, origin, bindings, proofs, runs, decision,
            permissions, card_versions, procedure_index)
        occurrence = CauseOccurrence(reason.code, phase, result.intrinsic_state, origin,
            (unit,) if unit else (), actions, reason.native_reason_codes, reason.criterion_ids, reason.scenario_ids)
        identity = cause_identity(snapshot.scope, occurrence)
        owner = owner_index.get((origin.kind, origin.identity))
        if owner is None and len(detail.implementation_card_ids) + len(detail.test_card_ids) == 1:
            owner = owner_index.get(('card', (*detail.implementation_card_ids, *detail.test_card_ids)[0]))
        detail = replace(detail, issue_id=identity, owner=owner.owner_id if owner and owner.visible else None,
            owner_state='restricted' if owner is None or not owner.visible else 'known' if owner.owner_id else 'unassigned')
        occurrences.append(occurrence)
        details[identity].append(detail)
        relations.append((identity, unit, phase, reason, bindings, detail.implementation_card_ids, decision))

    spec_rows = {row.binding: row for row in spec_assessment.obligations} if spec_assessment else {}
    for unit, phase, result in assessment_diagnostic_units(assessment):
        if unit.spec_edition != snapshot.scope.edition:
            raise DiagnosticIntegrityError('readiness_diagnostic_scope_invalid')
        if result.state in {UnitState.SATISFIED, UnitState.NOT_APPLICABLE}:
            continue
        if not result.reasons:
            raise DiagnosticIntegrityError('readiness_unit_cause_missing')
        required.add((unit, phase))
        for reason in result.reasons:
            add(unit, phase, result, reason)
            if unit.unit == 'scenario' and reason.code == ReadinessReason.VERIFICATION_DEPENDENCY_PENDING:
                needed_cards = {key for key in reason.source_ids if key in index.cards}
                for binding in reason.bindings:
                    source = spec_rows.get(binding)
                    if source and source.implementation.state not in {UnitState.SATISFIED, UnitState.NOT_APPLICABLE}:
                        for root in source.implementation.reasons:
                            root_sources = tuple(key for key in root.source_ids if not needed_cards or key in needed_cards
                                or key in index.implementations and index.implementations[key].card_id in needed_cards
                                or key in index.rejections and index.rejections[key].card_id in needed_cards)
                            if root.source_ids and not root_sources:
                                continue
                            add(unit, ReadinessPhase.IMPLEMENTATION, source.implementation,
                                replace(root, source_ids=root_sources, bindings=(binding,)))
    phases = (ReadinessPhase.VERIFICATION,) if assessment.card_type == 'test' else (
        ReadinessPhase.IMPLEMENTATION, ReadinessPhase.VERIFICATION)
    for reason in assessment.reasons:
        for phase in phases:
            add(None, phase, UnitPhaseAssessment(UnitState.UNKNOWN, UnitState.UNKNOWN, None, None), reason)
    if not occurrences and (not assessment.completeness.population_known or not assessment.completeness.evaluation_complete):
        raise DiagnosticIntegrityError('readiness_assessment_cause_missing')
    by_binding = defaultdict(dict)
    by_ref = defaultdict(set)
    for identity, unit, phase, reason, bindings, cards, decision in relations:
        for binding in bindings:
            if phase == ReadinessPhase.IMPLEMENTATION:
                by_binding[binding][identity] = set(cards)
            if phase == ReadinessPhase.VERIFICATION:
                by_ref[binding.obligation_ref].add(identity)
    edges = set()
    for identity, unit, phase, reason, bindings, cards, decision in relations:
        if reason.code == ReadinessReason.VERIFICATION_DEPENDENCY_PENDING:
            for binding in bindings:
                edges.update((identity, parent) for parent, parent_cards in by_binding[binding].items()
                    if parent != identity and (not cards or not parent_cards or set(cards) & parent_cards))
        if decision and decision.decision_plan and decision.decision_plan.verification:
            for reference in decision.decision_plan.verification.obligation_refs:
                edges.update((identity, parent) for parent in by_ref[reference] if parent != identity)
    if native_lifecycle and native_lifecycle.blocking_codes:
        entity, identity = ('card', assessment.card_id) if assessment.card_id else ('spec', snapshot.scope.spec_id)
        occurrence = CauseOccurrence(ReadinessReason.LIFECYCLE_GATE_PENDING, ReadinessPhase.LIFECYCLE,
            UnitState.MISSING, CauseOrigin(entity, identity), (),
            actions=(ActionCandidate(ActionKind.RESOLVE_NATIVE_GATE,
                context_refs=(f'{entity}:{identity}',)),),
            native_reason_codes=native_lifecycle.blocking_codes, blocks_proof=False,
            blocks_target_transition=True)
        key = cause_identity(snapshot.scope, occurrence)
        occurrences.append(occurrence)
        details[key].append(DiagnosticDetail(key, (), (), (), (), (), ()))
    elif (native_lifecycle and native_lifecycle.check_status == 'not_evaluated'
          and not native_lifecycle.completion_recorded):
        entity, identity = ('card', assessment.card_id) if assessment.card_id else ('spec', snapshot.scope.spec_id)
        needs_input = bool(native_lifecycle.required_inputs)
        action = (ActionCandidate(ActionKind.COMPLETE_LIFECYCLE,
            context_refs=(f'{entity}:{identity}',), missing_inputs=native_lifecycle.required_inputs)
            if needs_input else ActionCandidate(ActionKind.RESTORE_EVALUATION,
                arguments_json=json.dumps({'board_id': snapshot.scope.board_id,
                    'spec_id': snapshot.scope.spec_id, 'view': 'readiness',
                    **({'card_id': assessment.card_id} if assessment.card_id else {})}),
                context_refs=(f'{entity}:{identity}',),
                permission_allowed=permissions.get('code_traceability.evidence.read')))
        occurrence = CauseOccurrence(
            ReadinessReason.LIFECYCLE_CARD_OPEN if needs_input else ReadinessReason.ASSESSMENT_UNAVAILABLE,
            ReadinessPhase.LIFECYCLE, UnitState.MISSING if needs_input else UnitState.UNKNOWN,
            CauseOrigin(entity, identity), (), actions=(action,),
            native_reason_codes=native_lifecycle.reason_codes, blocks_proof=False, blocks_target_transition=None)
        key = cause_identity(snapshot.scope, occurrence)
        occurrences.append(occurrence)
        details[key].append(DiagnosticDetail(key, (), (), (), (), (), ()))
    graph = build_diagnostic_graph(snapshot.scope, tuple(occurrences), dependency_edges=tuple(edges), required_units=tuple(required))
    merged = []
    for cause in graph.causes:
        rows = details[cause.id]
        owner_values = {(row.owner, row.owner_state) for row in rows}
        owner, owner_state = next(iter(owner_values)) if len(owner_values) == 1 else (None, 'restricted')
        merged.append(DiagnosticDetail(cause.id,
            _unique(v for row in rows for v in row.expected), _unique(v for row in rows for v in row.observed),
            _unique(v for row in rows for v in row.evidence_refs), _unique(v for row in rows for v in row.pinpoints),
            tuple(sorted({v for row in rows for v in row.implementation_card_ids})),
            tuple(sorted({v for row in rows for v in row.test_card_ids})), owner, owner_state))
    return NativeDeliveryDiagnostics(graph, tuple(merged), snapshot.scope, assessment.card_id)


def render_diagnostic_unit(unit):
    return ReadinessUnitRef(unit=unit.unit, ref=unit.ref, semantic_sha256=_digest(unit.semantic_sha256),
        spec_edition=unit.spec_edition, card_id=unit.card_id or None)


def render_diagnostic_issue(cause, detail, *, scope, card_id, snapshot_id):
    """Bounded issue summary. Full arrays remain in DiagnosticDetail for paging."""
    def bounded(field, count, values):
        return {field: values[:10], count: len(values), field + '_truncated': len(values) > 10}
    units = tuple(render_diagnostic_unit(u) for u in cause.units)
    impacts = set(cause.phases) - {ReadinessPhase.LIFECYCLE}
    impact = 'both' if len(impacts) == 2 else next(iter(impacts)) if impacts else 'none'
    if units and all(unit.unit == 'scenario' for unit in units):
        impact = 'verification'  # Test implementation remains N/A, including root-cause detail.
    values = {}
    for field, count, entries in (
        ('affected_units', 'affected_unit_count', units), ('criterion_ids', 'criterion_count', cause.criterion_ids),
        ('scenario_ids', 'scenario_count', cause.scenario_ids),
        ('implementation_card_ids', 'implementation_card_count', detail.implementation_card_ids),
        ('test_card_ids', 'test_card_count', detail.test_card_ids),
        ('evidence_refs', 'evidence_count', detail.evidence_refs), ('pinpoints', 'pinpoint_count', detail.pinpoints),
        ('expected', 'expected_count', detail.expected), ('observed', 'observed_count', detail.observed)):
        values.update(bounded(field, count, entries))
    return ReadinessIssue(id=cause.id, code=cause.code, phase=cause.phases[0], score_impact=impact,
        intrinsic_state=cause.intrinsic_state, blocks_proof=cause.blocks_proof,
        blocks_target_transition=cause.blocks_target_transition, message_code=cause.message_code,
        message_params={'subject': cause.origin.identity}, message=cause.message, native_reason_codes=cause.native_reason_codes,
        owner=detail.owner, owner_state=detail.owner_state, detail_state=detail.detail_state,
        root_cause_id=cause.root_cause_id, depends_on_issue_ids=cause.depends_on_issue_ids, action_ids=cause.action_ids,
        detail_query=ReadinessDetailQuery(board_id=scope.board_id, spec_id=scope.spec_id, card_id=card_id,
            snapshot_id=snapshot_id, issue_id=cause.id), **values)


def render_diagnostic_action(action):
    return ReadinessAction(id=action.id, kind=action.kind, message_code=action.message_code,
        message_params={}, message=action.message, issue_ids=action.issue_ids, state=action.state,
        depends_on_action_ids=action.depends_on_action_ids, responsible_card_ids=action.responsible_card_ids,
        permission=action.permission, execution_surface=action.execution_surface, pulse_tool=action.pulse_tool,
        arguments=json.loads(action.arguments_json), missing_inputs=action.missing_inputs,
        context_refs=action.context_refs, resolves_unit_count=action.resolves_unit_count)
