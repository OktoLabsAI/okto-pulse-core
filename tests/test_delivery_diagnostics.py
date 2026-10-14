"""P04: deterministic causes and executable guidance, independent of scores."""
import asyncio
from dataclasses import replace
from itertools import permutations
import json

import pytest

from okto_pulse.core.domain.delivery_assessment import ReadinessPhase as Phase, ReadinessReason as Reason, UnitState
from okto_pulse.core.domain.delivery_diagnostics import (
    ACTION_REGISTRY, REASON_REGISTRY, ActionCandidate, ActionKind, ActionState,
    CauseOccurrence, CauseOrigin, DiagnosticIntegrityError, DiagnosticUnit,
    DiagnosticResolution, build_diagnostic_graph, cause_identity, diagnose_delivery_assessment,
    reason_definition, validate_diagnostic_registry,
)
from okto_pulse.core.domain.delivery_evidence import DeliveryScope


SCOPE = DeliveryScope('board', 'spec', 2)


def unit(name='fr-1'):
    return DiagnosticUnit('obligation', name, 'a' * 64, 2)


def stale(identity='execution-1', units=None):
    return CauseOccurrence(Reason.IMPLEMENTATION_PROOF_REVOKED, Phase.IMPLEMENTATION,
        UnitState.STALE, CauseOrigin('execution', identity), units or (unit(),),
        (ActionCandidate(ActionKind.REFRESH_IMPLEMENTATION_PROOF, context_refs=(identity,)),),
        native_reason_codes=('delivery_investigation_receipt_revoked',))


def scenario(identity, *, pending=False):
    return CauseOccurrence(Reason.VERIFICATION_DEPENDENCY_PENDING if pending else Reason.VERIFICATION_FAILED,
        Phase.VERIFICATION, UnitState.MISSING if pending else UnitState.FAILED,
        CauseOrigin('scenario', identity), (unit(identity),),
        (ActionCandidate(ActionKind.EXECUTE_VERIFICATION, context_refs=(identity,)),), scenario_ids=(identity,))


def test_closed_registry_and_unknown_reason_fail_explicitly():
    assert len(REASON_REGISTRY) == len(Reason) == 26
    assert len(ACTION_REGISTRY) == len(ActionKind) == 14
    validate_diagnostic_registry()
    with pytest.raises(DiagnosticIntegrityError, match='readiness_reason_unmapped'):
        reason_definition('unmapped_native_failure')
    with pytest.raises(DiagnosticIntegrityError, match='readiness_tool_unregistered'):
        validate_diagnostic_registry(set())


def test_action_tools_and_argument_names_exist_in_live_registry_and_docs():
    from okto_pulse.core.mcp import server
    from okto_pulse.core.mcp.tools_catalog_generator import tool_doc_index
    tools = asyncio.run(server.mcp.get_tools())
    validate_diagnostic_registry(set(tools))
    docs = tool_doc_index()
    for definition in ACTION_REGISTRY.values():
        if definition.pulse_tool:
            assert definition.pulse_tool in docs
            schema = tools[definition.pulse_tool].parameters
            assert set(definition.argument_fields) <= set(schema['properties'])
            assert set(schema.get('required', ())) <= set(definition.required_arguments)
        if definition.execution_surface == 'external_agent':
            assert definition.pulse_tool is None


def test_d02_one_revoked_receipt_has_five_impacts_one_action():
    rows = tuple(stale(units=(unit(f'fr-{i}'),)) for i in range(5))
    graph = build_diagnostic_graph(SCOPE, rows)
    assert len(graph.causes) == len(graph.actions) == 1
    assert len(graph.causes[0].units) == graph.actions[0].resolves_unit_count == 5
    assert graph.causes[0].native_reason_codes == ('delivery_investigation_receipt_revoked',)


def test_distinct_scenarios_and_exact_bases_do_not_collapse():
    a = stale()
    b = replace(a, origin=replace(a.origin, expected_bases=(('repo', 'revision-b'),)))
    graph = build_diagnostic_graph(SCOPE, (scenario('s-1'), scenario('s-2'), a, b))
    assert len(graph.causes) == 4


def test_d06_input_order_does_not_change_ids_messages_or_actions():
    rows = (stale(units=(unit('fr-2'),)), stale(), scenario('s-1'), scenario('s-2'))
    expected = build_diagnostic_graph(SCOPE, rows)
    for permutation in permutations(rows):
        assert build_diagnostic_graph(SCOPE, permutation) == expected


def test_d04_one_base_refresh_precedes_five_scenarios():
    root = stale()
    children = tuple(scenario(f's-{i}', pending=True) for i in range(5))
    edges = tuple((cause_identity(SCOPE, child), cause_identity(SCOPE, root)) for child in children)
    graph = build_diagnostic_graph(SCOPE, (*children, root), dependency_edges=edges)
    refresh, *verification = graph.actions
    assert refresh.kind == ActionKind.REFRESH_IMPLEMENTATION_PROOF
    assert refresh.state == ActionState.READY
    assert graph.next_action_ids == (refresh.id,)
    assert all(a.state == ActionState.WAITING_DEPENDENCY for a in verification)
    assert all(a.depends_on_action_ids == (refresh.id,) for a in verification)
    assert all(c.root_cause_id == cause_identity(SCOPE, root) for c in graph.causes)


def test_d05_exact_cycle_does_not_mark_downstream_as_cycle():
    a, b, child = (stale(name) for name in ('a', 'b', 'child'))
    aid, bid, cid = (cause_identity(SCOPE, row) for row in (a, b, child))
    edges = ((aid, bid), (bid, aid), (cid, bid))
    graph = build_diagnostic_graph(SCOPE, (child, b, a), dependency_edges=edges)
    assert set(graph.cycle_edges) == {(aid, bid), (bid, aid)}
    assert len(graph.action_cycle_edges) == 2
    assert not any(action.state == ActionState.READY for action in graph.actions)
    assert len(graph.next_action_ids) == 2
    by_issue = {action.issue_ids[0]: action for action in graph.actions}
    assert by_issue[cid].state == ActionState.WAITING_DEPENDENCY
    assert by_issue[aid].state == by_issue[bid].state == ActionState.UNAVAILABLE


def test_shared_resolution_is_not_made_to_wait_for_itself():
    a = stale()
    b = replace(a, code=Reason.IMPLEMENTATION_PROOF_STALE)
    graph = build_diagnostic_graph(SCOPE, (a, b), dependency_edges=((cause_identity(SCOPE, b), cause_identity(SCOPE, a)),))
    assert len(graph.actions) == 1
    assert graph.actions[0].state == ActionState.READY
    assert graph.next_action_ids == (graph.actions[0].id,)


def test_d09_permissions_change_availability_not_facts_or_identity():
    row = replace(stale(), actions=(replace(stale().actions[0], permission_allowed=True),))
    allowed = build_diagnostic_graph(SCOPE, (row,))
    denied = build_diagnostic_graph(SCOPE, (replace(row, actions=(replace(row.actions[0], permission_allowed=False),)),))
    assert allowed.causes == denied.causes
    assert allowed.actions[0].id == denied.actions[0].id
    assert allowed.actions[0].resolves_unit_count == denied.actions[0].resolves_unit_count
    assert denied.actions[0].state == ActionState.FORBIDDEN
    assert denied.next_action_ids == (denied.actions[0].id,)


def test_missing_native_preconditions_never_recommend_immediate_retest():
    graph = build_diagnostic_graph(SCOPE, (scenario('s-1'),))
    assert graph.actions[0].state == ActionState.REQUIRES_INPUT
    assert set(graph.actions[0].missing_inputs) == {'native_scenario', 'implementation_base'}
    row = scenario('s-1')
    row = replace(row, actions=(replace(row.actions[0], satisfied_preconditions=('native_scenario', 'implementation_base')),))
    assert build_diagnostic_graph(SCOPE, (row,)).actions[0].state == ActionState.READY


@pytest.mark.parametrize('value', [None, '', '   ', {}, []])
def test_empty_required_argument_is_not_executable(value):
    action = ActionCandidate(ActionKind.RESTORE_EVALUATION,
        arguments_json=json.dumps({'board_id': value, 'spec_id': 'spec'}), permission_allowed=True)
    row = CauseOccurrence(Reason.ASSESSMENT_UNAVAILABLE, Phase.IMPLEMENTATION, UnitState.UNKNOWN,
        CauseOrigin('spec', 'spec'), (unit(),), (action,))
    result = build_diagnostic_graph(SCOPE, (row,)).actions[0]
    assert result.state == ActionState.REQUIRES_INPUT
    assert 'board_id' in result.missing_inputs


@pytest.mark.parametrize('arguments', ['[]', '{"shell":"rm -rf"}', '{"board_id":"a","board_id":"b"}',
    '{"evidence":{"x":1,"x":2}}', '{"evidence":{"x":NaN}}'])
def test_ambiguous_or_unmapped_arguments_fail_closed(arguments):
    with pytest.raises(DiagnosticIntegrityError, match='readiness_action_arguments_invalid'):
        ActionCandidate(ActionKind.ASSOCIATE_VERIFICATION, arguments_json=arguments)


def test_d01_unexplained_unit_is_explicit_integrity_failure():
    with pytest.raises(DiagnosticIntegrityError, match='readiness_unit_cause_missing'):
        build_diagnostic_graph(SCOPE, (stale(),), required_units=((unit('other'), Phase.IMPLEMENTATION),))
    build_diagnostic_graph(SCOPE, (stale(),), required_units=((unit(), Phase.IMPLEMENTATION),))


def test_missing_action_and_invalid_intrinsic_state_fail_explicitly():
    with pytest.raises(DiagnosticIntegrityError, match='readiness_diagnostic_action_missing'):
        build_diagnostic_graph(SCOPE, (replace(stale(), actions=()),))
    with pytest.raises(DiagnosticIntegrityError, match='readiness_intrinsic_state_invalid'):
        build_diagnostic_graph(SCOPE, (replace(stale(), intrinsic_state=UnitState.WAIVED),))


def test_unresolved_dependency_and_foreign_edition_fail_closed():
    row = stale()
    with pytest.raises(DiagnosticIntegrityError, match='readiness_dependency_unresolved'):
        build_diagnostic_graph(SCOPE, (row,), dependency_edges=((cause_identity(SCOPE, row), 'unknown'),))
    with pytest.raises(DiagnosticIntegrityError, match='readiness_diagnostic_scope_invalid'):
        build_diagnostic_graph(SCOPE, (replace(row, units=(replace(unit(), spec_edition=3),)),))


def test_next_actions_bounded_and_reuse_prioritized():
    associate = CauseOccurrence(Reason.IMPLEMENTATION_ASSOCIATION_MISSING, Phase.IMPLEMENTATION,
        UnitState.MISSING, CauseOrigin('execution', 'existing'), (unit(),),
        (ActionCandidate(ActionKind.ASSOCIATE_IMPLEMENTATION, permission_allowed=False),))
    graph = build_diagnostic_graph(SCOPE, (associate, *(stale(str(i)) for i in range(6))))
    assert len(graph.next_action_ids) == 3
    assert graph.actions[0].kind == ActionKind.ASSOCIATE_IMPLEMENTATION
    assert graph.actions[0].id not in graph.next_action_ids  # denied action does not hide runnable repairs


def test_waiver_exposes_caveat_without_suggesting_another_waiver():
    row = CauseOccurrence(Reason.APPLICABLE_OBLIGATION_WAIVED, Phase.VERIFICATION,
        UnitState.MISSING, CauseOrigin('exception', 'waiver-1'), (unit(),))
    graph = build_diagnostic_graph(SCOPE, (row,))
    assert len(graph.causes) == 1
    assert graph.actions == ()
    assert 'cobertura' in graph.causes[0].message


def test_actual_shared_assessment_reuses_existing_result_without_changing_scores():
    from okto_pulse.core.domain.delivery_assessment import assess_obligation_delivery, aggregate_delivery_metrics
    from test_delivery_obligation_assessment import snapshot, with_facts
    value = with_facts(snapshot(), tests=())
    assessment = assess_obligation_delivery(value)
    metrics = aggregate_delivery_metrics(assessment)
    graph = diagnose_delivery_assessment(assessment)
    assert metrics.implementation.percent == 100 and metrics.verification.percent == 0
    assert {c.code for c in graph.causes} == {Reason.VERIFICATION_ASSOCIATION_MISSING}
    assert {a.kind for a in graph.actions} == {ActionKind.ASSOCIATE_VERIFICATION}
    assert all(a.state == ActionState.UNAVAILABLE for a in graph.actions)  # authority not loaded
    assert all('evidence' in a.missing_inputs for a in graph.actions)
    assert len({u for c in graph.causes for u in c.units}) == metrics.verification.unresolved == 1
    assert aggregate_delivery_metrics(assessment) == metrics


def test_d01_actual_pending_units_have_causes_or_explicit_integrity_failure():
    from okto_pulse.core.domain.delivery_assessment import assess_obligation_delivery
    from test_delivery_obligation_assessment import snapshot, with_facts
    assessment = assess_obligation_delivery(with_facts(snapshot(), implementations=(), tests=()))
    graph = diagnose_delivery_assessment(assessment)
    assert {phase for c in graph.causes for phase in c.phases} == {Phase.IMPLEMENTATION, Phase.VERIFICATION}
    row = assessment.obligations[0]
    broken = replace(assessment, obligations=(replace(row, implementation=replace(row.implementation, reasons=())),))
    with pytest.raises(DiagnosticIntegrityError, match='readiness_unit_cause_missing'):
        diagnose_delivery_assessment(broken)


def test_actual_test_scenario_projection_uses_scenario_denominator():
    from okto_pulse.core.domain.delivery_assessment import assess_test_delivery, aggregate_delivery_metrics
    from test_delivery_obligation_assessment import snapshot, with_facts
    assessment = assess_test_delivery(with_facts(snapshot(), tests=()), card_id='test-card-1',
        scenario_ids=('scenario-ui', 'scenario-authorization'))
    graph = diagnose_delivery_assessment(assessment)
    units = {u for c in graph.causes for u in c.units}
    assert len(units) == aggregate_delivery_metrics(assessment).verification.unresolved == 2
    assert {u.unit for u in units} == {'scenario'}
    assert all(c.phases == (Phase.VERIFICATION,) for c in graph.causes)


def test_exact_native_resolution_can_enrich_only_its_assessed_reason():
    from okto_pulse.core.domain.delivery_assessment import assess_obligation_delivery
    from test_delivery_obligation_assessment import snapshot, with_facts
    assessment = assess_obligation_delivery(with_facts(snapshot(), tests=()))
    row = assessment.obligations[0]
    subject = DiagnosticUnit('obligation', row.binding.obligation_ref, row.binding.semantic_sha256, row.spec_edition)
    reason = row.verification.reasons[0]
    resolution = DiagnosticResolution(subject, Phase.VERIFICATION, reason,
        CauseOrigin('scenario', reason.scenario_ids[0]),
        (ActionCandidate(ActionKind.ASSOCIATE_VERIFICATION, context_refs=reason.source_ids,
            permission_allowed=True, satisfied_preconditions=('authenticated_current_result',)),))
    graph = diagnose_delivery_assessment(assessment, resolutions=(resolution,))
    enriched = [a for a in graph.actions if a.state == ActionState.REQUIRES_INPUT]
    assert len(enriched) == 1 and 'evidence' in enriched[0].missing_inputs
    with pytest.raises(DiagnosticIntegrityError, match='readiness_resolution_unrelated'):
        diagnose_delivery_assessment(assessment, resolutions=(replace(resolution, unit=replace(subject, ref='foreign')),))
    with pytest.raises(DiagnosticIntegrityError, match='readiness_resolution_ambiguous'):
        diagnose_delivery_assessment(assessment, resolutions=(resolution, replace(resolution, origin=CauseOrigin('scenario', 'other'))))


def test_unknown_population_is_explained_without_fabricating_units():
    from okto_pulse.core.domain.delivery_assessment import assess_obligation_delivery
    from test_delivery_obligation_assessment import snapshot
    assessment = assess_obligation_delivery(replace(snapshot(), effective_context=None))
    graph = diagnose_delivery_assessment(assessment)
    assert graph.causes and all(c.units == () for c in graph.causes)
    assert graph.next_action_ids
    with pytest.raises(DiagnosticIntegrityError, match='readiness_assessment_cause_missing'):
        diagnose_delivery_assessment(replace(assessment, reasons=()))


def test_unknown_native_rejection_cannot_silently_become_generic_missing_proof():
    from okto_pulse.core.domain.delivery_assessment import assess_obligation_delivery, ReadinessIntegrityError
    from test_delivery_obligation_assessment import snapshot, with_facts
    value = snapshot()
    original = value.effective_context.implementations[0]
    damaged = replace(original, fact=replace(original.fact, executions=(replace(original.fact.executions[0],
        current_accepted_execution=False, rejection_reasons=('future_unmapped_receipt_error',)),)))
    with pytest.raises(ReadinessIntegrityError, match='readiness_native_reason_unmapped'):
        assess_obligation_delivery(with_facts(value, implementations=(damaged,)))


def test_grouping_same_native_fact_is_independent_of_affected_unit():
    from okto_pulse.core.domain.delivery_assessment import (
        AssessmentCompleteness, DeliveryAssessment, FactualReason, ObligationAssessment, UnitPhaseAssessment,
    )
    from okto_pulse.core.domain.delivery_evidence import DeliveryBinding
    stale_phase = UnitPhaseAssessment(UnitState.STALE, UnitState.STALE, False, False,
        (FactualReason(Reason.IMPLEMENTATION_PROOF_REVOKED, source_ids=('one-record',)),))
    verified = UnitPhaseAssessment(UnitState.SATISFIED, UnitState.SATISFIED, True, True)
    rows = tuple(ObligationAssessment(DeliveryBinding(f'fr:{i}', 'a' * 64), 2, ('task',),
        stale_phase, verified) for i in range(5))
    graph = diagnose_delivery_assessment(DeliveryAssessment(SCOPE, AssessmentCompleteness(True, True, True, True), rows))
    assert len(graph.causes) == len(graph.actions) == 1
    assert graph.actions[0].resolves_unit_count == 5


def test_scc_cycle_edges_match_reachability_in_all_three_node_graphs():
    # Includes self loops, diamonds, cross edges and nodes downstream of cycles.
    from okto_pulse.core.domain.delivery_diagnostics import _topology
    nodes = {'a', 'b', 'c'}
    possible = tuple((a, b) for a in sorted(nodes) for b in sorted(nodes))
    for bits in range(1 << len(possible)):
        edges = tuple(edge for index, edge in enumerate(possible) if bits & (1 << index))
        reachable = {node: set() for node in nodes}
        for a, b in edges:
            reachable[a].add(b)
        for _ in nodes:
            for node in nodes:
                reachable[node].update(target for via in tuple(reachable[node]) for target in reachable[via])
        order, cycles = _topology(nodes, edges, {node: 0 for node in nodes})
        assert len(order) == len(set(order)) == 3
        assert set(cycles) == {(a, b) for a, b in edges if a in reachable[b]}
        if not cycles:
            assert all(order.index(b) < order.index(a) for a, b in edges)


def test_associating_old_base_requires_actual_compatible_result():
    row = replace(scenario('s-1'), code=Reason.VERIFICATION_BASE_MISMATCH, intrinsic_state=UnitState.STALE,
        actions=(ActionCandidate(ActionKind.ASSOCIATE_VERIFICATION, permission_allowed=True,
            satisfied_preconditions=('implementation_base',)),))
    graph = build_diagnostic_graph(SCOPE, (row,))
    assert graph.actions[0].state == ActionState.REQUIRES_INPUT
    assert 'authenticated_current_result' in graph.actions[0].missing_inputs


def test_readiness_dto_uses_the_same_closed_action_enum():
    from okto_pulse.core.models.delivery_readiness import ReadinessAction, ReadinessIssue
    assert ReadinessAction.model_fields['kind'].annotation is ActionKind
    assert None in ReadinessIssue.model_fields['blocks_target_transition'].annotation.__args__ or (
        type(None) in ReadinessIssue.model_fields['blocks_target_transition'].annotation.__args__)
