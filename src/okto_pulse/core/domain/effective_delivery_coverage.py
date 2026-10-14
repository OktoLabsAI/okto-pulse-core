"""Coverage of current obligations and contributions over server-owned facts."""

from dataclasses import dataclass, replace

from okto_pulse.core.domain.delivery_evidence import (
    DeliveryBinding,
    DeliveryEvidenceSnapshot,
    DeliveryCoverageEvaluation,
    DeliveryCoverageRow,
    DeliveryObligation,
    ImplementationDeliveryFact,
    TestDeliveryFact,
    _evaluate_delivery_facts,
    implementation_binding_ready,
    implementation_binding_proof_issue,
    delivery_proof_lifecycle_eligible,
)
from okto_pulse.core.domain.effective_delivery_inventory import (
    EffectiveDeliveryInventory,
)
from okto_pulse.core.domain.enums import CardType, TestScenarioStatus
from okto_pulse.core.domain.decision_review import DecisionInspectionState
from okto_pulse.core.domain.verification_plan import RequiredVerificationScenario
from okto_pulse.core.domain.delivery_verification import (
    NativeVerificationPopulation, ScenarioVerificationFinding, ScenarioDeliveryCheck, missing_scenario_reason,
)
from okto_pulse.core.domain.delivery_assessment import (
    FactualReason, ReadinessReason, ReadinessIntegrityError, AssessmentCompleteness, DeliveryAssessment,
    ObligationAssessment, ScenarioAssessment, UnitPhaseAssessment, UnitState, apply_delivery_exceptions,
)
from okto_pulse.core.domain.delivery_evidence import DeliveryPhase


@dataclass(frozen=True, slots=True)
class DeliveryScopeAttestation:
    binding: DeliveryBinding
    scope_sha256: str

    def __post_init__(self):
        if (
            not isinstance(self.scope_sha256, str)
            or len(self.scope_sha256) != 64
            or any(c not in "0123456789abcdef" for c in self.scope_sha256)
        ):
            raise ValueError("delivery_contribution_scope_invalid")


@dataclass(frozen=True, slots=True)
class ScopedImplementationFact:
    fact: ImplementationDeliveryFact
    scopes: tuple[DeliveryScopeAttestation, ...]


def read_scoped_implementation(
    fact: ImplementationDeliveryFact, payload: dict
) -> ScopedImplementationFact:
    """Require immutable server-authored scope attestations for every binding."""
    rows = payload.get("contribution_scopes")
    if (
        payload.get("scope_contract_version") != "card-contribution-scope/v1"
        or not isinstance(rows, list)
        or not 1 <= len(rows) <= 1000
    ):
        raise ValueError("delivery_contribution_scope_payload_invalid")
    bindings = {binding.obligation_ref: binding for binding in fact.bindings}
    if len(bindings) != len(fact.bindings) or len(rows) != len(bindings):
        raise ValueError("delivery_contribution_scope_payload_invalid")
    seen = set()
    scopes = []
    for row in rows:
        if (
            not isinstance(row, dict)
            or set(row) != {"obligation_ref", "scope_sha256"}
            or not isinstance(row.get("obligation_ref"), str)
            or row["obligation_ref"] not in bindings
            or row["obligation_ref"] in seen
        ):
            raise ValueError("delivery_contribution_scope_payload_invalid")
        seen.add(row["obligation_ref"])
        scopes.append(
            DeliveryScopeAttestation(
                bindings[row["obligation_ref"]], row["scope_sha256"]
            )
        )
    return ScopedImplementationFact(fact, tuple(scopes))


@dataclass(frozen=True, slots=True)
class ScopedTestFact:
    fact: TestDeliveryFact
    # Current authenticated scenario contract, resolved by the edition. These
    # are not free client fields or assertions inferred from the result string.
    criterion_ids: tuple[str, ...]
    verification_method: str
    # Edition-derived from the same authenticated specialized report. None
    # retains aggregate-run semantics; () explicitly means no passing criterion.
    # This is an internal fact, never a client-supplied authority field.
    passing_criterion_ids: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class EffectiveDeliveryCoverageRow:
    binding: DeliveryBinding
    required_card_ids: tuple[str, ...]
    implementation_ids: tuple[str, ...]
    test_ids: tuple[str, ...]
    missing_card_ids: tuple[str, ...]
    missing_criteria: tuple[tuple[str, str], ...]  # (implementation record, criterion)
    implementation_waiver_ids: tuple[str, ...]
    test_waiver_ids: tuple[str, ...]
    implementation_satisfied: bool
    test_satisfied: bool
    decision_verification_status: str | None = None
    decision_review_ids: tuple[str, ...] = ()
    missing_scenarios: tuple[tuple[str, str, str], ...] = ()
    conflicting_scenario_receipts: tuple[tuple[str, tuple[str, ...]], ...] = ()
    scenario_findings: tuple[ScenarioVerificationFinding, ...] = ()


@dataclass(frozen=True, slots=True)
class EffectiveDeliveryCoverage:
    rows: tuple[EffectiveDeliveryCoverageRow, ...]
    blockers: tuple[str, ...]
    rejected_record_ids: tuple[str, ...]
    scenario_checks: tuple[ScenarioDeliveryCheck, ...] = ()

    @property
    def allowed(self):
        return not self.blockers


@dataclass(frozen=True, slots=True)
class EffectiveDeliveryContext:
    inventory: EffectiveDeliveryInventory
    implementations: tuple[ScopedImplementationFact, ...]
    tests: tuple[ScopedTestFact, ...]
    admitted_methods: frozenset[str] | None
    decision_inspections: tuple[DecisionInspectionState, ...] = ()
    required_scenarios: tuple[RequiredVerificationScenario, ...] | None = None
    native_verification: NativeVerificationPopulation | None = None


@dataclass(frozen=True, slots=True)
class ScopedDeliveryCoverageRow(DeliveryCoverageRow):
    contributions_complete: bool
    criteria_complete: bool
    required_card_ids: tuple[str, ...]
    missing_card_ids: tuple[str, ...]
    missing_criteria: tuple[tuple[str, str], ...]
    decision_verification_status: str | None = None
    decision_review_ids: tuple[str, ...] = ()
    missing_scenarios: tuple[tuple[str, str, str], ...] = ()
    conflicting_scenario_receipts: tuple[tuple[str, tuple[str, ...]], ...] = ()
    scenario_findings: tuple[ScenarioVerificationFinding, ...] = ()

    @property
    def implementation_satisfied(self):
        return self.contributions_complete

    @property
    def test_satisfied(self):
        return self.criteria_complete


def implementation_scope_current(snapshot, fact, binding):
    context = snapshot.effective_context
    if not isinstance(context, EffectiveDeliveryContext):
        return False
    scoped = [item for item in context.implementations if item.fact == fact]
    rows = [item for item in context.inventory.rows if item.binding == binding]
    if len(scoped) != 1 or len(rows) != 1 or rows[0].blockers:
        return False
    planned = [item for item in rows[0].contributions if item.card_id == fact.card_id]
    attestations = [item for item in scoped[0].scopes if item.binding == binding]
    return len(planned) == len(attestations) == 1 and planned[0].scope_sha256 == attestations[0].scope_sha256


def test_observes_contribution(snapshot, test, implementation, binding):
    """Admission checks relevance; final coverage separately requires all criteria."""
    context = snapshot.effective_context
    if not isinstance(context, EffectiveDeliveryContext) or not implementation_scope_current(snapshot, implementation, binding):
        return False
    observed = [item for item in context.tests if item.fact == test]
    if len(observed) != 1 or not isinstance(context.admitted_methods, frozenset):
        return False
    observation = observed[0]
    if (observation.verification_method not in context.admitted_methods
        or not isinstance(observation.criterion_ids, tuple)
        or any(not isinstance(value, str) or not value for value in observation.criterion_ids)):
        return False
    planned = next(row for row in context.inventory.rows if row.binding == binding)
    contribution = next(item for item in planned.contributions if item.card_id == implementation.card_id)
    criteria = contribution_verification_criteria(planned, contribution)
    return not criteria or bool(set(criteria).intersection(observation.criterion_ids))


def contribution_verification_criteria(obligation, contribution):
    """An acceptance criterion observes itself even for a whole-scope Card.

    Supplemental AC inventory rows preserve their existing allocation/digest;
    their empty selected-criteria list is not permission to use an unrelated run.
    """
    if obligation.family == 'ac':
        return (obligation.binding.obligation_ref.removeprefix('ac:'),)
    return contribution.criterion_ids


def evaluate_effective_facts(snapshot, *, require_lifecycle: bool = True):
    """Shared detailed result for obligation and scenario projections."""
    context = snapshot.effective_context
    if not isinstance(context, EffectiveDeliveryContext):
        return EffectiveDeliveryCoverage((), ('delivery_effective_context_unavailable',), ())
    return evaluate_effective_delivery_coverage(inventory=context.inventory, snapshot=snapshot,
        implementations=context.implementations, tests=context.tests, admitted_methods=context.admitted_methods,
        decision_inspections=context.decision_inspections, require_lifecycle=require_lifecycle,
        required_scenarios=context.required_scenarios, native_verification=context.native_verification)


def evaluate_effective_snapshot(snapshot, *, require_lifecycle: bool = True):
    result = evaluate_effective_facts(snapshot, require_lifecycle=require_lifecycle)
    obligations = {item.binding: item for item in snapshot.obligations}
    return DeliveryCoverageEvaluation(tuple(ScopedDeliveryCoverageRow(
        obligations[row.binding], row.implementation_ids, row.test_ids,
        row.implementation_waiver_ids, row.test_waiver_ids,
        row.implementation_satisfied, row.test_satisfied, row.required_card_ids,
        row.missing_card_ids, row.missing_criteria,
        row.decision_verification_status, row.decision_review_ids, row.missing_scenarios,
        row.conflicting_scenario_receipts,
        row.scenario_findings,
    ) for row in result.rows if row.binding in obligations), result.blockers, result.rejected_record_ids,
        tuple(item for item in snapshot.rejected_records if item.scope == snapshot.scope))


def evaluate_effective_delivery_coverage(
    *,
    inventory: EffectiveDeliveryInventory,
    snapshot: DeliveryEvidenceSnapshot,
    implementations: tuple[ScopedImplementationFact, ...],
    tests: tuple[ScopedTestFact, ...],
    admitted_methods: frozenset[str] | None,
    decision_inspections: tuple[DecisionInspectionState, ...] = (),
    require_lifecycle: bool = True,
    required_scenarios: tuple[RequiredVerificationScenario, ...] | None = None,
    native_verification: NativeVerificationPopulation | None = None,
) -> EffectiveDeliveryCoverage:
    """Require every approved Card contribution and every selected criterion.

    Definitions, contribution scopes and source receipts protect distinct facts.
    No percentage or union of partial declarations completes a contribution.
    Several authenticated tests may jointly cover it, but a passing functional
    condition cannot conceal a missing technical/operational condition.
    """
    blockers = set()
    native_runs = {}
    if native_verification is not None:
        if len(native_verification.runs) > 5000 or sum(
            1 + len(run.sources) + len(run.observations) for run in native_verification.runs
        ) > 20000:
            return EffectiveDeliveryCoverage((), ('delivery_effective_resolution_limit',), ())
        if not native_verification.complete:
            blockers.add('delivery_verification_run_population_unavailable')
        for run in native_verification.runs:
            native_runs.setdefault(run.scenario_id, []).append(run)
    # Required topology is independent of selected records. Missing topology
    # must not let a passing subset claim complete criterion verification.
    scenarios = {}
    by_criterion = {}
    scenarios_known = isinstance(required_scenarios, tuple)
    if scenarios_known:
        if len(required_scenarios) > 5000:
            return EffectiveDeliveryCoverage((), ('delivery_effective_resolution_limit',), ())
        for row in required_scenarios:
            if (not isinstance(row, RequiredVerificationScenario)
                or not isinstance(row.scenario_id, str) or not row.scenario_id
                or row.scenario_id in scenarios
                or type(row.criterion_ids) is not tuple
                or any(type(key) is not str or not key for key in row.criterion_ids)
                or len(set(row.criterion_ids)) != len(row.criterion_ids)
                or type(row.test_card_ids) is not tuple
                or any(type(key) is not str or not key for key in row.test_card_ids)
                or len(set(row.test_card_ids)) != len(row.test_card_ids)
                or type(row.complete) is not bool):
                scenarios_known = False
                break
            scenarios[row.scenario_id] = row
            for criterion in row.criterion_ids:
                by_criterion.setdefault(criterion, set()).add(row.scenario_id)
    if not scenarios_known:
        scenarios.clear()
        by_criterion.clear()
        if any(contribution_verification_criteria(row, contribution)
               for row in inventory.rows for contribution in row.contributions):
            blockers.add('delivery_verification_scenario_population_unavailable')
    elif sum(len(row.criterion_ids) + len(row.test_card_ids) for row in required_scenarios) > 20000:
        return EffectiveDeliveryCoverage((), ('delivery_effective_resolution_limit',), ())
    if any(row.passing_criterion_ids is not None and type(row.passing_criterion_ids) is not tuple for row in tests):
        return EffectiveDeliveryCoverage((), ('delivery_scoped_population_mismatch',), ())
    # Count expanded associations before evaluating them. A bounded failure is
    # unknown coverage, not a truncated population that could appear complete.
    if (
        len(inventory.rows) > 5000
        or len(snapshot.implementations) + len(snapshot.tests) + len(snapshot.waivers) + len(snapshot.rejected_records)
        > 10000
        or sum(
            len(row.contributions)
            + sum(len(c.criterion_ids) for c in row.contributions)
            for row in inventory.rows
        )
        + sum(len(row.scopes) for row in implementations)
        + sum(
            len(row.criterion_ids)
            + len(row.passing_criterion_ids or ())
            + len(row.fact.bindings)
            + len(row.fact.verified_implementation_ids)
            for row in tests
        )
        > 20000
    ):
        return EffectiveDeliveryCoverage(
            (), ("delivery_effective_resolution_limit",), ()
        )
    if not inventory.complete:
        blockers.add("delivery_effective_inventory_incomplete")
    expected = {row.binding: row for row in inventory.rows}
    if len(expected) != len(inventory.rows) or len(
        {b.obligation_ref for b in expected}
    ) != len(expected):
        blockers.add("delivery_obligations_ambiguous")
    if {row.binding for row in snapshot.obligations} != set(expected):
        blockers.add("delivery_effective_inventory_mismatch")
    if not isinstance(admitted_methods, frozenset):
        blockers.add("delivery_verification_methods_unavailable")
        admitted_methods = frozenset()

    actual_implementations = {fact.id: fact for fact in snapshot.implementations}
    actual_tests = {fact.id: fact for fact in snapshot.tests}
    scoped_implementations = {row.fact.id: row for row in implementations}
    scoped_tests = {row.fact.id: row for row in tests}
    if (
        len(scoped_implementations) != len(implementations)
        or len(scoped_tests) != len(tests)
        or set(scoped_implementations) != set(actual_implementations)
        or set(scoped_tests) != set(actual_tests)
        or any(
            row.fact != actual_implementations.get(row.fact.id)
            for row in implementations
        )
        or any(row.fact != actual_tests.get(row.fact.id) for row in tests)
    ):
        return EffectiveDeliveryCoverage(
            (), ("delivery_scoped_population_mismatch",), ()
        )

    # Receipt/lifecycle/waiver checks are necessary but not sufficient; the
    # effective verdict also requires the scopes and criteria resolved below.
    baseline = _evaluate_delivery_facts(snapshot, require_lifecycle=require_lifecycle)
    blockers.update(
        code
        for code in baseline.blockers
        if code
        not in {
            "delivery_implementation_missing",
            "delivery_test_result_missing",
        }
    )
    baseline_rows = {row.obligation.binding: row for row in baseline.rows}
    rejected = set(baseline.rejected_record_ids)
    accepted = {}
    for row in implementations:
        fact = row.fact
        by_binding = {item.binding: item.scope_sha256 for item in row.scopes}
        if (
            len(by_binding) != len(row.scopes)
            or set(by_binding) != set(fact.bindings)
            or fact.contributions is None
            or fact.scope != snapshot.scope
            or fact.card_type not in {CardType.NORMAL, CardType.BUG}
            or not delivery_proof_lifecycle_eligible(fact.card_status, require_done=require_lifecycle)
        ):
            rejected.add(fact.id)
            continue
        matched = False
        for binding, digest in by_binding.items():
            obligation = expected.get(binding)
            planned = (
                [
                    item
                    for item in obligation.contributions
                    if item.card_id == fact.card_id
                ]
                if obligation
                else []
            )
            if (
                len(planned) != 1
                or planned[0].scope_sha256 != digest
                or not implementation_binding_ready(fact, binding)
                or fact.id
                not in (
                    baseline_rows[binding].implementation_ids
                    if binding in baseline_rows
                    else ()
                )
            ):
                continue
            accepted.setdefault(binding, {}).setdefault(fact.card_id, []).append(
                fact.id
            )
            matched = True
        if not matched:
            rejected.add(fact.id)

    result = []
    scenario_checks = []
    relevant_tests = set()
    by_test_binding = {}
    current_receipts = {}
    for test in tests:
        for binding in set(test.fact.bindings):
            by_test_binding.setdefault(binding, []).append(test)
        if test.fact.current_verified_run is True and test.fact.scope == snapshot.scope:
            current_receipts.setdefault(test.fact.scenario_id, set()).add(test.fact.receipt_id)
    # Currentness is resolved by the edition's native pointer, never by time or
    # whichever result passed. Multiple bindings to one receipt are not retries.
    conflicts = {key: tuple(sorted(receipts)) for key, receipts in current_receipts.items()
                 if len(receipts) > 1}
    checks = 0
    for binding, obligation in expected.items():
        if obligation.family == "decision":
            continue
        base = baseline_rows.get(binding)
        required = {item.card_id: item for item in obligation.contributions}
        if len(required) != len(obligation.contributions) or not required:
            blockers.add("delivery_contribution_allocation_invalid")
        by_card = accepted.get(binding, {})
        ids = {identity for values in by_card.values() for identity in values}
        # Distinct associations can name rigorously the same admitted proof.
        # Keep every ID/author; observe the equivalence class only after the
        # named member passes the existing test predicate. No commit ancestry,
        # text similarity, partial declarations or cross-Card equivalence.
        groups = {}
        for identity in sorted(ids):
            implementation = actual_implementations[identity]
            contribution = next(c for c in implementation.contributions if c.binding == binding)
            scope_digest = next(s.scope_sha256 for s in scoped_implementations[identity].scopes if s.binding == binding)
            executions = tuple(sorted((p.execution_id, p.target_id, p.target_revision,
                p.source_ref, p.result_revision, p.relative_path)
                for p in implementation.executions if p.execution_id in contribution.execution_ids))
            key = (implementation.scope, implementation.card_id, binding, scope_digest,
                contribution.contribution, executions)
            groups.setdefault(key, set()).add(identity)
        equivalent = {identity: values for values in groups.values() for identity in values}
        missing_cards = set(required) - set(by_card)
        observed = {identity: set() for identity in ids}
        observed_by = {}
        tested = set()
        test_ids = set()
        for test in by_test_binding.get(binding, ()):
            fact = test.fact
            if native_verification is not None:
                runs = native_runs.get(fact.scenario_id, ())
                if (not native_verification.complete or len(runs) != 1
                    or not runs[0].authenticated or runs[0].scope != snapshot.scope
                    or runs[0].receipt_id != fact.receipt_id):
                    continue
            # The fact checks prove authentication, current passing result,
            # exact implementation IDs and Done. It may withhold its aggregate
            # test_ids if another implementation lacks tests, so evaluate each
            # admitted run against only the IDs that it explicitly names.
            named = ids.intersection(fact.verified_implementation_ids)
            equivalent_named = set().union(*(equivalent[identity] for identity in named))
            checks += len(equivalent_named) * max(1, len(test.criterion_ids))
            if checks > 20000:
                return EffectiveDeliveryCoverage(
                    (), ("delivery_effective_resolution_limit",), ()
                )
            if (
                not named
                or fact.scenario_id in conflicts
                or not isinstance(test.verification_method, str)
                or test.verification_method not in admitted_methods
                or len(set(test.criterion_ids)) != len(test.criterion_ids)
                or any(
                    not isinstance(identity, str) or not identity.strip()
                    for identity in test.criterion_ids
                )
            ):
                continue
            passing = test.passing_criterion_ids
            if passing is not None:
                if (test.verification_method not in {'inspection', 'static_analysis', 'demonstration'}
                    or type(passing) is not tuple or not passing
                    or any(type(key) is not str or not key for key in passing)
                    or len(set(passing)) != len(passing)
                    or not set(passing) <= set(test.criterion_ids)
                    or fact.result not in {TestScenarioStatus.PASSED, TestScenarioStatus.FAILED}):
                    continue
                # Only this derived criterion check uses passing. The stored
                # run verdict stays factual; all authentication, freshness,
                # Card completion and implementation checks still run below.
                checked_fact = replace(fact, result=TestScenarioStatus.PASSED)
            else:
                checked_fact = fact
            check = _evaluate_delivery_facts(
                DeliveryEvidenceSnapshot(
                    snapshot.scope,
                    (DeliveryObligation(binding, binding.obligation_ref),),
                    tuple(
                        actual_implementations[identity] for identity in sorted(named)
                    ),
                    (checked_fact,),
                    complete=snapshot.complete,
                ), require_lifecycle=require_lifecycle,
            )
            if not check.rows[0].test_satisfied:
                continue
            relevant = False
            for identity in equivalent_named:
                planned = required[actual_implementations[identity].card_id]
                criteria = set(contribution_verification_criteria(obligation, planned))
                covered = criteria.intersection(passing if passing is not None else test.criterion_ids)
                scenario = scenarios.get(fact.scenario_id)
                if criteria:
                    if (scenario is None or not scenario.complete
                        or scenario.method != test.verification_method
                        or fact.card_id not in scenario.test_card_ids):
                        continue
                    covered.intersection_update(scenario.criterion_ids)
                if passing is not None and not criteria and fact.result != TestScenarioStatus.PASSED:
                    # No criterion-specific proof may satisfy an unqualified
                    # supplemental obligation using the aggregate fallback.
                    continue
                if criteria and not covered:
                    continue
                observed[identity].update((criterion, fact.scenario_id) for criterion in covered)
                for criterion in covered:
                    observed_by.setdefault((identity, criterion, fact.scenario_id), set()).add(fact.id)
                tested.add(identity)
                relevant = True
            if relevant:
                test_ids.add(fact.id)
                relevant_tests.add(fact.id)
        checks += sum(
            max(1, len(by_criterion.get(criterion, ())))
            for identity in ids
            for criterion in contribution_verification_criteria(
                obligation, required[actual_implementations[identity].card_id])
        )
        if checks > 20000:
            return EffectiveDeliveryCoverage((), ('delivery_effective_resolution_limit',), ())
        missing_criteria = {
            (identity, criterion)
            for identity in ids
            for criterion in contribution_verification_criteria(obligation, required[
                actual_implementations[identity].card_id
            ])
            if not scenarios_known or not by_criterion.get(criterion) or any(
                (criterion, scenario_id) not in observed[identity]
                for scenario_id in by_criterion.get(criterion, ())
            )
        }
        missing_scenarios = {
            (identity, criterion, scenario_id)
            for identity, criterion in missing_criteria
            for scenario_id in by_criterion.get(criterion, ())
            if (criterion, scenario_id) not in observed[identity]
        }
        conflicting_receipts = tuple(sorted((key, conflicts[key])
            for key in {scenario_id for _, _, scenario_id in missing_scenarios}
            if key in conflicts))
        findings = []
        for identity, criterion, scenario_id in sorted(missing_scenarios):
            if scenario_id in conflicts:
                reason = FactualReason(ReadinessReason.VERIFICATION_CONFLICT,
                    source_ids=conflicts[scenario_id], criterion_ids=(criterion,), scenario_ids=(scenario_id,))
            elif not scenarios[scenario_id].complete:
                reason = FactualReason(
                    ReadinessReason.VERIFICATION_METHOD_UNSUPPORTED
                    if scenarios[scenario_id].method not in admitted_methods else ReadinessReason.VERIFICATION_PLAN_MISSING,
                    criterion_ids=(criterion,), scenario_ids=(scenario_id,))
            else:
                reason = missing_scenario_reason(scope=snapshot.scope, binding=binding,
                    implementation=actual_implementations[identity], scenario_id=scenario_id,
                    criterion_id=criterion, population=native_verification,
                    current_runs=tuple(native_runs.get(scenario_id, ())))
                if reason.code == ReadinessReason.VERIFICATION_ASSOCIATION_MISSING and require_lifecycle and any(
                    test.fact.scenario_id == scenario_id and identity in test.fact.verified_implementation_ids
                    and test.fact.receipt_id in reason.source_ids and test.fact.current_verified_run
                    and not delivery_proof_lifecycle_eligible(test.fact.card_status, require_done=True)
                    for test in by_test_binding.get(binding, ())
                ):
                    reason = replace(reason, code=ReadinessReason.LIFECYCLE_CARD_OPEN)
            findings.append(ScenarioVerificationFinding(identity, criterion, scenario_id, reason))
        missing_reasons = {(f.implementation_id, f.criterion_id, f.scenario_id): f.reason for f in findings}
        for card_id, contribution in sorted(required.items()):
            for criterion in contribution_verification_criteria(obligation, contribution):
                for scenario_id in sorted(by_criterion.get(criterion, ())):
                    for identity in sorted(by_card.get(card_id, ())) or (None,):
                        key = (identity, criterion, scenario_id)
                        evidence = tuple(sorted(observed_by.get(key, ())))
                        reason = missing_reasons.get(key)
                        if identity is None:
                            reason = FactualReason(ReadinessReason.VERIFICATION_DEPENDENCY_PENDING,
                                source_ids=(card_id,), criterion_ids=(criterion,), scenario_ids=(scenario_id,))
                        scenario_checks.append(ScenarioDeliveryCheck(binding, card_id, criterion,
                            scenario_id, identity, bool(evidence), evidence, (reason,) if reason else ()))
                        if len(scenario_checks) > 20000:
                            return EffectiveDeliveryCoverage((), ('delivery_effective_resolution_limit',), ())
        if not required and obligation.family == 'ac':
            criterion = binding.obligation_ref.removeprefix('ac:')
            for scenario_id in sorted(by_criterion.get(criterion, ())):
                scenario_checks.append(ScenarioDeliveryCheck(binding, None, criterion, scenario_id, None, False,
                    reasons=(FactualReason(ReadinessReason.SCOPE_ALLOCATION_UNRESOLVED,
                        criterion_ids=(criterion,), scenario_ids=(scenario_id,)),)))
                if len(scenario_checks) > 20000:
                    return EffectiveDeliveryCoverage((), ('delivery_effective_resolution_limit',), ())
        implementation_waivers = base.implementation_waiver_ids if base else ()
        test_waivers = base.test_waiver_ids if base else ()
        if conflicting_receipts and not test_waivers:
            blockers.add('delivery_verification_run_conflict')
        implemented = bool(implementation_waivers or (required and not missing_cards))
        verified = bool(
            test_waivers
            or (
                required
                and not missing_cards
                and ids
                and ids <= tested
                and not missing_criteria
            )
        )
        result.append(
            EffectiveDeliveryCoverageRow(
                binding,
                tuple(sorted(required)),
                tuple(sorted(ids)),
                tuple(sorted(test_ids)),
                tuple(sorted(missing_cards)),
                tuple(sorted(missing_criteria)),
                implementation_waivers,
                test_waivers,
                implemented,
                verified,
                missing_scenarios=tuple(sorted(missing_scenarios)),
                conflicting_scenario_receipts=conflicting_receipts,
                scenario_findings=tuple(findings),
            )
        )
    # Decisions reuse delivery proof; they never manufacture implementation
    # ownership or accept arbitrary Test Card bindings to a contextual choice.
    by_ref = {row.binding.obligation_ref: row for row in result}
    for binding, obligation in expected.items():
        if obligation.family != "decision":
            continue
        plan = obligation.decision_plan
        refs = plan.verification.obligation_refs if plan and plan.verification else ()
        dependencies = [by_ref.get(ref) for ref in refs]
        satisfied = bool(plan and plan.complete) and all(
            row is not None and row.implementation_satisfied and row.test_satisfied
            and not row.implementation_waiver_ids and not row.test_waiver_ids
            for row in dependencies
        )
        status = "verified" if satisfied else "obligations_pending"
        review_ids = ()
        if not plan or not plan.complete:
            status = "planning_pending"
        elif plan.verification.inspection is not None:
            states = [state for state in decision_inspections if state.decision_id == plan.decision_id]
            inspection = states[0] if len(states) == 1 else None
            review_ids = inspection.record_ids if inspection else ()
            satisfied = satisfied and inspection is not None and inspection.status == "verified"
            status = "verified" if satisfied else (
                inspection.status if inspection and inspection.status != "verified" else
                "obligations_pending" if inspection else "inspection_pending")
        if not satisfied:
            blockers.add("decision_verification_incomplete")
        result.append(EffectiveDeliveryCoverageRow(binding, (), (),
            tuple(sorted({identity for row in dependencies if row for identity in row.test_ids})),
            (), (), (), (), True, satisfied, status, review_ids))
    if any(not row.implementation_satisfied for row in result):
        blockers.add("delivery_implementation_missing")
    if any(not row.test_satisfied and row.decision_verification_status is None for row in result):
        blockers.add("delivery_test_result_missing")
    rejected.difference_update(relevant_tests)
    rejected.update(set(actual_tests) - relevant_tests)
    return EffectiveDeliveryCoverage(
        tuple(result), tuple(sorted(blockers)), tuple(sorted(rejected)), tuple(scenario_checks)
    )


# Closed native code mappings; diagnostic prose never establishes semantics.
_IMPLEMENTATION_REASONS = {
    'delivery_record_revoked': ReadinessReason.IMPLEMENTATION_PROOF_REVOKED,
    'delivery_investigation_receipt_revoked': ReadinessReason.IMPLEMENTATION_PROOF_REVOKED,
    'delivery_material_progress_unresolved': ReadinessReason.MATERIAL_PROGRESS_UNRESOLVED,
    'delivery_execution_superseded': ReadinessReason.IMPLEMENTATION_PROOF_STALE,
    'delivery_target_revision_changed': ReadinessReason.IMPLEMENTATION_PROOF_STALE,
    'delivery_target_unresolved': ReadinessReason.IMPLEMENTATION_PROOF_STALE,
    'delivery_investigation_receipt_unresolved': ReadinessReason.IMPLEMENTATION_PROOF_STALE,
    'delivery_execution_base_mismatch': ReadinessReason.IMPLEMENTATION_PROOF_STALE,
    'delivery_execution_binding_invalid': ReadinessReason.IMPLEMENTATION_PROOF_STALE,
    'delivery_accepted_committed_task_execution_required': ReadinessReason.IMPLEMENTATION_PROOF_STALE,
    'delivery_execution_set_unresolved': ReadinessReason.IMPLEMENTATION_PROOF_STALE,
    'delivery_execution_base_conflict': ReadinessReason.IMPLEMENTATION_PROOF_STALE,
    'delivery_execution_set_ambiguous': ReadinessReason.IMPLEMENTATION_PROOF_STALE,
    'delivery_record_payload_invalid': ReadinessReason.ASSESSMENT_UNAVAILABLE,
    'delivery_record_card_scope_mismatch': ReadinessReason.ASSESSMENT_UNAVAILABLE,
    'delivery_binding_payload_invalid': ReadinessReason.ASSESSMENT_UNAVAILABLE,
    'delivery_implementation_payload_invalid': ReadinessReason.ASSESSMENT_UNAVAILABLE,
    'delivery_implementation_context_unresolved': ReadinessReason.ASSESSMENT_UNAVAILABLE,
    'delivery_contribution_scope_payload_invalid': ReadinessReason.ASSESSMENT_UNAVAILABLE,
}


def implementation_readiness_reason(code):
    """Closed native rejection mapping; never silently absorb a new code."""
    try:
        return _IMPLEMENTATION_REASONS[code]
    except (KeyError, TypeError) as exc:
        raise ReadinessIntegrityError('readiness_native_reason_unmapped') from exc


def _implementation_cause_reasons(codes, record_id):
    """Retain a generic predicate failure under its concrete native cause."""
    codes = set(codes)
    generic = 'delivery_accepted_committed_task_execution_required'
    inherited = (generic,) if generic in codes and len(codes) > 1 else ()
    if inherited:
        codes.remove(generic)
    return tuple(FactualReason(implementation_readiness_reason(code),
        native_reason_codes=tuple(sorted((code, *inherited))), source_ids=(record_id,)) for code in sorted(codes))


_REASON_STATES = {
    ReadinessReason.SCOPE_ALLOCATION_UNRESOLVED: UnitState.MISSING,
    ReadinessReason.IMPLEMENTATION_MISSING: UnitState.MISSING,
    ReadinessReason.IMPLEMENTATION_ASSOCIATION_MISSING: UnitState.MISSING,
    ReadinessReason.VERIFICATION_PLAN_MISSING: UnitState.MISSING,
    ReadinessReason.VERIFICATION_NOT_RUN: UnitState.MISSING,
    ReadinessReason.VERIFICATION_INCONCLUSIVE: UnitState.MISSING,
    ReadinessReason.VERIFICATION_ASSOCIATION_MISSING: UnitState.MISSING,
    ReadinessReason.VERIFICATION_DEPENDENCY_PENDING: UnitState.MISSING,
    ReadinessReason.DECISION_VERIFICATION_PENDING: UnitState.MISSING,
    ReadinessReason.APPLICABLE_OBLIGATION_WAIVED: UnitState.MISSING,
    ReadinessReason.LIFECYCLE_CARD_OPEN: UnitState.MISSING,
    ReadinessReason.LIFECYCLE_GATE_PENDING: UnitState.MISSING,
    ReadinessReason.HISTORICAL_COMPLETION_DIVERGED: UnitState.MISSING,
    ReadinessReason.VERIFICATION_CONFLICT: UnitState.CONFLICT,
    ReadinessReason.VERIFICATION_FAILED: UnitState.FAILED,
    ReadinessReason.ASSESSMENT_UNAVAILABLE: UnitState.UNKNOWN,
    ReadinessReason.SCOPE_UNAVAILABLE: UnitState.UNKNOWN,
    ReadinessReason.VERIFICATION_BASE_UNRESOLVED: UnitState.UNKNOWN,
    ReadinessReason.VERIFICATION_METHOD_UNSUPPORTED: UnitState.UNKNOWN,
    ReadinessReason.SCOPE_CONTRACT_CHANGED: UnitState.STALE,
    ReadinessReason.IMPLEMENTATION_PROOF_REVOKED: UnitState.STALE,
    ReadinessReason.IMPLEMENTATION_PROOF_STALE: UnitState.STALE,
    ReadinessReason.MATERIAL_PROGRESS_UNRESOLVED: UnitState.STALE,
    ReadinessReason.VERIFICATION_BASE_MISMATCH: UnitState.STALE,
    ReadinessReason.VERIFICATION_RECEIPT_INVALID: UnitState.STALE,
    ReadinessReason.IMPLEMENTATION_PARTIAL: UnitState.PARTIAL,
}
_DECISION_REASONS = {
    'failed': ReadinessReason.VERIFICATION_FAILED,
    'conflict': ReadinessReason.VERIFICATION_CONFLICT,
    'inconclusive': ReadinessReason.VERIFICATION_INCONCLUSIVE,
    'aborted': ReadinessReason.VERIFICATION_INCONCLUSIVE,
    'unavailable': ReadinessReason.ASSESSMENT_UNAVAILABLE,
    'revoked': ReadinessReason.VERIFICATION_RECEIPT_INVALID,
    'planning_pending': ReadinessReason.DECISION_VERIFICATION_PENDING,
    'inspection_pending': ReadinessReason.DECISION_VERIFICATION_PENDING,
    'obligations_pending': ReadinessReason.DECISION_VERIFICATION_PENDING,
}


def readiness_reason_state(code):
    try:
        return _REASON_STATES[ReadinessReason(code)]
    except (ValueError, TypeError, KeyError) as exc:
        raise ReadinessIntegrityError('readiness_reason_unmapped') from exc


_STATE_PRECEDENCE = (UnitState.CONFLICT, UnitState.FAILED, UnitState.UNKNOWN,
    UnitState.STALE, UnitState.PARTIAL, UnitState.MISSING, UnitState.SATISFIED)
_KNOWN_ALLOCATION_GAPS = frozenset({
    'delivery_inventory_work_unassigned', 'delivery_inventory_allocation_unresolved',
    'implementation_plan_required', 'implementation_contribution_card_unavailable',
    'implementation_contribution_scope_uncovered', 'implementation_contribution_links_changed',
})
_UNEVALUABLE = frozenset({
    'delivery_effective_resolution_limit', 'delivery_scoped_population_mismatch',
    'delivery_effective_inventory_mismatch', 'delivery_obligations_ambiguous',
    'delivery_record_scope_mismatch', 'delivery_projection_incomplete',
    'delivery_record_identity_ambiguous',
})
_VERIFICATION_UNEVALUABLE = frozenset({
    'delivery_verification_methods_unavailable',
    'delivery_verification_scenario_population_unavailable',
    'delivery_verification_run_population_unavailable',
})


def _assessed_phase(*, satisfied=False, lifecycle=False, reasons=(), evidence_ids=(), state=None):
    reasons = tuple(sorted(set(reasons), key=lambda r: (
        r.code, r.native_reason_codes, r.source_ids, r.criterion_ids, r.scenario_ids,
        r.implementation_ids, tuple((b.obligation_ref, b.semantic_sha256) for b in r.bindings))))
    if state is None:
        states = {readiness_reason_state(reason.code) for reason in reasons}
        state = UnitState.SATISFIED if satisfied else next(
            (value for value in _STATE_PRECEDENCE if value in states), UnitState.MISSING)
    return UnitPhaseAssessment(state, state,
        None if state == UnitState.UNKNOWN else satisfied, lifecycle,
        reasons=reasons, evidence_ids=tuple(sorted(set(evidence_ids))))


def _implementation_findings(snapshot, obligation, row):
    """Classify missing contributions after the canonical proof evaluation."""
    if row.implementation_satisfied:
        return ()
    reasons = []
    missing = set(row.missing_card_ids)
    binding = obligation.binding
    processed = set()
    for fact in snapshot.implementations:
        if fact.scope != snapshot.scope or fact.card_id not in missing:
            continue
        if binding not in fact.bindings:
            if any(value.obligation_ref == binding.obligation_ref for value in fact.bindings):
                reasons.append(FactualReason(ReadinessReason.SCOPE_CONTRACT_CHANGED, source_ids=(fact.id,)))
            continue
        processed.add(fact.id)
        if not implementation_scope_current(snapshot, fact, binding):
            reasons.append(FactualReason(ReadinessReason.SCOPE_CONTRACT_CHANGED, source_ids=(fact.id,)))
            continue
        issue = implementation_binding_proof_issue(fact, binding)
        selected_ids = {identity for contribution in fact.contributions if contribution.binding == binding
            for identity in contribution.execution_ids}
        codes = {code for proof in fact.executions if proof.execution_id in selected_ids for code in proof.rejection_reasons}
        if issue:
            codes.add(issue)
        if fact.blocking_progress_ids or fact.blocking_progress_truncated:
            codes.add('delivery_material_progress_unresolved')
        reasons.extend(_implementation_cause_reasons(codes, fact.id))
        if not issue and any(c.binding == binding and c.contribution == 'partial' for c in fact.contributions or ()):
            reasons.append(FactualReason(ReadinessReason.IMPLEMENTATION_PARTIAL, source_ids=(fact.id,)))
    for rejection in snapshot.rejected_records:
        if (rejection.scope != snapshot.scope or rejection.kind != 'implementation'
            or rejection.card_id not in missing or binding not in rejection.bindings or rejection.record_id in processed):
            continue
        reasons.extend(_implementation_cause_reasons(rejection.reason_codes, rejection.record_id))
    if not reasons:
        reasons.append(FactualReason(ReadinessReason.IMPLEMENTATION_PARTIAL if row.implementation_ids
            else ReadinessReason.IMPLEMENTATION_MISSING, source_ids=tuple(sorted(missing))))
    return tuple(reasons)


def _verification_findings(context, obligation, row):
    if row.test_satisfied:
        return ()
    if obligation.family == 'decision':
        try:
            state_reason = _DECISION_REASONS[row.decision_verification_status]
        except (KeyError, TypeError) as exc:
            raise ReadinessIntegrityError('readiness_native_reason_unmapped') from exc
        return (FactualReason(state_reason, native_reason_codes=(row.decision_verification_status,),
            source_ids=row.decision_review_ids, bindings=(obligation.binding,)),)
    reasons = [replace(item.reason, implementation_ids=(item.implementation_id,), bindings=(obligation.binding,))
        for item in row.scenario_findings]
    if context.admitted_methods is None:
        reasons.append(FactualReason(ReadinessReason.ASSESSMENT_UNAVAILABLE,
            native_reason_codes=('delivery_verification_methods_unavailable',)))
    if context.native_verification is not None and not context.native_verification.complete:
        reasons.append(FactualReason(ReadinessReason.ASSESSMENT_UNAVAILABLE,
            native_reason_codes=('delivery_verification_run_population_unavailable',)))
    if context.required_scenarios is None and any(
        contribution_verification_criteria(obligation, c) for c in obligation.contributions
    ):
        reasons.append(FactualReason(ReadinessReason.ASSESSMENT_UNAVAILABLE,
            native_reason_codes=('delivery_verification_scenario_population_unavailable',)))
    if row.missing_card_ids:
        reasons.append(FactualReason(ReadinessReason.VERIFICATION_DEPENDENCY_PENDING,
            source_ids=row.missing_card_ids))
    if row.missing_criteria and not row.missing_scenarios and not reasons:
        reasons.append(FactualReason(ReadinessReason.VERIFICATION_PLAN_MISSING,
            criterion_ids=tuple(sorted({criterion for _, criterion in row.missing_criteria}))))
    if not reasons:
        reasons.append(FactualReason(ReadinessReason.VERIFICATION_NOT_RUN))
    return tuple(reasons)


def assess_effective_obligations(snapshot, *, card_id=None, card_type=None):
    """Produce factual Spec/normal/bug units using the existing evaluator.

    No proof checks are replaced by score rules. Scope is narrowed to the
    Card's contribution; tests from other Cards remain available. Exceptions
    are applied after proof-only evaluation and cannot manufacture credit.
    """
    if (card_id is None) != (card_type is None) or card_type not in {None, CardType.NORMAL, CardType.BUG}:
        raise ValueError('delivery_obligation_assessment_scope_invalid')
    context = snapshot.effective_context
    if not isinstance(context, EffectiveDeliveryContext):
        return DeliveryAssessment(snapshot.scope, AssessmentCompleteness(False, False, False, True), (),
            reasons=(FactualReason(ReadinessReason.SCOPE_UNAVAILABLE),), card_id=card_id, card_type=card_type)
    full_rows = context.inventory.rows
    bindings = tuple(item.binding for item in full_rows)
    population_known = bool(full_rows) and context.inventory.population_complete and (
        len(set(bindings)) == len(bindings)
        and len({b.obligation_ref for b in bindings}) == len(bindings)
        and set(bindings) == {item.binding for item in snapshot.obligations})
    rows = full_rows if card_id is None else tuple(replace(row,
        contributions=tuple(c for c in row.contributions if c.card_id == card_id))
        for row in full_rows if row.family != 'decision' and (card_id in row.declared_card_ids
            or any(c.card_id == card_id for c in row.contributions)))
    selected = {row.binding for row in rows}
    scoped_context = replace(context, inventory=replace(context.inventory, rows=rows))
    facts_complete = snapshot.complete if snapshot.facts_complete is None else snapshot.facts_complete
    proof_snapshot = replace(snapshot, waivers=(), complete=facts_complete,
        obligations=tuple(o for o in snapshot.obligations if o.binding in selected), effective_context=scoped_context)
    proof = evaluate_effective_snapshot(proof_snapshot, require_lifecycle=False)
    lifecycle = evaluate_effective_snapshot(proof_snapshot, require_lifecycle=True)
    proof_rows = {row.obligation.binding: row for row in proof.rows}
    lifecycle_rows = {row.obligation.binding: row for row in lifecycle.rows}
    unavailable = tuple(sorted(set(proof.blockers) & _UNEVALUABLE))
    values = []
    scope_resolved = bool(rows) and not context.inventory.issues
    for obligation in rows:
        row = proof_rows.get(obligation.binding)
        final = lifecycle_rows.get(obligation.binding)
        if row is None or unavailable or not facts_complete:
            implementation = verification = _assessed_phase(state=UnitState.UNKNOWN,
                reasons=(FactualReason(ReadinessReason.ASSESSMENT_UNAVAILABLE,
                    native_reason_codes=unavailable or ('delivery_facts_incomplete',)),))
        elif obligation.blockers:
            scope_resolved = False
            known_gap = set(obligation.blockers) <= _KNOWN_ALLOCATION_GAPS
            reason = FactualReason(ReadinessReason.SCOPE_ALLOCATION_UNRESOLVED if known_gap
                else ReadinessReason.ASSESSMENT_UNAVAILABLE, native_reason_codes=obligation.blockers)
            implementation = verification = _assessed_phase(reasons=(reason,))
        else:
            implementation = _assessed_phase(satisfied=row.implementation_satisfied,
                lifecycle=bool(final and final.implementation_satisfied), evidence_ids=row.implementation_ids,
                reasons=_implementation_findings(proof_snapshot, obligation, row))
            verification = _assessed_phase(satisfied=row.test_satisfied,
                lifecycle=bool(final and final.test_satisfied), evidence_ids=(*row.test_ids, *row.decision_review_ids),
                reasons=_verification_findings(scoped_context, obligation, row))
            verification_unavailable = tuple(sorted(set(proof.blockers) & _VERIFICATION_UNEVALUABLE))
            if verification_unavailable and obligation.family != 'decision':
                verification = _assessed_phase(reasons=(*verification.reasons,
                    FactualReason(ReadinessReason.ASSESSMENT_UNAVAILABLE,
                        native_reason_codes=verification_unavailable)), evidence_ids=verification.evidence_ids)
        if obligation.family == 'decision':
            implementation = _assessed_phase(state=UnitState.NOT_APPLICABLE)
        implementation = apply_delivery_exceptions(implementation, scope=snapshot.scope,
            binding=obligation.binding, phase=DeliveryPhase.IMPLEMENTATION, waivers=snapshot.waivers)
        verification = apply_delivery_exceptions(verification, scope=snapshot.scope,
            binding=obligation.binding, phase=DeliveryPhase.TEST, waivers=snapshot.waivers)
        values.append(ObligationAssessment(obligation.binding, snapshot.scope.edition,
            tuple(sorted({c.card_id for c in obligation.contributions})), implementation, verification))
    # Accepted risk cannot turn unavailable proof into a known measurement.
    # Authorized N/A is resolved first and legitimately removes applicability.
    implementation_complete = all(row.implementation.state == UnitState.NOT_APPLICABLE
        or row.implementation.proof_satisfied is not None for row in values)
    verification_complete = all(row.verification.state == UnitState.NOT_APPLICABLE
        or row.verification.proof_satisfied is not None for row in values)
    return DeliveryAssessment(snapshot.scope, AssessmentCompleteness(population_known, scope_resolved,
        implementation_complete and verification_complete, True, implementation_complete, verification_complete),
        tuple(values), card_id=card_id, card_type=card_type)


def _combine_scenario_checks(phases):
    """Group evaluated joins, preserving exceptions and intrinsic state."""
    reasons = tuple(reason for phase in phases for reason in phase.reasons)
    evidence = tuple(identity for phase in phases for identity in phase.evidence_ids)
    active = tuple(phase for phase in phases if phase.state != UnitState.NOT_APPLICABLE)
    intrinsic_states = {phase.intrinsic_state for phase in phases}
    intrinsic = next((state for state in _STATE_PRECEDENCE if state in intrinsic_states), UnitState.MISSING)
    result = _assessed_phase(satisfied=intrinsic == UnitState.SATISFIED,
        lifecycle=bool(active) and all(p.lifecycle_satisfied is True for p in active),
        state=intrinsic, reasons=reasons, evidence_ids=evidence)
    exceptions = tuple(sorted({identity for p in phases for identity in p.exception_ids}))
    unused = tuple(sorted({identity for p in phases for identity in p.unused_exception_ids}))
    if phases and not active:
        return replace(result, state=UnitState.NOT_APPLICABLE, exception_ids=exceptions, unused_exception_ids=unused)
    if active and all(p.proof_satisfied is True for p in active):
        return replace(result, state=UnitState.SATISFIED, intrinsic_state=UnitState.SATISFIED,
            proof_satisfied=True, exception_ids=exceptions, unused_exception_ids=unused)
    pending = [phase for phase in active if phase.proof_satisfied is not True]
    if pending and all(phase.state == UnitState.WAIVED for phase in pending):
        return replace(result, state=UnitState.WAIVED, exception_ids=exceptions, unused_exception_ids=unused)
    if result.state == UnitState.MISSING and any(phase.proof_satisfied is True for phase in active):
        result = replace(result, state=UnitState.PARTIAL, intrinsic_state=UnitState.PARTIAL)
    return replace(result, exception_ids=exceptions, unused_exception_ids=unused)


def assess_effective_scenarios(snapshot, *, card_id, scenario_ids):
    """Test Card denominator: assigned native scenarios, never obligations/cards.

    Sufficiency comes exclusively from the shared scenario/contribution joins.
    A passing complement cannot hide a failure and a native passing result
    without the required delivery associations cannot receive automatic credit.
    """
    if not isinstance(card_id, str) or not card_id.strip():
        raise ValueError('delivery_scenario_assessment_scope_invalid')
    context = snapshot.effective_context
    if (not isinstance(context, EffectiveDeliveryContext) or not isinstance(scenario_ids, tuple)
        or not scenario_ids or len(scenario_ids) > 5000
        or any(not isinstance(identity, str) or not identity.strip() for identity in scenario_ids)):
        return DeliveryAssessment(snapshot.scope, AssessmentCompleteness(False, False, False, True), (),
            reasons=(FactualReason(ReadinessReason.SCOPE_UNAVAILABLE),), card_id=card_id, card_type=CardType.TEST)
    facts_complete = snapshot.complete if snapshot.facts_complete is None else snapshot.facts_complete
    proof_snapshot = replace(snapshot, waivers=(), complete=facts_complete)
    proof = evaluate_effective_facts(proof_snapshot, require_lifecycle=False)
    lifecycle = evaluate_effective_facts(proof_snapshot, require_lifecycle=True)
    by_scenario = {}
    for check in proof.scenario_checks:
        by_scenario.setdefault(check.scenario_id, []).append(check)

    def check_key(check):
        return (check.binding, check.contribution_card_id, check.criterion_id, check.scenario_id, check.implementation_id)

    completed = {check_key(check) for check in lifecycle.scenario_checks if check.satisfied}
    runs = {}
    native = context.native_verification
    foreign = False
    for run in native.runs if native is not None else ():
        if run.scope != snapshot.scope:
            foreign = True
            continue
        runs.setdefault(run.scenario_id, []).append(run)
    plans = {}
    for plan in context.required_scenarios or ():
        if isinstance(plan, RequiredVerificationScenario):
            plans.setdefault(plan.scenario_id, []).append(plan)
    population_known = bool(native and native.complete) and context.inventory.population_complete and not foreign
    unavailable = tuple(sorted(set(proof.blockers) & (_UNEVALUABLE | _VERIFICATION_UNEVALUABLE)))
    scope_resolved = True
    values, top_reasons = [], []
    for identity in sorted(set(scenario_ids)):
        current = runs.get(identity, ())
        digests = {run.scenario_sha256 for run in current if run.scenario_sha256}
        if len(digests) != 1 or any(not run.scenario_sha256 for run in current):
            # Never invent a semantic identity for an absent/malformed subject.
            population_known = scope_resolved = False
            top_reasons.append(FactualReason(ReadinessReason.SCOPE_UNAVAILABLE, scenario_ids=(identity,)))
            continue
        definitions = plans.get(identity, ())
        plan = definitions[0] if len(definitions) == 1 else None
        applicable = by_scenario.get(identity, ())
        phases = []
        for check in applicable:
            phase = _assessed_phase(satisfied=check.satisfied, lifecycle=check_key(check) in completed,
                reasons=tuple(replace(reason, implementation_ids=(check.implementation_id,) if check.implementation_id else (),
                    bindings=(check.binding,)) for reason in check.reasons), evidence_ids=check.test_ids)
            if unavailable or not facts_complete or foreign:
                phase = _assessed_phase(state=UnitState.UNKNOWN, reasons=(FactualReason(
                    ReadinessReason.ASSESSMENT_UNAVAILABLE,
                    native_reason_codes=unavailable or ('delivery_facts_incomplete',)),))
            phases.append(apply_delivery_exceptions(phase, scope=snapshot.scope, binding=check.binding,
                phase=DeliveryPhase.TEST, waivers=snapshot.waivers))
        assigned = plan is not None and card_id in plan.test_card_ids
        covered = {check.criterion_id for check in applicable}
        if not assigned or not applicable or not plan.criterion_ids or set(plan.criterion_ids) - covered:
            scope_resolved = False
            phases.append(_assessed_phase(reasons=(FactualReason(
                ReadinessReason.VERIFICATION_PLAN_MISSING if not assigned else ReadinessReason.SCOPE_ALLOCATION_UNRESOLVED,
                criterion_ids=tuple(sorted(set(plan.criterion_ids) - covered)) if plan else (),
                scenario_ids=(identity,)),)))
        if not phases:
            phases.append(_assessed_phase(reasons=(FactualReason(ReadinessReason.ASSESSMENT_UNAVAILABLE),)))
        verification = _combine_scenario_checks(phases)
        values.append(ScenarioAssessment(identity, next(iter(digests)), snapshot.scope.edition, card_id, verification))
    evaluation_complete = all(row.verification.state == UnitState.NOT_APPLICABLE
        or row.verification.proof_satisfied is not None for row in values)
    return DeliveryAssessment(snapshot.scope, AssessmentCompleteness(population_known, scope_resolved,
        evaluation_complete, True, True, evaluation_complete), (), tuple(top_reasons),
        card_id=card_id, card_type=CardType.TEST, scenarios=tuple(values))
