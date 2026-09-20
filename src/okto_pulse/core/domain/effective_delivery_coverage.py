"""Coverage for the jointly adopted contract, over server-owned relational facts.

The legacy evaluator stays unchanged. Adoption must select this evaluator and
the effective inventory together; a historical binding has no authored scope
attestation and cannot acquire one merely by being read after an upgrade.
"""

from dataclasses import dataclass, replace

from okto_pulse.core.domain.delivery_evidence import (
    DeliveryBinding,
    DeliveryEvidenceSnapshot,
    DeliveryCoverageEvaluation,
    DeliveryCoverageRow,
    DeliveryObligation,
    ImplementationDeliveryFact,
    TestDeliveryFact,
    evaluate_delivery_coverage,
    implementation_binding_ready,
)
from okto_pulse.core.domain.effective_delivery_inventory import (
    EffectiveDeliveryInventory,
)
from okto_pulse.core.domain.enums import CardStatus, CardType


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
    """Read immutable server-authored scopes without upgrading historical rows."""
    if "scope_contract_version" not in payload and "contribution_scopes" not in payload:
        return ScopedImplementationFact(fact, ())
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


@dataclass(frozen=True, slots=True)
class EffectiveDeliveryCoverage:
    rows: tuple[EffectiveDeliveryCoverageRow, ...]
    blockers: tuple[str, ...]
    rejected_record_ids: tuple[str, ...]

    @property
    def allowed(self):
        return not self.blockers


@dataclass(frozen=True, slots=True)
class EffectiveDeliveryContext:
    inventory: EffectiveDeliveryInventory
    implementations: tuple[ScopedImplementationFact, ...]
    tests: tuple[ScopedTestFact, ...]
    admitted_methods: frozenset[str] | None


@dataclass(frozen=True, slots=True)
class ScopedDeliveryCoverageRow(DeliveryCoverageRow):
    contributions_complete: bool
    criteria_complete: bool
    required_card_ids: tuple[str, ...]
    missing_card_ids: tuple[str, ...]
    missing_criteria: tuple[tuple[str, str], ...]

    @property
    def implementation_satisfied(self):
        return self.contributions_complete

    @property
    def test_satisfied(self):
        return self.criteria_complete


def implementation_scope_current(snapshot, fact, binding):
    context = snapshot.effective_context
    if context is None:
        return True
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
    if context is None:
        return True
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
    return not contribution.criterion_ids or bool(set(contribution.criterion_ids).intersection(observation.criterion_ids))


def evaluate_adopted_snapshot(snapshot):
    context = snapshot.effective_context
    if not isinstance(context, EffectiveDeliveryContext):
        return DeliveryCoverageEvaluation((), ('delivery_effective_context_unavailable',), ())
    result = evaluate_effective_delivery_coverage(inventory=context.inventory, snapshot=snapshot,
        implementations=context.implementations, tests=context.tests, admitted_methods=context.admitted_methods)
    obligations = {item.binding: item for item in snapshot.obligations}
    return DeliveryCoverageEvaluation(tuple(ScopedDeliveryCoverageRow(
        obligations[row.binding], row.implementation_ids, row.test_ids,
        row.implementation_waiver_ids, row.test_waiver_ids,
        row.implementation_satisfied, row.test_satisfied, row.required_card_ids,
        row.missing_card_ids, row.missing_criteria,
    ) for row in result.rows if row.binding in obligations), result.blockers, result.rejected_record_ids)


def evaluate_effective_delivery_coverage(
    *,
    inventory: EffectiveDeliveryInventory,
    snapshot: DeliveryEvidenceSnapshot,
    implementations: tuple[ScopedImplementationFact, ...],
    tests: tuple[ScopedTestFact, ...],
    admitted_methods: frozenset[str] | None,
) -> EffectiveDeliveryCoverage:
    """Require every approved Card contribution and every selected criterion.

    Definitions, contribution scopes and source receipts protect distinct facts.
    No percentage or union of partial declarations completes a contribution.
    Several authenticated tests may jointly cover it, but a passing functional
    condition cannot conceal a missing technical/operational condition.
    """
    snapshot = replace(snapshot, effective_context=None)
    blockers = set()
    # Count expanded associations before evaluating them. A bounded failure is
    # unknown coverage, not a truncated population that could appear complete.
    if (
        len(inventory.rows) > 5000
        or len(snapshot.implementations) + len(snapshot.tests) + len(snapshot.waivers)
        > 10000
        or sum(
            len(row.contributions)
            + sum(len(c.criterion_ids) for c in row.contributions)
            for row in inventory.rows
        )
        + sum(len(row.scopes) for row in implementations)
        + sum(
            len(row.criterion_ids)
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

    # Reuse source/lifecycle/revocation and waiver verdicts. The adopted
    # contract adds scope/criterion checks, never a weaker proof predicate.
    baseline = evaluate_delivery_coverage(snapshot)
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
            or fact.card_status != CardStatus.DONE
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
    relevant_tests = set()
    by_test_binding = {}
    for test in tests:
        for binding in set(test.fact.bindings):
            by_test_binding.setdefault(binding, []).append(test)
    checks = 0
    for binding, obligation in expected.items():
        base = baseline_rows.get(binding)
        required = {item.card_id: item for item in obligation.contributions}
        if len(required) != len(obligation.contributions) or not required:
            blockers.add("delivery_contribution_allocation_invalid")
        by_card = accepted.get(binding, {})
        ids = {identity for values in by_card.values() for identity in values}
        missing_cards = set(required) - set(by_card)
        observed = {identity: set() for identity in ids}
        tested = set()
        test_ids = set()
        for test in by_test_binding.get(binding, ()):
            fact = test.fact
            # The legacy engine proves authentication, current passing result,
            # exact implementation IDs and Done. It may withhold its aggregate
            # test_ids if another implementation lacks tests, so evaluate each
            # admitted run against only the IDs that it explicitly names.
            named = ids.intersection(fact.verified_implementation_ids)
            checks += len(named) * max(1, len(test.criterion_ids))
            if checks > 20000:
                return EffectiveDeliveryCoverage(
                    (), ("delivery_effective_resolution_limit",), ()
                )
            if (
                not named
                or not isinstance(test.verification_method, str)
                or test.verification_method not in admitted_methods
                or len(set(test.criterion_ids)) != len(test.criterion_ids)
                or any(
                    not isinstance(identity, str) or not identity.strip()
                    for identity in test.criterion_ids
                )
            ):
                continue
            check = evaluate_delivery_coverage(
                DeliveryEvidenceSnapshot(
                    snapshot.scope,
                    (DeliveryObligation(binding, binding.obligation_ref),),
                    tuple(
                        actual_implementations[identity] for identity in sorted(named)
                    ),
                    (fact,),
                    complete=snapshot.complete,
                )
            )
            if not check.rows[0].test_satisfied:
                continue
            relevant = False
            for identity in named:
                planned = required[actual_implementations[identity].card_id]
                criteria = set(planned.criterion_ids)
                covered = criteria.intersection(test.criterion_ids)
                if criteria and not covered:
                    continue
                observed[identity].update(covered)
                tested.add(identity)
                relevant = True
            if relevant:
                test_ids.add(fact.id)
                relevant_tests.add(fact.id)
        missing_criteria = {
            (identity, criterion)
            for identity in ids
            for criterion in required[
                actual_implementations[identity].card_id
            ].criterion_ids
            if criterion not in observed[identity]
        }
        implementation_waivers = base.implementation_waiver_ids if base else ()
        test_waivers = base.test_waiver_ids if base else ()
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
            )
        )
    if any(not row.implementation_satisfied for row in result):
        blockers.add("delivery_implementation_missing")
    if any(not row.test_satisfied for row in result):
        blockers.add("delivery_test_result_missing")
    rejected.update(set(actual_tests) - relevant_tests)
    return EffectiveDeliveryCoverage(
        tuple(result), tuple(sorted(blockers)), tuple(sorted(rejected))
    )
