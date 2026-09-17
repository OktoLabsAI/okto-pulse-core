"""Deterministic delivery coverage over server-owned, authorized projections.

These internal facts are NOT request bodies or receipt verifiers. Application
adapters must authenticate receipts, resolve current heads and load a complete
same-scope obligation inventory before constructing a snapshot. No ORM, graph
provider, filesystem access, execution or state transitions occur here.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from okto_pulse.core.domain.enums import CardStatus, CardType, TestScenarioStatus


class DeliveryPhase(str, Enum):
    IMPLEMENTATION = "implementation"
    TEST = "test"


@dataclass(frozen=True, slots=True)
class DeliveryScope:
    board_id: str
    spec_id: str
    edition: int

    def __post_init__(self) -> None:
        if not self.board_id.strip() or not self.spec_id.strip():
            raise ValueError("delivery_scope_identity_required")
        if type(self.edition) is not int or self.edition < 1:
            raise ValueError("delivery_scope_edition_invalid")


@dataclass(frozen=True, slots=True)
class CardDeliveryScope:
    """Card-scoped addressing for the per-task delivery ledger.

    The snapshot scope itself remains ``DeliveryScope`` so the deterministic
    evaluator keeps comparing like with like; this type addresses the store
    surface (one task's bindings) without changing proof semantics.
    """

    board_id: str
    card_id: str
    spec_id: str
    spec_edition: int

    def __post_init__(self) -> None:
        if (
            not self.board_id.strip()
            or not self.card_id.strip()
            or not self.spec_id.strip()
        ):
            raise ValueError("delivery_scope_identity_required")
        if type(self.spec_edition) is not int or self.spec_edition < 1:
            raise ValueError("delivery_scope_edition_invalid")


@dataclass(frozen=True, slots=True)
class DeliveryBinding:
    obligation_ref: str
    semantic_sha256: str

    def __post_init__(self) -> None:
        if not self.obligation_ref.strip():
            raise ValueError("delivery_obligation_ref_required")
        if len(self.semantic_sha256) != 64 or any(
            char not in "0123456789abcdef" for char in self.semantic_sha256
        ):
            raise ValueError("delivery_obligation_digest_invalid")


@dataclass(frozen=True, slots=True)
class DeliveryObligation:
    binding: DeliveryBinding
    title: str


@dataclass(frozen=True, slots=True)
class ImplementationDeliveryFact:
    id: str
    scope: DeliveryScope
    card_id: str
    card_type: CardType
    card_status: CardStatus
    bindings: tuple[DeliveryBinding, ...]
    source_ref: str
    result_revision: str
    relative_path: str
    explanation: str
    receipt_id: str
    # Projection of an authenticated, non-revoked current target execution
    # record (not merely a planned target, receipt ID or agent-supplied flag).
    current_accepted_execution: bool
    actor_id: str
    symbol: str | None = None


@dataclass(frozen=True, slots=True)
class TestDeliveryFact:
    id: str
    scope: DeliveryScope
    card_id: str
    card_type: CardType
    card_status: CardStatus
    bindings: tuple[DeliveryBinding, ...]
    scenario_id: str
    result: TestScenarioStatus
    receipt_id: str
    # The current run must be authenticated against its scenario digest by
    # the existing test-evidence verifier; this says nothing about independent
    # review or whether the test proves the business behavior exhaustively.
    current_verified_run: bool
    actor_id: str
    verified_implementation_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DeliveryWaiverFact:
    id: str
    scope: DeliveryScope
    binding: DeliveryBinding
    phase: DeliveryPhase
    justification: str
    actor_id: str
    authorization_receipt_id: str
    current_authorized: bool


@dataclass(frozen=True, slots=True)
class DeliveryEvidenceSnapshot:
    scope: DeliveryScope
    obligations: tuple[DeliveryObligation, ...]
    implementations: tuple[ImplementationDeliveryFact, ...] = ()
    tests: tuple[TestDeliveryFact, ...] = ()
    waivers: tuple[DeliveryWaiverFact, ...] = ()
    complete: bool = False


@dataclass(frozen=True, slots=True)
class DeliveryCoverageRow:
    obligation: DeliveryObligation
    implementation_ids: tuple[str, ...]
    test_ids: tuple[str, ...]
    implementation_waiver_ids: tuple[str, ...]
    test_waiver_ids: tuple[str, ...]

    @property
    def implementation_satisfied(self) -> bool:
        return bool(self.implementation_ids or self.implementation_waiver_ids)

    @property
    def test_satisfied(self) -> bool:
        return bool(self.test_ids or self.test_waiver_ids)


@dataclass(frozen=True, slots=True)
class DeliveryCoverageEvaluation:
    rows: tuple[DeliveryCoverageRow, ...]
    blockers: tuple[str, ...]
    rejected_record_ids: tuple[str, ...]

    @property
    def allowed(self) -> bool:
        return not self.blockers


def _text(*values: str) -> bool:
    return all(isinstance(value, str) and bool(value.strip()) for value in values)


def evaluate_delivery_coverage(
    snapshot: DeliveryEvidenceSnapshot,
) -> DeliveryCoverageEvaluation:
    """Evaluate both obligations without mutating or auto-reopening any Spec.

    Spec status, advisory posture, greenfield context and existing Skip flags
    deliberately are not inputs. None can turn missing delivery proof into a
    successful result. Empty/incomplete inventories fail closed until explicitly
    resolved by the application, never as vacuous 100% coverage.
    """
    blockers: list[str] = []
    if snapshot.complete is not True:
        blockers.append("delivery_projection_incomplete")
    if not snapshot.obligations:
        blockers.append("delivery_obligations_missing")
    refs = [item.binding.obligation_ref for item in snapshot.obligations]
    if len(set(refs)) != len(refs):
        blockers.append("delivery_obligations_ambiguous")
    all_records = (*snapshot.implementations, *snapshot.tests, *snapshot.waivers)
    ids = [item.id for item in all_records]
    if len(set(ids)) != len(ids):
        blockers.append("delivery_record_identity_ambiguous")
    expected = {item.binding for item in snapshot.obligations}
    implementation_index: dict[DeliveryBinding, set[str]] = {}
    test_index: dict[DeliveryBinding, set[str]] = {}
    tested_implementation_index: dict[DeliveryBinding, set[str]] = {}
    waiver_index: dict[tuple[DeliveryBinding, DeliveryPhase], set[str]] = {}
    rejected: set[str] = set()

    for item in snapshot.implementations:
        valid = (
            item.scope == snapshot.scope
            and item.card_type in (CardType.NORMAL, CardType.BUG)
            and item.card_status == CardStatus.DONE
            and item.current_accepted_execution is True
            and _text(
                item.id,
                item.card_id,
                item.source_ref,
                item.result_revision,
                item.relative_path,
                item.explanation,
                item.receipt_id,
                item.actor_id,
            )
        )
        matches = expected.intersection(item.bindings) if valid else set()
        if not matches:
            rejected.add(item.id)
        for binding in matches:
            implementation_index.setdefault(binding, set()).add(item.id)

    for item in snapshot.tests:
        valid = (
            item.scope == snapshot.scope
            and item.card_type == CardType.TEST
            and item.card_status == CardStatus.DONE
            and item.result == TestScenarioStatus.PASSED
            and item.current_verified_run is True
            and _text(
                item.id, item.card_id, item.scenario_id, item.receipt_id, item.actor_id
            )
        )
        # A successful test of a previous implementation must not qualify a
        # newer delivery merely because the requirement text stayed unchanged.
        matches = (
            {
                binding
                for binding in expected.intersection(item.bindings)
                if implementation_index.get(binding, set()).intersection(
                    item.verified_implementation_ids
                )
            }
            if valid
            else set()
        )
        if not matches:
            rejected.add(item.id)
        for binding in matches:
            test_index.setdefault(binding, set()).add(item.id)
            tested_implementation_index.setdefault(binding, set()).update(
                implementation_index[binding].intersection(
                    item.verified_implementation_ids
                )
            )

    # A new independent implementation must not inherit coverage from a test
    # of a different target. Several test cards may jointly cover the delivery.
    for binding, implementation_ids in implementation_index.items():
        if not implementation_ids <= tested_implementation_index.get(binding, set()):
            test_index.pop(binding, None)

    for item in snapshot.waivers:
        valid = (
            item.scope == snapshot.scope
            and item.binding in expected
            and isinstance(item.phase, DeliveryPhase)
            and item.current_authorized is True
            and _text(
                item.id,
                item.justification,
                item.actor_id,
                item.authorization_receipt_id,
            )
        )
        if not valid:
            rejected.add(item.id)
            continue
        waiver_index.setdefault((item.binding, item.phase), set()).add(item.id)

    rows = tuple(
        DeliveryCoverageRow(
            obligation=item,
            implementation_ids=tuple(
                sorted(implementation_index.get(item.binding, ()))
            ),
            test_ids=tuple(sorted(test_index.get(item.binding, ()))),
            implementation_waiver_ids=tuple(
                sorted(
                    waiver_index.get((item.binding, DeliveryPhase.IMPLEMENTATION), ())
                )
            ),
            test_waiver_ids=tuple(
                sorted(waiver_index.get((item.binding, DeliveryPhase.TEST), ()))
            ),
        )
        for item in snapshot.obligations
    )
    if any(not row.implementation_satisfied for row in rows):
        blockers.append("delivery_implementation_missing")
    if any(not row.test_satisfied for row in rows):
        blockers.append("delivery_test_result_missing")
    return DeliveryCoverageEvaluation(rows, tuple(blockers), tuple(sorted(rejected)))
