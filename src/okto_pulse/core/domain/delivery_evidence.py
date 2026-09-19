"""Deterministic delivery coverage over server-owned, authorized projections.

These internal facts are NOT request bodies or receipt verifiers. Application
adapters must authenticate receipts, resolve current heads and load a complete
same-scope obligation inventory before constructing a snapshot. No ORM, graph
provider, filesystem access, execution or state transitions occur here.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Literal

from okto_pulse.core.domain.enums import CardStatus, CardType, TestScenarioStatus


class DeliveryPhase(str, Enum):
    IMPLEMENTATION = "implementation"
    TEST = "test"


def require_delivery_entry_card_type(card_type: object, kind: str) -> None:
    """A batch groups writes; it cannot borrow another Card type's authority."""
    card_type = getattr(card_type, "value", card_type)
    allowed = {
        "progress": {"normal", "bug", "test"},
        "implementation": {"normal", "bug"},
        "test": {"test"},
    }
    if card_type not in allowed.get(kind, set()):
        raise ValueError("delivery_entry_card_type_invalid")


def require_delivery_batch_state(card: object) -> None:
    status = getattr(card, "status", None)
    # Done keeps the existing authorized proof-association repair path. A
    # progress entry has its separate execution-only predicate. Neither kind
    # may start rework or mutate a frozen submission through a batch.
    if getattr(card, "archived", None) is not False or getattr(status, "value", status) not in {
        "started", "in_progress", "done",
    }:
        raise ValueError("delivery_batch_card_frozen")


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
class DeliveryContribution:
    binding: DeliveryBinding
    contribution: Literal["partial", "complete"]
    execution_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.contribution not in {"partial", "complete"}:
            raise ValueError("delivery_contribution_invalid")


def read_delivery_contributions(
    payload: dict, bindings: tuple[DeliveryBinding, ...]
) -> tuple[DeliveryContribution, ...] | None:
    """Decode server-persisted declarations; absence alone denotes legacy.

    Never infer complete from a receipt, a missing new-format field or an empty
    collection. Declaration data does not establish receipt validity.
    """
    if "contribution_contract_version" not in payload and "contributions" not in payload:
        return None
    rows = payload.get("contributions")
    version = payload.get("contribution_contract_version")
    if version not in {"card-binding-contribution/v1", "card-binding-contribution/v2"} or not isinstance(rows, list) or not rows:
        raise ValueError("delivery_contribution_payload_invalid")
    by_ref = {binding.obligation_ref: binding for binding in bindings}
    if len(by_ref) != len(bindings) or len(rows) != len(bindings):
        raise ValueError("delivery_contribution_payload_invalid")
    seen = set()
    result = []
    execution_count = 0
    for row in rows:
        fields = {"obligation_ref", "contribution"} | ({"execution_ids"} if version.endswith("/v2") else set())
        if not isinstance(row, dict) or set(row) != fields:
            raise ValueError("delivery_contribution_payload_invalid")
        ref = row["obligation_ref"]
        if not isinstance(ref, str) or ref not in by_ref or ref in seen:
            raise ValueError("delivery_contribution_payload_invalid")
        seen.add(ref)
        execution_ids = row.get("execution_ids", [])
        if version.endswith("/v2") and (
            not isinstance(execution_ids, list) or not 1 <= len(execution_ids) <= 100
            or any(not isinstance(identity, str) or not identity.strip() or len(identity) > 512 for identity in execution_ids)
            or len(set(execution_ids)) != len(execution_ids)
        ):
            raise ValueError("delivery_execution_set_invalid")
        execution_count += len(execution_ids)
        result.append(DeliveryContribution(by_ref[ref], row["contribution"], tuple(execution_ids)))
    if version.endswith("/v2") and execution_count + len(rows) > 200:
        raise ValueError("delivery_execution_set_invalid")
    return tuple(result)


def delivery_execution_ids(payload: dict, selected_bindings: tuple[DeliveryBinding, ...] | None = None) -> tuple[str, ...]:
    """Exact immutable receipt references, optionally restricted to tested bindings."""
    if payload.get("contribution_contract_version") == "card-binding-contribution/v2":
        bindings = tuple(DeliveryBinding(**value) for value in payload.get("bindings", []))
        contributions = read_delivery_contributions(payload, bindings)
        return tuple(sorted({identity for item in contributions
            if selected_bindings is None or item.binding in selected_bindings
            for identity in item.execution_ids}))
    identity = payload.get("execution_id")
    return (identity,) if isinstance(identity, str) and identity.strip() else ()


@dataclass(frozen=True, slots=True)
class ImplementationExecutionProof:
    execution_id: str
    target_id: str
    target_revision: int
    source_ref: str
    result_revision: str
    relative_path: str
    current_accepted_execution: bool
    symbol: str | None = None
    blocking_progress_ids: tuple[str, ...] = ()
    blocking_progress_truncated: bool = False


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
    # None is historical compatibility, not an authored complete declaration.
    contributions: tuple[DeliveryContribution, ...] | None = None
    executions: tuple[ImplementationExecutionProof, ...] | None = None
    blocking_progress_ids: tuple[str, ...] = ()
    blocking_progress_truncated: bool = False


def implementation_binding_proof_issue(fact: ImplementationDeliveryFact, binding: DeliveryBinding) -> str | None:
    """Check only the declared receipt set; no head is transferred to a new binding."""
    if binding not in fact.bindings:
        return "delivery_execution_set_unresolved"
    if fact.executions is None:
        valid = fact.current_accepted_execution is True and _text(
            fact.source_ref, fact.result_revision, fact.relative_path, fact.receipt_id,
        )
        return None if valid else "delivery_accepted_committed_task_execution_required"
    selected = [item for item in fact.contributions or () if item.binding == binding]
    if len(selected) != 1 or not selected[0].execution_ids:
        return "delivery_execution_set_unresolved"
    by_id = {proof.execution_id: proof for proof in fact.executions}
    ids = selected[0].execution_ids
    if len(by_id) != len(fact.executions) or len(set(ids)) != len(ids) or any(identity not in by_id for identity in ids):
        return "delivery_execution_set_unresolved"
    proofs = [by_id[identity] for identity in ids]
    # Different commits/repos are not evidence of a compatible integrated base.
    # Each binding can independently name a different observed immutable base.
    if not all(proof.current_accepted_execution is True
            and type(proof.target_revision) is int and proof.target_revision >= 1
            and _text(proof.execution_id, proof.target_id, proof.source_ref, proof.result_revision, proof.relative_path)
            for proof in proofs):
        return "delivery_accepted_committed_task_execution_required"
    if len({proof.target_id for proof in proofs}) != len(proofs):
        return "delivery_execution_set_ambiguous"
    if len({(proof.source_ref, proof.result_revision) for proof in proofs}) != 1:
        return "delivery_execution_base_conflict"
    return None


def implementation_binding_proof_current(fact: ImplementationDeliveryFact, binding: DeliveryBinding) -> bool:
    return implementation_binding_proof_issue(fact, binding) is None


def implementation_binding_ready(fact: ImplementationDeliveryFact, binding: DeliveryBinding) -> bool:
    return implementation_binding_complete(fact, binding) and implementation_binding_proof_current(fact, binding)


def implementation_binding_complete(fact: ImplementationDeliveryFact, binding: DeliveryBinding) -> bool:
    """One completion predicate for the pre-Done gate and final rollup.

    Two partial records do not accumulate completion. Technical admission,
    scope, lifecycle and review remain independent predicates of the caller.
    """
    if binding not in fact.bindings:
        return False
    if fact.contributions is None:
        return True  # Preserve the tested legacy verdict without rewriting it.
    declarations = {item.binding: item.contribution for item in fact.contributions}
    return (
        len(declarations) == len(fact.contributions) == len(fact.bindings)
        and set(declarations) == set(fact.bindings)
        and all(value in {"partial", "complete"} for value in declarations.values())
        and declarations.get(binding) == "complete"
    )


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
            and _text(
                item.id,
                item.card_id,
                item.explanation,
                item.actor_id,
            )
        )
        matches = {
            binding for binding in expected.intersection(item.bindings)
            if implementation_binding_ready(item, binding)
        } if valid else set()
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
