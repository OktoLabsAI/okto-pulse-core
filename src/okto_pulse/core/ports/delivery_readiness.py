"""Readiness facts from one authorized, read-only edition snapshot.

These are internal facts, not a transport response or an authorization token.
The use case reauthorizes access and filters detail visibility before rendering.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol

from okto_pulse.core.domain.delivery_diagnostics import ACTION_REGISTRY
from okto_pulse.core.domain.delivery_assessment import resolve_delivery_gate_policy as resolve_delivery_gate_policy
from okto_pulse.core.domain.delivery_evidence import DeliveryEvidenceSnapshot
from okto_pulse.core.domain.enums import CardStatus, CardType
from okto_pulse.core.domain.sdlc_registry import lifecycle_definition, transition_contracts, transition_permission_flag
from okto_pulse.core.models.delivery_readiness import ReadinessCursorBinding
from okto_pulse.core.ports.authentication import AuthorizationDenied
from okto_pulse.core.ports.permission_policy import DefaultPermissionPolicy, PermissionContext, PermissionSet


class ReadinessReadError(ValueError):
    def __init__(self, code: str, *, native_code: str | None = None, identity: str | None = None):
        super().__init__(code)
        self.code = code
        self.native_code = native_code
        self.identity = identity


class ReadinessAccessDenied(AuthorizationDenied):
    def __init__(self, operation: str):
        super().__init__('readiness_permission_denied')
        self.required_permission = operation


READINESS_PERMISSION_OPERATIONS = frozenset({
    'code_traceability.evidence.read', 'code_traceability.target.read',
    'code_traceability.investigation.read', 'spec.entity.read', 'spec.tests.read',
    'card.entity.read', 'agent.entity.read', 'guidelines.assessments.read',
    *(a.permission for a in ACTION_REGISTRY.values() if a.permission),
})


@dataclass(frozen=True, slots=True)
class ReadinessActor:
    """Authenticated identity only; cached permission claims confer no access."""
    actor_id: str
    actor_kind: str
    realm_id: str | None
    board_id: str | None = None


@dataclass(frozen=True, slots=True)
class ReadinessAccess:
    actor_id: str
    board_id: str
    realm_id: str
    allowed_operations: frozenset[str]
    spec_write_access: bool = False
    completion_states: frozenset[tuple[str, str]] = frozenset()

    def allows(self, operation: str) -> bool:
        return operation in self.allowed_operations

    def require(self, operation: str) -> None:
        if not self.allows(operation):
            raise ReadinessAccessDenied(operation)


def resolve_readiness_access(*, actor: ReadinessActor, board_id: str, realm_id: str,
                             permissions: PermissionSet, spec_write_access: bool) -> ReadinessAccess:
    """Project fresh native policy after the edition verifies Board membership.

    The aggregate and detail permissions remain independent. The returned facts
    contain no flags, identity-provider claims or credentials and authorize no
    subsequent mutation. Null permissions never imply trusted access here.
    """
    if not isinstance(permissions, PermissionSet):
        raise ReadinessAccessDenied('code_traceability.evidence.read')
    policy = DefaultPermissionPolicy()
    return ReadinessAccess(actor.actor_id, board_id, realm_id, frozenset(operation
        for operation in READINESS_PERMISSION_OPERATIONS
        if policy.evaluate(PermissionContext(operation=operation, permissions=permissions)).allowed),
        spec_write_access is True,
        frozenset((entity, state.value) for entity in ('spec', 'card')
            for state in lifecycle_definition(entity).status_enum
            if any(edge.to_status == 'done' and edge.visibility == 'public'
                for edge in transition_contracts(entity, state.value))
            and policy.evaluate(PermissionContext(
                operation=transition_permission_flag(entity, state.value, 'done'),
                entity=entity, state=state.value, permissions=permissions)).allowed))


class ReadinessClock(Protocol):
    def now(self) -> datetime: ...


class DeliveryReadinessCursorPort(Protocol):
    def encode(self, binding: ReadinessCursorBinding, offset: int) -> str: ...

    def decode(self, token: str, binding: ReadinessCursorBinding) -> int:
        """Authenticate actor/scope/filter and reject a changed projection base.

        No authorization is obtained from a token. The caller authorizes each
        read before supplying the current binding, never trusting cursor IDs.
        """
        ...


@dataclass(frozen=True, slots=True)
class ReadinessCard:
    id: str
    card_type: CardType
    status: CardStatus
    version: int
    scenario_ids: tuple[str, ...] | None
    assignee_id: str | None


@dataclass(frozen=True, slots=True)
class ReadinessScenarioProcedure:
    id: str
    title: str | None
    verification_method: str | None
    given: str | None
    when: str | None
    then: str | None
    notes: str | None
    criterion_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ReadinessNativeLifecycle:
    """Same-snapshot native read outcome, never a mutation authorization token.

    A short-circuit blocker proves blocking but not the total blocker count.
    Reason codes are native machine fields; no parsing of error prose.
    """
    card_id: str | None
    stored_status: str
    check_status: Literal['allowed', 'blocked', 'not_evaluated']
    reason_codes: tuple[str, ...]
    other_blocker_count: int | None = None
    blocking_codes: tuple[str, ...] = ()
    basis_digest: str | None = None
    required_inputs: tuple[str, ...] = ()
    # Persisted native completion observed in the authorized snapshot. This
    # records lifecycle history only, never current proof or write authority.
    completion_recorded: bool = False


@dataclass(frozen=True, slots=True)
class ReadinessSnapshot:
    facts: DeliveryEvidenceSnapshot
    snapshot_id: str
    evaluated_at: datetime
    spec_version: int
    spec_status: str
    cards: tuple[ReadinessCard, ...]
    scenarios: tuple[ReadinessScenarioProcedure, ...] = ()
    access: ReadinessAccess | None = None
    # Explicit capacity/unavailability, never a successfully truncated universe.
    unavailable_reason_codes: tuple[str, ...] = ()
    delivery_gate_mode: Literal['blocking', 'advisory'] = 'blocking'
    skip_delivery_evidence: bool = False
    native_lifecycle: ReadinessNativeLifecycle | None = None


class DeliveryReadinessReadPort(Protocol):
    async def load(self, *, board_id: str, spec_id: str, actor: ReadinessActor,
                   card_id: str | None = None) -> ReadinessSnapshot:
        """Load all related facts in one consistent batch, including other Cards.

        Resolve current membership and permissions in this same read snapshot,
        before loading evidence. Refuse denied access; never return an empty
        population or trust cached actor permissions. Return access facts for
        the Core's visibility and action projection.
        A requested foreign, missing or archived Card is a scope error, never
        an empty successful population. No writes, commits or lock_scope fence.
        Preserve native authentication, selection and reviewer authority. Retry
        a generation race at most once; never combine different generations.
        Fingerprint semantic inputs and effective validity, excluding clock time.
        """
        ...
