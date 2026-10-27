"""Closed deterministic delivery causes and action dependencies.

Input is already evaluated, authorized native context. This module never checks
receipts, executes an action, reads a repository or changes a score/authority.
"""

from dataclasses import dataclass, replace
from functools import lru_cache
from enum import StrEnum
import heapq
import json
from types import MappingProxyType

from okto_pulse.core.domain.delivery_assessment import (
    DeliveryAssessment, FactualReason, ReadinessIntegrityError, ReadinessPhase, ReadinessReason, UnitState,
)
from okto_pulse.core.domain.delivery_evidence import DeliveryScope
from okto_pulse.core.domain.delivery_inventory import delivery_digest
from okto_pulse.core.domain.enums import CardType


class DiagnosticIntegrityError(ReadinessIntegrityError):
    """An unmapped/inconsistent fact cannot become an empty diagnostic."""


class ActionKind(StrEnum):
    RESOLVE_SCOPE = 'resolve_scope'
    IMPLEMENT_CONTRIBUTION = 'implement_contribution'
    COMPLETE_CONTRIBUTION = 'complete_contribution'
    ASSOCIATE_IMPLEMENTATION = 'associate_implementation'
    REFRESH_IMPLEMENTATION_PROOF = 'refresh_implementation_proof'
    RESOLVE_MATERIAL_CHANGE = 'resolve_material_change'
    EXECUTE_VERIFICATION = 'execute_verification'
    ASSOCIATE_VERIFICATION = 'associate_verification'
    RESOLVE_VERIFICATION_CONFLICT = 'resolve_verification_conflict'
    REVIEW_DECISION = 'review_decision'
    COMPLETE_LIFECYCLE = 'complete_lifecycle'
    RESOLVE_NATIVE_GATE = 'resolve_native_gate'
    INSPECT_LIFECYCLE = 'inspect_lifecycle'
    RESTORE_EVALUATION = 'restore_evaluation'
    REQUEST_AUTHORIZED_DECISION = 'request_authorized_decision'


class ActionState(StrEnum):
    READY = 'ready'
    WAITING_DEPENDENCY = 'waiting_dependency'
    REQUIRES_INPUT = 'requires_input'
    REQUIRES_HUMAN = 'requires_human'
    FORBIDDEN = 'forbidden'
    UNAVAILABLE = 'unavailable'


@dataclass(frozen=True, slots=True)
class ActionDefinition:
    kind: ActionKind
    message: str
    execution_surface: str
    priority: int
    pulse_tool: str | None = None
    permission: str | None = None
    argument_fields: tuple[str, ...] = ()
    required_arguments: tuple[str, ...] = ()
    preconditions: tuple[str, ...] = ()


def _action(kind, message, surface, priority, tool=None, permission=None, arguments=(), required=()):
    preconditions = {
        'associate_implementation': ('admitted_execution',),
        'associate_verification': ('authenticated_current_result',),
        'execute_verification': ('native_scenario', 'implementation_base'),
        'review_decision': ('native_decision_plan',),
    }.get(kind, ())
    return ActionDefinition(ActionKind(kind), message, surface, priority, tool, permission, arguments, required, preconditions)


ACTION_REGISTRY = MappingProxyType({item.kind: item for item in (
    _action('resolve_scope', 'Corrigir a qualificação ou alocação indicada no fluxo nativo.', 'read_only', 1),
    _action('implement_contribution', 'Implementar e observar a contribuição delimitada.', 'external_agent', 3),
    _action('complete_contribution', 'Concluir a contribuição e registrar prova real de conclusão.', 'external_agent', 3),
    _action('associate_implementation', 'Associar a prova de implementação já disponível.', 'pulse_tool', 0,
        'okto_pulse_record_delivery_evidence', 'code_traceability.target.execution_submit',
        ('board_id', 'card_id', 'spec_id', 'evidence'), ('board_id', 'card_id', 'spec_id', 'evidence')),
    _action('refresh_implementation_proof', 'Atualizar somente a observação de implementação afetada.', 'external_agent', 1),
    _action('resolve_material_change', 'Concluir ou reobservar a alteração material registrada.', 'external_agent', 1),
    _action('execute_verification', 'Executar a verificação existente na base exigida e registrar o resultado real.', 'external_agent', 4,
        permission='spec.tests.execute',
        arguments=('board_id', 'spec_id', 'card_id', 'spec_edition', 'scenarios', 'implementation_bases'),
        required=('scenarios', 'implementation_bases')),
    _action('associate_verification', 'Associar o resultado válido às implementações exatas, sem repetir a execução.', 'pulse_tool', 0,
        'okto_pulse_record_delivery_evidence', 'spec.tests.execute',
        ('board_id', 'card_id', 'spec_id', 'evidence'), ('board_id', 'card_id', 'spec_id', 'evidence')),
    _action('resolve_verification_conflict', 'Reconciliar ou substituir os resultados pelos controles nativos.', 'read_only', 1),
    _action('review_decision', 'Consultar as fontes e inspeções exigidas pelo plano da Decision.', 'read_only', 2,
        'okto_pulse_get_decision_reviews', 'code_traceability.evidence.read',
        ('board_id', 'spec_id'), ('board_id', 'spec_id')),
    _action('complete_lifecycle', 'Concluir o card pela transição nativa disponível após conferir seus gates.', 'read_only', 5),
    _action('resolve_native_gate', 'Resolver o gate nativo identificado antes da conclusão.', 'read_only', 2),
    _action('inspect_lifecycle', 'Consultar as transições permitidas e seus gates para avançar a partir do estado atual.', 'read_only', 1,
        'okto_pulse_get_allowed_transitions', 'spec.entity.read',
        ('board_id', 'entity_type', 'entity_id'), ('board_id', 'entity_type', 'entity_id')),
    _action('restore_evaluation', 'Restabelecer a leitura autorizada e consultar novamente a entrega.', 'read_only', 1,
        'okto_pulse_get_delivery_evidence', 'code_traceability.evidence.read',
        ('board_id', 'spec_id', 'card_id', 'view'), ('board_id', 'spec_id', 'view')),
    _action('request_authorized_decision', 'Solicitar a decisão à autoridade nativa responsável.', 'human_decision', 6),
)})


@dataclass(frozen=True, slots=True)
class ReasonDefinition:
    code: ReadinessReason
    severity: str
    phases: tuple[ReadinessPhase, ...]
    message: str
    actions: tuple[ActionKind, ...]
    preconditions: tuple[str, ...]
    pinpoint_fields: tuple[str, ...]


def _reason(code, message, actions, *, phase='verification', severity='high', preconditions=(), fields=()):
    phases = tuple(ReadinessPhase(value) for value in phase.split(','))
    return ReasonDefinition(ReadinessReason(code), severity, phases, message,
        tuple(ActionKind(value) for value in actions), preconditions, fields)


REASON_REGISTRY = MappingProxyType({item.code: item for item in (
    _reason('scope_unavailable', 'O escopo de {subject} não pôde ser lido integralmente.', ('restore_evaluation',), phase='implementation,verification'),
    _reason('scope_allocation_unresolved', 'A responsabilidade por {subject} precisa ser resolvida.', ('resolve_scope',), phase='implementation,verification', fields=('entity_id',)),
    _reason('scope_contract_changed', 'A definição ou contribuição de {subject} mudou.', ('refresh_implementation_proof',), phase='implementation,verification', fields=('entity_id', 'record_id')),
    _reason('implementation_missing', 'Falta prova completa de implementação de {subject}.', ('implement_contribution',), phase='implementation', preconditions=('native_contribution',), fields=('entity_id', 'target_id')),
    _reason('implementation_partial', 'A contribuição de {subject} ainda está parcial.', ('complete_contribution',), phase='implementation', preconditions=('native_contribution',), fields=('entity_id', 'record_id')),
    _reason('implementation_association_missing', 'Existe prova de {subject}, mas falta associá-la.', ('associate_implementation',), phase='implementation', preconditions=('admitted_execution',), fields=('execution_id',)),
    _reason('implementation_proof_revoked', 'A prova de implementação de {subject} foi revogada.', ('refresh_implementation_proof',), phase='implementation', fields=('record_id', 'execution_id')),
    _reason('implementation_proof_stale', 'A prova de implementação de {subject} perdeu validade.', ('refresh_implementation_proof',), phase='implementation', fields=('target_id', 'execution_id')),
    _reason('material_progress_unresolved', 'A alteração material de {subject} ainda invalida a base.', ('resolve_material_change',), phase='implementation', fields=('record_id', 'target_id')),
    _reason('verification_plan_missing', 'A verificação aplicável de {subject} precisa ser qualificada.', ('resolve_scope',), fields=('entity_id', 'scenario_id')),
    _reason('verification_not_run', 'O cenário {subject} ainda não possui execução válida.', ('execute_verification',), preconditions=('native_scenario', 'implementation_base'), fields=('scenario_id',)),
    _reason('verification_failed', 'A verificação de {subject} falhou na base atual.', ('implement_contribution', 'execute_verification', 'review_decision'), fields=('scenario_id', 'observation_id')),
    _reason('verification_inconclusive', 'A observação de {subject} não permite concluir a verificação.', ('execute_verification', 'review_decision'), severity='medium', fields=('scenario_id', 'observation_id')),
    _reason('verification_conflict', 'Há resultados correntes incompatíveis para {subject}.', ('resolve_verification_conflict', 'review_decision'), fields=('scenario_id', 'record_id')),
    _reason('verification_association_missing', 'O resultado válido de {subject} ainda não está associado à entrega.', ('associate_verification',), preconditions=('authenticated_current_result',), fields=('scenario_id', 'record_id')),
    _reason('verification_base_mismatch', 'A base observada por {subject} difere da base entregue.', ('associate_verification', 'execute_verification'), preconditions=('implementation_base',), fields=('source_ref', 'revision', 'scenario_id')),
    _reason('verification_base_unresolved', 'A fonte ou revisão observada por {subject} não pôde ser resolvida.', ('refresh_implementation_proof',), fields=('source_ref', 'scenario_id')),
    _reason('verification_receipt_invalid', 'O recibo de {subject} não é válido para este escopo.', ('execute_verification', 'review_decision'), fields=('scenario_id', 'record_id')),
    _reason('verification_method_unsupported', 'O método de verificação de {subject} está indisponível.', ('restore_evaluation', 'request_authorized_decision'), fields=('scenario_id',)),
    _reason('verification_dependency_pending', 'A verificação de {subject} aguarda a implementação ou base exigida.', ('execute_verification',), preconditions=('implementation_base',), fields=('scenario_id', 'target_id')),
    _reason('decision_verification_pending', 'A Decision {subject} aguarda as provas ou inspeções do seu plano.', ('review_decision',), preconditions=('native_decision_plan',), fields=('entity_id', 'record_id')),
    _reason('applicable_obligation_waived', 'Uma dispensa substitui a prova de {subject}; isso não concede cobertura.', (), phase='implementation,verification', severity='medium', fields=('record_id',)),
    _reason('lifecycle_card_open', 'As provas de {subject} estão disponíveis; falta encerrar o card.', ('complete_lifecycle',), phase='lifecycle', severity='medium', preconditions=('native_transition',), fields=('entity_id',)),
    _reason('lifecycle_gate_pending', 'Um gate nativo ainda impede a conclusão de {subject}.', ('resolve_native_gate',), phase='lifecycle', fields=('entity_id',)),
    _reason('lifecycle_transition_required', 'O estado atual de {subject} não permite conclusão direta.', ('inspect_lifecycle',), phase='lifecycle', severity='medium', fields=('entity_id',)),
    _reason('assessment_unavailable', 'Não foi possível avaliar {subject} com os fatos ou verificadores disponíveis.', ('restore_evaluation',), phase='implementation,verification,lifecycle'),
    _reason('historical_completion_diverged', 'A conclusão histórica de {subject} diverge da qualificação atual.', ('resolve_native_gate',), phase='lifecycle', fields=('entity_id',)),
)})


def reason_definition(code):
    try:
        return REASON_REGISTRY[ReadinessReason(code)]
    except (ValueError, KeyError, TypeError) as exc:
        raise DiagnosticIntegrityError('readiness_reason_unmapped') from exc


def validate_diagnostic_registry(tool_names=None):
    if set(REASON_REGISTRY) != set(ReadinessReason) or set(ACTION_REGISTRY) != set(ActionKind):
        raise DiagnosticIntegrityError('readiness_registry_incomplete')
    if any(action not in ACTION_REGISTRY for definition in REASON_REGISTRY.values() for action in definition.actions):
        raise DiagnosticIntegrityError('readiness_action_unmapped')
    if tool_names is not None and any(definition.pulse_tool and definition.pulse_tool not in tool_names
        for definition in ACTION_REGISTRY.values()):
        raise DiagnosticIntegrityError('readiness_tool_unregistered')


@dataclass(frozen=True, slots=True, order=True)
class DiagnosticUnit:
    unit: str
    ref: str
    semantic_sha256: str
    spec_edition: int
    card_id: str = ''


@dataclass(frozen=True, slots=True)
class CauseOrigin:
    kind: str
    identity: str
    # Exact canonical source/revision pairs, not a label inferred from text.
    expected_bases: tuple[tuple[str, str], ...] = ()
    observed_bases: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class ActionCandidate:
    kind: ActionKind
    # Canonical JSON keeps nested known arguments immutable. Nothing executes it.
    arguments_json: str = '{}'
    context_refs: tuple[str, ...] = ()
    responsible_card_ids: tuple[str, ...] = ()
    missing_inputs: tuple[str, ...] = ()
    permission_allowed: bool | None = None
    human_required: bool = False
    capability_available: bool = True
    # Established by authorized native facts, never inferred from prose.
    satisfied_preconditions: tuple[str, ...] = ()

    def __post_init__(self):
        try:
            definition = ACTION_REGISTRY[ActionKind(self.kind)]
            arguments = json.loads(self.arguments_json, object_pairs_hook=_unique_json_object)
            if not isinstance(arguments, dict) or not set(arguments) <= set(definition.argument_fields):
                raise ValueError('fields')
            normalized = json.dumps(arguments, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
        except (KeyError, ValueError, TypeError) as exc:
            raise DiagnosticIntegrityError('readiness_action_arguments_invalid') from exc
        object.__setattr__(self, 'arguments_json', normalized)
        object.__setattr__(self, 'kind', ActionKind(self.kind))


def _unique_json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate_argument')
        result[key] = value
    return result


@dataclass(frozen=True, slots=True)
class CauseOccurrence:
    code: ReadinessReason
    phase: ReadinessPhase
    intrinsic_state: UnitState
    origin: CauseOrigin
    units: tuple[DiagnosticUnit, ...]
    actions: tuple[ActionCandidate, ...] = ()
    native_reason_codes: tuple[str, ...] = ()
    criterion_ids: tuple[str, ...] = ()
    scenario_ids: tuple[str, ...] = ()
    blocks_proof: bool = True
    # None means the real native transition gate was not evaluated.
    blocks_target_transition: bool | None = None


@dataclass(frozen=True, slots=True)
class DiagnosticCause:
    id: str
    code: ReadinessReason
    phases: tuple[ReadinessPhase, ...]
    origin: CauseOrigin
    units: tuple[DiagnosticUnit, ...]
    intrinsic_state: UnitState
    message_code: str
    message: str
    native_reason_codes: tuple[str, ...]
    criterion_ids: tuple[str, ...]
    scenario_ids: tuple[str, ...]
    blocks_proof: bool
    blocks_target_transition: bool | None
    action_ids: tuple[str, ...] = ()
    depends_on_issue_ids: tuple[str, ...] = ()
    root_cause_id: str = ''


@dataclass(frozen=True, slots=True)
class DiagnosticAction:
    id: str
    kind: ActionKind
    message_code: str
    message: str
    issue_ids: tuple[str, ...]
    state: ActionState
    depends_on_action_ids: tuple[str, ...]
    responsible_card_ids: tuple[str, ...]
    permission: str | None
    execution_surface: str
    pulse_tool: str | None
    arguments_json: str
    missing_inputs: tuple[str, ...]
    context_refs: tuple[str, ...]
    resolves_unit_count: int


@dataclass(frozen=True, slots=True)
class DiagnosticGraph:
    causes: tuple[DiagnosticCause, ...]
    actions: tuple[DiagnosticAction, ...]
    next_action_ids: tuple[str, ...]
    cycle_edges: tuple[tuple[str, str], ...]
    action_cycle_edges: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class DiagnosticResolution:
    """Exact authorized native context for one already evaluated reason.

    The read composition resolves this from the same snapshot. It cannot
    replace the factual reason, unit state, metric or native transition gate.
    Missing context remains visible as requires_input, not a made-up command.
    """
    unit: DiagnosticUnit | None
    phase: ReadinessPhase
    reason: FactualReason
    origin: CauseOrigin
    actions: tuple[ActionCandidate, ...]


def assessment_diagnostic_units(assessment):
    """Canonical factual phases for enrichment by the read composition."""
    if assessment.card_type == CardType.TEST:
        for row in assessment.scenarios:
            if row.card_id != assessment.card_id:
                raise DiagnosticIntegrityError('readiness_diagnostic_scope_invalid')
            yield DiagnosticUnit('scenario', row.scenario_id, row.semantic_sha256,
                row.spec_edition, row.card_id), ReadinessPhase.VERIFICATION, row.verification
    else:
        for row in assessment.obligations:
            unit = DiagnosticUnit('obligation', row.binding.obligation_ref, row.binding.semantic_sha256,
                row.spec_edition, assessment.card_id or '')
            yield unit, ReadinessPhase.IMPLEMENTATION, row.implementation
            yield unit, ReadinessPhase.VERIFICATION, row.verification


def _unresolved_context(assessment, unit, phase, reason):
    """Preserve actual identities while explicitly requesting native context.

    No positive authenticity, permission, target, owner or code location is
    inferred from an assessment. A result already admitted as association
    missing keeps its association action; this fallback never asks for rerun.
    """
    definition = reason_definition(reason.code)
    if len(reason.scenario_ids) == 1:
        origin = CauseOrigin('scenario', reason.scenario_ids[0])
    elif len(reason.source_ids) == 1:
        origin = CauseOrigin('native_fact', reason.source_ids[0])
    else:
        origin = CauseOrigin(unit.unit if unit else 'spec', unit.ref if unit else assessment.scope.spec_id)
    context = tuple(sorted(set((*reason.source_ids, *reason.criterion_ids, *reason.scenario_ids, origin.identity))))
    known = {'board_id': assessment.scope.board_id, 'spec_id': assessment.scope.spec_id,
        'view': 'readiness'}
    if assessment.card_id:
        known['card_id'] = assessment.card_id
    # Alternatives require a concrete native choice; do not propose every
    # possible operation or turn an old-base report into admitted reuse.
    preferred = {
        ReadinessReason.VERIFICATION_FAILED: ActionKind.IMPLEMENT_CONTRIBUTION,
        ReadinessReason.VERIFICATION_BASE_MISMATCH: ActionKind.ASSOCIATE_VERIFICATION,
        ReadinessReason.VERIFICATION_METHOD_UNSUPPORTED: ActionKind.RESTORE_EVALUATION,
    }.get(reason.code)
    kinds = (preferred,) if preferred else definition.actions
    actions = tuple(ActionCandidate(kind,
        arguments_json=json.dumps({key: value for key, value in known.items()
            if key in ACTION_REGISTRY[kind].argument_fields}, sort_keys=True),
        context_refs=context, missing_inputs=('native_action_context',),
    ) for kind in kinds)
    return DiagnosticResolution(unit, phase, reason, origin, actions)


def diagnose_delivery_assessment(assessment: DeliveryAssessment, *, resolutions=(), dependency_edges=()):
    """Project the shared factual assessment; never recalculate delivery proof.

    Every pending unit/factual phase must have a recoverable cause. Native
    context may enrich each exact reason but cannot erase it or add a different
    unit. The caller supplies canonical dependencies, never textual guesses.
    Lifecycle diagnostics remain a separate native-transition concern.
    """
    indexed = {}
    for resolution in resolutions:
        key = (resolution.unit, resolution.phase, resolution.reason)
        if key in indexed and indexed[key] != resolution:
            raise DiagnosticIntegrityError('readiness_resolution_ambiguous')
        indexed[key] = resolution
    used, required, occurrences = set(), set(), []
    for unit, phase, result in assessment_diagnostic_units(assessment):
        if unit.spec_edition != assessment.scope.edition:
            raise DiagnosticIntegrityError('readiness_diagnostic_scope_invalid')
        if result.state in {UnitState.SATISFIED, UnitState.NOT_APPLICABLE}:
            continue
        required.add((unit, phase))
        if not result.reasons:
            raise DiagnosticIntegrityError('readiness_unit_cause_missing')
        for reason in result.reasons:
            key = (unit, phase, reason)
            resolution = indexed.get(key) or _unresolved_context(assessment, unit, phase, reason)
            if key in indexed:
                used.add(key)
            occurrences.append(CauseOccurrence(reason.code, phase, result.intrinsic_state,
                resolution.origin, (unit,), resolution.actions, reason.native_reason_codes,
                reason.criterion_ids, reason.scenario_ids))
    if set(indexed) - used:
        raise DiagnosticIntegrityError('readiness_resolution_unrelated')
    phases = (ReadinessPhase.VERIFICATION,) if assessment.card_type == CardType.TEST else (
        ReadinessPhase.IMPLEMENTATION, ReadinessPhase.VERIFICATION)
    for reason in assessment.reasons:
        for phase in phases:
            resolution = _unresolved_context(assessment, None, phase, reason)
            occurrences.append(CauseOccurrence(reason.code, phase, UnitState.UNKNOWN,
                resolution.origin, (), resolution.actions, reason.native_reason_codes,
                reason.criterion_ids, reason.scenario_ids))
    if not required and not occurrences and (
        not assessment.completeness.population_known or not assessment.completeness.evaluation_complete
    ):
        # A missing population is not an empty healthy scope. This is a
        # diagnostic integrity error, not manufactured units or zero coverage.
        raise DiagnosticIntegrityError('readiness_assessment_cause_missing')
    return build_diagnostic_graph(assessment.scope, tuple(occurrences), dependency_edges=dependency_edges,
        required_units=tuple(required))


def _action_key(candidate):
    definition = ACTION_REGISTRY[candidate.kind]
    return (candidate.kind, candidate.arguments_json, tuple(sorted(set(candidate.context_refs))),
        tuple(sorted(set(candidate.responsible_card_ids))), definition.permission)


@lru_cache(maxsize=8192)
def _cause_identity_digest(board_id, spec_id, edition, code, origin_kind,
                           origin_identity, expected_bases, observed_bases,
                           action_keys):
    """Cache only the canonical identity, never a diagnostic or its authority."""
    return delivery_digest((board_id, spec_id, edition, code, origin_kind,
        origin_identity, expected_bases, observed_bases, action_keys))


def cause_identity(scope, occurrence):
    definition = reason_definition(occurrence.code)
    if occurrence.phase not in definition.phases:
        raise DiagnosticIntegrityError('readiness_reason_phase_invalid')
    if any(candidate.kind not in definition.actions for candidate in occurrence.actions):
        raise DiagnosticIntegrityError('readiness_reason_action_invalid')
    origin = occurrence.origin
    return 'cause_' + _cause_identity_digest(scope.board_id, scope.spec_id, scope.edition,
        occurrence.code, origin.kind, origin.identity,
        tuple(sorted(set(origin.expected_bases))),
        tuple(sorted(set(origin.observed_bases))),
        tuple(sorted({_action_key(candidate) for candidate in occurrence.actions})))


def _topology(nodes, edges, priorities):
    """Stable Kahn ordering and exact SCC cycle edges (not downstream victims)."""
    outgoing = {node: set() for node in nodes}
    incoming = {node: set() for node in nodes}
    for dependent, prerequisite in edges:
        if dependent not in nodes or prerequisite not in nodes:
            raise DiagnosticIntegrityError('readiness_dependency_unresolved')
        outgoing[prerequisite].add(dependent)
        incoming[dependent].add(prerequisite)
    indegree = {key: len(value) for key, value in incoming.items()}
    ready = [(priorities[node], node) for node in nodes if not indegree[node]]
    heapq.heapify(ready)
    ordered = []
    while ready:
        _, node = heapq.heappop(ready)
        ordered.append(node)
        for child in sorted(outgoing[node]):
            indegree[child] -= 1
            if not indegree[child]:
                heapq.heappush(ready, (priorities[child], child))
    # Iterative Kosaraju avoids recursion limits on long native dependency paths.
    visited, finish = set(), []
    for root in sorted(nodes):
        stack = [(root, False)]
        while stack:
            node, exiting = stack.pop()
            if exiting:
                finish.append(node)
            elif node not in visited:
                visited.add(node)
                stack.append((node, True))
                stack.extend((child, False) for child in sorted(outgoing[node], reverse=True) if child not in visited)
    components, assigned = {}, set()
    for root in reversed(finish):
        if root in assigned:
            continue
        stack = [root]
        while stack:
            node = stack.pop()
            if node in assigned:
                continue
            assigned.add(node)
            components[node] = root
            stack.extend(incoming[node] - assigned)
    cycles = tuple(sorted((a, b) for a, b in edges if components[a] == components[b]))
    ordered.extend(sorted(set(nodes) - set(ordered), key=lambda n: (priorities[n], n)))
    return tuple(ordered), cycles


def build_diagnostic_graph(scope: DeliveryScope, occurrences: tuple[CauseOccurrence, ...], *,
                           dependency_edges=(), required_units=()):
    """Dependencies are exact native cause-ID relations supplied by composition.

    No dependency is inferred from prose. Caller capability changes action
    availability, never cause identity, proof states or metric counts.
    """
    validate_diagnostic_registry()
    if len(occurrences) > 20000 or len(dependency_edges) > 20000 or len(required_units) > 40000:
        raise DiagnosticIntegrityError('readiness_diagnostic_resolution_limit')
    grouped = {}
    covered = set()
    for occurrence in occurrences:
        if not occurrence.origin.identity or any(unit.spec_edition != scope.edition for unit in occurrence.units):
            raise DiagnosticIntegrityError('readiness_diagnostic_scope_invalid')
        if occurrence.intrinsic_state not in set(UnitState) - {UnitState.WAIVED}:
            raise DiagnosticIntegrityError('readiness_intrinsic_state_invalid')
        definition = reason_definition(occurrence.code)
        if definition.actions and not occurrence.actions:
            raise DiagnosticIntegrityError('readiness_diagnostic_action_missing')
        covered.update((unit, occurrence.phase) for unit in occurrence.units)
        grouped.setdefault(cause_identity(scope, occurrence), []).append(occurrence)
    if set(required_units) - covered:
        raise DiagnosticIntegrityError('readiness_unit_cause_missing')
    causes, candidates, action_issues, action_units = {}, {}, {}, {}
    action_identity_cache = {}
    preconditions_missing = {}
    precedence = (UnitState.CONFLICT, UnitState.FAILED, UnitState.UNKNOWN, UnitState.STALE,
        UnitState.PARTIAL, UnitState.MISSING, UnitState.SATISFIED, UnitState.NOT_APPLICABLE)
    for identity, rows in sorted(grouped.items()):
        first = rows[0]
        units = tuple(sorted({unit for row in rows for unit in row.units}))
        action_ids = set()
        for row in rows:
            for candidate in row.actions:
                action_key = _action_key(candidate)
                action_id = action_identity_cache.get(action_key)
                if action_id is None:
                    action_id = 'action_' + delivery_digest((scope.board_id,
                        scope.spec_id, scope.edition, action_key))
                    action_identity_cache[action_key] = action_id
                candidates.setdefault(action_id, []).append(candidate)
                action_issues.setdefault(action_id, set()).add(identity)
                action_units.setdefault(action_id, set()).update(row.units)
                preconditions_missing.setdefault(action_id, set()).update(
                    (set(reason_definition(row.code).preconditions) | set(ACTION_REGISTRY[candidate.kind].preconditions))
                    - set(candidate.satisfied_preconditions))
                action_ids.add(action_id)
                if len(candidates) > 20000:
                    raise DiagnosticIntegrityError('readiness_diagnostic_resolution_limit')
        definition = reason_definition(first.code)
        native_gate = {row.blocks_target_transition for row in rows}
        gate = True if True in native_gate else None if None in native_gate else False
        causes[identity] = DiagnosticCause(identity, first.code, tuple(sorted({row.phase for row in rows})),
            replace(first.origin, expected_bases=tuple(sorted(set(first.origin.expected_bases))),
                observed_bases=tuple(sorted(set(first.origin.observed_bases)))), units,
            next(state for state in precedence if any(row.intrinsic_state == state for row in rows)),
            'readiness.' + first.code, definition.message.format(subject=first.origin.identity),
            tuple(sorted({code for row in rows for code in row.native_reason_codes})),
            tuple(sorted({key for row in rows for key in row.criterion_ids})),
            tuple(sorted({key for row in rows for key in row.scenario_ids})),
            any(row.blocks_proof for row in rows), gate, tuple(sorted(action_ids)), root_cause_id=identity)
    edges = tuple(sorted(set(dependency_edges)))
    cause_order, cause_cycles = _topology(set(causes), edges, {key: 0 for key in causes})
    dependencies = {identity: set() for identity in causes}
    action_edges = set()
    unresolved_predecessor = set()
    for dependent, prerequisite in edges:
        dependencies[dependent].add(prerequisite)
        for action in causes[dependent].action_ids:
            # A single exact operation may resolve both issues. Do not invent
            # self-dependency; real issue cycles remain unavailable below.
            if action in causes[prerequisite].action_ids:
                continue
            if not causes[prerequisite].action_ids:
                unresolved_predecessor.add(action)
            for parent in causes[prerequisite].action_ids:
                action_edges.add((action, parent))
                if len(action_edges) > 20000:
                    raise DiagnosticIntegrityError('readiness_diagnostic_resolution_limit')
    roots = {}
    for identity in cause_order:
        parents = dependencies[identity]
        roots[identity] = min((roots.get(parent, parent) for parent in parents), default=identity)
        causes[identity] = replace(causes[identity], depends_on_issue_ids=tuple(sorted(parents)), root_cause_id=roots[identity])
    action_order, action_cycles = _topology(set(candidates), action_edges,
        {key: ACTION_REGISTRY[value[0].kind].priority for key, value in candidates.items()})
    cyclic_actions = {identity for edge in action_cycles for identity in edge}
    cyclic_causes = {identity for edge in cause_cycles for identity in edge}
    cyclic_actions.update(action for identity in cyclic_causes for action in causes[identity].action_ids)
    action_parents = {identity: set() for identity in candidates}
    for dependent, prerequisite in action_edges:
        action_parents[dependent].add(prerequisite)
    actions = []
    for identity in action_order:
        choices = candidates[identity]
        candidate = choices[0]
        definition = ACTION_REGISTRY[candidate.kind]
        arguments = json.loads(candidate.arguments_json)
        missing = {key for key in definition.required_arguments
            if not arguments.get(key) or isinstance(arguments[key], str) and not arguments[key].strip()}
        missing.update(value for choice in choices for value in choice.missing_inputs)
        missing.update(preconditions_missing[identity])
        parents = tuple(sorted(action_parents[identity]))
        if identity in cyclic_actions or not all(choice.capability_available for choice in choices):
            state = ActionState.UNAVAILABLE
        elif any(choice.permission_allowed is False for choice in choices):
            state = ActionState.FORBIDDEN
        elif any(choice.human_required for choice in choices) or definition.execution_surface == 'human_decision':
            state = ActionState.REQUIRES_HUMAN
        elif definition.permission and any(choice.permission_allowed is None for choice in choices):
            state = ActionState.UNAVAILABLE
        elif parents or identity in unresolved_predecessor:
            state = ActionState.WAITING_DEPENDENCY
        elif missing:
            state = ActionState.REQUIRES_INPUT
        else:
            state = ActionState.READY
        actions.append(DiagnosticAction(identity, candidate.kind, 'readiness.action.' + candidate.kind,
            definition.message, tuple(sorted(action_issues[identity])), state, parents,
            tuple(sorted(set(candidate.responsible_card_ids))), definition.permission, definition.execution_surface,
            definition.pulse_tool, candidate.arguments_json, tuple(sorted(missing)),
            tuple(sorted(set(candidate.context_refs))), len(action_units[identity])))
    ready = [action.id for action in actions if action.state == ActionState.READY]
    fallback = [action.id for action in actions if action.state != ActionState.WAITING_DEPENDENCY]
    return DiagnosticGraph(tuple(causes[key] for key in cause_order), tuple(actions),
        tuple((ready or fallback or [action.id for action in actions])[:3]), cause_cycles, action_cycles)


validate_diagnostic_registry()
