"""Closed deterministic delivery causes and action dependencies.

Input is already evaluated, authorized native context. This module never checks
receipts, executes an action, reads a repository or changes a score/authority.
"""

from dataclasses import dataclass, replace
from enum import StrEnum
import json
from types import MappingProxyType

from okto_pulse.core.domain.delivery_assessment import ReadinessPhase, ReadinessReason, UnitState
from okto_pulse.core.domain.delivery_evidence import DeliveryScope
from okto_pulse.core.domain.delivery_inventory import delivery_digest


class DiagnosticIntegrityError(ValueError):
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


def _action(kind, message, surface, priority, tool=None, permission=None, arguments=(), required=()):
    return ActionDefinition(ActionKind(kind), message, surface, priority, tool, permission, arguments, required)


ACTION_REGISTRY = MappingProxyType({item.kind: item for item in (
    _action('resolve_scope', 'Corrigir a qualificação ou alocação indicada no fluxo nativo.', 'read_only', 1),
    _action('implement_contribution', 'Implementar e observar a contribuição delimitada.', 'external_agent', 3),
    _action('complete_contribution', 'Concluir a contribuição e registrar prova real de conclusão.', 'external_agent', 3),
    _action('associate_implementation', 'Associar a prova de implementação já disponível.', 'pulse_tool', 0,
        'okto_pulse_record_delivery_evidence', 'code_traceability.target.execution_submit',
        ('board_id', 'card_id', 'spec_id', 'evidence'), ('board_id', 'card_id', 'spec_id', 'evidence')),
    _action('refresh_implementation_proof', 'Atualizar somente a observação de implementação afetada.', 'external_agent', 1),
    _action('resolve_material_change', 'Concluir ou reobservar a alteração material registrada.', 'external_agent', 1),
    _action('execute_verification', 'Executar a verificação existente na base exigida e registrar o resultado real.', 'external_agent', 4),
    _action('associate_verification', 'Associar o resultado válido às implementações exatas, sem repetir a execução.', 'pulse_tool', 0,
        'okto_pulse_record_delivery_evidence', 'spec.tests.execute',
        ('board_id', 'card_id', 'spec_id', 'evidence'), ('board_id', 'card_id', 'spec_id', 'evidence')),
    _action('resolve_verification_conflict', 'Reconciliar ou substituir os resultados pelos controles nativos.', 'read_only', 1),
    _action('review_decision', 'Consultar as fontes e inspeções exigidas pelo plano da Decision.', 'read_only', 2,
        'okto_pulse_get_decision_reviews', 'code_traceability.evidence.read',
        ('board_id', 'spec_id'), ('board_id', 'spec_id')),
    _action('complete_lifecycle', 'Concluir o card pela transição nativa disponível após conferir seus gates.', 'read_only', 5),
    _action('resolve_native_gate', 'Resolver o gate nativo identificado antes da conclusão.', 'read_only', 2),
    _action('restore_evaluation', 'Restabelecer a leitura autorizada e consultar novamente a entrega.', 'read_only', 1,
        'okto_pulse_get_delivery_evidence', 'code_traceability.evidence.read',
        ('board_id', 'spec_id'), ('board_id', 'spec_id')),
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
    _reason('verification_failed', 'A verificação de {subject} falhou na base atual.', ('implement_contribution', 'execute_verification'), preconditions=('native_scenario', 'implementation_base'), fields=('scenario_id', 'observation_id')),
    _reason('verification_inconclusive', 'A observação de {subject} não permite concluir a verificação.', ('execute_verification',), severity='medium', preconditions=('native_scenario', 'implementation_base'), fields=('scenario_id', 'observation_id')),
    _reason('verification_conflict', 'Há resultados correntes incompatíveis para {subject}.', ('resolve_verification_conflict',), fields=('scenario_id', 'record_id')),
    _reason('verification_association_missing', 'O resultado válido de {subject} ainda não está associado à entrega.', ('associate_verification',), preconditions=('authenticated_current_result',), fields=('scenario_id', 'record_id')),
    _reason('verification_base_mismatch', 'A base observada por {subject} difere da base entregue.', ('associate_verification', 'execute_verification'), preconditions=('implementation_base',), fields=('source_ref', 'revision', 'scenario_id')),
    _reason('verification_base_unresolved', 'A fonte ou revisão observada por {subject} não pôde ser resolvida.', ('refresh_implementation_proof',), fields=('source_ref', 'scenario_id')),
    _reason('verification_receipt_invalid', 'O recibo de {subject} não é válido para este escopo.', ('execute_verification',), preconditions=('native_scenario', 'implementation_base'), fields=('scenario_id', 'record_id')),
    _reason('verification_method_unsupported', 'O método de verificação de {subject} está indisponível.', ('restore_evaluation', 'request_authorized_decision'), fields=('scenario_id',)),
    _reason('verification_dependency_pending', 'A verificação de {subject} aguarda a implementação ou base exigida.', ('execute_verification',), preconditions=('implementation_base',), fields=('scenario_id', 'target_id')),
    _reason('decision_verification_pending', 'A Decision {subject} aguarda as provas ou inspeções do seu plano.', ('review_decision',), preconditions=('native_decision_plan',), fields=('entity_id', 'record_id')),
    _reason('applicable_obligation_waived', 'Uma dispensa substitui a prova de {subject}; isso não concede cobertura.', (), phase='implementation,verification', severity='medium', fields=('record_id',)),
    _reason('lifecycle_card_open', 'As provas de {subject} estão disponíveis; falta encerrar o card.', ('complete_lifecycle',), phase='lifecycle', severity='medium', preconditions=('native_transition',), fields=('entity_id',)),
    _reason('lifecycle_gate_pending', 'Um gate nativo ainda impede a conclusão de {subject}.', ('resolve_native_gate',), phase='lifecycle', fields=('entity_id',)),
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

    def __post_init__(self):
        try:
            definition = ACTION_REGISTRY[ActionKind(self.kind)]
            arguments = json.loads(self.arguments_json)
            if not isinstance(arguments, dict) or not set(arguments) <= set(definition.argument_fields):
                raise ValueError('fields')
            normalized = json.dumps(arguments, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
        except (KeyError, ValueError, TypeError) as exc:
            raise DiagnosticIntegrityError('readiness_action_arguments_invalid') from exc
        object.__setattr__(self, 'arguments_json', normalized)


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


def _action_key(candidate):
    definition = ACTION_REGISTRY[candidate.kind]
    return (candidate.kind, candidate.arguments_json, tuple(sorted(set(candidate.context_refs))),
        tuple(sorted(set(candidate.responsible_card_ids))), definition.permission)


def cause_identity(scope, occurrence):
    definition = reason_definition(occurrence.code)
    if occurrence.phase not in definition.phases:
        raise DiagnosticIntegrityError('readiness_reason_phase_invalid')
    if any(candidate.kind not in definition.actions for candidate in occurrence.actions):
        raise DiagnosticIntegrityError('readiness_reason_action_invalid')
    origin = occurrence.origin
    return 'cause_' + delivery_digest((scope.board_id, scope.spec_id, scope.edition, occurrence.code,
        origin.kind, origin.identity, sorted(set(origin.expected_bases)), sorted(set(origin.observed_bases)),
        sorted({_action_key(candidate) for candidate in occurrence.actions})))


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
    import heapq
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


def build_diagnostic_graph(scope: DeliveryScope, occurrences: tuple[CauseOccurrence, ...], *, dependency_edges=()):
    """Dependencies are exact native cause-ID relations supplied by composition.

    No dependency is inferred from prose. Caller capability changes action
    availability, never cause identity, proof states or metric counts.
    """
    validate_diagnostic_registry()
    if len(occurrences) > 20000 or len(dependency_edges) > 20000:
        raise DiagnosticIntegrityError('readiness_diagnostic_resolution_limit')
    grouped = {}
    for occurrence in occurrences:
        if not occurrence.origin.identity or any(unit.spec_edition != scope.edition for unit in occurrence.units):
            raise DiagnosticIntegrityError('readiness_diagnostic_scope_invalid')
        grouped.setdefault(cause_identity(scope, occurrence), []).append(occurrence)
    causes, candidates, action_issues, action_units = {}, {}, {}, {}
    precedence = (UnitState.CONFLICT, UnitState.FAILED, UnitState.UNKNOWN, UnitState.STALE,
        UnitState.PARTIAL, UnitState.MISSING, UnitState.SATISFIED, UnitState.NOT_APPLICABLE)
    for identity, rows in sorted(grouped.items()):
        first = rows[0]
        units = tuple(sorted({unit for row in rows for unit in row.units}))
        action_ids = set()
        for row in rows:
            for candidate in row.actions:
                action_id = 'action_' + delivery_digest((scope.board_id, scope.spec_id, scope.edition, _action_key(candidate)))
                candidates.setdefault(action_id, []).append(candidate)
                action_issues.setdefault(action_id, set()).add(identity)
                action_units.setdefault(action_id, set()).update(row.units)
                action_ids.add(action_id)
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
    for dependent, prerequisite in edges:
        dependencies[dependent].add(prerequisite)
        action_edges.update((a, b) for a in causes[dependent].action_ids for b in causes[prerequisite].action_ids if a != b)
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
    actions = []
    for identity in action_order:
        choices = candidates[identity]
        candidate = choices[0]
        definition = ACTION_REGISTRY[candidate.kind]
        arguments = json.loads(candidate.arguments_json)
        missing = set(definition.required_arguments) - set(arguments)
        missing.update(value for choice in choices for value in choice.missing_inputs)
        parents = tuple(sorted(b for a, b in action_edges if a == identity))
        if identity in cyclic_actions or not all(choice.capability_available for choice in choices):
            state = ActionState.UNAVAILABLE
        elif any(choice.permission_allowed is False for choice in choices):
            state = ActionState.FORBIDDEN
        elif any(choice.human_required for choice in choices) or definition.execution_surface == 'human_decision':
            state = ActionState.REQUIRES_HUMAN
        elif definition.permission and any(choice.permission_allowed is None for choice in choices):
            state = ActionState.UNAVAILABLE
        elif parents or any(dependencies[issue] for issue in action_issues[identity]):
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
    return DiagnosticGraph(tuple(causes[key] for key in cause_order), tuple(actions), tuple((ready or fallback)[:3]), cause_cycles)


validate_diagnostic_registry()
