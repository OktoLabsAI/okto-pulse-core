"""Bounded projections of already evaluated, authorized readiness facts.

Pagination never evaluates proof or a transition and never changes metrics.
The caller supplies the native gate/lifecycle result; signing is a public port
implemented by the edition. Use the same serialization at REST/MCP boundaries.
"""
from dataclasses import asdict, replace
import json

from okto_pulse.core.domain.delivery_diagnostics import ACTION_REGISTRY, ActionKind, ActionState, CauseOrigin, DiagnosticIntegrityError
from okto_pulse.core.domain.delivery_evidence import DeliveryScope
from okto_pulse.core.domain.delivery_inventory import delivery_digest
from okto_pulse.core.domain.delivery_assessment import (
    AssessmentCompleteness, DeliveryAssessment, FactualReason, ReadinessReason,
    aggregate_delivery_metrics, assess_delivery_gate, qualify_delivery,
)
from okto_pulse.core.domain.effective_delivery_coverage import EvaluatedDeliverySnapshot
from okto_pulse.core.domain.enums import CardType
from okto_pulse.core.models.delivery_readiness import (
    DeliveryReadiness, DeliveryReadinessContext, DeliveryReadinessDetail, ReadinessCursorBinding,
    ReadinessDetailCounts, ReadinessDetailEntry, ReadinessPage, ReadinessReadQuery, ReadinessRelation,
)
from okto_pulse.core.ports.delivery_readiness import (
    DeliveryReadinessCursorPort, ReadinessAccessDenied, ReadinessReadError,
)
from okto_pulse.core.services.delivery_readiness_diagnostics import (
    DiagnosticOwner, NativeDeliveryDiagnostics, compose_delivery_diagnostics,
    render_diagnostic_action, render_diagnostic_issue, render_diagnostic_unit,
)

SUMMARY_MAX_BYTES = 32 * 1024
DETAIL_MAX_BYTES = 128 * 1024


class EvaluatedDeliveryReadiness:
    """Authorized, request-local Spec evaluation shared by its Card projections.

    Construct again after every read, including a cursor continuation. Neither
    this object nor its assessments authorize a transition or survive a new
    batch. Native gate/lifecycle context is composed separately.
    """
    def __init__(self, batch, *, actor_id):
        access, scope = batch.access, batch.facts.scope
        if access is None:
            raise ReadinessAccessDenied('code_traceability.evidence.read')
        if access.actor_id != actor_id or access.board_id != scope.board_id:
            raise ReadinessReadError('readiness_scope_mismatch')
        access.require('code_traceability.evidence.read')
        access.require('spec.entity.read')
        self._cards = {card.id: card for card in batch.cards}
        if len(self._cards) != len(batch.cards):
            raise ReadinessReadError('readiness_assessment_unavailable', native_code='readiness_duplicate_card')
        self._batch, self._actor_id = batch, actor_id
        self._evaluation = None if batch.unavailable_reason_codes else EvaluatedDeliverySnapshot(batch.facts)
        self._assessments = {None: self._evaluation.obligations() if self._evaluation else self._unavailable()}
        self._diagnostics = {}

    def _unavailable(self, card=None):
        return DeliveryAssessment(self._batch.facts.scope, AssessmentCompleteness(False, False, False, True), (),
            reasons=(FactualReason(ReadinessReason.ASSESSMENT_UNAVAILABLE,
                native_reason_codes=self._batch.unavailable_reason_codes),),
            card_id=card.id if card else None, card_type=card.card_type if card else None)

    def assessment(self, card_id=None):
        if card_id is not None:
            self._batch.access.require('card.entity.read')
            card = self._cards.get(card_id)
            if card is None or card.card_type not in {CardType.NORMAL, CardType.BUG, CardType.TEST}:
                raise ReadinessReadError('readiness_scope_mismatch')
            if card_id not in self._assessments:
                self._assessments[card_id] = (self._unavailable(card) if self._evaluation is None else
                    self._evaluation.scenarios(card_id=card_id, scenario_ids=card.scenario_ids)
                    if card.card_type == CardType.TEST else
                    self._evaluation.obligations(card_id=card_id, card_type=card.card_type))
        return self._assessments[card_id]

    def diagnostics(self, card_id=None):
        assessment = self.assessment(card_id)
        if card_id not in self._diagnostics:
            self._diagnostics[card_id] = compose_authorized_delivery_diagnostics(assessment, self._batch,
                actor_id=self._actor_id, spec_assessment=self._assessments[None])
        return self._diagnostics[card_id]

    def _native_lifecycle(self, card_id):
        native = self._batch.native_lifecycle
        if native is None or native.card_id != card_id:
            return None
        status = self._cards[card_id].status if card_id else self._batch.spec_status
        if native.stored_status != status:
            raise ReadinessReadError('readiness_snapshot_changed')
        return native

    def context(self, card_id=None):
        """Compose current facts; never advertise a transition not evaluated.

        Native transition composition must supply its own complete, read-only
        evaluation. Until then its missing context remains explicit, even when
        both proof metrics are complete. This is not a dry-run of a mutation.
        """
        assessment = self.assessment(card_id)
        batch, card = self._batch, self._cards.get(card_id)
        metrics = aggregate_delivery_metrics(assessment)
        gate = assess_delivery_gate(assessment, mode=batch.delivery_gate_mode,
            spec_override=batch.skip_delivery_evidence)
        status = card.status if card else batch.spec_status
        native = self._native_lifecycle(card_id)
        check = native.check_status if native else 'not_evaluated'
        qualification = qualify_delivery(assessment, gate=gate, stored_status=status,
            lifecycle_check=check, metrics=metrics,
            native_completion_recorded=native.completion_recorded if native else False)
        diagnostic = self.diagnostics(card_id)
        unit_counts = (metrics.implementation.unresolved, metrics.verification.unresolved)
        # Count canonical units once even if both of their phases are pending.
        unresolved = None if any(n is None for n in unit_counts) else (
            sum(s.verification.state not in {'satisfied', 'not_applicable'} for s in assessment.scenarios)
            if card and card.card_type == CardType.TEST else
            sum(any(p.state not in {'satisfied', 'not_applicable'} for p in (r.implementation, r.verification))
                for r in assessment.obligations))
        message_code = ('historical_completion_diverged' if qualification.historical_divergence
            else qualification.state.value)
        if qualification.proof_complete is True and check == 'blocked' and not qualification.historical_divergence:
            message_code = 'lifecycle_gate_pending'
        messages = {
            'unknown': 'Não foi possível avaliar a entrega com os fatos atuais.',
            'implementation_pending': 'Há contribuições de implementação pendentes.',
            'verification_pending': 'A implementação está comprovada; falta verificação aplicável.',
            'verification_failed': 'Há falha ou conflito na verificação atual.',
            'ready': 'Provas completas; confira os gates na transição nativa para concluir.',
            'delivered': 'Entrega concluída com qualificação atual.',
            'ready_with_exceptions': 'O gate de entrega admite ressalvas; confira os demais gates na transição nativa.',
            'delivered_with_exceptions': 'Entrega concluída com ressalvas efetivas.',
            'not_applicable': 'O escopo conhecido não possui fase de entrega aplicável.',
            'historical_completion_diverged': 'O status Done foi preservado, mas a qualificação atual da entrega diverge.',
            'lifecycle_gate_pending': 'Provas completas; um gate nativo ainda impede o encerramento.',
        }
        scope = assessment.scope
        return DeliveryReadinessContext(
            scope=dict(board_id=scope.board_id, spec_id=scope.spec_id, spec_edition=scope.edition,
                spec_version=batch.spec_version, card_id=card_id, card_type=card.card_type if card else None,
                card_version=card.version if card else None), snapshot_id=batch.snapshot_id,
            evaluated_at=batch.evaluated_at, scope_complete=assessment.completeness.population_known
                and assessment.completeness.scope_resolved,
            evaluation_complete=assessment.completeness.evaluation_complete,
            implementation=asdict(metrics.implementation), verification=asdict(metrics.verification),
            delivery_state=qualification.state, proof_complete=qualification.proof_complete,
            delivery_gate=dict(status=gate.status, mode=gate.mode, exception_applied=bool(gate.exception_refs),
                exception_refs=gate.exception_refs),
            lifecycle=dict(stored_status=status, check_status=check,
                other_blocker_count=native.other_blocker_count if native else None,
                qualification_is_current=qualification.qualification_is_current,
                reason_codes=native.reason_codes if native else ('native_transition_context_required',),
                context_refs=(f'card:{card.id}' if card else f'spec:{scope.spec_id}',)),
            summary=dict(message_code=message_code, message=messages[message_code],
                unresolved_units=unresolved, root_causes=len(diagnostic.graph.causes), lifecycle_pending=None,
                exceptions=len(gate.exception_refs)))


def compose_authorized_delivery_diagnostics(assessment, batch, *, actor_id, spec_assessment=None):
    """Enrich only through current, same-snapshot native authority.

    Aggregate facts/units remain intact. Restricted detail is explicit, never a
    false empty healthy population or a lower score. Unit references are already
    part of the authorized Spec/evidence contract; private content is removed
    from details, action arguments, context refs and the cause's display origin.
    This function does not grant authority for a future write or transition.
    """
    access, scope = batch.access, batch.facts.scope
    if access is None:
        raise ReadinessAccessDenied('code_traceability.evidence.read')
    if access.actor_id != actor_id or access.board_id != scope.board_id:
        raise ReadinessReadError('readiness_scope_mismatch')
    access.require('code_traceability.evidence.read')
    access.require('spec.entity.read')
    if assessment.card_id is not None:
        access.require('card.entity.read')
        if assessment.card_id not in {c.id for c in batch.cards}:
            raise ReadinessReadError('readiness_scope_mismatch')
    can_cards = access.allows('card.entity.read')
    can_owners = can_cards and access.allows('agent.entity.read')
    can_targets = access.allows('code_traceability.target.read')
    can_receipts = access.allows('code_traceability.investigation.read')
    can_tests = access.allows('spec.tests.read')
    native = batch.native_lifecycle
    if native is not None and native.card_id != assessment.card_id:
        native = None
    projection = compose_delivery_diagnostics(assessment, batch.facts, spec_assessment=spec_assessment,
        permissions={a.permission: access.allows(a.permission) for a in ACTION_REGISTRY.values() if a.permission},
        owners=tuple(DiagnosticOwner('card', c.id, c.assignee_id if can_owners else None, can_owners) for c in batch.cards),
        card_versions={c.id: c.version for c in batch.cards}, procedures=batch.scenarios if can_tests else (),
        native_lifecycle=native)
    causes = {c.id: c for c in projection.graph.causes}
    restricted, details = set(), []
    for detail in projection.details:
        cause = causes[detail.issue_id]
        has_targets = cause.origin.kind == 'execution' or any(
            f.target_id or f.execution_id or f.source_ref for f in (*detail.expected, *detail.observed))
        has_tests = bool(cause.scenario_ids or any(u.unit == 'scenario' for u in cause.units))
        has_cards = bool(detail.implementation_card_ids or detail.test_card_ids)
        if ((has_targets and not (can_targets and can_receipts)) or (has_tests and not can_tests)
            or (has_cards and not can_cards)):
            restricted.add(cause.id)
            # Redact the complete native detail if one of its required APIs is
            # restricted. Partial proof payloads can be misleading or leak the
            # same content through report text and cross-referenced IDs.
            detail = replace(detail, expected=(), observed=(), evidence_refs=(), pinpoints=(),
                implementation_card_ids=detail.implementation_card_ids if can_cards else (),
                test_card_ids=detail.test_card_ids if can_cards else (), owner=None,
                owner_state='restricted', detail_state='restricted')
        details.append(detail)
    actions = []
    for action in projection.graph.actions:
        # Native scenario execution/admission requires writable Spec membership;
        # associating an existing result uses its separate delivery-record path.
        # Do not make the latter inherit a gate that its native API does not have.
        if action.kind == ActionKind.EXECUTE_VERIFICATION and not access.spec_write_access:
            action = replace(action, state=ActionState.FORBIDDEN)
        if restricted.intersection(action.issue_ids):
            arguments = json.loads(action.arguments_json)
            visible = {key: value for key, value in arguments.items()
                if key in {'board_id', 'spec_id', 'spec_edition'} or key == 'card_id' and can_cards}
            action = replace(action, arguments_json=json.dumps(visible, sort_keys=True), context_refs=(),
                responsible_card_ids=action.responsible_card_ids if can_cards else (),
                missing_inputs=('authorized_detail_context',),
                state=ActionState.FORBIDDEN if action.state == ActionState.FORBIDDEN else ActionState.UNAVAILABLE)
        actions.append(action)
    ready = [a.id for a in actions if a.state == ActionState.READY]
    fallback = [a.id for a in actions if a.state != ActionState.WAITING_DEPENDENCY]
    graph = replace(projection.graph, actions=tuple(actions),
        causes=tuple(replace(c, origin=CauseOrigin('spec', scope.spec_id)) if c.id in restricted else c
            for c in projection.graph.causes),
        next_action_ids=tuple((ready or fallback or [a.id for a in actions])[:3]))
    return replace(projection, graph=graph, details=tuple(details))


def serialize_readiness_response(response: DeliveryReadiness | DeliveryReadinessDetail) -> bytes:
    """Exact compact UTF-8 representation, including every meaningful null."""
    return response.model_dump_json(exclude_none=False).encode('utf-8')


def _detail_identity(detail):
    return {name: [value.model_dump(mode='json') for value in getattr(detail, name)]
        for name in ('expected', 'observed', 'evidence_refs', 'pinpoints')} | {
            'id': detail.issue_id, 'owner': detail.owner, 'owner_state': detail.owner_state,
            'detail_state': detail.detail_state,
            'implementation_card_ids': detail.implementation_card_ids, 'test_card_ids': detail.test_card_ids}


class PreparedDeliveryReadiness:
    """One complete projection; paging/phase filters only slice its response.

    Construction occurs after authorization and visibility filtering. Do not
    accept a client-supplied context or graph, or use this object for writes.
    The visibility-sensitive projection digest invalidates continuation when
    an actor's permitted details or action availability change on the same facts.
    """
    def __init__(self, context: DeliveryReadinessContext, diagnostics: NativeDeliveryDiagnostics,
                 *, cursor: DeliveryReadinessCursorPort):
        self.context, self.diagnostics, self.cursor = context, diagnostics, cursor
        scope = context.scope
        if (diagnostics.scope != DeliveryScope(scope.board_id, scope.spec_id, scope.spec_edition)
            or diagnostics.card_id != scope.card_id):
            raise DiagnosticIntegrityError('readiness_diagnostic_scope_invalid')
        graph = diagnostics.graph
        self.causes = {c.id: c for c in graph.causes}
        self.details = {d.issue_id: d for d in diagnostics.details}
        self.actions = {a.id: a for a in graph.actions}
        if (len(self.causes) != len(graph.causes) or len(self.details) != len(diagnostics.details)
            or len(self.actions) != len(graph.actions) or set(self.causes) != set(self.details)
            or context.summary.root_causes != len(self.causes)
            or set(graph.next_action_ids) - self.actions.keys()
            or any(set(c.action_ids) - self.actions.keys() or set(c.depends_on_issue_ids) - self.causes.keys()
                for c in graph.causes)
            or any(set(a.issue_ids) - self.causes.keys() or set(a.depends_on_action_ids) - self.actions.keys()
                for a in graph.actions)
            or any(a not in self.causes or b not in self.causes for a, b in graph.cycle_edges)
            or any(a not in self.actions or b not in self.actions for a, b in graph.action_cycle_edges)):
            raise DiagnosticIntegrityError('readiness_projection_incomplete')
        # Evaluated time is informational and must not invalidate every cursor.
        self.projection_id = delivery_digest({'context': context.model_dump(mode='json', exclude={'evaluated_at'}),
            'graph': asdict(graph), 'details': [_detail_identity(self.details[key]) for key in sorted(self.details)]})
        self._entries = {}

    def _binding(self, query, actor_id):
        scope = self.context.scope
        if (query.board_id, query.spec_id, query.card_id) != (scope.board_id, scope.spec_id, scope.card_id):
            raise ReadinessReadError('readiness_scope_mismatch')
        if query.snapshot_id is not None and query.snapshot_id != self.context.snapshot_id:
            raise ReadinessReadError('readiness_cursor_stale')
        return ReadinessCursorBinding(actor_id=actor_id, board_id=scope.board_id, spec_id=scope.spec_id,
            card_id=scope.card_id, spec_edition=scope.spec_edition, snapshot_id=self.context.snapshot_id,
            projection_id=self.projection_id, phase=query.phase, issue_id=query.issue_id, limit=query.limit)

    def _page(self, binding, *, total, offset, returned, truncated=False):
        more = offset + returned < total
        return ReadinessPage(total=total, returned=returned, has_more=more,
            next_cursor=self.cursor.encode(binding, offset + returned) if more else None,
            response_truncated=more or truncated, phase_filter=binding.phase)

    @staticmethod
    def _fit(*, total, offset, limit, maximum, construct, identity):
        if offset < 0 or offset and offset >= total:
            raise ReadinessReadError('readiness_query_invalid', native_code='readiness_cursor_position_invalid')
        fitted = None
        # The last page drops its cursor: size need not be monotonic. Try all
        # bounded prefixes rather than rejecting a page whose final form fits.
        for size in range(1 if total else 0, min(limit, total - offset) + 1):
            candidate = construct(size)
            if len(serialize_readiness_response(candidate)) <= maximum:
                fitted = candidate
        if fitted is None:
            raise ReadinessReadError('readiness_limit_exceeded', native_code='readiness_response_limit', identity=identity)
        return fitted

    def page(self, query: ReadinessReadQuery, *, actor_id: str) -> DeliveryReadiness | DeliveryReadinessDetail:
        """Caller reauthorizes before every invocation, including continuations."""
        binding = self._binding(query, actor_id)
        offset = self.cursor.decode(query.cursor, binding) if query.cursor else 0
        if query.issue_id is not None:
            return self._detail_page(query, binding, offset)
        causes = tuple(c for c in self.diagnostics.graph.causes if query.phase == 'all' or query.phase in c.phases)
        scope = DeliveryScope(binding.board_id, binding.spec_id, binding.spec_edition)
        rendered = []
        for cause in causes[offset:offset + query.limit]:
            issue = render_diagnostic_issue(cause, self.details[cause.id], scope=scope,
                card_id=binding.card_id, snapshot_id=binding.snapshot_id)
            issue = issue.model_copy(update={'detail_query': issue.detail_query.model_copy(update={'phase': query.phase})})
            rendered.append(issue)
        related = {c.id for c in causes}
        actions = tuple(render_diagnostic_action(self.actions[key]) for key in self.diagnostics.graph.next_action_ids
            if related.intersection(self.actions[key].issue_ids))[:3]
        def construct(size):
            issues = tuple(rendered[:size])
            truncated = any(value for issue in issues for key, value in issue.model_dump().items() if key.endswith('_truncated'))
            return DeliveryReadiness(**self.context.model_dump(), issues=issues, next_actions=actions,
                page=self._page(binding, total=len(causes), offset=offset, returned=size, truncated=truncated))
        return self._fit(total=len(causes), offset=offset, limit=query.limit, maximum=SUMMARY_MAX_BYTES,
            construct=construct, identity=causes[offset].id if offset < len(causes) else self.context.scope.spec_id)

    def _detail_entries(self, issue_id):
        if issue_id in self._entries:
            return self._entries[issue_id]
        cause, detail = self.causes[issue_id], self.details[issue_id]
        entries = {}
        def add(kind, section, value):
            identity = 'entry_' + delivery_digest((section, value.model_dump(mode='json')))
            entries[identity] = ReadinessDetailEntry(id=identity, type=kind, section=section, value=value)
        for unit in cause.units:
            add('affected_unit', 'affected_units', render_diagnostic_unit(unit))
        for section in ('evidence_refs', 'expected', 'observed'):
            for value in getattr(detail, section):
                add('evidence', section, value)
        for kind, values in (('criterion', cause.criterion_ids), ('scenario', cause.scenario_ids),
                ('native_reason', cause.native_reason_codes), ('implementation_card', detail.implementation_card_ids),
                ('test_card', detail.test_card_ids)):
            for identity in values:
                add('evidence', 'relations', ReadinessRelation(kind=kind, source_id=identity))
        for parent in cause.depends_on_issue_ids:
            add('evidence', 'relations', ReadinessRelation(kind='issue_dependency', source_id=issue_id, target_id=parent))
        for child, parent in self.diagnostics.graph.cycle_edges:
            if child == issue_id:
                add('evidence', 'relations', ReadinessRelation(kind='cause_cycle', source_id=child, target_id=parent))
        for child, parent in self.diagnostics.graph.action_cycle_edges:
            if child in cause.action_ids:
                add('evidence', 'relations', ReadinessRelation(kind='action_cycle', source_id=child, target_id=parent))
        for value in detail.pinpoints:
            add('pinpoint', 'pinpoints', value)
        for identity in cause.action_ids:
            add('action', 'actions', render_diagnostic_action(self.actions[identity]))
        result = tuple(sorted(entries.values(), key=lambda e: (e.type, e.id)))
        self._entries[issue_id] = result
        return result

    def _detail_page(self, query, binding, offset):
        cause = self.causes.get(query.issue_id)
        if cause is None or query.phase != 'all' and query.phase not in cause.phases:
            raise ReadinessReadError('readiness_scope_mismatch', native_code='readiness_issue_outside_query')
        entries = self._detail_entries(query.issue_id)
        counts = ReadinessDetailCounts(**{kind: sum(e.type == kind for e in entries)
            for kind in ('affected_unit', 'evidence', 'pinpoint', 'action')})
        def construct(size):
            return DeliveryReadinessDetail(**self.context.model_dump(), issue_id=query.issue_id, entry_counts=counts,
                detail_state=self.details[query.issue_id].detail_state,
                entries=entries[offset:offset + size],
                page=self._page(binding, total=len(entries), offset=offset, returned=size))
        return self._fit(total=len(entries), offset=offset, limit=query.limit, maximum=DETAIL_MAX_BYTES,
            construct=construct, identity=query.issue_id)
