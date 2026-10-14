"""Bounded projections of already evaluated, authorized readiness facts.

Pagination never evaluates proof or a transition and never changes metrics.
The caller supplies the native gate/lifecycle result; signing is a public port
implemented by the edition. Use the same serialization at REST/MCP boundaries.
"""
from dataclasses import asdict

from okto_pulse.core.domain.delivery_diagnostics import DiagnosticIntegrityError
from okto_pulse.core.domain.delivery_evidence import DeliveryScope
from okto_pulse.core.domain.delivery_inventory import delivery_digest
from okto_pulse.core.models.delivery_readiness import (
    DeliveryReadiness, DeliveryReadinessContext, DeliveryReadinessDetail, ReadinessCursorBinding,
    ReadinessDetailCounts, ReadinessDetailEntry, ReadinessPage, ReadinessReadQuery, ReadinessRelation,
)
from okto_pulse.core.ports.delivery_readiness import DeliveryReadinessCursorPort, ReadinessReadError
from okto_pulse.core.services.delivery_readiness_diagnostics import (
    NativeDeliveryDiagnostics, render_diagnostic_action, render_diagnostic_issue, render_diagnostic_unit,
)

SUMMARY_MAX_BYTES = 32 * 1024
DETAIL_MAX_BYTES = 128 * 1024


def serialize_readiness_response(response: DeliveryReadiness | DeliveryReadinessDetail) -> bytes:
    """Exact compact UTF-8 representation, including every meaningful null."""
    return response.model_dump_json(exclude_none=False).encode('utf-8')


def _detail_identity(detail):
    return {name: [value.model_dump(mode='json') for value in getattr(detail, name)]
        for name in ('expected', 'observed', 'evidence_refs', 'pinpoints')} | {
            'id': detail.issue_id, 'owner': detail.owner, 'owner_state': detail.owner_state,
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
                entries=entries[offset:offset + size],
                page=self._page(binding, total=len(entries), offset=offset, returned=size))
        return self._fit(total=len(entries), offset=offset, limit=query.limit, maximum=DETAIL_MAX_BYTES,
            construct=construct, identity=query.issue_id)
