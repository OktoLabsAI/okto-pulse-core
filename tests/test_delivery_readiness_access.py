"""P05 current read authority is independent from detail and write access."""
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from okto_pulse.core.domain.delivery_assessment import assess_obligation_delivery, aggregate_delivery_metrics
from okto_pulse.core.domain.permissions import ALL_FLAGS
from okto_pulse.core.models.delivery_readiness import ReadinessReadQuery
from okto_pulse.core.ports.delivery_readiness import (
    READINESS_PERMISSION_OPERATIONS, ReadinessAccess, ReadinessAccessDenied, ReadinessActor,
    ReadinessCard, ReadinessReadError, ReadinessSnapshot, resolve_readiness_access,
)
from okto_pulse.core.ports.permission_policy import PermissionSet
from okto_pulse.core.services.delivery_readiness import (
    PreparedDeliveryReadiness, compose_authorized_delivery_diagnostics, serialize_readiness_response,
)
from test_delivery_obligation_assessment import snapshot, with_facts
from test_delivery_readiness_native_diagnostics import damaged_snapshot, execution_procedure_case
from test_delivery_readiness_paging import Cursor, context


def batch(value=None):
    value = value or with_facts(snapshot(), tests=())
    access = ReadinessAccess('reader', value.scope.board_id, 'local', READINESS_PERMISSION_OPERATIONS, True)
    cards = tuple(ReadinessCard(identity, kind, 'in_progress', 1, None, 'private-owner')
        for identity, kind in [('ui', 'normal'), ('authorization', 'normal'), ('test-card-1', 'test')])
    return ReadinessSnapshot(value, 'a' * 64, datetime(2026, 10, 14, tzinfo=timezone.utc), 1,
        'in_progress', cards, access=access)


def project(value):
    return compose_authorized_delivery_diagnostics(assess_obligation_delivery(value.facts), value, actor_id='reader')


def test_required_operations_are_native_and_null_authority_is_never_trusted():
    assert READINESS_PERMISSION_OPERATIONS <= set(ALL_FLAGS)
    actor = ReadinessActor('reader', 'human', 'local')
    with pytest.raises(ReadinessAccessDenied):
        resolve_readiness_access(actor=actor, board_id='board', realm_id='local', permissions=None, spec_write_access=True)
    resolved = resolve_readiness_access(actor=actor, board_id='board', realm_id='local',
        permissions=PermissionSet({'code_traceability': {'evidence': {'read': False}}}), spec_write_access=True)
    assert not resolved.allows('code_traceability.evidence.read')


@pytest.mark.parametrize('missing', ['access', 'spec.entity.read', 'code_traceability.evidence.read'])
def test_missing_aggregate_authority_is_an_error_not_empty_population(missing):
    value = batch()
    value = replace(value, access=None if missing == 'access' else replace(value.access,
        allowed_operations=value.access.allowed_operations - {missing}))
    with pytest.raises(ReadinessAccessDenied):
        project(value)


@pytest.mark.parametrize('field', ['actor_id', 'board_id'])
def test_authority_for_another_scope_cannot_enrich_diagnostics(field):
    value = batch()
    with pytest.raises(ReadinessReadError, match='readiness_scope_mismatch'):
        project(replace(value, access=replace(value.access, **{field: 'foreign'})))


@pytest.mark.parametrize('operation', ['code_traceability.target.read', 'code_traceability.investigation.read',
    'spec.tests.read', 'card.entity.read'])
def test_restricted_detail_is_removed_from_summary_actions_and_every_detail_page(operation):
    value = batch()
    assessment = assess_obligation_delivery(value.facts)
    metrics = aggregate_delivery_metrics(assessment)
    allowed = project(value)
    restricted = project(replace(value, access=replace(value.access,
        allowed_operations=value.access.allowed_operations - {operation})))
    assert {c.id for c in allowed.graph.causes} == {c.id for c in restricted.graph.causes}
    assert all(d.detail_state == 'restricted' for d in restricted.details)
    assert all(not d.expected and not d.observed and not d.evidence_refs and not d.pinpoints for d in restricted.details)
    assert all(a.state == 'unavailable' and not a.context_refs and 'evidence' not in a.arguments_json
        for a in restricted.graph.actions)
    scope = value.facts.scope
    prepared = PreparedDeliveryReadiness(context(restricted.graph,
        scope=dict(board_id=scope.board_id, spec_id=scope.spec_id, spec_edition=scope.edition,
            spec_version=1, card_id=None, card_type=None, card_version=None)), restricted, cursor=Cursor())
    response = prepared.page(ReadinessReadQuery(board_id=scope.board_id, spec_id=scope.spec_id), actor_id='reader')
    payloads = [serialize_readiness_response(response)]
    for issue in response.issues:
        assert issue.detail_state == 'restricted' and issue.affected_unit_count >= 1
        query = ReadinessReadQuery.model_validate(issue.detail_query.model_dump() | {'limit': 2})
        while True:
            detail = prepared.page(query, actor_id='reader')
            assert detail.detail_state == 'restricted'
            payloads.append(serialize_readiness_response(detail))
            if not detail.page.has_more:
                break
            query = query.model_copy(update={'cursor': detail.page.next_cursor})
    for encoded in payloads:
        assert b'private-owner' not in encoded
        for fact in value.facts.implementations:
            for execution in fact.executions:
                assert execution.source_ref.encode() not in encoded
                assert execution.execution_id.encode() not in encoded
    assert aggregate_delivery_metrics(assessment) == metrics


def test_current_write_denial_affects_guidance_not_proof_or_readable_details():
    value = batch()
    allowed = project(value)
    denied = project(replace(value, access=replace(value.access,
        allowed_operations=value.access.allowed_operations - {'spec.tests.execute'})))
    assert allowed.graph.causes == denied.graph.causes
    assert allowed.details == denied.details
    assert all(a.state == 'forbidden' for a in denied.graph.actions)


def test_D09_external_execution_guidance_honors_native_test_permission():
    facts, procedures = execution_procedure_case()
    value = replace(batch(facts), scenarios=procedures)
    allowed = project(value)
    denied = project(replace(value, access=replace(value.access,
        allowed_operations=value.access.allowed_operations - {'spec.tests.execute'})))
    assert all(a.kind == 'execute_verification' and a.state == 'ready' for a in allowed.graph.actions)
    assert all(a.state == 'forbidden' and a.permission == 'spec.tests.execute' for a in denied.graph.actions)
    assert allowed.graph.causes == denied.graph.causes and allowed.details == denied.details


def test_read_only_share_blocks_execution_but_does_not_invent_an_association_gate():
    facts, procedures = execution_procedure_case()
    value = replace(batch(facts), scenarios=procedures)
    readonly = replace(value, access=replace(value.access, spec_write_access=False))
    assert all(a.state == 'forbidden' for a in project(readonly).graph.actions)
    pending_association = batch()
    pending_association = replace(pending_association, access=replace(pending_association.access, spec_write_access=False))
    assert all(a.kind == 'associate_verification' and a.state == 'requires_input'
        for a in project(pending_association).graph.actions)


def test_visibility_change_invalidates_continuation_even_with_unchanged_factual_snapshot():
    value = batch()
    scope = value.facts.scope
    cursor = Cursor()
    def prepare(item):
        diagnostics = project(item)
        return PreparedDeliveryReadiness(context(diagnostics.graph,
            scope=dict(board_id=scope.board_id, spec_id=scope.spec_id, spec_edition=scope.edition,
                spec_version=1, card_id=None, card_type=None, card_version=None)), diagnostics, cursor=cursor)
    original = prepare(value)
    query = ReadinessReadQuery(board_id=scope.board_id, spec_id=scope.spec_id, limit=1)
    page = original.page(query, actor_id='reader')
    assert page.page.has_more
    restricted = prepare(replace(value, access=replace(value.access,
        allowed_operations=value.access.allowed_operations - {'code_traceability.target.read'})))
    assert restricted.context.snapshot_id == original.context.snapshot_id
    assert restricted.projection_id != original.projection_id
    with pytest.raises(ReadinessReadError, match='readiness_cursor_stale'):
        restricted.page(query.model_copy(update={'cursor': page.page.next_cursor}), actor_id='reader')


def test_owner_visibility_is_explicit_and_not_inferred_from_evidence_authorship():
    value = batch(damaged_snapshot())
    allowed = project(value)
    assert any(d.owner == 'private-owner' for d in allowed.details)
    restricted = project(replace(value, access=replace(value.access,
        allowed_operations=value.access.allowed_operations - {'agent.entity.read'})))
    assert 'private-owner' not in repr(restricted)
    assert all(d.owner is None and d.owner_state == 'restricted' for d in restricted.details)
    assert all(d.detail_state == 'available' for d in restricted.details)


def test_card_access_required_and_internal_actorless_batch_cannot_be_published():
    value = batch()
    assessment = assess_obligation_delivery(value.facts, card_id='ui', card_type='normal')
    with pytest.raises(ReadinessAccessDenied) as exc:
        compose_authorized_delivery_diagnostics(assessment, replace(value, access=replace(value.access,
            allowed_operations=value.access.allowed_operations - {'card.entity.read'})), actor_id='reader')
    assert exc.value.required_permission == 'card.entity.read'
