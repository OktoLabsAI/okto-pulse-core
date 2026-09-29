"""KG7.5: canonical graph presence must not discharge substantive debt."""
import uuid

import pytest

from okto_pulse.core.kg.canonical_learning_partition import (
    HISTORICAL_DEBT_REASON, PARTITION_TARGET_STATUS, _stable_content_hash,
    reconcile_canonical_learning_partition_debt,
    detect_historical_canonical_learning_debt,
)
from okto_pulse.core.ports.canonical_debt import get_canonical_debt_store
from okto_pulse.core.services.canonical_debt_service import upsert_canonical_debt
from test_kg_r7_imp2 import _setup_board, _seed_learning_validating_bug


@pytest.mark.asyncio
@pytest.mark.parametrize('restriction', ['target', 'reason', 'blocked', 'dlq'])
@pytest.mark.parametrize('operation', ['reconcile', 'detect'])
async def test_partition_reconciliation_preserves_mixed_restrictions(db_factory, restriction, operation):
    board = await _setup_board(db_factory)
    bug_id = str(uuid.uuid4())
    ref = f'bug:{bug_id}'
    learning_id, graph_bug = _seed_learning_validating_bug(
        board, learning_source_ref=ref, bug_layer='canonical' if operation == 'reconcile' else 'working')
    from kg_schema_testing import open_board_connection
    with open_board_connection(board) as (_, connection):
        connection.execute('MATCH (b:Bug) WHERE b.id = $id SET b.source_artifact_ref = $ref',
                           {'id': graph_bug, 'ref': ref}).close()
    async with db_factory() as db:
        original = await upsert_canonical_debt(db, board_id=board, artifact_type='bug',
            artifact_id=bug_id, source_ref=ref, content_hash=_stable_content_hash(ref, learning_id),
            target_status='quarantine' if restriction == 'target' else PARTITION_TARGET_STATUS,
            failure_reason='authority_denied' if restriction == 'reason' else HISTORICAL_DEBT_REASON,
            canonical_state='blocked' if restriction == 'blocked' else 'pending',
            dlq_ref='dlq:integrity-review' if restriction == 'dlq' else None)
        await db.commit()
    async with db_factory() as db:
        operation_fn = (reconcile_canonical_learning_partition_debt if operation == 'reconcile'
                        else detect_historical_canonical_learning_debt)
        result = await operation_fn(db, board_id=board,
            actor_id='system:maintenance')
        await db.commit()
    async with db_factory() as db:
        actual = await get_canonical_debt_store().get(db, debt_id=original.id)
    if operation == 'reconcile':
        assert result['committed_count'] == 0
    assert actual.canonical_state == original.canonical_state
    assert actual.failure_reason == original.failure_reason
    assert actual.dlq_ref == original.dlq_ref
    assert actual.evidence_ref == original.evidence_ref


@pytest.mark.asyncio
@pytest.mark.parametrize('damage', ['wrong_origin', 'superseded_bug', 'superseded_learning', 'asserted_layer', 'missing_version'])
async def test_partition_requires_matching_current_evidence(db_factory, damage):
    board = await _setup_board(db_factory)
    bug_id = str(uuid.uuid4())
    ref = f'bug:{bug_id}'
    learning_id, graph_bug = _seed_learning_validating_bug(board,
        learning_source_ref=ref, bug_layer='working' if damage == 'asserted_layer' else 'canonical')
    from kg_schema_testing import open_board_connection
    with open_board_connection(board) as (_, connection):
        if damage == 'wrong_origin':
            connection.execute('MATCH (b:Bug) WHERE b.id = $id SET b.source_artifact_ref = $ref',
                {'id': graph_bug, 'ref': f'bug:{uuid.uuid4()}'}).close()
        elif damage.startswith('superseded_'):
            label, identity = ('Bug', graph_bug) if damage == 'superseded_bug' else ('Learning', learning_id)
            connection.execute(f'MATCH (n:{label}) WHERE n.id = $id SET n.superseded_by = $replacement',
                {'id': identity, 'replacement': 'new-version'}).close()
    async with db_factory() as db:
        await upsert_canonical_debt(db, board_id=board, artifact_type='bug', artifact_id=bug_id,
            source_ref=ref, content_hash=_stable_content_hash(ref, learning_id),
            target_status=PARTITION_TARGET_STATUS, canonical_state='pending', failure_reason=HISTORICAL_DEBT_REASON,
            source_version='v2' if damage == 'missing_version' else None)
        await db.commit()
    async with db_factory() as db:
        result = await reconcile_canonical_learning_partition_debt(db, board_id=board,
            actor_id='system:maintenance', extra_evidence=[{'source_ref': ref,
                'content_hash': _stable_content_hash(ref, learning_id), 'evidence_layer': 'canonical',
                'source_version': 'v2'}])
        assert result['committed_count'] == 0
