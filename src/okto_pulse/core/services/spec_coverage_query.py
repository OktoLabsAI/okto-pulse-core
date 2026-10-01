"""Keep normative structural coverage and admitted delivery proof distinct.

This is a projection of the existing two resolvers, never a third gate. The
caller authorizes the source scope before collecting the internal snapshot.
"""
from dataclasses import asdict
import hashlib
import json
import re

from okto_pulse.core.domain.delivery_evidence import evaluate_delivery_coverage
from okto_pulse.core.domain.delivery_inventory import COLLECTIONS
from okto_pulse.core.ports.analytics_foundation import require_utc_datetime
from okto_pulse.core.ports.spec_coverage_query import (
    MAX_SPEC_COVERAGE_BYTES, MAX_SPEC_COVERAGE_FACTS, MAX_SPEC_COVERAGE_ITEMS,
    SpecCoverageQuery, SpecCoverageSnapshot,
)
from okto_pulse.core.services.analytics_service import spec_coverage_summary
from okto_pulse.core.services.spec_coverage_graph import project_spec_coverage_graph


def _encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def _proof_status(satisfied, records, waivers, *, complete):
    if not complete:
        return 'unknown'
    if satisfied:
        return 'satisfied_with_waiver' if waivers else 'proven'
    return 'partial' if records or waivers else 'missing'


def project_spec_coverage(query: SpecCoverageQuery, snapshot: SpecCoverageSnapshot) -> dict:
    scope = snapshot.scope
    spec = snapshot.spec
    if ((scope.board_id, scope.spec_id, snapshot.actor_scope_ref)
            != (query.board_id, query.spec_id, query.actor_scope_ref)
            or (getattr(spec, 'board_id', None), getattr(spec, 'id', None), getattr(spec, 'edition', None))
            != (scope.board_id, scope.spec_id, scope.edition)):
        raise ValueError('spec_coverage_scope_mismatch')
    checked = require_utc_datetime(snapshot.checked_at, field='spec_coverage_checked_at').isoformat()
    if type(snapshot.source_revision) is not str or not snapshot.source_revision.strip() or len(snapshot.source_revision) > 4096:
        raise ValueError('spec_coverage_source_revision_invalid')
    if type(snapshot.source_complete) is not bool or type(snapshot.cards) is not tuple:
        raise ValueError('spec_coverage_source_invalid')
    collections = [getattr(spec, name, None) or [] for _, name in COLLECTIONS]
    collections.append(getattr(spec, 'test_scenarios', None) or [])
    if any(type(items) is not list for items in collections):
        raise ValueError('spec_coverage_source_invalid')
    if len(snapshot.cards) > MAX_SPEC_COVERAGE_ITEMS or sum(map(len, collections)) > MAX_SPEC_COVERAGE_ITEMS:
        raise ValueError('spec_coverage_source_bound')
    ids = set()
    for card in snapshot.cards:
        if (getattr(card, 'board_id', None), getattr(card, 'spec_id', None)) != (scope.board_id, scope.spec_id):
            raise ValueError('spec_coverage_card_outside_scope')
        if type(getattr(card, 'id', None)) is not str or not card.id.strip():
            raise ValueError('spec_coverage_card_invalid')
        if card.id in ids:
            raise ValueError('spec_coverage_duplicate_card')
        ids.add(card.id)
    if snapshot.delivery_state not in {'available', 'restricted', 'unavailable'}:
        raise ValueError('spec_coverage_delivery_state_invalid')
    if (snapshot.delivery is None) != (snapshot.delivery_state != 'available'):
        raise ValueError('spec_coverage_delivery_state_inconsistent')
    if not query.read_delivery and (snapshot.delivery is not None or snapshot.delivery_state != 'restricted'):
        raise ValueError('spec_coverage_delivery_outside_authority')
    if snapshot.delivery is not None and snapshot.delivery.scope != scope:
        raise ValueError('spec_coverage_delivery_scope_mismatch')

    # Reuse the normative structural resolver verbatim. An incomplete inventory
    # cannot use its legacy zero-denominator convention as a coverage claim.
    structure = spec_coverage_summary(spec, cards=list(snapshot.cards)) if snapshot.source_complete else None
    rows = []
    blockers = []
    rejected = []
    proof_complete = False
    if snapshot.delivery is not None:
        delivery = snapshot.delivery
        if type(delivery.complete) is not bool:
            raise ValueError('spec_coverage_delivery_state_invalid')
        if (len(delivery.obligations) > MAX_SPEC_COVERAGE_ITEMS
                or sum(len(items) for items in (delivery.implementations, delivery.tests, delivery.waivers)) > MAX_SPEC_COVERAGE_FACTS):
            raise ValueError('spec_coverage_delivery_bound')
        evaluation = evaluate_delivery_coverage(delivery)
        blockers = list(evaluation.blockers)
        rejected = list(evaluation.rejected_record_ids)
        proof_complete = snapshot.source_complete and delivery.complete and not set(blockers).intersection({
            'delivery_projection_incomplete', 'delivery_obligations_ambiguous', 'delivery_record_identity_ambiguous',
            'delivery_effective_context_unavailable', 'delivery_effective_resolution_limit',
            'delivery_effective_inventory_incomplete', 'delivery_effective_inventory_mismatch',
            'delivery_verification_methods_unavailable', 'delivery_scoped_population_mismatch',
            'delivery_contribution_allocation_invalid',
        })
        for row in evaluation.rows:
            rows.append({
                'kind': 'delivery',
                'obligation_ref': row.obligation.binding.obligation_ref,
                'semantic_sha256': row.obligation.binding.semantic_sha256,
                'title': row.obligation.title[:240],
                'implementation': _proof_status(row.implementation_satisfied, row.implementation_ids,
                    row.implementation_waiver_ids, complete=proof_complete),
                'verification': _proof_status(row.test_satisfied, row.test_ids, row.test_waiver_ids, complete=proof_complete),
                'implementation_record_refs': list(row.implementation_ids),
                'verification_record_refs': list(row.test_ids),
                'implementation_waiver_refs': list(row.implementation_waiver_ids),
                'verification_waiver_refs': list(row.test_waiver_ids),
                'required_card_refs': [f'card:{value}' for value in getattr(row, 'required_card_ids', ())],
                'missing_card_refs': [f'card:{value}' for value in getattr(row, 'missing_card_ids', ())],
                'missing_criteria': [list(value) for value in getattr(row, 'missing_criteria', ())],
            })
    rows.sort(key=lambda row: row['obligation_ref'])
    counts = {
        'obligations': len(rows) if proof_complete else None,
        'implementation_proven': sum(row['implementation'] == 'proven' for row in rows) if proof_complete else None,
        'verification_proven': sum(row['verification'] == 'proven' for row in rows) if proof_complete else None,
        'observed_obligations': len(rows) if snapshot.delivery is not None else None,
    }
    graph_summary, graph_rows, graph_state, generation = project_spec_coverage_graph(query, snapshot)
    rows += graph_rows
    response = {
        'view': 'coverage', 'subject_ref': f'spec:{query.spec_id}', 'authority': 'informational',
        'data_source': 'composed' if snapshot.graph is not None and snapshot.graph.state == 'observed' else 'relational', 'edition': scope.edition,
        'projection_freshness': {'state': graph_state, 'graph_generation': generation,
            'source_checkpoint': snapshot.source_revision, 'projection_checkpoint': None, 'checked_at': checked},
        'completeness': {'complete_for_scope': False,
            'truncated': not snapshot.source_complete or bool(snapshot.graph and snapshot.graph.truncated),
            'limitations': ['full_projection_checkpoint_unavailable'] + ([] if snapshot.source_complete else ['source_inventory_incomplete'])},
        'structure': {'authority': 'spec_coverage_summary', 'interpretation': 'planning_links_not_delivery_proof',
            'complete_for_scope': snapshot.source_complete, 'summary': structure, 'graph': graph_summary},
        'delivery': {'state': snapshot.delivery_state, 'authority': 'evaluate_delivery_coverage',
            'complete_for_scope': proof_complete, 'counts': counts, 'blockers': blockers,
            'rejected_record_refs': rejected,
            'interpretation': 'admitted_proof_and_authorized_waivers_do_not_approve_other_gates'},
        'items': [], 'next_cursor': None,
    }
    # Bind the cursor to the actor, full source revision and all computed rows,
    # not just Spec.version, the current page or the observation clock.
    identity = asdict(query)
    identity.pop('cursor')
    stable = {**response, 'items': rows, 'projection_freshness': {
        key: value for key, value in response['projection_freshness'].items() if key != 'checked_at'}}
    digest = hashlib.sha256(_encoded([identity, stable])).hexdigest()
    offset = 0
    if query.cursor is not None:
        match = re.fullmatch(r'spec-coverage-v1:([0-9a-f]{64}):([1-9][0-9]{0,5})', query.cursor)
        if match is None:
            raise ValueError('spec_coverage_cursor_invalid')
        if match[1] != digest:
            raise ValueError('spec_coverage_cursor_stale')
        offset = int(match[2])
        if offset >= len(rows):
            raise ValueError('spec_coverage_cursor_invalid')
    if len(_encoded(response)) > MAX_SPEC_COVERAGE_BYTES:
        raise ValueError('spec_coverage_summary_payload_bound')
    for row in rows[offset:offset + query.limit]:
        response['items'].append(row)
        end = offset + len(response['items'])
        response['next_cursor'] = f'spec-coverage-v1:{digest}:{end}' if end < len(rows) else None
        if len(_encoded(response)) > MAX_SPEC_COVERAGE_BYTES:
            response['items'].pop()
            if not response['items']:
                raise ValueError('spec_coverage_single_row_payload_bound')
            response['next_cursor'] = f'spec-coverage-v1:{digest}:{end - 1}'
            break
    return response
