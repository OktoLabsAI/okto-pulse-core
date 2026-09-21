"""Prepare the real deterministic worker output through a read-only port view."""

from collections.abc import Mapping
from dataclasses import asdict, replace
from datetime import datetime
import json

from okto_pulse.core.ports.deterministic_projection import (
    DeterministicProjectionPlan, DeterministicProjectionSource,
)

_BOARD_PLAN_LIMIT = 64 * 1024 * 1024


class _BoardProjectionReader:
    """Only the three source reads used by projection preparation are exposed."""

    def __init__(self, delegate, board_id):
        self.delegate, self.board_id = delegate, board_id

    def _scoped(self, artifact):
        if artifact is not None:
            board = artifact.get('board_id') if isinstance(artifact, Mapping) else getattr(artifact, 'board_id', None)
            if board != self.board_id:
                raise ValueError('deterministic_projection_source_scope_mismatch')
        return artifact

    async def load_artifact(self, context, **kwargs):
        artifact = self._scoped(await self.delegate.load_artifact(context, **kwargs))
        if artifact is not None:
            identity = artifact.get('id') if isinstance(artifact, Mapping) else getattr(artifact, 'id', None)
            if identity != kwargs['artifact_id']:
                raise ValueError('deterministic_projection_source_identity_mismatch')
        return artifact

    async def list_artifacts(self, context, **kwargs):
        if kwargs.get('board_id', self.board_id) != self.board_id:
            raise ValueError('deterministic_projection_source_scope_mismatch')
        return tuple(self._scoped(item) for item in await self.delegate.list_artifacts(context, **kwargs))

    async def load_projection_inputs(self, context, **kwargs):
        if kwargs.get('board_id') != self.board_id:
            raise ValueError('deterministic_projection_source_scope_mismatch')
        return await self.delegate.load_projection_inputs(context, **kwargs)


class CoreDeterministicProjectionPlanner:
    def __init__(self, persistence, *, dependencies=None):
        self.persistence = persistence
        self.dependencies = dependencies

    def _dependency_sources(self, census, board_id, captured_at):
        cut = captured_at.isoformat()
        base = tuple({**row.to_dict(), '_rebuild_manifest_created_at': cut}
            for row in census.materializable_sources)
        candidates = tuple({**row.to_dict(), '_rebuild_manifest_created_at': cut,
            '_rebuild_dependency_closure_candidate': 'code_evidence_supersedence'}
            for row in census.skipped_expired_working
            if row.artifact_type == 'code_evidence' and row.source_artifact_status == 'superseded')
        if not any(row['artifact_type'] == 'code_evidence' for row in base):
            return base, ()
        if self.dependencies is None:
            raise ValueError('deterministic_projection_dependency_resolver_required')
        # The adapter may only select captured historical candidates. Keep an
        # independent copy so mutation of the resolver request cannot authorize
        # changed or omitted denominator sources.
        request = tuple(json.loads(json.dumps(base + candidates, allow_nan=False)))
        resolved = self.dependencies.resolve(board_id=board_id, sources=request)
        if type(resolved) is not tuple or len(resolved) > len(base) + len(candidates):
            raise ValueError('deterministic_projection_dependency_selection_invalid')
        expected = {(row['artifact_type'], row['id']): row for row in base}
        allowed = {}
        for candidate in candidates:
            row = dict(candidate)
            row['_rebuild_dependency_closure'] = row.pop('_rebuild_dependency_closure_candidate')
            allowed[(row['artifact_type'], row['id'])] = row
        seen, closure = set(), []
        for row in resolved:
            if type(row) is not dict:
                raise ValueError('deterministic_projection_dependency_selection_invalid')
            identity = (row.get('artifact_type'), row.get('id'))
            if identity in seen or row != (expected.get(identity) or allowed.get(identity)):
                raise ValueError('deterministic_projection_dependency_selection_invalid')
            seen.add(identity)
            if identity in allowed:
                closure.append(row)
        if not set(expected) <= seen:
            raise ValueError('deterministic_projection_dependency_selection_invalid')
        return resolved, tuple(closure)

    async def prepare_board(self, context, *, board_id, source_rows, cognitive_rows, captured_at):
        from okto_pulse.core.kg.rebuild_sources import RebuildSourceEnumerator, cognitive_durable_digest_from_rows
        from okto_pulse.core.kg.board_rebuild_adapter import (
            DETERMINISTIC_SOURCE_ARTIFACT_TYPES, queue_artifact_type, rebuild_source_order_key,
        )
        if (type(source_rows) is not tuple or type(cognitive_rows) is not tuple
                or len(source_rows) > 100_000 or len(cognitive_rows) > 100_000
                or not isinstance(captured_at, datetime) or captured_at.tzinfo is None):
            raise ValueError('deterministic_projection_census_invalid')
        # Detach the supplied census before the first awaited source read.
        raw = json.dumps({'sources': source_rows, 'cognitive': cognitive_rows},
            ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
        if len(raw) > _BOARD_PLAN_LIMIT:
            raise ValueError('deterministic_projection_census_limit')
        captured = json.loads(raw)
        if any(row.get('board_id') != board_id for row in captured['cognitive']):
            raise ValueError('deterministic_projection_cognitive_scope_mismatch')
        identities = [(row.get('artifact_type'), row.get('id')) for row in captured['sources']]
        if len(identities) != len(set(identities)):
            raise ValueError('deterministic_projection_duplicate_source')
        if any(row.get('artifact_type') == 'sprint' for row in captured['sources']):
            raise ValueError('deterministic_projection_retired_source')
        census = RebuildSourceEnumerator(source_store=lambda _: captured['sources'], now=captured_at,
            cognitive_digest_provider=lambda _: cognitive_durable_digest_from_rows(captured['cognitive'])).enumerate(board_id=board_id)
        census = replace(census, generated_at=captured_at.isoformat())
        if census.has_non_deterministic_inputs:
            raise ValueError('deterministic_projection_source_requires_review')
        sources, closure = self._dependency_sources(census, board_id, captured_at)
        plans = []
        accumulated_bytes = len(raw)
        for row in sorted(sources, key=rebuild_source_order_key):
            if row['artifact_type'] not in DETERMINISTIC_SOURCE_ARTIFACT_TYPES:
                if row['artifact_type'] == 'decision':
                    continue  # Derived decisions remain in the census; their Spec owns projection.
                raise ValueError('deterministic_projection_source_type_unsupported')
            source = DeterministicProjectionSource(board_id, queue_artifact_type(row['artifact_type']), row['id'])
            planned = await self.prepare(context, source)
            accumulated_bytes += len(planned.document)
            if accumulated_bytes > _BOARD_PLAN_LIMIT:
                raise ValueError('deterministic_projection_census_limit')
            plans.append(json.loads(planned.document))
        result = json.dumps({'format': 'deterministic-board-projection-plan/v2',
            'board_id': board_id, 'captured_at': captured_at.isoformat(), 'source_rows': captured['sources'],
            'cognitive_rows': captured['cognitive'], 'census': census.to_dict(),
            'dependency_closure': closure, 'plans': plans},
            ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
        if len(result) > _BOARD_PLAN_LIMIT:
            raise ValueError('deterministic_projection_census_limit')
        return result

    async def prepare(self, context, source):
        if type(source) is not DeterministicProjectionSource:
            raise TypeError('deterministic_projection_source_required')
        from okto_pulse.core.application.processors.consolidation import (
            _prepare_deterministic_projection, _worker_node_to_candidate, _worker_edge_to_candidate,
        )
        preparation = await _prepare_deterministic_projection(context, source,
            persistence=_BoardProjectionReader(self.persistence, source.board_id))
        if preparation is False:
            raise ValueError('deterministic_projection_source_unavailable')
        document = {'format': 'deterministic-projection-plan/v1', 'source': asdict(source),
            'disposition': 'skipped_cancelled' if preparation is True else 'prepared', 'projection': None}
        if preparation is not True:
            result, artifact = preparation
            document['projection'] = {
                'nodes': [_worker_node_to_candidate(node).model_dump(mode='json') for node in result.nodes],
                'edges': [_worker_edge_to_candidate(edge).model_dump(mode='json') for edge in result.edges],
                'missing_link_candidates': [asdict(item) for item in result.missing_link_candidates],
                'spec_lineage_parent_intent': result.spec_lineage_parent_intent.value,
                'relational_projection_candidate_ids': sorted(result.relational_projection_candidate_ids),
                'relational_projection_active_set_intent': (asdict(result.relational_projection_active_set_intent)
                    if result.relational_projection_active_set_intent is not None else None),
                'content_hash': result.content_hash, 'raw_content': result.raw_content,
            }
        return DeterministicProjectionPlan(json.dumps(document, ensure_ascii=False, sort_keys=True,
            separators=(',', ':'), allow_nan=False).encode('utf-8'))
