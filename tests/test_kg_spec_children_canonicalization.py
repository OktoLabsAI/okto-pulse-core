"""Behavioral tests for canonicalization of spec-done children (spec eaf185c9, card 302044a7).

    * ts_838e4ef9 (card 43bc4311) AC1 — a done spec materializes ALL structured children
      (FR/TR/AC/BR/API/TestScenario/IR/OR/Decision) as graph_layer=canonical + canonical_eligible.
    * ts_e16a76cd (card adcb1d25) AC2 — a pre-done spec keeps the same children working-only.
    * ts_bd28324f (card 42034e1c) AC5 — promotion working->canonical by source_artifact_ref
      without duplicating; including the human_curated subcase: maturity metadata promotes,
      but protected content (title/content/context/justification) is NOT overwritten.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.primitives import (
    _apply_graph_node_create,
    add_edge_candidate,
    begin_consolidation,
    commit_consolidation,
    propose_reconciliation,
)
from kg_schema_testing import open_board_connection
from okto_pulse.core.kg.schemas import (
    AddEdgeCandidateRequest,
    BeginConsolidationRequest,
    CommitConsolidationRequest,
    KGNodeType,
    NodeCandidate,
    ProposeReconciliationRequest,
)
from okto_pulse.core.application.processors.consolidation import (
    _worker_edge_to_candidate,
    _worker_node_to_candidate,
)
from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker


def _full_spec(status: str) -> dict:
    """A spec carrying ALL nine structured child families."""
    return {
        "id": f"spec-{uuid.uuid4().hex[:8]}",
        "title": "Full spec",
        "description": "spec with every child type",
        "status": status,
        "board_id": "board-canon",
        "context": "## Decisions\n- Use PostgreSQL\n",
        "functional_requirements": ["FR alpha", "FR beta"],
        "technical_requirements": [{"text": "TR alpha"}],
        "acceptance_criteria": ["AC alpha"],
        "business_rules": ["BR alpha"],
        "test_scenarios": [
            {
                "id": "ts_x",
                "title": "Scenario",
                "given": "g",
                "when": "w",
                "then": "t",
                "linked_criteria": ["AC alpha"],
            }
        ],
        "api_contracts": [{"name": "GET /x", "description": "an api"}],
        "integration_requirements": [
            {"id": "ir_x", "title": "IR alpha", "description": "integ"}
        ],
        "observability_requirements": [
            {"id": "or_x", "title": "OR alpha", "metric_name": "m", "description": "obs"}
        ],
        "decisions": [
            {"id": "dec_x", "title": "Decision alpha", "status": "active", "rationale": "r"}
        ],
    }


def _children(result) -> list:
    return [
        n
        for n in result.nodes
        if not n.source_artifact_ref.startswith("board:")
        and n.source_artifact_ref != "tech_entities.yml"
    ]


async def _ensure_relational_board(db_factory, board_id: str, owner_id: str) -> None:
    from sqlalchemy_test_models import Board

    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name=board_id, owner_id=owner_id))
            await db.commit()


def _count_api_implements_constraint(board_id: str, *, api_title: str, tr_title: str) -> int:
    with open_board_connection(board_id) as (_db, kconn):
        res = kconn.execute(
            "MATCH (a:APIContract)-[r:implements]->(c:Constraint) "
            "WHERE a.title = $api_title AND c.title = $tr_title "
            "RETURN count(r)",
            {"api_title": api_title, "tr_title": tr_title},
        )
        try:
            if res.has_next():
                return int(res.get_next()[0])
        finally:
            try:
                res.close()
            except Exception:
                pass
    return 0


# ---------------------------------------------------------------------------
# ts_838e4ef9 (card 43bc4311) AC1 — done spec -> all children canonical
# ---------------------------------------------------------------------------


def test_spec_done_materializes_all_children_canonical():
    full = _full_spec("done")
    children = _children(DeterministicWorker().process_spec(full))

    assert children
    assert all(n.graph_layer == "canonical" for n in children)
    assert all(n.maturity_status == "canonical_eligible" for n in children)

    # IR + OR + decisions materialize as their own canonical nodes — removing
    # them strictly reduces the emitted node set (so every family contributes).
    without = {
        **full,
        "integration_requirements": [],
        "observability_requirements": [],
        "decisions": [],
    }
    without_children = _children(DeterministicWorker().process_spec(without))
    assert len(children) > len(without_children)


# ---------------------------------------------------------------------------
# ts_e16a76cd (card adcb1d25) AC2 — pre-done spec -> working-only
# ---------------------------------------------------------------------------


def test_spec_predone_children_are_working_only():
    children = _children(DeterministicWorker().process_spec(_full_spec("draft")))

    assert children
    # working-only: a canonical-only KG read filters on graph_layer=canonical,
    # so these nodes are excluded by construction.
    assert all(n.graph_layer == "working" for n in children)
    assert all(n.maturity_status == "working_immature" for n in children)


@pytest.mark.asyncio
async def test_commit_materializes_api_contract_implements_tr_constraint(
    board_id, db_factory, board_handle,
):
    await _ensure_relational_board(db_factory, board_id, "system:historical_consolidation")
    spec_id = f"spec-{uuid.uuid4().hex[:8]}"
    spec = {
        **_full_spec("done"),
        "id": spec_id,
        "title": "TR linked API contract",
        "status": "done",
        "board_id": board_id,
        "created_at": "2001-01-02T00:00:00+00:00",
        "updated_at": "2002-03-04T00:00:00+00:00",
        "acceptance_criteria": [
            {"id": "ac-login", "text": "Login succeeds"},
            {"id": "ac-audit", "text": "Event is written"},
            {"id": "ac-duplicate-a", "text": "Repeated condition"},
            {"id": "ac-duplicate-b", "text": "Repeated condition"},
        ],
        "test_scenarios": [
            {"id": "ts-login", "title": "Login scenario", "given": "an account",
             "when": "login", "then": "a session", "linked_criteria": ["ac-login"]},
            {"id": "ts-audit", "title": "Audit scenario", "given": "an account",
             "when": "login", "then": "an event", "linked_criteria": ["ac-audit"]},
            {"id": "ts-ambiguous", "title": "Legacy ambiguous scenario", "given": "an account",
             "when": "login", "then": "an observation", "linked_criteria": ["Repeated condition"]},
        ],
        "functional_requirements": [
            {"id": "fr-login", "text": "User can log in"},
            {"id": "fr-logout", "text": "User can log out"},
        ],
        "technical_requirements": [
            {"id": "tr-audit-events", "text": "Login API emits audit events"},
        ],
        "decisions": [{"id": "dec_explicit", "title": "Require login audit", "status": "active",
            "linked_requirements": ["fr-login", "tr-audit-events"]}],
        "business_rules": [{"id": "br_one", "title": "Audit rule", "rule": "Keep audit", "linked_requirements": ["fr-login"]}],
        "integration_requirements": [{"id": "ir_one", "title": "Audit interface", "linked_requirements": ["fr-login"]}],
        "observability_requirements": [{"id": "or_one", "title": "Observe audit", "linked_integration_requirements": ["ir_one"]}],
        "api_contracts": [
            {
                "id": "api-login",
                "method": "POST",
                "path": "/login",
                "linked_requirements": ["tr-audit-events"],
                "linked_rules": ["br_one"],
            },
        ],
    }
    async def project_current_spec():
        worker_result = DeterministicWorker().process_spec(spec)
        from okto_pulse.core.kg.source_projection_metadata import prepare_root_metadata

        node_candidates = [_worker_node_to_candidate(node) for node in worker_result.nodes]
        metadata = await prepare_root_metadata(
            None, SimpleNamespace(artifact_type="spec", artifact_id=spec_id),
            spec, node_candidates, None,
        )
        for node in node_candidates:
            node._source_projection_metadata = metadata.get(node.candidate_id)

        async with db_factory() as db:
            begin = await begin_consolidation(
                BeginConsolidationRequest(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id=spec_id,
                    raw_content=worker_result.raw_content or "tr linked api contract",
                    deterministic_candidates=node_candidates,
                ),
                agent_id="system:historical_consolidation",
                db=db,
                force_reprocess=True,
                relational_projection_active_set_intents=worker_result.relational_projection_active_set_intents,
            )

        for edge in worker_result.edges:
            await add_edge_candidate(
                AddEdgeCandidateRequest(
                    session_id=begin.session_id,
                    candidate=_worker_edge_to_candidate(edge),
                ),
                agent_id="system:historical_consolidation",
            )
        await propose_reconciliation(
            ProposeReconciliationRequest(session_id=begin.session_id),
            agent_id="system:historical_consolidation",
            db=None,
            force_reprocess=True,
        )
        async with db_factory() as db:
            commit = await commit_consolidation(
                CommitConsolidationRequest(session_id=begin.session_id),
                agent_id="system:historical_consolidation",
                db=db,
            )

        assert commit.connectivity["passed"] is True
        return node_candidates

    node_candidates = await project_current_spec()
    assert await _count_api_implements_constraint_async(
        board_id,
        api_title="POST /login",
        tr_title="Login API emits audit events",
    ) == 1

    lineage_shapes = [
        ('Constraint', 'Requirement', 'derives_from', 'br_requirement', 'business_rule:br_one', 'fr:fr-login'),
        ('Requirement', 'Requirement', 'derives_from', 'ir_requirement', 'integration_requirement:ir_one', 'fr:fr-login'),
        ('Constraint', 'Requirement', 'derives_from', 'or_integration', 'observability_requirement:or_one', 'integration_requirement:ir_one'),
        ('APIContract', 'Constraint', 'implements', 'api_business_rule', 'api_contract:api-login', 'business_rule:br_one'),
    ]
    def lineage_pairs(graph):
        actual = set()
        for source_type, target_type, edge_type, rule, _source, _target in lineage_shapes:
            rows = graph.execute(f'MATCH (a:{source_type})-[r:{edge_type}]->(b:{target_type}) '
                'WHERE r.rule_id=$rule RETURN a.source_artifact_ref,b.source_artifact_ref,r.rule_id,r.confidence,r.layer,r.created_by',
                {'rule': f'{edge_type}/{rule}@v2.1'})
            try:
                while rows.has_next():
                    row = tuple(rows.get_next())
                    assert row not in actual
                    actual.add(row)
            finally:
                rows.close()
        return actual

    def assert_chronology():
        with open_board_connection(board_id) as (_db, graph):
            checked = {}
            for node in node_candidates:
                label = str(getattr(node.node_type, "value", node.node_type))
                result = graph.execute(
                    f"MATCH (n:{label}) WHERE n.source_artifact_ref=$ref "
                    "RETURN n.id, n.source_created_at, n.source_updated_at",
                    {"ref": node.source_artifact_ref},
                )
                try:
                    assert result.has_next(), node.source_artifact_ref
                    identity, created, updated = result.get_next()
                    assert not result.has_next(), node.source_artifact_ref
                    if node.source_artifact_ref == f"spec:{spec_id}":
                        assert created is not None and updated is not None
                    else:
                        assert created is None and updated is None, node.source_artifact_ref
                    checked[node.source_artifact_ref] = identity
                finally:
                    result.close()
            assert {f"spec:{spec_id}:fr:fr-login", f"spec:{spec_id}:tr:tr-audit-events"} <= checked.keys()
            links = graph.execute(
                "MATCH (s:TestScenario)-[r:tests]->(c:Criterion) "
                "RETURN s.source_artifact_ref, c.source_artifact_ref, r.confidence, r.rule_id"
            )
            try:
                actual = set()
                row_count = 0
                while links.has_next():
                    actual.add(tuple(links.get_next()))
                    row_count += 1
                assert row_count == len(actual)
                assert actual == {
                    (f"spec:{spec_id}:test_scenario:ts-{suffix}",
                     f"spec:{spec_id}:ac:ac-{suffix}", 1.0, "tests/ac_match@v2.1")
                    for suffix in ("login", "audit")
                }
            finally:
                links.close()
            decisions = set()
            for kind in ('Requirement', 'Constraint'):
                links = graph.execute(
                    f'MATCH (d:Decision)-[r:derives_from]->(t:{kind}) '
                    'RETURN d.source_artifact_ref,t.source_artifact_ref,r.rule_id,r.confidence')
                try:
                    while links.has_next():
                        row = tuple(links.get_next())
                        assert row not in decisions
                        decisions.add(row)
                finally:
                    links.close()
            assert decisions == {
                (f'spec:{spec_id}:decision:dec_explicit', f'spec:{spec_id}:{section}:{identity}',
                 'derives_from/explicit_link@v2.1', 1.0)
                for section, identity in [('fr', 'fr-login'), ('tr', 'tr-audit-events')]
            }
            assert lineage_pairs(graph) == {
                (f'spec:{spec_id}:{source}', f'spec:{spec_id}:{target}', f'{edge_type}/{rule}@v2.1',
                 1.0, 'deterministic', 'worker_layer1')
                for _source_type, _target_type, edge_type, rule, source, target in lineage_shapes
            }
            return checked

    original_ids = await run_blocking_graph_io(
        assert_chronology, task_name="tests.spec_children.source_chronology",
    )

    for field in ("acceptance_criteria", "functional_requirements", "test_scenarios"):
        spec[field] = list(reversed(spec[field]))
    node_candidates = await project_current_spec()
    reordered_ids = await run_blocking_graph_io(
        assert_chronology, task_name="tests.spec_children.reordered_identity",
    )
    assert reordered_ids == original_ids

    original_links = {scenario["id"]: list(scenario.get("linked_criteria") or []) for scenario in spec["test_scenarios"]}
    for scenario in spec["test_scenarios"]:
        scenario["linked_criteria"] = []
    decision_links = spec['decisions'][0]['linked_requirements']
    spec['decisions'][0]['linked_requirements'] = []
    lineage_fields = [('business_rules', 'linked_requirements'), ('integration_requirements', 'linked_requirements'),
        ('observability_requirements', 'linked_integration_requirements'), ('api_contracts', 'linked_rules')]
    lineage_links = {(collection, field): spec[collection][0][field] for collection, field in lineage_fields}
    for collection, field in lineage_fields:
        spec[collection][0][field] = []
    await project_current_spec()

    def assert_removed():
        with open_board_connection(board_id) as (_db, graph):
            assert lineage_pairs(graph) == set()
            links = graph.execute("MATCH (s:TestScenario)-[r:tests]->(c:Criterion) RETURN r.rule_id")
            try:
                assert not links.has_next()
            finally:
                links.close()
            for kind in ('Requirement', 'Constraint'):
                links = graph.execute(f'MATCH (d:Decision)-[r:derives_from]->(t:{kind}) RETURN r.rule_id')
                try:
                    assert not links.has_next()
                finally:
                    links.close()
    await run_blocking_graph_io(assert_removed, task_name="tests.spec_children.removed_links")
    for scenario in spec["test_scenarios"]:
        scenario["linked_criteria"] = original_links[scenario["id"]]
    spec['decisions'][0]['linked_requirements'] = decision_links
    for (collection, field), links in lineage_links.items():
        spec[collection][0][field] = links
    node_candidates = await project_current_spec()
    assert await run_blocking_graph_io(assert_chronology, task_name="tests.spec_children.restored_links") == original_ids


# ---------------------------------------------------------------------------
# ts_bd28324f (card 42034e1c) AC5 — promotion without duplicating + curated guard
# ---------------------------------------------------------------------------


def _seed_decision(board_id, *, title, human_curated):
    """Seed a root Entity + a WORKING (optionally human_curated) Decision that
    ``belongs_to`` it, so the source_artifact_ref merge has a CONNECTED node to
    reuse (the connectivity guard otherwise rejects an isolated node). Returns
    the Decision's source_artifact_ref."""
    from kg_schema_testing import open_board_connection
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    spec_ref = f"spec:{uuid.uuid4().hex[:10]}"
    decision_ref = f"{spec_ref}:decision:{uuid.uuid4().hex[:8]}"
    root_id = f"entity_seed_{uuid.uuid4().hex[:12]}"
    node_id = f"decision_seed_{uuid.uuid4().hex[:12]}"

    def _attrs(title_, content, ref, curated, layer):
        return {
            "title": title_,
            "content": content,
            "context": "ORIGINAL CONTEXT",
            "justification": "ORIGINAL JUSTIFICATION",
            "source_artifact_ref": ref,
            "created_at": "2026-06-08T00:00:00+00:00",
            "created_by_agent": "human",
            "source_confidence": 1.0,
            "relevance_score": 0.5,
            "query_hits": 0,
            "last_queried_at": None,
            "priority_boost": 0.0,
            "human_curated": curated,
            "graph_layer": layer,
            "maturity_status": (
                "working_immature" if layer == "working" else "canonical_eligible"
            ),
            "embedding": [0.0] * 384,
        }

    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            graph_scope=kconn,

            session_id=f"seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _apply_graph_node_create(
            orch, "Entity", root_id,
            _attrs("Spec root", "", spec_ref, False, "canonical"),
        )
        _apply_graph_node_create(
            orch, "Learning", node_id,
            _attrs(title, "ORIGINAL CONTENT", decision_ref, human_curated, "working"),
        )
        orch.create_edge(
            edge_type="belongs_to",
            from_id=node_id,
            to_id=root_id,
            attrs={"confidence": 1.0},
            from_type="Learning",
            to_type="Entity",
        )
    return decision_ref


def _read_decision(board_id, source_ref):
    from kg_schema_testing import open_board_connection

    with open_board_connection(board_id) as (_db, kconn):
        res = kconn.execute(
            "MATCH (n:Learning) WHERE n.source_artifact_ref = $ref "
            "RETURN n.id, coalesce(n.graph_layer, 'legacy_unknown'), "
            "n.maturity_status, n.title, n.content, n.superseded_by",
            {"ref": source_ref},
        )
        rows = []
        try:
            while res.has_next():
                row = res.get_next()
                rows.append(row)
        finally:
            try:
                res.close()
            except Exception:
                pass
        active = next((row for row in rows if not row[5]), None)
        if active is not None:
            return {
                "count": len(rows),
                "superseded_count": sum(bool(row[5]) for row in rows),
                "graph_layer": active[1],
                "maturity_status": active[2],
                "title": active[3],
                "content": active[4],
            }
    return None


async def _count_api_implements_constraint_async(
    board_id: str,
    *,
    api_title: str,
    tr_title: str,
) -> int:
    return await run_blocking_graph_io(
        lambda: _count_api_implements_constraint(
            board_id, api_title=api_title, tr_title=tr_title
        ),
        task_name="tests.spec_children.count_api_constraint",
    )


async def _seed_decision_async(board_id, *, title, human_curated):
    return await run_blocking_graph_io(
        lambda: _seed_decision(
            board_id, title=title, human_curated=human_curated
        ),
        task_name="tests.spec_children.seed_decision",
    )


async def _read_decision_async(board_id, source_ref):
    return await run_blocking_graph_io(
        lambda: _read_decision(board_id, source_ref),
        task_name="tests.spec_children.read_decision",
    )


async def _promote(board_id, agent_id, db_factory, *, source_ref, cand_id):
    await _ensure_relational_board(db_factory, board_id, agent_id)
    # The real promotion is the Layer 1 deterministic worker re-emitting a
    # done spec's child as canonical; model that with a DETERMINISTIC candidate
    # carrying graph_layer=canonical (a cognitive add would be re-classified to
    # the consolidation's maturity instead).
    cand = NodeCandidate(
        candidate_id=cand_id,
        node_type=KGNodeType.LEARNING,
        title="UPDATED TITLE",
        content="UPDATED CONTENT",
        source_artifact_ref=source_ref,
        source_confidence=0.9,
        graph_layer="canonical",
        maturity_status="canonical_eligible",
    )
    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id,
                artifact_type="spec",
                artifact_id=f"spec-{uuid.uuid4().hex[:8]}",
                raw_content=f"promote {source_ref}",
                deterministic_candidates=[cand],
            ),
            agent_id=agent_id,
            db=db,
        )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=begin.session_id),
        agent_id=agent_id,
        db=None,
        force_reprocess=True,
    )
    async with db_factory() as db:
        return await commit_consolidation(
            CommitConsolidationRequest(session_id=begin.session_id),
            agent_id=agent_id,
            db=db,
        )


@pytest.mark.asyncio
async def test_promotion_curated_promotes_maturity_preserves_content(
    board_id, agent_id, db_factory, board_handle,
):
    # A human-curated WORKING decision: promotion must lift graph_layer to
    # canonical (maturity metadata) WITHOUT overwriting the curated content.
    source_ref = await _seed_decision_async(
        board_id, title="CURATED TITLE", human_curated=True
    )

    commit = await _promote(
        board_id, agent_id, db_factory, source_ref=source_ref, cand_id="cand_curated"
    )
    assert commit.nodes_merged == 1  # reused, not duplicated
    assert commit.nodes_added == 0

    node = await _read_decision_async(board_id, source_ref)
    assert node is not None
    assert node["count"] == 1  # no duplicate
    assert node["graph_layer"] == "canonical"  # maturity METADATA promoted
    assert node["maturity_status"] == "canonical_eligible"
    assert node["title"] == "CURATED TITLE"  # protected content preserved
    assert node["content"] == "ORIGINAL CONTENT"


@pytest.mark.asyncio
async def test_promotion_non_curated_promotes_and_updates_content(
    board_id, agent_id, db_factory, board_handle,
):
    # A title change is identity-bearing under MKG-D: promotion creates a
    # canonical successor and preserves the previous working state as history.
    source_ref = await _seed_decision_async(
        board_id, title="OLD TITLE", human_curated=False
    )

    commit = await _promote(
        board_id, agent_id, db_factory, source_ref=source_ref, cand_id="cand_agent"
    )
    assert commit.nodes_superseded == 1
    assert commit.nodes_merged == 0
    assert commit.nodes_added == 0

    node = await _read_decision_async(board_id, source_ref)
    assert node is not None
    assert node["count"] == 2
    assert node["superseded_count"] == 1
    assert node["graph_layer"] == "canonical"
    assert node["title"] == "UPDATED TITLE"  # agent-managed content updated
    assert node["content"] == "UPDATED CONTENT"
