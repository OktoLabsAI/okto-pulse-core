"""S-KG-02 — Learning-centric canonical classification (read/diagnostic only).

The ``canonical_partition_integrity`` read model evolves from bug-centric to
Learning-centric: each canonical Learning is classified as REUSABLE canonical
knowledge (``canonical_learning_resolved``) when it has an auditable resolved
source AND a canonical association valid under the S-KG-01 taxonomy
(``validates`` -> canonical Bug when bug-derived, otherwise ``relates_to`` -> one
of the seven taxonomy endpoints). The ``Learning -> validates -> canonical Bug``
path is preserved (no regression). READ/DIAGNOSTIC ONLY: no new edge name, no
mutation, the connectivity guard is never bypassed — the evaluator CONSUMES
``LEARNING_RELATES_TO_TARGETS`` (S-KG-01), it never redefines it.

Scenarios (TS-KG02-01..08):
  01 Learning sem source                         -> missing_source
  02 source não-resolvido                         -> unresolved_source (REST + MCP)
  03 validates canonical Bug                      -> canonical_learning_resolved (no regression)
  04 validates working Bug                        -> NÃO canoniza
  05 relates_to each of the 7 canonical endpoints -> canonical_learning_resolved
  06 source resolvido sem endpoint válido         -> weak_provenance / provenance_only
  07 edge fora da taxonomia                        -> invalid_orphan_learning (fail-closed)
  08 REST e MCP consistentes                       -> status/classification equivalentes

Reproduce:
  uv run pytest -q tests/test_kg_s_kg_02_canonical_learning.py
"""

from __future__ import annotations

import contextlib
import json
import uuid
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.canonical_partition_integrity import (
    CLASSIFICATION_CANONICAL_LEARNING_RESOLVED,
    CLASSIFICATION_INVALID_ORPHAN_LEARNING,
    CLASSIFICATION_MISSING_SOURCE,
    CLASSIFICATION_UNRESOLVED_SOURCE,
    CLASSIFICATION_WEAK_PROVENANCE,
    STATUS_MIXED_DEFERRED,
    STATUS_PROVENANCE_ONLY,
    classify_canonical_learning,
    get_canonical_partition_integrity_detail,
    list_canonical_partition_integrity,
)
from okto_pulse.core.kg.cognitive_policy import LEARNING_RELATES_TO_TARGETS
from okto_pulse.core.kg.primitives import _apply_kuzu_node_create_with_timestamp
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_WORKING,
    MATURITY_CANONICAL_ELIGIBLE,
    MATURITY_WORKING_IMMATURE,
)
from okto_pulse.core.models.db import Board

USER_ID = "user-s-kg-02"


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    """Isolate the cognitive store the read model overlays (store/debt sources)."""
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


async def _setup_board(db_factory) -> str:
    from okto_pulse.core.kg.schema import bootstrap_board_graph

    board_id = f"skg02-{uuid.uuid4().hex[:12]}"
    bootstrap_board_graph(board_id)
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="s-kg-02", owner_id=USER_ID))
            await db.commit()
    return board_id


def _node_attrs(source_ref, graph_layer, maturity):
    return {
        "title": "skg02 seed", "content": "", "context": "", "justification": "",
        "source_artifact_ref": source_ref, "created_at": "2026-06-08T00:00:00+00:00",
        "created_by_agent": "test", "source_confidence": 1.0, "relevance_score": 0.5,
        "query_hits": 0, "last_queried_at": None, "priority_boost": 0.0,
        "human_curated": False, "embedding": [0.0] * 384,
        "graph_layer": graph_layer, "maturity_status": maturity,
    }


def _seed_learning(
    board_id, *, source_ref, canonical_bugs=0, working_bugs=0, relates_to=(),
) -> str:
    """Seed ONE canonical Learning with the requested canonical associations.

    ``relates_to`` is an iterable of ``(endpoint_node_type, endpoint_layer)`` —
    each materializes an endpoint node of that type/layer + a ``relates_to`` edge
    (existing edge name, never a new one)."""
    from okto_pulse.core.kg.schema import open_board_connection
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    learning_id = f"skg02l_{uuid.uuid4().hex[:12]}"
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn, sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}", board_id=board_id,
        )
        _apply_kuzu_node_create_with_timestamp(
            orch, "Learning", learning_id,
            _node_attrs(source_ref, GRAPH_LAYER_CANONICAL, MATURITY_CANONICAL_ELIGIBLE),
        )
        for _ in range(canonical_bugs):
            bug_id = f"skg02cb_{uuid.uuid4().hex[:10]}"
            _apply_kuzu_node_create_with_timestamp(
                orch, "Bug", bug_id,
                _node_attrs(f"bug:{bug_id}", GRAPH_LAYER_CANONICAL, MATURITY_CANONICAL_ELIGIBLE),
            )
            orch.create_edge(edge_type="validates", from_id=learning_id, to_id=bug_id,
                             attrs={"confidence": 1.0}, from_type="Learning", to_type="Bug")
        for _ in range(working_bugs):
            bug_id = f"skg02wb_{uuid.uuid4().hex[:10]}"
            _apply_kuzu_node_create_with_timestamp(
                orch, "Bug", bug_id,
                _node_attrs(f"bug:{bug_id}", GRAPH_LAYER_WORKING, MATURITY_WORKING_IMMATURE),
            )
            orch.create_edge(edge_type="validates", from_id=learning_id, to_id=bug_id,
                             attrs={"confidence": 0.9}, from_type="Learning", to_type="Bug")
        for endpoint_type, layer in relates_to:
            maturity = (
                MATURITY_CANONICAL_ELIGIBLE if layer == GRAPH_LAYER_CANONICAL
                else MATURITY_WORKING_IMMATURE
            )
            endpoint_id = f"skg02ep_{uuid.uuid4().hex[:10]}"
            _apply_kuzu_node_create_with_timestamp(
                orch, endpoint_type, endpoint_id,
                _node_attrs(f"{endpoint_type.lower()}:{endpoint_id}", layer, maturity),
            )
            orch.create_edge(edge_type="relates_to", from_id=learning_id, to_id=endpoint_id,
                             attrs={"confidence": 1.0}, from_type="Learning", to_type=endpoint_type)
    return learning_id


# --- MCP production path (mcp.get_tool(name).fn) ---------------------------


def _mcp(monkeypatch, db_factory):
    import okto_pulse.core.mcp.server as mcp_server

    async def _fake_ctx(_board_id):
        return SimpleNamespace(agent_id="mcp-agent", permissions=["*"])

    @contextlib.asynccontextmanager
    async def _fake_db():
        async with db_factory() as db:
            yield db

    monkeypatch.setattr(mcp_server, "_get_agent_ctx", _fake_ctx)
    monkeypatch.setattr(mcp_server, "get_db_for_mcp", _fake_db)
    # MCP-FU5: the migrated partition-integrity tool resolves its session through
    # get_unit_of_work_factory_for_mcp() over _mcp_session_factory (not the raw
    # get_db_for_mcp patched above); point that at the test factory too.
    monkeypatch.setattr(mcp_server, "_mcp_session_factory", db_factory)
    return mcp_server


async def _call(mcp_server, name, **kwargs):
    tool = await mcp_server.mcp.get_tool(name)
    return json.loads(await tool.fn(**kwargs))


async def _list_items_by_node(db_factory, board_id):
    async with db_factory() as db:
        result = await list_canonical_partition_integrity(db, board_id=board_id)
    return result, {i["node_id"]: i for i in result["items"]}


# ===========================================================================
# Pure classifier — the read model's classification authority (consumes S-KG-01)
# ===========================================================================


def test_classifier_consumes_skg01_taxonomy_not_redefined():
    # The classifier must canonize for EXACTLY the seven S-KG-01 endpoints and no
    # others — proving it consumes LEARNING_RELATES_TO_TARGETS rather than holding
    # its own copy.
    for endpoint_type in LEARNING_RELATES_TO_TARGETS:
        assert classify_canonical_learning(
            source_ref="spec:spec-1:learning:0", is_bug_derived=False,
            relates_to_endpoints=((endpoint_type, GRAPH_LAYER_CANONICAL),),
        ) == CLASSIFICATION_CANONICAL_LEARNING_RESOLVED, endpoint_type


def test_classifier_missing_then_unresolved_source():
    assert classify_canonical_learning(
        source_ref="", is_bug_derived=False,
    ) == CLASSIFICATION_MISSING_SOURCE
    assert classify_canonical_learning(
        source_ref="   ", is_bug_derived=False,
    ) == CLASSIFICATION_MISSING_SOURCE
    assert classify_canonical_learning(
        source_ref="mystery:xyz", is_bug_derived=False,
    ) == CLASSIFICATION_UNRESOLVED_SOURCE
    assert classify_canonical_learning(
        source_ref="no-colon-ref", is_bug_derived=False,
    ) == CLASSIFICATION_UNRESOLVED_SOURCE


def test_classifier_bug_derived_paths():
    # >=1 canonical Bug canonizes (even mixed with working); working-only never.
    assert classify_canonical_learning(
        source_ref="card:bug:abc:learning:0", is_bug_derived=True,
        canonical_bug_count=1, working_bug_count=2,
    ) == CLASSIFICATION_CANONICAL_LEARNING_RESOLVED
    assert classify_canonical_learning(
        source_ref="card:bug:abc:learning:0", is_bug_derived=True,
        canonical_bug_count=0, working_bug_count=3,
    ) == CLASSIFICATION_WEAK_PROVENANCE


def test_classifier_non_bug_layer_and_no_edge_are_weak():
    # Right taxonomy type but WORKING layer is fail-closed (not canonical).
    assert classify_canonical_learning(
        source_ref="spec:spec-1:learning:0", is_bug_derived=False,
        relates_to_endpoints=(("Decision", GRAPH_LAYER_WORKING),),
    ) == CLASSIFICATION_WEAK_PROVENANCE
    # No association edge at all -> weak provenance (the refined provenance-only).
    assert classify_canonical_learning(
        source_ref="spec:spec-1:learning:0", is_bug_derived=False,
    ) == CLASSIFICATION_WEAK_PROVENANCE


# ===========================================================================
# TS-KG02-07 — edge OUTSIDE the taxonomy is fail-closed (invalid_orphan_learning)
# ===========================================================================


def test_ts_kg02_07_off_taxonomy_endpoint_is_invalid_orphan_fail_closed():
    # The board graph cannot materialize a relates_to from a Learning to a
    # non-taxonomy type (the Kuzu rel table only declares the seven endpoints), so
    # the fail-closed branch is exercised at the classification authority itself.
    for off_type in ("Alternative", "Assumption", "Bug", "Learning"):
        assert classify_canonical_learning(
            source_ref="spec:spec-1:learning:0", is_bug_derived=False,
            relates_to_endpoints=((off_type, GRAPH_LAYER_CANONICAL),),
        ) == CLASSIFICATION_INVALID_ORPHAN_LEARNING, off_type
    # A valid canonical taxonomy endpoint alongside an off-taxonomy one still
    # canonizes — off-taxonomy never MASKS a genuine canonical association.
    assert classify_canonical_learning(
        source_ref="spec:spec-1:learning:0", is_bug_derived=False,
        relates_to_endpoints=(
            ("Alternative", GRAPH_LAYER_CANONICAL),
            ("Decision", GRAPH_LAYER_CANONICAL),
        ),
    ) == CLASSIFICATION_CANONICAL_LEARNING_RESOLVED


# ===========================================================================
# TS-KG02-01 — Learning with no source -> missing_source
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_kg02_01_missing_source(db_factory):
    board_id = await _setup_board(db_factory)
    node_id = _seed_learning(board_id, source_ref="")

    async with db_factory() as db:
        detail = await get_canonical_partition_integrity_detail(
            db, board_id=board_id, node_id=node_id,
        )
    assert detail["classification"] == CLASSIFICATION_MISSING_SOURCE

    _result, by_node = await _list_items_by_node(db_factory, board_id)
    assert by_node[node_id]["classification"] == CLASSIFICATION_MISSING_SOURCE


# ===========================================================================
# TS-KG02-02 — unresolved source -> unresolved_source (REST detail + MCP list)
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_kg02_02_unresolved_source_rest_detail_and_mcp_list(
    db_factory, monkeypatch,
):
    board_id = await _setup_board(db_factory)
    node_id = _seed_learning(board_id, source_ref="mystery:xyz")

    # REST detail (the core fn the REST router calls).
    async with db_factory() as db:
        detail = await get_canonical_partition_integrity_detail(
            db, board_id=board_id, node_id=node_id,
        )
    assert detail["classification"] == CLASSIFICATION_UNRESOLVED_SOURCE

    # MCP list (production path: mcp.get_tool(name).fn).
    mcp = _mcp(monkeypatch, db_factory)
    payload = await _call(
        mcp, "okto_pulse_kg_canonical_partition_integrity_list", board_id=board_id,
    )
    item = next(i for i in payload["items"] if i["node_id"] == node_id)
    assert item["classification"] == CLASSIFICATION_UNRESOLVED_SOURCE


# ===========================================================================
# TS-KG02-03 — validates canonical Bug -> canonical_learning_resolved (no regression)
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_kg02_03_validates_canonical_bug_resolved_no_regression(db_factory):
    board_id = await _setup_board(db_factory)
    node_id = _seed_learning(
        board_id, source_ref=f"card:bug:{uuid.uuid4()}:learning:0", canonical_bugs=1,
    )

    async with db_factory() as db:
        detail = await get_canonical_partition_integrity_detail(
            db, board_id=board_id, node_id=node_id,
        )
    assert detail["classification"] == CLASSIFICATION_CANONICAL_LEARNING_RESOLVED
    assert len(detail["canonical_edges"]) == 1
    assert detail["canonical_edges"][0]["to_graph_layer"] == GRAPH_LAYER_CANONICAL

    # No regression: a satisfied canonical-only bug Learning is healthy — it is
    # NOT surfaced as a partition-integrity problem (preserves prior behavior).
    _result, by_node = await _list_items_by_node(db_factory, board_id)
    assert node_id not in by_node


@pytest.mark.asyncio
async def test_ts_kg02_03_mixed_evidence_still_canonical_and_listed(db_factory):
    # Mixed (>=1 canonical + working) is canonical knowledge AND still an advisory
    # mixed_evidence_deferred signal (the working edges are deferred, never counted).
    board_id = await _setup_board(db_factory)
    node_id = _seed_learning(
        board_id, source_ref=f"card:bug:{uuid.uuid4()}:learning:0",
        canonical_bugs=1, working_bugs=1,
    )

    async with db_factory() as db:
        detail = await get_canonical_partition_integrity_detail(
            db, board_id=board_id, node_id=node_id,
        )
    assert detail["status"] == STATUS_MIXED_DEFERRED            # status preserved
    assert detail["classification"] == CLASSIFICATION_CANONICAL_LEARNING_RESOLVED

    _result, by_node = await _list_items_by_node(db_factory, board_id)
    assert by_node[node_id]["status"] == STATUS_MIXED_DEFERRED
    assert by_node[node_id]["classification"] == CLASSIFICATION_CANONICAL_LEARNING_RESOLVED


# ===========================================================================
# TS-KG02-04 — validates a WORKING Bug never canonizes
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_kg02_04_validates_working_bug_does_not_canonize(db_factory):
    board_id = await _setup_board(db_factory)
    node_id = _seed_learning(
        board_id, source_ref=f"card:bug:{uuid.uuid4()}:learning:0", working_bugs=1,
    )

    async with db_factory() as db:
        detail = await get_canonical_partition_integrity_detail(
            db, board_id=board_id, node_id=node_id,
        )
    assert detail["classification"] != CLASSIFICATION_CANONICAL_LEARNING_RESOLVED
    assert detail["classification"] == CLASSIFICATION_WEAK_PROVENANCE
    assert len(detail["canonical_edges"]) == 0
    assert len(detail["working_edges"]) == 1
    assert detail["working_edges"][0]["to_graph_layer"] == GRAPH_LAYER_WORKING


# ===========================================================================
# TS-KG02-05 — relates_to EACH of the 7 canonical endpoints -> resolved
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_kg02_05_relates_to_each_canonical_endpoint_resolved(db_factory):
    board_id = await _setup_board(db_factory)
    node_by_type = {
        endpoint_type: _seed_learning(
            board_id,
            source_ref=f"spec:spec-{endpoint_type.lower()}:learning:0",
            relates_to=((endpoint_type, GRAPH_LAYER_CANONICAL),),
        )
        for endpoint_type in LEARNING_RELATES_TO_TARGETS
    }

    async with db_factory() as db:
        for endpoint_type, node_id in node_by_type.items():
            detail = await get_canonical_partition_integrity_detail(
                db, board_id=board_id, node_id=node_id,
            )
            assert detail["classification"] == CLASSIFICATION_CANONICAL_LEARNING_RESOLVED, endpoint_type
            edge = detail["relates_to_edges"][0]
            assert edge["rel_type"] == "relates_to"              # existing edge name reused
            assert edge["to_node_type"] == endpoint_type
            assert edge["canonical_taxonomy_endpoint"] is True

    # Healthy non-bug canonical Learnings drop out of the problem list (the S-KG-02
    # refinement of the prior always-provenance_only behavior).
    _result, by_node = await _list_items_by_node(db_factory, board_id)
    assert set(node_by_type.values()).isdisjoint(by_node)


# ===========================================================================
# TS-KG02-06 — resolved source, no valid endpoint -> weak_provenance / provenance_only
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_kg02_06_resolved_without_valid_endpoint_is_weak(db_factory):
    board_id = await _setup_board(db_factory)
    # Right taxonomy TYPE but WORKING layer — fail-closed (not a canonical assoc).
    node_id = _seed_learning(
        board_id, source_ref="spec:spec-weak:learning:0",
        relates_to=(("Decision", GRAPH_LAYER_WORKING),),
    )

    async with db_factory() as db:
        detail = await get_canonical_partition_integrity_detail(
            db, board_id=board_id, node_id=node_id,
        )
    assert detail["classification"] == CLASSIFICATION_WEAK_PROVENANCE
    assert detail["relates_to_edges"][0]["in_taxonomy"] is True
    assert detail["relates_to_edges"][0]["canonical_taxonomy_endpoint"] is False

    _result, by_node = await _list_items_by_node(db_factory, board_id)
    item = by_node[node_id]
    assert item["classification"] == CLASSIFICATION_WEAK_PROVENANCE
    # Compat: the existing problem-signal status vocabulary is preserved.
    assert item["status"] == STATUS_PROVENANCE_ONLY


# ===========================================================================
# TS-KG02-08 — REST and MCP surfaces are consistent (status/classification/reason)
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_kg02_08_rest_and_mcp_consistent(db_factory, monkeypatch):
    board_id = await _setup_board(db_factory)
    unresolved_node = _seed_learning(board_id, source_ref="mystery:xyz")
    weak_node = _seed_learning(board_id, source_ref="spec:spec-weak:learning:0")

    # MCP list (production path).
    mcp = _mcp(monkeypatch, db_factory)
    mcp_payload = await _call(
        mcp, "okto_pulse_kg_canonical_partition_integrity_list", board_id=board_id,
    )
    mcp_by_node = {i["node_id"]: i for i in mcp_payload["items"]}

    # REST list + per-node detail (the core fns the REST router calls).
    async with db_factory() as db:
        rest_list = await list_canonical_partition_integrity(db, board_id=board_id)
        details = {
            n: await get_canonical_partition_integrity_detail(
                db, board_id=board_id, node_id=n,
            )
            for n in (unresolved_node, weak_node)
        }
    rest_by_node = {i["node_id"]: i for i in rest_list["items"]}

    for node_id, expected in (
        (unresolved_node, CLASSIFICATION_UNRESOLVED_SOURCE),
        (weak_node, CLASSIFICATION_WEAK_PROVENANCE),
    ):
        assert mcp_by_node[node_id]["classification"] == expected
        assert rest_by_node[node_id]["classification"] == expected
        assert details[node_id]["classification"] == expected
        # status + reason_code are semantically equivalent across both surfaces.
        assert mcp_by_node[node_id]["status"] == rest_by_node[node_id]["status"]
        assert mcp_by_node[node_id]["reason_code"] == rest_by_node[node_id]["reason_code"]

    # The Learning-centric census is present and identical between MCP and REST.
    assert mcp_payload["classification_counts"] == rest_list["classification_counts"]
    assert mcp_payload["classification_counts"][CLASSIFICATION_UNRESOLVED_SOURCE] == 1
    assert mcp_payload["classification_counts"][CLASSIFICATION_WEAK_PROVENANCE] == 1
