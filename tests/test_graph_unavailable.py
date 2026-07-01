"""F4 (spec deb61383) — uniform structured ``graph_unavailable`` across KG read tools.

Integration + unit tests for the shared open-or-classify helper
(``core/kg/graph_availability.py``) and the vector + Cypher read-path wiring.

TR6 test infra: seed a board via ``bootstrap_board_graph`` and simulate the
fail-closed open failure by monkeypatching the community graph runtime
``_open_kuzu_db_path_cached``
to raise — the REAL ``_raise_existing_graph_open_failed(operation="bootstrap_probe")``
guard then fires (we never corrupt on-disk files). The MCP tools are driven
through a tiny FastMCP double; ACL is satisfied by stubbing ``_get_user_boards``.

Scenario -> test-card map:
  ts_69283d46, ts_b4d2368a  -> card 7d883f70 (AC1/AC2: vector + Cypher open-failure)
  ts_cd67cf2b, ts_ddffb935  -> card 5e4f1286 (AC3/AC5: uniform code+graph_state, hard-reject)
  ts_92d7c9e5, ts_753b6aba  -> card 4539b882 (AC4/AC6: empty-vs-failure distinction)
  ts_e9f925d9, ts_f500022f  -> card 43247872 (AC7/AC8: registered code + guard untouched)
  ts_d4c8031f               -> card ef476e88 (AC9: full Cypher read family)
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import shutil
import uuid

import pytest

import okto_pulse.core.mcp.kg_query_tools as kqt
import okto_pulse.community.adapters.kg_runtime as kg_runtime
from okto_pulse.core.kg import graph_availability as ga
from okto_pulse.core.kg.backpressure import _RISK_STATE_HARD_REJECT
from okto_pulse.core.kg.cache import clear_cache
from okto_pulse.core.kg.kg_service import (
    KGToolError,
    _get_graph_store,
    reset_kg_service_for_tests,
)
from okto_pulse.core.kg.schema import (
    board_kuzu_path,
    bootstrap_board_graph,
    close_all_connections,
    reset_bootstrap_cache_for_tests,
)
from okto_pulse.core.kg.interfaces import get_kg_registry
from kg_registry_testing import configure_real_graph_test_kg_registry


# --------------------------------------------------------------------------- #
# Scaffolding
# --------------------------------------------------------------------------- #


class _MCPRegistryDouble:
    """Minimal FastMCP double that captures ``@mcp.tool()`` closures by name."""

    def __init__(self) -> None:
        self.tools: dict = {}

    def tool(self):
        def _decorator(fn):
            self.tools[fn.__name__] = fn
            return fn

        return _decorator


def _fresh_board_id() -> str:
    return f"graph-unavail-{uuid.uuid4().hex[:12]}"


def _purge(board_id: str) -> None:
    path = board_kuzu_path(board_id)
    close_all_connections(board_id)
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
    elif path.is_file():
        path.unlink(missing_ok=True)
        for sibling in path.parent.glob(path.name + ".*"):
            sibling.unlink(missing_ok=True)
    reset_bootstrap_cache_for_tests()
    reset_kg_service_for_tests()
    clear_cache()


def _register_query_tools(monkeypatch, board_id: str) -> dict:
    """Register the query tools on a double, granting the test agent access to
    ``board_id`` only (stub ``_get_user_boards`` so ``check_board_access`` passes
    without touching the SQLite AgentService)."""

    class _Agent:
        id = "agent-graph-unavail"

    async def _stub_user_boards(get_agent=None, get_uow=None):
        return _Agent(), [board_id]

    monkeypatch.setattr(kqt, "_get_user_boards", _stub_user_boards)
    mcp = _MCPRegistryDouble()
    kqt.register_kg_query_tools(mcp, get_agent=lambda: None, get_uow=lambda: None)
    return mcp.tools


def _invoke(tool, **kwargs) -> dict:
    return json.loads(asyncio.run(tool(**kwargs)))


@pytest.fixture
def open_failure_board(monkeypatch):
    """A board whose on-disk graph EXISTS but fails to open, so the real
    ``kg_runtime._raise_existing_graph_open_failed(operation="bootstrap_probe")``
    guard fires. TR6: monkeypatch the open, never corrupt real files."""
    configure_real_graph_test_kg_registry()
    bid = _fresh_board_id()
    _purge(bid)
    path = board_kuzu_path(bid)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"partial-lbug-bytes")  # file exists -> the probe reaches the open

    def _raise_corrupt(_path):
        raise RuntimeError(
            "Runtime exception: Corrupted wal file. Read out invalid WAL record type."
        )

    monkeypatch.setattr(kg_runtime, "_open_kuzu_db_path_cached", _raise_corrupt)
    reset_bootstrap_cache_for_tests()
    reset_kg_service_for_tests()
    clear_cache()
    yield bid
    _purge(bid)


@pytest.fixture
def healthy_empty_board():
    """A real, seeded board that opens successfully but has no Decision/Learning
    rows — the genuinely-empty contract (FR8)."""
    configure_real_graph_test_kg_registry()
    bid = _fresh_board_id()
    _purge(bid)
    bootstrap_board_graph(bid)
    yield bid
    _purge(bid)


# --------------------------------------------------------------------------- #
# AC7 / AC8 — card 43247872 (registered code, single predicate, guard untouched)
# --------------------------------------------------------------------------- #


def test_ts_e9f925d9_code_registered_and_single_shared_predicate():
    # graph_unavailable round-trips through the MCP error envelope, code preserved
    err = KGToolError(
        code="graph_unavailable",
        message="m",
        details={"graph_state": "recovery_needed"},
    )
    assert json.loads(kqt._err(err.code, err.message))["error"]["code"] == "graph_unavailable"

    # the read-tool handlers forward graph_state into the error object (FR7)
    forwarded = json.loads(kqt._err_from(err))
    assert forwarded["error"]["code"] == "graph_unavailable"
    assert forwarded["error"]["graph_state"] == "recovery_needed"

    # TR2: the helper REUSES backpressure._RISK_STATE_HARD_REJECT — the SAME
    # object, not a parallel literal set (strongest form: identity check).
    assert ga._RISK_STATE_HARD_REJECT is _RISK_STATE_HARD_REJECT
    src = inspect.getsource(ga)
    assert (
        "from okto_pulse.core.kg.backpressure import _RISK_STATE_HARD_REJECT" in src
    ), "the helper must import the predicate, not redefine it"

    # derive_graph_state always yields a hard-reject member, defaulting to the
    # conservative recovery_needed for any non-member candidate (FR3)
    assert ga.derive_graph_state() in _RISK_STATE_HARD_REJECT
    assert ga.derive_graph_state("quarantined") == "quarantined"
    assert ga.derive_graph_state("at_risk") == "recovery_needed"


def test_ts_f500022f_classification_preserves_files_and_guard(open_failure_board, monkeypatch):
    bid = open_failure_board
    path = board_kuzu_path(bid)
    before = path.read_bytes()
    before_hash = hashlib.sha256(before).hexdigest()
    before_mtime = path.stat().st_mtime

    tools = _register_query_tools(monkeypatch, bid)
    payload = _invoke(tools["okto_pulse_kg_get_learning_from_bugs"], board_id=bid, area="anything")
    assert payload["error"]["code"] == "graph_unavailable"

    # classification must NOT auto-bootstrap / purge / repair the on-disk graph
    after = path.read_bytes()
    assert after == before
    assert hashlib.sha256(after).hexdigest() == before_hash
    assert path.stat().st_mtime == before_mtime

    # the fail-closed guard body is unchanged: it still raises its RuntimeError
    with pytest.raises(RuntimeError, match="refusing to auto-bootstrap"):
        kg_runtime._raise_existing_graph_open_failed(
            board_id=bid,
            path=path,
            operation="bootstrap_probe",
            exc=RuntimeError("boom"),
        )


# --------------------------------------------------------------------------- #
# AC1 / AC2 — card 7d883f70 (vector + Cypher single-tool open-failure)
# --------------------------------------------------------------------------- #


def test_ts_69283d46_vector_open_failure_is_graph_unavailable(open_failure_board, monkeypatch):
    bid = open_failure_board
    tools = _register_query_tools(monkeypatch, bid)
    payload = _invoke(tools["okto_pulse_kg_find_similar_decisions"], board_id=bid, topic="anything")
    assert payload["error"]["code"] == "graph_unavailable"
    # NOT the former silent false-empty success envelope
    assert "decisions" not in payload


def test_ts_b4d2368a_cypher_open_failure_is_graph_unavailable(open_failure_board, monkeypatch):
    bid = open_failure_board
    tools = _register_query_tools(monkeypatch, bid)
    payload = _invoke(tools["okto_pulse_kg_get_learning_from_bugs"], board_id=bid, area="anything")
    assert payload["error"]["code"] == "graph_unavailable"
    # NOT the former generic kuzu_error
    assert payload["error"]["code"] != "kuzu_error"


# --------------------------------------------------------------------------- #
# AC3 / AC5 — card 5e4f1286 (uniform code + graph_state, hard-reject membership)
# --------------------------------------------------------------------------- #


def test_ts_cd67cf2b_uniform_code_and_graph_state_across_paths(open_failure_board, monkeypatch):
    bid = open_failure_board
    tools = _register_query_tools(monkeypatch, bid)
    vec = _invoke(tools["okto_pulse_kg_find_similar_decisions"], board_id=bid, topic="anything")
    cyp = _invoke(tools["okto_pulse_kg_get_learning_from_bugs"], board_id=bid, area="anything")
    # The load-bearing F4 regression: the two formerly-divergent paths now agree.
    assert vec["error"]["code"] == cyp["error"]["code"] == "graph_unavailable"
    assert vec["error"]["graph_state"] == cyp["error"]["graph_state"]


def test_ts_ddffb935_graph_state_in_hard_reject_set(open_failure_board, monkeypatch):
    bid = open_failure_board
    tools = _register_query_tools(monkeypatch, bid)
    payload = _invoke(tools["okto_pulse_kg_get_learning_from_bugs"], board_id=bid, area="anything")
    assert payload["error"]["code"] == "graph_unavailable"
    assert "graph_state" in payload["error"]
    assert payload["error"]["graph_state"] in _RISK_STATE_HARD_REJECT


# --------------------------------------------------------------------------- #
# AC4 / AC6 — card 4539b882 (empty-vs-failure distinction — load-bearing)
# --------------------------------------------------------------------------- #


def test_ts_92d7c9e5_healthy_empty_stays_empty(healthy_empty_board, monkeypatch):
    bid = healthy_empty_board
    tools = _register_query_tools(monkeypatch, bid)
    vec = _invoke(tools["okto_pulse_kg_find_similar_decisions"], board_id=bid, topic="no-match")
    cyp = _invoke(tools["okto_pulse_kg_get_learning_from_bugs"], board_id=bid, area="no-match")
    # Open succeeds, zero rows -> the existing empty contract, NOT an error (FR8)
    assert "error" not in vec
    assert vec["decisions"] == [] and vec["count"] == 0
    assert "error" not in cyp
    assert cyp["learnings"] == [] and cyp["count"] == 0


def test_ts_753b6aba_non_open_failure_stays_narrow(healthy_empty_board, monkeypatch):
    bid = healthy_empty_board

    # CYPHER: a non-open query error (NOT the bootstrap-probe RuntimeError) must
    # stay kuzu_error, never graph_unavailable (TR3 narrowness).
    store = _get_graph_store()

    def _raise_query_error(*_a, **_k):
        raise ValueError("simulated malformed cypher query")

    monkeypatch.setattr(store, "get_learnings_for_area", _raise_query_error)

    # VECTOR: a non-open failure inside the swallow site must stay [] — a bare
    # RuntimeError WITHOUT the fail-closed guard markers.
    def _raise_non_marker(_board_id):
        raise RuntimeError("transient lock contention on board graph")

    monkeypatch.setattr(
        get_kg_registry().cypher_executor,
        "execute_read_only",
        _raise_non_marker,
    )

    tools = _register_query_tools(monkeypatch, bid)
    cyp = _invoke(tools["okto_pulse_kg_get_learning_from_bugs"], board_id=bid, area="anything")
    assert cyp["error"]["code"] == "kuzu_error"
    assert cyp["error"]["code"] != "graph_unavailable"

    vec = _invoke(tools["okto_pulse_kg_find_similar_decisions"], board_id=bid, topic="anything")
    assert "error" not in vec
    assert vec["decisions"] == [] and vec["count"] == 0


# --------------------------------------------------------------------------- #
# AC9 — card ef476e88 (full Cypher read family)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "tool_name,extra_kwargs",
    [
        ("okto_pulse_kg_list_alternatives", {"decision_id": "dec-x"}),
        ("okto_pulse_kg_find_contradictions", {}),
        ("okto_pulse_kg_get_decision_history", {"topic": "anything"}),
    ],
)
def test_ts_d4c8031f_full_cypher_family_graph_unavailable(
    open_failure_board, monkeypatch, tool_name, extra_kwargs
):
    bid = open_failure_board
    tools = _register_query_tools(monkeypatch, bid)
    payload = _invoke(tools[tool_name], board_id=bid, **extra_kwargs)
    assert payload["error"]["code"] == "graph_unavailable"
    assert payload["error"]["graph_state"] in _RISK_STATE_HARD_REJECT


# --------------------------------------------------------------------------- #
# Extra coverage (TR3 narrowness at the predicate level + OR or_0a8b78be metric)
# --------------------------------------------------------------------------- #


def test_is_graph_unavailable_error_is_narrow():
    """The classifier matches the REAL fail-closed guard message for both
    operation values, and rejects look-alikes (TR3). Guards the false-positive
    surface that matching the interpolated ``bootstrap_probe`` token would open."""
    for operation in ("bootstrap_probe", "schema_migration_open"):
        with pytest.raises(RuntimeError) as exc_info:
            kg_runtime._raise_existing_graph_open_failed(
                board_id="b",
                path=board_kuzu_path("b"),
                operation=operation,
                exc=RuntimeError("inner open error"),
            )
        assert ga.is_graph_unavailable_error(exc_info.value)

    # a benign query error whose text merely mentions the operation token is NOT
    # a graph-open failure
    assert not ga.is_graph_unavailable_error(
        RuntimeError("Binder exception: Table bootstrap_probe does not exist")
    )
    # the regular (non-fail-closed) open error is NOT classified
    assert not ga.is_graph_unavailable_error(
        RuntimeError("Failed to open LadybugDB database at /tmp/x")
    )
    # marker text on a NON-RuntimeError type is NOT classified (type gate)
    assert not ga.is_graph_unavailable_error(
        ValueError("refusing to auto-bootstrap or purge it")
    )


def test_graph_unavailable_metric_emitted(open_failure_board, monkeypatch):
    """OR or_0a8b78be: every graph_unavailable surfacing is counted via
    emit_tool_metrics(error_code="graph_unavailable") on BOTH read paths."""
    import okto_pulse.core.kg.cache as cache_module

    calls: list = []
    real_emit = cache_module.emit_tool_metrics

    def _spy(**kwargs):
        calls.append(kwargs)
        return real_emit(**kwargs)

    monkeypatch.setattr(cache_module, "emit_tool_metrics", _spy)

    bid = open_failure_board
    tools = _register_query_tools(monkeypatch, bid)
    _invoke(tools["okto_pulse_kg_get_learning_from_bugs"], board_id=bid, area="anything")
    _invoke(tools["okto_pulse_kg_find_similar_decisions"], board_id=bid, topic="anything")

    gu_tools = {
        c["tool_name"] for c in calls if c.get("error_code") == "graph_unavailable"
    }
    assert "get_learning_from_bugs" in gu_tools  # Cypher path (_cached_call) metric
    assert "find_similar_decisions" in gu_tools  # vector path metric
