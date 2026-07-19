from __future__ import annotations

import json
import time
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.interfaces.reflective_query import ReflectiveRetrievalBatch
from okto_pulse.core.kg.retrieve_critic import reset_critic_cache, run_reflective_query
from okto_pulse.core.kg.retrieve_critic import orchestrator as reflective_orchestrator
from okto_pulse.core.kg.retrieve_critic.interfaces import (
    Adequacy,
    CriticAction,
    CriticDecision,
)


class _Retrieval:
    identity = "fake-retrieval"
    version = "1"

    def __init__(self, fn):
        self._fn = fn
        self.requests = []

    def retrieve(self, request):
        self.requests.append(request)
        return self._fn(request)


class _Critic:
    identity = "fake-critic"
    version = "1"

    def __init__(self, fn):
        self._fn = fn
        self.calls = 0

    def evaluate(self, request):
        self.calls += 1
        return self._fn(request)


def _run(retrieval, critic, **overrides):
    params = {
        "board_id": "board-1",
        "query": "decision context",
        "limit": 20,
        "min_confidence": 0.5,
        "graph_layer": "canonical",
        "max_iterations": 3,
        "deadline_ms": 1000,
        "budget_units": 10,
        "acl_scope_hash": "acl-a",
        "retrieval": retrieval,
        "critic": critic,
    }
    params.update(overrides)
    return run_reflective_query(**params)


def test_real_loop_expands_then_accepts_with_complete_trace():
    def retrieve(request):
        rows = (
            {"node_id": "a", "node_type": "Decision", "similarity": 0.2},
        )
        if request.action == CriticAction.EXPAND_HOPS:
            rows += (
                {"node_id": "b", "node_type": "Entity", "similarity": 0.7},
            )
        return ReflectiveRetrievalBatch(rows, "g1", "expand", 1)

    def evaluate(request):
        if request.iteration == 0:
            return CriticDecision(
                Adequacy.PARTIAL,
                "need_expand",
                CriticAction.EXPAND_HOPS,
                confidence=0.2,
            )
        return CriticDecision(
            Adequacy.SUFFICIENT,
            "evidence_complete",
            CriticAction.ACCEPT,
            confidence=0.8,
        )

    result = _run(_Retrieval(retrieve), _Critic(evaluate))

    assert result["accepted"] is True
    assert result["terminal_reason"] == "accepted"
    assert [item["action"] for item in result["iterations"]] == [
        "expand_hops",
        "accept",
    ]
    assert [row["node_id"] for row in result["nodes"]] == ["a", "b"]


def test_malformed_critic_output_never_accepts():
    retrieval = _Retrieval(
        lambda _request: ReflectiveRetrievalBatch(
            ({"node_id": "a", "node_type": "Decision", "similarity": 1.0},),
            "g1",
            "vector",
        )
    )
    result = _run(retrieval, _Critic(lambda _request: {"adequacy": "sufficient"}))

    assert result["accepted"] is False
    assert result["terminal_reason"] == "critic_malformed"


def test_no_progress_budget_deadline_and_retrieval_error_are_distinct():
    row = ({"node_id": "a", "node_type": "Decision", "similarity": 0.1},)
    expand = _Critic(
        lambda _request: CriticDecision(
            Adequacy.PARTIAL, "need_expand", CriticAction.EXPAND_HOPS
        )
    )
    no_progress = _run(
        _Retrieval(lambda _request: ReflectiveRetrievalBatch(row, "g1", "same")),
        expand,
    )
    assert no_progress["terminal_reason"] == "no_progress"

    budget = _run(
        _Retrieval(lambda _request: ReflectiveRetrievalBatch(row, "g1", "costly", 2)),
        expand,
        budget_units=1,
    )
    assert budget["terminal_reason"] == "budget_exhausted"

    def slow(_request):
        time.sleep(0.06)
        return ReflectiveRetrievalBatch(row, "g1", "slow")

    deadline = _run(_Retrieval(slow), expand, deadline_ms=50)
    assert deadline["terminal_reason"] == "deadline_exhausted"

    def broken(_request):
        raise OSError("graph offline")

    failed = _run(_Retrieval(broken), expand)
    assert failed["terminal_reason"] == "retrieval_error"


def test_zero_cost_retrieval_batch_is_malformed_before_critic() -> None:
    retrieval = _Retrieval(
        lambda _request: ReflectiveRetrievalBatch(
            (),
            "g1",
            "invalid-zero-cost",
            cost_units=0,
        )
    )
    critic = _Critic(lambda _request: pytest.fail("critic must not run"))

    result = _run(retrieval, critic)

    assert result["accepted"] is False
    assert result["terminal_reason"] == "retrieval_malformed"
    assert critic.calls == 0


def test_critic_cache_isolated_by_acl_scope_and_reports_hits(monkeypatch):
    reset_critic_cache()
    monkeypatch.setattr(reflective_orchestrator.time, "monotonic", lambda: 42.0)
    retrieval = _Retrieval(
        lambda _request: ReflectiveRetrievalBatch(
            ({"node_id": "a", "node_type": "Decision", "similarity": 0.9},),
            "graph-v7",
            "vector",
        )
    )
    critic = _Critic(
        lambda _request: CriticDecision(
            Adequacy.SUFFICIENT, "enough", CriticAction.ACCEPT
        )
    )

    first = _run(retrieval, critic, acl_scope_hash="acl-a")
    second = _run(retrieval, critic, acl_scope_hash="acl-a")
    third = _run(retrieval, critic, acl_scope_hash="acl-b")

    assert first["iterations"][0]["critic_cache_hit"] is False
    assert second["iterations"][0]["critic_cache_hit"] is True
    assert third["iterations"][0]["critic_cache_hit"] is False
    assert critic.calls == 2


def test_critic_cache_misses_when_semantic_row_content_or_rank_changes():
    reset_critic_cache()
    batches = iter(
        (
            ReflectiveRetrievalBatch(
                (
                    {
                        "node_id": "a",
                        "node_type": "Decision",
                        "similarity": 0.9,
                        "title": "Old decision",
                        "provenance": {"content_hash": "old"},
                    },
                    {
                        "node_id": "b",
                        "node_type": "Spec",
                        "similarity": 0.8,
                        "title": "Supporting spec",
                    },
                ),
                "schema-v1",
                "vector",
            ),
            ReflectiveRetrievalBatch(
                (
                    {
                        "node_id": "b",
                        "node_type": "Spec",
                        "similarity": 0.8,
                        "title": "Supporting spec",
                    },
                    {
                        "node_id": "a",
                        "node_type": "Decision",
                        "similarity": 0.9,
                        "title": "New decision",
                        "provenance": {"content_hash": "new"},
                    },
                ),
                "schema-v1",
                "vector",
            ),
        )
    )
    retrieval = _Retrieval(lambda _request: next(batches))
    critic = _Critic(
        lambda _request: CriticDecision(
            Adequacy.SUFFICIENT, "enough", CriticAction.ACCEPT
        )
    )

    first = _run(retrieval, critic)
    second = _run(retrieval, critic)

    assert first["iterations"][0]["critic_cache_hit"] is False
    assert second["iterations"][0]["critic_cache_hit"] is False
    assert critic.calls == 2


def test_critic_cache_key_includes_the_complete_execution_state():
    critic = _Critic(lambda _request: pytest.fail("key construction only"))
    base = {
        "board_id": "board-1",
        "query_hash": "query-hash",
        "acl_scope_hash": "acl-a",
        "limit": 20,
        "min_confidence": 0.5,
        "graph_layer": "canonical",
        "graph_version": "g1",
        "rows_digest": "current-digest",
        "iteration": 1,
        "previous_action": CriticAction.RETRY_WITH_REWRITE,
        "previous_rows_digest": "previous-digest",
        "remaining_budget_units": 8,
        "elapsed_ms": 12.5,
        "critic": critic,
    }

    expected = reflective_orchestrator._critic_cache_key(**base)
    variants = (
        {"iteration": 2},
        {"previous_action": CriticAction.FALLBACK_SEMANTIC},
        {"previous_rows_digest": "different-previous-digest"},
        {"remaining_budget_units": 7},
        {"elapsed_ms": 13.5},
    )

    for override in variants:
        assert reflective_orchestrator._critic_cache_key(
            **(base | override)
        ) != expected


def test_unchanged_rows_can_progress_rewrite_fallback_then_reject():
    reset_critic_cache()
    retrieval = _Retrieval(
        lambda _request: ReflectiveRetrievalBatch((), "g1", "empty")
    )

    def evaluate(request):
        if request.iteration == 0:
            return CriticDecision(
                Adequacy.IRRELEVANT,
                "try_rewrite",
                CriticAction.RETRY_WITH_REWRITE,
                rewritten_query="decision",
            )
        if request.previous_action == CriticAction.RETRY_WITH_REWRITE:
            return CriticDecision(
                Adequacy.IRRELEVANT,
                "try_semantic",
                CriticAction.FALLBACK_SEMANTIC,
            )
        return CriticDecision(
            Adequacy.IRRELEVANT,
            "no_evidence",
            CriticAction.REJECT,
        )

    result = _run(retrieval, _Critic(evaluate))

    assert result["terminal_reason"] == "rejected"
    assert [item["action"] for item in result["iterations"]] == [
        "retry_with_rewrite",
        "fallback_semantic",
        "reject",
    ]
    assert [request.action for request in retrieval.requests] == [
        None,
        CriticAction.RETRY_WITH_REWRITE,
        CriticAction.FALLBACK_SEMANTIC,
    ]


def test_unchanged_rewrite_evidence_can_fallback_then_accept_new_rows():
    reset_critic_cache()

    def retrieve(request):
        if request.action == CriticAction.FALLBACK_SEMANTIC:
            return ReflectiveRetrievalBatch(
                ({"node_id": "semantic", "similarity": 0.9},),
                "g1",
                "semantic_fallback",
            )
        return ReflectiveRetrievalBatch((), "g1", "empty")

    def evaluate(request):
        if request.rows:
            return CriticDecision(
                Adequacy.SUFFICIENT,
                "semantic_evidence",
                CriticAction.ACCEPT,
            )
        if request.iteration == 0:
            return CriticDecision(
                Adequacy.IRRELEVANT,
                "try_rewrite",
                CriticAction.RETRY_WITH_REWRITE,
                rewritten_query="decision",
            )
        return CriticDecision(
            Adequacy.IRRELEVANT,
            "try_semantic",
            CriticAction.FALLBACK_SEMANTIC,
        )

    result = _run(_Retrieval(retrieve), _Critic(evaluate))

    assert result["accepted"] is True
    assert result["terminal_reason"] == "accepted"
    assert [item["action"] for item in result["iterations"]] == [
        "retry_with_rewrite",
        "fallback_semantic",
        "accept",
    ]


class _MCP:
    def __init__(self):
        self.tools = {}

    def tool(self):
        def decorate(fn):
            self.tools[fn.__name__] = fn
            return fn

        return decorate


class _Auth:
    def __init__(self, boards):
        self._boards = boards

    async def get_agent_id(self):
        return "agent-1"

    async def get_accessible_boards(self):
        return self._boards


@pytest.mark.asyncio
async def test_mcp_reflective_tool_enforces_acl_then_runs_real_loop(monkeypatch):
    import okto_pulse.core.kg.interfaces as interfaces
    import okto_pulse.core.kg.kg_service as kg_service
    import okto_pulse.core.mcp.kg_power_tools as power_tools

    retrieval = _Retrieval(
        lambda _request: ReflectiveRetrievalBatch(
            ({"node_id": "a", "node_type": "Decision", "similarity": 0.9},),
            "g1",
            "vector",
        )
    )
    critic = _Critic(
        lambda _request: CriticDecision(
            Adequacy.SUFFICIENT, "enough", CriticAction.ACCEPT
        )
    )
    registry = SimpleNamespace(
        auth_context_factory=lambda: _Auth(["board-1"]),
        reflective_telemetry=None,
        require_reflective_retrieval=lambda: retrieval,
        require_reflective_critic=lambda: critic,
    )

    class _Service:
        @staticmethod
        def check_board_access(boards, board_id):
            if board_id not in boards:
                raise kg_service.KGToolError("permission_denied", "denied")

    monkeypatch.setattr(interfaces, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(kg_service, "get_kg_service", lambda: _Service())
    monkeypatch.setattr(power_tools, "check_rate_limit", lambda _agent_id: None)
    mcp = _MCP()

    async def get_agent():
        return SimpleNamespace(id="agent-1")

    power_tools.register_kg_power_tools(mcp, get_agent=get_agent)
    raw = await mcp.tools["okto_pulse_kg_query_reflective"](
        "board-1", "decision"
    )
    result = json.loads(raw)

    assert result["accepted"] is True
    assert result["terminal_reason"] == "accepted"
    assert result["applied_graph_layer"] == "canonical"
    assert len(retrieval.requests) == 1


@pytest.mark.asyncio
async def test_mcp_reflective_tool_denies_board_before_retrieval(monkeypatch):
    import okto_pulse.core.kg.interfaces as interfaces
    import okto_pulse.core.kg.kg_service as kg_service
    import okto_pulse.core.mcp.kg_power_tools as power_tools

    retrieval = _Retrieval(
        lambda _request: pytest.fail("retrieval must not run before ACL")
    )
    registry = SimpleNamespace(
        auth_context_factory=lambda: _Auth(["another-board"]),
        reflective_telemetry=None,
        require_reflective_retrieval=lambda: retrieval,
        require_reflective_critic=lambda: _Critic(lambda _request: None),
    )

    class _Service:
        @staticmethod
        def check_board_access(boards, board_id):
            if board_id not in boards:
                raise kg_service.KGToolError("permission_denied", "denied")

    monkeypatch.setattr(interfaces, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(kg_service, "get_kg_service", lambda: _Service())
    monkeypatch.setattr(power_tools, "check_rate_limit", lambda _agent_id: None)
    mcp = _MCP()

    async def get_agent():
        return SimpleNamespace(id="agent-1")

    power_tools.register_kg_power_tools(mcp, get_agent=get_agent)
    raw = await mcp.tools["okto_pulse_kg_query_reflective"](
        "board-1", "decision"
    )

    assert json.loads(raw)["error"]["code"] == "permission_denied"
    assert retrieval.requests == []
