"""Unit tests for kg.retrieve_critic (ideação db8e984f)."""

from __future__ import annotations

import dataclasses

import pytest

from okto_pulse.core.kg.retrieve_critic import (
    Adequacy,
    CriticAction,
    CriticDecision,
    ReflectResult,
    critic_evaluate,
    reflect,
    reset_critic_cache,
)


# ===========================================================================
# Enums + dataclasses
# ===========================================================================


def test_adequacy_is_str_enum():
    assert Adequacy("sufficient") == Adequacy.SUFFICIENT
    assert str(Adequacy.SUFFICIENT.value) == "sufficient"
    assert Adequacy.SUFFICIENT == "sufficient"


def test_critic_action_is_str_enum():
    assert CriticAction("retry_with_rewrite") == CriticAction.RETRY_WITH_REWRITE
    assert CriticAction.ACCEPT == "accept"


def test_critic_decision_frozen():
    d = CriticDecision(
        adequacy=Adequacy.SUFFICIENT,
        reason="ok",
        suggested_action=CriticAction.ACCEPT,
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        d.reason = "other"  # type: ignore[misc]


def test_reflect_result_frozen():
    r = ReflectResult(
        final_rows=(),
        iterations=(),
        final_adequacy=Adequacy.PARTIAL,
        stopped_reason="accepted",
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        r.stopped_reason = "other"  # type: ignore[misc]


# ===========================================================================
# critic_evaluate
# ===========================================================================


def test_critic_evaluate_happy_path():
    reset_critic_cache()
    def fn(query, rows):
        return {
            "adequacy": "sufficient",
            "reason": "ok",
            "suggested_action": "accept",
        }
    d = critic_evaluate("q", [], fn)
    assert d.adequacy == Adequacy.SUFFICIENT
    assert d.reason == "ok"
    assert d.suggested_action == CriticAction.ACCEPT


def test_critic_evaluate_invalid_adequacy_fallback():
    reset_critic_cache()
    def fn(query, rows):
        return {
            "adequacy": "bogus",
            "reason": "x",
            "suggested_action": "accept",
        }
    d = critic_evaluate("q", [], fn)
    assert d.adequacy == Adequacy.PARTIAL
    assert d.suggested_action == CriticAction.ACCEPT
    assert "bogus" in d.reason


def test_critic_evaluate_invalid_action_fallback():
    reset_critic_cache()
    def fn(query, rows):
        return {
            "adequacy": "irrelevant",
            "reason": "?",
            "suggested_action": "unknown_action",
        }
    d = critic_evaluate("q", [], fn)
    assert d.adequacy == Adequacy.IRRELEVANT
    assert d.suggested_action == CriticAction.ACCEPT
    assert "unknown_action" in d.reason


def test_critic_evaluate_lru_cache_hit():
    reset_critic_cache()
    counter = {"count": 0}
    def fn(query, rows):
        counter["count"] += 1
        return {"adequacy": "sufficient", "reason": "", "suggested_action": "accept"}

    critic_evaluate("same", [{"node_id": "n1", "similarity": 0.9}], fn)
    critic_evaluate("same", [{"node_id": "n1", "similarity": 0.9}], fn)
    assert counter["count"] == 1

    # Different rows → cache miss.
    critic_evaluate("same", [{"node_id": "n2", "similarity": 0.8}], fn)
    assert counter["count"] == 2


# ===========================================================================
# reflect — stop conditions
# ===========================================================================


class _RetrievalSpy:
    """Stub retrieval_fn that captures kwargs of each call."""

    def __init__(self, rows_per_call: list[list[dict]] | None = None):
        self._rows = rows_per_call or [[{"node_id": "n1"}]]
        self.calls: list[dict] = []

    def __call__(self, **kwargs):
        self.calls.append(dict(kwargs))
        idx = min(len(self.calls) - 1, len(self._rows) - 1)
        return list(self._rows[idx])


def _critic_script(*decisions: tuple[Adequacy, CriticAction]):
    """Build a critic_fn that returns decisions in order."""
    it = iter(decisions)
    last = decisions[-1]

    def fn(query, rows):
        nonlocal last
        try:
            adequacy, action = next(it)
        except StopIteration:
            adequacy, action = last
        return {
            "adequacy": adequacy.value,
            "reason": "",
            "suggested_action": action.value,
        }
    return fn


def test_reflect_accepts_on_sufficient_iter_0():
    reset_critic_cache()
    spy = _RetrievalSpy()
    result = reflect(
        "q",
        retrieval_fn=spy,
        critic_fn=_critic_script((Adequacy.SUFFICIENT, CriticAction.ACCEPT)),
    )
    assert len(result.iterations) == 1
    assert result.stopped_reason == "accepted"
    assert result.final_adequacy == Adequacy.SUFFICIENT
    assert len(spy.calls) == 1


def test_reflect_retry_once_then_accept():
    reset_critic_cache()
    spy = _RetrievalSpy(rows_per_call=[
        [{"node_id": "n1"}],
        [{"node_id": "n2"}],
    ])
    result = reflect(
        "q",
        retrieval_fn=spy,
        critic_fn=_critic_script(
            (Adequacy.IRRELEVANT, CriticAction.RETRY_WITH_REWRITE),
            (Adequacy.SUFFICIENT, CriticAction.ACCEPT),
        ),
    )
    assert len(result.iterations) == 2
    assert result.stopped_reason == "accepted"
    assert len(spy.calls) == 2
    # second call got the rewrite kwarg.
    assert spy.calls[1].get("rewrite") == "decompose"


def test_reflect_retries_exhausted():
    reset_critic_cache()
    spy = _RetrievalSpy(rows_per_call=[[{"node_id": f"n{i}"}] for i in range(5)])
    result = reflect(
        "q",
        retrieval_fn=spy,
        critic_fn=_critic_script(
            (Adequacy.IRRELEVANT, CriticAction.EXPAND_HOPS),
        ),
        max_retries=2,
    )
    assert len(result.iterations) == 3  # 1 + 2 retries
    assert result.stopped_reason == "retries_exhausted"
    assert len(spy.calls) == 3


# ===========================================================================
# reflect — action kwargs
# ===========================================================================


def test_reflect_retry_with_rewrite_kwarg():
    reset_critic_cache()
    spy = _RetrievalSpy(rows_per_call=[[{"node_id": "a"}], [{"node_id": "b"}]])
    reflect(
        "q",
        retrieval_fn=spy,
        critic_fn=_critic_script(
            (Adequacy.IRRELEVANT, CriticAction.RETRY_WITH_REWRITE),
            (Adequacy.SUFFICIENT, CriticAction.ACCEPT),
        ),
    )
    assert spy.calls[0] == {}  # initial call has no kwargs
    assert spy.calls[1].get("rewrite") == "decompose"


def test_reflect_expand_hops_ceiling():
    reset_critic_cache()
    spy = _RetrievalSpy(rows_per_call=[[{"node_id": f"n{i}"}] for i in range(5)])
    reflect(
        "q",
        retrieval_fn=spy,
        critic_fn=_critic_script(
            (Adequacy.IRRELEVANT, CriticAction.EXPAND_HOPS),
        ),
        max_retries=3,
    )
    # baseline hint = 1; first EXPAND_HOPS → 2; next → 3; next → clamped 3.
    hints = [c.get("fixed_hops_hint") for c in spy.calls[1:]]
    assert hints[0] == 2
    assert hints[1] == 3
    assert hints[2] == 3  # ceiling


def test_reflect_fallback_semantic_kwarg():
    reset_critic_cache()
    spy = _RetrievalSpy(rows_per_call=[[{"node_id": "a"}], [{"node_id": "b"}]])
    reflect(
        "q",
        retrieval_fn=spy,
        critic_fn=_critic_script(
            (Adequacy.IRRELEVANT, CriticAction.FALLBACK_SEMANTIC),
            (Adequacy.SUFFICIENT, CriticAction.ACCEPT),
        ),
    )
    assert spy.calls[1].get("fallback_semantic") is True


def test_reflect_change_intent_stops_v1():
    reset_critic_cache()
    spy = _RetrievalSpy()
    result = reflect(
        "q",
        retrieval_fn=spy,
        critic_fn=_critic_script(
            (Adequacy.IRRELEVANT, CriticAction.CHANGE_INTENT),
        ),
    )
    assert len(result.iterations) == 1
    assert result.stopped_reason == "change_intent_v1_not_implemented"
    assert len(spy.calls) == 1  # no retry


# ===========================================================================
# reflect — exception handling + audit_sink
# ===========================================================================


def test_reflect_critic_exception_captured():
    reset_critic_cache()
    spy = _RetrievalSpy()

    def exploding(query, rows):
        raise RuntimeError("LLM down")

    result = reflect("q", retrieval_fn=spy, critic_fn=exploding)
    assert result.stopped_reason == "critic_error"
    assert result.final_rows == ({"node_id": "n1"},)  # last rows preserved
    assert len(spy.calls) == 1  # critic exception stops further retries


def test_reflect_audit_sink_called_per_iter():
    reset_critic_cache()
    spy = _RetrievalSpy(rows_per_call=[[{"node_id": f"n{i}"}] for i in range(5)])
    audit_log: list[dict] = []
    reflect(
        "q",
        retrieval_fn=spy,
        critic_fn=_critic_script(
            (Adequacy.IRRELEVANT, CriticAction.EXPAND_HOPS),
        ),
        max_retries=2,
        audit_sink=audit_log.append,
    )
    assert len(audit_log) == 3  # 1 + 2 retries
    for entry in audit_log:
        assert set(entry.keys()) == {
            "iteration", "adequacy", "action", "rows_count", "query_hash",
        }


def test_reflect_audit_sink_failure_swallowed():
    reset_critic_cache()
    spy = _RetrievalSpy()

    def broken_sink(record):
        raise RuntimeError("disk full")

    result = reflect(
        "q",
        retrieval_fn=spy,
        critic_fn=_critic_script((Adequacy.SUFFICIENT, CriticAction.ACCEPT)),
        audit_sink=broken_sink,
    )
    # Audit failures are swallowed — reflect still completes.
    assert result.stopped_reason == "accepted"
