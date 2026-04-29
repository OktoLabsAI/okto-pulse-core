"""Spec 3d907a87 (D4 / TS6) — coverage_for_fr fuzzy contains.

Card IMPL-E swapped the strict membership check for a case-insensitive
substring match so users can type a short id (FR1) and still hit
linked_criteria entries that store the resolved text ("FR1 — Adicionar
handler ..."). These tests pin the behavior — both the matching path and
the FR1↔FR10 false-positive registered as TR5.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from okto_pulse.core.services import discovery_executor


def _spec(*, scenarios: list[dict]) -> SimpleNamespace:
    return SimpleNamespace(
        id="spec-1", title="Test spec", board_id="board-1",
        test_scenarios=scenarios,
    )


class _AwaitableSpecs:
    def __init__(self, specs: list):
        self._specs = specs

    def scalars(self):
        return self

    def all(self) -> list:
        return self._specs


class _StubSession:
    def __init__(self, specs: list):
        self._specs = specs

    async def execute(self, _stmt) -> _AwaitableSpecs:
        return _AwaitableSpecs(self._specs)


def _intent(name: str = "coverage_for_fr") -> SimpleNamespace:
    return SimpleNamespace(name=name)


@pytest.mark.asyncio
async def test_fr_id_fuzzy_match_returns_scenarios():
    """fr_id="FR1" matches linked_criteria=["FR1 — texto longo"]."""
    spec = _spec(scenarios=[
        {
            "id": "ts1",
            "title": "S1",
            "linked_criteria": ["FR1 — Adicionar handler"],
            "linked_task_ids": ["card-x"],
        },
    ])
    db = _StubSession([spec])
    out = await discovery_executor._exec_test_scenarios(
        db, "board-1", _intent("coverage_for_fr"), {"fr_id": "FR1"},
    )
    assert out["total"] == 1
    assert out["rows"][0]["id"] == "ts1"


@pytest.mark.asyncio
async def test_fr_id_fuzzy_match_is_case_insensitive():
    """fr_id="fr1" still matches "FR1 — ...".
    Justifies BR6 + the .lower() pair in the executor.
    """
    spec = _spec(scenarios=[
        {
            "id": "ts1",
            "title": "S1",
            "linked_criteria": ["FR1 — Texto"],
            "linked_task_ids": [],
        },
    ])
    db = _StubSession([spec])
    out = await discovery_executor._exec_test_scenarios(
        db, "board-1", _intent("coverage_for_fr"), {"fr_id": "fr1"},
    )
    assert out["total"] == 1


@pytest.mark.asyncio
async def test_fr_id_no_match_returns_empty():
    """fr_id="FR99" returns nothing when no criterion text contains it."""
    spec = _spec(scenarios=[
        {
            "id": "ts1",
            "title": "S1",
            "linked_criteria": ["FR1 — A", "AC2 — B"],
            "linked_task_ids": [],
        },
    ])
    db = _StubSession([spec])
    out = await discovery_executor._exec_test_scenarios(
        db, "board-1", _intent("coverage_for_fr"), {"fr_id": "FR99"},
    )
    assert out["total"] == 0


@pytest.mark.asyncio
async def test_fr1_collides_with_fr10_documented_in_tr5():
    """fr_id="FR1" matches "FR10 — ..." too (substring tradeoff TR5).

    This is the documented false-positive. If a future iteration tightens
    the match (word-boundary regex), update this test to assert the
    opposite outcome and bump the TR.
    """
    spec = _spec(scenarios=[
        {
            "id": "ts1",
            "title": "S1",
            "linked_criteria": ["FR10 — Mapeamento agregado"],
            "linked_task_ids": ["c1"],
        },
    ])
    db = _StubSession([spec])
    out = await discovery_executor._exec_test_scenarios(
        db, "board-1", _intent("coverage_for_fr"), {"fr_id": "FR1"},
    )
    assert out["total"] == 1


@pytest.mark.asyncio
async def test_no_fr_id_filter_returns_all_scenarios_with_tasks():
    """Without fr_id, scenarios pass the filter (subject to other rules)."""
    spec = _spec(scenarios=[
        {
            "id": "ts1",
            "title": "S1",
            "linked_criteria": ["FR1 — A"],
            "linked_task_ids": ["c1"],
        },
        {
            "id": "ts2",
            "title": "S2",
            "linked_criteria": ["AC2 — B"],
            "linked_task_ids": ["c2"],
        },
    ])
    db = _StubSession([spec])
    out = await discovery_executor._exec_test_scenarios(
        db, "board-1", _intent("coverage_for_fr"), {},
    )
    assert out["total"] == 2
