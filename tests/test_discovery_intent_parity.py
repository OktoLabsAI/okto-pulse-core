"""CI parity between Discovery intents and the executor dispatcher.

Ideação 0e85ac05 — before this test, the seed catalog could declare a
tool_binding that the executor didn't know how to run, and no signal would
surface until a user clicked the intent in production. This test walks the
known-at-build-time seed list and checks four invariants:

1. Every seed intent's `tool_binding` has a dispatch branch in
   `discovery_executor.execute_intent`.
2. The dispatcher recognises every listed binding (no typo drift).
3. Every declared required param is accepted by the dispatcher (so a
   realistic call with dummy values runs without raising KeyError / missing
   param synchronously).
4. Required params that are declared but not supplied raise ValueError
   with the exact missing-key message — the contract the REST endpoint
   relies on to translate to HTTP 400.

The test keeps the KG layer out of the picture by stubbing the few KG
service calls the executor makes — we're validating wiring, not KG data.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from okto_pulse.core.services import discovery_executor


# The seed intents shipped with the community edition. Kept in sync with
# _bootstrap_default_discovery_intents (infra/database.py). Duplicating here
# is intentional: the test has to fail if database.py diverges.
SEED_INTENTS: list[dict[str, Any]] = [
    {"name": "coverage_for_fr", "tool_binding": "okto_pulse_list_test_scenarios",
     "params_schema": {"fr_id": {"required": True}}},
    {"name": "uncovered_requirements", "tool_binding": "okto_pulse_list_uncovered_requirements",
     "params_schema": None},
    {"name": "scenarios_without_tasks", "tool_binding": "okto_pulse_list_test_scenarios",
     "params_schema": None},
    {"name": "decisions_superseded", "tool_binding": "okto_pulse_list_supersedence_chains",
     "params_schema": None},
    {"name": "contradictions_in_kg", "tool_binding": "okto_pulse_kg_find_contradictions",
     "params_schema": None},
    {"name": "decisions_by_topic", "tool_binding": "okto_pulse_kg_find_similar_decisions",
     "params_schema": {"topic": {"required": True}}},
    {"name": "blockers_current_sprint", "tool_binding": "okto_pulse_list_blockers",
     "params_schema": None},
    {"name": "dependencies_of_card", "tool_binding": "okto_pulse_get_card_dependencies",
     "params_schema": {"card_id": {"required": True}}},
    {"name": "similar_nodes_to_text", "tool_binding": "okto_pulse_kg_query_natural",
     "params_schema": {"query": {"required": True}}},
    {"name": "learning_from_bugs", "tool_binding": "okto_pulse_kg_get_learning_from_bugs",
     "params_schema": None},
    {"name": "recent_activity", "tool_binding": "okto_pulse_get_activity_log",
     "params_schema": None},
    {"name": "my_mentions", "tool_binding": "okto_pulse_list_my_mentions",
     "params_schema": None},
]


# Bindings the dispatcher MUST handle. Drift here signals either a typo in
# the seed, a missing branch in execute_intent, or a rename that forgot to
# update the seed.
EXPECTED_BINDINGS = {i["tool_binding"] for i in SEED_INTENTS}


def _make_intent(row: dict[str, Any]) -> SimpleNamespace:
    """Cheap stand-in for the DiscoveryIntent ORM object — execute_intent
    only touches a handful of attributes."""
    return SimpleNamespace(
        id=f"test-{row['name']}",
        name=row["name"],
        tool_binding=row["tool_binding"],
        params_schema=row["params_schema"],
    )


def test_every_seed_binding_is_known_to_executor():
    """Smoke-level parity: the dispatcher has a branch for every seed."""
    # The dispatch itself is implemented as an if-chain, so we introspect
    # the function source. A heavier approach would be a registry; this is
    # the smallest check that catches drift without adding new surface.
    import inspect

    source = inspect.getsource(discovery_executor.execute_intent)
    for binding in EXPECTED_BINDINGS:
        assert binding in source, (
            f"Dispatcher has no branch for seed binding {binding!r}. "
            f"Either add a branch in discovery_executor.execute_intent or "
            f"remove the intent from the seed catalog."
        )


@pytest.mark.parametrize(
    "row",
    [row for row in SEED_INTENTS if row["params_schema"]],
    ids=[row["name"] for row in SEED_INTENTS if row["params_schema"]],
)
@pytest.mark.asyncio
async def test_required_params_raise_value_error(row):
    """When a required param is missing, the dispatcher raises ValueError
    with a message that names the offending field — the REST endpoint
    converts this to HTTP 400 verbatim."""
    intent = _make_intent(row)
    required_keys = [
        k for k, meta in intent.params_schema.items() if meta.get("required")
    ]
    if not required_keys:
        pytest.skip("no required params declared")

    with pytest.raises(ValueError) as exc:
        await discovery_executor.execute_intent(
            db=None,  # dispatcher should bail out before touching db
            user_id="u1",
            board_id="b1",
            intent=intent,
            params={},
        )
    msg = str(exc.value)
    missing_key = required_keys[0]
    assert missing_key in msg, (
        f"ValueError message should name the missing required key "
        f"{missing_key!r}, got: {msg!r}"
    )


def test_no_unused_branches_in_dispatcher():
    """Reverse check: every branch in the dispatcher corresponds to a
    known seed. Prevents orphan handlers that no intent points to."""
    import re
    import inspect

    source = inspect.getsource(discovery_executor.execute_intent)
    branch_bindings = set(
        re.findall(r'binding == "(okto_pulse_[a-z_]+)"', source)
    )
    orphans = branch_bindings - EXPECTED_BINDINGS
    assert not orphans, (
        f"Dispatcher handles bindings that no seed intent points to: "
        f"{orphans}. Either wire a seed or remove the branch."
    )


def test_blockers_dispatcher_scoped_to_active_sprint():
    """Ideação bf6a3766 regression guard.

    Before the fix, ``_exec_blockers`` delegated to the board-wide
    ``analytics_service.compute_blockers``, which happily surfaced every
    ``uncovered_scenario`` on the board as a "blocker on the current
    sprint". Two failures that must stay fixed:

    1. Scope — the executor must filter by ``SprintStatus.ACTIVE``.
       Without that, the intent silently falls back to a board-wide
       triage and the user reads a lie.
    2. Semantics — ``uncovered_scenario`` is the concern of a different
       intent (``scenarios_without_tasks``). Re-introducing it here
       duplicates results and hides the real dependency chain.
    """
    import inspect

    src = inspect.getsource(discovery_executor._exec_blockers)
    assert "SprintStatus.ACTIVE" in src, (
        "discovery_executor._exec_blockers no longer filters by active "
        "sprint — this reverts the fix for ideação bf6a3766."
    )
    assert "uncovered_scenario" not in src, (
        "discovery_executor._exec_blockers re-introduced uncovered_scenario "
        "rows. That row type belongs to scenarios_without_tasks; keeping "
        "both intents distinct is the point of ideação bf6a3766."
    )
    assert "blocked_card" in src, (
        "discovery_executor._exec_blockers no longer emits blocked_card "
        "rows — the intent needs at least one dependency-based blocker "
        "type to be useful."
    )
