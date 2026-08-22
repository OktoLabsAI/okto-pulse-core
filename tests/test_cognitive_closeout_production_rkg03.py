"""RKG-03 — cognitive closeout production rules + candidate→persist (unit).

Scenarios:
  * ts_5cbf9193 / AC1 — spec done with a considered alternative persists an
    Alternative (queryable) and is idempotent on replay.
  * ts_208012a5 / AC2 — bug done with root cause/fix/evidence persists a Learning
    with a validates edge to the canonical Bug (resolved via the RKG-02 resolver).
  * ts_86939a76 / AC3 — absence + no_llm_config are classified honestly, separate
    from technical failure, and never fabricate a node.
"""

from __future__ import annotations

import asyncio
import threading

import pytest

from okto_pulse.core.kg import cognitive_closeout_production as ccp

U = "11111111-1111-1111-1111-111111111111"
ANALYSIS_WITH_ALT = (
    "## Analysis\n"
    "We considered using Redis instead of Postgres for the cache layer.\n"
    "Assuming that traffic stays under 1000 rps, a single node is enough.\n"
)


class _DummySummariser:
    def __init__(self, title="Guard encoding before regex", body="Normalise NFC first."):
        self.title = title
        self.body = body

    def summarise(self, *, bug_title, action_plan, context=None):
        return self.title, self.body


class _FakePersister:
    """Records persisted candidates instead of touching graph.lbug."""

    def __init__(self, *, existing=None, fail=False):
        self.persisted = []
        self._existing = set(existing or [])
        self.fail = fail

    def already_persisted(self, board_id, node_type, source_artifact_ref):
        return source_artifact_ref in self._existing

    async def persist(self, board_id, artifact_type, candidate):
        if self.fail:
            return False
        self.persisted.append(candidate)
        self._existing.add(candidate.source_artifact_ref)
        return True


class _OffLoopExistingPersister:
    """Fails if the synchronous graph probe runs on the asyncio loop thread."""

    def __init__(self) -> None:
        self.probe_thread_ids: list[int] = []

    def already_persisted(self, board_id, node_type, source_artifact_ref):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:  # pragma: no cover - assertion documents the regression contract
            raise AssertionError("synchronous graph probe ran on the event loop")
        self.probe_thread_ids.append(threading.get_ident())
        return True

    async def persist(self, board_id, artifact_type, candidate):
        raise AssertionError("an existing candidate must not be persisted again")


def _bug_probe(known):
    return lambda uuid: uuid in known


# ---------------------------------------------------------------------------
# AC1 — spec Alternative persisted + idempotent replay
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac1_spec_alternative_persisted():
    p = _FakePersister()
    res = await ccp.run_cognitive_closeout(
        board_id="b", artifact_type="spec", artifact_ref="spec:s1",
        spec_context=ANALYSIS_WITH_ALT, persister=p)
    assert res.outcome == "persisted"
    alt = [c for c in p.persisted if c.node_type == "Alternative"]
    assert alt, "an Alternative candidate should be persisted"
    assert alt[0].source_artifact_ref.startswith("spec:s1:alternative:")
    assert res.persisted_refs


@pytest.mark.asyncio
async def test_ac1_idempotent_replay_does_not_duplicate():
    p = _FakePersister()
    first = await ccp.run_cognitive_closeout(
        board_id="b", artifact_type="spec", artifact_ref="spec:s1",
        spec_context=ANALYSIS_WITH_ALT, persister=p)
    n_after_first = len(p.persisted)
    second = await ccp.run_cognitive_closeout(
        board_id="b", artifact_type="spec", artifact_ref="spec:s1",
        spec_context=ANALYSIS_WITH_ALT, persister=p)
    assert first.outcome == "persisted"
    assert second.outcome == "persisted"
    assert len(p.persisted) == n_after_first  # replay persisted nothing new
    assert second.skipped_existing_refs  # recognised as already persisted


@pytest.mark.asyncio
async def test_ac1_idempotency_probe_runs_off_event_loop():
    loop_thread_id = threading.get_ident()
    p = _OffLoopExistingPersister()

    result = await ccp.run_cognitive_closeout(
        board_id="b",
        artifact_type="spec",
        artifact_ref="spec:s1",
        spec_context=ANALYSIS_WITH_ALT,
        persister=p,
    )

    assert result.outcome == "persisted"
    assert result.skipped_existing_refs
    assert p.probe_thread_ids
    assert all(thread_id != loop_thread_id for thread_id in p.probe_thread_ids)


# ---------------------------------------------------------------------------
# AC2 — bug Learning persisted with validates -> canonical Bug
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac2_bug_learning_persisted_with_validates_edge():
    p = _FakePersister()
    res = await ccp.run_cognitive_closeout(
        board_id="b", artifact_type="bug", artifact_ref=f"card:{U}",
        bug_card_id=U, bug_title="Regex misfires on accented chars",
        bug_action_plan="Repro locally; root cause was missing NFC normalisation; fixed + added test.",
        llm_config={"provider": "openai", "model": "gpt-4"},
        summariser=_DummySummariser(), bug_probe=_bug_probe({U}),
        persister=p)
    assert res.outcome == "persisted"
    learning = [c for c in p.persisted if c.node_type == "Learning"]
    assert learning
    edges = learning[0].edges
    assert any(e.edge_type == "validates" and e.to_ref == f"card:{U}" for e in edges)


# ---------------------------------------------------------------------------
# AC3 — honest absence / config gap classification
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac3_spec_no_material():
    p = _FakePersister()
    res = await ccp.run_cognitive_closeout(
        board_id="b", artifact_type="spec", artifact_ref="spec:s2",
        spec_context="## Analysis\nWe implemented the feature as specified.\n", persister=p)
    assert res.outcome == "no_material"
    assert not p.persisted  # never fabricate a node


@pytest.mark.asyncio
async def test_ac3_bug_no_llm_config_skipped():
    p = _FakePersister()
    res = await ccp.run_cognitive_closeout(
        board_id="b", artifact_type="bug", artifact_ref=f"card:{U}",
        bug_card_id=U, bug_action_plan="Root cause found and fixed with a real long narrative here.",
        llm_config=None, summariser=_DummySummariser(), persister=p)
    assert res.outcome == "skipped_no_llm_config"
    assert not p.persisted


@pytest.mark.asyncio
async def test_ac3_bug_short_action_plan_not_applicable():
    p = _FakePersister()
    res = await ccp.run_cognitive_closeout(
        board_id="b", artifact_type="bug", artifact_ref=f"card:{U}",
        bug_card_id=U, bug_action_plan="too short",
        llm_config={"provider": "openai"}, summariser=_DummySummariser(), persister=p)
    assert res.outcome == "not_applicable"
    assert not p.persisted


# ---------------------------------------------------------------------------
# extractor_triggered_but_not_persisted + TR1
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_extractor_triggered_but_not_persisted_on_persist_failure():
    p = _FakePersister(fail=True)
    res = await ccp.run_cognitive_closeout(
        board_id="b", artifact_type="spec", artifact_ref="spec:s1",
        spec_context=ANALYSIS_WITH_ALT, persister=p)
    assert res.candidates_emitted >= 1
    assert res.outcome == "extractor_triggered_but_not_persisted"
    assert not res.persisted_refs


@pytest.mark.asyncio
async def test_tr1_only_cognitive_node_types_are_persisted():
    # The service only ever emits Alternative/Assumption/Learning/Decision.
    p = _FakePersister()
    await ccp.run_cognitive_closeout(
        board_id="b", artifact_type="spec", artifact_ref="spec:s1",
        spec_context=ANALYSIS_WITH_ALT, persister=p)
    assert all(c.node_type in ccp.COGNITIVE_NODE_TYPES for c in p.persisted)
    assert "Criterion" not in {c.node_type for c in p.persisted}
    assert "Constraint" not in {c.node_type for c in p.persisted}


@pytest.mark.asyncio
async def test_non_bug_card_fails_closed_no_bug_ref_fabrication():
    # #4 (codex): a card the probe does NOT confirm as a canonical bug must NOT
    # produce a Learning with a fabricated bug:<id> ref — it fails closed.
    p = _FakePersister()
    res = await ccp.run_cognitive_closeout(
        board_id="b", artifact_type="bug", artifact_ref=f"card:{U}",
        bug_card_id=U, bug_action_plan="A real root cause and fix narrative long enough to pass.",
        llm_config={"provider": "openai"}, summariser=_DummySummariser(),
        bug_probe=_bug_probe(set()), persister=p)
    assert res.outcome == "not_applicable"
    assert not p.persisted  # no Learning, no bug:<id> fabrication


@pytest.mark.asyncio
async def test_card_not_bug_no_spec_is_extractor_not_triggered():
    # #5 (codex): a done card that is neither a bug nor spec-backed -> the
    # cognitive extractor never triggers for it.
    p = _FakePersister()
    res = await ccp.run_cognitive_closeout(
        board_id="b", artifact_type="card", artifact_ref=f"card:{U}", persister=p)
    assert res.outcome == "extractor_not_triggered"
    assert not p.persisted


@pytest.mark.asyncio
async def test_ac2_learning_validates_ref_is_canonical_card_form():
    # The Learning's validates edge targets the RESOLVED canonical Bug (card:<uuid>),
    # never a fabricated bug:<id> (the source_ref of the Learning stays bug:<id>).
    p = _FakePersister()
    await ccp.run_cognitive_closeout(
        board_id="b", artifact_type="bug", artifact_ref=f"card:{U}",
        bug_card_id=U, bug_action_plan="Root cause + fix narrative long enough to pass the gate.",
        llm_config={"provider": "openai"}, summariser=_DummySummariser(),
        bug_probe=_bug_probe({U}), persister=p)
    learning = [c for c in p.persisted if c.node_type == "Learning"][0]
    assert learning.edges[0].edge_type == "validates"
    assert learning.edges[0].to_ref == f"card:{U}"  # canonical, not bug:<id>


@pytest.mark.asyncio
async def test_spec_alternative_carries_relates_to_when_decision_known():
    # #2 (codex): with a related Decision the Alternative gains a relates_to edge.
    p = _FakePersister()
    await ccp.run_cognitive_closeout(
        board_id="b", artifact_type="spec", artifact_ref="spec:s1",
        spec_context=ANALYSIS_WITH_ALT, decision_ref="decision_node_1", persister=p)
    alt = [c for c in p.persisted if c.node_type == "Alternative"][0]
    assert any(e.edge_type == "relates_to" and e.incoming and e.to_ref == "decision_node_1"
               for e in alt.edges)
