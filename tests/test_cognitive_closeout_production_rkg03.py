"""Native Spec candidate closeout and rejection of unauthored Bug inference."""

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
async def test_card_not_bug_no_spec_is_extractor_not_triggered():
    # #5 (codex): a done card that is neither a bug nor spec-backed -> the
    # cognitive extractor never triggers for it.
    p = _FakePersister()
    res = await ccp.run_cognitive_closeout(
        board_id="b", artifact_type="card", artifact_ref=f"card:{U}", persister=p)
    assert res.outcome == "extractor_not_triggered"
    assert not p.persisted


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


@pytest.mark.asyncio
async def test_bug_closeout_requires_authored_capture_without_persistence():
    persister = _FakePersister()
    with pytest.raises(ValueError, match="bug_closeout_requires_authored_capture"):
        await ccp.run_cognitive_closeout(board_id="b", artifact_type="bug",
            artifact_ref="bug:b1", persister=persister)
    assert persister.persisted == []


@pytest.mark.asyncio
@pytest.mark.parametrize("old_field", ["llm_config", "summariser", "bug_action_plan", "bug_probe"])
async def test_old_inference_arguments_are_not_accepted(old_field):
    persister = _FakePersister()
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        await ccp.run_cognitive_closeout(board_id="b", artifact_type="bug",
            artifact_ref="bug:b1", persister=persister, **{old_field: object()})
    assert persister.persisted == []
