"""R3 regressions: graph identity limits must not discard delivery inputs."""

from types import SimpleNamespace
from contextlib import contextmanager
import asyncio

import pytest

from okto_pulse.core.application.processors import global_outbox as outbox
from okto_pulse.core.ports.global_outbox import GlobalOutboxNodeRefFact


class IdentityGraph:
    """Read-port fixture with Grafx's independently reproduced list limit."""

    def __init__(self, *, missing=(), failure=None):
        self.calls = []
        self.missing = set(missing)
        self.failure = failure

    def execute_read_only(self, board_id, query, params, *, max_rows):
        assert board_id == "board"
        ids = params["ids"]
        if len(ids) > 1024:
            raise ValueError("A query list may hold at most 1024 elements")
        self.calls.append((query, list(ids), max_rows))
        if self.failure == "second_batch" and len(self.calls) == 2:
            raise RuntimeError("injected middle-batch failure")
        present = [key for key in ids if key not in self.missing]
        if "[:validates]" in query:
            rows = [[key, 2, 3] for key in present]
        elif "[:relates_to]" in query:
            rows = [[key, "Decision", "canonical", 1] for key in present]
        elif "source_artifact_ref" in query:
            rows = [[key, f"spec:{key}"] for key in present]
        elif "n.title" in query:
            rows = [[key, f"Title {key}", [0.25, 0.75], "canonical"] for key in present]
        else:
            rows = [[key, "canonical"] for key in present]
        if self.failure == "duplicate" and rows:
            rows.append(rows[0])
        if self.failure == "foreign" and rows:
            rows[0][0] = "foreign-identity"
        return {"rows": rows, "truncated": self.failure == "truncated" or (
            self.failure == "truncated_relations" and "[:relates_to]" in query
        )}


def _install(monkeypatch, graph):
    monkeypatch.setattr(outbox, "get_kg_registry", lambda: SimpleNamespace(cypher_executor=graph))


@pytest.mark.parametrize("size", [0, 1, 500, 501, 1024, 1025, 2000])
@pytest.mark.parametrize("reader", ["metadata", "nodes"])
def test_identity_reads_are_complete_and_bounded(monkeypatch, size, reader):
    ids = [f"entity-{i:05}" for i in range(size)]
    graph = IdentityGraph(missing=ids[-1:])
    _install(monkeypatch, graph)
    if reader == "metadata":
        result = outbox.GlobalOutboxProcessor._read_board_layer_meta(
            "board", {key: "Entity" for key in reversed(ids)},
        )
        assert result is not None
        observed = set(result)
    else:
        refs = [GlobalOutboxNodeRefFact(key, "Entity") for key in reversed(ids)]
        result = outbox.GlobalOutboxProcessor._read_board_nodes_for_refs("board", refs + refs)
        assert result is not None
        observed = {row["id"] for row in result}
        assert len(result) == len(observed)
    assert observed == set(ids[:-1])
    assert all(0 < len(batch) <= 500 for _, batch, _ in graph.calls)
    assert [key for _, batch, _ in graph.calls for key in batch] == ids


@pytest.mark.parametrize("nested", [True, False, "true", "false", 1, None, {}, []])
@pytest.mark.parametrize("top", ["absent", True, False, "false"])
@pytest.mark.parametrize("canonical", [False, True])
def test_retryability_requires_typed_authority(nested, top, canonical):
    from okto_pulse.core.mcp.outcome import coerce_mcp_tool_outcome

    payload = {"error": {"code": "lock_contention", "message": "Writer is busy", "retryable": nested}}
    if top != "absent":
        payload["retryable"] = top
    if canonical:
        payload.update(outcome="error", error_code="lock_contention", message="Writer is busy")
    result = coerce_mcp_tool_outcome(payload)
    assert result.code == "lock_contention"
    assert result.message == "Writer is busy"
    assert result.retryable is ((top is True) if top != "absent" else (nested is True))


@pytest.mark.asyncio
@pytest.mark.parametrize("failures", [0, 1, 3, 4])
async def test_writer_retries_acquisition_only(monkeypatch, failures):
    from okto_pulse.core.mcp import kg_tools

    events, sleeps = [], []

    @contextmanager
    def writer(*args, **kwargs):
        events.append("acquire")
        if events.count("acquire") <= failures:
            raise kg_tools.GuardedWriteError("lock_contention", "busy", retryable=True)
        try:
            yield "lease"
        finally:
            events.append("release")

    async def sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(kg_tools, "guarded_board_write", writer)
    monkeypatch.setattr(kg_tools.asyncio, "sleep", sleep)
    monkeypatch.setattr(kg_tools.random, "uniform", lambda *_: 0.025)
    try:
        async with kg_tools._acquire_consolidation_writer("board", owner_id="agent", mutation_ref="commit:1") as lease:
            assert lease == "lease"
            events.append("mutation")
    except kg_tools.GuardedWriteError as exc:
        assert failures == 4
        assert exc.code == "lock_contention" and exc.retryable is True
        assert exc.details["attempts"] == 4
    assert events.count("acquire") == min(failures + 1, 4)
    assert sleeps == pytest.approx([0.125, 0.225, 0.425][:min(failures, 3)])
    assert events.count("mutation") == events.count("release") == (failures < 4)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["acquire", "mutation", "release"])
async def test_writer_never_retries_non_acquisition_failures(monkeypatch, phase):
    from okto_pulse.core.mcp import kg_tools

    events = []
    code = "writer_lock_unavailable" if phase == "acquire" else "lock_contention"

    @contextmanager
    def writer(*args, **kwargs):
        events.append("acquire")
        if phase == "acquire":
            raise kg_tools.GuardedWriteError(code, "failed", retryable=True)
        try:
            yield "lease"
        finally:
            events.append("release")
            if phase == "release":
                raise kg_tools.GuardedWriteError(code, "failed", retryable=True)

    monkeypatch.setattr(kg_tools, "guarded_board_write", writer)
    with pytest.raises(kg_tools.GuardedWriteError) as caught:
        async with kg_tools._acquire_consolidation_writer("board", owner_id="agent", mutation_ref="commit:1"):
            if phase == "mutation":
                raise kg_tools.GuardedWriteError(code, "failed", retryable=True)
    assert caught.value.code == code
    assert events.count("acquire") == 1
    assert events.count("release") == (phase != "acquire")


@pytest.mark.asyncio
async def test_writer_wait_obeys_cancellation_and_original_deadline(monkeypatch):
    from okto_pulse.core.mcp import kg_tools

    attempts = []

    @contextmanager
    def writer(*args, **kwargs):
        attempts.append("acquire")
        raise kg_tools.GuardedWriteError("lock_contention", "busy", retryable=True)
        yield  # pragma: no cover

    monkeypatch.setattr(kg_tools, "guarded_board_write", writer)
    with pytest.raises(kg_tools.GuardedWriteError):
        async with kg_tools._acquire_consolidation_writer(
            "board", owner_id="agent", mutation_ref="commit:1", deadline=asyncio.get_running_loop().time() + 0.01,
        ):
            pytest.fail("expired acquisition must not mutate")
    assert attempts == ["acquire"]
    waiting = asyncio.Event()

    async def sleep(delay):
        waiting.set()
        await asyncio.Future()

    monkeypatch.setattr(kg_tools.asyncio, "sleep", sleep)

    async def run():
        async with kg_tools._acquire_consolidation_writer("board", owner_id="agent", mutation_ref="commit:2"):
            pytest.fail("cancelled acquisition must not mutate")

    task = asyncio.create_task(run())
    await waiting.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert attempts == ["acquire", "acquire"]


@pytest.mark.asyncio
async def test_writer_does_not_acquire_after_backoff_overruns_deadline(monkeypatch):
    from okto_pulse.core.mcp import kg_tools

    attempts = []
    original_sleep = asyncio.sleep

    @contextmanager
    def writer(*args, **kwargs):
        attempts.append("acquire")
        raise kg_tools.GuardedWriteError("lock_contention", "busy", retryable=True)
        yield  # pragma: no cover

    async def oversleep(delay):
        await original_sleep(0.21)

    monkeypatch.setattr(kg_tools, "guarded_board_write", writer)
    monkeypatch.setattr(kg_tools.asyncio, "sleep", oversleep)
    with pytest.raises(kg_tools.GuardedWriteError) as caught:
        async with kg_tools._acquire_consolidation_writer(
            "board", owner_id="agent", mutation_ref="commit:1", deadline=asyncio.get_running_loop().time() + 0.2,
        ):
            pytest.fail("expired request must not acquire a writer")
    assert attempts == ["acquire"]
    assert caught.value.details["attempts"] == 1


@pytest.mark.parametrize("payload,expected", [
    ({"error": {"code": "resource_busy"}}, True),
    ({"error": {"code": "unknown_error"}}, False),
    ({"success": True, "retryable": True}, False),
    ({"error": {"code": "resource_busy", "retryable": "false"}}, False),
])
def test_retryability_absence_and_success_keep_existing_meaning(payload, expected):
    from okto_pulse.core.mcp.outcome import coerce_mcp_tool_outcome

    assert coerce_mcp_tool_outcome(payload).retryable is expected


@pytest.mark.parametrize("reader", ["metadata", "nodes"])
def test_learning_truncated_relationships_refuse_complete_observation(monkeypatch, reader):
    _install(monkeypatch, IdentityGraph(failure="truncated_relations"))
    if reader == "metadata":
        result = outbox.GlobalOutboxProcessor._read_board_layer_meta("board", {"learning": "Learning"})
    else:
        result = outbox.GlobalOutboxProcessor._read_board_nodes_for_refs(
            "board", [GlobalOutboxNodeRefFact("learning", "Learning")],
        )
    assert result is None


def test_multiple_types_keep_their_authoritative_type(monkeypatch):
    graph = IdentityGraph()
    _install(monkeypatch, graph)
    sources = {f"entity-{i}": "Entity" for i in range(1025)}
    sources.update({f"decision-{i}": "Decision" for i in range(1025)})
    result = outbox.GlobalOutboxProcessor._read_board_layer_meta("board", sources)
    assert result is not None
    assert {key: row["node_type"] for key, row in result.items()} == sources
    assert all(len(batch) <= 500 for _, batch, _ in graph.calls)


@pytest.mark.parametrize("reader", ["metadata", "nodes"])
@pytest.mark.parametrize("failure", ["second_batch", "truncated", "duplicate", "foreign"])
def test_partial_or_ambiguous_observation_never_returns_success(monkeypatch, reader, failure):
    graph = IdentityGraph(failure=failure)
    _install(monkeypatch, graph)
    ids = [f"entity-{i:05}" for i in range(501)]
    if reader == "metadata":
        result = outbox.GlobalOutboxProcessor._read_board_layer_meta(
            "board", dict.fromkeys(ids, "Entity"),
        )
    else:
        result = outbox.GlobalOutboxProcessor._read_board_nodes_for_refs(
            "board", [GlobalOutboxNodeRefFact(key, "Entity") for key in ids],
        )
    assert result is None


@pytest.mark.parametrize("reader", ["metadata", "nodes", "evidence"])
def test_learning_relationships_are_batched_without_losing_completeness(monkeypatch, reader):
    graph = IdentityGraph()
    _install(monkeypatch, graph)
    ids = [f"learning-{i:05}" for i in range(2000)]
    if reader == "metadata":
        result = outbox.GlobalOutboxProcessor._read_board_layer_meta(
            "board", dict.fromkeys(ids, "Learning"),
        )
    elif reader == "nodes":
        rows = outbox.GlobalOutboxProcessor._read_board_nodes_for_refs(
            "board", [GlobalOutboxNodeRefFact(key, "Learning") for key in ids],
        )
        assert rows is not None
        result = {row["id"]: row for row in rows}
    else:
        result = outbox.GlobalOutboxProcessor._read_learning_completeness_evidence(
            "board", ids + ids,
        )
    assert result is not None and set(result) == set(ids)
    for key, row in result.items():
        assert row["canonical_bug_count"] == 2
        assert row["working_bug_count"] == 1
        assert row["relates_to_endpoints"] == [("Decision", "canonical")]
        if reader != "evidence":
            assert row["source_artifact_ref"] == f"spec:{key}"
    assert all(0 < len(batch) <= 500 for _, batch, _ in graph.calls)
