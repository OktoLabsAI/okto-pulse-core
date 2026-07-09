from __future__ import annotations

import ast
from pathlib import Path
from typing import Any, Mapping, Sequence

import pytest

from okto_pulse.core.ports.kg_operational import (
    KGCanonicalDebtSignal,
    KGDeadLetterSignal,
    KGOperationalProviderMissing,
    KGOutboxCounts,
    KGQueueEntrySnapshot,
    register_kg_operational_ports,
    reset_kg_operational_ports_for_tests,
)


class _Store:
    def latest_generation(self, board_id: str) -> str | None:
        return None

    def list_items(self, board_id: str, generation_id: str) -> list[Any]:
        return []


class _ReadModel:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    async def list_consolidation_audit(
        self, context: Any, *, board_id: str, limit: int
    ) -> Sequence[Mapping[str, Any]]:
        self.calls.append(("audit", context))
        return [{"session_id": "s1", "board_id": board_id, "nodes_added": limit}]

    async def list_all_board_ids(
        self, context: Any, *, limit: int = 100
    ) -> Sequence[str]:
        self.calls.append(("boards", context))
        return ["b1"][:limit]

    async def list_pending_entries(
        self, context: Any, *, board_id: str
    ) -> Sequence[Mapping[str, Any]]:
        self.calls.append(("pending", context))
        return [{"id": "q1", "board_id": board_id, "status": "pending"}]

    async def build_pending_tree(
        self, context: Any, *, board_id: str, depth: int = 5
    ) -> Mapping[str, Any]:
        self.calls.append(("tree", context))
        return {"board_id": board_id, "depth": depth, "tree": []}

    async def queue_status_counts(
        self, context: Any, *, board_id: str
    ) -> Mapping[str, int]:
        self.calls.append(("queue_counts", context))
        return {"pending": 1, "claimed": 0, "done": 2, "failed": 0}

    async def kuzu_node_ref_operation_counts(
        self, context: Any, *, board_id: str
    ) -> Mapping[str, int]:
        self.calls.append(("ref_counts", context))
        return {"add": 3, "update": 1, "supersede": 1}

    async def global_outbox_counts(
        self,
        context: Any,
        *,
        board_id: str,
        max_retries: int,
        dead_letter_retry_sentinel: int,
    ) -> KGOutboxCounts:
        self.calls.append(("outbox", context))
        return KGOutboxCounts(pending=0, dead_letter=1, processed=4)

    async def list_canonical_debt_signals(
        self, context: Any, *, board_id: str
    ) -> Sequence[KGCanonicalDebtSignal]:
        self.calls.append(("canonical_debt", context))
        return [
            KGCanonicalDebtSignal(
                artifact_type="card",
                artifact_id="debt-card",
                source_ref="card:debt-card",
                canonical_state="pending",
            )
        ]

    async def list_dead_letter_signals(
        self, context: Any, *, board_id: str
    ) -> Sequence[KGDeadLetterSignal]:
        self.calls.append(("dlq", context))
        return [KGDeadLetterSignal(artifact_type="card", artifact_id="dlq-card")]


class _QueuePort:
    def __init__(self) -> None:
        self.dlq_calls: list[tuple[Any, KGQueueEntrySnapshot, Sequence[Mapping[str, Any]]]] = []
        self.list_calls: list[tuple[Any, str, int]] = []

    async def route_to_dead_letter(
        self,
        context: Any,
        *,
        queue_entry: KGQueueEntrySnapshot,
        errors: Sequence[Mapping[str, Any]],
    ) -> Mapping[str, Any]:
        self.dlq_calls.append((context, queue_entry, errors))
        return {
            "id": "dlq-1",
            "original_queue_id": queue_entry.id,
            "attempts": queue_entry.attempts,
            "errors": list(errors),
        }

    async def list_dead_letter(
        self,
        context: Any,
        *,
        board_id: str,
        limit: int = 100,
    ) -> Sequence[Mapping[str, Any]]:
        self.list_calls.append((context, board_id, limit))
        return [{"id": "dlq-1", "board_id": board_id}]

    async def retry_pending_entry(
        self,
        context: Any,
        *,
        board_id: str,
        queue_entry_id: str,
        recursive: bool = False,
    ) -> Mapping[str, Any] | None:
        return {"board_id": board_id, "id": queue_entry_id, "recursive": recursive}


class _AuditPort:
    def __init__(self) -> None:
        self.events: list[Mapping[str, Any]] = []

    async def emit_outbox_event(
        self,
        context: Any,
        *,
        event_id: str,
        board_id: str,
        session_id: str,
        event_type: str,
        payload: Mapping[str, Any],
    ) -> None:
        self.events.append({
            "context": context,
            "event_id": event_id,
            "board_id": board_id,
            "session_id": session_id,
            "event_type": event_type,
            "payload": payload,
        })

    async def record_audit_event(
        self,
        context: Any,
        *,
        payload: Mapping[str, Any],
    ) -> None:
        self.events.append({"context": context, "payload": payload})


@pytest.fixture(autouse=True)
def _reset_ports(_register_test_kg_operational_read_model_port):
    reset_kg_operational_ports_for_tests()
    yield
    reset_kg_operational_ports_for_tests()


@pytest.mark.asyncio
async def test_missing_kg_operational_read_model_fails_closed() -> None:
    from okto_pulse.core.kg.dashboard_readers import list_all_board_ids

    with pytest.raises(KGOperationalProviderMissing) as exc:
        await list_all_board_ids(object())

    assert exc.value.provider == "read_model"


@pytest.mark.asyncio
async def test_dashboard_readers_delegate_to_registered_read_model() -> None:
    from okto_pulse.core.kg.dashboard_readers import (
        build_pending_tree,
        list_all_board_ids,
        list_consolidation_audit,
        list_pending_entries,
    )

    context = object()
    port = _ReadModel()
    register_kg_operational_ports(read_model=port)

    assert await list_all_board_ids(context) == ["b1"]
    assert (await list_consolidation_audit(context, "b1", limit=7))[0]["nodes_added"] == 7
    assert (await list_pending_entries(context, "b1"))[0]["id"] == "q1"
    assert (await build_pending_tree(context, "b1", depth=2))["depth"] == 2

    assert [name for name, ctx in port.calls if ctx is context] == [
        "boards",
        "audit",
        "pending",
        "tree",
    ]


@pytest.mark.asyncio
async def test_health_checks_use_operational_read_model_counts() -> None:
    from okto_pulse.core.kg.health import (
        check_kuzu_node_refs,
        check_outbox,
        check_queue,
    )

    context = object()
    port = _ReadModel()
    register_kg_operational_ports(read_model=port)

    queue = await check_queue(context, "b1")
    refs = await check_kuzu_node_refs(context, "b1", kuzu_total=2)
    outbox = await check_outbox(context, "b1")

    assert queue.counts["pending"] == 1
    assert queue.healthy is False
    assert refs.counts == {"add": 3, "update": 1, "supersede": 1, "total": 5}
    assert refs.healthy is True
    assert outbox.counts == {"pending": 0, "dead_letter": 1, "processed": 4}
    assert outbox.healthy is False


@pytest.mark.asyncio
async def test_cognitive_readiness_uses_port_signals_for_technical_precedence() -> None:
    from okto_pulse.core.kg.cognitive_readiness import (
        CognitiveReadinessService,
        ReadinessTier,
    )

    port = _ReadModel()
    register_kg_operational_ports(read_model=port)

    verdict = await CognitiveReadinessService(_Store()).evaluate_artifact(
        object(),
        board_id="b1",
        source_ref="card:dlq-card",
    )

    assert verdict.tier == ReadinessTier.TECHNICAL_DLQ.value
    assert verdict.blocking is True


@pytest.mark.asyncio
async def test_cognitive_action_center_gathers_debt_and_dlq_via_port() -> None:
    from okto_pulse.core.kg.cognitive_action_center import (
        CognitiveActionCenterReadModel,
        SIGNAL_DLQ,
        SIGNAL_OPEN_CANONICAL_DEBT,
    )
    from okto_pulse.core.kg.cognitive_readiness import CognitiveReadinessService

    port = _ReadModel()
    register_kg_operational_ports(read_model=port)

    result = await CognitiveActionCenterReadModel(
        CognitiveReadinessService(_Store())
    ).list_signals(object(), board_id="b1")

    signals = {item["signal"] for item in result["items"]}
    assert {SIGNAL_OPEN_CANONICAL_DEBT, SIGNAL_DLQ}.issubset(signals)
    assert result["summary"]["technical_blocking_signals"] == 2


@pytest.mark.asyncio
async def test_dead_letter_helper_delegates_queue_transition_to_port() -> None:
    from types import SimpleNamespace

    from okto_pulse.core.kg.workers.dead_letter import (
        list_dead_letter,
        route_to_dead_letter,
    )

    context = object()
    port = _QueuePort()
    register_kg_operational_ports(worker_queue=port)
    entry = SimpleNamespace(
        id="q1",
        board_id="b1",
        artifact_type="card",
        artifact_id="c1",
        attempts=3,
        last_error="RuntimeError: prior failure",
    )

    result = await route_to_dead_letter(
        context,
        entry,
        error_text="ValueError: final failure",
    )
    rows = await list_dead_letter(context, "b1", limit=5)

    assert result["original_queue_id"] == "q1"
    assert rows == [{"id": "dlq-1", "board_id": "b1"}]
    _, snapshot, errors = port.dlq_calls[0]
    assert snapshot == KGQueueEntrySnapshot(
        id="q1",
        board_id="b1",
        artifact_type="card",
        artifact_id="c1",
        attempts=3,
        last_error="RuntimeError: prior failure",
    )
    assert [item["attempt"] for item in errors] == [1, 2, 3]
    assert errors[-1]["error_type"] == "ValueError"
    assert errors[-1]["message"] == "final failure"
    assert port.list_calls == [(context, "b1", 5)]


@pytest.mark.asyncio
async def test_commit_events_delegate_outbox_write_to_audit_port() -> None:
    from okto_pulse.core.kg.workers.commit_events import (
        EVENT_TYPE_BOARD_CLEARED,
        EVENT_TYPE_SESSION_COMMITTED,
        emit_board_cleared,
        emit_session_committed,
    )

    context = object()
    port = _AuditPort()
    register_kg_operational_ports(worker_audit=port)

    session_event = await emit_session_committed(
        context,
        board_id="b1",
        session_id="s1",
        artifact_type="spec",
        artifact_id="sp1",
        nodes_added=2,
        edges_added=3,
        content_hash="abc",
    )
    clear_event = await emit_board_cleared(
        context,
        board_id="b1",
        reason="reset",
    )

    assert {event["event_id"] for event in port.events} == {
        session_event,
        clear_event,
    }
    assert port.events[0]["event_type"] == EVENT_TYPE_SESSION_COMMITTED
    assert port.events[0]["payload"]["nodes_added"] == 2
    assert port.events[0]["payload"]["edges_added"] == 3
    assert port.events[1]["event_type"] == EVENT_TYPE_BOARD_CLEARED
    assert port.events[1]["payload"]["reason"] == "reset"


def test_migrated_core_kg_reader_modules_do_not_import_sqlalchemy_or_models() -> None:
    root = Path("src/okto_pulse/core/kg")
    targets = [
        root / "dashboard_readers.py",
        root / "health.py",
        root / "cognitive_readiness.py",
        root / "cognitive_action_center.py",
    ]
    for path in targets:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                assert not module.startswith("sqlalchemy"), str(path)
                assert module != "okto_pulse.core.models.db", str(path)
            if isinstance(node, ast.Name):
                assert node.id not in {"AsyncSession", "select", "flag_modified"}, str(path)


def test_migrated_worker_helpers_do_not_import_sqlalchemy_or_models() -> None:
    root = Path("src/okto_pulse/core/kg/workers")
    targets = [
        root / "dead_letter.py",
        root / "commit_events.py",
    ]
    forbidden_names = {
        "AsyncSession",
        "ConsolidationDeadLetter",
        "ConsolidationQueue",
        "GlobalUpdateOutbox",
        "select",
    }
    for path in targets:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                assert not module.startswith("sqlalchemy"), str(path)
                assert module != "okto_pulse.core.models.db", str(path)
            if isinstance(node, ast.Name):
                assert node.id not in forbidden_names, str(path)
