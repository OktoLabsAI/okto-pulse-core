from __future__ import annotations

import ast
from pathlib import Path

from okto_pulse.core.kg.interfaces.cognitive_pending_work import (
    CognitivePendingRecordRef,
)
from okto_pulse.core.kg.workers.cognitive_closeout import CognitiveCloseoutWorker


CORE_ROOT = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"


def _calls(path: Path) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, ast.Call)
    ]


def test_af29_core_outbox_flush_has_no_local_artifact_calls() -> None:
    path = CORE_ROOT / "kg" / "global_discovery" / "outbox_worker.py"
    offenders: list[str] = []

    for call in _calls(path):
        func = call.func
        if (
            isinstance(func, ast.Attribute)
            and isinstance(func.value, ast.Name)
            and func.value.id == "os"
            and func.attr == "fsync"
        ):
            offenders.append("os.fsync")
        if isinstance(func, ast.Attribute) and func.attr == "glob":
            offenders.append(".glob")
        if isinstance(func, ast.Attribute) and func.attr == "open":
            first_arg = call.args[0] if call.args else None
            if isinstance(first_arg, ast.Constant) and first_arg.value == "r+b":
                offenders.append(".open('r+b')")

    assert offenders == []


def test_af29_core_closeout_worker_uses_pending_work_provider() -> None:
    path = CORE_ROOT / "kg" / "workers" / "cognitive_closeout.py"
    forbidden_attrs = {"iterdir", "glob"}
    offenders = [
        f".{call.func.attr}"
        for call in _calls(path)
        if isinstance(call.func, ast.Attribute)
        and call.func.attr in forbidden_attrs
    ]

    assert offenders == []


def test_af29_closeout_scan_keeps_policy_in_core_and_discovery_in_port() -> None:
    class _Item:
        def __init__(self, status: str) -> None:
            self.status = status

    class _Store:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []

        def list_items(self, board_id: str, kg_generation_id: str):
            self.calls.append((board_id, kg_generation_id))
            if kg_generation_id == "gen-drainable":
                return [_Item("pending")]
            if kg_generation_id == "gen-terminal":
                return [_Item("consolidated")]
            raise RuntimeError("corrupt-ledger")

    class _Provider:
        def list_records(self):
            return [
                CognitivePendingRecordRef("board-a", "gen-drainable"),
                CognitivePendingRecordRef("board-a", "gen-drainable"),
                CognitivePendingRecordRef("board-a", "gen-terminal"),
                CognitivePendingRecordRef("board-a", "gen-corrupt"),
            ]

    worker = CognitiveCloseoutWorker(
        session_factory=object(),
        store=_Store(),  # type: ignore[arg-type]
        pending_work_provider=_Provider(),
    )

    assert worker._scan_ledger_records() == [("board-a", "gen-drainable")]
