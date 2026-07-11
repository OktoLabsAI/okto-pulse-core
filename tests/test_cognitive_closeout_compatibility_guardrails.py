"""Compatibility guardrails for superseded cognitive closeout artifacts."""

from __future__ import annotations

from pathlib import Path

from okto_pulse.core.kg.cognitive_closeout_gate import (
    build_default_cognitive_closeout_gate,
)
from okto_pulse.core.kg.rebuild_audit import CognitiveConsolidationItemStore
from okto_pulse.core.mcp.kg_tools import register_kg_tools
from sqlalchemy_test_models import Spec
from okto_pulse.core.models.schemas import BoardSettings, SpecCreate, SpecUpdate


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src" / "okto_pulse" / "core"


def _source_texts() -> list[tuple[Path, str]]:
    ignored_parts = {"__pycache__"}
    texts: list[tuple[Path, str]] = []
    for path in SRC_ROOT.rglob("*.py"):
        if ignored_parts & set(path.parts):
            continue
        texts.append((path, path.read_text(encoding="utf-8")))
    return texts


def test_no_sql_cognitive_consolidation_state_table_is_introduced():
    offenders = [
        str(path.relative_to(REPO_ROOT))
        for path, text in _source_texts()
        if "cognitive_consolidation_state" in text
    ]

    assert offenders == []


def test_skip_cognitive_consolidation_is_board_only_not_spec_level():
    assert "skip_cognitive_consolidation" in BoardSettings.model_fields
    assert "skip_cognitive_consolidation" not in SpecCreate.model_fields
    assert "skip_cognitive_consolidation" not in SpecUpdate.model_fields
    assert not hasattr(Spec, "skip_cognitive_consolidation")


def test_mcp_list_cognitive_pending_items_name_remains_registered():
    class _MCPRegistryDouble:
        def __init__(self) -> None:
            self.tools = {}

        def tool(self):
            def _decorator(fn):
                self.tools[fn.__name__] = fn
                return fn

            return _decorator

    class _NullDb:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc):
            return False

    async def _agent():
        return object()

    mcp = _MCPRegistryDouble()
    register_kg_tools(mcp, get_agent=_agent, get_uow=lambda: _NullDb())

    assert "okto_pulse_kg_list_cognitive_pending_items" in mcp.tools
    assert "okto_pulse_kg_list_cognitive_closeout_items" not in mcp.tools


def test_default_gate_uses_file_backed_cognitive_item_store():
    gate = build_default_cognitive_closeout_gate()

    assert isinstance(gate.store, CognitiveConsolidationItemStore)
