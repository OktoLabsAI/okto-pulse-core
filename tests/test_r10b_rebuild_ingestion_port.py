"""R10B — rebuild ingestion moves behind the Community-owned port."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest

from kg_registry_testing import configure_test_kg_registry
from okto_pulse.core.composition import RuntimeProviderMissing
from okto_pulse.core.kg.rebuild_service import RebuildStepResult

CORE_ROOT = Path(__file__).resolve().parents[1]
COMMUNITY_ROOT = CORE_ROOT.parents[0] / "okto_labs_pulse_community"


def test_rest_and_mcp_consumers_resolve_ingestion_through_registry() -> None:
    rest = (CORE_ROOT / "src/okto_pulse/core/api/kg_rebuild.py").read_text(
        encoding="utf-8"
    )
    mcp = (CORE_ROOT / "src/okto_pulse/core/mcp/server.py").read_text(
        encoding="utf-8"
    )

    for src in (rest, mcp):
        assert "BoardRebuildIngestionAdapter" not in src
        assert "_resolve_pulse_db_path" not in src
        assert "sqlite3.connect" not in src
        assert "require_rebuild_ingestion_port" in src or "_build_rebuild_step_adapter" in src


def test_build_rebuild_step_adapter_fails_closed_without_provider() -> None:
    from okto_pulse.core.api.kg_rebuild import _build_rebuild_step_adapter

    configure_test_kg_registry(rebuild_ingestion_port=None)

    with pytest.raises(RuntimeProviderMissing) as exc_info:
        _build_rebuild_step_adapter(manifest_store_obj=object())

    assert exc_info.value.provider_key == "rebuild_ingestion_port"


@dataclass(frozen=True)
class _Row:
    artifact_type: str
    id: str

    def to_dict(self) -> dict[str, str]:
        return {"artifact_type": self.artifact_type, "id": self.id}


class _ManifestStore:
    def load(self, manifest_ref: str):
        assert manifest_ref == "manifest-1"
        return SimpleNamespace(materializable_sources=(_Row("spec", "s1"),))


class _RecordingRebuildIngestionPort:
    def __init__(self) -> None:
        self.sources: tuple[dict[str, str], ...] = ()

    def build_step_adapter(self, source_resolver):
        def _step(req):
            self.sources = tuple(source_resolver(req))
            return RebuildStepResult(ok=True, counts={"seen_sources": len(self.sources)})

        return _step


def test_build_rebuild_step_adapter_uses_registered_provider() -> None:
    from okto_pulse.core.api.kg_rebuild import _build_rebuild_step_adapter

    provider = _RecordingRebuildIngestionPort()
    configure_test_kg_registry(rebuild_ingestion_port=provider)

    step = _build_rebuild_step_adapter(manifest_store_obj=_ManifestStore())
    result = step(SimpleNamespace(manifest_ref="manifest-1"))

    assert result.ok is True
    assert result.counts == {"seen_sources": 1}
    assert provider.sources == ({"artifact_type": "spec", "id": "s1"},)


def test_community_composition_registers_real_rebuild_ingestion_provider(tmp_path: Path) -> None:
    from okto_pulse.community.adapters.board_rebuild_ingestion import (
        CommunityBoardRebuildIngestionAdapter,
    )
    from okto_pulse.community.adapters.composition import build_community_kg_composition

    composition = build_community_kg_composition(
        upload_dir=str(tmp_path),
        include_graph=False,
    )

    assert isinstance(
        composition.base_registry.rebuild_ingestion_port,
        CommunityBoardRebuildIngestionAdapter,
    )


def test_community_entrypoints_share_configure_community_kg_registry() -> None:
    for rel in (
        "src/okto_pulse/community/main.py",
        "src/okto_pulse/community/cli.py",
        "src/okto_pulse/community/seed.py",
    ):
        src = (COMMUNITY_ROOT / rel).read_text(encoding="utf-8")
        assert "configure_community_kg_registry" in src, rel
