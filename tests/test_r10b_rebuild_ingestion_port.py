"""R10B — rebuild ingestion moves behind the Community-owned port."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest

from kg_registry_testing import configure_test_kg_registry
from okto_pulse.core.composition import RuntimeProviderMissing
from okto_pulse.core.kg.rebuild_service import RebuildStepResult
from repository_checkout_testing import community_repo_for

CORE_ROOT = Path(__file__).resolve().parents[1]
COMMUNITY_ROOT = community_repo_for(CORE_ROOT)


def test_rest_and_mcp_consumers_resolve_ingestion_through_registry() -> None:
    rest = (COMMUNITY_ROOT / "src/okto_pulse/community/api/kg_rebuild.py").read_text(
        encoding="utf-8"
    )
    mcp = (CORE_ROOT / "src/okto_pulse/core/mcp/server.py").read_text(encoding="utf-8")
    recovery = (
        COMMUNITY_ROOT / "src/okto_pulse/community/kg_recovery_only.py"
    ).read_text(encoding="utf-8")

    for src in (rest, mcp):
        assert "BoardRebuildIngestionAdapter" not in src
        assert "_resolve_pulse_db_path" not in src
        assert "sqlite3.connect" not in src
    assert "build_rebuild_step_adapter" in recovery
    assert "require_rebuild_ingestion_port" not in rest


def test_build_rebuild_step_adapter_fails_closed_without_provider() -> None:
    from okto_pulse.core.application.kg_rebuild import build_rebuild_step_adapter

    configure_test_kg_registry(rebuild_ingestion_port=None)

    with pytest.raises(RuntimeProviderMissing) as exc_info:
        build_rebuild_step_adapter(manifest_store_obj=object())

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
        return SimpleNamespace(
            materializable_sources=(_Row("spec", "s1"),),
            skipped_expired_working=(),
            created_at="2026-08-15T00:00:00+00:00",
        )


class _RecordingRebuildIngestionPort:
    def __init__(self) -> None:
        self.sources: tuple[dict[str, str], ...] = ()

    def build_step_adapter(self, source_resolver):
        def _step(req):
            self.sources = tuple(source_resolver(req))
            return RebuildStepResult(
                ok=True, counts={"seen_sources": len(self.sources)}
            )

        return _step


def test_build_rebuild_step_adapter_uses_registered_provider() -> None:
    from okto_pulse.core.application.kg_rebuild import build_rebuild_step_adapter

    provider = _RecordingRebuildIngestionPort()
    configure_test_kg_registry(rebuild_ingestion_port=provider)

    step = build_rebuild_step_adapter(manifest_store_obj=_ManifestStore())
    result = step(
        SimpleNamespace(
            manifest_ref="manifest-1",
            rebaseline_source_rows=None,
            rebaseline_evidence_id=None,
            rebaseline_target_source_set_hash=None,
        )
    )

    assert result.ok is True
    assert result.counts == {"seen_sources": 1}
    assert provider.sources == (
        {
            "artifact_type": "spec",
            "id": "s1",
            "_rebuild_manifest_created_at": "2026-08-15T00:00:00+00:00",
        },
    )


def test_build_rebuild_step_adapter_uses_proved_rebaseline_projection() -> None:
    from okto_pulse.core.application.kg_rebuild import build_rebuild_step_adapter

    class _LegacyManifestStore:
        def load(self, _manifest_ref: str):
            pytest.fail("proved rebaseline must not reload legacy manifest rows")

    provider = _RecordingRebuildIngestionPort()
    configure_test_kg_registry(rebuild_ingestion_port=provider)
    evidence_id = "run_legacy:rebuild_manifest_legacy"
    projected = {
        "artifact_type": "spec",
        "id": "s1",
        "content_hash": "current-v3-hash",
        "_rebuild_manifest_created_at": "2026-08-15T00:00:00+00:00",
        "_rebuild_rebaseline_evidence_id": evidence_id,
    }

    step = build_rebuild_step_adapter(manifest_store_obj=_LegacyManifestStore())
    result = step(
        SimpleNamespace(
            manifest_ref="rebuild_manifest_legacy",
            rebaseline_source_rows=(projected,),
            rebaseline_evidence_id=evidence_id,
            rebaseline_target_source_set_hash="f" * 64,
        )
    )

    assert result.ok is True
    assert provider.sources == (projected,)


def test_community_composition_registers_real_rebuild_ingestion_provider(
    tmp_path: Path,
) -> None:
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
