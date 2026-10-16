"""R10B — rebuild ingestion moves behind the Community-owned port."""

from __future__ import annotations

from pathlib import Path


from repository_checkout_testing import community_repo_for

CORE_ROOT = Path(__file__).resolve().parents[1]
COMMUNITY_ROOT = community_repo_for(CORE_ROOT)


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
