from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import pytest

from kg_registry_testing import configure_test_kg_registry
from okto_pulse.core.kg.interfaces.rebuild_audit_storage import RebuildAuditKey
from memory_rebuild_audit_storage import (
    InMemoryRebuildAuditArtifactStore,
)
from okto_pulse.core.kg.rebuild_generation import (
    PromotionOutcome,
    RebuildAuditKGGenerationRepository,
    generate_kg_generation_id,
)
from sqlalchemy_test_models import Board
from okto_pulse.core.services.kg_health_service import get_kg_health
from repository_checkout_testing import community_source_for


class _UnavailableRebuildAuditStore:
    def write_json_atomic(
        self,
        key: RebuildAuditKey,
        payload: Mapping[str, Any],
    ) -> None:
        raise RuntimeError("store offline")

    def read_json(self, key: RebuildAuditKey) -> dict[str, Any] | None:
        raise RuntimeError("store offline")

    def exists(self, key: RebuildAuditKey) -> bool:
        raise RuntimeError("store offline")

    def list_json(self, prefix: RebuildAuditKey) -> Sequence[dict[str, Any]]:
        raise RuntimeError("store offline")

    def replace_json(
        self,
        key: RebuildAuditKey,
        transform: Callable[[dict[str, Any] | None], dict[str, Any]],
    ) -> dict[str, Any]:
        raise RuntimeError("store offline")


def test_af16_generation_repository_promotes_via_rebuild_audit_store() -> None:
    store = InMemoryRebuildAuditArtifactStore()
    repo = RebuildAuditKGGenerationRepository(artifact_store=store)
    board_id = "board-af16"
    generation_id = generate_kg_generation_id()

    assert repo.get_current(board_id) is None

    result = repo.promote_current(
        board_id=board_id,
        previous_kg_generation_id=None,
        kg_generation_id=generation_id,
        report_ref="rebuild-report:/board-af16/report-1",
        status="completed",
        structural_hash="structural-hash",
        source_hash="source-hash",
        promoted_by="pytest",
        run_id="run-af16",
    )

    assert result.outcome == PromotionOutcome.PROMOTED.value
    assert result.current_kg_generation_id == generation_id
    assert result.history_ref
    assert result.history_ref.startswith("rebuild-audit:/")
    assert repo.get_current(board_id) == generation_id
    history = repo.load_history(board_id, generation_id)
    assert history is not None
    assert history["report_ref"] == "rebuild-report:/board-af16/report-1"
    assert history["run_id"] == "run-af16"


def test_af16_generation_repository_surfaces_unavailable_store() -> None:
    repo = RebuildAuditKGGenerationRepository(
        artifact_store=_UnavailableRebuildAuditStore()
    )

    with pytest.raises(RuntimeError, match="store offline"):
        repo.get_current("board-af16")


async def _ensure_board(db_factory, board_id: str) -> None:
    async with db_factory() as session:
        existing = await session.get(Board, board_id)
        if existing is None:
            session.add(
                Board(
                    id=board_id,
                    name=board_id,
                    owner_id="af16-owner",
                )
            )
            await session.commit()


@pytest.mark.asyncio
async def test_af16_kg_health_reads_current_generation_from_in_memory_store(
    db_factory,
) -> None:
    board_id = "board-af16-health-current"
    await _ensure_board(db_factory, board_id)
    store = InMemoryRebuildAuditArtifactStore()
    configure_test_kg_registry(
        graph_provider="inmemory",
        rebuild_audit_artifact_store=store,
    )
    generation_id = generate_kg_generation_id()

    promotion = RebuildAuditKGGenerationRepository(
        artifact_store=store,
    ).promote_current(
        board_id=board_id,
        previous_kg_generation_id=None,
        kg_generation_id=generation_id,
        report_ref="rebuild-report:/board-af16-health-current/report-1",
        status="completed",
        structural_hash="structural-hash",
        source_hash="source-hash",
        promoted_by="pytest",
        run_id="run-af16-health-current",
    )
    assert promotion.outcome == PromotionOutcome.PROMOTED.value

    async with db_factory() as session:
        result = await get_kg_health(board_id, session)

    assert result["current_kg_generation_id"] == generation_id
    assert not any(
        issue["code"] == "rebuild_audit_artifact_store_unavailable"
        for issue in result["health_issues"]
    )


@pytest.mark.asyncio
async def test_af16_kg_health_surfaces_unavailable_generation_store(
    db_factory,
) -> None:
    board_id = "board-af16-health-store-offline"
    await _ensure_board(db_factory, board_id)
    configure_test_kg_registry(
        graph_provider="inmemory",
        rebuild_audit_artifact_store=_UnavailableRebuildAuditStore(),
    )

    async with db_factory() as session:
        result = await get_kg_health(board_id, session)

    assert result["current_kg_generation_id"] is None
    assert result["metric_status"] == "unavailable"
    issue = next(
        issue
        for issue in result["health_issues"]
        if issue["code"] == "rebuild_audit_artifact_store_unavailable"
    )
    assert issue["component"] == "kg_generation_store"
    assert issue["reason"] == "current_generation_store_unavailable"
    assert issue["operator_action"] == "inspect_runtime_provider"


def test_af16_kg_health_current_generation_does_not_recreate_tempdir() -> None:
    source = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "services"
        / "kg_health_service.py"
    ).read_text(encoding="utf-8")

    assert 'Path(tempfile.gettempdir()) / "okto_pulse_kg_rebuild"' not in source
    assert "RebuildAuditKGGenerationRepository" in source
    assert "require_rebuild_audit_artifact_store" in source


def test_af16_rest_mcp_health_wire_generation_through_artifact_store() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    core_root = repo_root / "src" / "okto_pulse" / "core"
    community_root = community_source_for(repo_root)
    migrated_sources = [
        community_root / "api" / "kg_rebuild.py",
        core_root / "mcp" / "server.py",
        core_root / "services" / "kg_health_service.py",
    ]

    for path in migrated_sources:
        source = path.read_text(encoding="utf-8")
        assert "RebuildAuditKGGenerationRepository" in source
        assert "require_rebuild_audit_artifact_store" in source
        assert "KGGenerationRepository(base_dir=_REBUILD_BASE_DIR)" not in source
        assert "KGGenerationRepository(\n            base_dir=rebuild_base" not in source
