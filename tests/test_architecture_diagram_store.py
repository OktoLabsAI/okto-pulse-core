"""Tests for the ArchitectureDiagramStore database adapter."""

from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.models.db import ArchitectureDesign, Board, Ideation
from okto_pulse.core.services.architecture import ArchitectureDiagramStore


USER_ID = "architecture-store-user"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


async def _seed_design(db_factory) -> tuple[str, str]:
    board_id = _id("architecture-store-board")
    ideation_id = _id("architecture-store-ideation")
    design_id = _id("architecture-store-design")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Architecture Store Board", owner_id=USER_ID))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Architecture Store Ideation",
                created_by=USER_ID,
            )
        )
        db.add(
            ArchitectureDesign(
                id=design_id,
                board_id=board_id,
                parent_type="ideation",
                ideation_id=ideation_id,
                title="Architecture Store Design",
                global_description="Stores diagram payloads behind adapter refs.",
                entities=[],
                interfaces=[],
                diagrams=[],
                created_by=USER_ID,
            )
        )
        await db.commit()
    return board_id, design_id


@pytest.mark.asyncio
async def test_store_save_load_stat_update_and_delete_payload(db_factory):
    board_id, design_id = await _seed_design(db_factory)
    async with db_factory() as db:
        store = ArchitectureDiagramStore(db)

        row = await store.save_payload(
            board_id=board_id,
            design_id=design_id,
            diagram_id="diagram-one",
            format="mermaid",
            payload="graph TD; A-->B",
        )
        await db.commit()

        assert await store.exists(row.id) is True
        assert await store.load_payload(row.id) == "graph TD; A-->B"

        stat = await store.stat(row.storage_key)
        assert stat["format"] == "mermaid"
        assert stat["storage_backend"] == "database"
        assert stat["size_bytes"] > 0

        updated = await store.save_payload(
            board_id=board_id,
            design_id=design_id,
            diagram_id="diagram-one",
            format="mermaid",
            payload="graph TD; A-->C",
        )
        await db.commit()

        assert updated.id == row.id
        assert await store.load_payload(row.id) == "graph TD; A-->C"

        await store.delete_payload(row.id)
        await db.commit()
        assert await store.exists(row.id) is False


@pytest.mark.asyncio
async def test_store_copies_payload_to_another_diagram_ref(db_factory):
    board_id, design_id = await _seed_design(db_factory)
    async with db_factory() as db:
        store = ArchitectureDiagramStore(db)
        row = await store.save_payload(
            board_id=board_id,
            design_id=design_id,
            diagram_id="source-diagram",
            format="raw",
            payload={"nodes": [{"id": "n1"}]},
        )
        copied = await store.copy_payload(row.id, design_id, "target-diagram")
        await db.commit()

        assert copied.id != row.id
        assert copied.diagram_id == "target-diagram"
        assert copied.content_hash == row.content_hash
        assert await store.load_payload(copied.id) == {"nodes": [{"id": "n1"}]}

