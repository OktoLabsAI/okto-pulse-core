"""Fixes da investigação 2026-06-10 — AFG inoperante em produção.

Três furos: (1) findings só nasciam em saves pós-feature (83% dos designs
sem run; tabela de findings vazia; gate nunca bloqueou) → backfill;
(2) diagramas com adapter_payload_ref crus eram pulados pelo engine no
update/import → re-hidratação sempre; (3) spec→done não passava pelo
finding gate → validate_or_raise_architecture_findings.
"""

from __future__ import annotations

import uuid

import pytest

from sqlalchemy import select

from okto_pulse.core.models.db import (
    ArchitectureDesign,
    ArchitectureFindingRun,
    Board,
    Spec,
    SpecStatus,
)
from okto_pulse.core.services.architecture import (
    ArchitectureDesignRepository,
    ArchitectureFindingRunStore,
    backfill_architecture_finding_runs,
)
from okto_pulse.core.services.resource_gate import (
    ResourceGateService,
    ResourceGateViolation,
)

USER = "afg-fix-user"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _design_payload_with_orphan_entity() -> dict:
    """Entities declaradas sem representação em diagrama → 1+ structured
    warning (entity_without_diagram) garantido pelo TopologyWarningEngine."""
    return {
        "title": "Runtime boundary",
        "global_description": "Validates the AFG backfill and gates.",
        "entities": [
            {
                "id": "ent-orphan",
                "name": "Orphan Service",
                "entity_type": "service",
                "description": "Declared but not drawn.",
            }
        ],
        "interfaces": [],
        "diagrams": [],
    }


async def _seed_spec_with_design(db_factory) -> tuple[str, str, str]:
    board_id, spec_id, design_id = _id("afg-board"), _id("afg-spec"), _id("afg-design")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="AFG Fix Board", owner_id=USER))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="AFG Spec",
                status=SpecStatus.IN_PROGRESS,
                version=1,
                created_by=USER,
            )
        )
        payload = _design_payload_with_orphan_entity()
        db.add(
            ArchitectureDesign(
                id=design_id,
                board_id=board_id,
                parent_type="spec",
                spec_id=spec_id,
                title=payload["title"],
                global_description=payload["global_description"],
                entities=payload["entities"],
                interfaces=payload["interfaces"],
                diagrams=payload["diagrams"],
                created_by=USER,
            )
        )
        await db.commit()
    return board_id, spec_id, design_id


@pytest.mark.asyncio
async def test_backfill_materializes_findings_for_designs_without_runs(db_factory):
    board_id, _spec_id, design_id = await _seed_spec_with_design(db_factory)

    async with db_factory() as db:
        stats = await backfill_architecture_finding_runs(db, board_id=board_id)

    assert stats["designs"] == 1
    assert stats["with_findings"] == 1
    assert stats["findings"] >= 1

    async with db_factory() as db:
        run = (
            await db.execute(
                select(ArchitectureFindingRun).where(
                    ArchitectureFindingRun.design_id == design_id
                )
            )
        ).scalars().first()
        assert run is not None
        assert run.active_count >= 1
        assert run.actor_id == "architecture-finding-backfill"

    # only_missing pula designs que já têm run (sweep de boot barato).
    async with db_factory() as db:
        again = await backfill_architecture_finding_runs(
            db, board_id=board_id, only_missing=True
        )
    assert again["designs"] == 0
    assert again["skipped"] == 1


@pytest.mark.asyncio
async def test_spec_done_architecture_findings_gate_blocks_and_releases(db_factory):
    board_id, spec_id, design_id = await _seed_spec_with_design(db_factory)
    async with db_factory() as db:
        await backfill_architecture_finding_runs(db, board_id=board_id)

    # Com finding ativo: a transição de spec para done deve ser bloqueada.
    async with db_factory() as db:
        gate = ResourceGateService(db)
        with pytest.raises(ResourceGateViolation) as exc_info:
            await gate.validate_or_raise_architecture_findings(
                board_id, "spec", spec_id, phase="spec_done"
            )
    assert exc_info.value.code == "architecture_findings_block_done"

    # Resolve: novo run sem warnings (entidade agora coberta) → gate libera.
    async with db_factory() as db:
        store = ArchitectureFindingRunStore(db)
        await store.upsert_latest_run(
            board_id=board_id,
            design_id=design_id,
            design_version=2,
            critic_run_id=f"archcrit:{design_id}:v2:{uuid.uuid4().hex[:8]}",
            actor={"actor_type": "user", "actor_id": USER, "actor_name": USER},
            validator_summary={"valid": True, "issues": [], "warnings_count": 0},
            structured_warnings=[],
        )
        await db.commit()

    async with db_factory() as db:
        gate = ResourceGateService(db)
        summary = await gate.validate_or_raise_architecture_findings(
            board_id, "spec", spec_id, phase="spec_done"
        )
    assert summary["architecture_findings"]["active_count"] == 0


@pytest.mark.asyncio
async def test_update_with_externalized_diagram_refs_rehydrates_for_critique(
    db_factory, monkeypatch
):
    """Patch com diagrams carregando adapter_payload_ref (sem inline) era
    avaliado com os diagramas PULADOS — falsos entity_without_diagram. Com a
    re-hidratação, o critique enxerga o payload externo que cobre a entity."""
    board_id, _spec_id, design_id = await _seed_spec_with_design(db_factory)

    covering_payload = {
        "type": "excalidraw",
        "elements": [
            {
                "id": "el-1",
                "type": "rectangle",
                "customData": {
                    "linkedEntityId": "ent-orphan",
                    "text": "Orphan Service",
                    "displayType": "service",
                    "architectureKind": "service",
                    "iconName": "server",
                },
            }
        ],
        "appState": {},
    }

    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        payload_row = await repo.diagram_store.save_payload(
            board_id=board_id,
            design_id=design_id,
            diagram_id="diag-1",
            format="excalidraw_json",
            payload=covering_payload,
        )
        await db.commit()
        ref = payload_row.id

    raw_diagrams = [
        {
            "id": "diag-1",
            "title": "Runtime",
            "diagram_type": "runtime",
            "format": "excalidraw_json",
            "adapter_payload_ref": ref,
        }
    ]

    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        captured: dict = {}
        original = repo.critique_payload

        def spy(payload):
            captured["diagrams"] = payload.get("diagrams")
            return original(payload)

        monkeypatch.setattr(repo, "critique_payload", spy)
        from okto_pulse.core.services.architecture import (
            ArchitectureWarningAcknowledgementRequired,
        )

        # O save pode exigir acknowledgement para warnings REAIS do payload
        # re-hidratado (ex.: nó isolado) — isso é o engine ENXERGANDO o
        # diagrama, exatamente o comportamento corrigido.
        ack_warnings: list[dict] = []
        try:
            await repo.update(
                design_id,
                {"diagrams": raw_diagrams, "change_summary": "attach diagram"},
                USER,
            )
            await db.commit()
        except ArchitectureWarningAcknowledgementRequired as exc:
            ack_warnings = list(exc.warnings or [])

    seen = captured["diagrams"]
    assert seen and isinstance(seen[0].get("adapter_payload"), dict), (
        "critique recebeu o diagrama CRU (sem adapter_payload) — engine o "
        "pularia e geraria falsos entity_without_diagram"
    )
    # A entity está coberta pelo payload externo: nenhum falso
    # entity_without_diagram pode aparecer (era o sintoma do bug).
    codes = {w.get("code") for w in ack_warnings}
    assert "entity_without_diagram" not in codes, (
        f"payload externo foi ignorado — falso entity_without_diagram: {codes}"
    )
