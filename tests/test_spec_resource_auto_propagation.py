from __future__ import annotations

import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy import select

from sqlalchemy_test_models import (
    ActivityLog,
    ArchitectureDesign,
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecKnowledgeBase,
    SpecStatus,
)
from okto_pulse.core.models.schemas import (
    ArchitectureDesignCreate,
    ArchitectureDesignUpdate,
    ArchitectureWarningAcknowledgementRequest,
    BoardSettings,
    BoardUpdate,
    CardCreate,
    SpecCreate,
    SpecKnowledgeCreate,
    SpecKnowledgeUpdate,
    SpecMove,
    SpecUpdate,
)
from okto_pulse.core.ports.knowledge_propagation import (
    KnowledgePropagationScope,
    register_knowledge_propagation_port,
    reset_knowledge_propagation_port_for_tests,
)
from okto_pulse.core.services.architecture import ArchitectureDesignRepository
from okto_pulse.core.services.main import (
    BoardService,
    CardService,
    SpecKnowledgeService,
    SpecService,
)
from okto_pulse.core.services.spec_resource_propagation import SpecResourcePropagationService


class _LegacyKnowledgeScopePort:
    async def load_scope(self, _context, request):
        return KnowledgePropagationScope(
            target=request.target,
            scope_revision=0,
            v2_active=False,
            selection_state=None,
        )


@pytest.fixture(autouse=True)
def _register_legacy_knowledge_scope_port():
    register_knowledge_propagation_port(_LegacyKnowledgeScopePort())
    try:
        yield
    finally:
        reset_knowledge_propagation_port_for_tests()


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _settings(*resource_types: str) -> dict:
    return {
        "auto_derive_spec_resources_enabled": True,
        "auto_derive_spec_resource_types": list(resource_types),
    }


def _governance_metadata(*, purpose: str = "Explain the source contract") -> dict:
    return {
        "contract_version": 1,
        "authority": "advisory",
        "classification": "technical_reference",
        "purpose": purpose,
        "audience": ["agent"],
        "relevance_reason": "Required by the linked implementation",
        "provenance": [{"kind": "code", "reference": "repo:core@abc123"}],
        "as_of": "2026-07-22T20:00:00-03:00",
        "version_ref": "commit:abc123",
        "version_not_applicable_reason": None,
        "scope": "Knowledge Base propagation",
        "limitations": "Advisory evidence only",
        "stable_references": [],
        "lifecycle_state": "current",
        "superseded_by": None,
        "superseded_reason": None,
        "exclusive_authority_check": "passed",
        "normative_destinations": [],
    }


async def _seed_spec_with_resources(db, *, board_id: str, actor_id: str) -> dict[str, str]:
    spec_id = _id("spec-auto-resources")
    mockup_id = _id("mock-primary")
    kb_id = _id("kb-primary")
    architecture_id = _id("arch-primary")
    db.add(
        Spec(
            id=spec_id,
            board_id=board_id,
            title="Spec with resources",
            status=SpecStatus.APPROVED,
            created_by=actor_id,
            functional_requirements=["FR"],
            acceptance_criteria=["AC"],
            test_scenarios=[],
            business_rules=[],
            api_contracts=[],
            technical_requirements=[],
            decisions=[],
            screen_mockups=[
                {
                    "id": mockup_id,
                    "title": "Primary mockup",
                    "description": "Expected screen",
                    "screen_type": "form",
                    "html_content": "<div>Mockup</div>",
                }
            ],
        )
    )
    db.add(
        SpecKnowledgeBase(
            id=kb_id,
            spec_id=spec_id,
            title="Spec API contract",
            description="Payload contract",
            content="The card needs this contract.",
            mime_type="text/markdown",
            created_by=actor_id,
        )
    )
    db.add(
        ArchitectureDesign(
            id=architecture_id,
            board_id=board_id,
            parent_type="spec",
            spec_id=spec_id,
            title="Spec architecture",
            global_description="Architecture context for implementation.",
            entities=[],
            interfaces=[],
            diagrams=[],
            created_by=actor_id,
        )
    )
    return {
        "spec_id": spec_id,
        "mockup_id": mockup_id,
        "knowledge_id": kb_id,
        "architecture_id": architecture_id,
    }


@pytest.mark.asyncio
async def test_board_settings_require_selected_resource_types_when_enabled():
    with pytest.raises(ValidationError, match="auto_derive_spec_resource_types"):
        BoardSettings(
            auto_derive_spec_resources_enabled=True,
            auto_derive_spec_resource_types=[],
        )

    settings = BoardSettings(
        auto_derive_spec_resources_enabled=True,
        auto_derive_spec_resource_types=["knowledge_base", "knowledge_base"],
    )
    assert [item.value for item in settings.auto_derive_spec_resource_types] == ["knowledge_base"]


@pytest.mark.asyncio
async def test_update_board_serializes_resource_type_enums_to_json(db_factory):
    board_id = _id("board-settings-json")
    actor_id = _id("agent-settings-json")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Settings JSON", owner_id=actor_id))
        await db.flush()

        updated = await BoardService(db).update_board(
            board_id,
            actor_id,
            BoardUpdate(
                settings=BoardSettings(
                    auto_derive_spec_resources_enabled=True,
                    auto_derive_spec_resource_types=["knowledge_base", "mockup"],
                )
            ),
        )

        assert updated is not None
        assert updated.settings["auto_derive_spec_resource_types"] == [
            "knowledge_base",
            "mockup",
        ]


@pytest.mark.asyncio
async def test_create_card_auto_propagates_selected_spec_resources_idempotently(db_factory):
    board_id = _id("board-auto-resources")
    actor_id = _id("agent-auto-resources")

    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Auto resource board",
                owner_id=actor_id,
                settings=_settings("knowledge_base", "mockup", "architecture"),
            )
        )
        ids = await _seed_spec_with_resources(db, board_id=board_id, actor_id=actor_id)
        spec_id = ids["spec_id"]
        await db.flush()

        card = await CardService(db).create_card(
            board_id,
            actor_id,
            CardCreate(title="Implementation card", spec_id=spec_id),
        )
        assert card is not None

        assert [item["id"] for item in card.knowledge_bases or []] == [f"cardkb_{ids['knowledge_id']}"]
        assert "governance_metadata" not in card.knowledge_bases[0]
        assert [item["id"] for item in card.screen_mockups or []] == [ids["mockup_id"]]

        arch_count = (
            await db.execute(
                select(ArchitectureDesign).where(ArchitectureDesign.card_id == card.id)
            )
        ).scalars().all()
        assert len(arch_count) == 1

        retry = await SpecResourcePropagationService(db).propagate_for_card(
            board_id=board_id,
            spec_id=spec_id,
            card_id=card.id,
            actor_id=actor_id,
            trigger="test_retry",
        )
        assert retry["results"]["knowledge_base"]["copied_count"] == 0
        assert retry["results"]["knowledge_base"]["ignored_count"] == 1
        assert retry["results"]["mockup"]["copied_count"] == 0
        assert retry["results"]["mockup"]["ignored_count"] == 1
        assert retry["results"]["architecture"]["copied_count"] == 0
        assert retry["results"]["architecture"]["ignored_count"] == 1

        refreshed = await db.get(Card, card.id)
        assert len(refreshed.knowledge_bases or []) == 1
        assert len(refreshed.screen_mockups or []) == 1
        arch_after_retry = (
            await db.execute(
                select(ArchitectureDesign).where(ArchitectureDesign.card_id == card.id)
            )
        ).scalars().all()
        assert len(arch_after_retry) == 1

        audits = (
            await db.execute(
                select(ActivityLog).where(
                    ActivityLog.board_id == board_id,
                    ActivityLog.card_id == card.id,
                    ActivityLog.action == "spec_resources_auto_propagated",
                )
            )
        ).scalars().all()
        assert audits
        assert audits[0].details["resource_types"] == [
            "knowledge_base",
            "mockup",
            "architecture",
        ]


@pytest.mark.asyncio
async def test_enabling_board_setting_backfills_existing_linked_cards(db_factory):
    board_id = _id("board-backfill-resources")
    actor_id = _id("agent-backfill-resources")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Backfill board", owner_id=actor_id))
        ids = await _seed_spec_with_resources(db, board_id=board_id, actor_id=actor_id)
        spec_id = ids["spec_id"]
        await db.flush()

        card = await CardService(db).create_card(
            board_id,
            actor_id,
            CardCreate(title="Existing implementation card", spec_id=spec_id),
        )
        assert card.knowledge_bases in (None, [])
        assert card.screen_mockups in (None, [])

        await BoardService(db).update_board(
            board_id,
            actor_id,
            BoardUpdate(
                settings=BoardSettings(
                    auto_derive_spec_resources_enabled=True,
                    auto_derive_spec_resource_types=[
                        "knowledge_base",
                        "mockup",
                        "architecture",
                    ],
                )
            ),
        )

        refreshed = await db.get(Card, card.id)
        assert [item["id"] for item in refreshed.knowledge_bases or []] == [
            f"cardkb_{ids['knowledge_id']}"
        ]
        assert [item["id"] for item in refreshed.screen_mockups or []] == [
            ids["mockup_id"]
        ]
        copied_architecture = (
            await db.execute(
                select(ArchitectureDesign).where(ArchitectureDesign.card_id == card.id)
            )
        ).scalars().all()
        assert len(copied_architecture) == 1


@pytest.mark.asyncio
async def test_create_spec_persists_mockups_for_later_auto_backfill(db_factory):
    board_id = _id("board-create-spec-resources")
    actor_id = _id("agent-create-spec-resources")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Create spec resources", owner_id=actor_id))
        await db.flush()

        spec = await SpecService(db).create_spec(
            board_id,
            actor_id,
            SpecCreate(
                title="Spec created with resources",
                delivery_context="brownfield",
                status=SpecStatus.APPROVED,
                screen_mockups=[
                    {
                        "id": "created-mockup",
                        "title": "Created mockup",
                        "screen_type": "page",
                        "html_content": "<div>Created</div>",
                    }
                ],
                business_rules=[
                    {
                        "id": "created-br",
                        "title": "Created BR",
                        "rule": "Persist rule",
                        "when": "Spec is created",
                        "then": "Rule is available",
                    }
                ],
                api_contracts=[
                    {
                        "id": "created-api",
                        "method": "GET",
                        "path": "/created",
                        "description": "Persist contract",
                    }
                ],
            ),
        )
        assert spec.screen_mockups[0]["id"] == "created-mockup"
        assert spec.business_rules[0]["id"] == "created-br"
        assert spec.api_contracts[0]["id"] == "created-api"

        card = await CardService(db).create_card(
            board_id,
            actor_id,
            CardCreate(title="Existing task", spec_id=spec.id),
        )
        assert card.screen_mockups in (None, [])

        await BoardService(db).update_board(
            board_id,
            actor_id,
            BoardUpdate(
                settings=BoardSettings(
                    auto_derive_spec_resources_enabled=True,
                    auto_derive_spec_resource_types=["mockup"],
                )
            ),
        )

        refreshed = await db.get(Card, card.id)
        assert [item["id"] for item in refreshed.screen_mockups or []] == [
            "created-mockup"
        ]


@pytest.mark.asyncio
async def test_spec_resource_changes_backfill_existing_linked_cards(db_factory):
    board_id = _id("board-resource-change")
    actor_id = _id("agent-resource-change")
    spec_id = _id("spec-resource-change")

    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Resource change board",
                owner_id=actor_id,
                settings=_settings("knowledge_base", "mockup"),
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Spec receives resources later",
                status=SpecStatus.APPROVED,
                created_by=actor_id,
                functional_requirements=[],
                acceptance_criteria=[],
                test_scenarios=[],
                business_rules=[],
                api_contracts=[],
            )
        )
        await db.flush()

        card = await CardService(db).create_card(
            board_id,
            actor_id,
            CardCreate(title="Linked before resources", spec_id=spec_id),
        )
        assert card.knowledge_bases in (None, [])
        assert card.screen_mockups in (None, [])

        await SpecService(db).move_spec(
            spec_id,
            actor_id,
            SpecMove(status=SpecStatus.DRAFT),
        )

        await SpecKnowledgeService(db).create_knowledge(
            spec_id,
            actor_id,
            SpecKnowledgeCreate(
                title="Late KB",
                description="Created after cards exist",
                content="Use this in linked tasks.",
            ),
        )
        await SpecService(db).update_spec(
            spec_id,
            actor_id,
            SpecUpdate(
                screen_mockups=[
                    {
                        "id": "late-mockup",
                        "title": "Late mockup",
                        "screen_type": "form",
                        "html_content": "<div>Late</div>",
                    }
                ]
            ),
        )

        refreshed = await db.get(Card, card.id)
        assert len(refreshed.knowledge_bases or []) == 1
        assert refreshed.knowledge_bases[0]["title"] == "Late KB"
        assert [item["id"] for item in refreshed.screen_mockups or []] == [
            "late-mockup"
        ]


@pytest.mark.asyncio
async def test_governance_metadata_refreshes_once_without_changing_card_identity(
    db_factory,
):
    board_id = _id("board-governance-refresh")
    actor_id = _id("agent-governance-refresh")
    spec_id = _id("spec-governance-refresh")

    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Governance refresh board",
                owner_id=actor_id,
                settings=_settings("knowledge_base"),
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Governed source spec",
                status=SpecStatus.APPROVED,
                created_by=actor_id,
                functional_requirements=[],
                acceptance_criteria=[],
                test_scenarios=[],
                business_rules=[],
                api_contracts=[],
            )
        )
        await db.flush()
        card = await CardService(db).create_card(
            board_id,
            actor_id,
            CardCreate(title="Governed target", spec_id=spec_id),
        )
        assert card is not None

        await SpecService(db).move_spec(
            spec_id,
            actor_id,
            SpecMove(status=SpecStatus.DRAFT),
        )

        service = SpecKnowledgeService(db)
        kb = await service.create_knowledge(
            spec_id,
            actor_id,
            SpecKnowledgeCreate(
                title="Governed KB",
                content="Reference content",
                governance_metadata=_governance_metadata(),
            ),
        )
        assert kb is not None

        loaded = await db.get(Card, card.id)
        first_snapshot = loaded.knowledge_bases[0]
        original_id = first_snapshot["id"]
        original_source = first_snapshot["source"]
        assert first_snapshot["governance_metadata"]["purpose"] == (
            "Explain the source contract"
        )

        changed = _governance_metadata(purpose="Explain the revised contract")
        await service.update_knowledge(
            kb.id,
            SpecKnowledgeUpdate(governance_metadata=changed),
        )
        await db.refresh(loaded)
        refreshed = loaded.knowledge_bases[0]
        assert len(loaded.knowledge_bases) == 1
        assert refreshed["id"] == original_id
        assert refreshed["source"] == original_source
        assert refreshed["governance_metadata"]["purpose"] == (
            "Explain the revised contract"
        )

        reordered = {key: changed[key] for key in reversed(tuple(changed))}
        await service.update_knowledge(
            kb.id,
            SpecKnowledgeUpdate(governance_metadata=reordered),
        )
        await db.refresh(loaded)
        assert loaded.knowledge_bases == [refreshed]


@pytest.mark.asyncio
async def test_auto_propagation_allows_selected_resource_types_absent_on_spec(db_factory):
    board_id = _id("board-no-resources")
    actor_id = _id("agent-no-resources")
    spec_id = _id("spec-no-resources")

    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="No resource board",
                owner_id=actor_id,
                settings=_settings("knowledge_base", "mockup", "architecture"),
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Spec without resources",
                status=SpecStatus.APPROVED,
                created_by=actor_id,
                functional_requirements=[],
                acceptance_criteria=[],
                test_scenarios=[],
                business_rules=[],
                api_contracts=[],
            )
        )
        await db.flush()

        card = await CardService(db).create_card(
            board_id,
            actor_id,
            CardCreate(title="Card still created", spec_id=spec_id),
        )

        assert card is not None
        assert card.knowledge_bases in (None, [])
        assert card.screen_mockups in (None, [])
        audits = (
            await db.execute(
                select(ActivityLog).where(
                    ActivityLog.board_id == board_id,
                    ActivityLog.card_id == card.id,
                    ActivityLog.action == "spec_resources_auto_propagated",
                )
            )
        ).scalars().all()
        assert audits[0].details["results"]["knowledge_base"]["source_count"] == 0
        assert audits[0].details["results"]["mockup"]["source_count"] == 0
        assert audits[0].details["results"]["architecture"]["source_count"] == 0


@pytest.mark.asyncio
async def test_link_card_to_spec_runs_auto_propagation(db_factory):
    board_id = _id("board-link-resources")
    actor_id = _id("agent-link-resources")

    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Link resource board",
                owner_id=actor_id,
                settings=_settings("knowledge_base"),
            )
        )
        ids = await _seed_spec_with_resources(db, board_id=board_id, actor_id=actor_id)
        spec_id = ids["spec_id"]
        card_id = _id("unlinked-card")
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=None,
                title="Existing card",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                created_by=actor_id,
            )
        )
        await db.flush()

        linked = await SpecService(db).link_card(spec_id, card_id, user_id=actor_id)
        assert linked is True

        card = await db.get(Card, card_id)
        assert card.spec_id == spec_id
        assert [item["id"] for item in card.knowledge_bases or []] == [f"cardkb_{ids['knowledge_id']}"]


# ---------------------------------------------------------------------------
# Spec 9af2cf05 — service-layer propagation closing the 10-path gap.
# Each test exercises a repository/service entrypoint directly to prove the
# fix can't regress if a new handler skips the call. See KB
# "Diagnostic: 10 propagation paths bypass auto-derive".
# ---------------------------------------------------------------------------


async def _seed_board_spec_card(db, *, resource_types: tuple[str, ...]) -> dict[str, str]:
    board_id = _id("board-svc")
    actor_id = _id("agent-svc")
    spec_id = _id("spec-svc")
    card_id = _id("card-svc")
    settings = (
        _settings(*resource_types) if resource_types else {"auto_derive_spec_resources_enabled": False}
    )
    db.add(Board(id=board_id, name="Service layer board", owner_id=actor_id, settings=settings))
    db.add(
        Spec(
            id=spec_id,
            board_id=board_id,
            title="Service layer spec",
            status=SpecStatus.DRAFT,
            created_by=actor_id,
            functional_requirements=["FR"],
            acceptance_criteria=["AC"],
            test_scenarios=[],
            business_rules=[],
            api_contracts=[],
        )
    )
    db.add(
        Card(
            id=card_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Service layer card",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.NORMAL,
            created_by=actor_id,
        )
    )
    await db.flush()
    return {"board_id": board_id, "actor_id": actor_id, "spec_id": spec_id, "card_id": card_id}


def _author_ack() -> ArchitectureWarningAcknowledgementRequest:
    """Genuine authoring acknowledgement (accepted, no key list -> no mismatch).
    The synthetic design carries real critic warnings; the author accepts them so
    the warning-ack gate is satisfied legitimately (not bypassed)."""
    return ArchitectureWarningAcknowledgementRequest(
        accepted=True,
        statement="Test author acknowledges the synthetic architecture critic warnings.",
    )


def _clean_arch_entities(api_name: str = "Checkout API") -> list[dict]:
    return [
        {"id": "customer-portal", "name": "Customer Portal", "entity_type": "web_app",
         "responsibility": "Sends checkout data to the API."},
        {"id": "checkout-api", "name": api_name, "entity_type": "api",
         "responsibility": "Handles checkout and orders."},
    ]


_CLEAN_ARCH_INTERFACES = [
    {"id": "create-order", "name": "Create order", "endpoint": "POST /orders",
     "description": "Portal sends checkout data to Checkout API.",
     "direction": "source_to_target", "protocol": "REST", "contract_type": "OpenAPI",
     "source_entity_id": "customer-portal", "target_entity_id": "checkout-api"},
]

_CLEAN_ARCH_DIAGRAMS = [
    {"id": "diagram-runtime", "title": "Runtime context", "diagram_type": "context",
     "format": "excalidraw_json",
     "adapter_payload": {"type": "excalidraw", "version": 2, "elements": [
         {"id": "node-customer-portal", "type": "rectangle", "label": "Customer Portal",
          "linkedEntityId": "customer-portal"},
         {"id": "node-checkout-api", "type": "rectangle", "label": "Checkout API",
          "linkedEntityId": "checkout-api"},
         {"id": "edge-create-order", "type": "arrow", "sourceElementId": "node-customer-portal",
          "targetElementId": "node-checkout-api", "linkedInterfaceIds": ["create-order"]},
     ], "appState": {}, "files": {}}},
]


async def _arch_create_payload() -> ArchitectureDesignCreate:
    # Spec B: a CLEAN, eligible source (two connected services — no isolated nodes and no
    # blocking structured warnings) so the auto-propagation feature can be verified end to
    # end under the new eligibility gate. Only non-blocking textual boundary/schema notes
    # remain, which neither require an ack nor create active findings.
    return ArchitectureDesignCreate(
        title="Service layer arch",
        global_description="Two connected services to verify clean propagation behavior end to end.",
        entities=_clean_arch_entities(),
        interfaces=_CLEAN_ARCH_INTERFACES,
        diagrams=_CLEAN_ARCH_DIAGRAMS,
    )


@pytest.mark.asyncio
async def test_architecture_create_via_repository_propagates_to_linked_cards(db_factory):
    """Spec 9af2cf05 — AC1 / ts_7046a567."""
    async with db_factory() as db:
        ctx = await _seed_board_spec_card(db, resource_types=("architecture",))
        repo = ArchitectureDesignRepository(db)
        design = await repo.create("spec", ctx["spec_id"], await _arch_create_payload(), ctx["actor_id"])

        card_designs = (
            await db.execute(
                select(ArchitectureDesign).where(ArchitectureDesign.card_id == ctx["card_id"])
            )
        ).scalars().all()
        assert len(card_designs) == 1
        assert card_designs[0].source_ref == f"architecture_design:{design.id}"

        audits = (
            await db.execute(
                select(ActivityLog).where(
                    ActivityLog.board_id == ctx["board_id"],
                    ActivityLog.action == "spec_resources_auto_propagated",
                )
            )
        ).scalars().all()
        triggers = [audit.details.get("trigger") for audit in audits]
        assert "spec_architecture_created" in triggers
        latest = next(audit for audit in audits if audit.details.get("trigger") == "spec_architecture_created")
        assert latest.details["results"]["architecture"]["copied_count"] == 1


@pytest.mark.asyncio
async def test_architecture_update_via_repository_propagates_to_linked_cards(db_factory):
    """Spec 9af2cf05 — AC2 / ts_b11f707b."""
    async with db_factory() as db:
        ctx = await _seed_board_spec_card(db, resource_types=("architecture",))
        repo = ArchitectureDesignRepository(db)
        design = await repo.create("spec", ctx["spec_id"], await _arch_create_payload(), ctx["actor_id"])

        await repo.update(
            design.id,
            ArchitectureDesignUpdate(
                entities=_clean_arch_entities(api_name="Demo API v2"),
                change_summary="Renamed entity",
            ),
            ctx["actor_id"],
        )

        card_designs = (
            await db.execute(
                select(ArchitectureDesign).where(ArchitectureDesign.card_id == ctx["card_id"])
            )
        ).scalars().all()
        assert len(card_designs) == 1
        refreshed = card_designs[0]
        assert refreshed.source_ref == f"architecture_design:{design.id}"
        entity_names = {entity["name"] for entity in refreshed.entities or []}
        assert "Demo API v2" in entity_names

        triggers = {
            audit.details.get("trigger")
            for audit in (
                await db.execute(
                    select(ActivityLog).where(
                        ActivityLog.board_id == ctx["board_id"],
                        ActivityLog.action == "spec_resources_auto_propagated",
                    )
                )
            ).scalars().all()
        }
        assert "spec_architecture_updated" in triggers


@pytest.mark.asyncio
async def test_architecture_delete_via_repository_cleans_linked_cards(db_factory):
    """Spec 9af2cf05 — AC3 / ts_eeac02f8."""
    async with db_factory() as db:
        ctx = await _seed_board_spec_card(db, resource_types=("architecture",))
        repo = ArchitectureDesignRepository(db)
        design = await repo.create("spec", ctx["spec_id"], await _arch_create_payload(), ctx["actor_id"])
        design_id = design.id

        deleted = await repo.delete(design_id, ctx["actor_id"])
        assert deleted is True

        remaining = (
            await db.execute(
                select(ArchitectureDesign).where(
                    ArchitectureDesign.card_id == ctx["card_id"],
                    ArchitectureDesign.source_ref == f"architecture_design:{design_id}",
                )
            )
        ).scalars().all()
        assert remaining == []

        delete_audits = [
            audit for audit in (
                await db.execute(
                    select(ActivityLog).where(
                        ActivityLog.board_id == ctx["board_id"],
                        ActivityLog.action == "spec_resources_auto_propagated",
                    )
                )
            ).scalars().all()
            if audit.details.get("trigger") == "spec_architecture_deleted"
        ]
        assert delete_audits
        assert delete_audits[-1].details["results"]["architecture"]["removed_count"] == 1


@pytest.mark.asyncio
async def test_spec_knowledge_update_propagates_content_to_linked_cards(db_factory):
    """Spec 9af2cf05 — AC4 / ts_b0835218."""
    async with db_factory() as db:
        ctx = await _seed_board_spec_card(db, resource_types=("knowledge_base",))
        kb_service = SpecKnowledgeService(db)
        kb = await kb_service.create_knowledge(
            ctx["spec_id"],
            ctx["actor_id"],
            SpecKnowledgeCreate(title="Old title", content="Old content", description="Old desc"),
        )
        assert kb is not None

        await kb_service.update_knowledge(
            kb.id,
            SpecKnowledgeUpdate(title="NEW TITLE", content="NEW CONTENT"),
        )

        card = await db.get(Card, ctx["card_id"])
        entry = next(item for item in (card.knowledge_bases or []) if item.get("id") == f"cardkb_{kb.id}")
        assert entry["title"] == "NEW TITLE"
        assert entry["content"] == "NEW CONTENT"

        update_audits = [
            audit for audit in (
                await db.execute(
                    select(ActivityLog).where(
                        ActivityLog.board_id == ctx["board_id"],
                        ActivityLog.action == "spec_resources_auto_propagated",
                    )
                )
            ).scalars().all()
            if audit.details.get("trigger") == "spec_knowledge_updated"
        ]
        assert update_audits
        assert update_audits[-1].details["results"]["knowledge_base"]["copied_count"] >= 1


@pytest.mark.asyncio
async def test_spec_knowledge_delete_removes_cardkb_from_linked_cards(db_factory):
    """Spec 9af2cf05 — AC5 / ts_37bba06b."""
    async with db_factory() as db:
        ctx = await _seed_board_spec_card(db, resource_types=("knowledge_base",))
        kb_service = SpecKnowledgeService(db)
        kb = await kb_service.create_knowledge(
            ctx["spec_id"],
            ctx["actor_id"],
            SpecKnowledgeCreate(title="Ephemeral", content="Ephemeral content"),
        )
        assert kb is not None
        kb_id = kb.id

        deleted = await kb_service.delete_knowledge(kb_id)
        assert deleted is True

        card = await db.get(Card, ctx["card_id"])
        ids = [item.get("id") for item in (card.knowledge_bases or [])]
        assert f"cardkb_{kb_id}" not in ids

        delete_audits = [
            audit for audit in (
                await db.execute(
                    select(ActivityLog).where(
                        ActivityLog.board_id == ctx["board_id"],
                        ActivityLog.action == "spec_resources_auto_propagated",
                    )
                )
            ).scalars().all()
            if audit.details.get("trigger") == "spec_knowledge_deleted"
        ]
        assert delete_audits
        assert delete_audits[-1].details["results"]["knowledge_base"]["removed_count"] >= 1


@pytest.mark.asyncio
async def test_disabled_auto_derive_skips_all_service_layer_propagation(db_factory):
    """Spec 9af2cf05 — AC6 / ts_23a809b1."""
    async with db_factory() as db:
        ctx = await _seed_board_spec_card(db, resource_types=())  # toggle OFF
        repo = ArchitectureDesignRepository(db)
        design = await repo.create("spec", ctx["spec_id"], await _arch_create_payload(), ctx["actor_id"])
        await repo.update(
            design.id,
            ArchitectureDesignUpdate(
                entities=_clean_arch_entities(api_name="Renamed"),
                change_summary="rename",
            ),
            ctx["actor_id"],
        )
        await repo.delete(design.id, ctx["actor_id"])

        kb_service = SpecKnowledgeService(db)
        kb = await kb_service.create_knowledge(
            ctx["spec_id"],
            ctx["actor_id"],
            SpecKnowledgeCreate(title="x", content="x"),
        )
        await kb_service.update_knowledge(kb.id, SpecKnowledgeUpdate(content="y"))
        await kb_service.delete_knowledge(kb.id)

        audits = (
            await db.execute(
                select(ActivityLog).where(
                    ActivityLog.board_id == ctx["board_id"],
                    ActivityLog.action == "spec_resources_auto_propagated",
                )
            )
        ).scalars().all()
        assert audits == []

        card = await db.get(Card, ctx["card_id"])
        assert (card.knowledge_bases or []) == []
        card_designs = (
            await db.execute(
                select(ArchitectureDesign).where(ArchitectureDesign.card_id == ctx["card_id"])
            )
        ).scalars().all()
        assert card_designs == []
