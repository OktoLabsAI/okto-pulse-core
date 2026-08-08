"""Spec 9df814bc / card d86f4f96 — DefaultBoardConfiguration service + applied
snapshot.

Covers FR1-FR4, FR9 and the validator checklist: snapshot lives OUTSIDE
Board.settings, one active template per scope (enforced in-transaction), single
provider (no parallel mechanism), reconstituible audit (global table + board-scoped
ActivityLog), and the graceful no-active-template fallback.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_default_board_configuration.py
"""

from __future__ import annotations

import uuid
from unittest.mock import patch

import pytest
import pytest_asyncio
from sqlalchemy import delete, select

from sqlalchemy_test_models import (
    ActivityLog,
    Board,
    DefaultBoardConfiguration,
    DefaultBoardConfigurationAudit,
    Guideline,
)
from okto_pulse.core.models.schemas import (
    BoardCreate,
    BoardResponse,
    BoardSettings,
    GuidelineCreate,
)
from okto_pulse.core.services.board_governance import BoardGovernanceService
from okto_pulse.core.services.default_board_configuration import (
    BOARD_EVENT_APPLIED,
    BOARD_EVENT_FALLBACK,
    EVENT_ACTIVATED,
    EVENT_CREATED,
    EVENT_DEACTIVATED,
    DefaultBoardConfigurationError,
    DefaultBoardConfigurationService,
)
from okto_pulse.core.services.main import BoardService, GuidelineService

pytestmark = pytest.mark.asyncio

USER_ID = "dbc-test-user"


@pytest_asyncio.fixture(autouse=True)
async def _isolate_global_templates(db_factory):
    async with db_factory() as db:
        await db.execute(
            delete(DefaultBoardConfigurationAudit).where(
                DefaultBoardConfigurationAudit.scope == "global"
            )
        )
        await db.execute(
            delete(DefaultBoardConfiguration).where(
                DefaultBoardConfiguration.scope == "global"
            )
        )
        await db.commit()


async def _activity_actions(db, board_id: str) -> list[str]:
    result = await db.execute(
        select(ActivityLog.action).where(ActivityLog.board_id == board_id)
    )
    return list(result.scalars())


async def _audit_events(db, template_id: str) -> list[str]:
    result = await db.execute(
        select(DefaultBoardConfigurationAudit.event_type)
        .where(DefaultBoardConfigurationAudit.template_id == template_id)
        .order_by(DefaultBoardConfigurationAudit.created_at)
    )
    return list(result.scalars())


# ---------------------------------------------------------------------------
# AC11 / checklist #5 — graceful no-active-template fallback.
# ---------------------------------------------------------------------------


async def test_create_board_without_template_uses_defaults_no_snapshot():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        board = await BoardService(db).create_board(
            USER_ID, BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}")
        )
        # Forward-safe new-board default; NO snapshot and no error.
        expected = BoardSettings().model_dump(mode="json")
        expected["reviewer_separation_mode"] = "enforce"
        assert board.settings == expected
        assert board.default_config_snapshot is None
        # board-scoped fallback audit recorded.
        assert BOARD_EVENT_FALLBACK in await _activity_actions(db, board.id)


# ---------------------------------------------------------------------------
# AC1 — active template is applied + snapshot persisted (outside settings).
# ---------------------------------------------------------------------------


async def test_create_board_applies_active_template_and_persists_snapshot():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DefaultBoardConfigurationService(db)
        template = await svc.create_version(
            settings_payload=BoardSettings(
                max_scenarios_per_card=5, skip_test_coverage_global=True
            ),
            actor=USER_ID,
            activate=True,
        )
        board = await BoardService(db).create_board(
            USER_ID, BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}")
        )

        # Effective settings come from the template payload.
        assert board.settings["max_scenarios_per_card"] == 5
        assert board.settings["skip_test_coverage_global"] is True
        # Snapshot persisted on its OWN column (FR4), not in settings.
        snap = board.default_config_snapshot
        assert snap is not None
        assert snap["template_id"] == template.id
        assert snap["template_version"] == template.version
        assert snap["applied_by"] == USER_ID
        assert snap["override_summary"] == {}  # no override
        assert "default_config_snapshot" not in board.settings
        assert BOARD_EVENT_APPLIED in await _activity_actions(db, board.id)


async def test_spec_checklist_default_is_versioned_snapshotted_and_inherited():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DefaultBoardConfigurationService(db)
        first = await svc.create_version(
            settings_payload=BoardSettings(max_scenarios_per_card=4),
            actor=USER_ID,
            spec_checklist_mode="blocking",
            activate=True,
        )
        second = await svc.create_version(
            settings_payload=BoardSettings(max_scenarios_per_card=6),
            actor=USER_ID,
            # Omission must preserve the human-selected component while another
            # default facet creates a copy-on-write version.
            activate=True,
        )

        assert first.spec_checklist_mode == "blocking"
        assert second.spec_checklist_mode == "blocking"

        board = await BoardService(db).create_board(
            USER_ID, BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}")
        )
        assert board.default_config_snapshot["template_id"] == second.id
        assert board.default_config_snapshot["spec_checklist"] == {
            "mode": "blocking",
            "template_version_id": "/specify/v1",
        }
        assert "spec_checklist_mode" not in board.settings


async def test_spec_checklist_default_rejects_unknown_modes():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        with pytest.raises(DefaultBoardConfigurationError) as exc:
            await DefaultBoardConfigurationService(db).create_version(
                settings_payload=BoardSettings(),
                actor=USER_ID,
                spec_checklist_mode="unsupported",
            )
        assert exc.value.code == "invalid_spec_checklist_mode"


# ---------------------------------------------------------------------------
# AC2 / TR3 — partial override wins over the template; override_summary records it.
# ---------------------------------------------------------------------------


async def test_partial_override_wins_and_records_override_summary():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DefaultBoardConfigurationService(db)
        await svc.create_version(
            settings_payload=BoardSettings(
                max_scenarios_per_card=5, skip_test_coverage_global=True
            ),
            actor=USER_ID,
            activate=True,
        )
        # Partial override: ONLY max_scenarios_per_card is explicitly set.
        board = await BoardService(db).create_board(
            USER_ID,
            BoardCreate(
                name=f"b-{uuid.uuid4().hex[:8]}",
                settings=BoardSettings(max_scenarios_per_card=9),
            ),
        )
        # Override wins on its field; template value survives elsewhere.
        assert board.settings["max_scenarios_per_card"] == 9
        assert board.settings["skip_test_coverage_global"] is True
        assert board.default_config_snapshot["override_summary"] == {
            "max_scenarios_per_card": 9
        }


# ---------------------------------------------------------------------------
# AC3 / TR5 — changing the active template later does NOT mutate existing boards.
# ---------------------------------------------------------------------------


async def test_template_change_after_creation_does_not_mutate_existing_board():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DefaultBoardConfigurationService(db)
        await svc.create_version(
            settings_payload=BoardSettings(max_scenarios_per_card=4),
            actor=USER_ID,
            activate=True,
        )
        board = await BoardService(db).create_board(
            USER_ID, BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}")
        )
        before_settings = dict(board.settings)
        before_snapshot = dict(board.default_config_snapshot)

        # A new active version with different settings.
        await svc.create_version(
            settings_payload=BoardSettings(max_scenarios_per_card=12),
            actor=USER_ID,
            activate=True,
        )
        board = await BoardService(db).get_board(board.id)
        assert board.settings == before_settings
        assert board.default_config_snapshot == before_snapshot


# ---------------------------------------------------------------------------
# Checklist #2 — at most one active template per scope, enforced in-transaction.
# ---------------------------------------------------------------------------


async def test_single_active_per_scope_enforced():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DefaultBoardConfigurationService(db)
        a = await svc.create_version(
            settings_payload=BoardSettings(), actor=USER_ID, activate=True
        )
        b = await svc.create_version(
            settings_payload=BoardSettings(), actor=USER_ID, activate=True
        )

        versions = {template.id: template for template in await svc.list_versions()}
        a = versions[a.id]
        b = versions[b.id]
        assert a.is_active is False and a.status == "inactive"
        assert b.is_active is True and b.status == "active"

        actives = await db.execute(
            select(DefaultBoardConfiguration).where(
                DefaultBoardConfiguration.scope == "global",
                DefaultBoardConfiguration.is_active.is_(True),
            )
        )
        assert [t.id for t in actives.scalars()] == [b.id]
        resolved = await svc.resolve_active()
        assert resolved is not None and resolved.id == b.id


# ---------------------------------------------------------------------------
# Checklist #4 / FR9 — global template audit is reconstituible by query, and
# board-scoped events stay in the existing ActivityLog mechanism.
# ---------------------------------------------------------------------------


async def test_global_audit_events_reconstituible_by_query():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DefaultBoardConfigurationService(db)
        template = await svc.create_version(
            settings_payload=BoardSettings(), actor=USER_ID
        )
        assert await _audit_events(db, template.id) == [EVENT_CREATED]

        await svc.activate_version(template.id, USER_ID)
        await svc.deactivate_version(template.id, USER_ID)
        events = await _audit_events(db, template.id)
        assert events == [EVENT_CREATED, EVENT_ACTIVATED, EVENT_DEACTIVATED]

        # Board-scoped event lives in ActivityLog (not the global table).
        board = await BoardService(db).create_board(
            USER_ID, BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}")
        )
        assert BOARD_EVENT_FALLBACK in await _activity_actions(db, board.id)


async def test_activate_second_template_audits_supersede_of_first():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DefaultBoardConfigurationService(db)
        a = await svc.create_version(
            settings_payload=BoardSettings(), actor=USER_ID, activate=True
        )
        await svc.create_version(
            settings_payload=BoardSettings(), actor=USER_ID, activate=True
        )
        # First template got a deactivated audit event when superseded.
        assert EVENT_DEACTIVATED in await _audit_events(db, a.id)


# ---------------------------------------------------------------------------
# Checklist #1 — snapshot lives OUTSIDE Board.settings; BoardSettings validation
# never sees/masks it.
# ---------------------------------------------------------------------------


async def test_snapshot_outside_board_settings_and_not_masked_by_boardsettings():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DefaultBoardConfigurationService(db)
        await svc.create_version(
            settings_payload=BoardSettings(max_scenarios_per_card=6),
            actor=USER_ID,
            activate=True,
        )
        board = await BoardService(db).create_board(
            USER_ID, BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}")
        )
        # Snapshot key is NOT inside settings.
        assert "default_config_snapshot" not in board.settings
        # BoardSettings round-trip over the effective settings is lossless and does
        # not surface the snapshot.
        normalized = BoardGovernanceService.normalize_settings(board.settings)
        assert "default_config_snapshot" not in normalized
        assert normalized["max_scenarios_per_card"] == 6
        # The snapshot is intact on its own column.
        assert board.default_config_snapshot is not None


# ---------------------------------------------------------------------------
# TR1 / TR8 — invalid settings_payload is rejected with a structured error.
# ---------------------------------------------------------------------------


async def test_create_version_rejects_invalid_settings_payload():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DefaultBoardConfigurationService(db)
        with pytest.raises(DefaultBoardConfigurationError) as exc:
            await svc.create_version(
                settings_payload={"max_ideation_ambiguity": 99}, actor=USER_ID
            )
        assert exc.value.code == "invalid_settings_payload"


async def test_resolve_active_none_when_no_active_template():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DefaultBoardConfigurationService(db)
        # A draft (non-active) version must NOT resolve as active.
        await svc.create_version(settings_payload=BoardSettings(), actor=USER_ID)
        assert await svc.resolve_active() is None


# ---------------------------------------------------------------------------
# Card 987b9ac5 — legacy forward-only compatibility + no-active-template fallback.
# ---------------------------------------------------------------------------


async def test_ts_dcd56041_template_changes_forward_only_and_legacy_boards_compatible():
    """ts_dcd56041 (TR4/TR5): activating a new template version does NOT mutate any
    existing board; a snapshot board reports its ORIGINAL applied version and a
    legacy board (no snapshot) reports legacy_no_snapshot WITHOUT backfill."""
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DefaultBoardConfigurationService(db)
        v1 = await svc.create_version(
            settings_payload=BoardSettings(max_scenarios_per_card=3),
            actor=USER_ID,
            activate=True,
        )
        # Board A: created with the active template (gets an applied snapshot).
        board_a = await BoardService(db).create_board(
            USER_ID, BoardCreate(name=f"a-{uuid.uuid4().hex[:8]}")
        )
        # Board B: a pre-existing legacy board with NO snapshot metadata.
        board_b = Board(
            name=f"b-{uuid.uuid4().hex[:8]}",
            owner_id=USER_ID,
            settings=BoardSettings().model_dump(mode="json"),
            default_config_snapshot=None,
        )
        db.add(board_b)
        await db.flush()

        a_settings_before = dict(board_a.settings)
        a_snapshot_before = dict(board_a.default_config_snapshot)
        b_settings_before = dict(board_b.settings)

        # Activate a NEW template version with different settings.
        await svc.create_version(
            settings_payload=BoardSettings(max_scenarios_per_card=11),
            actor=USER_ID,
            activate=True,
        )
        board_a = await BoardService(db).get_board(board_a.id)
        await db.refresh(board_b)

        # Forward-only: neither existing board was mutated, no backfill.
        assert board_a.settings == a_settings_before
        assert board_a.default_config_snapshot == a_snapshot_before
        assert board_b.settings == b_settings_before
        assert board_b.default_config_snapshot is None

        # Read-only diff/state report.
        desc_a = await svc.describe_board_config(board_a)
        assert desc_a["state"] == "applied"
        assert desc_a["applied_template_version"] == v1.version  # original (1)
        assert desc_a["active_template_version"] == v1.version + 1  # current (2)
        assert desc_a["is_outdated"] is True

        desc_b = await svc.describe_board_config(board_b)
        assert desc_b == {
            "state": "legacy_no_snapshot",
            "board_id": board_b.id,
            "configuration_presence": "null",
            "baseline_available": False,
            "comparable": False,
        }


async def test_ts_3312f7bd_bootstrap_fallback_no_template_no_error():
    """ts_3312f7bd (TR11/AC11): with NO template ever created (bootstrap), creating
    a board without explicit settings succeeds with forward-safe defaults, no
    snapshot, and a non-error fallback audit signal."""
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        # Bootstrap: no DefaultBoardConfiguration has ever been created.
        assert await DefaultBoardConfigurationService(db).resolve_active() is None
        board = await BoardService(db).create_board(
            USER_ID, BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}")
        )
        expected = BoardSettings().model_dump(mode="json")
        expected["reviewer_separation_mode"] = "enforce"
        assert board.settings == expected
        assert board.default_config_snapshot is None
        assert BOARD_EVENT_FALLBACK in await _activity_actions(db, board.id)


async def test_tr11_board_response_contract_exposes_default_config_snapshot():
    """TR11: the create_board response contract carries default_config_snapshot —
    null for the fallback path and the snapshot dict for an applied template."""
    from okto_pulse.core.infra.database import get_session_factory

    assert "default_config_snapshot" in BoardResponse.model_fields

    async with get_session_factory()() as db:
        bs = BoardService(db)
        # Fallback board -> response exposes default_config_snapshot = None.
        fb = await bs.create_board(
            USER_ID, BoardCreate(name=f"fb-{uuid.uuid4().hex[:8]}")
        )
        fetched = await bs.get_board(fb.id)
        fetched.attach("agents", [])
        fb_resp = BoardResponse.model_validate(fetched)
        assert "default_config_snapshot" in fb_resp.model_dump()
        assert fb_resp.default_config_snapshot is None

        # Applied template -> response exposes the snapshot dict.
        await DefaultBoardConfigurationService(db).create_version(
            settings_payload=BoardSettings(), actor=USER_ID, activate=True
        )
        tb = await bs.create_board(
            USER_ID, BoardCreate(name=f"tb-{uuid.uuid4().hex[:8]}")
        )
        fetched_tb = await bs.get_board(tb.id)
        fetched_tb.attach("agents", [])
        tb_resp = BoardResponse.model_validate(fetched_tb)
        assert tb_resp.default_config_snapshot is not None
        assert tb_resp.default_config_snapshot["template_id"]


# ---------------------------------------------------------------------------
# Card dc45987a — Guidelines default global-only adapter (FR5/TR6, br_512d374b).
# ---------------------------------------------------------------------------


async def _make_global_guideline(db, title: str = "G") -> Guideline:
    return await GuidelineService(db).create_guideline(
        USER_ID,
        GuidelineCreate(
            title=title,
            content="c",
            scope="global",
            board_id=None,
        ),
    )


def _default_ref(guideline: Guideline, *, priority: int = 0) -> dict:
    return {
        "guideline_id": guideline.id,
        "priority": priority,
        "revision_id": guideline.revision_id,
        "revision_number": guideline.version,
        "semantic_version": guideline.semantic_version,
        "revision_digest": guideline.revision_digest,
    }


async def _authoritative_bindings(db, board_id: str):
    from okto_pulse.core.ports.relational_application import (
        require_relational_application_adapter,
    )

    return (
        await require_relational_application_adapter()
        .guideline_policy(db)
        .list_bindings(board_id=board_id)
    )


async def test_ts_cdb70cc0_inline_guideline_default_blocks_activation():
    """ts_cdb70cc0 (part 1): a template whose guideline defaults include an inline
    guideline (no guideline_id) fails activation fail-closed and is NOT activated."""
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DefaultBoardConfigurationService(db)
        g = await _make_global_guideline(db)
        with pytest.raises(DefaultBoardConfigurationError) as exc:
            await svc.create_version(
                settings_payload=BoardSettings(),
                actor=USER_ID,
                activate=True,
                guideline_default_refs=[
                    _default_ref(g, priority=1),
                    {"title": "inline", "content": "x"},  # inline, no guideline_id
                ],
            )
        assert exc.value.code == "default_guideline_inline_not_allowed"
        assert await svc.resolve_active() is None  # not activated


async def test_default_guideline_non_global_blocks_activation():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DefaultBoardConfigurationService(db)
        board = await BoardService(db).create_board(
            USER_ID, BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}")
        )
        bg = await GuidelineService(db).create_guideline(
            USER_ID,
            GuidelineCreate(
                title="bg",
                content="c",
                scope="inline",
                board_id=board.id,
            ),
        )
        with pytest.raises(DefaultBoardConfigurationError) as exc:
            await svc.create_version(
                settings_payload=BoardSettings(),
                actor=USER_ID,
                activate=True,
                guideline_default_refs=[_default_ref(bg)],
            )
        assert exc.value.code == "default_guideline_not_global"


async def test_default_guideline_not_found_blocks_activation():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DefaultBoardConfigurationService(db)
        with pytest.raises(DefaultBoardConfigurationError) as exc:
            await svc.create_version(
                settings_payload=BoardSettings(),
                actor=USER_ID,
                activate=True,
                guideline_default_refs=[
                    {
                        "guideline_id": "does-not-exist",
                        "priority": 0,
                        "revision_id": str(uuid.uuid4()),
                        "revision_number": 1,
                        "semantic_version": "1.0.0",
                        "revision_digest": "0" * 64,
                    }
                ],
            )
        assert exc.value.code == "default_guideline_not_found"


async def test_ts_cdb70cc0_valid_template_materializes_global_board_guidelines():
    """An active template materializes exact authoritative global bindings."""
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DefaultBoardConfigurationService(db)
        g1 = await _make_global_guideline(db, "G1")
        g2 = await _make_global_guideline(db, "G2")
        await svc.create_version(
            settings_payload=BoardSettings(),
            actor=USER_ID,
            activate=True,
            guideline_default_refs=[
                _default_ref(g1, priority=5),
                _default_ref(g2, priority=2),
            ],
        )
        board = await BoardService(db).create_board(
            USER_ID, BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}")
        )
        links = {
            link.guideline_id: link.priority
            for link in await _authoritative_bindings(db, board.id)
        }
        assert links == {g1.id: 5, g2.id: 2}


async def test_materialize_default_guidelines_is_idempotent_and_unique():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DefaultBoardConfigurationService(db)
        g = await _make_global_guideline(db)
        await svc.create_version(
            settings_payload=BoardSettings(),
            actor=USER_ID,
            activate=True,
            guideline_default_refs=[_default_ref(g, priority=1)],
        )
        board = await BoardService(db).create_board(
            USER_ID, BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}")
        )
        # re-running the adapter must NOT create a duplicate (board/guideline unique).
        again = await svc.materialize_default_guidelines(board.id, actor=USER_ID)
        assert again == []
        assert len(await _authoritative_bindings(db, board.id)) == 1


async def test_no_active_template_materializes_no_guidelines():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DefaultBoardConfigurationService(db)
        board = await BoardService(db).create_board(
            USER_ID, BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}")
        )
        assert await svc.materialize_default_guidelines(board.id, actor=USER_ID) == []
        assert await _authoritative_bindings(db, board.id) == ()


# ---------------------------------------------------------------------------
# Card b494f852 — transactional adapters + minimal Design System default.
# ---------------------------------------------------------------------------


async def test_ts_d3363274_adapter_failure_rolls_back_board_creation():
    """ts_d3363274 (TR2/TR7): an adapter failure during materialization aborts with
    default_materialization_failed and leaves NO partially-created board/link/
    snapshot — proven from a fresh session the caller would use."""
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    board_name = f"rb-{uuid.uuid4().hex[:8]}"
    async with factory() as db:
        svc = DefaultBoardConfigurationService(db)
        g = await _make_global_guideline(db)
        await svc.create_version(
            settings_payload=BoardSettings(),
            actor=USER_ID,
            activate=True,
            guideline_default_refs=[_default_ref(g, priority=1)],
        )
        with patch.object(
            GuidelineService,
            "apply_default_guidelines",
            side_effect=RuntimeError("forced adapter failure"),
        ):
            with pytest.raises(DefaultBoardConfigurationError) as exc:
                await BoardService(db).create_board(
                    USER_ID,
                    BoardCreate(name=board_name),
                )
        assert exc.value.code == "default_materialization_failed"
        board_id = exc.value.details["board_id"]
        await db.rollback()

    # Fresh session: neither the board nor any additional link is observable.
    async with factory() as db2:
        board = (
            await db2.execute(select(Board).where(Board.name == board_name))
        ).scalar_one_or_none()
        assert board is None
        assert await _authoritative_bindings(db2, board_id) == ()


async def test_ts_d45c1602_umbrella_applies_both_defaults_without_parallel_store():
    """ts_d45c1602 (TR10/br_53d6de87): guideline AND design-system defaults are both
    applied through the umbrella service in the same create_board flow, with the
    design system recorded inside default_config_snapshot (no parallel store)."""
    from okto_pulse.core.infra.database import get_session_factory

    from okto_pulse.core.services.design_system import DesignSystemService

    async with get_session_factory()() as db:
        svc = DefaultBoardConfigurationService(db)
        g = await _make_global_guideline(db)
        # the design-system default must reference a REAL, global, active entity
        # (spec 3a006f65 enriched _validate_design_system_default_ref).
        ds_entity = await DesignSystemService(db).create_design_system(
            USER_ID, title="DS", scope="global", payload={"tokens": {}}
        )
        await svc.create_version(
            settings_payload=BoardSettings(),
            actor=USER_ID,
            activate=True,
            guideline_default_refs=[_default_ref(g, priority=3)],
            design_system_default_ref={
                "design_system_id": ds_entity.id,
                "version": ds_entity.version,
                "gate_mode": "advisory",
                "snapshot": {"tokens": {}},
            },
        )
        board = await BoardService(db).create_board(
            USER_ID, BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}")
        )
        # Guideline default -> authoritative binding via the umbrella adapter.
        links = await _authoritative_bindings(db, board.id)
        assert {link.guideline_id for link in links} == {g.id}
        # Design System default -> recorded INSIDE default_config_snapshot (no
        # parallel store, no separate entity/table).
        ds = board.default_config_snapshot["design_system"]
        assert ds["design_system_id"] == ds_entity.id
        assert ds["gate_mode"] == "advisory"


async def test_design_system_default_invalid_gate_mode_blocks_activation():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DefaultBoardConfigurationService(db)
        with pytest.raises(DefaultBoardConfigurationError) as exc:
            await svc.create_version(
                settings_payload=BoardSettings(),
                actor=USER_ID,
                activate=True,
                design_system_default_ref={
                    "design_system_id": "ds-1",
                    "gate_mode": "loud",
                },
            )
        assert exc.value.code == "design_system_default_invalid"
        assert await svc.resolve_active() is None
