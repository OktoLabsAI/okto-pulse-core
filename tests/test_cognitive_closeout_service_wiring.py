"""Service integration tests for the shared cognitive closeout gate."""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.kg.rebuild_audit import CognitivePendingMarker
from okto_pulse.core.kg.rebuild_audit import CognitiveConsolidationItemStore
from okto_pulse.core.kg.rebuild_audit import require_rebuild_audit_artifact_store
from okto_pulse.core.kg.rebuild_generation import generate_kg_generation_id
from sqlalchemy_test_models import (
    Board,
    Card,
    CardStatus,
    CardType,
    ConsolidationDeadLetter,
    Spec,
    SpecStatus,
)
from okto_pulse.core.models.schemas import CardMove, SpecMove
from okto_pulse.core.services.canonical_debt_service import upsert_canonical_debt
from okto_pulse.core.services.main import (
    CardService,
    SpecService,
    _board_cognitive_readiness_policy,
    _cognitive_readiness_blocking_active,
)
from okto_pulse.core.services.resource_gate import ResourceGateService


USER_ID = "ccg-service-wiring-agent"


def _id() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def isolated_closeout_kg_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    target = tmp_path / "ccg-service-wiring"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(target))
    return target


def _seed_pending_item(base_dir: Path, board_id: str, source_ref: str) -> None:
    del base_dir
    marker = CognitivePendingMarker(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    marker.mark_for_generation(
        board_id=board_id,
        kg_generation_id=generate_kg_generation_id(),
        source_set=[
            {
                "artifact_type": source_ref.split(":", 1)[0],
                "id": source_ref.rsplit(":", 1)[-1],
                "source_ref": source_ref,
            }
        ],
        event_ref="evt_ccg_service",
    )


async def _seed_spec(
    *,
    board_settings: dict | None = None,
    decisions: list[dict] | None = None,
) -> tuple[str, str]:
    board_id = _id()
    spec_id = _id()
    db_factory = get_session_factory()
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="CCG service wiring board",
                owner_id=USER_ID,
                settings=board_settings or {},
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="CCG service wiring spec",
                status=SpecStatus.IN_PROGRESS,
                created_by=USER_ID,
                decisions=decisions or [],
                acceptance_criteria=[],
                test_scenarios=[],
            )
        )
        await db.commit()
    return board_id, spec_id


async def _spec_status(spec_id: str) -> str:
    db_factory = get_session_factory()
    async with db_factory() as db:
        spec = (await db.execute(select(Spec).where(Spec.id == spec_id))).scalar_one()
        return spec.status.value


@pytest.mark.asyncio
async def test_spec_done_blocks_on_direct_spec_cognitive_item(
    isolated_closeout_kg_dir: Path,
) -> None:
    board_id, spec_id = await _seed_spec()
    _seed_pending_item(isolated_closeout_kg_dir, board_id, f"spec:{spec_id}")

    db_factory = get_session_factory()
    async with db_factory() as db:
        service = SpecService(db)
        with pytest.raises(ValueError, match="cognitive_consolidation_pending"):
            await service.move_spec(
                spec_id=spec_id,
                user_id=USER_ID,
                data=SpecMove(status=SpecStatus.DONE),
                actor_name=USER_ID,
            )
        await db.rollback()

    assert await _spec_status(spec_id) == SpecStatus.IN_PROGRESS.value


@pytest.mark.asyncio
async def test_spec_done_blocks_on_active_decision_child_cognitive_item(
    isolated_closeout_kg_dir: Path,
) -> None:
    decision_id = "dec_keep_kg_safe"
    board_id, spec_id = await _seed_spec(
        decisions=[
            {
                "id": decision_id,
                "title": "Do not close before cognitive consolidation",
                "status": "active",
            }
        ]
    )
    _seed_pending_item(
        isolated_closeout_kg_dir,
        board_id,
        f"decision:{spec_id}:{decision_id}",
    )

    db_factory = get_session_factory()
    async with db_factory() as db:
        service = SpecService(db)
        with pytest.raises(ValueError, match="cognitive_consolidation_pending"):
            await service.move_spec(
                spec_id=spec_id,
                user_id=USER_ID,
                data=SpecMove(status=SpecStatus.DONE),
                actor_name=USER_ID,
            )
        await db.rollback()

    assert await _spec_status(spec_id) == SpecStatus.IN_PROGRESS.value


class _BlockingGate:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def evaluate(self, **kwargs):
        self.calls.append(dict(kwargs))
        return SimpleNamespace(
            allowed=False,
            reason="cognitive_consolidation_pending",
            blocking_count=1,
            blocking_items=(object(),),
        )


async def _seed_card(
    card_type: CardType,
    status: CardStatus,
    *,
    board_settings: dict | None = None,
) -> tuple[str, str, str]:
    board_id = _id()
    spec_id = _id()
    card_id = _id()
    db_factory = get_session_factory()
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="CCG card board",
                owner_id=USER_ID,
                settings=board_settings or {},
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="CCG card spec",
                status=SpecStatus.IN_PROGRESS,
                created_by=USER_ID,
                acceptance_criteria=[],
                test_scenarios=[],
            )
        )
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="CCG card",
                status=status,
                card_type=card_type,
                position=0,
                created_by=USER_ID,
            )
        )
        await db.commit()
    return board_id, spec_id, card_id


async def _card_row(card_id: str) -> Card:
    db_factory = get_session_factory()
    async with db_factory() as db:
        return (await db.execute(select(Card).where(Card.id == card_id))).scalar_one()


async def _mark_card_resources_na(db, board_id: str, card_id: str) -> None:
    service = ResourceGateService(db)
    for resource_type in ("architecture", "mockup", "knowledge_base"):
        await service.mark_not_applicable(
            board_id,
            "card",
            card_id,
            resource_type,
            USER_ID,
            justification=f"{resource_type} is intentionally not applicable in this closeout service test.",
            source_channel="ui",
        )


@pytest.mark.asyncio
async def test_submit_task_validation_blocks_before_automatic_done() -> None:
    _, _, card_id = await _seed_card(CardType.NORMAL, CardStatus.VALIDATION)
    gate = _BlockingGate()
    validation_data = {
        "confidence": 99,
        "confidence_justification": "strong",
        "estimated_completeness": 100,
        "completeness_justification": "complete",
        "estimated_drift": 0,
        "drift_justification": "none",
        "recommendation": "approve",
        "general_justification": "ready",
    }

    db_factory = get_session_factory()
    async with db_factory() as db:
        service = CardService(db)
        service._cognitive_closeout_gate_factory = lambda: gate
        with pytest.raises(ValueError, match="cognitive_consolidation_pending"):
            await service.submit_task_validation(
                card_id=card_id,
                reviewer_id=USER_ID,
                reviewer_name=USER_ID,
                data=validation_data,
            )
        await db.rollback()

    card = await _card_row(card_id)
    assert card.status == CardStatus.VALIDATION
    assert card.validations in (None, [])
    assert gate.calls[0]["entity_type"] == "task"
    assert gate.calls[0]["target_status"] == "done"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("card_type", "expected_entity_type"),
    [
        (CardType.NORMAL, "task"),
        (CardType.TEST, "test"),
        (CardType.BUG, "bug"),
    ],
)
async def test_move_card_done_blocks_task_test_and_bug_before_status_mutation(
    card_type: CardType,
    expected_entity_type: str,
) -> None:
    _, _, card_id = await _seed_card(
        card_type,
        CardStatus.VALIDATION,
        board_settings={
            # This test isolates cognitive closeout.  Satisfy the independent
            # lifecycle prerequisites that otherwise (correctly) fail first.
            "require_task_validation": False,
            "skip_test_coverage_global": True,
        },
    )
    gate = _BlockingGate()

    db_factory = get_session_factory()
    async with db_factory() as db:
        service = CardService(db)
        service._cognitive_closeout_gate_factory = lambda: gate
        with pytest.raises(ValueError, match="cognitive_consolidation_pending"):
            await service.move_card(
                card_id=card_id,
                user_id=USER_ID,
                data=CardMove(
                    status=CardStatus.DONE,
                    conclusion="implemented and validated",
                    completeness=100,
                    completeness_justification="complete",
                    drift=0,
                    drift_justification="none",
                ),
                actor_name=USER_ID,
            )
        await db.rollback()

    card = await _card_row(card_id)
    assert card.status == CardStatus.VALIDATION
    assert card.conclusions in (None, [])
    assert gate.calls[0]["entity_type"] == expected_entity_type
    assert gate.calls[0]["target_status"] == "done"


@pytest.mark.asyncio
async def test_board_skip_allows_done_but_keeps_pending_item_visible_and_status_done(
    isolated_closeout_kg_dir: Path,
) -> None:
    board_id, _, card_id = await _seed_card(
        CardType.NORMAL,
        CardStatus.VALIDATION,
        board_settings={
            "skip_cognitive_consolidation": True,
            # The board skip is scoped to cognitive closeout; disable the
            # independent task-validation gate for this direct-move fixture.
            "require_task_validation": False,
        },
    )
    _seed_pending_item(isolated_closeout_kg_dir, board_id, f"task:{card_id}")

    db_factory = get_session_factory()
    async with db_factory() as db:
        await _mark_card_resources_na(db, board_id, card_id)
        await db.commit()

    async with db_factory() as db:
        service = CardService(db)
        await service.move_card(
            card_id=card_id,
            user_id=USER_ID,
            data=CardMove(
                status=CardStatus.DONE,
                conclusion="validated with board cognitive skip enabled",
                completeness=100,
                completeness_justification="pending item remains visible; only blocking is bypassed",
                drift=0,
                drift_justification="board skip semantics are unchanged",
            ),
            actor_name=USER_ID,
        )
        await db.commit()

    card = await _card_row(card_id)
    assert card.status == CardStatus.DONE

    store = CognitiveConsolidationItemStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    latest = store.latest_generation(board_id)
    assert latest is not None
    items = store.list_items(board_id, latest)
    assert [(item.source_ref, item.status) for item in items] == [
        (f"task:{card_id}", "pending")
    ]


# ===========================================================================
# S1.3 — production wiring of CognitiveReadinessService + safe policy rollout
# ===========================================================================


class _AllowGate:
    """Legacy closeout gate stub that always ALLOWS, so these tests isolate the
    NEW readiness wiring (DLQ / canonical_debt OPEN / skip-expired tiers)."""

    def evaluate(self, **_kwargs):
        return SimpleNamespace(allowed=True, reason="ok", blocking_count=0)


_APPROVE_VALIDATION = {
    "confidence": 99,
    "confidence_justification": "strong",
    "estimated_completeness": 100,
    "completeness_justification": "complete",
    "estimated_drift": 0,
    "drift_justification": "none",
    "recommendation": "approve",
    "general_justification": "ready",
}


def _enable_global_blocking_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """Flip the default-OFF global feature flag on the cached settings instance."""
    from okto_pulse.core.infra import config as config_mod

    settings = config_mod.get_settings()
    monkeypatch.setattr(
        settings, "cognitive_readiness_blocking_enabled", True, raising=False
    )


async def _seed_open_debt(board_id: str, card_id: str) -> None:
    db_factory = get_session_factory()
    async with db_factory() as db:
        await upsert_canonical_debt(
            db, board_id=board_id, artifact_type="task", artifact_id=card_id,
            source_ref=f"task:{card_id}", content_hash="h",
            target_status="done", canonical_state="failed",  # OPEN
        )
        await db.commit()


async def _seed_dlq(board_id: str, card_id: str) -> None:
    db_factory = get_session_factory()
    async with db_factory() as db:
        db.add(ConsolidationDeadLetter(
            id=_id(), board_id=board_id, artifact_type="task", artifact_id=card_id,
            original_queue_id=_id(), attempts=3,
            errors=[{"attempt": 1, "error_type": "X", "message": "boom"}],
        ))
        await db.commit()


# --- policy helper unit (safe rollout: default advisory, two-key blocking) ---


def test_board_policy_defaults_to_advisory():
    assert _board_cognitive_readiness_policy(None) == "advisory"
    assert _board_cognitive_readiness_policy(
        SimpleNamespace(settings=None)
    ) == "advisory"
    assert _board_cognitive_readiness_policy(
        SimpleNamespace(settings={"cognitive_readiness_policy": "blocking"})
    ) == "blocking"
    # unknown value falls back to advisory
    assert _board_cognitive_readiness_policy(
        SimpleNamespace(settings={"cognitive_readiness_policy": "weird"})
    ) == "advisory"


def test_blocking_active_needs_both_flag_and_policy(monkeypatch: pytest.MonkeyPatch):
    blocking_board = SimpleNamespace(settings={"cognitive_readiness_policy": "blocking"})
    advisory_board = SimpleNamespace(settings={})
    # global flag OFF (default) → never active
    assert _cognitive_readiness_blocking_active(blocking_board) is False
    # flag ON but policy advisory → not active
    _enable_global_blocking_flag(monkeypatch)
    assert _cognitive_readiness_blocking_active(advisory_board) is False
    # flag ON + policy blocking → active
    assert _cognitive_readiness_blocking_active(blocking_board) is True


# --- done-transition wiring (blocking policy active) ---


@pytest.mark.asyncio
async def test_done_blocks_on_open_canonical_debt_without_active_item(
    isolated_closeout_kg_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    board_id, _, card_id = await _seed_card(
        CardType.NORMAL, CardStatus.VALIDATION,
        board_settings={"cognitive_readiness_policy": "blocking"},
    )
    await _seed_open_debt(board_id, card_id)
    _enable_global_blocking_flag(monkeypatch)

    db_factory = get_session_factory()
    async with db_factory() as db:
        await _mark_card_resources_na(db, board_id, card_id)
        await db.commit()
    async with db_factory() as db:
        service = CardService(db)
        service._cognitive_closeout_gate_factory = lambda: _AllowGate()
        with pytest.raises(ValueError, match="canonical_debt_open"):
            await service.submit_task_validation(
                card_id=card_id, reviewer_id=USER_ID, reviewer_name=USER_ID,
                data=_APPROVE_VALIDATION,
            )
        await db.rollback()

    card = await _card_row(card_id)
    assert card.status == CardStatus.VALIDATION  # blocked before status mutation
    assert card.validations in (None, [])


@pytest.mark.asyncio
async def test_done_blocks_on_technical_dlq_before_status_mutation(
    isolated_closeout_kg_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    board_id, _, card_id = await _seed_card(
        CardType.NORMAL, CardStatus.VALIDATION,
        board_settings={
            "cognitive_readiness_policy": "blocking",
            # Isolate readiness from the independent task-validation gate.
            "require_task_validation": False,
        },
    )
    await _seed_dlq(board_id, card_id)
    _enable_global_blocking_flag(monkeypatch)

    db_factory = get_session_factory()
    async with db_factory() as db:
        service = CardService(db)
        service._cognitive_closeout_gate_factory = lambda: _AllowGate()
        with pytest.raises(ValueError, match="technical_dlq"):
            await service.move_card(
                card_id=card_id, user_id=USER_ID,
                data=CardMove(
                    status=CardStatus.DONE, conclusion="impl",
                    completeness=100, completeness_justification="c",
                    drift=0, drift_justification="n",
                ),
                actor_name=USER_ID,
            )
        await db.rollback()

    card = await _card_row(card_id)
    assert card.status == CardStatus.VALIDATION
    assert card.conclusions in (None, [])


@pytest.mark.asyncio
async def test_advisory_default_board_does_not_block_on_open_debt(
    isolated_closeout_kg_dir: Path,
) -> None:
    # default board (no policy set) → advisory → readiness wiring is a NO-OP even
    # with open canonical_debt; done succeeds (rollout safety for existing boards).
    board_id, _, card_id = await _seed_card(CardType.NORMAL, CardStatus.VALIDATION)
    await _seed_open_debt(board_id, card_id)

    db_factory = get_session_factory()
    async with db_factory() as db:
        await _mark_card_resources_na(db, board_id, card_id)
        await db.commit()
    async with db_factory() as db:
        service = CardService(db)
        service._cognitive_closeout_gate_factory = lambda: _AllowGate()
        result = await service.submit_task_validation(
            card_id=card_id, reviewer_id=USER_ID, reviewer_name=USER_ID,
            data=_APPROVE_VALIDATION,
        )
        await db.commit()
        assert result["card_status"] == CardStatus.DONE.value

    assert (await _card_row(card_id)).status == CardStatus.DONE


@pytest.mark.asyncio
async def test_blocking_policy_no_debt_passes(
    isolated_closeout_kg_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # policy blocking + flag on, but NO debt/DLQ/active item → readiness ready,
    # done succeeds.
    board_id, _, card_id = await _seed_card(
        CardType.NORMAL, CardStatus.VALIDATION,
        board_settings={"cognitive_readiness_policy": "blocking"},
    )
    _enable_global_blocking_flag(monkeypatch)

    db_factory = get_session_factory()
    async with db_factory() as db:
        await _mark_card_resources_na(db, board_id, card_id)
        await db.commit()
    async with db_factory() as db:
        service = CardService(db)
        service._cognitive_closeout_gate_factory = lambda: _AllowGate()
        result = await service.submit_task_validation(
            card_id=card_id, reviewer_id=USER_ID, reviewer_name=USER_ID,
            data=_APPROVE_VALIDATION,
        )
        await db.commit()
        assert result["card_status"] == CardStatus.DONE.value

    assert (await _card_row(card_id)).status == CardStatus.DONE


class _ExplodingReadiness:
    """Readiness service whose evaluation always fails — to prove blocking-active
    enforcement is fail-CLOSED (visible) rather than a silent skip."""

    async def evaluate_artifact(self, *_a, **_k):
        raise RuntimeError("readiness backend down")


@pytest.mark.asyncio
async def test_blocking_active_fails_closed_when_readiness_service_errors(
    isolated_closeout_kg_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # blocking policy + flag on + readiness service explodes → done MUST block
    # with cognitive_readiness_unavailable BEFORE any status/validation mutation.
    board_id, _, card_id = await _seed_card(
        CardType.NORMAL, CardStatus.VALIDATION,
        board_settings={"cognitive_readiness_policy": "blocking"},
    )
    _enable_global_blocking_flag(monkeypatch)

    db_factory = get_session_factory()
    async with db_factory() as db:
        await _mark_card_resources_na(db, board_id, card_id)
        await db.commit()
    async with db_factory() as db:
        service = CardService(db)
        service._cognitive_closeout_gate_factory = lambda: _AllowGate()
        service._cognitive_readiness_service_factory = lambda: _ExplodingReadiness()
        with pytest.raises(ValueError, match="cognitive_readiness_unavailable"):
            await service.submit_task_validation(
                card_id=card_id, reviewer_id=USER_ID, reviewer_name=USER_ID,
                data=_APPROVE_VALIDATION,
            )
        await db.rollback()

    card = await _card_row(card_id)
    assert card.status == CardStatus.VALIDATION
    assert card.validations in (None, [])


@pytest.mark.asyncio
async def test_advisory_default_does_not_instantiate_readiness_service(
    isolated_closeout_kg_dir: Path,
) -> None:
    # default (advisory) board → the wiring is a NO-OP that never even touches
    # the readiness factory; done succeeds even if the factory would explode.
    board_id, _, card_id = await _seed_card(CardType.NORMAL, CardStatus.VALIDATION)

    def _boom_factory():
        raise AssertionError(
            "readiness factory must not be called under advisory policy"
        )

    db_factory = get_session_factory()
    async with db_factory() as db:
        await _mark_card_resources_na(db, board_id, card_id)
        await db.commit()
    async with db_factory() as db:
        service = CardService(db)
        service._cognitive_closeout_gate_factory = lambda: _AllowGate()
        service._cognitive_readiness_service_factory = _boom_factory
        result = await service.submit_task_validation(
            card_id=card_id, reviewer_id=USER_ID, reviewer_name=USER_ID,
            data=_APPROVE_VALIDATION,
        )
        await db.commit()
        assert result["card_status"] == CardStatus.DONE.value

    assert (await _card_row(card_id)).status == CardStatus.DONE


@pytest.mark.asyncio
async def test_blocking_active_fails_closed_when_source_ref_resolution_errors(
    isolated_closeout_kg_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # blocking policy + flag on + source-ref resolution explodes (non-unsupported)
    # → done MUST fail-closed with cognitive_readiness_unavailable, no mutation.
    board_id, _, card_id = await _seed_card(
        CardType.NORMAL, CardStatus.VALIDATION,
        board_settings={"cognitive_readiness_policy": "blocking"},
    )
    _enable_global_blocking_flag(monkeypatch)

    import okto_pulse.core.kg.cognitive_closeout_gate as ccg_mod

    def _boom_resolve(**_kwargs):
        raise RuntimeError("source ref backend down")

    monkeypatch.setattr(ccg_mod, "resolve_cognitive_source_refs", _boom_resolve)

    db_factory = get_session_factory()
    async with db_factory() as db:
        await _mark_card_resources_na(db, board_id, card_id)
        await db.commit()
    async with db_factory() as db:
        service = CardService(db)
        service._cognitive_closeout_gate_factory = lambda: _AllowGate()
        with pytest.raises(ValueError, match="cognitive_readiness_unavailable"):
            await service.submit_task_validation(
                card_id=card_id, reviewer_id=USER_ID, reviewer_name=USER_ID,
                data=_APPROVE_VALIDATION,
            )
        await db.rollback()

    card = await _card_row(card_id)
    assert card.status == CardStatus.VALIDATION
    assert card.validations in (None, [])
