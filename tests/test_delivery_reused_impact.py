from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.domain.delivery_impact import (
    DeliveryImpactObservation, compose_delivery_impact, reusable_impact_block,
    require_impact_observation, progress_affects_impact_source,
)
from okto_pulse.core.domain.delivery_progress import DeliveryProgress
from okto_pulse.core.domain.delivery_selection import current_delivery_selection, seal_delivery_selection
from okto_pulse.core.models.delivery_selection import DeliverySelectionInput
from okto_pulse.core.models.schemas import CardMove
from test_delivery_net_impact import A, B, claim, file
from test_delivery_selection import SCOPE, RECORD
from test_delivery_evidence_domain import SNAPSHOT


def basis():
    return dict(source_ref="source", source_identity_sha256="a" * 64, base_revision=A, result_revision=B,
                observation_receipt_id="receipt", record_ids=["record"])


def test_reused_block_preserves_authored_scenario_links_and_empty_net_stays_empty():
    projection = compose_delivery_impact((claim("one", A, B, tests=[dict(repo="core", test_file_path="test.py", action="added", scenario_id="scenario")]),))
    assert reusable_impact_block(projection).tests[0].scenario_id == "scenario"
    assert not reusable_impact_block(compose_delivery_impact((claim("one", A, B),))).is_minimally_populated()
    with pytest.raises(ValueError, match="needs_reconciliation"):
        reusable_impact_block(compose_delivery_impact(()))


def test_repo_alias_cannot_silently_refer_to_two_distinct_sources():
    projection = compose_delivery_impact((claim("one", A, B, files=[file("created")]),
        claim("two", A, B, source="other", files=[file("modified", "different.py")])))
    with pytest.raises(ValueError, match="repo_source_ambiguous"):
        reusable_impact_block(projection)


def test_observation_must_match_identity_revision_and_all_material_checkpoints():
    now = datetime.now(timezone.utc)
    observation = DeliveryImpactObservation("receipt", "source", "a" * 64, B, now, True)
    source = dict(source_ref="source", result_revision=B)
    require_impact_observation(source, observation, (now - timedelta(seconds=1),), expected_identity="a" * 64)
    for changed in (None, replace(observation, revision=A), replace(observation, current_accepted_clean=False), replace(observation, source_identity_sha256="b" * 64)):
        with pytest.raises(ValueError, match="current_observation_required"):
            require_impact_observation(source, changed, (), expected_identity="a" * 64)
    with pytest.raises(ValueError, match="material_progress_unobserved"):
        require_impact_observation(source, observation, (now,))
    progress = DeliveryProgress(contract_version="delivery-progress/v2", material_change="targets", target_ids=["independent"],
        source_state=dict(workspace_state="dirty", recoverability="unknown"), remaining="Continue")
    assert not progress_affects_impact_source(progress, "source", {"independent": "other"})
    assert progress_affects_impact_source(progress, "source", {})


def test_v2_seals_basis_without_changing_v1_digest_shape():
    legacy = seal_delivery_selection(scope=SCOPE, card_version=1, revision=1, records=[RECORD], obligations=SNAPSHOT.obligations, impact=None)
    assert "impact_basis" not in legacy
    manifest = seal_delivery_selection(scope=SCOPE, card_version=1, revision=1, records=[RECORD], obligations=SNAPSHOT.obligations, impact=None, impact_basis=[basis()])
    card = SimpleNamespace(status="validation", conclusions=[dict(source="move_to_validation", delivery_manifest=manifest)])
    assert current_delivery_selection(card, SCOPE, obligations=SNAPSHOT.obligations, record_hashes={"record": "a" * 64}) == {"record"}
    manifest["impact_basis"][0]["result_revision"] = A
    with pytest.raises(ValueError, match="manifest_invalid"):
        current_delivery_selection(card, SCOPE, obligations=SNAPSHOT.obligations, record_hashes={"record": "a" * 64})


@pytest.mark.parametrize("extra", [{"reuse_impact": 1}, {"impact_basis": [basis()]}, {"current": True}])
def test_client_cannot_supply_trust_metadata(extra):
    with pytest.raises(ValueError):
        DeliverySelectionInput(expected_card_version=1, expected_spec_edition=1, expected_delivery_revision=1, record_ids=["record"], **extra)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["valid", "empty", "stale", "manual_conflict"])
async def test_real_report_writer_reuses_port_result_under_require_without_retyping(db_factory, monkeypatch, case):
    from test_impact_evidence_enforcement import _card_in_progress, _REPORT_KWARGS, _VALID_BLOCK
    from test_card_lifecycle import USER_ID
    from okto_pulse.core.services.main import CardService
    from okto_pulse.core.services import delivery_evidence as service
    card_id = await _card_in_progress(db_factory, mode="require")
    async with db_factory() as session:
        writer = CardService(session)
        card = await writer.get_card(card_id)
        impact = reusable_impact_block(compose_delivery_impact((claim("record", A, B, files=[] if case == "empty" else [file("created")]),))).model_dump(mode="json", exclude_none=True)
        resolved = dict(impact_evidence=impact, impact_basis=[basis()])
        store = SimpleNamespace(resolve_selection_impact=AsyncMock(return_value=resolved,
            side_effect=ValueError("delivery_impact_current_observation_required") if case == "stale" else None), seal_selection=AsyncMock(return_value={"server": "manifest"}))
        monkeypatch.setattr(service, "card_delivery_store", lambda _: store)
        request = CardMove(status="validation", **_REPORT_KWARGS, impact_evidence=_VALID_BLOCK if case == "manual_conflict" else None,
            delivery_selection=dict(expected_card_version=card.policy_version, expected_spec_edition=1, expected_delivery_revision=1, record_ids=["record"], reuse_impact=True))
        if case != "valid":
            with pytest.raises(ValueError) as error:
                await writer.move_card(card_id, USER_ID, request)
            if case == "empty":
                assert error.value.code == "impact_evidence_required"
            else:
                assert {"stale": "current_observation_required", "manual_conflict": "inputs_conflict"}[case] in str(error.value)
            await session.commit()
            assert (await writer.get_card(card_id)).status == "in_progress"
            store.seal_selection.assert_not_called()
        else:
            moved = await writer.move_card(card_id, USER_ID, request)
            assert moved.conclusions[-1]["impact_evidence"] == impact
            assert store.seal_selection.await_args.kwargs["impact_basis"] == [basis()]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["off", "advisory", "require"])
async def test_completion_impact_policy_is_independent_of_advisory_delivery(db_factory, monkeypatch, mode):
    from test_impact_evidence_enforcement import _card_in_progress, _VALID_BLOCK
    from sqlalchemy_test_models import Board, Card
    from okto_pulse.core.domain.enums import CardStatus
    from okto_pulse.core.services.main import CardService
    from okto_pulse.core.services import delivery_evidence as service
    card_id = await _card_in_progress(db_factory, mode=mode)
    async with db_factory() as session:
        writer = CardService(session)
        stored = await session.get(Card, card_id)
        board = await session.get(Board, stored.board_id)
        original_settings = dict(board.settings)
        board.settings = {**board.settings, "delivery_evidence_gate": "advisory"}
        stored.status = CardStatus.VALIDATION
        stored.conclusions = [dict(source="move_to_validation", impact_evidence=_VALID_BLOCK,
            delivery_manifest=dict(contract_version="card-delivery-selection/v2"))]
        await session.commit()
        card = await writer.get_card(card_id)
        store = SimpleNamespace(report_impact_status=AsyncMock(return_value=dict(current=False, reason="known_source_changed")))
        monkeypatch.setattr(service, "card_delivery_store", lambda _: store)
        try:
            failures = await writer._task_completion_gate_failures(card=card, board=board)
            assert any(row.code == "impact_evidence_required" for row in failures) == (mode == "require")
            assert store.report_impact_status.await_count == int(mode == "require")
            if mode == "require":
                assert store.report_impact_status.await_args.kwargs == {"for_update": True}
        finally:
            # The module-scoped board is shared with the existing gate tests.
            board.settings = original_settings
            await session.commit()
