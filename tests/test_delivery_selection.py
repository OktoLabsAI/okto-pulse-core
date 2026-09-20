from copy import deepcopy
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.delivery_evidence import CardDeliveryScope
from okto_pulse.core.domain.delivery_selection import current_delivery_selection, seal_delivery_selection
from okto_pulse.core.models.delivery_selection import DeliverySelectionInput
from okto_pulse.core.models.schemas import CardMove
from test_delivery_evidence_domain import SNAPSHOT

SCOPE = CardDeliveryScope("b", "c", "s", 1)
RECORD = dict(id="record", kind="implementation", sha256="a" * 64)


def report():
    manifest = seal_delivery_selection(scope=SCOPE, card_version=1, revision=2,
        records=[RECORD], obligations=SNAPSHOT.obligations, impact=None)
    return dict(source="move_to_validation", delivery_manifest=manifest)


def selected(value, *, status="validation", hashes=None):
    return current_delivery_selection(SimpleNamespace(status=status, conclusions=[value]), SCOPE,
        obligations=SNAPSHOT.obligations, record_hashes=hashes or {"record": "a" * 64})


def test_manifest_pins_exact_records_and_rework_preserves_historical_report():
    value = report()
    original = deepcopy(value)
    assert selected(value) == {"record"}
    assert selected(value, status="done") == {"record"}
    assert selected(value, status="rejected") == {"record"}
    assert selected(value, status="in_progress") is None
    assert value == original
    assert selected(dict(source="move_to_validation")) is None


@pytest.mark.parametrize("corrupt", [False, True])
def test_prospective_report_checks_selection_without_changing_history(corrupt):
    card = SimpleNamespace(status="in_progress", conclusions=[{"text": "Prior report"}])
    original = deepcopy(card.__dict__)
    value = report()
    if corrupt:
        value["delivery_manifest"]["sha256"] = "0" * 64
        with pytest.raises(ValueError, match="manifest_invalid"):
            current_delivery_selection(card, SCOPE, obligations=SNAPSHOT.obligations,
                record_hashes={"record": "a" * 64}, prospective_report=value)
    else:
        assert current_delivery_selection(card, SCOPE, obligations=SNAPSHOT.obligations,
            record_hashes={"record": "a" * 64}, prospective_report=value) == {"record"}
    assert card.__dict__ == original


@pytest.mark.parametrize("field,value", [
    ("spec_edition", 2), ("card_id", "foreign"), ("scope_sha256", "b" * 64),
    ("sha256", "b" * 64), ("contract_version", "future"),
])
def test_manifest_corruption_cannot_fall_back_to_all_records(field, value):
    data = report()
    data["delivery_manifest"][field] = value
    with pytest.raises(ValueError, match="manifest_invalid"):
        selected(data)


def test_record_or_impact_change_is_not_the_sealed_submission():
    with pytest.raises(ValueError, match="manifest_invalid"):
        selected(report(), hashes={"record": "b" * 64})
    data = report()
    data["impact_evidence"] = {"files": []}
    with pytest.raises(ValueError, match="manifest_invalid"):
        selected(data)


@pytest.mark.parametrize("patch", [
    {"record_ids": ["same", "same"]}, {"record_ids": [str(i) for i in range(201)]},
    {"sha256": "a" * 64}, {"expected_delivery_revision": True},
    {"expected_spec_edition": 0}, {"current": True},
])
def test_selection_is_closed_and_bounded(patch):
    with pytest.raises(ValidationError):
        DeliverySelectionInput.model_validate({"expected_card_version": 1, "expected_spec_edition": 1,
            "expected_delivery_revision": 0, "record_ids": [], **patch})


def test_move_schema_exposes_selection_outside_placement_one_of():
    schema = CardMove.model_json_schema()
    assert "delivery_selection" in schema["properties"]
    request = CardMove(status="validation", delivery_selection=dict(
        expected_card_version=1, expected_spec_edition=1, expected_delivery_revision=0, record_ids=[]))
    assert request.delivery_selection.record_ids == []


@pytest.mark.asyncio
@pytest.mark.parametrize("stale", [False, True])
async def test_report_writer_persists_port_manifest_or_refuses_conflict_without_report(db_factory, stale, monkeypatch):
    from test_impact_evidence_enforcement import _card_in_progress, _REPORT_KWARGS
    from test_card_lifecycle import USER_ID
    from okto_pulse.core.services.main import CardService
    from okto_pulse.core.services import delivery_evidence as delivery_service
    from unittest.mock import AsyncMock
    card_id = await _card_in_progress(db_factory, mode="off")
    async with db_factory() as session:
        service = CardService(session)
        card = await service.get_card(card_id)
        manifest = seal_delivery_selection(scope=CardDeliveryScope(card.board_id, card.id, card.spec_id, 1),
            card_version=card.policy_version, revision=1, records=[RECORD], obligations=SNAPSHOT.obligations, impact=None)
        seal = AsyncMock(return_value=manifest, side_effect=ValueError("delivery_revision_conflict") if stale else None)
        monkeypatch.setattr(delivery_service, "card_delivery_store", lambda _: SimpleNamespace(seal_selection=seal))
        request = CardMove(status="validation", **_REPORT_KWARGS, delivery_selection=dict(
            expected_card_version=card.policy_version, expected_spec_edition=1,
            expected_delivery_revision=0 if stale else 1, record_ids=["record"]))
        if stale:
            before = list(card.conclusions or [])
            with pytest.raises(ValueError, match="delivery_revision_conflict"):
                await service.move_card(card_id, USER_ID, request)
            await session.commit()
            current = await service.get_card(card_id)
            assert current.status == "in_progress" and (current.conclusions or []) == before
        else:
            moved = await service.move_card(card_id, USER_ID, request)
            await session.commit()
            assert moved.conclusions[-1]["delivery_manifest"] == manifest
            assert seal.await_args.kwargs == dict(expected_status="in_progress", impact=None)
