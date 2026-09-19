from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from okto_pulse.core.models.delivery_evidence import (
    CardDeliveryEvidenceBatchCommand,
    CardDeliveryEvidenceCommand,
    card_delivery_command,
)
from okto_pulse.core.application.use_cases.delivery_evidence import (
    RecordCardDeliveryEvidenceUseCase,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)


def progress(ref="first"):
    return {
        "client_ref": ref,
        "kind": "progress",
        "justification": "Partial parser",
        "progress": {
            "source_state": {
                "workspace_state": "dirty",
                "recoverability": "external_workspace",
            },
            "remaining": "Normalize",
        },
    }


def batch(**changes):
    return CardDeliveryEvidenceBatchCommand.model_validate(
        {
            "board_id": "b",
            "card_id": "c",
            "spec_id": "s",
            "contract_version": "card-delivery-batch/v1",
            "expected_card_version": 1,
            "expected_spec_edition": 1,
            "expected_delivery_revision": 0,
            "idempotency_key": "batch",
            "entries": [progress()],
            **changes,
        }
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"entries": []},
        {"entries": [progress()] * 51},
        {"entries": [progress(), progress()]},
        {"expected_delivery_revision": True},
        {"expected_delivery_revision": -1},
        {"actor_id": "forged"},
        {"verified": True},
        {"entries": [{**progress(), "kind": "revoke", "record_id": "old"}]},
        {"entries": [{**progress(), "board_id": "foreign"}]},
        {"entries": [{**progress(), "idempotency_key": "per-entry"}]},
        {"entries": [{**progress(), "client_ref": "../foreign"}]},
    ],
)
def test_batch_is_closed_and_single_scope(changes):
    with pytest.raises(ValidationError):
        batch(**changes)


def test_aggregate_link_and_byte_limits_apply_to_entire_batch():
    entries = [
        {**progress(str(i)), "obligation_refs": [f"fr:{j}" for j in range(100)]}
        for i in range(3)
    ]
    with pytest.raises(ValidationError, match="batch_payload_limit"):
        batch(entries=entries)
    entries = [{**progress(str(i)), "justification": "界" * 15000} for i in range(3)]
    with pytest.raises(ValidationError, match="batch_payload_limit"):
        batch(entries=entries)


@pytest.mark.parametrize(
    "card_type,kind",
    [
        ("normal", "test"),
        ("bug", "test"),
        ("test", "implementation"),
        ("unknown", "progress"),
    ],
)
def test_grouping_cannot_borrow_another_card_types_authority(card_type, kind):
    from okto_pulse.core.domain.delivery_evidence import (
        require_delivery_entry_card_type,
    )

    with pytest.raises(ValueError, match="card_type_invalid"):
        require_delivery_entry_card_type(card_type, kind)


def test_one_and_many_use_same_builder_without_changing_legacy_shape():
    value = batch()
    assert (
        card_delivery_command(
            board_id="b",
            card_id="c",
            spec_id="s",
            evidence=value.model_dump(exclude={"board_id", "card_id", "spec_id"}),
        )
        == value
    )
    legacy = CardDeliveryEvidenceCommand(
        board_id="b",
        card_id="c",
        spec_id="s",
        expected_card_version=1,
        expected_spec_edition=1,
        idempotency_key="old",
        kind="implementation",
        obligation_refs=["fr:x"],
        execution_id="execution",
        justification="Existing proof",
    )
    assert "progress" not in legacy.model_dump()
    assert (
        card_delivery_command(
            board_id="b",
            card_id="c",
            spec_id="s",
            evidence=legacy.model_dump(exclude={"board_id", "card_id", "spec_id"}),
        )
        == legacy
    )


@pytest.mark.asyncio
async def test_all_subactions_are_authorized_before_any_store_call():
    actor = ActorContext(
        actor_id="agent",
        source="mcp",
        actor_kind="agent",
        board_id="b",
        permissions=["card.conclusion.write"],
    )
    store = SimpleNamespace(record_card=AsyncMock())
    uow = SimpleNamespace(
        services=SimpleNamespace(delivery_evidence=store), commit=AsyncMock()
    )
    command = batch(
        entries=[
            progress(),
            {
                "client_ref": "proof",
                "kind": "implementation",
                "obligation_refs": ["fr:x"],
                "execution_id": "execution",
                "justification": "Proof",
            },
        ]
    )
    with pytest.raises(PermissionDeniedError):
        await RecordCardDeliveryEvidenceUseCase().execute(command, actor=actor, uow=uow)
    store.record_card.assert_not_awaited()
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_authorized_batch_commits_once_and_replay_rechecks_authority(monkeypatch):
    from okto_pulse.core.application.use_cases import delivery_evidence as app

    store = SimpleNamespace(
        record_card=AsyncMock(
            return_value={
                "entries": [{"id": "saved", "client_ref": "first"}],
                "replayed": True,
            }
        )
    )
    uow = SimpleNamespace(
        services=SimpleNamespace(delivery_evidence=store), commit=AsyncMock()
    )
    actor = ActorContext(
        actor_id="agent",
        source="mcp",
        actor_kind="agent",
        board_id="b",
        permissions=["card.conclusion.write"],
    )
    commit = AsyncMock()
    monkeypatch.setattr(app, "commit", commit)
    await RecordCardDeliveryEvidenceUseCase().execute(batch(), actor=actor, uow=uow)
    commit.assert_awaited_once_with(uow)
    actor.permissions = ["board.read"]
    with pytest.raises(PermissionDeniedError):
        await RecordCardDeliveryEvidenceUseCase().execute(batch(), actor=actor, uow=uow)
    store.record_card.assert_awaited_once()
