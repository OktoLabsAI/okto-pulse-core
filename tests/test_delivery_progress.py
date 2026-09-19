from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from okto_pulse.core.models.delivery_evidence import CardDeliveryEvidenceCommand
from okto_pulse.core.domain.delivery_progress import require_delivery_progress_mutable
from okto_pulse.core.application.use_cases.delivery_evidence import (
    RecordCardDeliveryEvidenceUseCase,
)


def command(**changes):
    return CardDeliveryEvidenceCommand.model_validate(
        {
            "board_id": "board",
            "card_id": "card",
            "spec_id": "spec",
            "expected_card_version": 3,
            "expected_spec_edition": 1,
            "idempotency_key": "checkpoint-1",
            "kind": "progress",
            "justification": "Parser changed, normalization still missing",
            "progress": {
                "source_state": {
                    "workspace_state": "dirty",
                    "recoverability": "external_workspace",
                },
                "remaining": "Implement normalization",
            },
            **changes,
        }
    )


def test_dirty_progress_does_not_require_execution_commit_or_obligation():
    value = command()
    assert value.execution_id is None and value.obligation_refs == []
    assert value.progress.source_state.declared_revision is None


@pytest.mark.parametrize(
    "changes",
    [
        {"execution_id": "receipt"},
        {"implementation_ids": ["proof"]},
        {"scenario_id": "scenario"},
        {"verified": True},
        {"progress": None},
        {
            "kind": "implementation",
            "execution_id": "receipt",
            "obligation_refs": ["fr:x"],
        },
        {
            "progress": {
                "source_state": {
                    "workspace_state": "dirty",
                    "recoverability": "declared_commit",
                },
                "remaining": "Next",
            }
        },
        {
            "progress": {
                "source_state": {
                    "workspace_state": "dirty",
                    "recoverability": "unknown",
                    "source_ref": "D:/private/repo",
                },
                "remaining": "Next",
            }
        },
        {
            "progress": {
                "source_state": {
                    "workspace_state": "unknown",
                    "recoverability": "unknown",
                },
                "remaining": "Next",
                "complete": True,
            }
        },
    ],
)
def test_progress_contract_rejects_authority_or_ambiguous_proof(changes):
    with pytest.raises(ValidationError):
        command(**changes)


def test_legacy_serialization_keeps_prior_digest_shape():
    value = command(
        kind="implementation",
        progress=None,
        execution_id="receipt",
        obligation_refs=["fr:x"],
    )
    assert "progress" not in value.model_dump()
    assert "progress" not in value.model_dump_json()


@pytest.mark.parametrize(
    "status", ["not_started", "validation", "rejected", "done", "cancelled", "on_hold"]
)
def test_progress_cannot_bypass_execution_lifecycle(status):
    with pytest.raises(ValueError, match="execution_state"):
        require_delivery_progress_mutable(
            SimpleNamespace(status=status, card_type="normal", archived=False)
        )


@pytest.mark.asyncio
async def test_progress_uses_report_authority_and_single_commit(monkeypatch):
    from okto_pulse.core.application.use_cases import delivery_evidence as module

    authorize, commit = AsyncMock(), AsyncMock()
    monkeypatch.setattr(module, "require_authorization", authorize)
    monkeypatch.setattr(module, "commit", commit)
    store = SimpleNamespace(record_card=AsyncMock(return_value={"id": "saved"}))
    uow = SimpleNamespace(services=SimpleNamespace(delivery_evidence=store))
    actor = SimpleNamespace(actor_id="author", actor_kind="agent")
    await RecordCardDeliveryEvidenceUseCase().execute(command(), actor=actor, uow=uow)
    assert authorize.await_args.args[1].operation == "card.conclusion.write"
    store.record_card.assert_awaited_once()
    commit.assert_awaited_once_with(uow)


def test_aggregate_byte_limit_and_impact_path_validation():
    value = command().model_dump()
    value["progress"]["impact_delta"] = {
        "files": [{"repo": "core", "path": "../secret", "change_kind": "modified"}]
    }
    with pytest.raises(ValidationError):
        CardDeliveryEvidenceCommand.model_validate(value)
    value["progress"]["impact_delta"] = {
        "files": [
            {
                "repo": "core",
                "path": "src/a.py",
                "change_kind": "modified",
                "note": "界" * 1800,
            }
        ]
        * 100
    }
    with pytest.raises(ValidationError, match="payload_limit"):
        CardDeliveryEvidenceCommand.model_validate(value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "permissions",
    [
        ["board.read"],
        ["code_traceability.target.execution_submit"],
        ["spec.tests.execute"],
    ],
)
async def test_other_authorities_do_not_grant_progress_write(permissions):
    from okto_pulse.core.application.use_cases.base import (
        ActorContext,
        PermissionDeniedError,
    )

    store = SimpleNamespace(record_card=AsyncMock())
    uow = SimpleNamespace(services=SimpleNamespace(delivery_evidence=store))
    actor = ActorContext(
        actor_id="author",
        source="mcp",
        actor_kind="agent",
        board_id="board",
        permissions=permissions,
    )
    with pytest.raises(PermissionDeniedError):
        await RecordCardDeliveryEvidenceUseCase().execute(
            command(), actor=actor, uow=uow
        )
    store.record_card.assert_not_awaited()
