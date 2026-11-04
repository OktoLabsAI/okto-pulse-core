"""Human declarations cannot forge agent authority or weaken delivery gates."""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from okto_pulse.core.application.use_cases import human_task_submission as use_cases
from okto_pulse.core.application.use_cases.base import PermissionDeniedError
from okto_pulse.core.models.delivery_evidence import CardDeliveryEvidenceCommand
from okto_pulse.core.models.human_task_submission import (
    HumanSubmissionReceipt, HumanTaskSubmissionCommand, HumanTaskSubmissionInput,
    canonical_submission_json, submission_digest,
)


def body(**changes):
    return dict(contract_version="human-task-submission/v1", action="progress",
                content="Atualizei a tela", expected_card_version=4, expected_spec_edition=2,
                idempotency_key="human-intent-0001", **changes)


def command(**changes):
    value = body()
    value.update(changes)
    return HumanTaskSubmissionCommand(board_id="board", card_id="card", spec_id="spec", **value)


@pytest.mark.parametrize("field", ["receipt_id", "actor_id", "actor_kind", "received_at",
                                  "authority", "origin", "content_sha256", "execution_id",
                                  "material_change", "impact_evidence", "completeness", "drift"])
def test_client_cannot_supply_system_metadata_or_technical_claims(field):
    with pytest.raises(ValidationError, match="extra_forbidden"):
        HumanTaskSubmissionInput.model_validate(body(**{field: "forged"}))


@pytest.mark.parametrize("changes", [
    {"content": " \r\n\t "}, {"content": "a" * 20001}, {"remaining": "a" * 8001},
    {"expected_card_version": 0}, {"expected_card_version": True}, {"expected_spec_edition": "1"},
    {"expected_spec_edition": -1}, {"idempotency_key": "short"},
    {"idempotency_key": "chave-não-ascii-0001"}, {"idempotency_key": "a" * 129},
    {"idempotency_key": "a" * 16 + "\n"}, {"requested_status": None},
    {"requested_status": "done"}, {"action": "delivery_report"},
    {"action": "delivery_report", "requested_status": "cancelled"},
])
def test_invalid_intents_are_rejected_before_storage(changes):
    value = body()
    value.update(changes)
    with pytest.raises(ValidationError):
        HumanTaskSubmissionInput.model_validate(value)


def test_normalization_and_canonical_hash_are_portable_and_include_all_fences():
    first = command(content="  cafe\u0301\r\nfeito\rrestante  ", remaining="  revisão\r\n ")
    second = command(content="café\nfeito\nrestante", remaining="revisão")
    assert first.content == second.content == "café\nfeito\nrestante"
    assert first.content_digest() == second.content_digest()
    assert first.intent_digest() == second.intent_digest()
    assert canonical_submission_json({"z": "é", "a": 1}) == b'{"a":1,"z":"\xc3\xa9"}'
    for field, value in [("expected_card_version", 5), ("expected_spec_edition", 3),
                         ("board_id", "other"), ("card_id", "other"), ("spec_id", "other"),
                         ("supersedes_submission_id", "prior")]:
        changed = second.model_copy(update={field: value})
        assert changed.intent_digest() != second.intent_digest()
    with pytest.raises(ValueError):
        canonical_submission_json({"not_a_number": float("nan")})


def test_human_receipt_is_immutable_and_not_a_technical_evidence_kind():
    receipt = HumanSubmissionReceipt(id="receipt", submission_id="submission", board_id="board",
        card_id="card", spec_id="spec", spec_edition=2, actor_id="person", actor_kind="user",
        received_at=datetime.now(timezone.utc), action="progress", before_card_version=4,
        after_card_version=4, content_sha256=submission_digest({}), intent_sha256=submission_digest({}))
    assert receipt.authority == "human_declaration"
    with pytest.raises(ValidationError):
        receipt.authority = "technical_execution"
    with pytest.raises(ValidationError):
        HumanSubmissionReceipt.model_validate({**receipt.model_dump(), "actor_kind": "agent"})
    with pytest.raises(ValidationError):
        HumanSubmissionReceipt.model_validate({**receipt.model_dump(), "received_at": datetime(2026, 1, 1)})
    with pytest.raises(ValidationError):
        CardDeliveryEvidenceCommand(board_id="board", card_id="card", spec_id="spec",
            expected_card_version=4, expected_spec_edition=2, idempotency_key="test",
            kind="human_submission", justification="A human statement is not proof")


def setup(monkeypatch, *, kind="user", fail_permission=None, store_error=None):
    events = []

    async def authorize(actor, requirement, **_):
        events.append(requirement.operation)
        if requirement.operation == fail_permission:
            raise PermissionDeniedError("revoked")

    async def record(*args, **kwargs):
        events.append("record_or_replay")
        if store_error:
            raise store_error
        return {"submission_id": "s", "receipt_id": "r", "replayed": True}

    monkeypatch.setattr(use_cases, "require_authorization", authorize)
    store = SimpleNamespace(record_human_submission=AsyncMock(side_effect=record))
    uow = SimpleNamespace(services=SimpleNamespace(delivery_evidence=store),
                          begin_write=AsyncMock(), commit=AsyncMock(), rollback=AsyncMock())
    actor = SimpleNamespace(actor_id="person", actor_kind=kind, actor_name="Person", source="rest")
    return actor, uow, events


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["agent", "system", "unknown"])
async def test_nonhuman_principal_is_rejected_even_on_rest(monkeypatch, kind):
    actor, uow, events = setup(monkeypatch, kind=kind)
    with pytest.raises(PermissionDeniedError, match="human_authorization_required"):
        await use_cases.SubmitHumanTaskUseCase().execute(command(), actor=actor, uow=uow)
    assert events == []
    uow.begin_write.assert_not_awaited()
    uow.services.delivery_evidence.record_human_submission.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["human", "user"])
async def test_progress_reauthorizes_before_replay_and_commits_once(monkeypatch, kind):
    actor, uow, events = setup(monkeypatch, kind=kind)
    result = await use_cases.SubmitHumanTaskUseCase().execute(command(), actor=actor, uow=uow)
    assert events == ["card.entity.read", "spec.entity.read", "card.conclusion.write", "record_or_replay"]
    assert result == {"submission_id": "s", "receipt_id": "r", "replayed": True}
    assert uow.services.delivery_evidence.record_human_submission.call_args.kwargs["report_submitter"] is None
    uow.commit.assert_awaited_once()
    uow.rollback.assert_not_awaited()


@pytest.mark.asyncio
async def test_report_permission_revocation_prevents_even_replay_read(monkeypatch):
    actor, uow, events = setup(monkeypatch, fail_permission="card.move.in_progress_to_done")
    with pytest.raises(PermissionDeniedError):
        await use_cases.SubmitHumanTaskUseCase().execute(
            command(action="delivery_report", requested_status="done"), actor=actor, uow=uow)
    assert events[-1] == "card.move.in_progress_to_done"
    uow.services.delivery_evidence.record_human_submission.assert_not_awaited()
    uow.commit.assert_not_awaited()
    uow.rollback.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [ValueError("gate_blocked"), RuntimeError("storage_unavailable")])
async def test_failure_rolls_back_and_never_falls_back_to_progress(monkeypatch, failure):
    actor, uow, events = setup(monkeypatch, store_error=failure)
    with pytest.raises(type(failure), match=str(failure)):
        await use_cases.SubmitHumanTaskUseCase().execute(
            command(action="delivery_report", requested_status="validation"), actor=actor, uow=uow)
    assert events.count("record_or_replay") == 1
    uow.commit.assert_not_awaited()
    uow.rollback.assert_awaited_once()


@pytest.mark.asyncio
async def test_commit_failure_is_not_a_success_response(monkeypatch):
    actor, uow, _ = setup(monkeypatch)
    uow.commit.side_effect = RuntimeError("commit_failed")
    with pytest.raises(RuntimeError, match="commit_failed"):
        await use_cases.SubmitHumanTaskUseCase().execute(command(), actor=actor, uow=uow)
    uow.rollback.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["accepted", "replayed", "rejected", "failed"])
async def test_operational_telemetry_is_after_commit_and_excludes_human_content(monkeypatch, caplog, outcome):
    actor, uow, _ = setup(monkeypatch)
    secret = "private-human-text-and-secret"
    cmd = command(content=secret, remaining=secret, idempotency_key="private-idempotency-key")
    result = {"submission_id": "s", "receipt_id": "r", "replayed": outcome == "replayed"}
    uow.services.delivery_evidence.record_human_submission = AsyncMock(return_value=result)
    committed = []

    async def commit_write():
        assert not [r for r in caplog.records if getattr(r, "submission_outcome", None) in {"accepted", "replayed"}]
        if outcome == "failed":
            raise RuntimeError(secret)
        committed.append(True)

    uow.commit.side_effect = commit_write
    if outcome == "rejected":
        uow.services.delivery_evidence.record_human_submission.side_effect = PermissionDeniedError(secret)
    with caplog.at_level("INFO", logger=use_cases.__name__):
        if outcome in {"rejected", "failed"}:
            with pytest.raises((PermissionDeniedError, RuntimeError)):
                await use_cases.SubmitHumanTaskUseCase().execute(cmd, actor=actor, uow=uow)
        else:
            await use_cases.SubmitHumanTaskUseCase().execute(cmd, actor=actor, uow=uow)
    records = [r for r in caplog.records if hasattr(r, "submission_outcome")]
    assert len(records) == 1
    assert records[0].submission_outcome == outcome
    assert records[0].submission_action == "progress"
    assert committed == ([True] if outcome in {"accepted", "replayed"} else [])
    assert secret not in str(records[0].__dict__)
    assert cmd.idempotency_key not in str(records[0].__dict__)
