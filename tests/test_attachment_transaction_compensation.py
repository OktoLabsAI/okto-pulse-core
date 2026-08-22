"""Failure-injection coverage for attachment relational/file compensation."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.card_collaboration import (
    DeleteCardAttachmentCommand,
    DeleteCardAttachmentUseCase,
    UploadCardAttachmentCommand,
    UploadCardAttachmentUseCase,
)
from okto_pulse.core.application.use_cases.mcp_collaboration import (
    McpDeleteAttachmentCommand,
    McpDeleteAttachmentUseCase,
    McpUploadAttachmentCommand,
    McpUploadAttachmentUseCase,
)
from okto_pulse.core.services import main as services_main


BOARD_ID = "attachment-compensation-board"
CARD_ID = "attachment-compensation-card"
ATTACHMENT_ID = "attachment-compensation-id"
ACTOR = ActorContext("attachment-actor", "mcp", board_id=BOARD_ID)


class _MemoryStorage:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.saved_path = f"/opaque/{BOARD_ID}/stored.bin"
        self.deleted: list[str] = []
        self.restored: list[str] = []

    async def save(self, board_id: str, filename: str, content: bytes) -> str:
        del board_id, filename
        self.objects[self.saved_path] = content
        return self.saved_path

    async def load(self, path: str) -> bytes:
        return self.objects[path]

    async def delete(self, path: str) -> bool:
        self.deleted.append(path)
        return self.objects.pop(path, None) is not None

    async def restore(self, path: str, content: bytes) -> None:
        self.restored.append(path)
        self.objects[path] = content


@pytest.mark.asyncio
async def test_upload_staging_failure_removes_unowned_object(monkeypatch) -> None:
    storage = _MemoryStorage()
    card = SimpleNamespace(id=CARD_ID, board_id=BOARD_ID)

    async def _get(*_args, **_kwargs):
        return card

    async def _add(*_args, **_kwargs):
        raise RuntimeError("injected attachment add failure")

    monkeypatch.setattr(services_main, "get_storage_provider", lambda: storage)
    monkeypatch.setattr(services_main, "_application_get", _get)
    monkeypatch.setattr(services_main, "_application_add", _add)

    with pytest.raises(RuntimeError, match="injected attachment add failure"):
        await services_main.AttachmentService(object()).upload_attachment(
            card_id=CARD_ID,
            user_id=ACTOR.actor_id,
            filename="evidence.txt",
            content=b"evidence",
            mime_type="text/plain",
        )

    assert storage.objects == {}
    assert storage.deleted == [storage.saved_path]


@pytest.mark.asyncio
async def test_delete_staging_failure_restores_exact_object(monkeypatch) -> None:
    storage = _MemoryStorage()
    storage.objects[storage.saved_path] = b"evidence"
    attachment = SimpleNamespace(
        id=ATTACHMENT_ID,
        card_id=CARD_ID,
        path=storage.saved_path,
    )

    async def _get(_db, entity_type, _entity_id):
        return attachment if entity_type == "attachment" else None

    async def _delete(*_args, **_kwargs):
        raise RuntimeError("injected attachment delete staging failure")

    monkeypatch.setattr(services_main, "get_storage_provider", lambda: storage)
    monkeypatch.setattr(services_main, "_application_get", _get)
    monkeypatch.setattr(services_main, "_application_delete", _delete)

    with pytest.raises(
        RuntimeError,
        match="injected attachment delete staging failure",
    ):
        await services_main.AttachmentService(object()).delete_attachment(ATTACHMENT_ID)

    assert storage.objects[storage.saved_path] == b"evidence"
    assert storage.deleted == [storage.saved_path]
    assert storage.restored == [storage.saved_path]


def _uow(*, commit_error: BaseException | None = None):
    card = SimpleNamespace(id=CARD_ID, board_id=BOARD_ID)
    board = SimpleNamespace(id=BOARD_ID, owner_id=ACTOR.actor_id, realm_id=None)
    attachment = SimpleNamespace(
        id=ATTACHMENT_ID,
        card_id=CARD_ID,
        path=f"/opaque/{BOARD_ID}/stored.bin",
        original_filename="evidence.txt",
        mime_type="text/plain",
        size=8,
    )
    receipt = SimpleNamespace(
        attachment_id=ATTACHMENT_ID,
        path=attachment.path,
        content=b"evidence",
    )
    attachments = SimpleNamespace(
        upload_attachment=AsyncMock(return_value=attachment),
        discard_uploaded_attachment=AsyncMock(),
        get_attachment=AsyncMock(return_value=attachment),
        delete_attachment=AsyncMock(return_value=receipt),
        restore_deleted_attachment=AsyncMock(),
    )
    services = SimpleNamespace(
        cards=SimpleNamespace(get_card=AsyncMock(return_value=card)),
        attachments=attachments,
        boards=SimpleNamespace(_log_activity=AsyncMock()),
        log_card_collaboration_activity=AsyncMock(),
    )
    uow = SimpleNamespace(
        boards=SimpleNamespace(get=AsyncMock(return_value=board)),
        services=services,
        commit=AsyncMock(side_effect=commit_error),
        reload=AsyncMock(),
    )
    return uow, attachment, receipt


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["activity", "commit"])
async def test_rest_upload_failure_discards_saved_object(failure_stage: str) -> None:
    uow, attachment, _receipt = _uow(
        commit_error=(
            RuntimeError("injected upload commit failure")
            if failure_stage == "commit"
            else None
        )
    )
    if failure_stage == "activity":
        uow.services.log_card_collaboration_activity.side_effect = RuntimeError(
            "injected upload activity failure"
        )

    with pytest.raises(RuntimeError, match=f"injected upload {failure_stage} failure"):
        await UploadCardAttachmentUseCase().execute(
            UploadCardAttachmentCommand(
                BOARD_ID,
                CARD_ID,
                "evidence.txt",
                b"evidence",
                "text/plain",
            ),
            actor=ACTOR,
            uow=uow,
        )

    uow.services.attachments.discard_uploaded_attachment.assert_awaited_once_with(
        attachment
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["activity", "commit"])
async def test_rest_delete_failure_restores_removed_object(failure_stage: str) -> None:
    uow, _attachment, receipt = _uow(
        commit_error=(
            RuntimeError("injected delete commit failure")
            if failure_stage == "commit"
            else None
        )
    )
    if failure_stage == "activity":
        uow.services.log_card_collaboration_activity.side_effect = RuntimeError(
            "injected delete activity failure"
        )

    with pytest.raises(RuntimeError, match=f"injected delete {failure_stage} failure"):
        await DeleteCardAttachmentUseCase().execute(
            DeleteCardAttachmentCommand(BOARD_ID, CARD_ID, ATTACHMENT_ID),
            actor=ACTOR,
            uow=uow,
        )

    uow.services.attachments.restore_deleted_attachment.assert_awaited_once_with(
        receipt
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["activity", "commit"])
async def test_mcp_upload_failure_discards_saved_object(failure_stage: str) -> None:
    uow, attachment, _receipt = _uow(
        commit_error=(
            RuntimeError("injected MCP upload commit failure")
            if failure_stage == "commit"
            else None
        )
    )
    if failure_stage == "activity":
        uow.services.boards._log_activity.side_effect = RuntimeError(
            "injected MCP upload activity failure"
        )

    with pytest.raises(
        RuntimeError,
        match=f"injected MCP upload {failure_stage} failure",
    ):
        await McpUploadAttachmentUseCase().execute(
            McpUploadAttachmentCommand(
                BOARD_ID,
                CARD_ID,
                "evidence.txt",
                b"evidence",
                "text/plain",
            ),
            actor=ACTOR,
            uow=uow,
        )

    uow.services.attachments.discard_uploaded_attachment.assert_awaited_once_with(
        attachment
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["activity", "commit"])
async def test_mcp_delete_failure_restores_removed_object(failure_stage: str) -> None:
    uow, _attachment, receipt = _uow(
        commit_error=(
            RuntimeError("injected MCP delete commit failure")
            if failure_stage == "commit"
            else None
        )
    )
    if failure_stage == "activity":
        uow.services.boards._log_activity.side_effect = RuntimeError(
            "injected MCP delete activity failure"
        )

    with pytest.raises(
        RuntimeError,
        match=f"injected MCP delete {failure_stage} failure",
    ):
        await McpDeleteAttachmentUseCase().execute(
            McpDeleteAttachmentCommand(BOARD_ID, ATTACHMENT_ID),
            actor=ACTOR,
            uow=uow,
        )

    uow.services.attachments.restore_deleted_attachment.assert_awaited_once_with(
        receipt
    )
