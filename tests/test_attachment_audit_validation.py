"""Attachment audit parity and fail-fast filename validation regressions."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.card_collaboration import (
    AttachmentStorageError,
    DeleteCardAttachmentCommand,
    DeleteCardAttachmentUseCase,
    InvalidAttachmentFilenameError,
    MAX_ATTACHMENT_FILENAME_BYTES,
    UploadCardAttachmentCommand,
    UploadCardAttachmentUseCase,
    validate_attachment_filename,
)
from okto_pulse.core.application.use_cases.mcp_collaboration import (
    McpDeleteAttachmentCommand,
    McpDeleteAttachmentUseCase,
    McpUploadAttachmentCommand,
    McpUploadAttachmentUseCase,
)
from okto_pulse.core.domain.realm import LOCAL_REALM_ID


BOARD_ID = "attachment-audit-board"
CARD_ID = "attachment-audit-card"
ATTACHMENT_ID = "attachment-audit-id"
REST_ACTOR = ActorContext(
    "attachment-user",
    "rest",
    board_id=BOARD_ID,
    realm_id=LOCAL_REALM_ID,
    permissions=("attachments:upload", "attachments:delete"),
)
MCP_ACTOR = ActorContext("attachment-agent", "mcp", board_id=BOARD_ID)


def _uow(
    *,
    upload_error: BaseException | None = None,
    delete_error: BaseException | None = None,
):
    card = SimpleNamespace(id=CARD_ID, board_id=BOARD_ID)
    board = SimpleNamespace(
        id=BOARD_ID,
        owner_id=REST_ACTOR.actor_id,
        realm_id=LOCAL_REALM_ID,
    )
    attachment = SimpleNamespace(
        id=ATTACHMENT_ID,
        card_id=CARD_ID,
        path="/opaque/attachment.bin",
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
        upload_attachment=AsyncMock(
            return_value=attachment,
            side_effect=upload_error,
        ),
        discard_uploaded_attachment=AsyncMock(),
        get_attachment=AsyncMock(return_value=attachment),
        delete_attachment=AsyncMock(
            return_value=receipt,
            side_effect=delete_error,
        ),
        restore_deleted_attachment=AsyncMock(),
    )
    services = SimpleNamespace(
        cards=SimpleNamespace(get_card=AsyncMock(return_value=card)),
        shares=SimpleNamespace(get_user_permission=AsyncMock(return_value=None)),
        attachments=attachments,
        boards=SimpleNamespace(_log_activity=AsyncMock()),
        log_card_collaboration_activity=AsyncMock(),
    )
    return SimpleNamespace(
        boards=SimpleNamespace(get=AsyncMock(return_value=board)),
        services=services,
        commit=AsyncMock(),
        reload=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_rest_upload_and_delete_emit_each_activity_exactly_once() -> None:
    uow = _uow()

    await UploadCardAttachmentUseCase().execute(
        UploadCardAttachmentCommand(
            BOARD_ID,
            CARD_ID,
            "evidence.txt",
            b"evidence",
            "text/plain",
        ),
        actor=REST_ACTOR,
        uow=uow,
    )
    await DeleteCardAttachmentUseCase().execute(
        DeleteCardAttachmentCommand(BOARD_ID, CARD_ID, ATTACHMENT_ID),
        actor=REST_ACTOR,
        uow=uow,
    )

    actions = [
        call.kwargs["action"]
        for call in uow.services.log_card_collaboration_activity.await_args_list
    ]
    assert actions == ["attachment_uploaded", "attachment_deleted"]
    assert actions.count("attachment_uploaded") == 1
    assert actions.count("attachment_deleted") == 1
    uow.services.boards._log_activity.assert_not_awaited()


@pytest.mark.asyncio
async def test_mcp_upload_and_delete_emit_each_activity_exactly_once() -> None:
    uow = _uow()

    upload = await McpUploadAttachmentUseCase().execute(
        McpUploadAttachmentCommand(
            BOARD_ID,
            CARD_ID,
            "evidence.txt",
            b"evidence",
            "text/plain",
        ),
        actor=MCP_ACTOR,
        uow=uow,
    )
    delete = await McpDeleteAttachmentUseCase().execute(
        McpDeleteAttachmentCommand(BOARD_ID, ATTACHMENT_ID),
        actor=MCP_ACTOR,
        uow=uow,
    )

    assert upload.payload["success"] is True
    assert delete.payload == {"success": True}
    actions = [
        call.kwargs["action"]
        for call in uow.services.boards._log_activity.await_args_list
    ]
    assert actions == ["attachment_uploaded", "attachment_deleted"]
    assert actions.count("attachment_uploaded") == 1
    assert actions.count("attachment_deleted") == 1
    uow.services.log_card_collaboration_activity.assert_not_awaited()


@pytest.mark.parametrize(
    "filename",
    [
        "",
        " evidence.txt",
        "evidence.txt ",
        ".",
        "..",
        "../evidence.txt",
        r"..\evidence.txt",
        "bad:name.txt",
        "bad\x00name.txt",
        "bad\nname.txt",
        "CON.txt",
        "evidence.",
        "x" * (MAX_ATTACHMENT_FILENAME_BYTES + 1),
        "é" * ((MAX_ATTACHMENT_FILENAME_BYTES // 2) + 1),
    ],
    ids=[
        "empty",
        "leading-space",
        "trailing-space",
        "dot",
        "dot-dot",
        "forward-separator",
        "back-separator",
        "reserved-character",
        "nul",
        "control-character",
        "reserved-device",
        "trailing-dot",
        "ascii-too-long",
        "utf8-too-long",
    ],
)
def test_invalid_attachment_filenames_have_typed_path_free_error(
    filename: str,
) -> None:
    with pytest.raises(InvalidAttachmentFilenameError) as captured:
        validate_attachment_filename(filename)

    assert captured.value.code == "invalid_attachment_filename"
    if filename:
        assert filename not in str(captured.value)
    assert "OSError" not in str(captured.value)


def test_attachment_filename_byte_limit_accepts_exact_boundary() -> None:
    filename = "x" * MAX_ATTACHMENT_FILENAME_BYTES
    assert validate_attachment_filename(filename) == filename


@pytest.mark.asyncio
async def test_long_filename_stops_both_surfaces_before_storage_writer() -> None:
    filename = "x" * (MAX_ATTACHMENT_FILENAME_BYTES + 1)

    rest_uow = _uow()
    with pytest.raises(InvalidAttachmentFilenameError):
        await UploadCardAttachmentUseCase().execute(
            UploadCardAttachmentCommand(
                BOARD_ID,
                CARD_ID,
                filename,
                b"evidence",
                "text/plain",
            ),
            actor=REST_ACTOR,
            uow=rest_uow,
        )
    rest_uow.services.attachments.upload_attachment.assert_not_awaited()
    rest_uow.services.log_card_collaboration_activity.assert_not_awaited()
    rest_uow.commit.assert_not_awaited()

    mcp_uow = _uow()
    result = await McpUploadAttachmentUseCase().execute(
        McpUploadAttachmentCommand(
            BOARD_ID,
            CARD_ID,
            filename,
            b"evidence",
            "text/plain",
        ),
        actor=MCP_ACTOR,
        uow=mcp_uow,
    )
    assert result.payload["code"] == "invalid_attachment_filename"
    assert filename not in str(result.payload)
    assert "OSError" not in str(result.payload)
    mcp_uow.services.attachments.upload_attachment.assert_not_awaited()
    mcp_uow.services.boards._log_activity.assert_not_awaited()
    mcp_uow.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_storage_oserror_is_translated_without_local_path() -> None:
    local_path = r"C:\private\pulse\uploads\evidence.txt"

    rest_uow = _uow(upload_error=OSError(f"name too long: {local_path}"))
    with pytest.raises(AttachmentStorageError) as captured:
        await UploadCardAttachmentUseCase().execute(
            UploadCardAttachmentCommand(
                BOARD_ID,
                CARD_ID,
                "evidence.txt",
                b"evidence",
                "text/plain",
            ),
            actor=REST_ACTOR,
            uow=rest_uow,
        )
    assert captured.value.code == "attachment_storage_error"
    assert local_path not in str(captured.value)
    assert "OSError" not in str(captured.value)

    mcp_uow = _uow(delete_error=OSError(f"missing object: {local_path}"))
    result = await McpDeleteAttachmentUseCase().execute(
        McpDeleteAttachmentCommand(BOARD_ID, ATTACHMENT_ID),
        actor=MCP_ACTOR,
        uow=mcp_uow,
    )
    assert result.payload == {
        "error": "Attachment storage operation failed",
        "code": "attachment_storage_error",
    }
    assert local_path not in str(result.payload)
    assert "OSError" not in str(result.payload)
