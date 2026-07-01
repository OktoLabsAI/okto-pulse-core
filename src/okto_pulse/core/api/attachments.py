"""Attachment API endpoints."""

import hashlib
import secrets
from email.utils import formatdate
from urllib.parse import quote

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import StreamingResponse

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.infra.storage import StorageObjectStat, get_storage_provider
from okto_pulse.core.models import AttachmentResponse
from okto_pulse.core.services import AttachmentService, BoardService

router = APIRouter()

#: Streamed-download chunk size — mirrors Starlette's file-download chunk size so
#: the provider-backed response chunks identically to the prior filesystem response.
_DOWNLOAD_CHUNK_SIZE = 64 * 1024


def _content_disposition(filename: str) -> str:
    """Replicate Starlette's file-download Content-Disposition encoding so the
    StorageProvider-backed download preserves the EXACT header the filesystem
    bypass produced (ASCII -> quoted filename; non-ASCII -> RFC 5987 filename*)."""
    quoted = quote(filename)
    if quoted != filename:
        return f"attachment; filename*=utf-8''{quoted}"
    return f'attachment; filename="{filename}"'


def _content_type(media_type: str) -> str:
    """Replicate Starlette's media-type handling: a ``text/*`` type without an
    explicit charset gets ``; charset=utf-8`` appended — matching the header the
    prior filesystem response (which passed ``media_type``) produced."""
    if media_type.startswith("text/") and "charset=" not in media_type.lower():
        return f"{media_type}; charset=utf-8"
    return media_type


def _stat_headers(meta: StorageObjectStat) -> dict[str, str]:
    """Reproduce the prior filesystem response's stat headers EXACTLY: always
    ``Content-Length``; ``Last-Modified`` + ``ETag`` derived from (mtime, size)
    with the same formula when the provider exposes a modification clock. When the
    provider has no mtime the baseline had no such headers either, so they are
    omitted (documented parity, not degradation — TR5)."""
    headers = {"content-length": str(meta.size)}
    if meta.modified_time is not None:
        headers["last-modified"] = formatdate(meta.modified_time, usegmt=True)
        etag_base = f"{meta.modified_time}-{meta.size}"
        headers["etag"] = f'"{hashlib.md5(etag_base.encode(), usedforsecurity=False).hexdigest()}"'
    return headers


def _parse_byte_ranges(range_header: str | None, size: int):
    """Parse a ``bytes=`` Range header with the SAME semantics as the prior
    filesystem file-download response, so single / multi / suffix / unsatisfiable /
    malformed all resolve identically to the baseline.

    Returns one of:
      ``("single", start, end_exclusive)`` — one satisfiable range -> 206;
      ``("multiple", [(start, end_exclusive), ...])`` — merged+sorted -> 206
        multipart/byteranges (overlaps that merge down to one collapse to single);
      ``("unsatisfiable", size)`` — every range out of bounds -> 416;
      ``("malformed",)`` — bad unit / no numeric range -> 400 (baseline parity);
      ``None`` — no Range header.
    """
    if not range_header:
        return None
    try:
        units, spec = range_header.split("=", 1)
    except ValueError:
        return ("malformed",)
    if units.strip().lower() != "bytes":
        return ("malformed",)

    ranges: list[tuple[int, int]] = []
    for part in spec.split(","):
        part = part.strip()
        if not part or part == "-" or "-" not in part:
            continue
        start_s, end_s = part.split("-", 1)
        start_s, end_s = start_s.strip(), end_s.strip()
        try:
            start = int(start_s) if start_s else size - int(end_s)
            end = int(end_s) + 1 if start_s and end_s and int(end_s) < size else size
        except ValueError:
            continue
        ranges.append((start, end))

    if not ranges:
        return ("malformed",)
    if any(not (0 <= start < size) for start, _ in ranges):
        return ("unsatisfiable", size)
    if any(start > end for start, end in ranges):
        return ("malformed",)
    if len(ranges) == 1:
        return ("single", ranges[0][0], ranges[0][1])

    ranges.sort()
    merged: list[tuple[int, int]] = [ranges[0]]
    for start, end in ranges[1:]:
        last_start, last_end = merged[-1]
        if start <= last_end:
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))
    if len(merged) == 1:
        return ("single", merged[0][0], merged[0][1])
    return ("multiple", merged)


def _multipart_content_length(
    ranges: list[tuple[int, int]], boundary: str, size: int, content_type: str
) -> int:
    """Byte length of the ``multipart/byteranges`` body — IDENTICAL formula to the
    baseline's multipart generator (the constant 49 folds each part's fixed header
    chars plus the trailing CRLF after its data)."""
    static = 49 + len(boundary) + len(content_type) + len(str(size))
    return sum(
        (len(str(start)) + len(str(end - 1)) + static) + (end - start)
        for start, end in ranges
    ) + (4 + len(boundary))


def _multipart_part_header(boundary: str, start: int, end: int, size: int, content_type: str) -> bytes:
    return (
        f"--{boundary}\r\n"
        f"Content-Type: {content_type}\r\n"
        f"Content-Range: bytes {start}-{end - 1}/{size}\r\n"
        "\r\n"
    ).encode("latin-1")


async def _multipart_stream(storage, path, ranges, boundary, size, content_type):
    """Stream a ``multipart/byteranges`` body byte-for-byte like the baseline:
    per range a part header + the range's bytes (THROUGH the provider) + CRLF, then
    the closing ``--boundary--``. Each range is read via the provider's offloaded
    chunked stream — the whole file is never materialised (AC6)."""
    for start, end in ranges:
        yield _multipart_part_header(boundary, start, end, size, content_type)
        async for chunk in storage.open_stream(path, start=start, end=end, chunk_size=_DOWNLOAD_CHUNK_SIZE):
            yield chunk
        yield b"\r\n"
    yield f"--{boundary}--".encode("latin-1")


async def _log(db: AsyncSession, card_id: str, action: str, user_id: str, details: dict | None = None):
    from okto_pulse.core.models.db import Card
    from okto_pulse.core.services.main import resolve_actor_name
    card = await db.get(Card, card_id)
    if card:
        actor_name = await resolve_actor_name(db, user_id, card.board_id)
        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=card.board_id, card_id=card_id,
            action=action, actor_type="user", actor_id=user_id, actor_name=actor_name,
            details=details,
        )


async def _validate_card_belongs_to_board(db: AsyncSession, board_id: str, card_id: str):
    """Validate that the card belongs to the specified board."""
    from okto_pulse.core.models.db import Card
    card = await db.get(Card, card_id)
    if not card or card.board_id != board_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Card not found in this board")
    return card


@router.post("/{board_id}/{card_id}", response_model=AttachmentResponse, status_code=status.HTTP_201_CREATED)
async def upload_attachment(
    board_id: str,
    card_id: str,
    file: UploadFile = File(...),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Upload a file attachment to a card."""
    await _validate_card_belongs_to_board(db, board_id, card_id)

    service = AttachmentService(db)
    content = await file.read()

    attachment = await service.upload_attachment(
        card_id=card_id,
        user_id=user_id,
        filename=file.filename or "unnamed",
        content=content,
        mime_type=file.content_type or "application/octet-stream",
    )
    if not attachment:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Card not found")
    await _log(db, card_id, "attachment_uploaded", user_id, {"filename": file.filename, "size": len(content)})
    await db.commit()
    await db.refresh(attachment, attribute_names=["id", "card_id", "filename", "original_filename", "mime_type", "size", "uploaded_by", "created_at"])
    return attachment


@router.get("/{board_id}/{card_id}/{attachment_id}")
async def download_attachment(
    board_id: str,
    card_id: str,
    attachment_id: str,
    request: Request,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Download an attachment through the registered StorageProvider.

    R02 (FR1/FR2/AC1-AC6): the core NEVER touches a concrete filesystem path. It
    reads metadata + bytes through the provider and reproduces the prior
    filesystem response's observable HTTP contract — Content-Type (incl. text/*
    charset), Content-Disposition, Content-Length, Last-Modified, ETag,
    Accept-Ranges, single-range 206, multi-range 206 multipart/byteranges, 416 and
    malformed 400 — while STREAMING the body in chunks (never materialising the
    whole file by default, AC6). Fails closed (503) when no provider is registered;
    never falls back to a local path (AC3/TR1).
    """
    await _validate_card_belongs_to_board(db, board_id, card_id)

    service = AttachmentService(db)
    attachment = await service.get_attachment(attachment_id)
    if not attachment or attachment.card_id != card_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Attachment not found")

    # FR1/AC3/TR1: serve through the registered provider — fail closed if absent.
    try:
        storage = get_storage_provider()
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage provider not configured",
        ) from exc

    # Read-side metadata (size + optional mtime) THROUGH the provider — no path.
    try:
        meta = await storage.stat(attachment.path)
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Attachment file not found",
        ) from exc

    base_headers = {
        "content-type": _content_type(attachment.mime_type),
        "content-disposition": _content_disposition(attachment.original_filename),
        "accept-ranges": "bytes",
    }
    stat_headers = _stat_headers(meta)
    etag = stat_headers.get("etag")
    last_modified = stat_headers.get("last-modified")

    # Honour Range only when there is no If-Range or its validator still matches
    # (mirrors the prior filesystem response's If-Range semantics).
    range_header = request.headers.get("range")
    if_range = request.headers.get("if-range")
    use_range = range_header is not None and (
        if_range is None or if_range == etag or if_range == last_modified
    )
    parsed = _parse_byte_ranges(range_header, meta.size) if use_range else None

    if parsed is not None and parsed[0] == "unsatisfiable":
        raise HTTPException(
            status_code=status.HTTP_416_RANGE_NOT_SATISFIABLE,
            detail="Requested Range Not Satisfiable",
            headers={"content-range": f"bytes */{meta.size}"},
        )

    if parsed is not None and parsed[0] == "malformed":
        # The baseline filesystem response answers a malformed Range with 400.
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Malformed range header.",
        )

    if parsed is not None and parsed[0] == "single":
        _, start, end = parsed
        headers = {**base_headers, **stat_headers}
        headers["content-length"] = str(end - start)
        headers["content-range"] = f"bytes {start}-{end - 1}/{meta.size}"
        return StreamingResponse(
            storage.open_stream(attachment.path, start=start, end=end, chunk_size=_DOWNLOAD_CHUNK_SIZE),
            status_code=status.HTTP_206_PARTIAL_CONTENT,
            headers=headers,
        )

    if parsed is not None and parsed[0] == "multiple":
        _, ranges = parsed
        boundary = secrets.token_hex(13)
        content_type = base_headers["content-type"]
        headers = {**base_headers, **stat_headers}
        headers["content-type"] = f"multipart/byteranges; boundary={boundary}"
        headers["content-length"] = str(
            _multipart_content_length(ranges, boundary, meta.size, content_type)
        )
        return StreamingResponse(
            _multipart_stream(storage, attachment.path, ranges, boundary, meta.size, content_type),
            status_code=status.HTTP_206_PARTIAL_CONTENT,
            headers=headers,
        )

    # Full 200 — no Range header, or an If-Range whose validator no longer matches.
    return StreamingResponse(
        storage.open_stream(attachment.path, chunk_size=_DOWNLOAD_CHUNK_SIZE),
        status_code=status.HTTP_200_OK,
        headers={**base_headers, **stat_headers},
    )


@router.delete("/{board_id}/{card_id}/{attachment_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_attachment(
    board_id: str,
    card_id: str,
    attachment_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete an attachment."""
    await _validate_card_belongs_to_board(db, board_id, card_id)

    service = AttachmentService(db)
    attachment = await service.get_attachment(attachment_id)
    if not attachment or attachment.card_id != card_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Attachment not found")

    deleted = await service.delete_attachment(attachment_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Attachment not found")
    await _log(db, card_id, "attachment_deleted", user_id, {"attachment_id": attachment_id})
    await db.commit()
