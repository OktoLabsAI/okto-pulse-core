"""Pure cursor contract shared by KG services and inbound adapters."""

from __future__ import annotations

import base64


class InvalidCursorError(ValueError):
    """Raised when an opaque KG pagination cursor cannot be decoded."""


def encode_cursor(created_at_iso: str, node_id: str) -> str:
    """Encode a ``(created_at, node_id)`` pair as the legacy opaque token."""
    if not created_at_iso or not node_id:
        raise ValueError("cursor components must be non-empty")
    payload = f"{created_at_iso};{node_id}"
    return base64.b64encode(payload.encode()).decode()


def decode_cursor(cursor: str) -> tuple[str, str]:
    """Decode a legacy cursor without depending on any transport adapter."""
    try:
        payload = base64.b64decode(cursor.encode()).decode()
    except Exception as exc:
        raise InvalidCursorError(f"cursor is not valid base64: {exc}") from exc
    if ";" not in payload:
        raise InvalidCursorError("cursor missing separator")
    created_at, _, node_id = payload.partition(";")
    if not created_at or not node_id:
        raise InvalidCursorError("cursor has empty components")
    return created_at, node_id
