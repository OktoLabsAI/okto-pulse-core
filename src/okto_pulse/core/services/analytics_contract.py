"""Canonical contracts shared by analytics, REST and MCP surfaces.

Core owns temporal semantics, disjoint card classification and the opaque
activity-cursor protocol.  Concrete persistence normalization remains an
edition adapter concern (Community uses SQLite-specific ``julianday`` binds).
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import re
import secrets
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from typing import Any, Literal, TypeAlias


ANALYTICS_CONTRACT_VERSION = "2"
ANALYTICS_TIMEZONE = "UTC"
ACTIVITY_CURSOR_VERSION = 2
ACTIVITY_CURSOR_DIRECTION = "desc"

CardAnalyticsCategory: TypeAlias = Literal["implementation", "test", "bug"]

_DATE_ONLY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CURSOR_INTEGRITY_CONTEXT = b"okto-pulse.activity-cursor.v2"


@lru_cache(maxsize=1)
def _process_cursor_signing_key() -> bytes:
    """Return an unguessable per-process fallback key.

    Distributed editions should pass a shared ``signing_key`` explicitly.
    Community is single-process, so this secure fallback keeps cursors opaque;
    a restart deliberately invalidates outstanding cursors instead of relying
    on a public source-code constant that clients could use to forge them.
    """

    return secrets.token_bytes(32)


def _normalized_signing_key(signing_key: bytes | str | None) -> bytes:
    if signing_key is None:
        return _process_cursor_signing_key()
    if isinstance(signing_key, str):
        signing_key = signing_key.encode("utf-8")
    if not isinstance(signing_key, bytes) or len(signing_key) < 32:
        raise ValueError("activity_cursor_signing_key_must_be_at_least_32_bytes")
    return signing_key


def parse_analytics_datetime(
    value: str | None,
    *,
    end_exclusive: bool = False,
) -> datetime | None:
    """Parse one public analytics bound under the canonical UTC contract.

    A date-only upper bound is converted to the start of the following day and
    must be applied with ``created_at < bound``.  Full timestamps preserve their
    exact instant (normalised to UTC) and use the same half-open comparison.
    Invalid/empty input remains ``None`` for legacy optional-filter behaviour.
    """

    if not value or not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        if _DATE_ONLY.fullmatch(raw):
            parsed_date = date.fromisoformat(raw)
            instant = datetime.combine(parsed_date, time.min, tzinfo=timezone.utc)
            return instant + timedelta(days=1) if end_exclusive else instant

        instant = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if instant.tzinfo is None:
            instant = instant.replace(tzinfo=timezone.utc)
        return instant.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def _card_type_value(card: Any) -> str:
    raw = getattr(card, "card_type", None)
    raw = getattr(raw, "value", raw)
    value = str(raw or "").strip().lower()
    return value.rsplit(".", 1)[-1]


def classify_analytics_card(card: Any) -> CardAnalyticsCategory:
    """Return exactly one category using bug → test → implementation.

    ``test_scenario_ids`` is retained as a legacy test signal, but it can never
    make a bug count twice. Unknown legacy rows fall into implementation so the
    category sum always reconciles with the selected card total.
    """

    card_type = _card_type_value(card)
    if card_type == "bug":
        return "bug"
    test_scenario_ids = getattr(card, "test_scenario_ids", None)
    if card_type == "test" or (
        isinstance(test_scenario_ids, list) and bool(test_scenario_ids)
    ):
        return "test"
    return "implementation"


def partition_analytics_cards(cards: list[Any]) -> dict[CardAnalyticsCategory, list[Any]]:
    partitions: dict[CardAnalyticsCategory, list[Any]] = {
        "implementation": [],
        "test": [],
        "bug": [],
    }
    for card in cards:
        partitions[classify_analytics_card(card)].append(card)
    return partitions


def normalize_activity_timestamp(value: datetime | str) -> datetime:
    """Normalize a cursor/storage timestamp to an aware UTC instant."""

    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise TypeError("activity timestamp must be a datetime or ISO string")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _cursor_scope_hash(*, action: str, card_id: str, direction: str) -> str:
    scope = json.dumps(
        {
            "action": action or "",
            "card_id": card_id or "",
            "direction": direction,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(scope).hexdigest()


def _cursor_payload_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _cursor_signature(
    payload: dict[str, Any], *, signing_key: bytes | str | None = None
) -> str:
    # Integrity binding is independent from board authorization, which still
    # runs on every request. Unlike the legacy contextual constant, the HMAC key
    # is not derivable by a remote client.
    return hmac.new(
        _normalized_signing_key(signing_key),
        _CURSOR_INTEGRITY_CONTEXT + b"\x00" + _cursor_payload_bytes(payload),
        hashlib.sha256,
    ).hexdigest()


def encode_activity_cursor(
    created_at: datetime | str,
    row_id: str,
    *,
    action: str = "",
    card_id: str = "",
    direction: str = ACTIVITY_CURSOR_DIRECTION,
    signing_key: bytes | str | None = None,
) -> str:
    if direction != ACTIVITY_CURSOR_DIRECTION:
        raise ValueError("unsupported_activity_cursor_direction")
    if not isinstance(row_id, str) or not row_id:
        raise ValueError("activity cursor row_id is required")
    instant = normalize_activity_timestamp(created_at)
    payload = {
        "v": ACTIVITY_CURSOR_VERSION,
        "ts": instant.isoformat(timespec="microseconds").replace("+00:00", "Z"),
        "id": row_id,
        "direction": direction,
        "scope_hash": _cursor_scope_hash(
            action=action,
            card_id=card_id,
            direction=direction,
        ),
    }
    envelope = {
        "payload": payload,
        "integrity": _cursor_signature(payload, signing_key=signing_key),
    }
    return base64.urlsafe_b64encode(_cursor_payload_bytes(envelope)).decode("ascii")


@dataclass(frozen=True, slots=True)
class ActivityCursorDecodeResult:
    position: tuple[datetime, str] | None
    error_code: str | None = None
    version: int | None = None

    @property
    def valid(self) -> bool:
        return self.position is not None and self.error_code is None


def decode_activity_cursor(
    cursor: str,
    *,
    action: str = "",
    card_id: str = "",
    direction: str = ACTIVITY_CURSOR_DIRECTION,
    signing_key: bytes | str | None = None,
) -> ActivityCursorDecodeResult:
    """Decode and bind a cursor to its version, direction and filters."""

    if not cursor or not isinstance(cursor, str):
        return ActivityCursorDecodeResult(None, "invalid_cursor")
    try:
        raw = base64.b64decode(
            cursor.encode("ascii"),
            altchars=b"-_",
            validate=True,
        ).decode("utf-8")
        envelope = json.loads(raw)
    except (
        UnicodeEncodeError,
        UnicodeDecodeError,
        binascii.Error,
        json.JSONDecodeError,
        ValueError,
    ):
        return ActivityCursorDecodeResult(None, "invalid_cursor")

    if not isinstance(envelope, dict):
        return ActivityCursorDecodeResult(None, "invalid_cursor")

    # Read-only compatibility for cursors issued before v2. They had no scope,
    # so accepting them with filters would silently change the result set.
    if set(envelope) == {"ts", "id"}:
        if action or card_id or direction != ACTIVITY_CURSOR_DIRECTION:
            return ActivityCursorDecodeResult(None, "cursor_scope_mismatch", version=1)
        try:
            position = (
                normalize_activity_timestamp(envelope["ts"]),
                str(envelope["id"]),
            )
        except (TypeError, ValueError):
            return ActivityCursorDecodeResult(None, "invalid_cursor", version=1)
        if not position[1]:
            return ActivityCursorDecodeResult(None, "invalid_cursor", version=1)
        return ActivityCursorDecodeResult(position, version=1)

    payload = envelope.get("payload")
    integrity = envelope.get("integrity")
    if not isinstance(payload, dict) or not isinstance(integrity, str):
        return ActivityCursorDecodeResult(None, "invalid_cursor")
    version = payload.get("v")
    if version != ACTIVITY_CURSOR_VERSION:
        return ActivityCursorDecodeResult(
            None,
            "unsupported_cursor_version",
            version=version if isinstance(version, int) else None,
        )
    if not hmac.compare_digest(
        integrity, _cursor_signature(payload, signing_key=signing_key)
    ):
        return ActivityCursorDecodeResult(
            None,
            "cursor_integrity_failed",
            version=ACTIVITY_CURSOR_VERSION,
        )
    if payload.get("direction") != direction:
        return ActivityCursorDecodeResult(
            None,
            "cursor_scope_mismatch",
            version=ACTIVITY_CURSOR_VERSION,
        )
    expected_scope = _cursor_scope_hash(
        action=action,
        card_id=card_id,
        direction=direction,
    )
    if payload.get("scope_hash") != expected_scope:
        return ActivityCursorDecodeResult(
            None,
            "cursor_scope_mismatch",
            version=ACTIVITY_CURSOR_VERSION,
        )
    try:
        position = (
            normalize_activity_timestamp(payload["ts"]),
            str(payload["id"]),
        )
    except (KeyError, TypeError, ValueError):
        return ActivityCursorDecodeResult(
            None,
            "invalid_cursor",
            version=ACTIVITY_CURSOR_VERSION,
        )
    if not position[1]:
        return ActivityCursorDecodeResult(
            None,
            "invalid_cursor",
            version=ACTIVITY_CURSOR_VERSION,
        )
    return ActivityCursorDecodeResult(position, version=ACTIVITY_CURSOR_VERSION)


__all__ = [
    "ACTIVITY_CURSOR_DIRECTION",
    "ACTIVITY_CURSOR_VERSION",
    "ANALYTICS_CONTRACT_VERSION",
    "ANALYTICS_TIMEZONE",
    "ActivityCursorDecodeResult",
    "CardAnalyticsCategory",
    "classify_analytics_card",
    "decode_activity_cursor",
    "encode_activity_cursor",
    "normalize_activity_timestamp",
    "parse_analytics_datetime",
    "partition_analytics_cards",
]
