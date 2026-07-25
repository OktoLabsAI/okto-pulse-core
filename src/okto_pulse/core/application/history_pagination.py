"""Transport-neutral validation and metadata for history/snapshot reads.

The MCP surface historically accepted decimal strings while REST supplied
integers.  Keeping the coercion here gives both adapters one bounded contract
and prevents raw ``ValueError``/database integer-overflow failures from leaking
through either transport.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from okto_pulse.core.ports.application_persistence import (
    PAGE_LIMIT_MAX,
    PAGE_LIMIT_MIN,
    PAGE_OFFSET_MAX,
)

_DECIMAL_INTEGER = re.compile(r"-?[0-9]+\Z")
SNAPSHOT_VERSION_MAX = PAGE_OFFSET_MAX


class HistoryReadValidationError(ValueError):
    """Stable, adapter-mappable failure for a bounded history/snapshot input."""

    def __init__(
        self,
        code: str,
        *,
        field: str,
        minimum: int,
        maximum: int,
    ) -> None:
        self.code = code
        self.field = field
        self.minimum = minimum
        self.maximum = maximum
        super().__init__(
            f"{code}: {field} must be an integer in {minimum}..{maximum}"
        )

    def to_error_dict(self) -> dict[str, Any]:
        return {
            "error": self.code,
            "code": self.code,
            "detail": str(self),
            "field": self.field,
            "minimum": self.minimum,
            "maximum": self.maximum,
            "retryable": False,
        }


@dataclass(frozen=True, slots=True)
class HistoryWindow:
    limit: int
    offset: int


@dataclass(frozen=True, slots=True)
class HistoryPageMetadata:
    total: int
    has_more: bool
    next_offset: int | None
    truncated: bool


def _bounded_integer(
    value: object,
    *,
    code: str,
    field: str,
    minimum: int,
    maximum: int,
) -> int:
    parsed: int
    if isinstance(value, bool):
        raise HistoryReadValidationError(
            code,
            field=field,
            minimum=minimum,
            maximum=maximum,
        )
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and _DECIMAL_INTEGER.fullmatch(value):
        try:
            parsed = int(value)
        except ValueError as exc:
            # Python bounds decimal conversion length.  Normalize that runtime
            # implementation detail to the same public validation contract.
            raise HistoryReadValidationError(
                code,
                field=field,
                minimum=minimum,
                maximum=maximum,
            ) from exc
    else:
        raise HistoryReadValidationError(
            code,
            field=field,
            minimum=minimum,
            maximum=maximum,
        )
    if not minimum <= parsed <= maximum:
        raise HistoryReadValidationError(
            code,
            field=field,
            minimum=minimum,
            maximum=maximum,
        )
    return parsed


def validate_snapshot_version(value: object) -> int:
    """Return a signed-64-bit-safe, positive snapshot version."""

    return _bounded_integer(
        value,
        code="snapshot_version_invalid",
        field="version",
        minimum=1,
        maximum=SNAPSHOT_VERSION_MAX,
    )


def validate_history_window(
    limit: object = 50,
    offset: object = 0,
) -> HistoryWindow:
    """Normalize one exact bounded history page window."""

    return HistoryWindow(
        limit=_bounded_integer(
            limit,
            code="history_limit_invalid",
            field="limit",
            minimum=PAGE_LIMIT_MIN,
            maximum=PAGE_LIMIT_MAX,
        ),
        offset=_bounded_integer(
            offset,
            code="history_offset_invalid",
            field="offset",
            minimum=0,
            maximum=PAGE_OFFSET_MAX,
        ),
    )


def history_page_metadata(
    *,
    total: int,
    returned: int,
    window: HistoryWindow,
) -> HistoryPageMetadata:
    """Describe an exact page without inferring the total from page length."""

    if total < 0 or returned < 0:
        raise ValueError("history_page_metadata counts must be non-negative")
    has_more = window.offset + returned < total
    return HistoryPageMetadata(
        total=total,
        has_more=has_more,
        next_offset=window.offset + returned if has_more else None,
        # Match the canonical list-envelope meaning: this page was cut before
        # the end of the matching result set.
        truncated=has_more,
    )


__all__ = [
    "HistoryPageMetadata",
    "HistoryReadValidationError",
    "HistoryWindow",
    "SNAPSHOT_VERSION_MAX",
    "history_page_metadata",
    "validate_history_window",
    "validate_snapshot_version",
]
