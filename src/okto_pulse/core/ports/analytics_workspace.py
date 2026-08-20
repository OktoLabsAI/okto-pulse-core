"""Transport-neutral contract for the Analytics workspace.

The workspace is a consumer of the canonical Analytics foundation.  These DTOs
bind navigation, panel isolation and request supersession without duplicating
metric formulas in a UI or transport adapter.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Protocol, runtime_checkable

from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsFoundationProjection,
    AnalyticsFoundationQuery,
    AnalyticsUtcWindow,
    require_utc_datetime,
)


ANALYTICS_WORKSPACE_CONTRACT_VERSION = "1"
MAX_ANALYTICS_CURSOR_LENGTH = 2048
MAX_ANALYTICS_ENTITY_ID_LENGTH = 255

_ENTITY_ID = re.compile(r"^[^\x00-\x1f\x7f]{1,255}$")
_PANEL_ID = re.compile(r"^[a-z][a-z0-9_.-]{0,127}$")
_SORT_FIELD = re.compile(r"^[a-z][a-z0-9_.-]{0,127}$")
_ERROR_CODE = re.compile(r"^[a-z][a-z0-9_.-]{0,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _utc_text(value: datetime) -> str:
    return (
        require_utc_datetime(value, field="workspace_timestamp")
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


class AnalyticsWorkspaceLevel(str, Enum):
    BOARD = "board"
    ENTITY = "entity"


class AnalyticsSortDirection(str, Enum):
    ASC = "asc"
    DESC = "desc"


class AnalyticsPanelState(str, Enum):
    LOADING = "loading"
    AVAILABLE = "available"
    EMPTY = "empty"
    RESTRICTED = "restricted"
    STALE = "stale"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class AnalyticsSortClause:
    field: str
    direction: AnalyticsSortDirection

    def __post_init__(self) -> None:
        if not isinstance(self.field, str) or not _SORT_FIELD.fullmatch(self.field):
            raise ValueError("analytics_sort_field_invalid")
        if not isinstance(self.direction, AnalyticsSortDirection):
            raise ValueError("analytics_sort_direction_invalid")

    def canonical_dict(self) -> dict[str, str]:
        return {"field": self.field, "direction": self.direction.value}


@dataclass(frozen=True, slots=True)
class AnalyticsWorkspaceQuery:
    foundation: AnalyticsFoundationQuery
    level: AnalyticsWorkspaceLevel
    entity_id: str | None = None
    sort: AnalyticsSortClause = AnalyticsSortClause(
        "metric_id", AnalyticsSortDirection.ASC
    )
    cursor: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.foundation, AnalyticsFoundationQuery):
            raise ValueError("analytics_foundation_query_required")
        if not isinstance(self.level, AnalyticsWorkspaceLevel):
            raise ValueError("analytics_workspace_level_invalid")
        if not isinstance(self.sort, AnalyticsSortClause):
            raise ValueError("analytics_sort_required")
        if self.level is AnalyticsWorkspaceLevel.BOARD:
            if self.entity_id is not None:
                raise ValueError("analytics_board_level_entity_forbidden")
        else:
            if (
                not isinstance(self.entity_id, str)
                or not _ENTITY_ID.fullmatch(self.entity_id)
                or len(self.entity_id) > MAX_ANALYTICS_ENTITY_ID_LENGTH
            ):
                raise ValueError("analytics_entity_id_invalid")
        if self.cursor is not None:
            if (
                not isinstance(self.cursor, str)
                or not self.cursor
                or len(self.cursor) > MAX_ANALYTICS_CURSOR_LENGTH
                or any(ord(char) < 32 or ord(char) == 127 for char in self.cursor)
            ):
                raise ValueError("analytics_cursor_invalid")

    def _fingerprint_payload(self, *, include_cursor: bool) -> dict[str, object]:
        return {
            "contract_version": ANALYTICS_WORKSPACE_CONTRACT_VERSION,
            "foundation_fingerprint": self.foundation.fingerprint,
            "level": self.level.value,
            "entity_id": self.entity_id,
            "sort": self.sort.canonical_dict(),
            "cursor": self.cursor if include_cursor else None,
        }

    @staticmethod
    def _digest(payload: dict[str, object]) -> str:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
        return hashlib.sha256(encoded).hexdigest()

    @property
    def query_fingerprint(self) -> str:
        """Logical query identity shared by REST, MCP, UI and full CSV export."""

        return self._digest(self._fingerprint_payload(include_cursor=False))

    @property
    def request_fingerprint(self) -> str:
        """Page request identity used to reject superseded or late responses."""

        return self._digest(self._fingerprint_payload(include_cursor=True))

    def without_cursor(self) -> AnalyticsWorkspaceQuery:
        return AnalyticsWorkspaceQuery(
            foundation=self.foundation,
            level=self.level,
            entity_id=self.entity_id,
            sort=self.sort,
            cursor=None,
        )

    def canonical_dict(self) -> dict[str, object]:
        return {
            "contract_version": ANALYTICS_WORKSPACE_CONTRACT_VERSION,
            "board_id": self.foundation.board_id,
            "actor_scope_ref": self.foundation.actor_scope_ref,
            "window": self.foundation.window.canonical_dict(),
            "filters": [
                item.canonical_dict()
                for item in sorted(
                    self.foundation.filters,
                    key=lambda clause: json.dumps(
                        clause.canonical_dict(),
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                )
            ],
            "as_of": (
                _utc_text(self.foundation.as_of)
                if self.foundation.as_of is not None
                else None
            ),
            "level": self.level.value,
            "entity_id": self.entity_id,
            "sort": self.sort.canonical_dict(),
            "cursor": self.cursor,
            "query_fingerprint": self.query_fingerprint,
            "request_fingerprint": self.request_fingerprint,
        }


@dataclass(frozen=True, slots=True)
class AnalyticsPanelEnvelope:
    panel_id: str
    state: AnalyticsPanelState
    query_fingerprint: str
    request_fingerprint: str
    foundation_fingerprint: str
    result: AnalyticsFoundationProjection | None = None
    stale_as_of: datetime | None = None
    retryable: bool = False
    error_code: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.panel_id, str) or not _PANEL_ID.fullmatch(self.panel_id):
            raise ValueError("analytics_panel_id_invalid")
        if not isinstance(self.state, AnalyticsPanelState):
            raise ValueError("analytics_panel_state_invalid")
        if not _SHA256.fullmatch(self.query_fingerprint):
            raise ValueError("analytics_query_fingerprint_invalid")
        if not _SHA256.fullmatch(self.request_fingerprint):
            raise ValueError("analytics_request_fingerprint_invalid")
        if not _SHA256.fullmatch(self.foundation_fingerprint):
            raise ValueError("analytics_foundation_fingerprint_invalid")
        if not isinstance(self.retryable, bool):
            raise ValueError("analytics_panel_retryable_invalid")
        if self.error_code is not None and not _ERROR_CODE.fullmatch(self.error_code):
            raise ValueError("analytics_panel_error_code_invalid")
        if self.result is not None:
            if not isinstance(self.result, AnalyticsFoundationProjection):
                raise ValueError("analytics_panel_result_invalid")
            if self.result.query_fingerprint != self.foundation_fingerprint:
                raise ValueError("analytics_panel_result_query_mismatch")

        if self.state is AnalyticsPanelState.AVAILABLE:
            if (
                self.result is None
                or self.stale_as_of is not None
                or self.error_code is not None
                or self.retryable
            ):
                raise ValueError("analytics_available_panel_shape_invalid")
        elif self.state is AnalyticsPanelState.STALE:
            if self.result is None or not self.retryable or self.error_code is None:
                raise ValueError("analytics_stale_panel_shape_invalid")
            cut = require_utc_datetime(self.stale_as_of, field="panel_stale_as_of")
            if cut != self.result.as_of:
                raise ValueError("analytics_stale_panel_as_of_mismatch")
            object.__setattr__(self, "stale_as_of", cut)
        elif self.state is AnalyticsPanelState.ERROR:
            if (
                self.result is not None
                or self.stale_as_of is not None
                or self.error_code is None
                or not self.retryable
            ):
                raise ValueError("analytics_error_panel_shape_invalid")
        else:
            if (
                self.result is not None
                or self.stale_as_of is not None
                or self.error_code is not None
                or self.retryable
            ):
                raise ValueError("analytics_non_result_panel_shape_invalid")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "panel_id": self.panel_id,
            "state": self.state.value,
            "query_fingerprint": self.query_fingerprint,
            "request_fingerprint": self.request_fingerprint,
            "foundation_fingerprint": self.foundation_fingerprint,
            "result": self.result.canonical_dict() if self.result is not None else None,
            "stale_as_of": (
                _utc_text(self.stale_as_of) if self.stale_as_of is not None else None
            ),
            "retryable": self.retryable,
            "error_code": self.error_code,
        }


@dataclass(frozen=True, slots=True)
class AnalyticsWorkspaceProjection:
    contract_version: str
    query_fingerprint: str
    request_fingerprint: str
    as_of: datetime
    panels: tuple[AnalyticsPanelEnvelope, ...]
    next_cursor: str | None = None

    def __post_init__(self) -> None:
        if self.contract_version != ANALYTICS_WORKSPACE_CONTRACT_VERSION:
            raise ValueError("analytics_workspace_contract_version_unsupported")
        if not _SHA256.fullmatch(self.query_fingerprint):
            raise ValueError("analytics_query_fingerprint_invalid")
        if not _SHA256.fullmatch(self.request_fingerprint):
            raise ValueError("analytics_request_fingerprint_invalid")
        object.__setattr__(
            self,
            "as_of",
            require_utc_datetime(self.as_of, field="workspace_as_of"),
        )
        if not isinstance(self.panels, tuple) or any(
            not isinstance(panel, AnalyticsPanelEnvelope) for panel in self.panels
        ):
            raise ValueError("analytics_workspace_panels_invalid")
        panel_ids = tuple(panel.panel_id for panel in self.panels)
        if len(set(panel_ids)) != len(panel_ids):
            raise ValueError("analytics_workspace_panel_duplicate")
        if any(
            panel.query_fingerprint != self.query_fingerprint
            or panel.request_fingerprint != self.request_fingerprint
            for panel in self.panels
        ):
            raise ValueError("analytics_workspace_panel_fingerprint_mismatch")
        if self.next_cursor is not None and (
            not isinstance(self.next_cursor, str)
            or not self.next_cursor
            or len(self.next_cursor) > MAX_ANALYTICS_CURSOR_LENGTH
        ):
            raise ValueError("analytics_next_cursor_invalid")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "query_fingerprint": self.query_fingerprint,
            "request_fingerprint": self.request_fingerprint,
            "as_of": _utc_text(self.as_of),
            "panels": [panel.canonical_dict() for panel in self.panels],
            "next_cursor": self.next_cursor,
        }


@runtime_checkable
class AnalyticsWorkspaceProjectionPort(Protocol):
    async def project_workspace(
        self,
        context: object,
        query: AnalyticsWorkspaceQuery,
    ) -> AnalyticsWorkspaceProjection: ...


__all__ = [
    "ANALYTICS_WORKSPACE_CONTRACT_VERSION",
    "MAX_ANALYTICS_CURSOR_LENGTH",
    "MAX_ANALYTICS_ENTITY_ID_LENGTH",
    "AnalyticsPanelEnvelope",
    "AnalyticsPanelState",
    "AnalyticsSortClause",
    "AnalyticsSortDirection",
    "AnalyticsWorkspaceLevel",
    "AnalyticsWorkspaceProjection",
    "AnalyticsWorkspaceProjectionPort",
    "AnalyticsWorkspaceQuery",
    "AnalyticsUtcWindow",
]
