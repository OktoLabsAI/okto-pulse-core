"""Canonical URL and state semantics for the Analytics workspace."""

from __future__ import annotations

import json
from datetime import datetime
from urllib.parse import parse_qsl, quote, urlencode

from okto_pulse.core.ports.analytics_foundation import (
    MAX_ANALYTICS_FILTERS,
    AnalyticsFilterClause,
    AnalyticsFoundationQuery,
    AnalyticsUtcWindow,
    require_utc_datetime,
)
from okto_pulse.core.ports.analytics_workspace import (
    ANALYTICS_WORKSPACE_CONTRACT_VERSION,
    AnalyticsPanelEnvelope,
    AnalyticsSortClause,
    AnalyticsSortDirection,
    AnalyticsWorkspaceLevel,
    AnalyticsWorkspaceProjection,
    AnalyticsWorkspaceQuery,
)


_URL_FIELDS = frozenset(
    {"level", "entity_id", "from", "to", "filters", "sort", "cursor", "as_of"}
)


def _utc_text(value: datetime) -> str:
    return (
        require_utc_datetime(value, field="url_timestamp")
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _parse_utc(value: str, *, field: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError(f"analytics_url_{field}_invalid")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"analytics_url_{field}_invalid") from exc
    return require_utc_datetime(parsed, field=f"url_{field}")


def _canonical_filters(
    filters: tuple[AnalyticsFilterClause, ...],
) -> tuple[AnalyticsFilterClause, ...]:
    return tuple(
        sorted(
            filters,
            key=lambda item: json.dumps(
                item.canonical_dict(), sort_keys=True, separators=(",", ":")
            ),
        )
    )


def _parse_filters(raw: str) -> tuple[AnalyticsFilterClause, ...]:
    try:
        payload = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("analytics_url_filters_invalid") from exc
    if not isinstance(payload, list) or len(payload) > MAX_ANALYTICS_FILTERS:
        raise ValueError("analytics_url_filters_invalid")
    parsed: list[AnalyticsFilterClause] = []
    for item in payload:
        if not isinstance(item, dict) or set(item) != {"field", "operator", "value"}:
            raise ValueError("analytics_url_filters_invalid")
        value = item["value"]
        if isinstance(value, list):
            value = tuple(value)
        try:
            parsed.append(
                AnalyticsFilterClause(
                    field=item["field"], operator=item["operator"], value=value
                )
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("analytics_url_filters_invalid") from exc
    return _canonical_filters(tuple(parsed))


class AnalyticsWorkspaceService:
    """Pure workspace orchestration shared by UI, REST, MCP and CSV adapters."""

    @staticmethod
    def to_query_string(query: AnalyticsWorkspaceQuery) -> str:
        filters = _canonical_filters(query.foundation.filters)
        params: list[tuple[str, str]] = [
            ("level", query.level.value),
            ("from", _utc_text(query.foundation.window.from_inclusive)),
            ("to", _utc_text(query.foundation.window.to_exclusive)),
            (
                "filters",
                json.dumps(
                    [item.canonical_dict() for item in filters],
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ),
            ("sort", f"{query.sort.field}:{query.sort.direction.value}"),
        ]
        if query.entity_id is not None:
            params.append(("entity_id", query.entity_id))
        if query.cursor is not None:
            params.append(("cursor", query.cursor))
        if query.foundation.as_of is not None:
            params.append(("as_of", _utc_text(query.foundation.as_of)))
        params.sort(key=lambda item: item[0])
        return urlencode(params, doseq=False, quote_via=quote, safe="")

    @staticmethod
    def from_query_string(
        query_string: str,
        *,
        board_id: str,
        actor_scope_ref: str,
    ) -> AnalyticsWorkspaceQuery:
        if not isinstance(query_string, str) or len(query_string) > 16_384:
            raise ValueError("analytics_url_query_invalid")
        raw = query_string[1:] if query_string.startswith("?") else query_string
        try:
            pairs = parse_qsl(
                raw,
                keep_blank_values=True,
                strict_parsing=True,
                max_num_fields=16,
            )
        except ValueError as exc:
            raise ValueError("analytics_url_query_invalid") from exc
        if any(key not in _URL_FIELDS for key, _ in pairs):
            raise ValueError("analytics_url_field_unsupported")
        if len({key for key, _ in pairs}) != len(pairs):
            raise ValueError("analytics_url_field_duplicate")
        values = dict(pairs)
        required = {"level", "from", "to", "filters", "sort"}
        if not required.issubset(values):
            raise ValueError("analytics_url_field_missing")
        try:
            level = AnalyticsWorkspaceLevel(values["level"])
        except ValueError as exc:
            raise ValueError("analytics_workspace_level_invalid") from exc
        sort_parts = values["sort"].split(":")
        if len(sort_parts) != 2:
            raise ValueError("analytics_url_sort_invalid")
        try:
            sort = AnalyticsSortClause(
                sort_parts[0], AnalyticsSortDirection(sort_parts[1])
            )
        except ValueError as exc:
            raise ValueError("analytics_url_sort_invalid") from exc
        window = AnalyticsUtcWindow(
            _parse_utc(values["from"], field="from"),
            _parse_utc(values["to"], field="to"),
        )
        foundation = AnalyticsFoundationQuery(
            board_id=board_id,
            actor_scope_ref=actor_scope_ref,
            window=window,
            filters=_parse_filters(values["filters"]),
            as_of=(
                _parse_utc(values["as_of"], field="as_of")
                if "as_of" in values
                else None
            ),
        )
        return AnalyticsWorkspaceQuery(
            foundation=foundation,
            level=level,
            entity_id=values.get("entity_id"),
            sort=sort,
            cursor=values.get("cursor"),
        )

    @staticmethod
    def accept_panel_update(
        *,
        current: AnalyticsPanelEnvelope,
        incoming: AnalyticsPanelEnvelope,
        current_request_fingerprint: str,
    ) -> AnalyticsPanelEnvelope:
        """Apply only a response belonging to the current panel request."""

        if incoming.panel_id != current.panel_id:
            raise ValueError("analytics_panel_update_id_mismatch")
        if incoming.request_fingerprint != current_request_fingerprint:
            return current
        return incoming

    @staticmethod
    def projection(
        *,
        query: AnalyticsWorkspaceQuery,
        as_of: datetime,
        panels: tuple[AnalyticsPanelEnvelope, ...],
        next_cursor: str | None = None,
    ) -> AnalyticsWorkspaceProjection:
        if any(
            panel.foundation_fingerprint != query.foundation.fingerprint
            for panel in panels
        ):
            raise ValueError("analytics_workspace_panel_foundation_mismatch")
        return AnalyticsWorkspaceProjection(
            contract_version=ANALYTICS_WORKSPACE_CONTRACT_VERSION,
            query_fingerprint=query.query_fingerprint,
            request_fingerprint=query.request_fingerprint,
            as_of=as_of,
            panels=panels,
            next_cursor=next_cursor,
        )


__all__ = ["AnalyticsWorkspaceService"]
