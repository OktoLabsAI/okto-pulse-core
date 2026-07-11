"""Server-side selector option catalog for discovery intent parameters."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
import logging
import time
from types import SimpleNamespace
from typing import Any, Literal, Protocol, runtime_checkable

from okto_pulse.core.ports.discovery_selector import (
    SelectorCardFact,
    SelectorSpecFact,
    get_discovery_selector_read_port,
)
from okto_pulse.core.services.reference_resolution import resolve_spec_references


logger = logging.getLogger(__name__)

SelectorKind = Literal["spec", "spec_child", "card"]
SpecChildType = Literal[
    "functional_requirement",
    "business_rule",
    "technical_requirement",
    "decision",
    "acceptance_criterion",
    "api_contract",
    "integration_requirement",
    "observability_requirement",
]

SELECTOR_KIND_SPEC: SelectorKind = "spec"
SELECTOR_KIND_SPEC_CHILD: SelectorKind = "spec_child"
SELECTOR_KIND_CARD: SelectorKind = "card"

SPEC_CHILD_TYPE_FUNCTIONAL_REQUIREMENT: SpecChildType = "functional_requirement"
SPEC_CHILD_TYPE_BUSINESS_RULE: SpecChildType = "business_rule"
SPEC_CHILD_TYPE_TECHNICAL_REQUIREMENT: SpecChildType = "technical_requirement"
SPEC_CHILD_TYPE_DECISION: SpecChildType = "decision"
SPEC_CHILD_TYPE_ACCEPTANCE_CRITERION: SpecChildType = "acceptance_criterion"
SPEC_CHILD_TYPE_API_CONTRACT: SpecChildType = "api_contract"
SPEC_CHILD_TYPE_INTEGRATION_REQUIREMENT: SpecChildType = "integration_requirement"
SPEC_CHILD_TYPE_OBSERVABILITY_REQUIREMENT: SpecChildType = "observability_requirement"

SUPPORTED_SPEC_CHILD_TYPES: tuple[SpecChildType, ...] = (
    SPEC_CHILD_TYPE_FUNCTIONAL_REQUIREMENT,
    SPEC_CHILD_TYPE_BUSINESS_RULE,
    SPEC_CHILD_TYPE_TECHNICAL_REQUIREMENT,
    SPEC_CHILD_TYPE_DECISION,
    SPEC_CHILD_TYPE_ACCEPTANCE_CRITERION,
    SPEC_CHILD_TYPE_API_CONTRACT,
    SPEC_CHILD_TYPE_INTEGRATION_REQUIREMENT,
    SPEC_CHILD_TYPE_OBSERVABILITY_REQUIREMENT,
)

SPEC_CHILD_REFERENCE_GROUPS: dict[SpecChildType, str] = {
    SPEC_CHILD_TYPE_FUNCTIONAL_REQUIREMENT: "functional_requirements",
    SPEC_CHILD_TYPE_BUSINESS_RULE: "business_rules",
    SPEC_CHILD_TYPE_TECHNICAL_REQUIREMENT: "technical_requirements",
    SPEC_CHILD_TYPE_DECISION: "decisions",
    SPEC_CHILD_TYPE_ACCEPTANCE_CRITERION: "acceptance_criteria",
    SPEC_CHILD_TYPE_API_CONTRACT: "api_contracts",
    SPEC_CHILD_TYPE_INTEGRATION_REQUIREMENT: "integration_requirements",
    SPEC_CHILD_TYPE_OBSERVABILITY_REQUIREMENT: "observability_requirements",
}

SPEC_CHILD_LABEL_PREFIX: dict[SpecChildType, str] = {
    SPEC_CHILD_TYPE_FUNCTIONAL_REQUIREMENT: "FR",
    SPEC_CHILD_TYPE_BUSINESS_RULE: "BR",
    SPEC_CHILD_TYPE_TECHNICAL_REQUIREMENT: "TR",
    SPEC_CHILD_TYPE_DECISION: "Decision",
    SPEC_CHILD_TYPE_ACCEPTANCE_CRITERION: "AC",
    SPEC_CHILD_TYPE_API_CONTRACT: "API",
    SPEC_CHILD_TYPE_INTEGRATION_REQUIREMENT: "IR",
    SPEC_CHILD_TYPE_OBSERVABILITY_REQUIREMENT: "OR",
}

SAFE_SELECTOR_OPTION_FIELDS = frozenset(
    {
        "id",
        "label",
        "entity_type",
        "subtitle",
        "spec_id",
        "spec_title",
        "child_type",
        "child_id",
        "child_index",
        "child_ref",
        "status",
        "version",
        "order",
        "refs",
    }
)

SAFE_REF_FIELDS = frozenset(
    {
        "linked_requirements",
        "linked_rules",
        "linked_task_ids",
        "linked_criteria",
        "linked_api_contracts",
        "linked_integration_requirements",
        "linked_observability_requirements",
        "supersedes_decision_id",
    }
)

SELECTOR_CACHE_MAX_TTL_SECONDS = 60
SELECTOR_CACHE_DEFAULT_TTL_SECONDS = 60

SELECTOR_EVENT_STRUCTURED_CREATED = "structured_entity.created"
SELECTOR_EVENT_STRUCTURED_UPDATED = "structured_entity.updated"
SELECTOR_EVENT_STRUCTURED_REVOKED = "structured_entity.revoked"
SELECTOR_EVENT_SPEC_UPDATED = "spec.updated"
SELECTOR_EVENT_KG_REBUILT = "kg.rebuilt"
SUPPORTED_SELECTOR_INVALIDATION_EVENTS = frozenset(
    {
        SELECTOR_EVENT_STRUCTURED_CREATED,
        SELECTOR_EVENT_STRUCTURED_UPDATED,
        SELECTOR_EVENT_STRUCTURED_REVOKED,
        SELECTOR_EVENT_SPEC_UPDATED,
        SELECTOR_EVENT_KG_REBUILT,
    }
)


class DiscoverySelectorCatalogError(Exception):
    """Base error for selector catalog failures."""


class DiscoverySelectorAccessDenied(DiscoverySelectorCatalogError, PermissionError):
    """Raised when selector data is requested without board/spec access."""


class DiscoverySelectorInvalidRequest(DiscoverySelectorCatalogError, ValueError):
    """Raised when the selector request does not match the catalog contract."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "invalid_selector_dependency",
    ) -> None:
        super().__init__(message)
        self.code = code


class DiscoverySelectorSpecNotFound(DiscoverySelectorCatalogError, LookupError):
    """Raised when a requested spec cannot be found on the board."""


class DiscoverySelectorUnsafeProjection(DiscoverySelectorCatalogError, RuntimeError):
    """Raised if a selector payload would leak a field outside the allowlist."""


@runtime_checkable
class SelectorAccessPolicy(Protocol):
    """Authorization boundary required before selector projection."""

    async def can_read_board(
        self,
        db: Any,
        identity: Any,
        board_id: str,
    ) -> bool:
        """Return whether the identity can list board-level selector options."""

    async def can_read_spec(
        self,
        db: Any,
        identity: Any,
        spec: SelectorSpecFact,
    ) -> bool:
        """Return whether the identity can read selector options for the spec."""


class DenyAllSelectorAccessPolicy:
    """Default fail-closed selector access policy."""

    async def can_read_board(
        self,
        db: Any,
        identity: Any,
        board_id: str,
    ) -> bool:
        return False

    async def can_read_spec(
        self,
        db: Any,
        identity: Any,
        spec: SelectorSpecFact,
    ) -> bool:
        return False


class AllowAllSelectorAccessPolicy:
    """Explicit test/dev helper; production wiring should inject real auth."""

    async def can_read_board(
        self,
        db: Any,
        identity: Any,
        board_id: str,
    ) -> bool:
        return True

    async def can_read_spec(
        self,
        db: Any,
        identity: Any,
        spec: SelectorSpecFact,
    ) -> bool:
        return True


@dataclass(frozen=True)
class SelectorOption:
    """Safe selector option projected from a first-class entity or child item."""

    id: str
    label: str
    entity_type: str
    subtitle: str | None = None
    spec_id: str | None = None
    spec_title: str | None = None
    child_type: SpecChildType | None = None
    child_id: str | None = None
    child_index: int | None = None
    child_ref: str | None = None
    status: str | None = None
    version: int | str | None = None
    order: int | None = None
    refs: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "id": self.id,
            "label": self.label,
            "entity_type": self.entity_type,
            "subtitle": self.subtitle,
            "spec_id": self.spec_id,
            "spec_title": self.spec_title,
            "child_type": self.child_type,
            "child_id": self.child_id,
            "child_index": self.child_index,
            "child_ref": self.child_ref,
            "status": self.status,
            "version": self.version,
            "order": self.order,
            "refs": self.refs,
        }
        compact = {key: value for key, value in payload.items() if value not in (None, {}, [])}
        validate_safe_selector_payload(compact)
        return compact


@dataclass(frozen=True)
class SelectorOptionsResult:
    """Catalog response envelope with explicit source semantics."""

    options: list[SelectorOption]
    source: str = "board_db_spec_json"
    cache_status: str = "bypass"
    global_refs_used: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "options": [option.to_dict() for option in self.options],
            "source": self.source,
            "cache_status": self.cache_status,
            "global_refs_used": self.global_refs_used,
        }


@dataclass(frozen=True)
class SelectorCacheKey:
    """Normalized selector option cache key."""

    board_id: str
    selector_kind: SelectorKind
    spec_id: str | None
    child_type: str | None
    status: str
    q: str | None
    limit: int
    offset: int
    include_superseded: bool


@dataclass(frozen=True)
class SelectorMetricEvent:
    """Safe selector metric event emitted through a pluggable sink."""

    metric_name: str
    value: int | float
    labels: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class SelectorMetricsSinkProtocol(Protocol):
    """Sink for selector metrics/log-backed counters."""

    def emit(self, event: SelectorMetricEvent) -> None:
        """Emit one metric event."""


class LoggingSelectorMetricsSink:
    """Default metric sink backed by structured logs with safe labels only."""

    def emit(self, event: SelectorMetricEvent) -> None:
        logger.info(
            "discovery.selector.metric name=%s value=%s",
            event.metric_name,
            event.value,
            extra={
                "event": "discovery.selector.metric",
                "metric_name": event.metric_name,
                "value": event.value,
                **event.labels,
            },
        )


@dataclass(frozen=True)
class SelectorCacheInvalidationResult:
    """Result from event-driven selector cache invalidation."""

    outcome: str
    invalidated_count: int
    matched_scope: dict[str, Any]


@dataclass(frozen=True)
class _SelectorCacheEntry:
    result: SelectorOptionsResult
    expires_at: float


def _normalized_q(q: str | None) -> str | None:
    if q is None:
        return None
    normalized = " ".join(str(q).strip().split()).casefold()
    return normalized or None


def normalize_selector_cache_key(
    *,
    board_id: str,
    selector_kind: SelectorKind,
    spec_id: str | None = None,
    child_type: str | None = None,
    status: str | None = "active",
    q: str | None = None,
    limit: int | None = 50,
    offset: int | None = 0,
    include_superseded: bool = False,
) -> SelectorCacheKey:
    """Normalize selector cache dimensions into a deterministic key."""

    normalized_status = str(status or "active")
    return SelectorCacheKey(
        board_id=str(board_id),
        selector_kind=selector_kind,
        spec_id=str(spec_id) if spec_id else None,
        child_type=str(child_type) if child_type else None,
        status=normalized_status,
        q=_normalized_q(q),
        limit=_normalize_limit(limit),
        offset=_normalize_offset(offset),
        include_superseded=bool(include_superseded),
    )


def _result_with_cache_status(
    result: SelectorOptionsResult,
    cache_status: str,
) -> SelectorOptionsResult:
    return SelectorOptionsResult(
        options=list(result.options),
        source=result.source,
        cache_status=cache_status,
        global_refs_used=result.global_refs_used,
    )


class DiscoverySelectorCache:
    """Small in-memory cache for selector option results.

    The cache stores derived metadata only. Authorization still happens before
    catalog calls can return cached data.
    """

    def __init__(
        self,
        *,
        ttl_seconds: int = SELECTOR_CACHE_DEFAULT_TTL_SECONDS,
        now_fn: Any = time.monotonic,
        metrics_sink: SelectorMetricsSinkProtocol | None = None,
    ) -> None:
        self.ttl_seconds = max(0, min(int(ttl_seconds), SELECTOR_CACHE_MAX_TTL_SECONDS))
        self._now_fn = now_fn
        self._entries: dict[SelectorCacheKey, _SelectorCacheEntry] = {}
        self._metrics_sink = metrics_sink or LoggingSelectorMetricsSink()

    def get(self, key: SelectorCacheKey) -> SelectorOptionsResult | None:
        entry = self._entries.get(key)
        if entry is None:
            self._emit_cache_total(key, outcome="miss")
            return None
        if entry.expires_at <= self._now_fn():
            self._entries.pop(key, None)
            self._emit_cache_total(key, outcome="miss")
            return None
        self._emit_cache_total(key, outcome="hit")
        return _result_with_cache_status(entry.result, "hit")

    def set(self, key: SelectorCacheKey, result: SelectorOptionsResult) -> None:
        if self.ttl_seconds <= 0:
            self._emit_cache_total(key, outcome="bypass")
            return
        self._entries[key] = _SelectorCacheEntry(
            result=_result_with_cache_status(result, "stored"),
            expires_at=self._now_fn() + self.ttl_seconds,
        )
        self._emit_cache_total(key, outcome="stored")

    def clear(self) -> int:
        count = len(self._entries)
        self._entries.clear()
        return count

    def invalidate(
        self,
        *,
        board_id: str,
        spec_id: str | None = None,
        child_type: str | None = None,
    ) -> int:
        keys_to_delete = [
            key
            for key in self._entries
            if key.board_id == board_id
            and (spec_id is None or key.spec_id == spec_id)
            and (child_type is None or key.child_type == child_type)
        ]
        for key in keys_to_delete:
            self._entries.pop(key, None)
        return len(keys_to_delete)

    def invalidate_event(self, event: dict[str, Any]) -> SelectorCacheInvalidationResult:
        event_type = str(event.get("event_type") or event.get("event") or "")
        board_id = str(event.get("board_id") or "")
        if event_type not in SUPPORTED_SELECTOR_INVALIDATION_EVENTS or not board_id:
            result = SelectorCacheInvalidationResult(
                outcome="noop",
                invalidated_count=0,
                matched_scope={"event_type": event_type or "unknown"},
            )
            self._emit_invalidation(event_type=event_type, board_id=board_id, result=result)
            return result

        spec_id = event.get("spec_id")
        child_type = event.get("child_type")
        if event_type == SELECTOR_EVENT_KG_REBUILT and not spec_id:
            spec_id = None
            child_type = None
        elif event_type == SELECTOR_EVENT_SPEC_UPDATED:
            child_type = None

        count = self.invalidate(
            board_id=board_id,
            spec_id=str(spec_id) if spec_id else None,
            child_type=str(child_type) if child_type else None,
        )
        result = SelectorCacheInvalidationResult(
            outcome="invalidated" if count else "noop",
            invalidated_count=count,
            matched_scope={
                "event_type": event_type,
                "board_id": board_id,
                "spec_id": str(spec_id) if spec_id else None,
                "child_type": str(child_type) if child_type else None,
            },
        )
        self._emit_invalidation(event_type=event_type, board_id=board_id, result=result)
        return result

    def _emit_cache_total(self, key: SelectorCacheKey, *, outcome: str) -> None:
        self._metrics_sink.emit(
            SelectorMetricEvent(
                metric_name="discovery_selector_cache_total",
                value=1,
                labels={
                    "board_id": key.board_id,
                    "selector_kind": key.selector_kind,
                    "child_type": key.child_type or "none",
                    "cache_status": outcome,
                    "outcome": outcome,
                },
            )
        )

    def _emit_invalidation(
        self,
        *,
        event_type: str,
        board_id: str,
        result: SelectorCacheInvalidationResult,
    ) -> None:
        scope = result.matched_scope
        self._metrics_sink.emit(
            SelectorMetricEvent(
                metric_name="discovery_selector_cache_invalidation_total",
                value=result.invalidated_count,
                labels={
                    "board_id": board_id or "unknown",
                    "event_type": event_type or "unknown",
                    "child_type": scope.get("child_type") or "none",
                    "outcome": result.outcome,
                },
            )
        )


_DEFAULT_SELECTOR_CACHE = DiscoverySelectorCache()


def get_default_discovery_selector_cache() -> DiscoverySelectorCache:
    return _DEFAULT_SELECTOR_CACHE


def validate_safe_selector_payload(payload: dict[str, Any]) -> None:
    """Reject selector payloads that contain non-allowlisted fields."""

    unsafe = set(payload) - SAFE_SELECTOR_OPTION_FIELDS
    if unsafe:
        LoggingSelectorMetricsSink().emit(
            SelectorMetricEvent(
                metric_name="discovery_selector_unsafe_projection_total",
                value=1,
                labels={"outcome": "rejected"},
            )
        )
        raise DiscoverySelectorUnsafeProjection(
            f"Selector projection contains unsafe fields: {sorted(unsafe)}"
        )


def _enum_value(value: Any) -> Any:
    return getattr(value, "value", value)


def _coerce_child_type(child_type: str | None) -> SpecChildType:
    if child_type not in SPEC_CHILD_REFERENCE_GROUPS:
        supported = ", ".join(SUPPORTED_SPEC_CHILD_TYPES)
        raise DiscoverySelectorInvalidRequest(
            f"Unsupported spec_child type {child_type!r}; supported types: {supported}",
            code="unsupported_child_type",
        )
    return child_type  # type: ignore[return-value]


def _normalize_limit(limit: int | None) -> int:
    if limit is None:
        return 50
    return max(1, min(limit, 100))


def _normalize_offset(offset: int | None) -> int:
    if offset is None:
        return 0
    return max(0, offset)


def _compact_text(value: Any, *, max_len: int = 96) -> str | None:
    if value is None:
        return None
    text = str(value).replace("\r", " ").replace("\n", " ").strip()
    text = " ".join(text.split())
    if not text:
        return None
    if len(text) <= max_len:
        return text
    return f"{text[: max_len - 1].rstrip()}..."


def _status_matches(value: Any, requested: str | None) -> bool:
    if requested in (None, "", "all"):
        return True
    status = _enum_value(value)
    if status is None:
        return requested == "active"
    return str(status) == requested


def _matches_query(option: SelectorOption, query: str | None) -> bool:
    if not query:
        return True
    needle = query.casefold()
    haystack = " ".join(
        part
        for part in (
            option.label,
            option.id,
            option.subtitle,
            option.spec_title,
            option.child_id,
            option.child_ref,
        )
        if part
    ).casefold()
    return needle in haystack


def _slice_options(
    options: Sequence[SelectorOption],
    *,
    offset: int,
    limit: int,
) -> list[SelectorOption]:
    start = _normalize_offset(offset)
    end = start + _normalize_limit(limit)
    return list(options[start:end])


def _safe_refs(item: dict[str, Any]) -> dict[str, Any]:
    refs: dict[str, Any] = {}
    for key in SAFE_REF_FIELDS:
        value = item.get(key)
        if value not in (None, [], ""):
            refs[key] = value
    return refs


def _child_identity(
    *,
    spec_id: str,
    child_type: SpecChildType,
    item: dict[str, Any],
    fallback_index: int,
) -> tuple[str, int | None, str]:
    raw_id = item.get("id")
    index = item.get("index")
    if isinstance(index, bool):
        index = None
    if not isinstance(index, int):
        index = fallback_index
    child_id = str(raw_id if raw_id not in (None, "") else index)
    child_ref = f"spec:{spec_id}:{child_type}:{child_id}"
    return child_id, index, child_ref


def _child_label(
    *,
    child_type: SpecChildType,
    item: dict[str, Any],
    index: int | None,
) -> str:
    prefix = SPEC_CHILD_LABEL_PREFIX[child_type]
    if child_type == SPEC_CHILD_TYPE_API_CONTRACT:
        method = _compact_text(item.get("method"), max_len=16)
        path = _compact_text(item.get("path"), max_len=80)
        if method and path:
            return f"{method.upper()} {path}"
    for key in ("title", "name", "label"):
        label = _compact_text(item.get(key), max_len=96)
        if label:
            return label
    if child_type == SPEC_CHILD_TYPE_DECISION:
        rationale = _compact_text(item.get("rationale"), max_len=96)
        if rationale:
            return rationale
    display_index = (index + 1) if isinstance(index, int) else None
    return f"{prefix} {display_index}" if display_index else prefix


def _child_subtitle(
    *,
    child_type: SpecChildType,
    item: dict[str, Any],
) -> str | None:
    candidate_keys = {
        SPEC_CHILD_TYPE_FUNCTIONAL_REQUIREMENT: ("text",),
        SPEC_CHILD_TYPE_TECHNICAL_REQUIREMENT: ("text", "description"),
        SPEC_CHILD_TYPE_ACCEPTANCE_CRITERION: ("text", "description"),
        SPEC_CHILD_TYPE_BUSINESS_RULE: ("rule", "description"),
        SPEC_CHILD_TYPE_DECISION: ("rationale", "notes"),
        SPEC_CHILD_TYPE_API_CONTRACT: ("description",),
        SPEC_CHILD_TYPE_INTEGRATION_REQUIREMENT: ("provider", "consumer", "endpoint"),
        SPEC_CHILD_TYPE_OBSERVABILITY_REQUIREMENT: ("metric_name", "target", "threshold"),
    }[child_type]
    parts = [
        _compact_text(item.get(key), max_len=48)
        for key in candidate_keys
        if item.get(key) not in (None, "")
    ]
    text = " · ".join(part for part in parts if part)
    return _compact_text(text, max_len=128)


def _project_spec_option(spec: SelectorSpecFact) -> SelectorOption:
    status = _enum_value(getattr(spec, "status", None))
    return SelectorOption(
        id=str(spec.id),
        label=str(getattr(spec, "title", None) or spec.id),
        entity_type="spec",
        spec_id=str(spec.id),
        spec_title=getattr(spec, "title", None),
        status=str(status) if status is not None else None,
        version=getattr(spec, "version", None),
        order=None,
    )


def _project_card_option(card: SelectorCardFact) -> SelectorOption:
    status = _enum_value(getattr(card, "status", None))
    priority = _enum_value(getattr(card, "priority", None))
    card_type = _enum_value(getattr(card, "card_type", None))
    subtitle_parts = [
        f"status={status}" if status else None,
        f"priority={priority}" if priority else None,
    ]
    subtitle = " · ".join(part for part in subtitle_parts if part)
    refs = {
        key: value
        for key, value in {
            "card_id": str(card.id),
            "spec_id": getattr(card, "spec_id", None),
            "sprint_id": getattr(card, "sprint_id", None),
            "card_type": str(card_type) if card_type is not None else None,
        }.items()
        if value not in (None, "", [], {})
    }
    return SelectorOption(
        id=str(card.id),
        label=str(getattr(card, "title", None) or card.id),
        entity_type="card",
        subtitle=subtitle or None,
        status=str(status) if status is not None else None,
        order=getattr(card, "position", None),
        refs=refs,
    )


def _project_child_option(
    *,
    spec: SelectorSpecFact,
    child_type: SpecChildType,
    item: dict[str, Any],
    fallback_index: int,
) -> SelectorOption:
    spec_id = str(spec.id)
    child_id, child_index, child_ref = _child_identity(
        spec_id=spec_id,
        child_type=child_type,
        item=item,
        fallback_index=fallback_index,
    )
    status = item.get("status", "active")
    order = item.get("order")
    if not isinstance(order, int):
        order = child_index
    return SelectorOption(
        id=child_ref,
        label=_child_label(child_type=child_type, item=item, index=child_index),
        entity_type="spec_child",
        subtitle=_child_subtitle(child_type=child_type, item=item),
        spec_id=spec_id,
        spec_title=getattr(spec, "title", None),
        child_type=child_type,
        child_id=child_id,
        child_index=child_index,
        child_ref=child_ref,
        status=str(_enum_value(status)) if status is not None else None,
        version=item.get("version") or getattr(spec, "version", None),
        order=order,
        refs=_safe_refs(item),
    )


def structured_spec_snapshot(spec: SelectorSpecFact) -> Any:
    """Return only structured selector fields, avoiding artifact lazy loads."""

    return SimpleNamespace(
        id=getattr(spec, "id", None),
        title=getattr(spec, "title", None),
        functional_requirements=list(getattr(spec, "functional_requirements", None) or []),
        business_rules=list(getattr(spec, "business_rules", None) or []),
        technical_requirements=list(getattr(spec, "technical_requirements", None) or []),
        decisions=list(getattr(spec, "decisions", None) or []),
        acceptance_criteria=list(getattr(spec, "acceptance_criteria", None) or []),
        api_contracts=list(getattr(spec, "api_contracts", None) or []),
        integration_requirements=list(
            getattr(spec, "integration_requirements", None) or []
        ),
        observability_requirements=list(
            getattr(spec, "observability_requirements", None) or []
        ),
    )


# Backward-compatible private alias used by older tests/imports.
_structured_spec_snapshot = structured_spec_snapshot


class DiscoverySelectorCatalog:
    """Build discovery selector options from the board DB and spec JSON."""

    def __init__(
        self,
        access_policy: SelectorAccessPolicy | None = None,
        *,
        cache: DiscoverySelectorCache | None = None,
    ) -> None:
        self._access_policy = access_policy or DenyAllSelectorAccessPolicy()
        self._cache = cache

    async def list_options(
        self,
        db: Any,
        *,
        board_id: str,
        selector_kind: SelectorKind,
        identity: Any = None,
        spec_id: str | None = None,
        child_type: str | None = None,
        status: str | None = "active",
        q: str | None = None,
        limit: int | None = 50,
        offset: int | None = 0,
        include_superseded: bool = False,
    ) -> SelectorOptionsResult:
        """Return safe selector options for a board or spec-child collection."""

        if selector_kind == SELECTOR_KIND_SPEC:
            if spec_id or child_type:
                raise DiscoverySelectorInvalidRequest(
                    "spec selector options must not include spec_id or child_type"
                )
            return await self._list_spec_options(
                db,
                board_id=board_id,
                identity=identity,
                status=status,
                q=q,
                limit=limit,
                offset=offset,
                include_superseded=include_superseded,
            )

        if selector_kind == SELECTOR_KIND_CARD:
            if spec_id or child_type:
                raise DiscoverySelectorInvalidRequest(
                    "card selector options must not include spec_id or child_type"
                )
            return await self._list_card_options(
                db,
                board_id=board_id,
                identity=identity,
                status=status,
                q=q,
                limit=limit,
                offset=offset,
                include_superseded=include_superseded,
            )

        if selector_kind == SELECTOR_KIND_SPEC_CHILD:
            canonical_child_type = _coerce_child_type(child_type)
            if not spec_id:
                raise DiscoverySelectorInvalidRequest(
                    "spec_child selector options require spec_id"
                )
            return await self._list_spec_child_options(
                db,
                board_id=board_id,
                identity=identity,
                spec_id=spec_id,
                child_type=canonical_child_type,
                status=status,
                q=q,
                limit=limit,
                offset=offset,
                include_superseded=include_superseded,
            )

        raise DiscoverySelectorInvalidRequest(
            f"Unsupported selector kind {selector_kind!r}",
            code="unsupported_selector_kind",
        )

    async def _list_spec_options(
        self,
        db: Any,
        *,
        board_id: str,
        identity: Any,
        status: str | None,
        q: str | None,
        limit: int | None,
        offset: int | None,
        include_superseded: bool,
    ) -> SelectorOptionsResult:
        if not await self._access_policy.can_read_board(db, identity, board_id):
            raise DiscoverySelectorAccessDenied("Board selector access denied")

        specs = await self._load_specs(db, board_id=board_id, status=status)
        readable_specs: list[SelectorSpecFact] = []
        for spec in specs:
            if await self._access_policy.can_read_spec(db, identity, spec):
                readable_specs.append(spec)
        cache_key = normalize_selector_cache_key(
            board_id=board_id,
            selector_kind=SELECTOR_KIND_SPEC,
            status=status,
            q=q,
            limit=limit,
            offset=offset,
            include_superseded=include_superseded,
        )
        if self._cache:
            cached = self._cache.get(cache_key)
            if cached is not None:
                readable_ids = {str(spec.id) for spec in readable_specs}
                return SelectorOptionsResult(
                    options=[
                        option
                        for option in cached.options
                        if option.spec_id in readable_ids or option.id in readable_ids
                    ],
                    source=cached.source,
                    cache_status=cached.cache_status,
                    global_refs_used=cached.global_refs_used,
                )
        options = [_project_spec_option(spec) for spec in readable_specs]
        options = [option for option in options if _matches_query(option, q)]
        result = SelectorOptionsResult(
            options=_slice_options(options, offset=_normalize_offset(offset), limit=_normalize_limit(limit))
        )
        if self._cache:
            self._cache.set(cache_key, result)
            return _result_with_cache_status(result, "miss")
        return result

    async def _list_card_options(
        self,
        db: Any,
        *,
        board_id: str,
        identity: Any,
        status: str | None,
        q: str | None,
        limit: int | None,
        offset: int | None,
        include_superseded: bool,
    ) -> SelectorOptionsResult:
        if not await self._access_policy.can_read_board(db, identity, board_id):
            raise DiscoverySelectorAccessDenied("Board selector access denied")

        cache_key = normalize_selector_cache_key(
            board_id=board_id,
            selector_kind=SELECTOR_KIND_CARD,
            status=status,
            q=q,
            limit=limit,
            offset=offset,
            include_superseded=include_superseded,
        )
        if self._cache:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached

        cards = await self._load_cards(db, board_id=board_id, status=status)
        options = [_project_card_option(card) for card in cards]
        options = [option for option in options if _matches_query(option, q)]
        result = SelectorOptionsResult(
            options=_slice_options(
                options,
                offset=_normalize_offset(offset),
                limit=_normalize_limit(limit),
            )
        )
        if self._cache:
            self._cache.set(cache_key, result)
            return _result_with_cache_status(result, "miss")
        return result

    async def _list_spec_child_options(
        self,
        db: Any,
        *,
        board_id: str,
        identity: Any,
        spec_id: str,
        child_type: SpecChildType,
        status: str | None,
        q: str | None,
        limit: int | None,
        offset: int | None,
        include_superseded: bool,
    ) -> SelectorOptionsResult:
        spec = await self._load_spec(db, board_id=board_id, spec_id=spec_id)
        if spec is None:
            raise DiscoverySelectorSpecNotFound(f"Spec {spec_id!r} not found on board")
        if not await self._access_policy.can_read_spec(db, identity, spec):
            raise DiscoverySelectorAccessDenied("Spec selector access denied")

        cache_key = normalize_selector_cache_key(
            board_id=board_id,
            selector_kind=SELECTOR_KIND_SPEC_CHILD,
            spec_id=spec_id,
            child_type=child_type,
            status=status,
            q=q,
            limit=limit,
            offset=offset,
            include_superseded=include_superseded,
        )
        if self._cache:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached

        refs = resolve_spec_references(
            structured_spec_snapshot(spec),
            include_superseded=include_superseded,
            include_content=False,
        )
        group_name = SPEC_CHILD_REFERENCE_GROUPS[child_type]
        projected = [
            _project_child_option(
                spec=spec,
                child_type=child_type,
                item=item,
                fallback_index=index,
            )
            for index, item in enumerate(refs.get(group_name) or [])
            if _status_matches(item.get("status", "active"), status)
        ]
        projected = [option for option in projected if _matches_query(option, q)]
        result = SelectorOptionsResult(
            options=_slice_options(
                projected,
                offset=_normalize_offset(offset),
                limit=_normalize_limit(limit),
            )
        )
        if self._cache:
            self._cache.set(cache_key, result)
            return _result_with_cache_status(result, "miss")
        return result

    async def _load_specs(
        self,
        db: Any,
        *,
        board_id: str,
        status: str | None,
    ) -> list[SelectorSpecFact]:
        return list(
            await get_discovery_selector_read_port().list_specs(
                db,
                board_id=board_id,
                status=status,
            )
        )

    async def _load_cards(
        self,
        db: Any,
        *,
        board_id: str,
        status: str | None,
    ) -> list[SelectorCardFact]:
        return list(
            await get_discovery_selector_read_port().list_cards(
                db,
                board_id=board_id,
                status=status,
            )
        )

    async def _load_spec(
        self,
        db: Any,
        *,
        board_id: str,
        spec_id: str,
    ) -> SelectorSpecFact | None:
        return await get_discovery_selector_read_port().get_spec(
            db,
            board_id=board_id,
            spec_id=spec_id,
        )
