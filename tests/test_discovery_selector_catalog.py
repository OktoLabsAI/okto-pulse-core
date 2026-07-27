from __future__ import annotations

from types import SimpleNamespace

import pytest

from okto_pulse.core.events.handlers.discovery_selector_cache import (
    DiscoverySelectorCacheInvalidationHandler,
)
from okto_pulse.core.events.types import SpecVersionBumped
from sqlalchemy_test_models import Card, CardPriority, CardStatus, Spec, SpecStatus
from okto_pulse.core.services.discovery_selector_catalog import (
    SELECTOR_CACHE_MAX_TTL_SECONDS,
    SELECTOR_EVENT_KG_REBUILT,
    SELECTOR_EVENT_SPEC_UPDATED,
    SAFE_SELECTOR_OPTION_FIELDS,
    SUPPORTED_SPEC_CHILD_TYPES,
    DiscoverySelectorCache,
    DiscoverySelectorAccessDenied,
    DiscoverySelectorCatalog,
    DiscoverySelectorInvalidRequest,
    DiscoverySelectorSpecNotFound,
    DiscoverySelectorUnsafeProjection,
    SelectorMetricEvent,
    SelectorOptionsResult,
    SelectorAccessPolicy,
    get_default_discovery_selector_cache,
    normalize_selector_cache_key,
    structured_spec_snapshot,
    validate_safe_selector_payload,
)


class RecordingAccessPolicy(SelectorAccessPolicy):
    def __init__(
        self,
        *,
        board_allowed: bool = True,
        spec_allowed: bool = True,
    ) -> None:
        self.board_allowed = board_allowed
        self.spec_allowed = spec_allowed
        self.calls: list[tuple[str, str]] = []

    async def can_read_board(self, db, identity, board_id: str) -> bool:
        self.calls.append(("board", board_id))
        return self.board_allowed

    async def can_read_spec(self, db, identity, spec: Spec) -> bool:
        self.calls.append(("spec", spec.id))
        return self.spec_allowed


class RecordingMetricSink:
    def __init__(self) -> None:
        self.events: list[SelectorMetricEvent] = []

    def emit(self, event: SelectorMetricEvent) -> None:
        self.events.append(event)


def _assert_selector_metric_labels_are_safe(events: list[SelectorMetricEvent]) -> None:
    forbidden_keys = {
        "q",
        "query",
        "spec_id",
        "child_id",
        "child_ref",
        "payload",
        "description",
        "context",
        "request_body",
    }
    forbidden_fragments = {
        "secret query",
        "spec-secret",
        "spec:spec-secret:decision:dec-secret",
        "full rule body",
        "must not leak",
    }
    for event in events:
        assert not (set(event.labels) & forbidden_keys), event
        rendered = repr(event.labels).casefold()
        for fragment in forbidden_fragments:
            assert fragment not in rendered, event


class MemorySelectorCatalog(DiscoverySelectorCatalog):
    def __init__(
        self,
        specs: list[Spec],
        *,
        cards: list[Card] | None = None,
        access_policy: RecordingAccessPolicy | None = None,
        cache: DiscoverySelectorCache | None = None,
    ) -> None:
        super().__init__(access_policy or RecordingAccessPolicy(), cache=cache)
        self._specs = specs
        self._cards = cards or []
        self.load_spec_calls = 0

    async def _load_specs(self, db, *, board_id: str, status: str | None) -> list[Spec]:
        return [
            spec
            for spec in self._specs
            if spec.board_id == board_id and status in (None, "", "active", "all")
        ]

    async def _load_spec(self, db, *, board_id: str, spec_id: str) -> Spec | None:
        self.load_spec_calls += 1
        return next(
            (spec for spec in self._specs if spec.board_id == board_id and spec.id == spec_id),
            None,
        )

    async def _load_cards(self, db, *, board_id: str, status: str | None) -> list[Card]:
        return [
            card
            for card in self._cards
            if card.board_id == board_id
            and not card.archived
            and (
                status in (None, "", "active", "all")
                or getattr(card.status, "value", card.status) == status
            )
        ]


def _spec(**overrides) -> Spec:
    defaults = {
        "id": "spec-1",
        "board_id": "board-1",
        "title": "Discovery Selectors Spec",
        "description": "unsafe description should never appear in selector options",
        "context": "unsafe full context should never appear in selector options",
        "status": SpecStatus.APPROVED,
        "version": 7,
        "created_by": "tester",
        "functional_requirements": [
            "FR payload with enough words to prove it is compacted as subtitle metadata"
        ],
        "technical_requirements": [
            {"id": "tr-1", "title": "Selector catalog reuses spec refs", "description": "full TR"}
        ],
        "acceptance_criteria": [{"id": "ac-1", "title": "Exact child selected"}],
        "test_scenarios": [],
        "business_rules": [
            {
                "id": "br-1",
                "title": "Safe metadata projection",
                "rule": "full rule body stays outside option payload fields",
                "linked_requirements": ["FR1"],
            }
        ],
        "api_contracts": [
            {
                "id": "api-1",
                "method": "GET",
                "path": "/api/v1/discovery/selector-options",
                "description": "full API description",
                "request_body": {"secret": "must not leak"},
            }
        ],
        "integration_requirements": [
            {
                "id": "ir-1",
                "title": "REST uses catalog",
                "provider": "DiscoverySelectorCatalog",
                "consumer": "Discovery API",
            }
        ],
        "observability_requirements": [
            {
                "id": "or-1",
                "title": "Unsafe projection metric",
                "metric_name": "discovery_selector_unsafe_projection_total",
                "threshold": "0",
            }
        ],
        "decisions": [
            {
                "id": "dec-active",
                "title": "Use metadata-only selectors",
                "rationale": "prevents KG/global discovery leakage",
                "status": "active",
            },
            {
                "id": "dec-old",
                "title": "Legacy selector source",
                "rationale": "superseded rationale",
                "status": "superseded",
            },
        ],
        "labels": [],
    }
    defaults.update(overrides)
    return Spec(**defaults)


def _card(**overrides) -> Card:
    defaults = {
        "id": "card-1",
        "board_id": "board-1",
        "spec_id": "spec-1",
        "sprint_id": "sprint-1",
        "title": "Implement selector-backed card dependencies",
        "description": "unsafe card description must never appear in selector options",
        "details": "unsafe card details must never appear in selector options",
        "status": CardStatus.IN_PROGRESS,
        "priority": CardPriority.HIGH,
        "position": 3,
        "created_by": "tester",
        "archived": False,
    }
    defaults.update(overrides)
    return Card(**defaults)


@pytest.mark.asyncio
async def test_lists_spec_options_after_board_and_spec_authorization():
    policy = RecordingAccessPolicy()
    catalog = MemorySelectorCatalog([_spec()], access_policy=policy)

    result = await catalog.list_options(
        None,
        board_id="board-1",
        selector_kind="spec",
        identity={"user_id": "u-1"},
    )

    payload = result.to_dict()
    assert policy.calls == [("board", "board-1"), ("spec", "spec-1")]
    assert payload["source"] == "board_db_spec_json"
    assert payload["global_refs_used"] is False
    option = payload["options"][0]
    assert set(option) <= SAFE_SELECTOR_OPTION_FIELDS
    assert option["id"] == "spec-1"
    assert option["label"] == "Discovery Selectors Spec"
    assert "description" not in option
    assert "context" not in option


@pytest.mark.asyncio
async def test_lists_all_eight_spec_child_types_from_spec_references():
    catalog = MemorySelectorCatalog([_spec()])

    seen_child_types = set()
    for child_type in SUPPORTED_SPEC_CHILD_TYPES:
        result = await catalog.list_options(
            None,
            board_id="board-1",
            selector_kind="spec_child",
            spec_id="spec-1",
            child_type=child_type,
        )
        payload = result.to_dict()
        assert payload["source"] == "board_db_spec_json"
        assert len(payload["options"]) == 1
        option = payload["options"][0]
        assert set(option) <= SAFE_SELECTOR_OPTION_FIELDS
        assert option["entity_type"] == "spec_child"
        assert option["child_type"] == child_type
        assert option["child_ref"].startswith(f"spec:spec-1:{child_type}:")
        assert "request_body" not in option
        assert "description" not in option
        assert "context" not in option
        seen_child_types.add(child_type)

    assert seen_child_types == set(SUPPORTED_SPEC_CHILD_TYPES)


@pytest.mark.asyncio
async def test_lists_card_selector_options_with_metadata_only():
    policy = RecordingAccessPolicy()
    catalog = MemorySelectorCatalog([_spec()], cards=[_card()], access_policy=policy)

    result = await catalog.list_options(
        None,
        board_id="board-1",
        selector_kind="card",
        identity={"user_id": "u-1"},
        q="dependencies",
    )

    payload = result.to_dict()
    assert policy.calls == [("board", "board-1")]
    assert len(payload["options"]) == 1
    option = payload["options"][0]
    assert set(option) <= SAFE_SELECTOR_OPTION_FIELDS
    assert option["id"] == "card-1"
    assert option["entity_type"] == "card"
    assert option["label"] == "Implement selector-backed card dependencies"
    assert option["status"] == "in_progress"
    assert option["refs"]["card_id"] == "card-1"
    assert "description" not in option
    assert "details" not in option


@pytest.mark.asyncio
async def test_decisions_default_to_active_and_include_superseded_is_explicit():
    catalog = MemorySelectorCatalog([_spec()])

    active_result = await catalog.list_options(
        None,
        board_id="board-1",
        selector_kind="spec_child",
        spec_id="spec-1",
        child_type="decision",
    )
    assert [item["child_id"] for item in active_result.to_dict()["options"]] == [
        "dec-active"
    ]

    all_result = await catalog.list_options(
        None,
        board_id="board-1",
        selector_kind="spec_child",
        spec_id="spec-1",
        child_type="decision",
        status="all",
        include_superseded=True,
    )
    assert [item["child_id"] for item in all_result.to_dict()["options"]] == [
        "dec-active",
        "dec-old",
    ]
    assert all_result.to_dict()["options"][1]["status"] == "superseded"


@pytest.mark.asyncio
async def test_access_policy_denies_before_spec_option_projection():
    policy = RecordingAccessPolicy(board_allowed=False)
    catalog = MemorySelectorCatalog([_spec()], access_policy=policy)

    with pytest.raises(DiscoverySelectorAccessDenied):
        await catalog.list_options(
            None,
            board_id="board-1",
            selector_kind="spec",
            identity={"user_id": "u-1"},
        )

    assert policy.calls == [("board", "board-1")]


@pytest.mark.asyncio
async def test_access_policy_denies_before_spec_child_projection():
    policy = RecordingAccessPolicy(spec_allowed=False)
    catalog = MemorySelectorCatalog([_spec()], access_policy=policy)

    with pytest.raises(DiscoverySelectorAccessDenied):
        await catalog.list_options(
            None,
            board_id="board-1",
            selector_kind="spec_child",
            spec_id="spec-1",
            child_type="business_rule",
        )

    assert policy.calls == [("spec", "spec-1")]


@pytest.mark.asyncio
async def test_rejects_invalid_selector_dependencies_and_unknown_child_types():
    catalog = MemorySelectorCatalog([_spec()])

    with pytest.raises(DiscoverySelectorInvalidRequest):
        await catalog.list_options(
            None,
            board_id="board-1",
            selector_kind="spec",
            spec_id="spec-1",
        )

    with pytest.raises(DiscoverySelectorInvalidRequest):
        await catalog.list_options(
            None,
            board_id="board-1",
            selector_kind="spec_child",
            spec_id="spec-1",
            child_type="test_scenario",
        )

    with pytest.raises(DiscoverySelectorInvalidRequest):
        await catalog.list_options(
            None,
            board_id="board-1",
            selector_kind="spec_child",
            child_type="decision",
        )


@pytest.mark.asyncio
async def test_missing_spec_raises_not_found():
    catalog = MemorySelectorCatalog([_spec(id="spec-2")])

    with pytest.raises(DiscoverySelectorSpecNotFound):
        await catalog.list_options(
            None,
            board_id="board-1",
            selector_kind="spec_child",
            spec_id="missing",
            child_type="decision",
        )


@pytest.mark.asyncio
async def test_query_limit_and_offset_are_applied_after_safe_projection():
    spec = _spec(
        business_rules=[
            {"id": "br-1", "title": "Alpha rule"},
            {"id": "br-2", "title": "Beta rule"},
            {"id": "br-3", "title": "Alpha follow-up"},
        ]
    )
    catalog = MemorySelectorCatalog([spec])

    result = await catalog.list_options(
        None,
        board_id="board-1",
        selector_kind="spec_child",
        spec_id="spec-1",
        child_type="business_rule",
        q="alpha",
        limit=1,
        offset=1,
    )

    assert [option["child_id"] for option in result.to_dict()["options"]] == ["br-3"]


def test_validate_safe_selector_payload_rejects_non_allowlisted_fields():
    with pytest.raises(DiscoverySelectorUnsafeProjection):
        validate_safe_selector_payload({"id": "x", "label": "X", "description": "leak"})


def test_structured_snapshot_excludes_artifacts_and_global_discovery_payloads():
    spec = SimpleNamespace(
        id="spec-1",
        title="Spec SoR",
        functional_requirements=["FR from spec JSON"],
        business_rules=[{"id": "br-1", "title": "BR from spec JSON"}],
        technical_requirements=[{"id": "tr-1", "title": "TR from spec JSON"}],
        decisions=[{"id": "dec-1", "title": "Decision from spec JSON"}],
        acceptance_criteria=[{"id": "ac-1", "title": "AC from spec JSON"}],
        api_contracts=[{"id": "api-1", "path": "/safe"}],
        integration_requirements=[{"id": "ir-1", "title": "IR from spec JSON"}],
        observability_requirements=[{"id": "or-1", "title": "OR from spec JSON"}],
        knowledge_bases=[
            {
                "title": "Unsafe KE",
                "content": "full knowledge base body must not feed selectors",
            }
        ],
        screen_mockups=[
            {
                "title": "Unsafe mockup",
                "html_content": "<script>must-not-leak</script>",
            }
        ],
        architecture_designs=[
            {"title": "Unsafe architecture", "adapter_payload": {"raw": "must-not-leak"}}
        ],
        global_discovery_payload={
            "structured_children": [
                {"child_type": "decision", "title": "must not become selector SoR"}
            ]
        },
    )

    snapshot = structured_spec_snapshot(spec)

    assert hasattr(snapshot, "functional_requirements")
    assert hasattr(snapshot, "decisions")
    assert not hasattr(snapshot, "knowledge_bases")
    assert not hasattr(snapshot, "screen_mockups")
    assert not hasattr(snapshot, "architecture_designs")
    assert not hasattr(snapshot, "global_discovery_payload")


def test_selector_cache_key_normalization_is_deterministic():
    key_a = normalize_selector_cache_key(
        board_id="board-1",
        selector_kind="spec_child",
        spec_id="spec-1",
        child_type="decision",
        status=None,
        q="  Foo   BAR  ",
        limit=500,
        offset=-3,
        include_superseded=True,
    )
    key_b = normalize_selector_cache_key(
        board_id="board-1",
        selector_kind="spec_child",
        spec_id="spec-1",
        child_type="decision",
        status="active",
        q="foo bar",
        limit=100,
        offset=0,
        include_superseded=True,
    )

    assert key_a == key_b
    assert key_a.q == "foo bar"
    assert key_a.limit == 100
    assert key_a.offset == 0


def test_selector_cache_hit_miss_ttl_and_safe_metrics():
    now = {"value": 100.0}
    sink = RecordingMetricSink()
    cache = DiscoverySelectorCache(
        ttl_seconds=999,
        now_fn=lambda: now["value"],
        metrics_sink=sink,
    )
    key = normalize_selector_cache_key(
        board_id="board-1",
        selector_kind="spec_child",
        spec_id="spec-secret",
        child_type="decision",
        q=" Secret   Query ",
    )

    cache.set(key, SelectorOptionsResult(options=[]))
    assert cache.ttl_seconds == SELECTOR_CACHE_MAX_TTL_SECONDS
    assert cache.get(key).cache_status == "hit"

    now["value"] += SELECTOR_CACHE_MAX_TTL_SECONDS + 1
    assert cache.get(key) is None
    assert [event.metric_name for event in sink.events].count(
        "discovery_selector_cache_total"
    ) >= 3
    _assert_selector_metric_labels_are_safe(sink.events)


def test_selector_cache_invalidation_scopes_and_kg_rebuilt_boardwide():
    sink = RecordingMetricSink()
    cache = DiscoverySelectorCache(metrics_sink=sink)
    spec_a = normalize_selector_cache_key(
        board_id="board-1",
        selector_kind="spec_child",
        spec_id="spec-a",
        child_type="decision",
    )
    spec_b = normalize_selector_cache_key(
        board_id="board-1",
        selector_kind="spec_child",
        spec_id="spec-b",
        child_type="decision",
    )
    other_board = normalize_selector_cache_key(board_id="board-2", selector_kind="spec")

    for key in (spec_a, spec_b, other_board):
        cache.set(key, SelectorOptionsResult(options=[]))

    result = cache.invalidate_event(
        {
            "event_type": SELECTOR_EVENT_SPEC_UPDATED,
            "board_id": "board-1",
            "spec_id": "spec-a",
        }
    )
    assert result.invalidated_count == 1
    assert cache.get(spec_a) is None
    assert cache.get(spec_b) is not None

    result = cache.invalidate_event(
        {"event_type": SELECTOR_EVENT_KG_REBUILT, "board_id": "board-1"}
    )
    assert result.invalidated_count == 1
    assert cache.get(spec_b) is None
    assert cache.get(other_board) is not None
    assert any(
        event.metric_name == "discovery_selector_cache_invalidation_total"
        for event in sink.events
    )
    _assert_selector_metric_labels_are_safe(sink.events)


@pytest.mark.asyncio
async def test_catalog_uses_cache_after_authorization_without_bypassing_rbac():
    cache = DiscoverySelectorCache(metrics_sink=RecordingMetricSink())
    policy = RecordingAccessPolicy()
    catalog = MemorySelectorCatalog([_spec()], access_policy=policy, cache=cache)

    first = await catalog.list_options(
        None,
        board_id="board-1",
        selector_kind="spec_child",
        spec_id="spec-1",
        child_type="decision",
    )
    second = await catalog.list_options(
        None,
        board_id="board-1",
        selector_kind="spec_child",
        spec_id="spec-1",
        child_type="decision",
    )

    assert first.cache_status == "miss"
    assert second.cache_status == "hit"
    assert policy.calls == [("spec", "spec-1"), ("spec", "spec-1")]


@pytest.mark.asyncio
async def test_spec_domain_event_handler_invalidates_default_selector_cache():
    cache = get_default_discovery_selector_cache()
    cache.clear()
    key = normalize_selector_cache_key(
        board_id="board-1",
        selector_kind="spec_child",
        spec_id="spec-1",
        child_type="decision",
    )
    cache.set(key, SelectorOptionsResult(options=[]))

    handler = DiscoverySelectorCacheInvalidationHandler()
    await handler.handle(
        SpecVersionBumped(
            board_id="board-1",
            actor_id="agent-1",
            spec_id="spec-1",
            old_version=1,
            new_version=2,
            changed_fields=["decisions"],
        ),
        session=None,
    )

    assert cache.get(key) is None
    cache.clear()
