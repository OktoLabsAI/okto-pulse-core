"""KG-02.1 — RebuildPreflightService + /kg/rebuild/preflight endpoint.

Covers FR2 (preflight enumerates without mutation), TR13 (read-only),
TR17 (status composition), OR or_4494881d (counter labels).
"""

from __future__ import annotations

import pytest

from okto_pulse.core.kg.rebuild_preflight import (
    PreflightBlockReason,
    PreflightOutcome,
    RebuildHealthSummary,
    RebuildPreflightResult,
    RebuildPreflightService,
    RebuildSourceSummary,
    get_preflight_count,
    get_preflight_counter_labels,
    get_preflight_samples,
    reset_preflight_counter,
)


@pytest.fixture(autouse=True)
def _reset_counter():
    reset_preflight_counter()
    yield
    reset_preflight_counter()


def _health(state: str = "healthy", generation: str | None = None) -> RebuildHealthSummary:
    return RebuildHealthSummary(
        base_state=state,
        metric_status="available",
        current_kg_generation_id=generation,
    )


def _source(
    eligible: int = 10,
    skipped: int = 0,
    non_det: bool = False,
) -> RebuildSourceSummary:
    return RebuildSourceSummary(
        eligible_count=eligible,
        skipped_cancelled_count=skipped,
        has_non_deterministic_inputs=non_det,
    )


def _service(health=None, source=None, status_probe=None) -> RebuildPreflightService:
    return RebuildPreflightService(
        source_probe=lambda _b: source if source else _source(),
        health_probe=lambda _b: health if health else _health(),
        rebuild_status_probe=status_probe or (lambda _b: ("idle", "")),
    )


# --- Happy paths --------------------------------------------------------------


def test_healthy_board_returns_ready_not_required():
    svc = _service()
    result = svc.run(board_id="b1")
    assert isinstance(result, RebuildPreflightResult)
    assert result.outcome == PreflightOutcome.READY.value
    assert result.action_required == "not_required"
    assert result.reason is None
    assert result.base_state == "healthy"
    assert result.eligible_source_count == 10
    assert result.preflight_hash  # deterministic
    assert result.generated_at
    assert result.rebuild_status == "idle"


def test_at_risk_returns_ready_recommended():
    svc = _service(health=_health(state="at_risk"))
    result = svc.run(board_id="b1")
    assert result.outcome == PreflightOutcome.READY.value
    assert result.action_required == "recommended"


# --- Confirmation-required paths ---------------------------------------------


def test_recovery_needed_requires_confirmation():
    svc = _service(health=_health(state="recovery_needed"))
    result = svc.run(board_id="b1")
    assert result.outcome == PreflightOutcome.CONFIRMATION_REQUIRED.value
    assert result.action_required == "required"
    assert result.reason == PreflightBlockReason.HEALTH_RECOVERY_NEEDED.value


def test_backpressure_requires_confirmation():
    svc = _service(health=_health(state="backpressure"))
    result = svc.run(board_id="b1")
    assert result.outcome == PreflightOutcome.CONFIRMATION_REQUIRED.value
    assert result.action_required == "recommended"
    assert result.reason == PreflightBlockReason.HEALTH_BACKPRESSURE_WITHOUT_ADMIN.value


def test_non_deterministic_sources_require_confirmation():
    svc = _service(source=_source(non_det=True))
    result = svc.run(board_id="b1")
    assert result.outcome == PreflightOutcome.CONFIRMATION_REQUIRED.value
    assert result.reason == PreflightBlockReason.NON_DETERMINISTIC_SOURCES.value


# --- Blocked paths ------------------------------------------------------------


def test_quarantined_is_blocked():
    svc = _service(health=_health(state="quarantined"))
    result = svc.run(board_id="b1")
    assert result.outcome == PreflightOutcome.BLOCKED.value
    assert result.reason == PreflightBlockReason.HEALTH_QUARANTINED.value


def test_source_store_failure_is_blocked():
    def boom(_b):
        raise RuntimeError("source store down")

    svc = RebuildPreflightService(
        source_probe=boom,
        health_probe=lambda _b: _health(),
    )
    result = svc.run(board_id="b1")
    assert result.outcome == PreflightOutcome.BLOCKED.value
    assert result.reason == PreflightBlockReason.SOURCE_STORE_UNAVAILABLE.value
    # Defaults applied so the response remains coherent.
    assert result.eligible_source_count == 0


# --- TR13 read-only invariant -------------------------------------------------


def test_preflight_never_calls_mutation_apis():
    """Static-source proof: rebuild_preflight.py MUST NOT reference any
    primitive that mutates KG storage."""
    import inspect
    import okto_pulse.core.kg.rebuild_preflight as module

    src = inspect.getsource(module)
    # Forbidden mutation calls.
    for forbidden in (
        "KGSingleWriterLock.acquire",
        "KGQuarantineService(",
        "purge_board_graph_storage",
        "purge_global_discovery_storage",
        "shutil.move",
        "os.unlink",
    ):
        assert forbidden not in src, (
            f"preflight must be read-only; found forbidden: {forbidden}"
        )


# --- TR15-aligned hash determinism --------------------------------------------


def test_preflight_hash_is_deterministic_across_runs():
    svc = _service(
        health=_health(state="at_risk", generation="gen-fixed"),
        source=_source(eligible=42, skipped=3),
    )
    a = svc.run(board_id="b1")
    b = svc.run(board_id="b1")
    assert a.preflight_hash == b.preflight_hash


def test_preflight_hash_changes_when_input_changes():
    svc_a = _service(source=_source(eligible=10))
    svc_b = _service(source=_source(eligible=11))
    a = svc_a.run(board_id="b1")
    b = svc_b.run(board_id="b1")
    assert a.preflight_hash != b.preflight_hash


# --- OR or_4494881d counter labels -------------------------------------------


def test_counter_labels_match_required_or_shape():
    assert get_preflight_counter_labels() == ("board_id", "outcome", "reason")


def test_counter_bumps_per_outcome():
    svc_ready = _service()
    svc_ready.run(board_id="b1")
    svc_block = _service(health=_health(state="quarantined"))
    svc_block.run(board_id="b1")
    svc_confirm = _service(health=_health(state="recovery_needed"))
    svc_confirm.run(board_id="b1")

    samples = get_preflight_samples()
    outcomes = {s["outcome"] for s in samples}
    assert PreflightOutcome.READY.value in outcomes
    assert PreflightOutcome.BLOCKED.value in outcomes
    assert PreflightOutcome.CONFIRMATION_REQUIRED.value in outcomes

    for s in samples:
        for label in get_preflight_counter_labels():
            assert label in s
            assert isinstance(s[label], str) and s[label]
        assert isinstance(s["count"], int) and s["count"] >= 1


# --- TR17 status composition --------------------------------------------------


def test_rebuild_status_is_composed_from_probe():
    svc = _service(status_probe=lambda _b: ("in_progress", "rebuild_running"))
    result = svc.run(board_id="b1")
    assert result.rebuild_status == "in_progress"
    assert result.operational_substatus == "rebuild_running"


def test_rebuild_status_default_is_idle():
    svc = RebuildPreflightService(
        source_probe=lambda _b: _source(),
        health_probe=lambda _b: _health(),
    )
    result = svc.run(board_id="b1")
    assert result.rebuild_status == "idle"


# --- Input validation --------------------------------------------------------


def test_empty_board_id_raises():
    svc = _service()
    with pytest.raises(ValueError):
        svc.run(board_id="")


# --- val_7a768ee2 rework: route registered + TestClient integration ---------


def test_kg_rebuild_router_is_registered_in_api_router():
    """val_7a768ee2: the new router MUST be wired into api_router so
    the endpoint is actually reachable via the live FastAPI app."""
    from okto_pulse.core.api.router import api_router

    paths = {route.path for route in api_router.routes}
    assert "/api/v1/kg/rebuild/preflight" in paths, (
        f"preflight route not registered; have: {sorted(paths)}"
    )


def test_post_preflight_endpoint_returns_safe_payload_via_test_client():
    """ASGI integration: POST /api/v1/kg/rebuild/preflight returns the
    contract-shaped payload via the live router with auth override."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from okto_pulse.core.api.router import api_router
    from okto_pulse.core.infra.auth import require_user

    app = FastAPI()
    app.include_router(api_router)

    async def _fake_user():
        return "user-rebuild-test"

    app.dependency_overrides[require_user] = _fake_user

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/kg/rebuild/preflight",
            params={"board_id": "b-test"},
        )
    assert response.status_code == 200, response.text
    body = response.json()
    # Contract-shaped response fields all present.
    for field in (
        "board_id",
        "outcome",
        "action_required",
        "base_state",
        "metric_status",
        "eligible_source_count",
        "skipped_cancelled_count",
        "has_non_deterministic_inputs",
        "preflight_hash",
        "generated_at",
        "rebuild_status",
    ):
        assert field in body, f"missing field {field}"
    # Stub probes return healthy + 0 sources for KG-02.1 placeholder.
    assert body["board_id"] == "b-test"
    assert body["base_state"] == "healthy"
    assert body["outcome"] in {"ready", "confirmation_required", "blocked"}
    assert len(body["preflight_hash"]) == 64  # sha256 hex
    # val_d0da4a75 rework: preflight now ALSO returns manifest_ref +
    # source_set_hash so the operator can pass them to /confirm.
    assert body["manifest_ref"].startswith("rebuild_manifest_")
    assert len(body["source_set_hash"]) == 64


def test_post_preflight_missing_board_id_returns_400():
    """Empty/missing board_id is rejected with HTTP 400."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from okto_pulse.core.api.router import api_router
    from okto_pulse.core.infra.auth import require_user

    app = FastAPI()
    app.include_router(api_router)

    async def _fake_user():
        return "user-test"

    app.dependency_overrides[require_user] = _fake_user

    with TestClient(app) as client:
        response = client.post("/api/v1/kg/rebuild/preflight")
    # Missing required query param → FastAPI emits 422.
    assert response.status_code in (400, 422)
