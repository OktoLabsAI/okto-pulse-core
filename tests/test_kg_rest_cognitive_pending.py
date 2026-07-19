"""Tests for KG-03.4 — REST GET /api/v1/kg/cognitive-pending.

Aligns with api_cce40fa6 contract:

Request:
    board_id (required), kg_generation_id?, status?, limit (1..100, default 50)

Response (success):
    board_id, selected_kg_generation_id, readonly (const true),
    legacy_mode, counts {pending,in_progress,consolidated,skipped,failed,total},
    items[] with safe-projection fields only

Errors:
    invalid_status (400), generation_not_found (404),
    cognitive_pending_unavailable (503)

Invariants tested:
    * ir_0b66c7af / br_2065f80b — endpoint is read-only; NO POST/PUT/PATCH/DELETE
      under /kg/cognitive-pending exists in the live router.
    * br_858a0859 / FR9 / AC10 — no raw artifact body or sensitive fields
      land in the REST response.
    * tr_08d89b34 — counter labels remain bounded.
    * AC11 — KG-01/KG-02 regression: rebuild endpoints still reachable.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api.router import api_router
from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitiveItemListOutcome,
    CognitiveItemListReasonCode,
    CognitiveItemListSurface,
    CognitiveItemStatus,
    CognitivePendingMarker,
    get_list_counter_labels,
    get_list_event_count,
    get_list_samples,
    require_rebuild_audit_artifact_store,
    reset_list_counter,
)
from okto_pulse.core.kg.rebuild_generation import generate_kg_generation_id


BOARD = "board-kg03-4"


@pytest.fixture
def isolated_base_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    target = tmp_path / "kg-03-4"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(target))
    return target


@pytest.fixture(autouse=True)
def _reset_counters() -> None:
    reset_list_counter()
    yield
    reset_list_counter()


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(api_router)

    async def _fake_user() -> str:
        return "user-kg03-4-test"

    async def _fake_uow():
        async def _get_board(board_id: str):
            return SimpleNamespace(
                id=board_id,
                owner_id="user-kg03-4-test",
            )

        yield SimpleNamespace(
            boards=SimpleNamespace(get=_get_board),
        )

    app.dependency_overrides[require_user] = _fake_user
    app.dependency_overrides[get_unit_of_work] = _fake_uow
    return TestClient(app)


def _row(artifact_type: str, id_: str) -> dict:
    return {
        "artifact_type": artifact_type,
        "id": id_,
        "source_ref": f"{artifact_type}:{id_}",
    }


def _seed(base_dir: Path, sources: list[dict]) -> str:
    del base_dir
    gen = generate_kg_generation_id()
    artifact_store = require_rebuild_audit_artifact_store()
    marker = CognitivePendingMarker(artifact_store=artifact_store)
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=sources,
        event_ref="evt_kg03_4",
    )
    return gen


def _openapi_paths() -> dict[str, dict]:
    app = FastAPI()
    app.include_router(api_router)
    return app.openapi()["paths"]


# -------- Router registration -------------------------------------------


def test_cognitive_pending_route_is_registered_in_api_router() -> None:
    """The new GET endpoint must be wired into the live api_router so
    UI requests actually reach it."""

    paths = set(_openapi_paths())
    assert "/api/v1/kg/cognitive-pending" in paths, (
        f"cognitive-pending route not registered; have: "
        f"{sorted(p for p in paths if 'cognitive' in p or '/kg/' in p)}"
    )


def test_endpoint_is_read_only_no_mutating_methods_registered() -> None:
    """ir_0b66c7af + br_2065f80b: ONLY GET is exposed at this path;
    POST/PUT/PATCH/DELETE must NOT be registered."""

    path_contract = _openapi_paths()["/api/v1/kg/cognitive-pending"]
    methods_for_path = {method.upper() for method in path_contract}
    assert "GET" in methods_for_path
    forbidden = {"POST", "PUT", "PATCH", "DELETE"}
    assert not forbidden.intersection(methods_for_path), (
        f"mutating methods leaked into read-only endpoint: "
        f"{forbidden.intersection(methods_for_path)}"
    )


# -------- AC11 — KG-02 routes still reachable ---------------------------


def test_kg02_rebuild_routes_still_registered() -> None:
    """AC11 regression: adding the new router must not unwire existing
    KG-01/KG-02 endpoints."""

    paths = set(_openapi_paths())
    for required in (
        "/api/v1/kg/rebuild/preflight",
        "/api/v1/kg/rebuild/confirm",
        "/api/v1/kg/rebuild/run",
    ):
        assert required in paths, f"{required} unwired"


# -------- Response shape: api_cce40fa6 ----------------------------------


def test_response_has_all_required_contract_keys(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    gen = _seed(isolated_base_dir, [
        _row("spec", "s1"),
        _row("refinement", "r1"),
    ])
    response = client.get(
        "/api/v1/kg/cognitive-pending",
        params={"board_id": BOARD, "kg_generation_id": gen},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    required = {
        "board_id",
        "selected_kg_generation_id",
        "readonly",
        "legacy_mode",
        "counts",
        "items",
    }
    assert required.issubset(body.keys())
    # readonly is const true.
    assert body["readonly"] is True


def test_counts_have_all_status_buckets_plus_total(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    gen = _seed(isolated_base_dir, [
        _row("spec", "s1"),
        _row("spec", "s2"),
        _row("refinement", "r1"),
    ])
    response = client.get(
        "/api/v1/kg/cognitive-pending",
        params={"board_id": BOARD, "kg_generation_id": gen},
    )
    body = response.json()
    assert set(body["counts"].keys()) == {
        "pending", "in_progress", "consolidated", "skipped", "failed", "total"
    }
    assert body["counts"]["pending"] == 3
    assert body["counts"]["total"] == 3


def test_items_use_safe_projection_with_reason_code(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    """AC10 + br_858a0859: response items expose reason_code, never raw
    reason text or storage-only fields."""

    gen = _seed(isolated_base_dir, [_row("spec", "s1")])
    response = client.get(
        "/api/v1/kg/cognitive-pending",
        params={"board_id": BOARD, "kg_generation_id": gen},
    )
    body = response.json()
    item = body["items"][0]
    # Contract-required fields present.
    for required in ("item_id", "source_ref", "artifact_type", "status", "recorded_at"):
        assert required in item, f"missing required item field {required}"
    # Contract-optional projection fields present.
    assert "reason_code" in item
    # Storage-only fields NOT echoed.
    assert "reason" not in item
    assert "board_id" not in item
    assert "kg_generation_id" not in item
    assert "event_ref" not in item


def test_safe_payload_no_raw_artifact_body_or_sensitive_fields(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    """AC10: even if the source rows are populated with title/description/
    secret_token, the REST response carries none of them."""

    rich = {
        "artifact_type": "spec",
        "id": "s1",
        "source_ref": "spec:s1",
        "title": "Sensitive REST Title",
        "description": "leaky description in REST",
        "secret_token": "leak-bait-rest",
    }
    gen = _seed(isolated_base_dir, [rich])
    response = client.get(
        "/api/v1/kg/cognitive-pending",
        params={"board_id": BOARD, "kg_generation_id": gen},
    )
    raw = response.text
    assert "Sensitive REST Title" not in raw
    assert "leaky description in REST" not in raw
    assert "secret_token" not in raw
    assert "leak-bait-rest" not in raw


# -------- legacy_mode wiring --------------------------------------------


def test_legacy_mode_true_for_aggregate_only_record(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    del isolated_base_dir
    gen = generate_kg_generation_id()
    legacy = {
        "board_id": BOARD,
        "kg_generation_id": gen,
        "event_ref": "evt_legacy",
        "pending_count": 1,
        "pending_refs": ["spec:l1"],
        "status": "pending_marked",
        "recorded_at": "2026-05-25T00:00:00+00:00",
    }
    store = CognitiveConsolidationItemStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    store.artifact_store.write_json_atomic(store._record_key(BOARD, gen), legacy)

    body = client.get(
        "/api/v1/kg/cognitive-pending",
        params={"board_id": BOARD, "kg_generation_id": gen},
    ).json()
    assert body["legacy_mode"] is True
    assert body["counts"]["total"] == 1


def test_legacy_mode_false_for_kg03_record(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    gen = _seed(isolated_base_dir, [_row("spec", "s1")])
    body = client.get(
        "/api/v1/kg/cognitive-pending",
        params={"board_id": BOARD, "kg_generation_id": gen},
    ).json()
    assert body["legacy_mode"] is False


# -------- Status filter (bounded enum) ----------------------------------


@pytest.mark.parametrize(
    "bad_status",
    ["completed", "PENDING", "any", "in-progress"],
)
def test_invalid_status_returns_400(
    isolated_base_dir: Path,
    client: TestClient,
    bad_status: str,
) -> None:
    response = client.get(
        "/api/v1/kg/cognitive-pending",
        params={"board_id": BOARD, "status": bad_status},
    )
    assert response.status_code == 400, response.text
    detail = response.json()["detail"]
    assert detail["code"] == "invalid_status"
    assert (
        detail["reason_code"]
        == CognitiveItemListReasonCode.INVALID_STATUS.value
    )


def test_status_filter_applies_to_response(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    gen = _seed(isolated_base_dir, [
        _row("spec", "s1"),
        _row("spec", "s2"),
        _row("refinement", "r1"),
    ])
    store = CognitiveConsolidationItemStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    items = store.list_items(BOARD, gen)
    store.update_item(
        board_id=BOARD,
        kg_generation_id=gen,
        item_id=items[0].item_id,
        new_status=CognitiveItemStatus.CONSOLIDATED.value,
        updated_by_agent_id="agent-1",
        consolidation_session_id="sess-1",
    )

    pending = client.get(
        "/api/v1/kg/cognitive-pending",
        params={"board_id": BOARD, "kg_generation_id": gen, "status": "pending"},
    ).json()
    consolidated = client.get(
        "/api/v1/kg/cognitive-pending",
        params={"board_id": BOARD, "kg_generation_id": gen, "status": "consolidated"},
    ).json()
    assert pending["counts"]["pending"] == 2
    assert consolidated["counts"]["consolidated"] == 1


# -------- Limit (FastAPI Query validation) ------------------------------


def test_limit_validates_within_1_to_100_range(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    response = client.get(
        "/api/v1/kg/cognitive-pending",
        params={"board_id": BOARD, "limit": 0},
    )
    assert response.status_code == 422

    response = client.get(
        "/api/v1/kg/cognitive-pending",
        params={"board_id": BOARD, "limit": 101},
    )
    assert response.status_code == 422


def test_limit_defaults_to_50_and_caps_returned_items(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    gen = _seed(isolated_base_dir, [
        _row("spec", f"s{i}") for i in range(60)
    ])
    body = client.get(
        "/api/v1/kg/cognitive-pending",
        params={"board_id": BOARD, "kg_generation_id": gen},
    ).json()
    # Default limit=50 — only 50 of the 60 items returned.
    assert len(body["items"]) == 50
    # Counts are generation-level, not page-level, so the health summary
    # can request a small page without under-reporting pending state.
    assert body["counts"]["pending"] == 60
    assert body["counts"]["total"] == 60

    body_with_limit = client.get(
        "/api/v1/kg/cognitive-pending",
        params={"board_id": BOARD, "kg_generation_id": gen, "limit": 10},
    ).json()
    assert len(body_with_limit["items"]) == 10
    assert body_with_limit["counts"]["pending"] == 60
    assert body_with_limit["counts"]["total"] == 60


def test_offset_paginates_items_without_changing_generation_counts(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    gen = _seed(isolated_base_dir, [
        _row("spec", f"s{i}") for i in range(6)
    ])

    page_1 = client.get(
        "/api/v1/kg/cognitive-pending",
        params={
            "board_id": BOARD,
            "kg_generation_id": gen,
            "limit": 2,
            "offset": 0,
        },
    ).json()
    page_2 = client.get(
        "/api/v1/kg/cognitive-pending",
        params={
            "board_id": BOARD,
            "kg_generation_id": gen,
            "limit": 2,
            "offset": 2,
        },
    ).json()

    assert [item["source_ref"] for item in page_1["items"]] == [
        "spec:s0",
        "spec:s1",
    ]
    assert [item["source_ref"] for item in page_2["items"]] == [
        "spec:s2",
        "spec:s3",
    ]
    assert page_1["counts"]["total"] == 6
    assert page_2["counts"]["total"] == 6


def test_offset_validates_as_non_negative(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    response = client.get(
        "/api/v1/kg/cognitive-pending",
        params={"board_id": BOARD, "offset": -1},
    )
    assert response.status_code == 422


# -------- generation_not_found ------------------------------------------


def test_explicit_unknown_generation_returns_404_generation_not_found(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    nonexistent = generate_kg_generation_id()
    response = client.get(
        "/api/v1/kg/cognitive-pending",
        params={"board_id": BOARD, "kg_generation_id": nonexistent},
    )
    assert response.status_code == 404, response.text
    detail = response.json()["detail"]
    assert detail["code"] == "generation_not_found"
    assert (
        detail["reason_code"]
        == CognitiveItemListReasonCode.NO_GENERATION_FOUND.value
    )


def test_omitted_generation_with_no_latest_returns_safe_empty(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    response = client.get(
        "/api/v1/kg/cognitive-pending",
        params={"board_id": "board-no-rebuild-yet"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["selected_kg_generation_id"] is None
    assert body["legacy_mode"] is False
    assert body["items"] == []
    assert body["counts"]["total"] == 0


# -------- Latest-generation fallback ------------------------------------


def test_omitting_kg_generation_id_uses_latest(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    import time
    _ = _seed(isolated_base_dir, [_row("spec", "a")])
    time.sleep(0.01)
    gen_b = _seed(isolated_base_dir, [
        _row("spec", "b1"),
        _row("refinement", "b2"),
    ])

    body = client.get(
        "/api/v1/kg/cognitive-pending", params={"board_id": BOARD}
    ).json()
    assert body["selected_kg_generation_id"] == gen_b
    assert body["counts"]["total"] == 2


# -------- tr_08d89b34 — bounded metric labels with surface=rest --------


def test_counter_emits_with_surface_rest(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    gen = _seed(isolated_base_dir, [_row("spec", "s1")])
    client.get(
        "/api/v1/kg/cognitive-pending",
        params={"board_id": BOARD, "kg_generation_id": gen},
    )
    samples = get_list_samples()
    assert len(samples) == 1
    assert samples[0]["surface"] == CognitiveItemListSurface.REST.value
    assert samples[0]["outcome"] == CognitiveItemListOutcome.SUCCESS.value
    # Labels remain bounded — same tuple shared with MCP.
    assert get_list_counter_labels() == (
        "surface", "board_id", "outcome",
        "status_filter_present", "reason_code",
    )


def test_counter_emits_validation_error_with_surface_rest_on_invalid_status(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    client.get(
        "/api/v1/kg/cognitive-pending",
        params={"board_id": BOARD, "status": "bogus"},
    )
    samples = get_list_samples()
    assert len(samples) == 1
    s = samples[0]
    assert s["surface"] == CognitiveItemListSurface.REST.value
    assert s["outcome"] == CognitiveItemListOutcome.VALIDATION_ERROR.value
    assert s["reason_code"] == CognitiveItemListReasonCode.INVALID_STATUS.value


def test_counter_emits_not_found_with_surface_rest_on_explicit_missing_gen(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    client.get(
        "/api/v1/kg/cognitive-pending",
        params={"board_id": BOARD, "kg_generation_id": generate_kg_generation_id()},
    )
    assert get_list_event_count(
        surface=CognitiveItemListSurface.REST.value,
        outcome=CognitiveItemListOutcome.NOT_FOUND.value,
    ) == 1


def test_counter_labels_exclude_high_cardinality_dims(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    gen = _seed(isolated_base_dir, [_row("spec", "s1")])
    client.get(
        "/api/v1/kg/cognitive-pending",
        params={"board_id": BOARD, "kg_generation_id": gen},
    )
    sample = get_list_samples()[0]
    forbidden = {
        "source_ref", "item_id", "status", "status_filter",
        "status_value", "actor", "reason", "kg_generation_id",
    }
    assert not forbidden.intersection(sample.keys())


# -------- Required board_id ---------------------------------------------


def test_missing_board_id_returns_422(
    client: TestClient,
) -> None:
    response = client.get("/api/v1/kg/cognitive-pending")
    assert response.status_code == 422
