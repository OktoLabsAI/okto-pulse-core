"""Tests for KG-03A.2 — Badge eligibility extension for decisions and
docstring cleanup.

Scope (no parallel routes; extend existing handlers):
* AC4 — Decision source_ref with NO active item returns
  ``show_badge=false, reason=not_found``, and ``eligible_entity_types``
  must include ``decision``.
* AC5 — Ideation source_ref remains ineligible
  (``reason=ineligible_entity_type``).
* tr_482c7be5 — Decision in ``ELIGIBLE_ENTITY_TYPES``; resolver returns
  ``not_found`` (not ``ineligible_entity_type``) for missing items.
* Architecture: single GET endpoint at ``/kg/cognitive-pending/badges``
  (no parallel route added).
* Batching: request validation still caps at 200 source_refs (KG-03.6
  invariant preserved).
* Doc drift: docstring in ``cognitive_badge_resolver.py`` no longer
  declares ``decision`` ineligible.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api.router import api_router
from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.core.kg.cognitive_badge_resolver import (
    CognitiveBadgeReason,
    ELIGIBLE_ENTITY_TYPES,
    INELIGIBLE_ENTITY_TYPES,
    EntityCardType,
    resolve_entity_badges,
)
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitivePendingMarker,
)
from okto_pulse.core.kg.rebuild_generation import generate_kg_generation_id


BOARD = "board-kg03a-2"


@pytest.fixture
def isolated_base_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    target = tmp_path / "kg-03a-2"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(target))
    return target


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(api_router)

    async def _fake_user() -> str:
        return "user-kg03a-2-test"

    app.dependency_overrides[require_user] = _fake_user
    return TestClient(app)


# -------- Eligibility invariants (KG-03A.2 + tr_482c7be5) ----------------


def test_decision_is_eligible_in_resolver_constants() -> None:
    """tr_482c7be5: ``decision`` MUST be in the eligible set after
    KG-03A.2 — KG-03.6's earlier exclusion is obsoleted."""

    assert EntityCardType.DECISION.value in ELIGIBLE_ENTITY_TYPES
    assert EntityCardType.DECISION.value not in INELIGIBLE_ENTITY_TYPES


def test_ideation_remains_ineligible() -> None:
    """KG-03A spec dec_0d0f2d9c: ideation stays outside the cognitive
    pending source enumeration and badge surface."""

    assert EntityCardType.IDEATION.value in INELIGIBLE_ENTITY_TYPES
    assert EntityCardType.IDEATION.value not in ELIGIBLE_ENTITY_TYPES


def test_other_remains_ineligible() -> None:
    """Unknown/malformed entity types collapse to ``other`` and stay
    ineligible — the fallback path must NOT accidentally let them
    render the badge."""

    assert EntityCardType.OTHER.value in INELIGIBLE_ENTITY_TYPES


def test_eligible_set_is_exactly_six_types() -> None:
    assert ELIGIBLE_ENTITY_TYPES == frozenset({
        "spec",
        "decision",
        "refinement",
        "task",
        "test",
        "bug",
    })


# -------- AC4 — decision without pending item ---------------------------


def test_ac4_decision_without_pending_item_returns_not_found(
    isolated_base_dir: Path,
) -> None:
    """AC4: ``decision:<spec_id>:<decision_id>`` source_ref with no
    pending item returns ``show_badge=false, reason=not_found`` (NOT
    ``ineligible_entity_type``)."""

    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    badges, eligible = resolve_entity_badges(
        board_id=BOARD,
        source_refs=["decision:s1:dec-a"],
        card_entity_types={"decision:s1:dec-a": "decision"},
        kg_generation_id=None,
        store=store,
    )
    badge = badges["decision:s1:dec-a"]
    assert badge.show_badge is False
    assert badge.reason == CognitiveBadgeReason.NOT_FOUND.value
    assert badge.status is None
    # eligible set must include decision so the UI knows decision is
    # part of the badge contract.
    assert "decision" in eligible


def test_decision_with_active_pending_item_shows_badge(
    isolated_base_dir: Path,
) -> None:
    """Sanity check: a real pending item for a decision source_ref does
    render the badge — extension is functional, not a feature flag."""

    gen = generate_kg_generation_id()
    marker = CognitivePendingMarker(base_dir=isolated_base_dir)
    marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=[
            {
                "artifact_type": "decision",
                "id": "s1:dec-a",
                "source_ref": "decision:s1:dec-a",
            }
        ],
        event_ref="evt_kg03a_2",
    )
    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    badges, _ = resolve_entity_badges(
        board_id=BOARD,
        source_refs=["decision:s1:dec-a"],
        card_entity_types={"decision:s1:dec-a": "decision"},
        kg_generation_id=gen,
        store=store,
    )
    badge = badges["decision:s1:dec-a"]
    assert badge.show_badge is True
    assert badge.reason == CognitiveBadgeReason.ACTIVE_COGNITIVE_ITEM.value
    assert badge.status == "pending"


# -------- AC5 — ideation remains ineligible -----------------------------


def test_ac5_ideation_returns_ineligible_entity_type(
    isolated_base_dir: Path,
) -> None:
    """AC5: ideation source_ref returns ``show_badge=false,
    reason=ineligible_entity_type`` regardless of whether any cognitive
    item exists in the store."""

    store = CognitiveConsolidationItemStore(base_dir=isolated_base_dir)
    badges, _ = resolve_entity_badges(
        board_id=BOARD,
        source_refs=["ideation:i1"],
        card_entity_types={"ideation:i1": "ideation"},
        kg_generation_id=None,
        store=store,
    )
    badge = badges["ideation:i1"]
    assert badge.show_badge is False
    assert badge.reason == CognitiveBadgeReason.INELIGIBLE_ENTITY_TYPE.value


# -------- Endpoint extension (no parallel routes) -----------------------


def test_only_one_badges_endpoint_registered() -> None:
    """KG-03A.2 invariant: extending the existing handler MUST NOT
    create a parallel route. Walk every route on api_router; only ONE
    path equals ``/api/v1/kg/cognitive-pending/badges`` and it accepts
    GET only."""

    matching = [
        route for route in api_router.routes
        if getattr(route, "path", None) == "/api/v1/kg/cognitive-pending/badges"
    ]
    assert len(matching) == 1, (
        f"expected exactly one /badges route registered; found "
        f"{len(matching)} — parallel route violates KG-03A.2 invariant"
    )
    methods = matching[0].methods or set()
    assert "GET" in methods
    assert not {"POST", "PUT", "PATCH", "DELETE"}.intersection(methods)


def test_endpoint_advertises_decision_in_eligible_entity_types(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    """The existing GET endpoint surfaces the extended eligibility
    set: ``decision`` must appear in ``eligible_entity_types``."""

    response = client.get(
        "/api/v1/kg/cognitive-pending/badges",
        params=[("board_id", BOARD), ("source_refs", "decision:s1:dec-a")],
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert "decision" in body["eligible_entity_types"]
    # Without any seeded item the response is not_found, not ineligible.
    assert body["badges"]["decision:s1:dec-a"]["show_badge"] is False
    assert (
        body["badges"]["decision:s1:dec-a"]["reason"]
        == CognitiveBadgeReason.NOT_FOUND.value
    )


def test_endpoint_routes_decision_and_ideation_in_same_batch(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    """Mixed batch: a decision ref and an ideation ref. Decision must
    resolve to not_found; ideation must resolve to
    ineligible_entity_type. The extension MUST NOT collapse them."""

    response = client.get(
        "/api/v1/kg/cognitive-pending/badges",
        params=[
            ("board_id", BOARD),
            ("source_refs", "decision:s1:dec-a"),
            ("source_refs", "ideation:i1"),
        ],
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert (
        body["badges"]["decision:s1:dec-a"]["reason"]
        == CognitiveBadgeReason.NOT_FOUND.value
    )
    assert (
        body["badges"]["ideation:i1"]["reason"]
        == CognitiveBadgeReason.INELIGIBLE_ENTITY_TYPE.value
    )


# -------- Batch limit preserved (KG-03.6 invariant) ---------------------


def test_batch_limit_200_is_preserved(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    """KG-03A.2 must not relax KG-03.6's 200-ref cap."""

    refs = [("source_refs", f"decision:s1:dec-{i}") for i in range(201)]
    response = client.get(
        "/api/v1/kg/cognitive-pending/badges",
        params=[("board_id", BOARD), *refs],
    )
    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "invalid_source_refs"


def test_batch_limit_200_exactly_is_accepted(
    isolated_base_dir: Path,
    client: TestClient,
) -> None:
    """Lower-bound regression: exactly 200 refs must still be accepted."""

    refs = [("source_refs", f"decision:s1:dec-{i}") for i in range(200)]
    response = client.get(
        "/api/v1/kg/cognitive-pending/badges",
        params=[("board_id", BOARD), *refs],
    )
    assert response.status_code == 200, response.text
    assert len(response.json()["badges"]) == 200


# -------- Docstring drift regression ------------------------------------


def test_module_docstring_no_longer_lists_decision_as_ineligible() -> None:
    """KG-03A.2 explicit requirement — the module docstring must reflect
    the extended eligibility (decision IS eligible). A regression test
    for this is cheap and catches future doc drift the next time someone
    edits the eligibility table."""

    from okto_pulse.core.kg import cognitive_badge_resolver
    doc = cognitive_badge_resolver.__doc__ or ""
    # The eligibility table must declare decision under ELIGIBLE.
    assert "decision" in doc
    # No line in the doc may declare decision under INELIGIBLE.
    # Look for "INELIGIBLE" with "decision" on the same line.
    for line in doc.split("\n"):
        if "INELIGIBLE" in line.upper():
            assert "decision" not in line.lower(), (
                f"docstring drift: decision is listed as INELIGIBLE "
                f"in line: {line!r}"
            )
        if "->" in line and "decision" in line.lower():
            # The decision-bearing line must say ELIGIBLE (or be the
            # blank rationale section that doesn't say either).
            if "ineligible" in line.lower():
                pytest.fail(
                    f"docstring drift: decision flagged ineligible in {line!r}"
                )
