"""test_scenario CRUD + NC-9 service gate (spec 6f1e75bf, item #1).

Covers the 7 TC cards / 10 scenarios:

- TC-1 (ts_0547ae44, ts_92e31fdb): leaf single-source + anti-cycle import.
- TC-2 (ts_5d2e7aa5, ts_f25daf3d): update_spec NC-9 gate (transition + new + skip).
- TC-3 (ts_4e5c1170, ts_57e928b8): update_test_scenario body/clear/no-status +
  evidence invalidation on semantic edit (cosmetic preserves).
- TC-4 (ts_3b602216): content-lock on update + delete.
- TC-5 (ts_9de4528d): delete cascade — no orphan in Card.test_scenario_ids.
- TC-6 (ts_29836555): status guard — in_progress allowed, validated/done blocked
  unless a linked executable test card is carrying post-lock evidence.
- TC-7 (ts_144b47eb): scoped status path (no update_spec, no content-lock,
  preserves non-target scenarios) + REST endpoint.
"""

from __future__ import annotations

import ast
import inspect
import logging
import uuid

import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import auth_deps as _auth_mod
from okto_pulse.core.infra.database import get_db
from sqlalchemy_test_models import Board, Card, CardStatus, CardType, Spec, SpecStatus
from okto_pulse.core.models.schemas import CardMove, SpecUpdate
from okto_pulse.core.services.main import CardService, SpecLockedError, SpecService
from okto_pulse.core.services.resource_gate import ResourceGateService
from okto_pulse.core.services.test_scenario_lifecycle import StatusNotMutableError

USER = "tsl-user"
# A grandfathered direct automated-test-pointer (test_file_path + test_function)
# so it passes the spec-9e0bf979 write gate; the run-log fields are kept because
# some assertions read evidence["last_run_at"]. These lifecycle tests exercise
# status mutability / narrow-persist / REST gating, NOT evidence quality — the
# re-executable evidence contract itself is covered in test_evidence_class_contract.
_VALID_EVIDENCE = {
    "test_file_path": "tests/test_test_scenario_lifecycle.py",
    "test_function": "test_lifecycle_evidence",
    "last_run_at": "2026-01-01T00:00:00",
    "output_snippet": "1 passed",
}


async def _seed_spec(
    db_factory,
    *,
    status: SpecStatus = SpecStatus.DRAFT,
    scenarios: list | None = None,
    acs: list | None = None,
    skip_evidence: bool = False,
    locked: bool = False,
    card_scenarios: list | None = None,
    card_status: CardStatus = CardStatus.NOT_STARTED,
) -> tuple[str, str, str]:
    """Seed a board + spec (and optionally one card linking ``card_scenarios``)."""
    board_id = f"board-{uuid.uuid4()}"
    spec_id = str(uuid.uuid4())
    card_id = str(uuid.uuid4())
    async with db_factory() as db:
        settings = {"skip_test_evidence_global": True} if skip_evidence else {}
        db.add(Board(id=board_id, name="TSL", owner_id=USER, settings=settings))
        spec_kwargs = dict(
            id=spec_id,
            board_id=board_id,
            title="S",
            status=status,
            created_by=USER,
            functional_requirements=["FR1"],
            acceptance_criteria=acs
            if acs is not None
            else [{"id": "ac_one", "text": "AC one", "status": "active"}],
            test_scenarios=scenarios or [],
        )
        if locked:
            spec_kwargs["validations"] = [{"id": "val_x", "outcome": "success"}]
            spec_kwargs["current_validation_id"] = "val_x"
        db.add(Spec(**spec_kwargs))
        if card_scenarios is not None:
            db.add(
                Card(
                    id=card_id,
                    board_id=board_id,
                    spec_id=spec_id,
                    title="T",
                    status=card_status,
                    card_type=CardType.TEST,
                    created_by=USER,
                    test_scenario_ids=card_scenarios,
                )
            )
        await db.commit()
    return board_id, spec_id, card_id


async def _mark_card_resources_na(db, board_id: str, card_id: str) -> None:
    service = ResourceGateService(db)
    for resource_type in ("architecture", "mockup", "knowledge_base"):
        await service.mark_not_applicable(
            board_id,
            "card",
            card_id,
            resource_type,
            USER,
            justification=f"{resource_type} is intentionally not applicable in this lifecycle test.",
            source_channel="ui",
        )


# ====================================================================
# TC-1 — leaf single-source + anti-cycle import
# ====================================================================


def test_leaf_exports_and_local_copies_removed():
    # ts_0547ae44
    import okto_pulse.core.mcp.server as server
    import okto_pulse.core.services.main as main
    import okto_pulse.core.services.test_scenario_lifecycle as leaf

    for name in (
        "EVIDENCE_REQUIRED_KEYS",
        "GATED_STATUSES",
        "VALID_SCENARIO_STATUSES",
        "SEMANTIC_FIELDS",
        "COSMETIC_FIELDS",
        "validate_test_scenario_evidence",
        "scenario_has_required_evidence",
        "require_test_scenario_status_mutable",
    ):
        assert hasattr(leaf, name), name

    # The duplicated copies are gone from their old homes.
    assert not hasattr(server, "_validate_evidence")
    assert not hasattr(server, "_EVIDENCE_REQUIRED_KEYS")
    assert not hasattr(main, "_test_scenario_has_required_evidence")


def test_services_main_does_not_import_mcp_server():
    # ts_92e31fdb — no inverted dependency services -> mcp.server
    import okto_pulse.core.services.main as main

    tree = ast.parse(inspect.getsource(main))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                imported.add(alias.name)
    offenders = [m for m in imported if m and m.startswith("okto_pulse.core.mcp")]
    assert offenders == [], f"services.main must not import mcp: {offenders}"


# ====================================================================
# TC-2 — update_spec NC-9 gate
# ====================================================================


async def test_update_spec_rejects_gated_without_evidence_transition_and_new(db_factory):
    # ts_5d2e7aa5
    _b, spec_id, _c = await _seed_spec(
        db_factory, scenarios=[{"id": "ts_a", "title": "A", "status": "ready"}]
    )
    # (A) transition ready -> passed without evidence
    async with db_factory() as db:
        svc = SpecService(db)
        with pytest.raises(ValueError, match="evidence_required"):
            await svc.update_spec(
                spec_id,
                USER,
                SpecUpdate(test_scenarios=[{"id": "ts_a", "title": "A", "status": "passed"}]),
            )
    # (B) NEW scenario already passed without evidence (no old_s)
    async with db_factory() as db:
        svc = SpecService(db)
        with pytest.raises(ValueError, match="evidence_required"):
            await svc.update_spec(
                spec_id,
                USER,
                SpecUpdate(
                    test_scenarios=[
                        {"id": "ts_a", "title": "A", "status": "ready"},
                        {"id": "ts_new", "title": "New", "status": "passed"},
                    ]
                ),
            )


async def test_update_spec_accepts_gated_with_evidence(db_factory):
    _b, spec_id, _c = await _seed_spec(
        db_factory, scenarios=[{"id": "ts_a", "title": "A", "status": "ready"}]
    )
    async with db_factory() as db:
        svc = SpecService(db)
        spec = await svc.update_spec(
            spec_id,
            USER,
            SpecUpdate(
                test_scenarios=[
                    {"id": "ts_a", "title": "A", "status": "passed", "evidence": _VALID_EVIDENCE}
                ]
            ),
        )
        await db.commit()
    assert spec.test_scenarios[0]["status"] == "passed"


async def test_update_spec_skip_flag_allows_and_audits(db_factory, caplog):
    # ts_f25daf3d
    _b, spec_id, _c = await _seed_spec(
        db_factory,
        skip_evidence=True,
        scenarios=[{"id": "ts_a", "title": "A", "status": "ready"}],
    )
    async with db_factory() as db:
        svc = SpecService(db)
        with caplog.at_level(logging.INFO, logger="okto_pulse.spec.test_scenario"):
            spec = await svc.update_spec(
                spec_id,
                USER,
                SpecUpdate(test_scenarios=[{"id": "ts_a", "title": "A", "status": "passed"}]),
            )
            await db.commit()
    assert spec.test_scenarios[0]["status"] == "passed"
    assert any("evidence_gate_skipped" in r.getMessage() for r in caplog.records)


# ====================================================================
# TC-3 — update_test_scenario (body / clear / no-status / invalidation)
# ====================================================================


def test_update_test_scenario_does_not_accept_status():
    # ts_4e5c1170 — status is structurally absent from the handler
    sig = inspect.signature(SpecService.update_test_scenario)
    assert "status" not in sig.parameters


async def test_update_test_scenario_edits_body_and_clears(db_factory):
    # ts_4e5c1170
    _b, spec_id, _c = await _seed_spec(
        db_factory,
        scenarios=[
            {
                "id": "ts_a",
                "title": "A",
                "status": "ready",
                "given": "old g",
                "notes": "keep?",
                "linked_criteria": ["ac_one"],
            }
        ],
    )
    async with db_factory() as db:
        svc = SpecService(db)
        result = await svc.update_test_scenario(
            spec_id, USER, "ts_a", given="new g", title="A2", clear=["notes", "linked_criteria"]
        )
    assert set(result["updated_fields"]) == {"given", "title", "notes", "linked_criteria"}
    async with db_factory() as db:
        svc = SpecService(db)
        spec = await svc.get_spec(spec_id)
    sc = spec.test_scenarios[0]
    assert sc["given"] == "new g" and sc["title"] == "A2"
    assert sc["notes"] == "" and sc["linked_criteria"] == []
    assert sc["status"] == "ready"  # omitted/untouched field preserved
    assert sc.get("when", "") == ""  # never-set field stays absent/empty


async def test_update_test_scenario_resolves_linked_criteria(db_factory):
    # ts_4e5c1170 — reuse resolve_linked_criteria_to_ids
    _b, spec_id, _c = await _seed_spec(
        db_factory, scenarios=[{"id": "ts_a", "title": "A", "status": "ready"}]
    )
    async with db_factory() as db:
        svc = SpecService(db)
        await svc.update_test_scenario(spec_id, USER, "ts_a", linked_criteria=["0"])
        spec = await svc.get_spec(spec_id)
    assert spec.test_scenarios[0]["linked_criteria"] == ["ac_one"]


async def test_update_test_scenario_unresolved_criteria_fails_closed(db_factory):
    _b, spec_id, _c = await _seed_spec(
        db_factory, scenarios=[{"id": "ts_a", "title": "A", "status": "ready"}]
    )
    async with db_factory() as db:
        svc = SpecService(db)
        with pytest.raises(ValueError, match="unresolved_criteria"):
            await svc.update_test_scenario(spec_id, USER, "ts_a", linked_criteria=["ghost"])


async def test_semantic_edit_invalidates_evidence_cosmetic_preserves(db_factory):
    # ts_57e928b8
    base = {
        "id": "ts_a",
        "title": "A",
        "status": "passed",
        "given": "g",
        "evidence": dict(_VALID_EVIDENCE),
    }
    # (A) semantic edit (given) invalidates
    _b, spec_id, _c = await _seed_spec(db_factory, scenarios=[dict(base)])
    async with db_factory() as db:
        svc = SpecService(db)
        result = await svc.update_test_scenario(spec_id, USER, "ts_a", given="changed")
        assert result["evidence_invalidated"] is True
        spec = await svc.get_spec(spec_id)
    sc = spec.test_scenarios[0]
    assert sc["status"] == "ready" and not sc.get("evidence")

    # (B) cosmetic edit (title) preserves status + evidence
    _b2, spec_id2, _c2 = await _seed_spec(db_factory, scenarios=[dict(base)])
    async with db_factory() as db:
        svc = SpecService(db)
        result = await svc.update_test_scenario(spec_id2, USER, "ts_a", title="A-renamed")
        assert result["evidence_invalidated"] is False
        spec = await svc.get_spec(spec_id2)
    sc = spec.test_scenarios[0]
    assert sc["status"] == "passed" and sc["evidence"]["last_run_at"] == _VALID_EVIDENCE["last_run_at"]


# ====================================================================
# TC-4 — content-lock on update + delete
# ====================================================================


async def test_update_and_delete_respect_content_lock(db_factory):
    # ts_3b602216
    _b, spec_id, _c = await _seed_spec(
        db_factory,
        status=SpecStatus.IN_PROGRESS,
        locked=True,
        scenarios=[{"id": "ts_a", "title": "A", "status": "ready"}],
    )
    async with db_factory() as db:
        svc = SpecService(db)
        with pytest.raises(SpecLockedError):
            await svc.update_test_scenario(spec_id, USER, "ts_a", title="nope")
    async with db_factory() as db:
        svc = SpecService(db)
        with pytest.raises(SpecLockedError):
            await svc.delete_test_scenario(spec_id, USER, "ts_a")


# ====================================================================
# TC-5 — delete cascade (no orphan)
# ====================================================================


async def test_delete_test_scenario_cascade_no_orphan(db_factory):
    # ts_9de4528d
    board_id, spec_id, card_id = await _seed_spec(
        db_factory,
        scenarios=[
            {"id": "ts_a", "title": "A", "status": "ready"},
            {"id": "ts_b", "title": "B", "status": "ready"},
        ],
        card_scenarios=["ts_a", "ts_b"],
    )
    async with db_factory() as db:
        svc = SpecService(db)
        result = await svc.delete_test_scenario(spec_id, USER, "ts_a")
    assert result["cards_unlinked"] == [card_id]
    async with db_factory() as db:
        svc = SpecService(db)
        spec = await svc.get_spec(spec_id)
        card = await db.get(Card, card_id)
    assert [s["id"] for s in spec.test_scenarios] == ["ts_b"]
    assert card.test_scenario_ids == ["ts_b"]  # no orphan


async def test_delete_test_scenario_not_found(db_factory):
    _b, spec_id, _c = await _seed_spec(
        db_factory, scenarios=[{"id": "ts_a", "title": "A", "status": "ready"}]
    )
    async with db_factory() as db:
        svc = SpecService(db)
        with pytest.raises(ValueError, match="scenario_not_found"):
            await svc.delete_test_scenario(spec_id, USER, "ts_ghost")


# ====================================================================
# TC-6 — status guard (in_progress allowed, locked specs need executable test card)
# ====================================================================


async def test_status_allowed_in_progress_blocked_validated_done(db_factory):
    # ts_29836555
    scenarios = [{"id": "ts_a", "title": "A", "status": "ready"}]

    # in_progress (locked by a passed validation) — still allowed, proving the
    # guard is by STATUS and NOT the content-lock.
    _b, ip_id, _c = await _seed_spec(
        db_factory, status=SpecStatus.IN_PROGRESS, locked=True, scenarios=list(scenarios)
    )
    async with db_factory() as db:
        svc = SpecService(db)
        res = await svc.set_test_scenario_status(ip_id, USER, "ts_a", "passed", _VALID_EVIDENCE)
        assert res["new_status"] == "passed"

    for blocked_status in (SpecStatus.VALIDATED, SpecStatus.DONE):
        _b, sid, _c = await _seed_spec(
            db_factory, status=blocked_status, scenarios=list(scenarios)
        )
        async with db_factory() as db:
            svc = SpecService(db)
            with pytest.raises(StatusNotMutableError):
                await svc.set_test_scenario_status(sid, USER, "ts_a", "passed", _VALID_EVIDENCE)


@pytest.mark.parametrize("locked_status", [SpecStatus.VALIDATED, SpecStatus.DONE])
async def test_status_allowed_on_locked_spec_when_scenario_has_executable_test_card(
    db_factory,
    locked_status: SpecStatus,
):
    # Regression for post-lock / post-done regression evidence: status changes
    # are operational evidence, not semantic spec edits, but only after a real
    # test card is linked and has entered execution/review.
    board_id, spec_id, card_id = await _seed_spec(
        db_factory,
        status=locked_status,
        locked=locked_status == SpecStatus.VALIDATED,
        scenarios=[{"id": "ts_a", "title": "A", "status": "ready"}],
        card_scenarios=["ts_a"],
        card_status=CardStatus.VALIDATION,
    )
    async with db_factory() as db:
        svc = SpecService(db)
        res = await svc.set_test_scenario_status(
            spec_id, USER, "ts_a", "passed", _VALID_EVIDENCE
        )
        assert res["new_status"] == "passed"

    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        card = await db.get(Card, card_id)
        scenario = next(sc for sc in spec.test_scenarios if sc["id"] == "ts_a")
        assert scenario["status"] == "passed"
        assert scenario["evidence"] == _VALID_EVIDENCE
        assert spec.status == locked_status
        if locked_status == SpecStatus.VALIDATED:
            assert spec.current_validation_id == "val_x"
        assert card.test_scenario_ids == ["ts_a"]


async def test_status_still_blocked_on_done_spec_when_test_card_not_executable(db_factory):
    _b, spec_id, _card_id = await _seed_spec(
        db_factory,
        status=SpecStatus.DONE,
        scenarios=[{"id": "ts_a", "title": "A", "status": "ready"}],
        card_scenarios=["ts_a"],
        card_status=CardStatus.NOT_STARTED,
    )
    async with db_factory() as db:
        svc = SpecService(db)
        with pytest.raises(StatusNotMutableError):
            await svc.set_test_scenario_status(
                spec_id, USER, "ts_a", "passed", _VALID_EVIDENCE
            )


async def test_done_spec_test_card_can_record_evidence_then_move_done(db_factory):
    # Regression for the observed catch-22: a done spec's residual/regression
    # test card must be closable without reopening the spec when the scenario is
    # already linked to that executable test card.
    board_id, spec_id, card_id = await _seed_spec(
        db_factory,
        status=SpecStatus.DONE,
        scenarios=[{"id": "ts_a", "title": "A", "status": "ready"}],
        card_scenarios=["ts_a"],
        card_status=CardStatus.VALIDATION,
    )
    async with db_factory() as db:
        await _mark_card_resources_na(db, board_id, card_id)
        await SpecService(db).set_test_scenario_status(
            spec_id,
            USER,
            "ts_a",
            "passed",
            _VALID_EVIDENCE,
        )
        moved = await CardService(db).move_card(
            card_id,
            USER,
            CardMove(
                status=CardStatus.DONE,
                conclusion="Residual test evidence was recorded against the locked spec scenario.",
                completeness=100,
                completeness_justification="Scenario status and evidence are complete.",
                drift=0,
                drift_justification="No deviation from the original test reconciliation scope.",
            ),
        )
        assert moved.status == CardStatus.DONE

    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        scenario = next(sc for sc in spec.test_scenarios if sc["id"] == "ts_a")
        assert scenario["status"] == "passed"
        assert scenario["evidence"] == _VALID_EVIDENCE
        assert spec.status == SpecStatus.DONE


async def test_status_rejects_gated_without_evidence(db_factory):
    _b, spec_id, _c = await _seed_spec(
        db_factory,
        status=SpecStatus.APPROVED,
        scenarios=[{"id": "ts_a", "title": "A", "status": "ready"}],
    )
    async with db_factory() as db:
        svc = SpecService(db)
        with pytest.raises(ValueError, match="evidence_required"):
            await svc.set_test_scenario_status(spec_id, USER, "ts_a", "passed", None)


# ====================================================================
# TC-7 — scoped status path + REST endpoint
# ====================================================================


async def test_status_path_does_not_call_update_spec(db_factory, monkeypatch):
    # ts_144b47eb — scoped path bypasses update_spec AND the content-lock.
    _b, spec_id, _c = await _seed_spec(
        db_factory,
        status=SpecStatus.IN_PROGRESS,
        locked=True,
        scenarios=[{"id": "ts_a", "title": "A", "status": "ready"}],
    )
    async with db_factory() as db:
        svc = SpecService(db)

        async def _boom(*a, **k):
            raise AssertionError("status path must not call update_spec")

        monkeypatch.setattr(SpecService, "update_spec", _boom)
        res = await svc.set_test_scenario_status(spec_id, USER, "ts_a", "passed", _VALID_EVIDENCE)
        assert res["new_status"] == "passed"


async def test_status_path_preserves_non_target_scenarios(db_factory):
    # ts_144b47eb — non-target scenarios stay semantically identical.
    other = {"id": "ts_y", "title": "Y", "status": "ready", "given": "g", "when": "w", "then": "t"}
    _b, spec_id, _c = await _seed_spec(
        db_factory,
        status=SpecStatus.APPROVED,
        scenarios=[{"id": "ts_x", "title": "X", "status": "ready"}, dict(other)],
    )
    async with db_factory() as db:
        svc = SpecService(db)
        await svc.set_test_scenario_status(spec_id, USER, "ts_x", "passed", _VALID_EVIDENCE)
    async with db_factory() as db:
        svc = SpecService(db)
        spec = await svc.get_spec(spec_id)
    by_id = {s["id"]: s for s in spec.test_scenarios}
    assert by_id["ts_x"]["status"] == "passed"
    assert by_id["ts_y"] == other  # untouched, byte-for-byte


@pytest_asyncio.fixture
async def rest_client(db_factory):
    from okto_pulse.community.api.specs import router as specs_router

    _b, approved_id, _c = await _seed_spec(
        db_factory,
        status=SpecStatus.APPROVED,
        scenarios=[{"id": "ts_a", "title": "A", "status": "ready"}],
    )
    _b2, validated_id, _c2 = await _seed_spec(
        db_factory,
        status=SpecStatus.VALIDATED,
        scenarios=[{"id": "ts_a", "title": "A", "status": "ready"}],
    )
    app = FastAPI()
    app.include_router(specs_router, prefix="/api/v1")

    async def _override_db():
        async with db_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER
    app.dependency_overrides[_auth_mod.get_realm_id] = lambda: "local"
    return TestClient(app), approved_id, validated_id


def test_rest_status_endpoint_gate_and_scoped(rest_client):
    # ts_144b47eb — REST PATCH rejects passed without evidence (422), accepts with.
    client, spec_id, _validated = rest_client
    base = f"/api/v1/specs/{spec_id}/scenarios/ts_a/status"

    resp = client.patch(base, json={"status": "passed"})
    assert resp.status_code == 422, resp.text

    resp = client.patch(base, json={"status": "passed", "evidence": _VALID_EVIDENCE})
    assert resp.status_code == 200, resp.text
    assert resp.json()["scenario"]["status"] == "passed"


def test_rest_status_endpoint_blocks_validated(rest_client):
    # ts_29836555 / ts_144b47eb — arbitrary status change on a validated spec
    # with no executable linked test card → 409.
    client, _approved, validated_id = rest_client
    resp = client.patch(
        f"/api/v1/specs/{validated_id}/scenarios/ts_a/status",
        json={"status": "passed", "evidence": _VALID_EVIDENCE},
    )
    assert resp.status_code == 409, resp.text
