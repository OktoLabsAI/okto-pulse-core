"""Paridade REST/MCP da spec evaluation + fix do doc-drift de complexity.

Findings do ciclo KGDL.01 (2026-06-10):
1. O gate validated→in_progress exige uma evaluation approve, mas a
   submissão só existia como MCP tool — usuários UI/REST ficavam presos em
   'validated' sem caminho de escrita (dead-end, não bypass: o gate é
   server-side em move_spec). Fix: POST/GET /specs/{id}/evaluations +
   caminho de escrita único em SpecService.submit_spec_evaluation.
2. IdeationCreate.complexity anunciava low/medium/high/very_high na
   description, mas o enum é small/medium/large — e valor inválido virava
   500 dentro do service. Fix: description corrigida + validator → 422.
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.core.api.ideations import router as ideations_router
from okto_pulse.core.api.specs import router as specs_router
from okto_pulse.core.infra import auth as _auth_mod
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models.db import Board, Spec, SpecStatus

USER_ID = "spec-eval-rest-user"


def _evaluation_payload(recommendation: str = "approve") -> dict:
    return {
        "breakdown_completeness": 92,
        "breakdown_justification": "Cards mapeiam todos os FRs sem sobreposicao.",
        "granularity": 90,
        "granularity_justification": "Cards atomicos por seam de codigo.",
        "dependency_coherence": 91,
        "dependency_justification": "Ordem IMPL-1 -> IMPL-2 respeitada.",
        "test_coverage_quality": 93,
        "test_coverage_justification": "Cenarios cobrem corrida e durabilidade.",
        "overall_score": 92,
        "overall_justification": "Breakdown executavel e coberto por testes.",
        "recommendation": recommendation,
    }


@pytest_asyncio.fixture
async def spec_eval_client(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    validated_spec_id = str(uuid.uuid4())
    draft_spec_id = str(uuid.uuid4())
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Spec Eval REST", owner_id=USER_ID))
        db.add(Spec(
            id=validated_spec_id, board_id=board_id, title="Validated Spec",
            status=SpecStatus.VALIDATED, created_by=USER_ID,
            functional_requirements=["FR1"], acceptance_criteria=["AC1"],
            test_scenarios=[], business_rules=[], api_contracts=[],
            # Skips de cobertura: o teste isola o gate QUALITATIVO de
            # in_progress (evaluations) — os gates de cobertura têm suíte
            # própria.
            skip_test_coverage=True, skip_rules_coverage=True,
            skip_trs_coverage=True, skip_contract_coverage=True,
            skip_ir_coverage=True, skip_or_coverage=True,
            skip_decisions_coverage=True,
            # O gate decision-required (spec 4028ebd4) ignora o skip acima e
            # roda antes do gate qualitativo de evaluations sob teste.
            decisions=[{"id": "dec_seed", "title": "Seed decision",
                        "rationale": "Decision de fixture p/ o gate decision-required.",
                        "status": "active"}],
        ))
        db.add(Spec(
            id=draft_spec_id, board_id=board_id, title="Draft Spec",
            status=SpecStatus.DRAFT, created_by=USER_ID,
            functional_requirements=["FR1"], acceptance_criteria=["AC1"],
            test_scenarios=[], business_rules=[], api_contracts=[],
        ))
        await db.commit()

    app = FastAPI()
    app.include_router(specs_router, prefix="/api/v1")
    app.include_router(ideations_router, prefix="/api/v1")

    async def _override_db():
        async with db_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER_ID
    return TestClient(app), validated_spec_id, draft_spec_id, board_id


def test_submit_and_list_spec_evaluation_via_rest(spec_eval_client):
    client, spec_id, _draft, _board = spec_eval_client

    resp = client.post(f"/api/v1/specs/{spec_id}/evaluations", json=_evaluation_payload())
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["success"] is True
    evaluation = body["evaluation"]
    assert evaluation["recommendation"] == "approve"
    assert evaluation["evaluator_type"] == "user"
    assert evaluation["dimensions"]["test_coverage_quality"]["score"] == 93
    assert evaluation["stale"] is False

    listed = client.get(f"/api/v1/specs/{spec_id}/evaluations")
    assert listed.status_code == 200, listed.text
    payload = listed.json()
    assert payload["spec_id"] == spec_id
    assert payload["active_count"] == 1
    assert payload["evaluations"][0]["id"] == evaluation["id"]


def test_rest_evaluation_satisfies_in_progress_gate(spec_eval_client):
    """O cenario exato do finding: usuario so-REST consegue destravar
    validated→in_progress sem MCP."""
    client, spec_id, _draft, _board = spec_eval_client

    blocked = client.post(f"/api/v1/specs/{spec_id}/move", json={"status": "in_progress"})
    assert blocked.status_code == 400
    assert "no evaluation with 'approve'" in blocked.text

    assert client.post(
        f"/api/v1/specs/{spec_id}/evaluations", json=_evaluation_payload()
    ).status_code == 201

    moved = client.post(f"/api/v1/specs/{spec_id}/move", json={"status": "in_progress"})
    assert moved.status_code == 200, moved.text
    assert moved.json()["status"] == "in_progress"


def test_evaluation_rejected_outside_validated_status(spec_eval_client):
    client, _validated, draft_spec_id, _board = spec_eval_client
    resp = client.post(f"/api/v1/specs/{draft_spec_id}/evaluations", json=_evaluation_payload())
    assert resp.status_code == 409
    assert "must be in 'validated' status" in resp.text


def test_evaluation_validates_scores_and_recommendation(spec_eval_client):
    client, spec_id, _draft, _board = spec_eval_client

    bad_score = _evaluation_payload()
    bad_score["overall_score"] = 150
    assert client.post(f"/api/v1/specs/{spec_id}/evaluations", json=bad_score).status_code == 422

    bad_rec = _evaluation_payload(recommendation="maybe")
    assert client.post(f"/api/v1/specs/{spec_id}/evaluations", json=bad_rec).status_code == 422


def test_mcp_tool_shares_service_write_path():
    """Anti-drift: o MCP tool não pode reconstruir a evaluation inline — deve
    delegar ao SpecService (caminho de escrita único)."""
    from pathlib import Path

    server_src = (
        Path(__file__).resolve().parents[1]
        / "src" / "okto_pulse" / "core" / "mcp" / "server.py"
    ).read_text(encoding="utf-8")
    start = server_src.index("async def okto_pulse_submit_spec_evaluation")
    end = server_src.index("async def okto_pulse_list_spec_evaluations")
    tool_body = server_src[start:end]
    assert "service.submit_spec_evaluation(" in tool_body
    assert '"evaluator_type": "agent"' not in tool_body, (
        "tool reconstruiu a evaluation inline — use o SpecService"
    )


# ---------------------------------------------------------------------------
# Doc-drift de IdeationCreate.complexity (finding 2)
# ---------------------------------------------------------------------------


def test_ideation_complexity_invalid_value_is_422_not_500(spec_eval_client):
    client, _spec, _draft, board_id = spec_eval_client
    resp = client.post(
        f"/api/v1/boards/{board_id}/ideations",
        json={"title": "Drift check", "complexity": "high"},
    )
    assert resp.status_code == 422, resp.text
    assert "small, medium, large" in resp.text


@pytest.mark.parametrize("value", ["small", "medium", "large"])
def test_ideation_complexity_accepts_enum_values(value):
    from okto_pulse.core.models.schemas import IdeationCreate, IdeationUpdate

    assert IdeationCreate(title="t", complexity=value).complexity == value
    assert IdeationUpdate(complexity=value).complexity == value


def test_ideation_complexity_description_matches_enum():
    from okto_pulse.core.models.db import IdeationComplexity
    from okto_pulse.core.models.schemas import IdeationCreate, IdeationUpdate

    for model in (IdeationCreate, IdeationUpdate):
        desc = model.model_fields["complexity"].description or ""
        for member in IdeationComplexity:
            assert member.value in desc, (
                f"{model.__name__}.complexity description nao menciona "
                f"'{member.value}' — doc drift reintroduzido"
            )
        assert "very_high" not in desc and "low" not in desc
