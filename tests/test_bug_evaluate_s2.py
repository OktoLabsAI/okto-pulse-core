"""S2 / card 13b43f3d — bug evidence evaluation + REST/MCP twin.

Cenários (spec 6b91a862), via o core evaluate_bug_cognitive_closure (REST + MCP
twin compartilham este core):
  * ts_dbea2946 — readiness_effect/blocking/precedence_explanation VÊM do
    CognitiveReadinessService (espelhados, sem recomputar precedência local).
  * ts_6cc3403d — bug node ausente NÃO fabrica relação cognitiva; sinaliza
    remediation técnica.
  * ts_22aa3b7e — trivial/duplicate/no_reusable → no_action sem Learning.
  * ts_6b31aba2 — path_b_pending exige revisit_at (400).
  * ts_9fa1bd98 — DLQ/canonical_debt aberto → 409 antes de escrita.
  * ts_494731f9 — MCP skip/no_action fail-closes at the agent boundary; direct
    service tests still cover revisit_at 400.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.bug_cognitive_closure import (
    BugCognitiveActionLabel,
    evaluate_bug_cognitive_closure,
)
from okto_pulse.core.kg.cognitive_readiness import (
    CognitiveReadinessError,
    CognitiveReadinessService,
)
from okto_pulse.core.kg.rebuild_audit import CognitiveConsolidationItemStore
from okto_pulse.core.kg.rebuild_generation import generate_kg_generation_id
from okto_pulse.core.ports.bug_cognitive_context import BugCognitiveContext
from okto_pulse.core.ports.test_evidence import (
    TestEvidenceWriteVerification as EvidenceWriteVerification,
    register_test_evidence_write_verifier,
    reset_test_evidence_write_verifier_for_tests,
)
from sqlalchemy_test_models import Board, ConsolidationDeadLetter
from okto_pulse.core.services.canonical_debt_service import upsert_canonical_debt

UUID_A = "11111111-1111-1111-1111-111111111111"
GOOD_EVIDENCE = {"root_cause": "off-by-one", "fix_narrative": "guarded the bound"}
ACCEPTANCE_CRITERIA = (
    {"id": "ac-version", "text": "The requested version is rendered."},
)


class _TrustedEvidenceVerifier:
    def verify(self, **_request):  # noqa: ANN003, ANN201
        return EvidenceWriteVerification(True)


@pytest.fixture(autouse=True)
def _trusted_evidence_verifier():
    register_test_evidence_write_verifier(_TrustedEvidenceVerifier())
    yield
    reset_test_evidence_write_verifier_for_tests()


def _verified_scenario(board_id: str) -> dict:
    from okto_pulse.core.services.test_scenario_lifecycle import (
        compute_execution_attestation_sha256,
        compute_test_scenario_semantic_sha256,
    )

    scenario = {
        "id": "scenario-regression",
        "scenario_type": "regression",
        "given": "a stale compiled version was previously rendered",
        "when": "the corrected build is exercised",
        "then": "the requested version is rendered",
        "linked_criteria": ["ac-version"],
        "status": "passed",
    }
    scenario_sha256 = compute_test_scenario_semantic_sha256(
        board_id=board_id,
        spec_id="spec-evidence",
        scenario=scenario,
        acceptance_criteria=list(ACCEPTANCE_CRITERIA),
    )
    manifest_ref = "mcp://replay/bug-closeout"
    attestation = {
        "schema_version": 2,
        "run_id": "run-bug-closeout",
        "executed_at": "2026-07-14T12:00:00+00:00",
        "scenario_id": "scenario-regression",
        "scenario_sha256": scenario_sha256,
        "outcome": "passed",
        "product_runtime_exercised": True,
        "manifest_sha256": "sha256:" + "b" * 64,
        "assertions": [{
            "name": "bug is no longer reproduced",
            "expected": "fixed",
            "observed": "fixed",
            "status": "passed",
            "message": None,
        }],
        "provenance": {
            "producer": "okto-pulse-community",
            "producer_version": "0.3.0",
            "adapter": "mcp-replay-runner",
            "environment": "test",
        },
    }
    attestation["attestation_sha256"] = compute_execution_attestation_sha256(
        attestation,
        manifest_ref=manifest_ref,
    )
    return {
        **scenario,
        "evidence": {
            "manifest_ref": manifest_ref,
            "execution_attestation": attestation,
            "execution_receipt": "opaque-test-receipt",
        },
    }


def _complete_context(
    board_id: str,
    *,
    canonical_bug_present: bool | None = True,
    action_plan: str | None = None,
    conclusions: tuple[dict, ...] | None = None,
) -> BugCognitiveContext:
    return BugCognitiveContext(
        board_id=board_id,
        bug_id=UUID_A,
        card_exists=True,
        card_type="bug",
        status="done",
        expected_behavior="The requested version is rendered.",
        observed_behavior="A stale version is rendered.",
        action_plan=action_plan or (
            "Root cause was stale compiled metadata; rebuild the frontend from "
            "the authoritative release version."
        ),
        conclusions=(
            ({"text": "Rebuilt and verified the frontend bundle."},)
            if conclusions is None
            else conclusions
        ),
        validations=({"outcome": "success"},),
        comments=({"content": "Bundle and source version now agree."},),
        spec_id="spec-evidence",
        origin_task_id="task-origin",
        acceptance_criteria=ACCEPTANCE_CRITERIA,
        test_scenarios=(_verified_scenario(board_id),),
        canonical_bug_present=canonical_bug_present,
        provenance_refs=(f"sql:cards/{UUID_A}",),
    )


def _store(tmp_path) -> CognitiveConsolidationItemStore:
    return CognitiveConsolidationItemStore(base_dir=tmp_path)


def _seed_bug_pending(store, board, gen) -> None:
    store.materialize_from_marker(
        board_id=board, kg_generation_id=gen, event_ref="evt",
        source_set=[{"artifact_type": "bug", "id": UUID_A,
                     "source_ref": f"bug:{UUID_A}", "content_hash": "h"}],
    )


async def _board(db_factory, board_id) -> None:
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="bug-eval", owner_id="owner-e"))
            await db.commit()


# ---------------------------------------------------------------------------
# Core evaluate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_bug_evidence_returns_400(tmp_path, db_factory):
    board, gen = "be-noev", generate_kg_generation_id()
    await _board(db_factory, board)
    store = _store(tmp_path)
    _seed_bug_pending(store, board, gen)
    svc = CognitiveReadinessService(store)
    async with db_factory() as db:
        with pytest.raises(CognitiveReadinessError) as exc:
            await evaluate_bug_cognitive_closure(
                svc, db, board_id=board, bug_id=UUID_A, evidence={},
            )
    assert exc.value.code == "missing_bug_evidence" and exc.value.http_status == 400


@pytest.mark.asyncio
async def test_ts_dbea2946_mirrors_service_readiness(tmp_path, db_factory):
    board, gen = "be-mirror", generate_kg_generation_id()
    await _board(db_factory, board)
    store = _store(tmp_path)
    _seed_bug_pending(store, board, gen)
    async with db_factory() as db:
        await upsert_canonical_debt(
            db, board_id=board, artifact_type="card", artifact_id=UUID_A,
            source_ref=f"card:{UUID_A}", content_hash="h",
            target_status="done", canonical_state="failed",  # OPEN
        )
        await db.commit()
    svc = CognitiveReadinessService(store)
    async with db_factory() as db:
        verdict = await svc.evaluate_artifact(db, board_id=board, source_ref=f"bug:{UUID_A}")
        resp = await evaluate_bug_cognitive_closure(
            svc, db, board_id=board, bug_id=UUID_A, evidence=GOOD_EVIDENCE,
            requested_action="evaluate",
        )
    # espelhado, sem recomputar.
    assert resp["readiness_effect"] == verdict.readiness_effect == "blocking_technical"
    assert resp["blocking"] is verdict.blocking is True
    assert resp["precedence_explanation"] == verdict.precedence_explanation
    assert resp["precedence_explanation"]["tier"] == "canonical_debt_open"


@pytest.mark.asyncio
async def test_ts_6cc3403d_missing_bug_node_no_fabrication(tmp_path, db_factory):
    board, gen = "be-nonode", generate_kg_generation_id()
    await _board(db_factory, board)
    store = _store(tmp_path)
    _seed_bug_pending(store, board, gen)
    svc = CognitiveReadinessService(store)
    async with db_factory() as db:
        resp = await evaluate_bug_cognitive_closure(
            svc, db, board_id=board, bug_id=UUID_A, evidence=GOOD_EVIDENCE,
            requested_action="create_learning",
            bug_context=_complete_context(board, canonical_bug_present=False),
        )
    assert resp["status"] == "canonical_bug_node_absent"
    assert resp["technical_remediation"] == "reconcile_canonical_bug_node"
    assert resp["graph_commit_required"] is False
    assert resp["outcome_type"] is None  # nada fabricado
    assert resp["bug_action_label"] is None


@pytest.mark.asyncio
async def test_ts_22aa3b7e_no_action_via_evaluate(tmp_path, db_factory):
    board, gen = "be-noact", generate_kg_generation_id()
    await _board(db_factory, board)
    store = _store(tmp_path)
    _seed_bug_pending(store, board, gen)
    svc = CognitiveReadinessService(store)
    async with db_factory() as db:
        resp = await evaluate_bug_cognitive_closure(
            svc, db, board_id=board, bug_id=UUID_A, evidence=GOOD_EVIDENCE,
            requested_action="skip", reason_code="trivial_fix", actor="a",
        )
    assert resp["status"] == "skipped"
    assert resp["outcome_type"] == "no_action_required"
    assert resp["reason_code"] == "trivial_fix"
    assert resp["bug_action_label"] == BugCognitiveActionLabel.NO_REUSABLE_LEARNING.value


@pytest.mark.asyncio
async def test_ts_6b31aba2_path_b_missing_revisit_400(tmp_path, db_factory):
    board, gen = "be-pathb", generate_kg_generation_id()
    await _board(db_factory, board)
    store = _store(tmp_path)
    _seed_bug_pending(store, board, gen)
    svc = CognitiveReadinessService(store)
    async with db_factory() as db:
        with pytest.raises(CognitiveReadinessError) as exc:
            await evaluate_bug_cognitive_closure(
                svc, db, board_id=board, bug_id=UUID_A, evidence=GOOD_EVIDENCE,
                requested_action="skip", reason_code="path_b_pending", actor="a",
            )
    assert exc.value.code == "revisit_at_required" and exc.value.http_status == 400


@pytest.mark.asyncio
async def test_ts_9fa1bd98_open_dlq_409(tmp_path, db_factory):
    board, gen = "be-dlq", generate_kg_generation_id()
    await _board(db_factory, board)
    store = _store(tmp_path)
    _seed_bug_pending(store, board, gen)
    async with db_factory() as db:
        db.add(ConsolidationDeadLetter(
            id="dlq-e", board_id=board, artifact_type="card", artifact_id=UUID_A,
            original_queue_id="q", attempts=2, errors=[{"message": "x"}],
        ))
        await db.commit()
    svc = CognitiveReadinessService(store)
    async with db_factory() as db:
        with pytest.raises(CognitiveReadinessError) as exc:
            await evaluate_bug_cognitive_closure(
                svc, db, board_id=board, bug_id=UUID_A, evidence=GOOD_EVIDENCE,
                requested_action="skip", reason_code="trivial_fix", actor="a",
            )
    assert exc.value.code == "technical_debt_cannot_be_skipped"


@pytest.mark.asyncio
async def test_create_learning_ready_to_commit_with_reusable_evidence(tmp_path, db_factory):
    board, gen = "be-commit", generate_kg_generation_id()
    await _board(db_factory, board)
    store = _store(tmp_path)
    _seed_bug_pending(store, board, gen)
    svc = CognitiveReadinessService(store)
    async with db_factory() as db:
        resp = await evaluate_bug_cognitive_closure(
            svc, db, board_id=board, bug_id=UUID_A, evidence=GOOD_EVIDENCE,
            requested_action="create_learning",
            bug_context=_complete_context(board),
        )
    assert resp["status"] == "ready_to_commit"
    assert resp["graph_commit_required"] is True
    assert resp["bug_action_label"] == BugCognitiveActionLabel.CREATE_LEARNING_VALIDATES_BUG.value


@pytest.mark.asyncio
async def test_create_learning_without_reusable_evidence_fails_closed(tmp_path, db_factory):
    board, gen = "be-noreuse", generate_kg_generation_id()
    await _board(db_factory, board)
    store = _store(tmp_path)
    _seed_bug_pending(store, board, gen)
    svc = CognitiveReadinessService(store)
    async with db_factory() as db:
        resp = await evaluate_bug_cognitive_closure(
            svc, db, board_id=board, bug_id=UUID_A,
            evidence={"impact": "low"},  # sem root_cause/fix → não reutilizável
            requested_action="create_learning",
            bug_context=_complete_context(
                board,
                action_plan="short",
                conclusions=(),
            ),
        )
    assert resp["status"] == "evidence_incomplete"
    assert resp["reason_code"] is None
    assert resp["blocking"] is True
    assert resp["graph_commit_required"] is False  # nada fabricado


# ---------------------------------------------------------------------------
# ts_494731f9 — MCP skip/no_action is a human-only control
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts494731f9_mcp_skip_fail_closes_before_writepath(tmp_path, db_factory, monkeypatch):
    board, gen = "be-mcp", generate_kg_generation_id()
    await _board(db_factory, board)
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    _seed_bug_pending(_store(tmp_path), board, gen)

    import okto_pulse.core.mcp.server as mcp_server

    async def _fake_ctx(board_id: str):
        return SimpleNamespace(agent_id="mcp-agent")

    monkeypatch.setattr(mcp_server, "_get_agent_ctx", _fake_ctx)

    tool = await mcp_server.mcp.get_tool("okto_pulse_kg_evaluate_bug_cognitive_closure")

    # Agent-facing skip/no_action is human-only. The MCP tool fail-closes before
    # the central write path, so even an invalid skip reason shape must not
    # persist or leak into the central skip ledger.
    out = json.loads(await tool.fn(
        board_id=board, bug_id=UUID_A, evidence=GOOD_EVIDENCE,
        requested_action="skip", reason_code="path_b_pending",
    ))
    assert out["error"] == "human_control_required"
    assert out["code"] == "human_control_required"
    assert out["details"]["mutation_allowed"] is False
    assert out["details"]["state_changed"] is False
    assert out["details"]["required_actor"] == "human"
    assert out["details"]["blocked_tool"] == "okto_pulse_kg_evaluate_bug_cognitive_closure"
    assert out["details"]["blocked_action"] == "evaluate_bug_cognitive_closure:skip"
    assert out["details"]["target_ref"] == f"bug:{UUID_A}"

    # A semantically valid skip is still blocked at the same agent boundary.
    out2 = json.loads(await tool.fn(
        board_id=board, bug_id=UUID_A, evidence=GOOD_EVIDENCE,
        requested_action="skip", reason_code="trivial_fix",
    ))
    assert out2["error"] == "human_control_required"
    assert out2["details"]["mutation_allowed"] is False
    assert out2["details"]["state_changed"] is False


# ---------------------------------------------------------------------------
# ts_c4f11007 (reforço) — falha técnica permanece debt/blocking, nunca no_action
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tsc4f11007_create_learning_with_open_debt_stays_blocked(tmp_path, db_factory):
    # Uma falha técnica de Bug/Learning/relationship vira canonical_debt/DLQ. Quando
    # esse sinal técnico está OPEN, o evaluate de create_learning permanece BLOCKING
    # (status=blocked, blocking_technical) — NUNCA convertido em no_action e NUNCA
    # fabricando relação (graph_commit_required continua True; sem outcome).
    board, gen = "be-techdebt", generate_kg_generation_id()
    await _board(db_factory, board)
    store = _store(tmp_path)
    _seed_bug_pending(store, board, gen)
    async with db_factory() as db:
        await upsert_canonical_debt(
            db, board_id=board, artifact_type="card", artifact_id=UUID_A,
            source_ref=f"card:{UUID_A}", content_hash="h",
            target_status="done", canonical_state="failed",  # OPEN = falha técnica
        )
        await db.commit()
    svc = CognitiveReadinessService(store)
    async with db_factory() as db:
        resp = await evaluate_bug_cognitive_closure(
            svc, db, board_id=board, bug_id=UUID_A, evidence=GOOD_EVIDENCE,
            requested_action="create_learning",
            bug_context=_complete_context(board),
        )
    assert resp["status"] == "blocked"
    assert resp["readiness_effect"] == "blocking_technical"
    assert resp["blocking"] is True
    assert resp["graph_commit_required"] is False
    assert resp["outcome_type"] is None          # nada fabricado
    assert resp["reason_code"] is None            # NÃO virou no_action
    assert resp["technical_remediation"] == "resolve_technical_debt_before_commit"
