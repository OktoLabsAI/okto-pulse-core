"""Composed planning gate against real Community persistence.

Lint acceptance is isolated by the existing fixture; coverage, lifecycle,
decomposition, native writes and checklist snapshot use their real services.
The complete externally accepted lint/UI/MCP flow belongs to the U4 rehearsal.
"""
import pytest
import pytest_asyncio

from sqlalchemy_test_models import Spec, SpecStatus
from okto_pulse.core.services.main import SpecService
from okto_pulse.core.models.schemas import SpecMove
from okto_pulse.core.ports.application_persistence import get_application_persistence_port
from test_allowed_transitions_mutation_parity_regressions import _direct_spec_context_fields
from test_e2e_regression_hardening import (
    USER_ID, _seed_board_and_spec, _validation_payload,
    _install_current_external_requirement_lint,
)


def decomposition_payload(recommendation="approve", score=90):
    return {
        "breakdown_completeness": score,
        "breakdown_justification": "Every planned obligation has an owner.",
        "granularity": score,
        "granularity_justification": "Each task has one bounded deliverable.",
        "dependency_coherence": score,
        "dependency_justification": "Dependencies match the execution ordering.",
        "test_coverage_quality": score,
        "test_coverage_justification": "Tests address each observable acceptance outcome.",
        "overall_score": score,
        "overall_justification": "The decomposition was independently reviewed.",
        "recommendation": recommendation,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("quality,review,score,expected,blocker", [
    ("approve", None, 90, "approved", "decomposition_approval_missing"),
    ("approve", "approve", 90, "validated", None),
    ("approve", "reject", 90, "approved", "decomposition_rejected"),
    ("approve", "request_changes", 90, "approved", "decomposition_approval_missing"),
    ("approve", "approve", 40, "approved", "decomposition_score_below_threshold"),
    ("reject", "approve", 90, "approved", "spec_validation_failed"),
    ("reject", "reject", 90, "approved", "spec_validation_failed"),
])
async def test_composition_keeps_native_results_and_promotes_only_joint_success(
    db_factory, monkeypatch, quality, review, score, expected, blocker,
):
    _install_current_external_requirement_lint(monkeypatch)
    async with db_factory() as db:
        _, spec_id = await _seed_board_and_spec(db)
        payload = {**_validation_payload(), "recommendation": quality}
        if review:
            payload["decomposition"] = decomposition_payload(review, score)
        result = await SpecService(db).submit_spec_validation(
            spec_id, USER_ID, "Evaluator", payload, actor_type="agent",
        )
        await get_application_persistence_port().flush(db)
        db.expire_all()
        spec = await db.get(Spec, spec_id)
        assert spec.status.value == expected
        assert result["planning_result"] == ("ready" if expected == "validated" else "blocked")
        assert result["outcome"] == ("success" if quality == "approve" else "failed")
        assert len(spec.validations) == 1
        assert spec.validations[0]["outcome"] == result["outcome"]
        assert "planning_result" not in spec.validations[0]
        assert len(spec.evaluations or []) == int(review is not None)
        if review:
            native = spec.evaluations[0]
            assert native["recommendation"] == review
            assert native["evaluator_id"] == USER_ID
            assert native["evaluator_type"] == "agent"
            assert native["spec_edition"] == spec.edition
            assert result["decomposition_ids"] == [native["id"]]
        assert (blocker in result["planning_blockers"]) if blocker else not result["planning_blockers"]


@pytest.mark.asyncio
async def test_conclusion_consumes_another_reviewers_current_review(db_factory, monkeypatch):
    _install_current_external_requirement_lint(monkeypatch)
    async with db_factory() as db:
        _, spec_id = await _seed_board_and_spec(db)
        service = SpecService(db)
        review = await service.submit_spec_evaluation(
            spec_id, "independent-reviewer", "Independent reviewer", decomposition_payload(),
        )
        await get_application_persistence_port().flush(db)
        spec = await service.get_spec(spec_id)
        payload = {**_validation_payload(), "expected_spec_version": spec.version}
        result = await service.submit_spec_validation(spec_id, USER_ID, "Validator", payload)
        await get_application_persistence_port().flush(db)
        spec = await service.get_spec(spec_id)
        assert result["spec_status"] == "validated"
        assert result["decomposition_ids"] == [review["id"]]
        assert spec.evaluations == [review]
        assert spec.validations[0]["reviewer_id"] == USER_ID


@pytest.mark.asyncio
async def test_current_rejection_survives_joint_approval(db_factory, monkeypatch):
    _install_current_external_requirement_lint(monkeypatch)
    async with db_factory() as db:
        _, spec_id = await _seed_board_and_spec(db)
        service = SpecService(db)
        rejected = await service.submit_spec_evaluation(
            spec_id, "independent-reviewer", "Independent reviewer", decomposition_payload("reject"),
        )
        await get_application_persistence_port().flush(db)
        spec = await service.get_spec(spec_id)
        result = await service.submit_spec_validation(spec_id, USER_ID, "Validator", {
            **_validation_payload(), "expected_spec_version": spec.version,
            "decomposition": decomposition_payload(),
        })
        await get_application_persistence_port().flush(db)
        spec = await service.get_spec(spec_id)
        assert result["spec_status"] == "approved"
        assert result["planning_blockers"] == ["decomposition_rejected"]
        assert rejected["id"] in result["decomposition_ids"]
        assert len(spec.evaluations) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("auto_validate,skip,allowed", [(False, False, False), (True, False, True), (False, True, True)])
async def test_direct_promotion_cannot_bypass_enabled_decomposition(
    db_factory, monkeypatch, auto_validate, skip, allowed,
):
    _install_current_external_requirement_lint(monkeypatch)
    async with db_factory() as db:
        _, spec_id = await _seed_board_and_spec(db, board_settings={
            "require_spec_validation": False, "auto_validate": auto_validate,
        })
        spec = await db.get(Spec, spec_id)
        spec.skip_qualitative_validation = skip
        for key, value in _direct_spec_context_fields(spec_id).items():
            setattr(spec, key, value)
        service = SpecService(db)
        if allowed:
            result = await service.move_spec(spec_id, USER_ID, SpecMove(status=SpecStatus.VALIDATED))
            assert result.status == SpecStatus.VALIDATED
            assert not result.evaluations
        else:
            with pytest.raises(ValueError, match="no evaluation"):
                await service.move_spec(spec_id, USER_ID, SpecMove(status=SpecStatus.VALIDATED))
            assert spec.status == SpecStatus.APPROVED


@pytest.mark.asyncio
async def test_composed_permission_failure_writes_neither_review():
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.application.use_cases.authorization import PermissionDeniedError
    from okto_pulse.core.application.use_cases.submit_spec_validation import (
        SubmitSpecValidationCommand, SubmitSpecValidationUseCase,
    )
    from test_spec_validation_pinpoint_snapshots import _Uow, _payload

    uow = _Uow()
    actor = ActorContext("validator", "mcp", board_id="board-1", actor_kind="agent",
                         permissions=["spec.read", "spec.update", "spec.validation.submit"])
    with pytest.raises(PermissionDeniedError, match="spec.evaluations.submit"):
        await SubmitSpecValidationUseCase().execute(
            SubmitSpecValidationCommand("spec-1", {**_payload(), "decomposition": decomposition_payload()}),
            actor=actor, uow=uow,
        )
    assert uow.specs.submitted is None
    assert uow.commit_calls == 0


@pytest_asyncio.fixture
async def composition_client(db_factory, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from okto_pulse.community.api import auth_deps
    from okto_pulse.community.api.specs import router
    from okto_pulse.core.infra.database import get_db

    _install_current_external_requirement_lint(monkeypatch)
    async with db_factory() as db:
        _, spec_id = await _seed_board_and_spec(db)
        await db.commit()

    async def override_db():
        async with db_factory() as db:
            yield db

    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[auth_deps.require_user] = lambda: USER_ID
    app.dependency_overrides[auth_deps.get_realm_id] = lambda: "local"
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client, spec_id


@pytest.mark.asyncio
@pytest.mark.parametrize("recommendation,status", [("approve", "validated"), ("reject", "approved")])
async def test_rest_composition_persists_both_native_histories(
    composition_client, db_factory, recommendation, status,
):
    client, spec_id = composition_client
    response = client.post(f"/api/v1/specs/{spec_id}/validation", json={
        **_validation_payload(), "decomposition": decomposition_payload(recommendation),
    })
    assert response.status_code == 201, response.text
    result = response.json()
    assert result["spec_status"] == status
    assert result["outcome"] == "success"
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        assert spec.status.value == status
        assert len(spec.validations) == len(spec.evaluations) == 1
        assert spec.evaluations[0]["evaluator_type"] == "user"
        assert spec.evaluations[0]["evaluator_id"] == spec.validations[0]["reviewer_id"] == USER_ID
        assert result["decomposition_ids"] == [spec.evaluations[0]["id"]]
        assert result["validation_id"] == spec.validations[0]["id"]


@pytest.mark.asyncio
async def test_rest_technical_failure_rolls_back_both_reviews_and_promotion(
    composition_client, db_factory, monkeypatch,
):
    client, spec_id = composition_client

    async def fail_history(*args, **kwargs):
        raise RuntimeError("injected history outage after both native appends")

    monkeypatch.setattr(SpecService, "_record_history", fail_history)
    response = client.post(f"/api/v1/specs/{spec_id}/validation", json={
        **_validation_payload(), "decomposition": decomposition_payload(),
    })
    assert response.status_code == 500, response.text
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        assert spec.status == SpecStatus.APPROVED
        assert not spec.validations
        assert not spec.evaluations
        assert spec.current_validation_id is None


@pytest.mark.asyncio
async def test_rest_rejects_forged_decomposition_identity_without_writes(
    composition_client, db_factory,
):
    client, spec_id = composition_client
    response = client.post(f"/api/v1/specs/{spec_id}/validation", json={
        **_validation_payload(),
        "decomposition": {**decomposition_payload(), "evaluator_id": "someone-else"},
    })
    assert response.status_code == 422, response.text
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        assert spec.status == SpecStatus.APPROVED
        assert not spec.validations and not spec.evaluations
