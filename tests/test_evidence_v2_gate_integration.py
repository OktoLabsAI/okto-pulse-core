"""Evidence V2 reuse across status, whole-spec, card and sprint gates."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy.orm.attributes import flag_modified

from sqlalchemy_test_models import (
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
    Sprint,
    SprintStatus,
)
from okto_pulse.core.models.schemas import CardMove, SpecUpdate, SprintMove
from okto_pulse.core.ports.test_evidence import (
    TestEvidenceWriteVerification as EvidenceWriteVerification,
    register_test_evidence_write_verifier,
    reset_test_evidence_write_verifier_for_tests,
)
from okto_pulse.core.services.gate_contracts import GateContractError
from okto_pulse.core.services.main import CardService, SpecService, SprintService
from okto_pulse.core.services.resource_gate import ResourceGateService
from okto_pulse.core.services.test_scenario_lifecycle import (
    compute_execution_attestation_sha256,
    compute_test_scenario_semantic_sha256,
)


ACTOR = "evidence-v2-gate-agent"


class _TrustedEditionVerifier:
    def __init__(self) -> None:
        self.calls = []

    def verify(self, **_request):  # noqa: ANN003, ANN201
        self.calls.append(_request)
        evidence = _request.get("evidence") or {}
        if evidence.get("execution_receipt") == "unregistered-receipt":
            return EvidenceWriteVerification(
                False, ("evidence_v2.receipt_not_registered",)
            )
        if evidence.get("execution_receipt") == "semantic-bound-receipt":
            attestation = evidence.get("execution_attestation") or {}
            if attestation.get("scenario_sha256") != _request.get(
                "scenario_sha256"
            ):
                return EvidenceWriteVerification(
                    False, ("evidence_v2.scenario_semantic_binding_mismatch",)
                )
        return EvidenceWriteVerification(True)


@pytest.fixture(autouse=True)
def _trusted_edition_verifier():
    verifier = _TrustedEditionVerifier()
    register_test_evidence_write_verifier(verifier)
    yield verifier
    reset_test_evidence_write_verifier_for_tests()


def _evidence(
    scenario_id: str,
    *,
    runtime: bool = True,
    receipt: str = "opaque-installation-receipt",
    scenario_sha256: str = "sha256:" + "c" * 64,
) -> dict:
    manifest_ref = "manifests/evidence-v2-gates.json"
    attestation = {
        "schema_version": 2,
        "run_id": f"run-{scenario_id}",
        "executed_at": "2026-07-14T15:00:00Z",
        "scenario_id": scenario_id,
        "scenario_sha256": scenario_sha256,
        "outcome": "passed",
        "product_runtime_exercised": runtime,
        "manifest_sha256": "sha256:" + "b" * 64,
        "assertions": [
            {
                "name": "runtime-output",
                "expected": "v0.3.0",
                "observed": "v0.3.0",
                "status": "passed",
            }
        ],
        "provenance": {
            "producer": "okto-pulse-community",
            "producer_version": "0.3.0",
            "adapter": "okto_pulse.community.adapters.test_evidence",
            "environment": "pytest",
        },
    }
    attestation["attestation_sha256"] = compute_execution_attestation_sha256(
        attestation, manifest_ref=manifest_ref
    )
    return {
        "evidence_class": "mcp_replay_manifest",
        "manifest_ref": manifest_ref,
        "execution_attestation": attestation,
        "execution_receipt": receipt,
    }


async def _seed(db_factory, *, scenario_status: str = "ready", evidence=None):
    token = uuid.uuid4().hex
    board_id = f"board-v2-{token}"
    spec_id = f"spec-v2-{token}"
    scenario_id = f"ts-v2-{token}"
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Evidence V2", owner_id=ACTOR, settings={}))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Evidence V2",
                status=SpecStatus.IN_PROGRESS,
                created_by=ACTOR,
                functional_requirements=["FR1"],
                acceptance_criteria=[{"id": "ac1", "text": "runtime is real"}],
                test_scenarios=[
                    {
                        "id": scenario_id,
                        "title": "runtime evidence",
                        "status": scenario_status,
                        "linked_criteria": ["ac1"],
                        **({"evidence": evidence} if evidence is not None else {}),
                    }
                ],
            )
        )
        await db.commit()
    return board_id, spec_id, scenario_id


@pytest.mark.asyncio
async def test_status_and_whole_spec_paths_reject_same_runtime_false(db_factory):
    board_id, spec_id, scenario_id = await _seed(db_factory)
    bad = _evidence(scenario_id, runtime=False)
    async with db_factory() as db:
        service = SpecService(db)
        with pytest.raises(ValueError, match="product_runtime_not_exercised"):
            await service.set_test_scenario_status(
                spec_id, ACTOR, scenario_id, "passed", bad
            )

    async with db_factory() as db:
        service = SpecService(db)
        with pytest.raises(ValueError, match="evidence_required"):
            await service.update_spec(
                spec_id,
                ACTOR,
                SpecUpdate(
                    test_scenarios=[
                        {
                            "id": scenario_id,
                            "title": "runtime evidence",
                            "status": "passed",
                            "linked_criteria": ["ac1"],
                            "evidence": bad,
                        }
                    ]
                ),
            )

    # A valid attestation follows the same path and persists losslessly.
    good = _evidence(scenario_id)
    async with db_factory() as db:
        result = await SpecService(db).set_test_scenario_status(
            spec_id, ACTOR, scenario_id, "passed", good
        )
        assert result["new_status"] == "passed"
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        assert spec.test_scenarios[0]["evidence"] == good


@pytest.mark.asyncio
async def test_direct_service_write_fails_closed_without_edition_verifier(db_factory):
    _board_id, spec_id, scenario_id = await _seed(db_factory)
    reset_test_evidence_write_verifier_for_tests()
    async with db_factory() as db:
        with pytest.raises(ValueError, match="concrete_verifier_not_configured"):
            await SpecService(db).set_test_scenario_status(
                spec_id,
                ACTOR,
                scenario_id,
                "passed",
                _evidence(scenario_id),
            )


@pytest.mark.asyncio
async def test_whole_spec_semantic_edit_cannot_replay_old_receipt(db_factory):
    board_id, spec_id, scenario_id = await _seed(db_factory)
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        scenario = spec.test_scenarios[0]
        old_digest = compute_test_scenario_semantic_sha256(
            board_id=board_id,
            spec_id=spec_id,
            scenario=scenario,
            acceptance_criteria=list(spec.acceptance_criteria or []),
        )
        old_evidence = _evidence(
            scenario_id,
            receipt="semantic-bound-receipt",
            scenario_sha256=old_digest,
        )
        scenario["status"] = "passed"
        scenario["evidence"] = old_evidence
        flag_modified(spec, "test_scenarios")
        await db.commit()

    async with db_factory() as db:
        with pytest.raises(ValueError, match="scenario_semantic_binding_mismatch"):
            await SpecService(db).update_spec(
                spec_id,
                ACTOR,
                SpecUpdate(
                    test_scenarios=[
                        {
                            "id": scenario_id,
                            "title": "runtime evidence",
                            "status": "passed",
                            "given": "a new precondition invalidates the old run",
                            "linked_criteria": ["ac1"],
                            "evidence": old_evidence,
                        }
                    ]
                ),
            )


@pytest.mark.asyncio
async def test_card_done_reauthenticates_persisted_v2(
    db_factory, _trusted_edition_verifier
):
    board_id, spec_id, scenario_id = await _seed(db_factory)
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        spec.test_scenarios[0]["status"] = "passed"
        spec.test_scenarios[0]["evidence"] = _evidence(
            scenario_id, receipt="unregistered-receipt"
        )
        flag_modified(spec, "test_scenarios")
        card = Card(
            id=f"card-{uuid.uuid4().hex}",
            board_id=board_id,
            spec_id=spec_id,
            title="Evidence test card",
            status=CardStatus.IN_PROGRESS,
            card_type=CardType.TEST,
            test_scenario_ids=[scenario_id],
            created_by=ACTOR,
        )
        db.add(card)
        await db.commit()
        card_id = card.id
        resource_gate = ResourceGateService(db)
        for resource_type in ("architecture", "mockup", "knowledge_base"):
            await resource_gate.mark_not_applicable(
                board_id,
                "card",
                card_id,
                resource_type,
                ACTOR,
                justification=f"{resource_type} is not applicable to this gate test.",
                source_channel="ui",
            )
        with pytest.raises(GateContractError):
            await CardService(db).move_card(
                card_id,
                ACTOR,
                CardMove(
                    status=CardStatus.DONE,
                    conclusion="The card cannot close with unverified evidence.",
                    completeness=100,
                    completeness_justification="Implementation is otherwise complete.",
                    drift=0,
                    drift_justification="No scope drift.",
                ),
            )
        assert _trusted_edition_verifier.calls
        assert _trusted_edition_verifier.calls[-1]["actor_id"] is None
        assert _trusted_edition_verifier.calls[-1]["scenario_sha256"].startswith(
            "sha256:"
        )


@pytest.mark.asyncio
async def test_sprint_close_reauthenticates_persisted_v2(
    db_factory, _trusted_edition_verifier
):
    board_id, spec_id, scenario_id = await _seed(db_factory)
    sprint_id = f"sprint-{uuid.uuid4().hex}"
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        spec.test_scenarios[0]["status"] = "passed"
        spec.test_scenarios[0]["evidence"] = _evidence(
            scenario_id, receipt="unregistered-receipt"
        )
        flag_modified(spec, "test_scenarios")
        db.add(
            Sprint(
                id=sprint_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Evidence V2 sprint",
                status=SprintStatus.REVIEW,
                created_by=ACTOR,
                test_scenario_ids=[scenario_id],
                skip_qualitative_validation=True,
            )
        )
        db.add(
            Card(
                id=f"sprint-card-{uuid.uuid4().hex}",
                board_id=board_id,
                spec_id=spec_id,
                sprint_id=sprint_id,
                title="Finished test card",
                status=CardStatus.DONE,
                card_type=CardType.TEST,
                test_scenario_ids=[scenario_id],
                created_by=ACTOR,
            )
        )
        await db.commit()
        with pytest.raises(ValueError, match="scoped gate blocker"):
            await SprintService(db).move_sprint(
                sprint_id, ACTOR, SprintMove(status=SprintStatus.CLOSED)
            )
        assert _trusted_edition_verifier.calls
        assert _trusted_edition_verifier.calls[-1]["actor_id"] is None
