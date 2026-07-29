"""Evidence V2 adversarial contract and legacy round-trip regressions."""

from __future__ import annotations

from copy import deepcopy

from okto_pulse.core.models.schemas import (
    SpecUpdate,
    TestScenarioEvidence as ScenarioEvidenceModel,
)
from okto_pulse.core.services.test_scenario_lifecycle import (
    compute_execution_attestation_sha256,
    compute_test_scenario_semantic_sha256,
    reexecutable_evidence_reference,
    scenario_has_required_evidence,
    validate_test_scenario_evidence,
    verify_mcp_replay_evidence_v2,
)


def _evidence(*, scenario_id: str = "ts_v2", outcome: str = "passed") -> dict:
    assertion = {
        "name": "about-version",
        "expected": "Community Edition — v0.3.0",
        "observed": "Community Edition — v0.3.0",
        "status": "passed",
    }
    attestation = {
        "schema_version": 2,
        "run_id": "run-evidence-v2-1",
        "executed_at": "2026-07-14T12:00:00-03:00",
        "scenario_id": scenario_id,
        "scenario_sha256": "sha256:" + "c" * 64,
        "outcome": outcome,
        "product_runtime_exercised": True,
        "manifest_sha256": "sha256:" + "a" * 64,
        "assertions": [assertion],
        "provenance": {
            "producer": "okto-pulse-community",
            "producer_version": "0.3.0",
            "adapter": "okto_pulse.community.adapters.test_evidence",
            "environment": "test",
        },
    }
    attestation["attestation_sha256"] = compute_execution_attestation_sha256(
        attestation, manifest_ref="manifests/about-v030.json"
    )
    return {
        "evidence_class": "mcp_replay_manifest",
        "manifest_ref": "manifests/about-v030.json",
        "execution_attestation": attestation,
        "execution_receipt": "opaque-installation-receipt",
    }


def _resign(evidence: dict) -> None:
    evidence["execution_attestation"]["attestation_sha256"] = (
        compute_execution_attestation_sha256(
            evidence["execution_attestation"],
            manifest_ref=evidence["manifest_ref"],
        )
    )


def test_valid_v2_is_typed_and_semantically_verified():
    evidence = _evidence()
    parsed = ScenarioEvidenceModel.model_validate(evidence)
    assert parsed.manifest_ref == "manifests/about-v030.json"
    assert parsed.execution_attestation is not None
    assert parsed.execution_attestation.schema_version == 2
    verdict = verify_mcp_replay_evidence_v2(
        "passed", parsed, scenario_id="ts_v2"
    )
    assert verdict.verified is True
    assert validate_test_scenario_evidence(
        "passed", evidence, for_write=True, scenario_id="ts_v2"
    ) == (True, [])
    reference = reexecutable_evidence_reference(
        {"id": "ts_v2", "status": "passed", "evidence": evidence}
    )
    assert reference.startswith("mcp_replay_manifest:sha256:")
    assert "manifests/about-v030.json" not in reference


def test_reexecutable_reference_keeps_legacy_pointer_and_rejects_weak_status():
    pointer = {
        "id": "ts_pointer",
        "status": "automated",
        "evidence": {
            "evidence_class": "automated_test_pointer",
            "test_file_path": "tests/test_about.py",
            "test_function": "test_version",
        },
    }
    assert reexecutable_evidence_reference(pointer) == (
        "tests/test_about.py::test_version"
    )
    pointer["status"] = "ready"
    assert reexecutable_evidence_reference(pointer) == ""


def test_product_runtime_false_fails_even_with_fresh_valid_digest():
    evidence = _evidence()
    evidence["execution_attestation"]["product_runtime_exercised"] = False
    _resign(evidence)
    verdict = verify_mcp_replay_evidence_v2(
        "passed", evidence, scenario_id="ts_v2"
    )
    assert verdict.verified is False
    assert "evidence_v2.product_runtime_not_exercised" in verdict.reason_codes


def test_passed_observed_expected_contradiction_fails_even_when_resigned():
    evidence = _evidence()
    assertion = evidence["execution_attestation"]["assertions"][0]
    assertion["observed"] = "Community Edition — v0.2.5"
    _resign(evidence)
    verdict = verify_mcp_replay_evidence_v2(
        "passed", evidence, scenario_id="ts_v2"
    )
    assert verdict.verified is False
    assert any("observed_expected_mismatch" in reason for reason in verdict.reason_codes)


def test_tampering_manifest_ref_or_attestation_breaks_envelope_digest():
    evidence = _evidence()
    evidence["manifest_ref"] = "manifests/different.json"
    verdict = verify_mcp_replay_evidence_v2(
        "passed", evidence, scenario_id="ts_v2"
    )
    assert "evidence_v2.attestation_sha256_mismatch" in verdict.reason_codes

    evidence = _evidence()
    evidence["execution_attestation"]["run_id"] = "tampered"
    verdict = verify_mcp_replay_evidence_v2(
        "passed", evidence, scenario_id="ts_v2"
    )
    assert "evidence_v2.attestation_sha256_mismatch" in verdict.reason_codes


def test_scenario_binding_and_provenance_fail_closed():
    evidence = _evidence()
    verdict = verify_mcp_replay_evidence_v2(
        "passed", evidence, scenario_id="another-scenario"
    )
    assert "evidence_v2.scenario_binding_mismatch" in verdict.reason_codes

    evidence = _evidence()
    evidence["execution_attestation"]["provenance"]["producer"] = ""
    _resign(evidence)
    verdict = verify_mcp_replay_evidence_v2(
        "passed", evidence, scenario_id="ts_v2"
    )
    assert "evidence_v2.provenance_producer_required" in verdict.reason_codes


def test_failed_status_requires_a_real_failed_assertion():
    evidence = _evidence(outcome="failed")
    _resign(evidence)
    verdict = verify_mcp_replay_evidence_v2(
        "failed", evidence, scenario_id="ts_v2"
    )
    assert "evidence_v2.failed_requires_failed_assertion" in verdict.reason_codes

    assertion = evidence["execution_attestation"]["assertions"][0]
    assertion["observed"] = "Community Edition — v0.2.5"
    assertion["status"] = "failed"
    _resign(evidence)
    assert verify_mcp_replay_evidence_v2(
        "failed", evidence, scenario_id="ts_v2"
    ).verified


def test_legacy_manifest_string_and_object_round_trip_but_are_unverified():
    for legacy in (
        "manifests/legacy.json",
        {
            "product_runtime_exercised": False,
            "observed_output": "v0.2.5",
            "expected_output_snapshot": "v0.3.0",
        },
    ):
        raw = {
            "evidence_class": "mcp_replay_manifest",
            "mcp_replay_manifest": legacy,
        }
        parsed = ScenarioEvidenceModel.model_validate(raw)
        assert parsed.model_dump(exclude_none=True)["mcp_replay_manifest"] == legacy
        ok, reasons = validate_test_scenario_evidence(
            "passed", raw, for_write=True, scenario_id="ts_v2"
        )
        assert ok is False
        assert "evidence_v2.legacy_mcp_replay_manifest_unverified" in reasons

        update = SpecUpdate(
            test_scenarios=[
                {
                    "id": "ts_v2",
                    "title": "legacy",
                    "status": "passed",
                    "evidence": raw,
                }
            ]
        )
        dumped = update.model_dump(mode="python", exclude_none=True)
        assert dumped["test_scenarios"][0]["evidence"]["mcp_replay_manifest"] == legacy


def test_scenario_gate_uses_same_verifier_for_card_and_sprint_consumers():
    scenario = {
        "id": "ts_v2",
        "title": "About version",
        "status": "passed",
        "evidence": _evidence(),
    }
    assert scenario_has_required_evidence(scenario) is True
    contradicted = deepcopy(scenario)
    contradicted["evidence"]["execution_attestation"]["assertions"][0][
        "observed"
    ] = "v0.2.5"
    _resign(contradicted["evidence"])
    assert scenario_has_required_evidence(contradicted) is False


def test_semantic_digest_binds_identity_type_gwt_and_acceptance_criteria_only():
    scenario = {
        "id": "ts_v2",
        "title": "cosmetic title",
        "notes": "cosmetic note",
        "scenario_type": "e2e",
        "given": "runtime is installed",
        "when": "About opens",
        "then": "v0.3.0 is shown",
        "linked_criteria": ["ac1"],
        "linked_task_ids": ["card-1"],
    }
    criteria = [{"id": "ac1", "text": "About is v0.3.0", "linked_task_ids": []}]
    digest = compute_test_scenario_semantic_sha256(
        board_id="board-1",
        spec_id="spec-1",
        scenario=scenario,
        acceptance_criteria=criteria,
    )
    cosmetic = {
        **scenario,
        "title": "renamed",
        "notes": "new note",
        "linked_task_ids": ["card-2"],
    }
    assert compute_test_scenario_semantic_sha256(
        board_id="board-1",
        spec_id="spec-1",
        scenario=cosmetic,
        acceptance_criteria=[
            {"id": "ac1", "text": "About is v0.3.0", "linked_task_ids": ["card-2"]}
        ],
    ) == digest

    assert compute_test_scenario_semantic_sha256(
        board_id="board-1",
        spec_id="spec-1",
        scenario={**scenario, "scenario_type": "negative"},
        acceptance_criteria=criteria,
    ) != digest

    for changed_scenario, changed_criteria, board_id in (
        ({**scenario, "given": "different precondition"}, criteria, "board-1"),
        (scenario, [{"id": "ac1", "text": "About is v9.9.9"}], "board-1"),
        (scenario, criteria, "board-2"),
    ):
        assert compute_test_scenario_semantic_sha256(
            board_id=board_id,
            spec_id="spec-1",
            scenario=changed_scenario,
            acceptance_criteria=changed_criteria,
        ) != digest


def test_semantic_digest_mismatch_invalidates_an_otherwise_resigned_attestation():
    evidence = _evidence()
    verdict = verify_mcp_replay_evidence_v2(
        "passed",
        evidence,
        scenario_id="ts_v2",
        scenario_sha256="sha256:" + "d" * 64,
    )
    assert verdict.verified is False
    assert "evidence_v2.scenario_semantic_binding_mismatch" in verdict.reason_codes
