"""Code Traceability settings and fail-closed permission contracts."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.permissions import (
    ALL_FLAGS,
    CODE_TRACEABILITY_PERMISSION_INTRODUCTION_V1,
    PERMISSION_INTRODUCTION_MANIFESTS,
    PermissionSet,
)
from okto_pulse.core.domain.code_traceability import (
    CodeTraceabilityEnforcement,
)
from okto_pulse.core.models.schemas import (
    BoardSettings,
    BoardUpdate,
    CodeTraceabilitySettings,
)
from okto_pulse.core.services.code_traceability_gate import (
    resolve_code_traceability_settings,
)
from okto_pulse.core.services.board_governance import BoardGovernanceService
from okto_pulse.core.services.default_board_configuration import (
    DefaultBoardConfigurationService,
)


def _flags(*paths: str) -> dict[str, object]:
    result: dict[str, object] = {}
    for path in paths:
        current = result
        parts = path.split(".")
        for part in parts[:-1]:
            child = current.setdefault(part, {})
            assert isinstance(child, dict)
            current = child
        current[parts[-1]] = True
    return result


def test_traceability_defaults_advisory_and_authored_policy_is_closed() -> None:
    default_policy = BoardSettings().code_traceability
    assert default_policy.mode is CodeTraceabilityEnforcement.ADVISORY
    policy = BoardSettings(code_traceability={"mode": "advisory"}).code_traceability
    assert policy.mode is CodeTraceabilityEnforcement.ADVISORY
    assert policy.evidence_attestation == "preferred"
    assert policy.target_resolution == "advisory"
    assert policy.preflight_freshness_seconds == 1800

    with pytest.raises(ValidationError):
        CodeTraceabilitySettings(mode="verified")  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        CodeTraceabilitySettings(mode="off")  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        BoardUpdate.model_validate({"settings": {"code_traceability": {"mode": "off"}}})
    with pytest.raises(ValidationError):
        CodeTraceabilitySettings(repository_url="https://example.invalid/repo")

    assert {
        BoardUpdate.model_validate(
            {"settings": {"code_traceability": {"mode": mode}}}
        ).settings.code_traceability.mode
        for mode in ("advisory", "blocking")
    } == {
        CodeTraceabilityEnforcement.ADVISORY,
        CodeTraceabilityEnforcement.BLOCKING,
    }


@pytest.mark.parametrize(
    "legacy",
    [None, {}, {"code_traceability": None}, {"code_traceability": {"mode": "off"}}],
)
def test_legacy_absence_null_and_off_resolve_to_advisory(legacy) -> None:
    resolved = resolve_code_traceability_settings(legacy)
    assert resolved.mode is CodeTraceabilityEnforcement.ADVISORY


def test_unrelated_board_patch_upgrades_legacy_off_but_authored_off_is_rejected() -> (
    None
):
    merged = BoardGovernanceService.merge_settings_patch(
        {"code_traceability": {"mode": "off"}, "max_scenarios_per_card": 3},
        {"max_scenarios_per_card": 5},
    )
    assert merged["code_traceability"]["mode"] == "advisory"
    assert merged["max_scenarios_per_card"] == 5

    with pytest.raises(ValidationError):
        BoardGovernanceService.merge_settings_patch(
            {"code_traceability": {"mode": "advisory"}},
            {"code_traceability": {"mode": "off"}},
        )


@pytest.mark.parametrize("value", [59, 86_401])
def test_preflight_freshness_rejects_client_widening(value: int) -> None:
    with pytest.raises(ValidationError):
        CodeTraceabilitySettings(preflight_freshness_seconds=value)


@pytest.mark.asyncio
async def test_new_board_and_template_defaults_are_forward_only_advisory(
    db_factory,
) -> None:
    scope = "code-traceability-defaults"
    async with db_factory() as db:
        service = DefaultBoardConfigurationService(db)
        fallback, snapshot = await service.build_snapshot_for_create(
            applied_by="agent-defaults",
            scope=scope,
        )
        assert snapshot is None
        assert fallback["code_traceability"]["mode"] == "advisory"

        with pytest.raises(ValidationError):
            await service.build_snapshot_for_create(
                settings_override={"code_traceability": {"mode": "off"}},
                applied_by="agent-defaults",
                scope=scope,
            )

        template = await service.create_version(
            settings_payload={},
            actor="agent-defaults",
            scope=scope,
        )
        assert template.settings_payload["code_traceability"]["mode"] == "advisory"


def test_code_traceability_permission_generation_is_fail_closed() -> None:
    manifest = CODE_TRACEABILITY_PERMISSION_INTRODUCTION_V1
    assert manifest in PERMISSION_INTRODUCTION_MANIFESTS
    assert manifest.version == "CODE-TRACEABILITY/v1"
    assert manifest.legacy_compatible is False
    assert len(manifest.leaves) == 22
    assert len(ALL_FLAGS) == 587
    assert set(manifest.leaves) <= set(ALL_FLAGS)
    assert set(dict(manifest.historical_authorities)) == set(manifest.leaves)

    operation = "code_traceability.investigation.receipt_submit"
    authority = manifest.historical_authority_for(operation)
    assert authority == "agent.entity.read"
    assert PermissionSet({}).has(operation) is False
    assert PermissionSet(_flags(operation)).has(operation) is False
    assert PermissionSet(_flags(authority)).has(operation) is False
    assert PermissionSet(_flags(operation, authority)).has(operation) is True


def test_only_the_five_preexisting_migration_manifests_allow_legacy_fallback() -> None:
    assert [
        manifest.version
        for manifest in PERMISSION_INTRODUCTION_MANIFESTS
        if manifest.legacy_compatible
    ] == [
        "ADMIN-CATALOG/v1",
        "OPERATIONAL/v1",
        "MCP-GAPS/v1",
        "KG-OPERATIONS/v1",
        "SDLC-TRANSITIONS/v1",
    ]
