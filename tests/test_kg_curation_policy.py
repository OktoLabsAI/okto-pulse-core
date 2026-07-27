"""MKG-C C2 — CurationPolicy pure gate (scenario S6).

Real-inventory classification, fail-closed default for unknown operations,
auto for the deterministic decay tick, structured refusal payload, and
purity (no I/O, immutable mapping).
"""

from __future__ import annotations

import pytest

from okto_pulse.core.kg.curation_policy import (
    CURATION_LEVEL_AUTO,
    CURATION_LEVEL_FORBIDDEN,
    CURATION_LEVEL_PROPOSE_ONLY,
    CURATION_POLICY,
    CurationPolicyError,
    require_curation_allowed,
    resolve_curation_level,
)

_REAL_INVENTORY = {
    "kg_dedup_entities": CURATION_LEVEL_PROPOSE_ONLY,
    "kg_unmerge": CURATION_LEVEL_PROPOSE_ONLY,
    "kg_backfill_apply": CURATION_LEVEL_PROPOSE_ONLY,
    "kg_restore": CURATION_LEVEL_PROPOSE_ONLY,
    "kg_reset": CURATION_LEVEL_PROPOSE_ONLY,
    "kg_dlq_reprocess": CURATION_LEVEL_PROPOSE_ONLY,
    "kg_connectivity_dlq_reprocess": CURATION_LEVEL_PROPOSE_ONLY,
    "kg_rebuild_run": CURATION_LEVEL_PROPOSE_ONLY,
    "kg_decay_tick": CURATION_LEVEL_AUTO,
    "kg_dedup_hard_delete": CURATION_LEVEL_FORBIDDEN,
}


def test_s6_real_inventory_fully_classified():
    for operation, expected in _REAL_INVENTORY.items():
        assert resolve_curation_level(operation) == expected, operation
    # The policy contains exactly the audited inventory — additions must be
    # deliberate (a new operation defaults to propose_only anyway).
    assert dict(CURATION_POLICY) == _REAL_INVENTORY


def test_s6_unknown_operation_fails_closed_to_propose_only():
    assert resolve_curation_level("kg_operacao_nova") == CURATION_LEVEL_PROPOSE_ONLY
    with pytest.raises(CurationPolicyError):
        require_curation_allowed("kg_operacao_nova")
    # With an explicit confirmation artifact it may proceed.
    assert (
        require_curation_allowed("kg_operacao_nova", confirmed=True)
        == CURATION_LEVEL_PROPOSE_ONLY
    )


def test_s6_forbidden_always_refuses_even_confirmed():
    with pytest.raises(CurationPolicyError) as excinfo:
        require_curation_allowed("kg_dedup_hard_delete", confirmed=True)
    err = excinfo.value
    assert err.code == "curation_policy_violation"
    assert err.operation == "kg_dedup_hard_delete"
    assert err.level == CURATION_LEVEL_FORBIDDEN
    assert "rebuild" in err.remediation


def test_s6_propose_only_requires_confirmation():
    with pytest.raises(CurationPolicyError) as excinfo:
        require_curation_allowed("kg_dedup_entities")
    assert excinfo.value.level == CURATION_LEVEL_PROPOSE_ONLY
    assert "--confirm" in excinfo.value.remediation
    assert (
        require_curation_allowed("kg_dedup_entities", confirmed=True)
        == CURATION_LEVEL_PROPOSE_ONLY
    )


def test_s6_auto_runs_unattended():
    assert require_curation_allowed("kg_decay_tick") == CURATION_LEVEL_AUTO


def test_s6_policy_mapping_is_immutable():
    with pytest.raises(TypeError):
        CURATION_POLICY["kg_dedup_hard_delete"] = CURATION_LEVEL_AUTO  # type: ignore[index]
