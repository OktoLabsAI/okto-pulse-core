from copy import deepcopy

import pytest

from okto_pulse.core.ports.historical_archive_authority import HistoricalArchivePresetFacts
from okto_pulse.core.ports.permission_policy import (
    PermissionSet, flatten_permission_flags, legacy_permissions_to_flags, registered_permission_flags,
    resolve_effective_permissions,
)
from okto_pulse.core.ports.permission_retirement import (
    PermissionRetirementParityError, capture_permission_retirement_authority,
    parse_permission_retirement_authority, require_permission_retirement_parity, retire_permission_document,
)
from test_historical_archive_authority_v034 import GOLDEN, RETIRED


def test_closed_retirement_policy_matches_frozen_authority():
    from okto_pulse.core.ports.permission_retirement import retired_feature_permission_flags
    flags = retired_feature_permission_flags()
    assert flags == tuple(sorted(RETIRED))
    assert len(flags) == 61
    assert sum(flag.startswith("sprint.") for flag in flags) == 33


@pytest.mark.parametrize("drift", ["missing", "added"])
def test_closed_retirement_policy_does_not_authorize_registry_drift(monkeypatch, drift):
    from okto_pulse.core.ports import permission_retirement as policy
    registry = deepcopy(registered_permission_flags())
    if drift == "missing":
        del registry["board"]["read"]
    else:
        registry["unapproved"] = {"read": True}
    monkeypatch.setattr(policy, "registered_permission_flags", lambda: registry)
    with pytest.raises(ValueError):
        policy.retired_feature_permission_flags()


def capture(flags=None, *, legacy=None, preset=None, presets=(), overrides=None):
    return capture_permission_retirement_authority(agent_flags=flags, legacy_permissions=legacy,
        preset_id=preset, presets=presets, board_overrides=overrides)


def test_retirement_preserves_surviving_values_extensions_and_source_tree():
    original = deepcopy(GOLDEN["layers"]["full"])
    original["kg"]["operations"]["tick"]["extension"] = {"run": False}
    original["kg.operations.tick.run"] = "literal extension"
    before = deepcopy(original)
    result = retire_permission_document(original, retired_flags=tuple(sorted(RETIRED)))
    assert original == before
    assert set(result.removed_paths) == RETIRED
    assert result.document["kg"]["operations"]["tick"] == {"extension": {"run": False}}
    assert result.document["kg.operations.tick.run"] == "literal extension"
    assert result.document["board"] == original["board"]
    assert retire_permission_document(result.document, retired_flags=tuple(sorted(RETIRED))).removed_paths == ()


@pytest.mark.parametrize("document", [None, {}, [], False, "invalid"])
def test_retirement_does_not_reinterpret_empty_or_malformed_root(document):
    result = retire_permission_document(document, retired_flags=tuple(sorted(RETIRED)))
    assert result.document == document and result.removed_paths == ()


def test_only_entirely_retired_malformed_branch_can_disappear():
    result = retire_permission_document({"kg": {"operations": {"tick": 1}}}, retired_flags=tuple(sorted(RETIRED)))
    assert result.document == {} and result.removed_paths == ("kg.operations.tick",)
    result = retire_permission_document({"kg": False}, retired_flags=tuple(sorted(RETIRED)))
    assert result.document == {"kg": False} and result.removed_paths == ()


def check(source, candidate, retired=tuple(sorted(RETIRED))):
    require_permission_retirement_parity(source, candidate, retired_flags=retired)


def test_original_full_control_retains_all_surviving_decisions_and_exact_review_state():
    source = capture(GOLDEN["layers"]["full"])
    assert len(source.decisions) == 599
    assert all(allowed for _, allowed in source.decisions)
    assert not source.owner_review_required and source.review_reason is None
    check(source, resolve_effective_permissions(None, None, None))
    assert parse_permission_retirement_authority(source.document()) == source


@pytest.mark.parametrize("value", [False, None, 1])
@pytest.mark.parametrize("branch,leaf", [("node", "boost"), ("schema", "migrate"), ("integrity", "backfill"), ("integrity", "reconcile"), ("integrity", "read"), ("historical", "start"), ("historical", "cancel"), ("historical", "read"), ("settings", "read"), ("settings", "write"), ("queue", "read"), ("queue", "reprocess")])
def test_retired_schema_authority_cannot_promote_partial_original_full_control(value, branch, leaf):
    from okto_pulse.core.ports.permission_policy import resolve_agent_permission_facts

    document = deepcopy(GOLDEN["layers"]["full"])
    if value is None:
        del document["kg"]["operations"][branch][leaf]
    else:
        document["kg"]["operations"][branch][leaf] = value
    source = capture(document)
    assert source.owner_review_required
    candidate = resolve_agent_permission_facts(
        agent_flags=document, legacy_permissions=None, preset_id=None,
        presets=(), board_overrides=None,
    )
    assert candidate.owner_review_required
    assert not any(candidate.has(path) for path in flatten_permission_flags(registered_permission_flags()))
    check(source, candidate)


def test_smaller_full_control_lookalike_cannot_become_trusted_by_deleting_flags():
    source = capture(registered_permission_flags())
    assert source.owner_review_required
    assert source.review_reason == "unrecognized_direct_permissions"
    assert not any(allowed for _, allowed in source.decisions)
    with pytest.raises(PermissionRetirementParityError) as error:
        check(source, resolve_effective_permissions(None, None, None))
    assert error.value.review_changed
    assert set(error.value.changed_flags) == set(flatten_permission_flags(registered_permission_flags()))


@pytest.mark.parametrize("value", [False, 1, "invalid"])
@pytest.mark.parametrize("branch", ["integrity", "historical", "settings", "queue"])
def test_retired_parent_does_not_resolve_historical_owner_review(value, branch):
    from okto_pulse.core.ports.permission_policy import resolve_agent_permission_facts

    document = deepcopy(GOLDEN["layers"]["full"])
    document["kg"]["operations"][branch] = value
    source = capture(document)
    candidate = resolve_agent_permission_facts(
        agent_flags=document, legacy_permissions=None, preset_id=None,
        presets=(), board_overrides=None,
    )
    assert source.owner_review_required and candidate.owner_review_required
    check(source, candidate)


def test_an_all_denied_rewrite_does_not_silently_resolve_owner_review():
    source = capture({})
    denied = resolve_effective_permissions({}, None, None, owner_review_required=True,
        review_reason=source.review_reason)
    check(source, denied)
    denied.owner_review_required = False
    denied.review_reason = None
    with pytest.raises(PermissionRetirementParityError) as error:
        check(source, denied)
    assert error.value.changed_flags == ()
    assert error.value.review_changed


@pytest.mark.parametrize("layer", ["preset", "board"])
def test_malformed_retired_layer_preserves_review_even_after_its_last_operation_is_removed(layer):
    malformed = {"kg": {"operations": {"tick": {"run": 1}}}}
    source = capture(preset="base", presets=(HistoricalArchivePresetFacts("base", malformed if layer == "preset" else {}),),
        overrides=malformed if layer == "board" else None)
    assert source.owner_review_required
    assert not any(allowed for _, allowed in source.decisions)
    with pytest.raises(PermissionRetirementParityError):
        check(source, resolve_effective_permissions(None, registered_permission_flags(), None))


def test_empty_legacy_list_keeps_reads_without_creating_admin_or_write_grants():
    source = capture(legacy=[])
    decisions = dict(source.decisions)
    assert decisions["card.entity.read"]
    assert not decisions["board.admin.delete"] and not decisions["card.entity.edit_fields"]
    check(source, resolve_effective_permissions(legacy_permissions_to_flags([]), None, None))


def test_board_denial_cannot_be_lost_or_copied_to_global_context():
    global_source = capture()
    board_source = capture(overrides={"card": {"entity": {"edit_fields": False}}})
    with pytest.raises(PermissionRetirementParityError) as error:
        check(board_source, resolve_effective_permissions(None, None, None))
    assert "card.entity.edit_fields" in error.value.changed_flags
    # A sparse Board ceiling also denies absent introduced capabilities. Those
    # old denials must survive; it never meant only one explicit False leaf.
    assert "agent.api_key.rotate" in error.value.changed_flags
    assert not error.value.review_changed
    with pytest.raises(PermissionRetirementParityError):
        check(global_source, resolve_effective_permissions(None, None, {"card": {"entity": {"edit_fields": False}}}))


@pytest.mark.parametrize("retired", [(), ("not.registered",), (*sorted(RETIRED), "card.entity.edit_fields"), (*sorted(RETIRED), "kg.operations.tick.run")])
def test_caller_cannot_omit_a_surviving_flag_or_compare_only_convenient_flags(retired):
    with pytest.raises(ValueError, match="permission_retirement_(flags_invalid|registry_mismatch)"):
        check(capture(), PermissionSet({}), retired)


@pytest.mark.parametrize("mutation", ["format", "source_blob", "extension", "missing", "integer", "review", "reason", "review_grant"])
def test_serialized_evidence_is_closed_and_strict(mutation):
    value = capture().document()
    if mutation in {"format", "source_blob"}:
        value[mutation] = "unknown"
    elif mutation == "extension":
        value["extra"] = True
    elif mutation == "missing":
        value["decisions"].pop("card.entity.read")
    elif mutation == "integer":
        value["decisions"]["card.entity.read"] = 1
    elif mutation == "review":
        value["owner_review_required"] = 0
    elif mutation == "reason":
        value["review_reason"] = "invented"
    else:
        value["owner_review_required"] = True
        value["review_reason"] = "invalid_agent_flags"
    with pytest.raises(ValueError, match="authority_invalid"):
        parse_permission_retirement_authority(value)


def test_evidence_is_detached_from_inputs_and_exported_documents():
    flags = deepcopy(GOLDEN["layers"]["full"])
    source = capture(flags)
    flags.clear()
    exported = source.document()
    exported["decisions"].clear()
    assert len(source.decisions) == 599


@pytest.mark.parametrize("path", [
    "runtime.settings.read", "runtime.settings.write", "metrics.local.summary.read",
    "metrics.local.purge", "amendment.coverage.confirm",
])
@pytest.mark.parametrize("value", [False, None, 1])
def test_runtime_retirement_requires_complete_original_operational_generation(path, value):
    from okto_pulse.core.ports.permission_policy import resolve_agent_permission_facts

    document = deepcopy(GOLDEN["layers"]["full"])
    parts = path.split(".")
    parent = document
    for part in parts[:-1]:
        parent = parent[part]
    if value is None:
        del parent[parts[-1]]
    else:
        parent[parts[-1]] = value
    source = capture(document)
    candidate = resolve_agent_permission_facts(
        agent_flags=document, legacy_permissions=None, preset_id=None,
        presets=(), board_overrides=None,
    )
    assert source.owner_review_required and candidate.owner_review_required
    check(source, candidate)


@pytest.mark.parametrize("path", ["runtime", "runtime.settings"])
@pytest.mark.parametrize("value", [False, 1, "invalid", {}, {"extension": True}])
def test_runtime_retirement_preserves_malformed_and_extended_parent_review(path, value):
    from okto_pulse.core.ports.permission_policy import resolve_agent_permission_facts

    document = deepcopy(GOLDEN["layers"]["full"])
    if path == "runtime":
        document["runtime"] = value
    else:
        document["runtime"]["settings"] = value
    source = capture(document)
    candidate = resolve_agent_permission_facts(
        agent_flags=document, legacy_permissions=None, preset_id=None,
        presets=(), board_overrides=None,
    )
    assert source.owner_review_required and candidate.owner_review_required
    check(source, candidate)
