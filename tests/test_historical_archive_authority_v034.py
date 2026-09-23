import ast
from copy import deepcopy
from dataclasses import astuple
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

from okto_pulse.core.ports.historical_archive_authority import HistoricalArchivePresetFacts, capture_authenticated_human_sections_v034, resolve_historical_archive_sections_v034
from okto_pulse.core.ports.permission_policy import flatten_permission_flags, normalize_agent_permission_layer, registered_permission_flags

GOLDEN = json.loads((Path(__file__).parent / "fixtures/historical_archive_authority_v034.json").read_text(encoding="utf-8"))
READS = ("sprint.entity.read", "sprint.qa.read", "sprint.evaluations.read", "sprint.history_read")
RETIRED = frozenset(path for path in (
    "kg.operations.integrity.backfill",
    "kg.operations.integrity.read",
    "kg.operations.integrity.reconcile",
    "kg.operations.schema.migrate",
    "kg.operations.rebuild.preflight", "kg.operations.rebuild.confirm", "kg.operations.rebuild.run",
    "kg.operations.global_recovery.preflight", "kg.operations.global_recovery.confirm", "kg.operations.global_recovery.read",
    "kg.operations.global_recovery.cancel", "kg.operations.global_recovery.resume", "kg.operations.global_recovery.run",
    "kg.operations.quarantine.restore", "kg.operations.global_outbox.read", "kg.operations.global_outbox.reprocess",
    "kg.operations.global_outbox.verify", "kg.operations.tick.run",
)) | frozenset(flatten_permission_flags({'sprint': GOLDEN['layers']['full']['sprint']}))


def put(document, path, value):
    current = document
    parts = path.split(".")
    for key in parts[:-1]:
        current = current.setdefault(key, {})
    current[parts[-1]] = value


def mask_flags(mask):
    result = {}
    for index, path in enumerate(READS):
        put(result, path, bool(mask & (1 << index)))
    return result


@pytest.mark.parametrize("case", GOLDEN["matrix"])
def test_every_source_read_and_board_ceiling_matches_the_original_policy(case):
    sections = resolve_historical_archive_sections_v034(agent_flags=None, legacy_permissions=None,
        preset_id="base", presets=(HistoricalArchivePresetFacts("base", mask_flags(case["preset_mask"])),),
        board_overrides=mask_flags(case["board_mask"]))
    assert list(astuple(sections)) == case["sections"]


@pytest.mark.parametrize("case", GOLDEN["cases"])
def test_original_direct_and_legacy_documents_preserve_admission_and_denial(case):
    raw = deepcopy(GOLDEN["layers"][case["direct"]])
    before = deepcopy(raw)
    result = resolve_historical_archive_sections_v034(agent_flags=raw, legacy_permissions=case["legacy"],
        preset_id=None, presets=(), board_overrides=None)
    assert list(astuple(result)) == case["sections"]
    assert raw == before


@pytest.mark.parametrize("presets,preset_id", [
    ((), "missing"),
    ((HistoricalArchivePresetFacts("one", {}, "missing"),), "one"),
    ((HistoricalArchivePresetFacts("one", {}, "two"), HistoricalArchivePresetFacts("two", {}, "one")), "one"),
    ((HistoricalArchivePresetFacts("one", {}), HistoricalArchivePresetFacts("one", {})), "one"),
    ((HistoricalArchivePresetFacts("one", {"sprint": False}),), "one"),
])
def test_invalid_preset_lineage_cannot_become_legacy_full_control(presets, preset_id):
    result = resolve_historical_archive_sections_v034(agent_flags=None, legacy_permissions=None,
        preset_id=preset_id, presets=presets, board_overrides=None)
    assert astuple(result) == (False,) * 4


def test_frozen_capture_ignores_removal_or_corruption_of_live_registry_and_lifecycle(monkeypatch):
    from okto_pulse.core.domain import permissions, sdlc_registry
    monkeypatch.setattr(permissions, "ALL_FLAGS", [])
    monkeypatch.setattr(permissions, "PERMISSION_REGISTRY", {})
    monkeypatch.setattr(permissions, "PERMISSION_INTRODUCTION_MANIFESTS", ())
    monkeypatch.setattr(sdlc_registry, "SDLC_REGISTRY", {})
    result = resolve_historical_archive_sections_v034(agent_flags=GOLDEN["layers"]["full"], legacy_permissions=None,
        preset_id=None, presets=(), board_overrides={"sprint": {"qa": {"read": False}}})
    assert astuple(result) == (True, False, True, True)
    assert astuple(capture_authenticated_human_sections_v034({})) == (True,) * 4


def test_versioned_source_is_immutable_and_has_no_live_core_or_mechanism_dependencies():
    source = Path(importlib.util.find_spec("okto_pulse.core.domain.historical_permission_policy_v034").origin).read_bytes()
    # A changed historic evaluator needs a new source version, not silent edits.
    assert hashlib.sha256(source.replace(b"\r\n", b"\n")).hexdigest() == GOLDEN["frozen_sha256"]
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom):
            assert node.module in {"__future__", "typing", "dataclasses"}
        elif isinstance(node, ast.Import):
            assert all(alias.name in {"json", "copy"} for alias in node.names)


@pytest.mark.parametrize("remaining", [
    RETIRED,
    frozenset(path for path in RETIRED if ".rebuild." not in path),
    frozenset(path for path in RETIRED if ".global_outbox." in path or ".tick." in path),
    frozenset({"kg.operations.tick.run"}),
])
def test_only_the_complete_original_generation_is_recognized_and_no_operation_is_reintroduced(remaining):
    current = registered_permission_flags()
    for path in remaining:
        put(current, path, True)
    before = deepcopy(current)
    if remaining == RETIRED:
        assert normalize_agent_permission_layer(current) is None
    else:
        # These shapes also describe partial original documents. Local commits
        # alone do not prove which registry authored a persisted snapshot.
        assert normalize_agent_permission_layer(current) is not None
    assert current == before
    from okto_pulse.core.domain.permissions import ALL_FLAGS, PermissionContext, DefaultPermissionPolicy
    for path in remaining:
        assert path not in ALL_FLAGS
        assert not DefaultPermissionPolicy().evaluate(PermissionContext(operation=path, permissions=None)).allowed


@pytest.mark.parametrize("value", [False, None, 1, "true", {}, []])
def test_retired_false_or_malformed_values_never_normalize_to_full_control(value):
    current = registered_permission_flags()
    put(current, "kg.operations.tick.run", value)
    assert normalize_agent_permission_layer(current) is not None


@pytest.mark.parametrize("value", [None, 1, "true", {}, []])
def test_retired_malformed_shapes_still_require_review_with_a_named_preset(value):
    from okto_pulse.core.ports.permission_policy import resolve_effective_permissions
    direct = {}
    put(direct, "kg.operations.tick.run", value)
    effective = resolve_effective_permissions(direct, registered_permission_flags(), None)
    assert effective.owner_review_required
    assert not effective.has("sprint.entity.read")


def test_empty_retired_subtree_does_not_disappear_into_a_full_control_fingerprint():
    current = registered_permission_flags()
    current["kg"]["operations"]["tick"] = {}
    assert normalize_agent_permission_layer(current) is not None


def test_partial_introductions_and_unknown_extensions_stay_unrecognized():
    for variant in ("partial_retired", "partial_live", "unknown"):
        current = registered_permission_flags()
        put(current, "kg.operations.tick.run", True)
        if variant == "partial_retired":
            put(current, "kg.operations.rebuild.preflight", True)
        elif variant == "partial_live":
            current["kg"]["operations"].pop("health")
        else:
            put(current, "kg.operations.unknown.read", True)
        assert normalize_agent_permission_layer(current) is not None
