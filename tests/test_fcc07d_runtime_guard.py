"""FCC-07D-IMP2 runtime-composition provider guard tests.

Covers the closed D-IMP2 scope: the guard that validates a live
``RuntimeComposition`` (or an injected, abstract provider report) by consuming
the D-IMP1 policy — fail-closed, deterministic, never importing Community.

* AC3 — a production ``RuntimeComposition`` wiring a real test-only provider
  (``okto_pulse.core.kg.providers.testing.*``) is REJECTED (``blocked``) before
  being declared ready, with provider_key + module + composition_path.
* AC4 — Community wiring covered by an INJECTED report: no test-only provider is
  registered as a productive fallback; the core validates only the injected
  observations and imports no ``okto_pulse.community``.
* AC6 — the report is consumable by FCC-07E: violations filterable by
  provider_key / module / composition_path.
* TR4 — the SAME production composition under an explicit ``test`` context is not
  blocked (only the context flips the verdict).
* Schema (``api_504fbaca``) — exact fields + summary.
* Determinism — same input -> byte-identical report; violations ordered.
* No-Community-import — AST source scan of the guard module.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import okto_pulse.core.application.boundary.runtime_composition_guard as _guard_mod
from okto_pulse.core.application.boundary.conformance_matrix import (
    TESTING_PROVIDER_PREFIX,
)
from okto_pulse.core.application.boundary.runtime_composition_guard import (
    InvalidContextError,
    InvalidReportShapeError,
    RuntimeProviderObservation,
    observe_runtime_composition,
    validate_injected_providers,
    validate_runtime_composition_providers,
)
from okto_pulse.core.composition import RuntimeComposition

# A REAL sanctioned test-only provider: its ``__module__`` is
# ``okto_pulse.core.kg.providers.testing.memory_event_bus``, which is under the
# single-source-of-truth TESTING_PROVIDER_PREFIX. Importing it proves the guard
# operates over a genuine test-only namespace, not a hand-built string.
from okto_pulse.core.kg.providers.testing.memory_event_bus import InMemoryEventBus

GUARD_PY = Path(_guard_mod.__file__).resolve()

_TESTING_EVENT_BUS_MODULE = "okto_pulse.core.kg.providers.testing.memory_event_bus"


class _ProductionSettings:
    """A plain production-namespace stand-in (its ``__module__`` is this test
    module, which is NOT under the test-only namespace)."""


def _synthetic_production_composition() -> RuntimeComposition:
    """Build a RuntimeComposition WITHOUT importing Community.

    Required slots are filled with production-namespace objects, EXCEPT
    ``event_bus`` which is a real test-only ``InMemoryEventBus`` — exactly the
    forbidden case AC3 must catch. ``_ProductionSettings`` instances have
    ``__module__`` == this test module (outside the test-only namespace), so they
    classify as production_allowed.
    """
    return RuntimeComposition(
        settings_provider=_ProductionSettings(),
        auth_provider=_ProductionSettings(),
        storage_provider=_ProductionSettings(),
        event_bus=InMemoryEventBus(),  # test-only namespace provider
        uow_factory=_ProductionSettings(),
    )


# --------------------------------------------------------------------------- #
# AC3: production composition with a test-only provider -> blocked + diagnostics
# --------------------------------------------------------------------------- #
def test_ac3_production_composition_with_test_only_provider_is_blocked():
    composition = _synthetic_production_composition()

    report = validate_runtime_composition_providers(composition, context="production")

    # The composition must be REJECTED before being declared ready.
    assert report.overall_status == "blocked"
    assert report.blocked is True
    assert report.context == "production"

    # Exactly one provider (the test-only event_bus) is the violation, with the
    # provider_key, module and composition_path required by AC3.
    assert len(report.violations) == 1
    violation = report.violations[0]
    assert violation.classification == "violation"
    assert violation.provider_key == "event_bus"
    assert violation.module == _TESTING_EVENT_BUS_MODULE
    assert violation.module.startswith(TESTING_PROVIDER_PREFIX + ".")
    assert violation.composition_path == "RuntimeComposition.providers.event_bus"
    assert violation.object_type == "InMemoryEventBus"
    assert violation.reason  # non-empty diagnostic
    assert violation.remediation  # non-empty remediation guidance


def test_ac3_fail_closed_default_context_blocks_without_explicit_context():
    """Omitting the context defaults to production (fail-closed, TR2)."""
    composition = _synthetic_production_composition()

    report = validate_runtime_composition_providers(composition)

    assert report.context == "production"
    assert report.overall_status == "blocked"
    assert report.filter_violations(provider_key="event_bus")


# --------------------------------------------------------------------------- #
# TR4: the SAME composition under an explicit test context is NOT blocked
# --------------------------------------------------------------------------- #
def test_tr4_same_composition_in_test_context_is_not_blocked():
    composition = _synthetic_production_composition()

    report = validate_runtime_composition_providers(composition, context="test")

    assert report.overall_status == "passed"
    assert report.blocked is False
    assert report.violations == ()
    # The test-only provider is now a sanctioned test_only_allowed verdict.
    assert report.test_only_allowed == 1
    test_bus = report.filter_violations(provider_key="event_bus")
    assert test_bus == ()  # no longer a violation
    # Only the context flipped the verdict; the provider identity is unchanged.
    prod = validate_runtime_composition_providers(composition, context="production")
    assert prod.overall_status == "blocked"


# --------------------------------------------------------------------------- #
# AC4: Community wiring via an INJECTED report -> no test-only fallback;
# the core validates only the injected observations, importing no Community.
# --------------------------------------------------------------------------- #
def _community_wiring_observations() -> list[RuntimeProviderObservation]:
    """Injected provider specs standing in for the Community edition's wiring.

    These are abstract observations (plain string module paths) — the core never
    imports ``okto_pulse.community``; it only classifies what it is handed
    (FR4/TR6). Every module is a production namespace: a correct Community wiring
    registers no test-only provider as a productive fallback.
    """
    return [
        RuntimeProviderObservation(
            provider_key="event_bus",
            module="okto_pulse.community.adapters.relational_event_bus",
            object_type="RelationalEventBus",
            composition_path="CommunityComposition.providers.event_bus",
        ),
        RuntimeProviderObservation(
            provider_key="storage_provider",
            module="okto_pulse.community.adapters.sqlalchemy_storage",
            object_type="SqlAlchemyStorage",
            composition_path="CommunityComposition.providers.storage_provider",
        ),
        RuntimeProviderObservation(
            provider_key="auth_provider",
            module="okto_pulse.community.adapters.oidc_auth",
            object_type="OidcAuthProvider",
            composition_path="CommunityComposition.providers.auth_provider",
        ),
    ]


def test_ac4_injected_community_wiring_has_no_test_only_fallback():
    report = validate_injected_providers(
        _community_wiring_observations(), context="production"
    )

    # A correct Community wiring passes: no test-only provider in production.
    assert report.overall_status == "passed"
    assert report.violations == ()
    assert report.test_only_allowed == 0
    assert report.production_allowed == 3
    payload = report.as_dict()
    assert payload["summary"]["test_only_allowed"] == 0
    assert payload["summary"]["violations"] == 0
    assert {v.provider_kind for v in report.verdicts} == {"community_adapter"}


def test_ac4_injected_report_detects_test_only_fallback_when_present():
    """The injected contract is real teeth: a test-only provider smuggled into a
    productive Community fallback is caught with its composition_path."""
    observations = _community_wiring_observations()
    observations.append(
        RuntimeProviderObservation(
            provider_key="graph_store",
            module=f"{TESTING_PROVIDER_PREFIX}.memory_graph_store",
            object_type="InMemoryGraphStore",
            composition_path="CommunityComposition.providers.graph_store",
        )
    )

    report = validate_injected_providers(observations, context="production")

    assert report.overall_status == "blocked"
    offenders = report.filter_violations(
        composition_path="CommunityComposition.providers.graph_store"
    )
    assert len(offenders) == 1
    assert offenders[0].provider_key == "graph_store"
    assert offenders[0].module == f"{TESTING_PROVIDER_PREFIX}.memory_graph_store"
    assert offenders[0].provider_kind == "sanctioned_test_provider"


def test_ac4_core_guard_does_not_import_community():
    """Source scan: the guard module imports nothing from okto_pulse.community."""
    tree = ast.parse(GUARD_PY.read_text(encoding="utf-8"))
    imported_modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module)

    assert not any(
        mod == "okto_pulse.community" or mod.startswith("okto_pulse.community.")
        for mod in imported_modules
    ), f"guard must not import Community; saw: {sorted(imported_modules)}"


# --------------------------------------------------------------------------- #
# AC6: report consumable by FCC-07E — filter by provider_key/module/path
# --------------------------------------------------------------------------- #
def test_ac6_violations_filterable_by_key_module_and_path():
    observations = [
        RuntimeProviderObservation(
            provider_key="event_bus",
            module=f"{TESTING_PROVIDER_PREFIX}.memory_event_bus",
            object_type="InMemoryEventBus",
            composition_path="RuntimeComposition.providers.event_bus",
        ),
        RuntimeProviderObservation(
            provider_key="graph_store",
            module=f"{TESTING_PROVIDER_PREFIX}.memory_graph_store",
            object_type="InMemoryGraphStore",
            composition_path="RuntimeComposition.providers.graph_store",
        ),
            RuntimeProviderObservation(
                provider_key="storage_provider",
                module="okto_pulse.community.adapters.memory",
                object_type="CommunityInMemorySessionStore",
            composition_path="RuntimeComposition.providers.storage_provider",
        ),
    ]

    report = validate_injected_providers(observations, context="production")

    assert report.overall_status == "blocked"
    assert len(report.violations) == 2  # the two test-only providers

    by_key = report.filter_violations(provider_key="graph_store")
    assert len(by_key) == 1
    assert by_key[0].provider_key == "graph_store"

    by_module = report.filter_violations(
        module=f"{TESTING_PROVIDER_PREFIX}.memory_event_bus"
    )
    assert len(by_module) == 1
    assert by_module[0].provider_key == "event_bus"

    by_path = report.filter_violations(
        composition_path="RuntimeComposition.providers.graph_store"
    )
    assert len(by_path) == 1
    assert by_path[0].module == f"{TESTING_PROVIDER_PREFIX}.memory_graph_store"

    # The edition-owned Community provider is never a test-only violation.
    assert report.filter_violations(provider_key="storage_provider") == ()


# --------------------------------------------------------------------------- #
# Schema: the report matches the api_504fbaca contract exactly
# --------------------------------------------------------------------------- #
def test_schema_matches_api_504fbaca_contract():
    composition = _synthetic_production_composition()
    report = validate_runtime_composition_providers(composition, context="production")
    payload = report.as_dict()

    assert set(payload) == {"overall_status", "context", "violations", "summary"}
    assert payload["overall_status"] in {"blocked", "passed"}
    assert payload["context"] in {"production", "test"}

    assert isinstance(payload["violations"], list) and payload["violations"]
    for row in payload["violations"]:
        assert set(row) == {
            "provider_key",
            "module",
            "object_type",
            "composition_path",
            "classification",
            "reason",
            "remediation",
        }
        assert "provider_kind" not in row

    assert set(payload["summary"]) == {
        "violations",
        "production_allowed",
        "test_only_allowed",
    }
    summary = payload["summary"]
    assert summary["violations"] == len(payload["violations"])
    # Counts cover every classified verdict (production providers + the violation):
    # one per supplied provider slot in the composition.
    total = (
        summary["violations"]
        + summary["production_allowed"]
        + summary["test_only_allowed"]
    )
    assert total == len(report.verdicts) == len(composition.provider_keys())


# --------------------------------------------------------------------------- #
# Determinism: same input -> byte-identical report; violations ordered
# --------------------------------------------------------------------------- #
def test_report_is_deterministic_and_violations_sorted_by_path():
    observations = [
        RuntimeProviderObservation(
            provider_key="z",
            module=f"{TESTING_PROVIDER_PREFIX}.zeta",
            composition_path="RuntimeComposition.providers.zeta",
        ),
            RuntimeProviderObservation(
                provider_key="r",
                module="okto_pulse.community.adapters.memory",
            composition_path="RuntimeComposition.providers.real",
        ),
        RuntimeProviderObservation(
            provider_key="a",
            module=f"{TESTING_PROVIDER_PREFIX}.alpha",
            composition_path="RuntimeComposition.providers.alpha",
        ),
    ]

    first = validate_injected_providers(observations, context="production")
    second = validate_injected_providers(list(reversed(observations)), context="production")

    assert first.as_dict() == second.as_dict()
    paths = [v.composition_path for v in first.violations]
    assert paths == sorted(paths)
    # Only the two test-only providers are violations; the Community one is not.
    assert {v.provider_key for v in first.violations} == {"a", "z"}


# --------------------------------------------------------------------------- #
# Reuse proof: the guard consumes the D-IMP1 policy (no reclassification here)
# --------------------------------------------------------------------------- #
def test_guard_consumes_d_imp1_policy_classification():
    # An edition-owned provider OUTSIDE the test-only namespace is production_allowed in both
    # contexts (the policy governs only the test-only namespace) — proving the
    # guard delegates the decision to classify_provider rather than inventing it.
    obs = [
            RuntimeProviderObservation(
                provider_key="storage_provider",
                module="okto_pulse.community.adapters.memory",
            composition_path="RuntimeComposition.providers.storage_provider",
        )
    ]
    for context in ("production", "test"):
        report = validate_injected_providers(obs, context=context)  # type: ignore[arg-type]
        assert report.overall_status == "passed"
        assert report.production_allowed == 1


def test_observe_runtime_composition_is_deterministically_ordered():
    composition = _synthetic_production_composition()
    observations = observe_runtime_composition(composition)
    keys = [obs.provider_key for obs in observations]
    assert keys == sorted(keys)
    # Each observation carries a resolved composition_path under the prefix.
    for obs in observations:
        assert obs.composition_path == f"RuntimeComposition.providers.{obs.provider_key}"


# --------------------------------------------------------------------------- #
# Contract: invalid_context — a context outside {production, test} is rejected at
# BOTH public entry points (api_504fbaca response_error), never a report with an
# arbitrary context.
# --------------------------------------------------------------------------- #
def test_invalid_context_is_rejected_at_both_entry_points():
    obs = [
        RuntimeProviderObservation(
            provider_key="event_bus",
            module=f"{TESTING_PROVIDER_PREFIX}.memory_event_bus",
            composition_path="RuntimeComposition.providers.event_bus",
        )
    ]
    with pytest.raises(InvalidContextError) as exc:
        validate_injected_providers(obs, context="prod")  # type: ignore[arg-type]
    assert exc.value.code == "invalid_context"
    assert "production" in str(exc.value) and "test" in str(exc.value)

    # the live entry point validates the runtime context too (not just the Literal).
    composition = _synthetic_production_composition()
    with pytest.raises(InvalidContextError):
        validate_runtime_composition_providers(
            composition, context="staging"  # type: ignore[arg-type]
        )


# --------------------------------------------------------------------------- #
# Contract: invalid_report_shape — an INJECTED observation missing a mandatory
# field (here composition_path) fails closed; the core never synthesises a path
# for an injected report, so provenance is never silently lost.
# --------------------------------------------------------------------------- #
def test_injected_observation_without_composition_path_is_invalid_report_shape():
    incomplete = [
        RuntimeProviderObservation(
            provider_key="event_bus",
            module=f"{TESTING_PROVIDER_PREFIX}.memory_event_bus",
            # composition_path omitted -> injected report shape is incomplete.
        )
    ]
    with pytest.raises(InvalidReportShapeError) as exc:
        validate_injected_providers(incomplete, context="production")
    assert exc.value.code == "invalid_report_shape"
    assert "composition_path" in str(exc.value)

    # an empty module is likewise rejected (non-empty required).
    with pytest.raises(InvalidReportShapeError):
        validate_injected_providers(
            [
                RuntimeProviderObservation(
                    provider_key="x", module="", composition_path="p"
                )
            ],
            context="production",
        )


def test_live_composition_auto_generates_path_and_never_raises_shape_error():
    # The live path synthesises composition_path inside observe_runtime_composition,
    # so validate_runtime_composition_providers never raises invalid_report_shape
    # and every verdict carries a non-empty composition_path.
    composition = _synthetic_production_composition()
    report = validate_runtime_composition_providers(composition, context="production")
    assert report.overall_status == "blocked"  # the test-only event_bus
    for verdict in report.verdicts:
        assert verdict.composition_path  # auto-generated, never empty
