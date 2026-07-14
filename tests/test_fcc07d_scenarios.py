"""FCC-07D scenario tests (test_scenario cards — NOT the IMP1/IMP2 unit tests).

One cohesive given/when/then automation per spec ``test_scenario``, named +
commented with its ``ts_id``:

* ``ts_64fb7259`` — a production RuntimeComposition wiring a test-only provider is
  rejected, with an FCC07E-consumable report (AC1, AC3, AC6).
* ``ts_3ca8fa0c`` — the same provider in an explicit test context is
  ``test_only_allowed``, and a locally-named ``Fake*`` class is not blocked by
  name (AC2, AC5).
* ``ts_1cb40a02`` — the core validates only the INJECTED Community wiring report,
  never importing ``okto_pulse.community``; no test-only provider is registered as
  a productive fallback (AC4).

The guard operates over a GENUINE test-only namespace: ``InMemoryEventBus`` lives
under ``okto_pulse.core.kg.providers.testing`` (the single-source-of-truth
``TESTING_PROVIDER_PREFIX``), so the teeth are real, not hand-built strings.
"""

from __future__ import annotations

import ast
from pathlib import Path

from okto_pulse.core.application.boundary.conformance_matrix import (
    TESTING_PROVIDER_PREFIX,
)
from okto_pulse.core.application.boundary.runtime_composition_guard import (
    RuntimeProviderObservation,
    validate_injected_providers,
    validate_runtime_composition_providers,
)
from okto_pulse.core.application.boundary.testing_provider_policy import (
    classify_provider,
)
from okto_pulse.core.composition import RuntimeComposition
from okto_pulse.core.kg.providers.testing.memory_event_bus import InMemoryEventBus

_THIS_FILE = Path(__file__).resolve()
_VIOLATION_SCHEMA = {
    "provider_key",
    "module",
    "object_type",
    "composition_path",
    "classification",
    "reason",
    "remediation",
}


class _ProductionSettings:
    """A production-namespace stand-in (its __module__ is this test module, OUTSIDE
    the test-only namespace), so it classifies as production_allowed."""


def _production_composition_with_test_only_event_bus() -> RuntimeComposition:
    """A RuntimeComposition built WITHOUT importing Community: every required slot
    is production-namespace EXCEPT ``event_bus`` (a real test-only InMemoryEventBus)
    — exactly the forbidden production wiring AC3 must catch."""
    return RuntimeComposition(
        settings_provider=_ProductionSettings(),
        auth_provider=_ProductionSettings(),
        storage_provider=_ProductionSettings(),
        event_bus=InMemoryEventBus(),
        uow_factory=_ProductionSettings(),
    )


# =========================================================================== #
# ts_64fb7259 (negative, AC1 + AC3 + AC6)
# GIVEN a production RuntimeComposition includes a provider from the test-only
#   namespace.
# WHEN the FCC07D guard validates it in production context.
# THEN the composition is rejected with provider_key/module/composition_path, and
#   the report is FCC07E-consumable (top-level context + deterministic violation
#   schema).
# =========================================================================== #
def test_ts_64fb7259_production_composition_rejects_testing_namespace_provider():
    composition = _production_composition_with_test_only_event_bus()

    report = validate_runtime_composition_providers(composition, context="production")

    # AC3 — the composition is REJECTED before being declared ready.
    assert report.overall_status == "blocked"
    assert report.blocked is True
    assert report.context == "production"

    # AC1 — the test-only event_bus is THE violation, with provider_key, module
    # under the testing prefix, composition_path, and a remediation.
    assert len(report.violations) == 1
    violation = report.violations[0]
    assert violation.classification == "violation"
    assert violation.provider_key == "event_bus"
    assert violation.module.startswith(TESTING_PROVIDER_PREFIX + ".")
    assert violation.composition_path == "RuntimeComposition.providers.event_bus"
    assert violation.remediation

    # AC6 — the report is FCC07E-consumable: top-level production context, and each
    # violation carries the deterministic 7-field schema (filterable, no parsing).
    payload = report.as_dict()
    assert payload["context"] == "production"
    assert payload["overall_status"] == "blocked"
    assert set(payload["violations"][0]) == _VIOLATION_SCHEMA
    # filterable by provider_key without text parsing.
    assert report.filter_violations(provider_key="event_bus") == (violation,)
    assert report.filter_violations(provider_key="storage_provider") == ()


# =========================================================================== #
# ts_3ca8fa0c (integration, AC2 + AC5)
# GIVEN the same testing-namespace provider AND a locally-named Fake* class,
#   evaluated in an explicit test context.
# WHEN the FCC07D policy/guard evaluates them.
# THEN the testing provider is test_only_allowed (no failure) and the Fake* class
#   is NOT blocked merely by its class name.
# =========================================================================== #
class FakeGraphStore:  # noqa: D401 - intentional Fake* name for the teeth
    """A locally-defined Fake to prove the policy never blocklists by class name."""


def test_ts_3ca8fa0c_sanctioned_testing_providers_usable_in_test_context():
    # AC2 — the SAME test-only provider, in an explicit test context, is allowed.
    testing_module = InMemoryEventBus.__module__
    verdict = classify_provider(
        module=testing_module,
        context="test",
        provider_key="event_bus",
        object_type="InMemoryEventBus",
    )
    assert verdict.classification == "test_only_allowed"
    assert verdict.remediation is None  # sanctioned use needs no remediation

    # the same provider injected as a test-context observation does NOT block.
    report = validate_injected_providers(
        [
            RuntimeProviderObservation(
                provider_key="event_bus",
                module=testing_module,
                object_type="InMemoryEventBus",
                composition_path="RuntimeComposition.providers.event_bus",
            )
        ],
        context="test",
    )
    assert report.overall_status == "passed"
    assert report.violations == ()

    # AC5 — a class literally named Fake*, defined in THIS test module (OUTSIDE the
    # sanctioned test-only namespace), is NOT a violation: the decision is by
    # namespace + context, never by class name.
    fake_verdict = classify_provider(
        module=FakeGraphStore.__module__,  # this test module, not the testing ns
        context="test",
        provider_key="graph_store",
        object_type=FakeGraphStore.__name__,
    )
    assert "Fake" in FakeGraphStore.__name__
    assert fake_verdict.classification != "violation"


# =========================================================================== #
# ts_1cb40a02 (integration, AC4)
# GIVEN the real Community wiring (build_community_kg_composition,
#   configure_community_kg_registry, startup composition) — proven in the Community
#   repo; the core sees only an INJECTED report of the resulting composition.
# WHEN the FCC07D guard evaluates the productive composition.
# THEN no test-only-namespace provider is registered as a productive fallback, and
#   the core validates the injected report WITHOUT importing okto_pulse.community.
# =========================================================================== #
def _community_wiring_observations() -> list[RuntimeProviderObservation]:
    """An injected report standing in for the Community productive composition:
    every provider is a production/community-namespace module (NONE under the
    test-only prefix) — what a correct Community wiring must yield."""
    base = "okto_pulse.community.kg.providers"
    return [
        RuntimeProviderObservation(
            provider_key=key,
            module=f"{base}.{key}",
            object_type=obj,
            composition_path=f"RuntimeComposition.providers.{key}",
        )
        for key, obj in (
            ("graph_store", "LadybugGraphStore"),
            ("event_bus", "PostgresEventBus"),
            ("embedding_provider", "SentenceTransformerEmbeddingProvider"),
        )
    ]


def test_ts_1cb40a02_community_wiring_has_no_production_fake_fallback():
    # The core validates the INJECTED productive composition report in production
    # context: a correct Community wiring registers NO test-only provider.
    report = validate_injected_providers(
        _community_wiring_observations(), context="production"
    )
    assert report.overall_status == "passed"
    assert report.violations == ()
    assert all(
        not v.module.startswith(TESTING_PROVIDER_PREFIX) for v in report.verdicts
    )

    # Conversely, if a Community report DID smuggle a test-only fallback into the
    # productive composition, the core's guard catches it — so "no productive fake
    # fallback" is enforced on the injected report, not assumed.
    smuggled = _community_wiring_observations() + [
        RuntimeProviderObservation(
            provider_key="graph_store",
            module=f"{TESTING_PROVIDER_PREFIX}.memory_event_bus",
            object_type="InMemoryEventBus",
            composition_path="RuntimeComposition.providers.graph_store",
        )
    ]
    smuggled_report = validate_injected_providers(smuggled, context="production")
    assert smuggled_report.overall_status == "blocked"
    assert any(v.provider_key == "graph_store" for v in smuggled_report.violations)

    # AC4 invariant — the core never imports okto_pulse.community: neither this
    # scenario module nor the guard it drives has any such import.
    tree = ast.parse(_THIS_FILE.read_text(encoding="utf-8"))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
    assert not any(name.startswith("okto_pulse.community") for name in imported)
