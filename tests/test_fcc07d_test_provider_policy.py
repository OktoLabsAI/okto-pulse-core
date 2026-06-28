"""FCC-07D-IMP1 test-only provider policy tests.

Covers the closed D-IMP1 scope: the pure classifier / report.

* AC1 — test-only namespace provider in ``production`` context -> ``violation``
  with provider_key + remediation.
* AC2 — the SAME provider in an explicit ``test`` context -> ``test_only_allowed``
  and does not fail the report.
* AC5 — a locally defined ``Fake*`` class in a test module, evaluated in a
  ``test`` context, is NOT blocked by its class name (FR6 anti-requirement).
* Determinism + fail-closed default + "reuses TESTING_PROVIDER_PREFIX".
"""

from __future__ import annotations

from typing import get_args

from okto_pulse.core.application.boundary.conformance_matrix import (
    TESTING_PROVIDER_PREFIX,
)
from okto_pulse.core.application.boundary.testing_provider_policy import (
    ProviderClassification,
    ProviderSpec,
    classify_provider,
    evaluate_provider_policy,
    is_test_only_namespace,
)

_TESTING_PROVIDER_MODULE = f"{TESTING_PROVIDER_PREFIX}.memory_graph_store"


# --------------------------------------------------------------------------- #
# AC1: test-only provider in production context -> violation + remediation
# --------------------------------------------------------------------------- #
def test_ac1_testing_provider_in_production_is_violation_with_remediation():
    verdict = classify_provider(
        module=_TESTING_PROVIDER_MODULE,
        context="production",
        provider_key="graph_store",
        object_type="class",
    )

    assert verdict.classification == "violation"
    assert verdict.provider_key == "graph_store"
    assert verdict.module == _TESTING_PROVIDER_MODULE
    assert verdict.object_type == "class"
    assert verdict.remediation  # non-empty remediation guidance
    # composition_path is owned by D-IMP2 runtime; the pure policy leaves it None.
    assert verdict.composition_path is None

    report = evaluate_provider_policy(
        [ProviderSpec(module=_TESTING_PROVIDER_MODULE, provider_key="graph_store")],
        context="production",
    )
    assert report.context == "production"
    assert report.ok is False
    assert len(report.violations) == 1
    only = report.violations[0]
    assert only.classification == "violation"
    assert only.provider_key == "graph_store"
    assert only.remediation
    # Report schema: top-level context + violations[] carrying the full row.
    payload = report.as_dict()
    assert payload["context"] == "production"
    assert payload["ok"] is False
    assert payload["violations"][0]["classification"] == "violation"
    assert payload["violations"][0]["provider_key"] == "graph_store"
    assert payload["violations"][0]["remediation"]
    assert payload["violations"][0]["composition_path"] is None


# --------------------------------------------------------------------------- #
# AC2: same provider in an explicit test context -> test_only_allowed, no fail
# --------------------------------------------------------------------------- #
def test_ac2_same_testing_provider_in_test_context_is_test_only_allowed():
    verdict = classify_provider(
        module=_TESTING_PROVIDER_MODULE,
        context="test",
        provider_key="graph_store",
    )

    assert verdict.classification == "test_only_allowed"
    assert verdict.provider_key == "graph_store"
    assert verdict.remediation is None  # sanctioned use needs no remediation

    report = evaluate_provider_policy(
        [ProviderSpec(module=_TESTING_PROVIDER_MODULE, provider_key="graph_store")],
        context="test",
    )
    assert report.context == "test"
    assert report.ok is True
    assert report.violations == ()


def test_ac1_ac2_same_provider_only_context_flips_the_verdict():
    """The provider identity is constant; only the context changes the verdict."""
    prod = classify_provider(module=_TESTING_PROVIDER_MODULE, context="production")
    test = classify_provider(module=_TESTING_PROVIDER_MODULE, context="test")

    assert prod.module == test.module
    assert prod.classification == "violation"
    assert test.classification == "test_only_allowed"


# --------------------------------------------------------------------------- #
# AC5 / FR6: a Fake* class in a test module, in test context, is NOT a violation
# (the decision is by namespace + context, never by class name)
# --------------------------------------------------------------------------- #
class FakeGraphStore:  # noqa: D401 - intentional Fake* name for the teeth below
    """A locally defined Fake to prove the policy does not blocklist by name."""


def test_ac5_fake_named_class_in_test_module_is_not_blocked_by_name():
    # A class literally named ``Fake*`` defined in THIS test module. Its module
    # is the test module (not the sanctioned test-only namespace), evaluated in
    # a test context.
    fake_module = FakeGraphStore.__module__
    assert "Fake" in FakeGraphStore.__name__
    assert not is_test_only_namespace(fake_module)

    verdict = classify_provider(
        module=fake_module,
        context="test",
        provider_key="graph_store",
        object_type=FakeGraphStore.__name__,
    )

    assert verdict.classification != "violation"
    assert verdict.classification == "production_allowed"
    # The Fake name reached the policy via object_type and was carried through,
    # never consulted for the decision.
    assert verdict.object_type == "FakeGraphStore"
    assert verdict.remediation is None


def test_fr6_fake_named_provider_inside_testing_namespace_still_decided_by_namespace():
    """A Fake* object UNDER the test-only namespace is governed by namespace +
    context, not by its Fake name: violation in production, allowed in test."""
    module = f"{TESTING_PROVIDER_PREFIX}.fake_graph_store"

    prod = classify_provider(
        module=module, context="production", object_type="FakeGraphStore"
    )
    test = classify_provider(
        module=module, context="test", object_type="FakeGraphStore"
    )

    assert prod.classification == "violation"
    assert test.classification == "test_only_allowed"


# --------------------------------------------------------------------------- #
# Fail-closed default: omitting context defaults to production
# --------------------------------------------------------------------------- #
def test_fail_closed_default_context_is_production():
    verdict = classify_provider(module=_TESTING_PROVIDER_MODULE)
    assert verdict.classification == "violation"

    report = evaluate_provider_policy(
        [ProviderSpec(module=_TESTING_PROVIDER_MODULE)]
    )
    assert report.context == "production"
    assert report.ok is False
    assert len(report.violations) == 1


# --------------------------------------------------------------------------- #
# Reuse proof: the namespace comes from FCC-07A's TESTING_PROVIDER_PREFIX
# --------------------------------------------------------------------------- #
def test_reuses_testing_provider_prefix_from_conformance_matrix():
    # Driving the policy with the imported constant + ".x" must classify it as
    # the test-only namespace. If the policy redeclared its own string and it
    # drifted from FCC-07A, this would break.
    under_prefix = classify_provider(
        module=TESTING_PROVIDER_PREFIX + ".x", context="production"
    )
    assert under_prefix.classification == "violation"

    exact_prefix = classify_provider(module=TESTING_PROVIDER_PREFIX, context="test")
    assert exact_prefix.classification == "test_only_allowed"

    assert is_test_only_namespace(TESTING_PROVIDER_PREFIX)
    assert is_test_only_namespace(TESTING_PROVIDER_PREFIX + ".anything")
    # A sibling namespace that merely shares a prefix string but is not below it
    # must NOT be captured (guards against a naive ``startswith`` without dot).
    assert not is_test_only_namespace(TESTING_PROVIDER_PREFIX + "_sibling")


def test_non_testing_namespace_is_production_allowed_in_both_contexts():
    module = "okto_pulse.core.kg.providers.ladybug_graph_store"
    for context in ("production", "test"):
        verdict = classify_provider(module=module, context=context)  # type: ignore[arg-type]
        assert verdict.classification == "production_allowed"
        assert verdict.remediation is None


# --------------------------------------------------------------------------- #
# Determinism: same input -> byte-identical report; violations are ordered
# --------------------------------------------------------------------------- #
def test_report_is_deterministic_and_violations_are_sorted():
    specs = [
        ProviderSpec(module=f"{TESTING_PROVIDER_PREFIX}.zeta", provider_key="z"),
        ProviderSpec(module="okto_pulse.core.kg.providers.real", provider_key="r"),
        ProviderSpec(module=f"{TESTING_PROVIDER_PREFIX}.alpha", provider_key="a"),
        ProviderSpec(module=f"{TESTING_PROVIDER_PREFIX}.alpha", provider_key="a2"),
    ]

    first = evaluate_provider_policy(specs, context="production")
    second = evaluate_provider_policy(list(reversed(specs)), context="production")

    # Order-independent: same set of violations, identically ordered, identical dict.
    assert first.as_dict()["violations"] == second.as_dict()["violations"]
    violation_modules = [v.module for v in first.violations]
    assert violation_modules == sorted(violation_modules)
    # The non-testing provider is not a violation.
    assert all("real" not in v.module for v in first.violations)
    # Stable tie-break on provider_key for the same module.
    alpha = [v for v in first.violations if v.module.endswith(".alpha")]
    assert [v.provider_key for v in alpha] == ["a", "a2"]


def test_classification_literal_matches_taxonomy():
    assert set(get_args(ProviderClassification)) == {
        "production_allowed",
        "test_only_allowed",
        "violation",
    }
