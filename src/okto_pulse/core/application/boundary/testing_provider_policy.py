"""FCC-07D-IMP1 test-only provider policy (pure classifier).

This module is the deterministic POLICY that classifies a provider (an import
path / wired object) as ``production_allowed``, ``test_only_allowed`` or
``violation`` based on its **module namespace**, an **explicit context**
(``production`` | ``test``) and an optional ``provider_key``.

It is additive and pure: it does not scan source, touch global state, or import
Community code. The runtime guard that consumes this policy over a concrete
``RuntimeComposition`` — plus the Community wiring coverage and the injected
``api_504fbaca`` report — is FCC-07D-IMP2 and lives elsewhere.

Single source of truth for the sanctioned test-only namespace is FCC-07A's
:data:`okto_pulse.core.application.boundary.conformance_matrix.TESTING_PROVIDER_PREFIX`
— it is imported and reused here, never redeclared. The ``test_only`` vs
``violation`` outcome for that namespace is THIS policy's own context-based
decision (its three-outcome taxonomy is intentionally distinct from FCC-07A's
``MatrixClassification``); only the namespace prefix is reused from FCC-07A —
no private FCC-07A helper is consumed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Literal

from .conformance_matrix import TESTING_PROVIDER_PREFIX

#: The three policy outcomes. Intentionally distinct from FCC-07A's broader
#: ``MatrixClassification`` taxonomy: this policy answers a narrower question
#: (is wiring this provider in this context allowed?).
ProviderClassification = Literal[
    "production_allowed",
    "test_only_allowed",
    "violation",
]

#: The two evaluation contexts. There is no implicit / heuristic third state:
#: callers must declare the context explicitly (TR2/TR5). The default everywhere
#: in this module is ``production`` (fail-closed).
ProviderContext = Literal["production", "test"]

_REASON_PRODUCTION_ALLOWED = (
    "Module is outside the sanctioned test-only provider namespace "
    f"'{TESTING_PROVIDER_PREFIX}'; this policy does not govern it."
)
_REASON_TEST_ONLY_ALLOWED = (
    "Sanctioned test-only provider under namespace "
    f"'{TESTING_PROVIDER_PREFIX}'; permitted in an explicit test context."
)
_REASON_PRODUCTION_VIOLATION = (
    "Test-only provider under namespace "
    f"'{TESTING_PROVIDER_PREFIX}' is forbidden in a production context."
)

_REMEDIATION_PRODUCTION_VIOLATION = (
    "Move concrete production provider wiring to a registered runtime adapter, "
    "or pass an explicit test context for sanctioned test fixtures."
)


@dataclass(frozen=True)
class ProviderSpec:
    """A provider to classify: its module namespace and identity.

    ``module`` is the import path of the provider object (e.g.
    ``okto_pulse.core.kg.providers.testing.memory_graph_store``). ``provider_key``
    and ``object_type`` are carried through verbatim for the report and play no
    part in the classification decision (FR6: the verdict is by namespace +
    context, never by class name).
    """

    module: str
    provider_key: str | None = None
    object_type: str | None = None


@dataclass(frozen=True)
class ProviderVerdict:
    """The classification of a single provider in a single context.

    Schema is aligned with the ``api_504fbaca`` runtime-composition report:
    ``provider_key`` / ``module`` / ``object_type`` / ``classification`` /
    ``reason`` / ``remediation``. ``composition_path`` is owned by FCC-07D-IMP2
    (runtime) and is always ``None`` in this pure policy.
    """

    module: str
    classification: ProviderClassification
    reason: str
    provider_key: str | None = None
    object_type: str | None = None
    remediation: str | None = None
    composition_path: str | None = None

    @property
    def is_violation(self) -> bool:
        return self.classification == "violation"

    def as_dict(self) -> dict[str, object]:
        return {
            "provider_key": self.provider_key,
            "module": self.module,
            "object_type": self.object_type,
            "classification": self.classification,
            "reason": self.reason,
            "remediation": self.remediation,
            "composition_path": self.composition_path,
        }


@dataclass(frozen=True)
class TestProviderPolicyReport:
    """Deterministic report for a batch of providers under one context.

    Top-level ``context`` (``production`` | ``test``) plus a deterministically
    ordered ``violations`` tuple. ``verdicts`` carries every classified provider
    for callers that want the full picture; ``as_dict`` emits the documented
    ``context`` + ``violations`` schema.
    """

    context: ProviderContext
    verdicts: tuple[ProviderVerdict, ...]
    violations: tuple[ProviderVerdict, ...]

    @property
    def ok(self) -> bool:
        return not self.violations

    def as_dict(self) -> dict[str, object]:
        return {
            "context": self.context,
            "ok": self.ok,
            "violations": [v.as_dict() for v in self.violations],
        }


def is_test_only_namespace(module: str) -> bool:
    """True when ``module`` is the sanctioned test-only namespace or below it.

    Uses the imported :data:`TESTING_PROVIDER_PREFIX` as the single source of
    truth — the string is never redeclared here.
    """

    return module == TESTING_PROVIDER_PREFIX or module.startswith(
        TESTING_PROVIDER_PREFIX + "."
    )


def classify_provider(
    *,
    module: str,
    context: ProviderContext = "production",
    provider_key: str | None = None,
    object_type: str | None = None,
) -> ProviderVerdict:
    """Classify one provider by module namespace + explicit context.

    Decision (deterministic; no global state, no environment heuristics):

    * ``module`` outside the test-only namespace -> ``production_allowed``
      (this policy governs only the test-only namespace; it does not ban
      other providers here).
    * ``module`` in the test-only namespace, ``context == "test"`` ->
      ``test_only_allowed``.
    * ``module`` in the test-only namespace, ``context == "production"`` ->
      ``violation`` (with remediation).

    Within the test-only namespace the outcome is a pure function of the explicit
    context (this policy's own decision; only the namespace prefix is reused from
    FCC-07A). The class NAME of the provider is never consulted (FR6).
    """

    if not is_test_only_namespace(module):
        return ProviderVerdict(
            module=module,
            classification="production_allowed",
            reason=_REASON_PRODUCTION_ALLOWED,
            provider_key=provider_key,
            object_type=object_type,
            remediation=None,
        )

    # Within the test-only namespace the outcome is a pure function of the
    # explicit context — this policy's own decision (its three-outcome taxonomy
    # is distinct from FCC-07A's MatrixClassification). Only the namespace prefix
    # is reused from FCC-07A; the class NAME is never consulted (FR6).
    if context == "test":
        return ProviderVerdict(
            module=module,
            classification="test_only_allowed",
            reason=_REASON_TEST_ONLY_ALLOWED,
            provider_key=provider_key,
            object_type=object_type,
            remediation=None,
        )
    return ProviderVerdict(
        module=module,
        classification="violation",
        reason=_REASON_PRODUCTION_VIOLATION,
        provider_key=provider_key,
        object_type=object_type,
        remediation=_REMEDIATION_PRODUCTION_VIOLATION,
    )


def _violation_sort_key(verdict: ProviderVerdict) -> tuple[str, str, str]:
    return (
        verdict.module,
        verdict.provider_key or "",
        verdict.object_type or "",
    )


def evaluate_provider_policy(
    providers: Iterable[ProviderSpec],
    *,
    context: ProviderContext = "production",
) -> TestProviderPolicyReport:
    """Classify a batch of providers under one explicit top-level context.

    ``context`` defaults to ``production`` (fail-closed, TR2). Returns a
    deterministic report: every verdict plus the ordered subset that are
    violations.
    """

    verdicts = tuple(
        classify_provider(
            module=spec.module,
            context=context,
            provider_key=spec.provider_key,
            object_type=spec.object_type,
        )
        for spec in providers
    )
    violations = tuple(
        sorted(
            (verdict for verdict in verdicts if verdict.is_violation),
            key=_violation_sort_key,
        )
    )
    return TestProviderPolicyReport(
        context=context,
        verdicts=verdicts,
        violations=violations,
    )


__all__ = [
    "ProviderClassification",
    "ProviderContext",
    "ProviderSpec",
    "ProviderVerdict",
    "TestProviderPolicyReport",
    "is_test_only_namespace",
    "classify_provider",
    "evaluate_provider_policy",
]
