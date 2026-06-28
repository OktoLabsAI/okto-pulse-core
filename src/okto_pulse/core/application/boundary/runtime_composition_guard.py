"""FCC-07D-IMP2 runtime-composition provider guard.

This module is the GUARD that validates a concrete :class:`RuntimeComposition`
(or an injected, abstract provider report) against the FCC-07D-IMP1 test-only
provider POLICY. It answers one question, fail-closed: *are any of the providers
wired into this composition sanctioned only for tests, yet present in a
production context?*

Design boundaries (load-bearing):

* It **consumes** the D-IMP1 policy via the public
  :func:`okto_pulse.core.application.boundary.testing_provider_policy.classify_provider`
  — it never reimplements the namespace/context classification, and it touches
  no private symbol of that module (it defines its own deterministic ordering).
* It **never imports** ``okto_pulse.community``. The core validates only the
  abstract things it is handed: a :class:`RuntimeComposition` it iterates by its
  public ``provider_keys()`` / ``require_provider()`` API, or a sequence of
  injected :class:`RuntimeProviderObservation` (provider specs with
  ``composition_path`` already resolved). Proving the Community edition's wiring
  is itself test-only-free is the Community repo's integration suite — here we
  only validate the report it injects, with the same contract (FR4/TR6).
* The emitted report schema is aligned with the ``api_504fbaca`` contract
  (``internal://fcc07d/runtime-composition-provider-guard-report``):
  ``overall_status`` (``blocked`` | ``passed``) + ``context``
  (``production`` | ``test``) + ``violations[]`` (the seven fields
  ``provider_key`` / ``module`` / ``object_type`` / ``composition_path`` /
  ``classification`` / ``reason`` / ``remediation``) + ``summary``
  (``violations`` / ``production_allowed`` / ``test_only_allowed``).

Reusable by the FCC-07E runner (TR1): introspect a composition into abstract
observations with :func:`observe_runtime_composition`, validate either a live
composition (:func:`validate_runtime_composition_providers`) or an injected
report (:func:`validate_injected_providers`), and filter the resulting
violations by ``provider_key`` / ``module`` / ``composition_path``
(:meth:`RuntimeCompositionGuardReport.filter_violations`, AC6).
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Iterable, Literal

from ...composition import RuntimeComposition
from .testing_provider_policy import (
    ProviderContext,
    ProviderVerdict,
    classify_provider,
)

#: Overall status of a runtime-composition guard report. ``blocked`` whenever at
#: least one provider is a violation in the evaluated context; ``passed``
#: otherwise. The default context is ``production`` (fail-closed), so a guard run
#: with no explicit context blocks any test-only provider.
GuardStatus = Literal["blocked", "passed"]

#: Prefix used to build a stable ``composition_path`` for each provider iterated
#: out of a :class:`RuntimeComposition` (e.g.
#: ``RuntimeComposition.providers.event_bus``).
RUNTIME_COMPOSITION_PATH_PREFIX = "RuntimeComposition.providers"

#: The only contexts the guard accepts (api_504fbaca ``invalid_context``).
_VALID_CONTEXTS: frozenset[str] = frozenset({"production", "test"})


class InvalidContextError(ValueError):
    """An evaluation context outside ``{production, test}`` (api_504fbaca
    ``invalid_context``). The guard validates the context at RUNTIME — the static
    ``ProviderContext`` Literal is not enough at a public boundary."""

    code = "invalid_context"


class InvalidReportShapeError(ValueError):
    """An INJECTED provider observation missing a mandatory field (api_504fbaca
    ``invalid_report_shape``). For an injected report ``provider_key``, ``module``
    AND ``composition_path`` are all required and non-empty — the core never
    synthesises a ``composition_path`` for injected (e.g. Community) reports, so
    the provenance of where the provider was wired is never lost."""

    code = "invalid_report_shape"


@dataclass(frozen=True)
class RuntimeProviderObservation:
    """One provider observed in a composition, ready for classification.

    This is the abstract, edition-agnostic unit the guard validates. It carries
    exactly what the policy needs plus the resolved ``composition_path`` so the
    same contract covers both a live :class:`RuntimeComposition` and an injected
    Community wiring report — without the core importing any edition code.

    * ``provider_key`` — the composition slot (e.g. ``event_bus``).
    * ``module`` — the import path of the provider object
      (``type(obj).__module__``).
    * ``object_type`` — the provider object's class name
      (``type(obj).__name__``); carried for diagnostics, never consulted by the
      policy decision (FR6).
    * ``composition_path`` — where the provider is wired (e.g.
      ``RuntimeComposition.providers.event_bus``). The live-composition path
      (:func:`observe_runtime_composition`) sets it automatically; an INJECTED
      report MUST supply it (api_504fbaca ``invalid_report_shape``) so the
      provenance of where the provider came from is never lost.
    """

    provider_key: str
    module: str
    object_type: str | None = None
    composition_path: str | None = None


@dataclass(frozen=True)
class RuntimeCompositionGuardReport:
    """Deterministic guard report aligned with the ``api_504fbaca`` contract.

    ``verdicts`` carries every classified provider for callers that want the full
    picture; ``violations`` is the deterministically ordered subset that blocks
    the composition. ``as_dict`` emits the documented ``overall_status`` +
    ``context`` + ``violations`` + ``summary`` schema.
    """

    context: ProviderContext
    verdicts: tuple[ProviderVerdict, ...]
    violations: tuple[ProviderVerdict, ...]
    production_allowed: int
    test_only_allowed: int

    @property
    def overall_status(self) -> GuardStatus:
        """``blocked`` if any provider is a violation, else ``passed``."""
        return "blocked" if self.violations else "passed"

    @property
    def blocked(self) -> bool:
        """True when the composition must be rejected before it is declared ready."""
        return bool(self.violations)

    def filter_violations(
        self,
        *,
        provider_key: str | None = None,
        module: str | None = None,
        composition_path: str | None = None,
    ) -> tuple[ProviderVerdict, ...]:
        """Filter violations by ``provider_key`` / ``module`` / ``composition_path``.

        Consumed by the FCC-07E runner (AC6) to locate the specific provider(s)
        responsible for a block. Any argument left ``None`` is not constrained.
        """
        return tuple(
            verdict
            for verdict in self.violations
            if (provider_key is None or verdict.provider_key == provider_key)
            and (module is None or verdict.module == module)
            and (composition_path is None or verdict.composition_path == composition_path)
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "overall_status": self.overall_status,
            "context": self.context,
            "violations": [verdict.as_dict() for verdict in self.violations],
            "summary": {
                "violations": len(self.violations),
                "production_allowed": self.production_allowed,
                "test_only_allowed": self.test_only_allowed,
            },
        }


def _validate_context(context: str) -> None:
    """Reject a context outside ``{production, test}`` (api_504fbaca
    ``invalid_context``). Validated at RUNTIME at every public entry point."""
    if context not in _VALID_CONTEXTS:
        raise InvalidContextError(
            f"context must be 'production' or 'test'; got {context!r}."
        )


def _validate_injected_observation(observation: RuntimeProviderObservation) -> None:
    """Reject an injected observation missing any mandatory field (api_504fbaca
    ``invalid_report_shape``): ``provider_key`` / ``module`` / ``composition_path``
    must all be present and non-empty. The core never synthesises a
    ``composition_path`` for an injected report, so an incomplete Community report
    fails closed instead of silently losing its provenance."""
    missing = [
        field
        for field in ("provider_key", "module", "composition_path")
        if not getattr(observation, field)
    ]
    if missing:
        raise InvalidReportShapeError(
            "injected provider report is missing required, non-empty field(s) "
            f"{missing}: provider_key, module and composition_path are all "
            f"mandatory for an injected report (got {observation!r})."
        )


def _violation_sort_key(verdict: ProviderVerdict) -> tuple[str, str, str]:
    """Deterministic ordering: by composition_path, then provider_key, then module.

    Defined locally (not imported from the policy module) so the guard does not
    couple to a private symbol of D-IMP1.
    """
    return (
        verdict.composition_path or "",
        verdict.provider_key or "",
        verdict.module,
    )


def observe_runtime_composition(
    composition: RuntimeComposition,
) -> tuple[RuntimeProviderObservation, ...]:
    """Introspect a live :class:`RuntimeComposition` into abstract observations.

    Iterates only the composition's public surface — ``provider_keys()`` (the
    supplied, non-``None`` slots) and ``require_provider(key)`` — and extracts
    ``module`` / ``object_type`` from the provider object's *type*. The result is
    deterministically ordered by provider_key so the runner (FCC-07E) gets a
    stable view. No edition/community code is imported.
    """
    observations: list[RuntimeProviderObservation] = []
    for provider_key in composition.provider_keys():
        obj = composition.require_provider(provider_key)
        obj_type = type(obj)
        observations.append(
            RuntimeProviderObservation(
                provider_key=provider_key,
                module=obj_type.__module__,
                object_type=obj_type.__name__,
                composition_path=f"{RUNTIME_COMPOSITION_PATH_PREFIX}.{provider_key}",
            )
        )
    observations.sort(key=lambda obs: obs.provider_key)
    return tuple(observations)


def validate_injected_providers(
    observations: Iterable[RuntimeProviderObservation],
    *,
    context: ProviderContext = "production",
) -> RuntimeCompositionGuardReport:
    """Validate an INJECTED sequence of provider observations (FR4/TR6).

    This is the edition-agnostic entry point: the caller injects provider specs
    (with ``composition_path`` already resolved, e.g. the Community wiring
    surfaced by the Community integration suite) and the core classifies each via
    the D-IMP1 policy, emitting the same ``api_504fbaca`` report. The core never
    imports the edition — it only validates the abstract observations handed to
    it.

    ``context`` defaults to ``production`` (fail-closed, TR2): a sanctioned
    test-only provider injected without an explicit test context is a violation
    and blocks the report.
    """
    _validate_context(context)
    observations = list(observations)
    for observation in observations:
        _validate_injected_observation(observation)

    verdicts: list[ProviderVerdict] = []
    production_allowed = 0
    test_only_allowed = 0
    for observation in observations:
        verdict = classify_provider(
            module=observation.module,
            context=context,
            provider_key=observation.provider_key,
            object_type=observation.object_type,
        )
        # The pure policy leaves composition_path None; attach the validated,
        # caller-supplied composition_path via public dataclasses.replace — no
        # private coupling, no reclassification, and NO synthesis for an injected
        # report (its path is mandatory, enforced above).
        verdict = replace(verdict, composition_path=observation.composition_path)
        verdicts.append(verdict)
        if verdict.classification == "production_allowed":
            production_allowed += 1
        elif verdict.classification == "test_only_allowed":
            test_only_allowed += 1

    violations = tuple(
        sorted(
            (verdict for verdict in verdicts if verdict.is_violation),
            key=_violation_sort_key,
        )
    )
    return RuntimeCompositionGuardReport(
        context=context,
        verdicts=tuple(verdicts),
        violations=violations,
        production_allowed=production_allowed,
        test_only_allowed=test_only_allowed,
    )


def validate_runtime_composition_providers(
    composition: RuntimeComposition,
    *,
    context: ProviderContext = "production",
) -> RuntimeCompositionGuardReport:
    """Validate a live :class:`RuntimeComposition` against the D-IMP1 policy.

    Iterates each supplied provider, extracts ``module`` / ``object_type`` from
    its type and a stable ``composition_path``
    (``RuntimeComposition.providers.<key>``), classifies it via the policy, and
    produces the ``api_504fbaca`` report.

    ``context`` defaults to ``production`` (fail-closed, AC3/TR2): a composition
    that wires a test-only provider in production is ``blocked`` — it must be
    rejected before being declared ready. The same composition under an explicit
    ``test`` context is ``passed`` (TR4).
    """
    return validate_injected_providers(
        observe_runtime_composition(composition),
        context=context,
    )


__all__ = [
    "GuardStatus",
    "RUNTIME_COMPOSITION_PATH_PREFIX",
    "InvalidContextError",
    "InvalidReportShapeError",
    "RuntimeProviderObservation",
    "RuntimeCompositionGuardReport",
    "observe_runtime_composition",
    "validate_injected_providers",
    "validate_runtime_composition_providers",
]
