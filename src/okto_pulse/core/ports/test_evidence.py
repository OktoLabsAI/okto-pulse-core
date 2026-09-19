"""Edition port for concrete Test Evidence V2 verification."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol

from okto_pulse.core.domain.test_scenarios import ADMITTED_VERIFICATION_METHODS

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    reset_runtime_values,
    resolve_runtime_value,
)


@dataclass(frozen=True, slots=True)
class TestEvidenceWriteVerification:
    verified: bool
    reason_codes: tuple[str, ...] = ()


class TestEvidenceWriteVerifier(Protocol):
    @property
    def verification_methods(self) -> frozenset[str]:
        """Methods this concrete verifier authenticates; never client input."""
        ...

    def verify(
        self,
        *,
        board_id: str,
        spec_id: str,
        status: str,
        scenario_id: str,
        scenario_sha256: str,
        actor_id: str | None,
        evidence: object,
    ) -> TestEvidenceWriteVerification: ...


@dataclass(frozen=True, slots=True)
class TestEvidenceExecutionRequest:
    """Transport-neutral request for a trusted edition replay runtime.

    ``inline_replay`` is intentionally opaque to CORE. Concrete editions own
    its schema validation, canonical materialization and execution effects.
    """

    board_id: str
    spec_id: str
    scenario_id: str
    status: str
    manifest_ref: str | None
    actor_id: str
    scenario_sha256: str
    inline_replay: object | None = None


@dataclass(frozen=True, slots=True)
class TestEvidenceExecutionResult:
    """Canonical evidence emitted only after the runtime replay completed."""

    evidence: Mapping[str, Any]


class TestEvidenceExecutionIssuer(Protocol):
    async def execute(
        self, request: TestEvidenceExecutionRequest
    ) -> TestEvidenceExecutionResult: ...


_VERIFIER_KEY = "ports.test_evidence.write_verifier"
_ISSUER_KEY = "ports.test_evidence.execution_issuer"


def register_test_evidence_write_verifier(
    verifier: TestEvidenceWriteVerifier,
) -> None:
    register_runtime_value(_VERIFIER_KEY, verifier)


def resolve_test_evidence_write_verifier() -> TestEvidenceWriteVerifier | None:
    value = resolve_runtime_value(_VERIFIER_KEY)
    return value  # type: ignore[return-value]


def reset_test_evidence_write_verifier_for_tests() -> None:
    reset_runtime_values(_VERIFIER_KEY)


def supported_test_verification_methods() -> frozenset[str] | None:
    """Unknown capability stays unavailable, including legacy verifier doubles."""
    verifier = resolve_test_evidence_write_verifier()
    declared = getattr(verifier, "verification_methods", None)
    if not isinstance(declared, frozenset) or not all(
        isinstance(item, str) for item in declared
    ):
        return None
    return declared & ADMITTED_VERIFICATION_METHODS


def require_supported_test_verification_method(method: object) -> None:
    if method is None:
        return  # Compatibility: absence does not adopt the new verification contract.
    supported = supported_test_verification_methods()
    if not isinstance(method, str) or supported is None or method not in supported:
        raise ValueError("verification_method_unsupported")


def register_test_evidence_execution_issuer(
    issuer: TestEvidenceExecutionIssuer,
) -> None:
    register_runtime_value(_ISSUER_KEY, issuer)


def resolve_test_evidence_execution_issuer() -> TestEvidenceExecutionIssuer | None:
    value = resolve_runtime_value(_ISSUER_KEY)
    return value  # type: ignore[return-value]


def reset_test_evidence_execution_issuer_for_tests() -> None:
    reset_runtime_values(_ISSUER_KEY)


__all__ = [
    "TestEvidenceWriteVerification",
    "TestEvidenceWriteVerifier",
    "TestEvidenceExecutionIssuer",
    "TestEvidenceExecutionRequest",
    "TestEvidenceExecutionResult",
    "register_test_evidence_execution_issuer",
    "register_test_evidence_write_verifier",
    "reset_test_evidence_execution_issuer_for_tests",
    "reset_test_evidence_write_verifier_for_tests",
    "resolve_test_evidence_execution_issuer",
    "resolve_test_evidence_write_verifier",
    "supported_test_verification_methods",
    "require_supported_test_verification_method",
]
