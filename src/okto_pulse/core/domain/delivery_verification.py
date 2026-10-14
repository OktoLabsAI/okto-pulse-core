"""Normalized native verification facts and factual rejection reasons.

The edition authenticates native current heads. These immutable values do not
execute tests, read receipts, select the latest passing run or grant authority.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from okto_pulse.core.domain.delivery_assessment import FactualReason, ReadinessReason
from okto_pulse.core.domain.delivery_evidence import DeliveryBinding, DeliveryScope, ImplementationDeliveryFact


@dataclass(frozen=True, slots=True)
class VerificationSourceFact:
    source_ref: str
    revision: str
    sha256: str | None
    origin: Literal['verification_report.sources', 'execution_attestation.execution_basis']


@dataclass(frozen=True, slots=True)
class VerificationObservationFact:
    observation_id: str
    criterion_id: str
    observation_ref: str
    expected: str
    observed: str
    outcome: str


@dataclass(frozen=True, slots=True)
class NativeVerificationRun:
    scope: DeliveryScope
    scenario_id: str
    scenario_sha256: str
    method: str | None
    receipt_id: str | None
    evidence_ref: str | None
    outcome: str | None
    authenticated: bool
    sources: tuple[VerificationSourceFact, ...] = ()
    observations: tuple[VerificationObservationFact, ...] = ()
    observed_at: datetime | None = None
    rejection_reason: ReadinessReason | None = None
    native_reason_codes: tuple[str, ...] = ()
    contract_version: Literal['delivery-verification-run/v1'] = 'delivery-verification-run/v1'


@dataclass(frozen=True, slots=True)
class NativeVerificationPopulation:
    runs: tuple[NativeVerificationRun, ...]
    complete: bool
    native_reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ScenarioVerificationFinding:
    implementation_id: str
    criterion_id: str
    scenario_id: str
    reason: FactualReason


def verification_sources_issue(observed: tuple[tuple[str, str], ...],
                               expected: tuple[tuple[str, str], ...]) -> Literal[
                                   'verification_base_unresolved', 'verification_base_mismatch'] | None:
    """Exact canonical equality, shared by reports and execution attestations."""
    if not observed or not expected or any(
        not isinstance(source, str) or not source.strip()
        or not isinstance(revision, str) or not revision.strip()
        for source, revision in (*observed, *expected)
    ):
        return 'verification_base_unresolved'
    observed_index = dict(observed)
    expected_index = dict(expected)
    if (any(observed_index[source] != revision for source, revision in observed)
        or any(expected_index[source] != revision for source, revision in expected)):
        return 'verification_base_unresolved'
    if any(observed_index.get(source) != revision for source, revision in expected):
        return 'verification_base_mismatch'
    return None


def missing_scenario_reason(*, scope: DeliveryScope, binding: DeliveryBinding,
                            implementation: ImplementationDeliveryFact, scenario_id: str,
                            criterion_id: str, population: NativeVerificationPopulation | None,
                            current_runs: tuple[NativeVerificationRun, ...] | None = None) -> FactualReason:
    """Explain a missing positive join without turning an unbound run into credit.

    The shared delivery evaluator determines whether a binding proves delivery.
    This classifies the native outcome when that join is missing. Incompatible
    old-base failures are base mismatches, never current product failures.
    """
    def reason(code, runs=(), native=()):
        return FactualReason(code, tuple(native),
            tuple(sorted({run.receipt_id for run in runs if run.receipt_id})),
            (criterion_id,), (scenario_id,))

    if population is None or not population.complete:
        return reason(ReadinessReason.ASSESSMENT_UNAVAILABLE,
            native=population.native_reason_codes if population else ('native_verification_population_unavailable',))
    runs = (tuple(run for run in population.runs if run.scenario_id == scenario_id)
            if current_runs is None else current_runs)
    if not runs:
        return reason(ReadinessReason.VERIFICATION_NOT_RUN)
    if len(set(runs)) != 1:
        return reason(ReadinessReason.VERIFICATION_CONFLICT, runs)
    run = runs[0]
    if run.scope != scope:
        return reason(ReadinessReason.VERIFICATION_RECEIPT_INVALID, runs, ('verification_scope_mismatch',))
    if run.rejection_reason is not None:
        return reason(run.rejection_reason, runs, run.native_reason_codes)
    if run.outcome is None and not run.receipt_id:
        return reason(ReadinessReason.VERIFICATION_NOT_RUN, runs)
    if not run.authenticated or not run.receipt_id:
        return reason(ReadinessReason.VERIFICATION_RECEIPT_INVALID, runs, run.native_reason_codes)
    selected = [contribution for contribution in implementation.contributions or () if contribution.binding == binding]
    proofs = {proof.execution_id: proof for proof in implementation.executions}
    if len(selected) != 1 or not selected[0].execution_ids or any(
        identity not in proofs or not proofs[identity].current_accepted_execution
        for identity in selected[0].execution_ids
    ):
        return reason(ReadinessReason.VERIFICATION_DEPENDENCY_PENDING, runs)
    expected = tuple(proofs[identity] for identity in selected[0].execution_ids)
    base_issue = verification_sources_issue(
        tuple((source.source_ref, source.revision) for source in run.sources),
        tuple((proof.source_ref, proof.result_revision) for proof in expected))
    if base_issue:
        return reason(ReadinessReason(base_issue), runs)
    if run.observed_at is None or any(proof.observed_at is None for proof in expected):
        return reason(ReadinessReason.VERIFICATION_BASE_UNRESOLVED, runs, ('verification_observation_time_unavailable',))
    if any(run.observed_at < proof.observed_at for proof in expected):
        return reason(ReadinessReason.VERIFICATION_RECEIPT_INVALID, runs, ('verification_observation_predates_implementation',))
    outcomes = {item.outcome for item in run.observations if item.criterion_id == criterion_id}
    if run.observations and not outcomes:
        return reason(ReadinessReason.VERIFICATION_RECEIPT_INVALID, runs, ('verification_criterion_result_missing',))
    outcomes = outcomes or {run.outcome}
    if 'failed' in outcomes:
        return reason(ReadinessReason.VERIFICATION_FAILED, runs)
    if outcomes != {'passed'}:
        return reason(ReadinessReason.VERIFICATION_INCONCLUSIVE, runs)
    return reason(ReadinessReason.VERIFICATION_ASSOCIATION_MISSING, runs)
