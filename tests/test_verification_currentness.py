"""Verification authority policy tests; no synthetic native receipt is admitted."""

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.verification_currentness import (
    MAX_VERIFICATION_COUNTER,
    AuthenticatedVerificationResult,
    VerificationBasis,
    VerificationContext,
    VerificationCurrentnessError,
    VerificationReservation,
    VerificationSourceVersion,
    canonical_scenario_sha256,
    next_verification_counter,
    plan_verification_publication,
)
from okto_pulse.core.ports.test_evidence import (
    TestEvidenceExecutionRequest as ExecutionRequest,
)
from okto_pulse.core.ports.test_evidence import (
    TestVerificationReportRequest as ReportRequest,
)


def context(**changes):
    fields = {
        "board_id": "board-a", "spec_id": "spec-a", "spec_edition": 2, "scenario_id": "scenario-a",
        "reservation_id": "reservation-1", "scenario_sha256": "a" * 64,
        "verification_method": "automated_test", "generation": 1, "ordinal": 1,
    }
    return VerificationContext.model_validate(fields | changes)


def basis(*sources):
    return VerificationBasis(sources=tuple(
        VerificationSourceVersion(source_ref=source, revision=revision)
        for source, revision in sources
    ))


def result(ordinal=1, revision="rev-a", **changes):
    fields = {
        "context": context(ordinal=ordinal, reservation_id=f"reservation-{ordinal}"),
        "actor_id": "actor-a", "actor_kind": "human", "receipt_id": f"receipt-{ordinal}",
        "evidence_sha256": "b" * 64, "basis": basis(("core", revision)), "outcome": "passed",
    }
    return AuthenticatedVerificationResult.model_validate(fields | changes)


def publish(incoming, head=None, prior=None, summary=0, **changes):
    args = {
        "reservation": VerificationReservation(
            context=incoming.context, actor_id=incoming.actor_id, actor_kind=incoming.actor_kind,
        ),
        "scope": incoming.context.scope, "generation": 1, "scenario_sha256": "a" * 64,
        "verification_method": "automated_test", "basis_head": head, "prior_acceptance": prior,
        "operational_summary_ordinal": summary,
    }
    return plan_verification_publication(incoming, **(args | changes))


def test_full_basis_is_order_independent_and_retains_companion():
    a = basis(("core", "a"), ("community", "b"))
    assert a == basis(("community", "b"), ("core", "a"))
    assert a.digest == basis(("community", "b"), ("core", "a")).digest
    assert a.digest != basis(("core", "a")).digest
    assert a.digest != basis(("core", "a"), ("community", "changed")).digest


@pytest.mark.parametrize("sources", [(), (("core", "a"), ("core", "a")),
                                    (("core", "a"), ("core", "b")),
                                    (("", "a"),), (("core", " "),)])
def test_missing_duplicate_or_conflicting_sources_are_rejected(sources):
    with pytest.raises(ValueError):
        basis(*sources)


def test_report_and_runner_have_distinct_source_bounds():
    basis(("core", "a")).require_runner_shape()
    basis(("core", "a"), ("community", "b")).require_runner_shape()
    report_basis = basis(*[(f"source-{i}", "a") for i in range(20)])
    assert len(report_basis.sources) == 20
    with pytest.raises(VerificationCurrentnessError, match="verification_basis_invalid"):
        report_basis.require_runner_shape()
    with pytest.raises(ValidationError):
        basis(*[(f"source-{i}", "a") for i in range(21)])


def test_basis_digest_cannot_confuse_delimiter_content():
    assert basis(("a:b", "c")).digest != basis(("a", "b:c")).digest
    assert basis(("á", "1")).digest == basis(("á", "1")).digest


@pytest.mark.parametrize("field", ["spec_edition", "generation", "ordinal"])
@pytest.mark.parametrize("value", [True, "1", 1.0, 0, -1, MAX_VERIFICATION_COUNTER + 1])
def test_context_counters_require_bounded_positive_integers(field, value):
    with pytest.raises(ValidationError):
        context(**{field: value})


def test_context_roundtrip_is_complete_frozen_and_rejects_unknown_fields():
    value = context(ordinal=MAX_VERIFICATION_COUNTER)
    assert VerificationContext.model_validate_json(value.model_dump_json()) == value
    with pytest.raises(ValidationError):
        context(contract_version="verification-context/v2")
    with pytest.raises(ValidationError):
        context(verified=True)
    with pytest.raises(ValidationError):
        context(verification_method="unknown")
    with pytest.raises(ValidationError):
        value.generation = 9


@pytest.mark.parametrize("value", [-1, True, "1", 1.0, MAX_VERIFICATION_COUNTER + 1])
def test_counter_refuses_invalid_values_without_coercion(value):
    with pytest.raises(VerificationCurrentnessError, match="verification_context_invalid"):
        next_verification_counter(value)


def test_counter_exhaustion_never_wraps_or_reuses_identity():
    assert next_verification_counter(0) == 1
    assert next_verification_counter(MAX_VERIFICATION_COUNTER - 1) == MAX_VERIFICATION_COUNTER
    with pytest.raises(VerificationCurrentnessError, match="verification_counter_exhausted"):
        next_verification_counter(MAX_VERIFICATION_COUNTER)


def test_independent_bases_coexist_and_delayed_other_basis_keeps_global_summary():
    a, b = result(1, "a"), result(2, "b")
    heads = {}
    for incoming in (b, a):
        plan = publish(incoming, heads.get(incoming.basis.digest), summary=2 if heads else 0)
        assert plan.append_acceptance and plan.replace_basis_head and plan.current
        assert plan.replace_operational_summary == (incoming is b)
        heads[incoming.basis.digest] = incoming
    assert set(heads.values()) == {a, b}


@pytest.mark.parametrize("outcome", ["failed", "inconclusive", "aborted", "unavailable"])
def test_nonpassing_result_replaces_only_its_full_basis(outcome):
    a, b = result(1, "a"), result(2, "b")
    newer_b = result(3, "b", outcome=outcome)
    heads = {a.basis.digest: a, b.basis.digest: b}
    plan = publish(newer_b, head=heads[newer_b.basis.digest], summary=2)
    assert plan.replace_basis_head and plan.replace_operational_summary
    heads[newer_b.basis.digest] = newer_b
    assert heads[a.basis.digest].outcome == "passed"
    assert heads[b.basis.digest].outcome == outcome


def test_delayed_success_cannot_replace_newer_same_basis_failure():
    older, newer = result(1), result(2, outcome="failed")
    with pytest.raises(VerificationCurrentnessError, match="verification_result_superseded"):
        publish(older, head=newer, summary=2)


def test_exact_replay_changes_no_authority_and_stale_history_gains_no_credit():
    old, current = result(1), result(2, outcome="failed")
    replay = publish(old, head=old, prior=old, summary=1)
    assert replay.replayed and replay.current
    assert not any((replay.append_acceptance, replay.replace_basis_head, replay.replace_operational_summary))
    stale_replay = publish(old, head=current, prior=old, summary=2)
    assert stale_replay.replayed and not stale_replay.current
    assert not stale_replay.replace_basis_head


def test_reservation_cannot_be_reused_for_another_basis_or_payload():
    accepted = result()
    for changed in (result(revision="other"), result(outcome="failed"),
                    result(evidence_sha256="c" * 64), result(receipt_id="other")):
        with pytest.raises(VerificationCurrentnessError, match="verification_currentness_conflict"):
            publish(changed, prior=accepted)


@pytest.mark.parametrize("change", [
    {"actor_id": "another"}, {"actor_kind": "agent"},
    {"context": context(reservation_id="another")},
    {"context": context(ordinal=2)},
])
def test_native_reservation_binds_actual_actor_and_context(change):
    original = result()
    reserved = VerificationReservation(context=original.context, actor_id="actor-a", actor_kind="human")
    with pytest.raises(VerificationCurrentnessError, match="verification_reservation_invalid"):
        publish(result(**change), reservation=reserved)


@pytest.mark.parametrize("change", [
    {"generation": 2}, {"scenario_sha256": "c" * 64},
    {"verification_method": "inspection"}, {"scope": context(spec_edition=3).scope},
    {"scope": context(board_id="foreign").scope},
    {"scope": context(spec_id="foreign").scope},
    {"scope": context(scenario_id="foreign").scope},
])
def test_current_scope_fences_apply_even_to_exact_replay(change):
    accepted = result()
    with pytest.raises(VerificationCurrentnessError, match="verification_context_stale"):
        publish(accepted, head=accepted, prior=accepted, **change)


def test_restoring_identical_semantics_after_reset_does_not_revive_receipt():
    accepted = result()
    with pytest.raises(VerificationCurrentnessError, match="verification_context_stale"):
        publish(accepted, prior=accepted, generation=3, scenario_sha256=accepted.context.scenario_sha256)


def test_wrong_basis_head_is_unavailable_instead_of_inferred_as_a_pass():
    with pytest.raises(VerificationCurrentnessError, match="verification_currentness_unavailable"):
        publish(result(2, "a"), head=result(1, "b"))


def test_same_ordinal_conflict_does_not_replace_current_result():
    with pytest.raises(VerificationCurrentnessError, match="verification_currentness_conflict"):
        publish(result(receipt_id="other"), head=result(), summary=1)


@pytest.mark.parametrize("summary", [-1, True, "1", MAX_VERIFICATION_COUNTER + 1])
def test_invalid_operational_summary_is_not_an_empty_head(summary):
    with pytest.raises(VerificationCurrentnessError, match="verification_currentness_unavailable"):
        publish(result(), summary=summary)


def test_both_evidence_provider_requests_can_carry_the_reserved_context():
    reserved = context()
    runner = ExecutionRequest(
        board_id="board-a", spec_id="spec-a", scenario_id="scenario-a", status="passed",
        manifest_ref="registered-manifest", actor_id="actor-a", scenario_sha256="a" * 64,
        verification_context=reserved,
    )
    report = ReportRequest(
        board_id="board-a", spec_id="spec-a", scenario_id="scenario-a",
        scenario_sha256="a" * 64, actor_id="actor-a", report={}, verification_context=reserved,
    )
    assert runner.verification_context is reserved
    assert report.verification_context is reserved


def test_native_semantic_digest_has_a_lossless_context_representation():
    from okto_pulse.core.services.test_scenario_lifecycle import (
        compute_test_scenario_semantic_sha256,
    )

    original = compute_test_scenario_semantic_sha256(
        board_id="board-a", spec_id="spec-a",
        scenario={"id": "scenario-a", "given": "A", "when": "B", "then": "C",
                  "linked_criteria": [], "verification_method": "automated_test"},
        acceptance_criteria=[],
    )
    reserved = context(scenario_sha256=canonical_scenario_sha256(original))
    assert original == "sha256:" + reserved.scenario_sha256
    assert canonical_scenario_sha256(reserved.scenario_sha256) == reserved.scenario_sha256


@pytest.mark.parametrize("digest", [None, "", "sha1:" + "a" * 64,
                                     "sha256:sha256:" + "a" * 64, "A" * 64,
                                     " " + "a" * 64, "a" * 63, "a" * 65])
def test_semantic_digest_translation_rejects_ambiguous_input(digest):
    with pytest.raises(VerificationCurrentnessError, match="verification_context_invalid"):
        canonical_scenario_sha256(digest)
