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


@pytest.fixture
def issuance_boundary(monkeypatch):
    """Isolated ports test orchestration, not receipt authentication or persistence."""
    from types import SimpleNamespace

    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.application.use_cases.spec_crud import (
        AdmitTestVerificationReportCommand,
        AdmitTestVerificationReportUseCase,
        ExecuteTestScenarioEvidenceCommand,
        ExecuteTestScenarioEvidenceUseCase,
    )
    from okto_pulse.core.domain.permissions import PermissionSet
    from okto_pulse.core.ports import test_evidence as evidence_ports
    from okto_pulse.core.ports import verification_currentness as currentness_ports

    state = SimpleNamespace(events=[], reservation_change={}, issued_change={},
                            fail_issuer=False, verified=True, omit_context=False)
    scenario = {"id": "scenario-a", "given": "A", "when": "B", "then": "C",
                "linked_criteria": ["ac-1"], "verification_method": "automated_test"}
    spec = SimpleNamespace(id="spec-a", board_id="board-a", edition=2, version=7,
                           test_scenarios=[scenario], acceptance_criteria=[{"id": "ac-1", "text": "A"}])

    async def get_spec(spec_id):
        state.events.append("scope")
        return spec if spec_id == spec.id else None

    class ReservationPort:
        async def reserve(self, request):
            state.events.append("reserve")
            state.request = request
            reserved_context = context(**request.scope.model_dump(),
                scenario_sha256=request.scenario_sha256, verification_method=request.verification_method)
            state.reservation = VerificationReservation.model_validate({
                "context": reserved_context, "actor_id": request.actor_id,
                "actor_kind": request.actor_kind, **state.reservation_change})
            return state.reservation

    class Issuer:
        async def execute(self, request):
            state.events.append("issue")
            state.issue_request = request
            if state.fail_issuer:
                raise RuntimeError("execution_failed_once")
            payload = {} if state.omit_context else {
                "verification_context": request.verification_context.model_dump(mode="json") | state.issued_change}
            return evidence_ports.TestEvidenceExecutionResult(payload)

        admit = execute

    class Verifier:
        verification_methods = frozenset({"automated_test", "inspection"})

        def verify(self, **request):
            state.events.append("verify")
            return evidence_ports.TestEvidenceWriteVerification(state.verified, () if state.verified else ("invalid_signature",))

    issuer = Issuer()
    monkeypatch.setattr(evidence_ports, "resolve_test_evidence_execution_issuer", lambda: issuer)
    monkeypatch.setattr(evidence_ports, "resolve_test_verification_report_issuer", lambda: issuer)
    monkeypatch.setattr(evidence_ports, "resolve_test_evidence_write_verifier", lambda: Verifier())
    state.port = ReservationPort()
    monkeypatch.setattr(currentness_ports, "resolve_verification_reservation_port", lambda: state.port)

    async def invoke(method="automated_test", *, actor_kind="agent", permitted=True, board="board-a"):
        scenario["verification_method"] = method
        actor = ActorContext("actor-a", "mcp", actor_kind=actor_kind, board_id=board,
            permissions=PermissionSet({"spec": {"tests": {"execute": permitted, "update_status": permitted}}}))
        uow = SimpleNamespace(services=SimpleNamespace(specs=SimpleNamespace(get_spec=get_spec)))
        if method == "automated_test":
            command = ExecuteTestScenarioEvidenceCommand("spec-a", "scenario-a", "passed", "registered.json")
            use_case = ExecuteTestScenarioEvidenceUseCase()
        else:
            source = {"reference": "repo:component", "revision": "commit-a", "sha256": "a" * 64}
            report = {"schema_version": "verification-report/v1", "method": "inspection",
                "report_id": "report-a", "observed_at": "2026-11-05T00:00:00Z", "sources": [source],
                "inspection_procedure": source, "result": "passed", "conclusion": "Observed",
                "observations": [{"observation_id": "o1", "criterion_id": "ac-1",
                    "observation_ref": "repo:component#A", "expected": "A", "observed": "A", "outcome": "passed"}]}
            command = AdmitTestVerificationReportCommand("spec-a", "scenario-a", report)
            use_case = AdmitTestVerificationReportUseCase()
        return await use_case.execute(command, actor=actor, uow=uow)

    state.invoke = invoke
    return state


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["automated_test", "inspection"])
@pytest.mark.parametrize("actor_kind", ["human", "agent"])
async def test_native_issuance_reserves_before_execution_and_preserves_identity(issuance_boundary, method, actor_kind):
    state = issuance_boundary
    issued = await state.invoke(method, actor_kind=actor_kind)
    assert state.events == ["scope", "reserve", "issue", "verify"]
    assert state.request.actor_id == "actor-a" and state.request.actor_kind == actor_kind
    assert state.request.expected_spec_version == 7
    assert state.request.scope.spec_edition == 2
    assert state.issue_request.verification_context == state.reservation.context
    assert issued.evidence["verification_context"] == state.reservation.context.model_dump(mode="json")


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["automated_test", "inspection"])
@pytest.mark.parametrize("failure", ["denied", "foreign", "missing_port", "wrong_actor", "wrong_kind"])
async def test_scope_authorization_and_reservation_fail_before_any_execution(issuance_boundary, method, failure):
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError, PermissionDeniedError

    state = issuance_boundary
    args = {}
    if failure == "denied":
        args["permitted"] = False
    elif failure == "foreign":
        args["board"] = "another-board"
    elif failure == "missing_port":
        state.port = None
    else:
        state.reservation_change = {"actor_id": "another"} if failure == "wrong_actor" else {"actor_kind": "human"}
    error = EntityNotFoundError if failure == "foreign" else (
        PermissionDeniedError if failure == "denied" else VerificationCurrentnessError)
    with pytest.raises(error):
        await state.invoke(method, **args)
    assert "issue" not in state.events and "verify" not in state.events
    if failure in {"denied", "foreign", "missing_port"}:
        assert "reserve" not in state.events


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["automated_test", "inspection"])
@pytest.mark.parametrize("change", [{"ordinal": 2}, {"generation": 2}, {"spec_edition": 3},
                                    {"reservation_id": "another"}, {"scenario_sha256": "f" * 64},
                                    {"scenario_id": "another"}, None])
async def test_compatible_issuer_must_return_exact_reserved_context(issuance_boundary, method, change):
    state = issuance_boundary
    state.omit_context = change is None
    state.issued_change = change or {}
    with pytest.raises(VerificationCurrentnessError, match="verification_reservation_invalid"):
        await state.invoke(method)
    assert state.events == ["scope", "reserve", "issue", "verify"]


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["automated_test", "inspection"])
@pytest.mark.parametrize("failure", ["issuer", "authentication"])
async def test_issuance_failure_never_retries_execution(issuance_boundary, method, failure):
    state = issuance_boundary
    state.fail_issuer = failure == "issuer"
    state.verified = failure != "authentication"
    with pytest.raises((RuntimeError, ValueError), match="execution_failed_once|evidence_unverified"):
        await state.invoke(method)
    assert state.events.count("issue") == 1 and state.events.count("reserve") == 1
