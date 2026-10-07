"""Single-owner, complete-source contracts for the native overlap projection."""
import pytest
from pydantic import ValidationError

from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker
from okto_pulse.core.events.handlers.consolidation_enqueuer import ConsolidationEnqueuer
from okto_pulse.core.events.types import ImplementationTargetUpdated
from okto_pulse.core.ports.code_evidence_projection import (
    TARGET_OVERLAP_NAMESPACE, TARGET_OVERLAP_RULE, TargetOverlapFamily,
)
from test_code_traceability_events_kg import _target


@pytest.mark.parametrize("damage", ["absent", "null", "mapping"])
def test_incomplete_overlap_source_cannot_authorize_retraction(damage):
    source = _target()
    if damage == "absent":
        source.pop("overlap_target_ids")
    else:
        source["overlap_target_ids"] = None if damage == "null" else {}
    with pytest.raises(ValueError, match="target_overlaps_incomplete"):
        DeterministicWorker().process_implementation_target(source)


def test_only_first_target_emits_and_owns_each_overlap():
    source = {**_target(), "overlap_target_ids": ["target-0", "target-2"]}
    result = DeterministicWorker().process_implementation_target(source)
    edges = [edge for edge in result.edges if edge.rule_id == TARGET_OVERLAP_RULE]
    assert len(edges) == 1 and edges[0].to_candidate_id == "kgref:Entity:implementation_target:target-2"
    intent, = [intent for intent in result.relational_projection_active_set_intents
        if intent.namespace == TARGET_OVERLAP_NAMESPACE]
    assert intent.owner_id == "target-1"
    assert [ref.candidate_id for ref in intent.active_edges] == [edge.candidate_id for edge in edges]
    source["overlap_target_ids"] = ["target-0"]
    empty = DeterministicWorker().process_implementation_target(source)
    assert not [edge for edge in empty.edges if edge.rule_id == TARGET_OVERLAP_RULE]
    intent, = [intent for intent in empty.relational_projection_active_set_intents
        if intent.namespace == TARGET_OVERLAP_NAMESPACE]
    assert intent.active_edges == ()


@pytest.mark.parametrize("target_type,target_ref,expected", [
    ("Entity", "implementation_target:target-2", True),
    ("Entity", "implementation_target:target-0", False),
    ("Entity", "implementation_target:target-1", False),
    ("Bug", "implementation_target:target-2", False),
    ("Entity", "code_evidence:target-2", False),
    ("Entity", "implementation_target:target-2:foreign", False),
])
def test_pair_owner_is_exact_and_cannot_prune_another_direction(target_type, target_ref, expected):
    assert TargetOverlapFamily().owns_endpoints(owner_id="target-1", source_type="Entity",
        target_type=target_type, source_ref="implementation_target:target-1", target_ref=target_ref) is expected


def event(**changes):
    return ImplementationTargetUpdated(board_id="board", actor_id="owner",
        target_id="target-z", card_id="card", lifecycle_status="active",
        previous_revision=1, revision=2, change_reason_sha256="a" * 64,
        **({"overlap_projection_owner_ids": ("target-a", "target-z")} | changes))


def test_event_enqueues_old_and_new_pair_owners_once_without_a_board_sweep():
    assert ConsolidationEnqueuer()._map_targets(event()) == [
        ("implementation_target", "target-a"), ("implementation_target", "target-z")]


def test_owner_event_snapshot_is_required_bounded_and_immutable():
    payload = event().model_dump()
    payload.pop("overlap_projection_owner_ids")
    with pytest.raises(ValidationError):
        ImplementationTargetUpdated.model_validate(payload)
    with pytest.raises(ValidationError):
        event(overlap_projection_owner_ids=tuple(f"target-{i}" for i in range(401)))
    with pytest.raises(ValidationError):
        event().overlap_projection_owner_ids = ()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["mutation", "replay", "failed_source"])
async def test_resolution_event_uses_union_of_pair_owners_and_preserves_replay(monkeypatch, mode):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from okto_pulse.core.application.use_cases import code_traceability as use_cases
    from okto_pulse.core.application.use_cases.base import ActorContext

    # Authority/proof admission are covered by the existing use-case/service
    # suites; this test isolates orchestration after successful authorization.
    monkeypatch.setattr(use_cases, "_load_policy", AsyncMock(return_value=SimpleNamespace(
        minimum_trust="single_attestation", require_committed_state=False)))
    monkeypatch.setattr(use_cases, "_authorize", AsyncMock())
    monkeypatch.setattr(use_cases, "_require_attestor_policy", AsyncMock())
    monkeypatch.setattr(use_cases, "_resolution_card_version_or_replay", AsyncMock(return_value=1))
    prior = SimpleNamespace(subject_version=1) if mode == "replay" else None
    store = SimpleNamespace(
        resolve_resolution_replay=AsyncMock(return_value=prior),
        get_target=AsyncMock(return_value=SimpleNamespace(
            id="target-m", board_id="board", card_id="card")))
    old = SimpleNamespace(board_id="board", target_a_id="target-a", target_b_id="target-m")
    new = SimpleNamespace(board_id="board", target_a_id="target-m", target_b_id="target-z")
    reader = SimpleNamespace(overlap_report=AsyncMock(side_effect=[
        (old,), RuntimeError("current overlap source unavailable") if mode == "failed_source" else (new,)]))
    publish = AsyncMock()
    uow = SimpleNamespace(services=SimpleNamespace(code_traceability=store,
        code_traceability_read=reader, code_investigations=object(),
        publish_domain_event=publish), commit=AsyncMock())
    result = SimpleNamespace(replayed=mode == "replay", resolution=SimpleNamespace(
        target_id="target-m", id="resolution", investigation_receipt_id="receipt",
        state=SimpleNamespace(value="resolved"), target_revision=1, receipt_generation=1,
        candidate_count=0, selector_fingerprint="a" * 64, payload_sha256="b" * 64))
    service = SimpleNamespace(submit_resolution=AsyncMock(return_value=result))
    command = SimpleNamespace(board_id="board", card_id="card", target_id="target-m",
        investigation_receipt_id="receipt", idempotency_key="request")
    execute = use_cases.SubmitImplementationTargetResolutionUseCase(object(), service).execute
    actor = ActorContext("agent", "mcp", actor_kind="agent", board_id="board")
    if mode == "failed_source":
        with pytest.raises(RuntimeError, match="current overlap source unavailable"):
            await execute(command, actor=actor, uow=uow)
        publish.assert_not_awaited()
        uow.commit.assert_not_awaited()
    else:
        assert await execute(command, actor=actor, uow=uow) is result
        if mode == "replay":
            reader.overlap_report.assert_not_awaited()
            store.get_target.assert_not_awaited()
            publish.assert_not_awaited()
            uow.commit.assert_not_awaited()
        else:
            emitted = publish.await_args.args[0]
            assert emitted.overlap_projection_owner_ids == ("target-a", "target-m")
            assert ConsolidationEnqueuer()._map_targets(emitted) == [
                ("implementation_target", "target-a"), ("implementation_target", "target-m")]
            assert reader.overlap_report.await_count == 2
            uow.commit.assert_awaited_once()
