"""Only native targets enter contracts; native revision history stays immutable."""

from dataclasses import replace
from datetime import timedelta
import json

import pytest

from okto_pulse.core.application.use_cases.policy_governance import (
    CreateGuidelineRevisionCommand, CreateGuidelineRevisionUseCase,
    METRICS_AUTHOR, REVISIONS_CREATE,
)
from okto_pulse.core.domain.guideline_lifecycle import (
    GuidelinePatchCommand, GuidelineRevisionPatch,
    execute_guideline_patch,
)
from okto_pulse.core.domain.guideline_policy import PolicyEntityType, GuidelinePolicyContractError
from okto_pulse.core.domain.guideline_import_export import (
    GuidelineExportSnapshot, build_guideline_export_v3, guideline_export_payload,
    GuidelineImportExportError, guideline_export_json_bytes,
    parse_guideline_export, plan_guideline_import,
)
from okto_pulse.core.ports.guideline_policy import GuidelineRevisionReplay
from test_skb_b12_guideline_import_export_domain import (
    _aggregate, _envelope, _metric, _revision,
)
from test_skb_b13_policy_governance_use_cases import (
    NOW, _GovernanceUow, _UnboundGlobalPolicyPort, _owner_actor,
)
from okto_pulse.core.application.use_cases.guideline_import_export import (
    ImportGuidelinePolicyCommand, ImportGuidelinePolicyUseCase,
)
from test_skb_b12_guideline_import_export_use_case import ACTOR, _Port, _Uow


def _historical_metric():
    return replace(_metric(), target_entity_types=(PolicyEntityType.SPEC, PolicyEntityType.CARD))


def _historical_port():
    port = _UnboundGlobalPolicyPort()
    port.revision = replace(port.revision, metrics=(_historical_metric(),), revision_digest=None)
    port.revisions[port.revision.revision_id] = port.revision
    return port


def test_unsupported_target_cannot_construct_a_metric_or_entity_type():
    with pytest.raises(ValueError, match="not a valid PolicyEntityType"):
        PolicyEntityType("sprint")
    with pytest.raises(GuidelinePolicyContractError, match="guideline_metric_target_entity_types_invalid"):
        replace(_metric(), target_entity_types=("sprint",))


@pytest.mark.asyncio
async def test_explicit_authorized_removal_preserves_old_revision_and_semver_gate():
    port = _historical_port()
    old = port.revision
    uow = _GovernanceUow(port)
    result = await CreateGuidelineRevisionUseCase().execute(
        CreateGuidelineRevisionCommand("board-1", "guideline-1", GuidelineRevisionPatch(metrics=(_metric(),)), "new-key", occurred_at=NOW + timedelta(seconds=1)),
        actor=_owner_actor(REVISIONS_CREATE, METRICS_AUTHOR), uow=uow,
    )
    assert result.revision.metrics == (_metric(),)
    assert result.revision.semantic_version == "2.0.0"
    assert port.revisions[old.revision_id] == old
    assert port.append_count == uow.commit_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit_metrics", [False, True])
async def test_historical_revision_replay_keeps_exact_patch_and_digest(explicit_metrics):
    port = _UnboundGlobalPolicyPort() if explicit_metrics else _historical_port()
    patch = GuidelineRevisionPatch(metrics=(_historical_metric(),)) if explicit_metrics else GuidelineRevisionPatch(title="Historic title")
    candidate = execute_guideline_patch(GuidelinePatchCommand(
        current_revision=port.revision, current_head=port.head, patch=patch,
        next_revision_id="historical-revision-2", actor_id="owner-2",
        occurred_at=NOW + timedelta(seconds=1), idempotency_key="historical-key",
    ))
    port.revision_replays["historical-key"] = GuidelineRevisionReplay(
        revision=candidate.revision, published_head=candidate.head, request_digest=candidate.request_digest,
    )
    uow = _GovernanceUow(port)
    result = await CreateGuidelineRevisionUseCase().execute(
        CreateGuidelineRevisionCommand("board-1", "guideline-1", patch, "historical-key", next_revision_id="historical-revision-2"),
        actor=_owner_actor(REVISIONS_CREATE, *((METRICS_AUTHOR,) if explicit_metrics else ())), uow=uow,
    )
    assert result.revision == candidate.revision
    assert port.append_count == uow.commit_count == 0


def test_import_rejects_unsupported_target_before_digest_or_planning():
    old = replace(_revision(), metrics=(_historical_metric(),), revision_digest=None)
    aggregate = _aggregate(revisions=(old,), bindings=())
    envelope = _envelope(aggregate)
    original_bytes = guideline_export_json_bytes(envelope)
    assert guideline_export_json_bytes(parse_guideline_export(json.loads(original_bytes))) == original_bytes
    payload = json.loads(original_bytes)
    payload["guidelines"][0]["revisions"][0]["metrics"][0]["target_entity_types"] = ["sprint"]
    with pytest.raises(GuidelineImportExportError, match="guideline_export_metric_target_invalid"):
        parse_guideline_export(payload)
    assert guideline_export_json_bytes(envelope) == original_bytes


@pytest.mark.parametrize("historical_kind", ["retired", "older_revision", "identical_replay"])
def test_import_keeps_inert_history_and_identical_replay(historical_kind):
    old = replace(_revision(), metrics=(_historical_metric(),), revision_digest=None)
    revisions = (old,)
    if historical_kind == "older_revision":
        revisions += (_revision(revision_id="revision-2", revision_number=2, semantic_version="2.0.0", parent_revision_id=old.revision_id),)
    aggregate = _aggregate(revisions=revisions, bindings=(), retired=historical_kind == "retired")
    plan = plan_guideline_import(_envelope(aggregate), existing_aggregates=(aggregate,) if historical_kind == "identical_replay" else (), dry_run=False, target_owner_id="actor-1")
    assert plan.can_apply
    assert not plan.entries[0].identity_conflicts
    assert plan.entries[0].aggregate.revisions[0].revision.metrics == old.metrics


@pytest.mark.asyncio
@pytest.mark.parametrize("same_id", [False, True])
@pytest.mark.parametrize("dry_run", [False, True])
async def test_import_use_case_rejects_whole_batch_before_access_including_same_id(same_id, dry_run):
    old = replace(_revision(), metrics=(_historical_metric(),), revision_digest=None)
    source = _aggregate(revisions=(old,), bindings=())
    valid = _aggregate(guideline_id="guideline-2", bindings=())
    envelope = build_guideline_export_v3(GuidelineExportSnapshot(aggregates=(source, valid)), exported_at=NOW + timedelta(days=1))
    existing = _aggregate(bindings=())
    port = _Port(snapshot=GuidelineExportSnapshot(aggregates=(existing,) if same_id else ()))
    uow = _Uow(port)
    payload = guideline_export_payload(envelope)
    # A malformed late item must not let the earlier valid aggregate be applied.
    payload["guidelines"][1]["revisions"][0]["metrics"][0]["target_entity_types"] = ["sprint"]
    with pytest.raises(GuidelineImportExportError, match="guideline_export_metric_target_invalid"):
        await ImportGuidelinePolicyUseCase().execute(
            ImportGuidelinePolicyCommand(envelope=payload, dry_run=dry_run),
            actor=ACTOR, uow=uow,
        )
    assert not port.apply_calls
    assert uow.commit_count == uow.rollback_count == 0
