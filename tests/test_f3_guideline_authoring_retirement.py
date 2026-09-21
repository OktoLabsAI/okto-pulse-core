"""Retired targets remain readable, but cannot enter a new normative head."""

from dataclasses import replace
from datetime import timedelta
import json

import pytest

from okto_pulse.core.application.use_cases.policy_governance import (
    CreateGuidelineRevisionCommand, CreateGuidelineRevisionUseCase,
    METRICS_AUTHOR, REVISIONS_CREATE,
)
from okto_pulse.core.domain.guideline_lifecycle import (
    GuidelineLifecycleError, GuidelinePatchCommand, GuidelineRevisionPatch,
    execute_guideline_patch,
)
from okto_pulse.core.domain.guideline_policy import PolicyEntityType
from okto_pulse.core.domain.guideline_import_export import (
    GuidelineExportSnapshot, build_guideline_export_v3, guideline_export_payload,
    GuidelineImportTransactionStatus, guideline_export_json_bytes,
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
    return replace(_metric(), target_entity_types=(PolicyEntityType.SPEC, PolicyEntityType.SPRINT))


def _historical_port():
    port = _UnboundGlobalPolicyPort()
    port.revision = replace(port.revision, metrics=(_historical_metric(),), revision_digest=None)
    port.revisions[port.revision.revision_id] = port.revision
    return port


@pytest.mark.asyncio
@pytest.mark.parametrize("inherited", [False, True])
async def test_new_revision_refuses_explicit_or_inherited_retired_targets(inherited):
    port = _historical_port() if inherited else _UnboundGlobalPolicyPort()
    uow = _GovernanceUow(port)
    patch = GuidelineRevisionPatch(title="Next") if inherited else GuidelineRevisionPatch(metrics=(_historical_metric(),))
    before = dict(port.revisions)
    with pytest.raises(GuidelineLifecycleError, match="guideline_metric_target_type_retired"):
        await CreateGuidelineRevisionUseCase().execute(
            CreateGuidelineRevisionCommand("board-1", "guideline-1", patch, "new-key", occurred_at=NOW + timedelta(seconds=1)),
            actor=_owner_actor(REVISIONS_CREATE, METRICS_AUTHOR), uow=uow,
        )
    assert port.revisions == before
    assert port.append_count == uow.commit_count == 0


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


@pytest.mark.parametrize("dry_run", [False, True])
def test_import_reports_retired_live_head_without_rewriting_history(dry_run):
    old = replace(_revision(), metrics=(_historical_metric(),), revision_digest=None)
    aggregate = _aggregate(revisions=(old,), bindings=())
    envelope = _envelope(aggregate)
    original_bytes = guideline_export_json_bytes(envelope)
    assert guideline_export_json_bytes(parse_guideline_export(json.loads(original_bytes))) == original_bytes
    plan = plan_guideline_import(envelope, existing_aggregates=(), dry_run=dry_run, target_owner_id="actor-1")
    assert plan.transaction_status is GuidelineImportTransactionStatus.ROLLED_BACK
    assert "guideline_metric_target_type_retired" in plan.entries[0].identity_conflicts
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
async def test_import_use_case_rolls_back_whole_batch_including_same_id_versioning(same_id, dry_run):
    old = replace(_revision(), metrics=(_historical_metric(),), revision_digest=None)
    source = _aggregate(revisions=(old,), bindings=())
    valid = _aggregate(guideline_id="guideline-2", bindings=())
    envelope = build_guideline_export_v3(GuidelineExportSnapshot(aggregates=(source, valid)), exported_at=NOW + timedelta(days=1))
    existing = _aggregate(bindings=())
    port = _Port(snapshot=GuidelineExportSnapshot(aggregates=(existing,) if same_id else ()))
    uow = _Uow(port)
    result = await ImportGuidelinePolicyUseCase().execute(
        ImportGuidelinePolicyCommand(envelope=guideline_export_payload(envelope), dry_run=dry_run),
        actor=ACTOR, uow=uow,
    )
    assert result.result.transaction_status is GuidelineImportTransactionStatus.ROLLED_BACK
    assert "guideline_metric_target_type_retired" in result.plan.entries[0].identity_conflicts
    assert not port.apply_calls
    assert len(result.plan.entries) == 2 and not result.plan.entries[1].has_conflict
    assert uow.commit_count == 0 and uow.rollback_count == 1
    if same_id:
        assert "same_id_import_version_bump" in result.plan.entries[0].aggregate.migration_notes
        assert result.plan.entries[0].aggregate.revisions[0] == existing.revisions[0]
