"""SK-B B04 semantic guideline lifecycle v2 contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.domain.guideline_lifecycle import (
    GUIDELINE_LIFECYCLE_CONTRACT_VERSION,
    GUIDELINE_REQUEST_DIGEST_CONTRACT_VERSION,
    GUIDELINE_REVISION_DIGEST_CONTRACT_VERSION,
    GuidelineBindingApplied,
    GuidelineBindingNoop,
    GuidelineBindingTransitionCommand,
    GuidelineCreateCommand,
    GuidelineLifecycleError,
    GuidelinePatchApplied,
    GuidelinePatchCommand,
    GuidelinePatchNoop,
    GuidelinePatchRejected,
    GuidelineRetirementCommand,
    GuidelineRevisionPatch,
    GuidelineVersionBump,
    classify_guideline_change,
    execute_guideline_patch,
    guideline_create_request_digest_v1,
    guideline_revision_content_digest_v2,
    plan_guideline_binding_transition,
    plan_guideline_creation,
    plan_guideline_patch,
    plan_guideline_retirement,
)
from okto_pulse.core.domain.guideline_policy import (
    BoardGuidelineBinding,
    GuidelineBindingState,
    GuidelineEnforcement,
    GuidelineHead,
    GuidelineLifecycleStatus,
    GuidelineMetric,
    GuidelineMetricDirection,
    GuidelineRevision,
    GuidelineScope,
    PolicyEntityType,
)


NOW = datetime(2026, 7, 29, 12, tzinfo=timezone.utc)


def _metric(
    *,
    metric_id: str = "metric-coverage",
    code: str = "quality.coverage",
    title: str = "Evidence coverage",
    description: str = "Scores the completeness of linked evidence.",
    evaluation_rubric: str = "0 means absent; 100 means complete.",
    target_entity_types: tuple[PolicyEntityType, ...] = (PolicyEntityType.SPEC,),
    direction: GuidelineMetricDirection = GuidelineMetricDirection.MINIMUM,
    default_threshold: int = 70,
) -> GuidelineMetric:
    return GuidelineMetric(
        metric_id=metric_id,
        code=code,
        title=title,
        description=description,
        evaluation_rubric=evaluation_rubric,
        target_entity_types=target_entity_types,
        direction=direction,
        default_threshold=default_threshold,
    )


def _revision(
    *,
    metrics: tuple[GuidelineMetric, ...] | None = None,
    semantic_version: str = "1.2.3",
    title: str = "Engineering guidance",
    content: str = "Keep evidence current.",
    tags: tuple[str, ...] = ("architecture", "quality"),
    revision_id: str = "revision-3",
    revision_number: int = 3,
) -> GuidelineRevision:
    return GuidelineRevision(
        revision_id=revision_id,
        guideline_id="guideline-1",
        revision_number=revision_number,
        semantic_version=semantic_version,
        title=title,
        content=content,
        metrics=(_metric(),) if metrics is None else metrics,
        tags=tags,
        created_by="actor-previous",
        created_at=NOW,
        parent_revision_id=(
            None if revision_number == 1 else "revision-previous"
        ),
    )


def _head(revision: GuidelineRevision) -> GuidelineHead:
    return GuidelineHead(
        guideline_id=revision.guideline_id,
        revision_id=revision.revision_id,
        revision_number=revision.revision_number,
        semantic_version=revision.semantic_version,
        head_revision=revision.revision_number,
        updated_at=revision.created_at,
    )


def _patch_command(
    revision: GuidelineRevision,
    patch: GuidelineRevisionPatch,
    *,
    declared: str | None = None,
) -> GuidelinePatchCommand:
    return GuidelinePatchCommand(
        current_revision=revision,
        current_head=_head(revision),
        patch=patch,
        next_revision_id="revision-next",
        actor_id="actor-next",
        occurred_at=NOW + timedelta(minutes=1),
        idempotency_key="patch:key",
        declared_semantic_version=declared,
    )


def _semantic_patch(
    revision: GuidelineRevision,
    case: str,
) -> GuidelineRevisionPatch:
    metric = revision.metrics[0]
    if case == "add":
        added = _metric(
            metric_id="metric-freshness",
            code="quality.freshness",
            title="Evidence freshness",
        )
        return GuidelineRevisionPatch(metrics=(*revision.metrics, added))
    if case == "remove":
        return GuidelineRevisionPatch(metrics=())
    if case == "identity-id":
        return GuidelineRevisionPatch(
            metrics=(replace(metric, metric_id="metric-renamed"),)
        )
    if case == "identity-code":
        return GuidelineRevisionPatch(
            metrics=(replace(metric, code="quality.evidence_coverage"),)
        )
    if case == "direction":
        return GuidelineRevisionPatch(
            metrics=(
                replace(metric, direction=GuidelineMetricDirection.MAXIMUM),
            )
        )
    if case == "targets":
        return GuidelineRevisionPatch(
            metrics=(
                replace(
                    metric,
                    target_entity_types=(PolicyEntityType.CARD,),
                ),
            )
        )
    if case == "rubric":
        return GuidelineRevisionPatch(
            metrics=(
                replace(
                    metric,
                    evaluation_rubric="Score against the approved evidence map.",
                ),
            )
        )
    if case == "tighten":
        return GuidelineRevisionPatch(
            metrics=(replace(metric, default_threshold=80),)
        )
    if case == "relax":
        return GuidelineRevisionPatch(
            metrics=(replace(metric, default_threshold=60),)
        )
    if case == "editorial":
        return GuidelineRevisionPatch(
            content="Keep evidence current and clearly linked."
        )
    raise AssertionError(f"unknown semantic case: {case}")


def _active_binding_command(
    *,
    expected: int | None,
    occurred_at: datetime,
    enforcement: GuidelineEnforcement = GuidelineEnforcement.BLOCKING,
    minimum_confidence: int = 85,
    metric_threshold_overrides: Mapping[str, int] | None = None,
) -> GuidelineBindingTransitionCommand:
    overrides = (
        {"quality.coverage": 82}
        if metric_threshold_overrides is None
        else metric_threshold_overrides
    )
    return GuidelineBindingTransitionCommand(
        binding_id="binding-1",
        board_id="board-1",
        guideline_id="guideline-1",
        state=GuidelineBindingState.ACTIVE,
        actor_id="actor-binding",
        occurred_at=occurred_at,
        idempotency_key=f"binding:{expected}:{occurred_at.isoformat()}",
        expected_binding_revision=expected,
        revision_id="revision-3",
        semantic_version="1.2.3",
        revision_digest="a" * 64,
        priority=2,
        enforcement=enforcement,
        minimum_confidence=minimum_confidence,
        metric_threshold_overrides=overrides,
    )


def _unlink_command(
    *,
    expected: int,
    occurred_at: datetime,
) -> GuidelineBindingTransitionCommand:
    return GuidelineBindingTransitionCommand(
        binding_id="binding-1",
        board_id="board-1",
        guideline_id="guideline-1",
        state=GuidelineBindingState.UNLINKED,
        actor_id="actor-binding",
        occurred_at=occurred_at,
        idempotency_key=f"unlink:{expected}",
        expected_binding_revision=expected,
    )


def _initial_binding() -> BoardGuidelineBinding:
    result = plan_guideline_binding_transition(
        _active_binding_command(expected=None, occurred_at=NOW),
        current=None,
    )
    assert isinstance(result, GuidelineBindingApplied)
    return result.binding


def test_v2_contract_versions_and_revision_digest_are_stable() -> None:
    revision = _revision()

    assert GUIDELINE_LIFECYCLE_CONTRACT_VERSION == "guideline-lifecycle/v2"
    assert (
        GUIDELINE_REVISION_DIGEST_CONTRACT_VERSION
        == "guideline-revision-digest/v2"
    )
    assert (
        GUIDELINE_REQUEST_DIGEST_CONTRACT_VERSION
        == "guideline-request-digest/v2"
    )
    assert revision.revision_digest == guideline_revision_content_digest_v2(
        semantic_version=revision.semantic_version,
        title=revision.title,
        content=revision.content,
        metrics=revision.metrics,
        tags=revision.tags,
    )
    assert (
        revision.revision_digest
        == "2d7d0902f5928365db3486827a1864410c2101cbb1eb908e94de8114bf52de2e"
    )
    assert revision.revision_digest != guideline_revision_content_digest_v2(
        semantic_version="1.2.4",
        title=revision.title,
        content=revision.content,
        metrics=revision.metrics,
        tags=revision.tags,
    )
    assert revision.revision_digest != guideline_revision_content_digest_v2(
        semantic_version=revision.semantic_version,
        title=revision.title,
        content=revision.content,
        metrics=(
            replace(
                revision.metrics[0],
                evaluation_rubric="A semantically different rubric.",
            ),
        ),
        tags=revision.tags,
    )


@pytest.mark.parametrize(
    ("case", "minimum_bump", "minimum_version"),
    (
        pytest.param("add", GuidelineVersionBump.MINOR, "1.3.0", id="add-metric"),
        pytest.param(
            "remove",
            GuidelineVersionBump.MAJOR,
            "2.0.0",
            id="remove-metric",
        ),
        pytest.param(
            "identity-id",
            GuidelineVersionBump.MAJOR,
            "2.0.0",
            id="change-metric-id",
        ),
        pytest.param(
            "identity-code",
            GuidelineVersionBump.MAJOR,
            "2.0.0",
            id="change-metric-code",
        ),
        pytest.param(
            "direction",
            GuidelineVersionBump.MAJOR,
            "2.0.0",
            id="change-direction",
        ),
        pytest.param(
            "targets",
            GuidelineVersionBump.MAJOR,
            "2.0.0",
            id="change-targets",
        ),
        pytest.param(
            "rubric",
            GuidelineVersionBump.MAJOR,
            "2.0.0",
            id="change-rubric",
        ),
        pytest.param(
            "tighten",
            GuidelineVersionBump.MAJOR,
            "2.0.0",
            id="tighten-threshold",
        ),
        pytest.param(
            "relax",
            GuidelineVersionBump.MINOR,
            "1.3.0",
            id="relax-threshold",
        ),
        pytest.param(
            "editorial",
            GuidelineVersionBump.PATCH,
            "1.2.4",
            id="editorial",
        ),
    ),
)
def test_semver_matrix_is_exact(
    case: str,
    minimum_bump: GuidelineVersionBump,
    minimum_version: str,
) -> None:
    revision = _revision()
    plan = plan_guideline_patch(revision, _semantic_patch(revision, case))

    assert (
        classify_guideline_change(
            revision,
            title=plan.title,
            content=plan.content,
            tags=plan.tags,
            metrics=plan.metrics,
        )
        is minimum_bump
    )
    assert plan.minimum_bump is minimum_bump
    assert plan.semantic_version == minimum_version


def test_create_builds_initial_revision_with_absence_cas_fence() -> None:
    command = GuidelineCreateCommand(
        guideline_id="guideline-created",
        revision_id="revision-created-1",
        owner_id="owner-1",
        scope=GuidelineScope.GLOBAL,
        title="Semantic quality",
        content="Assess quality through authored metrics.",
        created_by="actor-create",
        created_at=NOW,
        idempotency_key="create:key",
        tags=("quality", "architecture"),
        metrics=(_metric(),),
    )

    result = plan_guideline_creation(command)

    assert result.expected_head_revision == 0
    assert result.revision.revision_number == 1
    assert result.revision.semantic_version == "1.0.0"
    assert result.revision.metrics == command.metrics
    assert result.revision.revision_digest == guideline_revision_content_digest_v2(
        semantic_version="1.0.0",
        title=command.title,
        content=command.content,
        metrics=command.metrics,
        tags=command.tags,
    )
    assert result.head.head_revision == 1
    assert result.head.revision_id == result.revision.revision_id
    assert result.request_digest == guideline_create_request_digest_v1(command)


def test_patch_noop_has_exact_cas_fence_and_no_write_bundle() -> None:
    revision = _revision()
    command = _patch_command(
        revision,
        GuidelineRevisionPatch(
            title=f"  {revision.title}  ",
            metrics=revision.metrics,
        ),
    )

    result = execute_guideline_patch(command)

    assert isinstance(result, GuidelinePatchNoop)
    assert result.revision is None
    assert result.head is None
    assert result.expected_head_revision == command.current_head.head_revision
    assert result.expected_revision_id == revision.revision_id
    assert result.expected_revision_number == revision.revision_number
    assert result.expected_semantic_version == revision.semantic_version
    assert result.expected_revision_digest == revision.revision_digest


def test_patch_applies_with_exact_cas_fence_and_rejects_stale_snapshot() -> None:
    revision = _revision()
    command = _patch_command(
        revision,
        GuidelineRevisionPatch(content="Keep evidence current and attributable."),
    )

    result = execute_guideline_patch(command)

    assert isinstance(result, GuidelinePatchApplied)
    assert result.minimum_bump is GuidelineVersionBump.PATCH
    assert result.revision.semantic_version == "1.2.4"
    assert result.revision.parent_revision_id == revision.revision_id
    assert result.expected_head_revision == command.current_head.head_revision
    assert result.expected_revision_id == revision.revision_id
    assert result.expected_revision_number == revision.revision_number
    assert result.expected_semantic_version == revision.semantic_version
    assert result.expected_revision_digest == revision.revision_digest
    assert result.head.head_revision == command.current_head.head_revision + 1

    stale_head = replace(_head(revision), revision_id="revision-stale")
    with pytest.raises(
        GuidelineLifecycleError,
        match="guideline_patch_snapshot_mismatch",
    ):
        GuidelinePatchCommand(
            current_revision=revision,
            current_head=stale_head,
            patch=GuidelineRevisionPatch(content="A stale edit."),
            next_revision_id="revision-stale-next",
            actor_id="actor-next",
            occurred_at=NOW + timedelta(minutes=1),
            idempotency_key="patch:stale",
        )


def test_patch_underbump_is_typed_and_has_no_write_bundle() -> None:
    revision = _revision()
    command = _patch_command(
        revision,
        _semantic_patch(revision, "rubric"),
        declared="1.3.0",
    )

    result = execute_guideline_patch(command)

    assert isinstance(result, GuidelinePatchRejected)
    assert result.code == "guideline_semver_below_minimum"
    assert result.minimum_bump is GuidelineVersionBump.MAJOR
    assert result.minimum_semantic_version == "2.0.0"
    assert result.declared_semantic_version == "1.3.0"
    assert result.revision is None
    assert result.head is None
    assert result.expected_revision_digest == revision.revision_digest


def test_patch_prerelease_below_stable_minimum_is_underbumped() -> None:
    revision = _revision()
    command = _patch_command(
        revision,
        _semantic_patch(revision, "editorial"),
        declared="1.2.4-alpha.1",
    )

    result = execute_guideline_patch(command)

    assert isinstance(result, GuidelinePatchRejected)
    assert result.code == "guideline_semver_below_minimum"
    assert result.minimum_bump is GuidelineVersionBump.PATCH
    assert result.minimum_semantic_version == "1.2.4"
    assert result.declared_semantic_version == "1.2.4-alpha.1"
    assert result.revision is None
    assert result.head is None


def test_binding_create_and_exact_snapshot_noop() -> None:
    create = _active_binding_command(expected=None, occurred_at=NOW)

    created = plan_guideline_binding_transition(create, current=None)

    assert isinstance(created, GuidelineBindingApplied)
    binding = created.binding
    assert binding.binding_revision == 1
    assert binding.enforcement is GuidelineEnforcement.BLOCKING
    assert binding.minimum_confidence == 85
    assert dict(binding.metric_threshold_overrides) == {"quality.coverage": 82}
    assert binding.configuration_digest is not None

    same_snapshot = _active_binding_command(
        expected=1,
        occurred_at=NOW + timedelta(minutes=1),
    )
    noop = plan_guideline_binding_transition(
        same_snapshot,
        current=binding,
    )
    assert isinstance(noop, GuidelineBindingNoop)
    assert noop.binding is None
    assert noop.current_binding is binding
    assert noop.expected_binding_revision == 1


@pytest.mark.parametrize(
    "changed_field",
    ("enforcement", "minimum_confidence", "metric_threshold_overrides"),
)
def test_binding_semantic_configuration_changes_append_revision(
    changed_field: str,
) -> None:
    current = _initial_binding()
    enforcement = GuidelineEnforcement.BLOCKING
    minimum_confidence = 85
    overrides: Mapping[str, int] = {"quality.coverage": 82}
    if changed_field == "enforcement":
        enforcement = GuidelineEnforcement.ADVISORY
    elif changed_field == "minimum_confidence":
        minimum_confidence = 90
    else:
        overrides = {"quality.coverage": 88}
    command = _active_binding_command(
        expected=1,
        occurred_at=NOW + timedelta(minutes=1),
        enforcement=enforcement,
        minimum_confidence=minimum_confidence,
        metric_threshold_overrides=overrides,
    )

    changed = plan_guideline_binding_transition(command, current=current)

    assert isinstance(changed, GuidelineBindingApplied)
    assert changed.expected_binding_revision == 1
    assert changed.binding.binding_revision == 2
    assert changed.binding.enforcement is enforcement
    assert changed.binding.minimum_confidence == minimum_confidence
    assert dict(changed.binding.metric_threshold_overrides) == dict(overrides)
    assert changed.binding.configuration_digest != current.configuration_digest


def test_binding_unlink_preserves_exact_snapshot_and_repeated_unlink_is_noop() -> None:
    current = _initial_binding()

    unlinked = plan_guideline_binding_transition(
        _unlink_command(
            expected=1,
            occurred_at=NOW + timedelta(minutes=1),
        ),
        current=current,
    )

    assert isinstance(unlinked, GuidelineBindingApplied)
    tombstone = unlinked.binding
    assert tombstone.state is GuidelineBindingState.UNLINKED
    assert tombstone.binding_revision == 2
    assert tombstone.revision_id == current.revision_id
    assert tombstone.semantic_version == current.semantic_version
    assert tombstone.revision_digest == current.revision_digest
    assert tombstone.priority == current.priority
    assert tombstone.enforcement is current.enforcement
    assert tombstone.minimum_confidence == current.minimum_confidence
    assert (
        dict(tombstone.metric_threshold_overrides)
        == dict(current.metric_threshold_overrides)
    )
    assert tombstone.configuration_digest == current.configuration_digest

    repeated = plan_guideline_binding_transition(
        _unlink_command(
            expected=2,
            occurred_at=NOW + timedelta(minutes=2),
        ),
        current=tombstone,
    )
    assert isinstance(repeated, GuidelineBindingNoop)
    assert repeated.binding is None


def test_binding_rejects_mismatched_cas_fence() -> None:
    current = _initial_binding()
    stale = _active_binding_command(
        expected=2,
        occurred_at=NOW + timedelta(minutes=1),
    )

    with pytest.raises(
        GuidelineLifecycleError,
        match="guideline_binding_fence_mismatch",
    ):
        plan_guideline_binding_transition(stale, current=current)


def test_retirement_freezes_exact_head_and_is_terminal() -> None:
    revision = _revision()
    command = GuidelineRetirementCommand(
        current_revision=revision,
        current_head=_head(revision),
        retirement_id="retirement-1",
        status=GuidelineLifecycleStatus.RETIRED,
        reason="Guidance replaced by a new operating model.",
        actor_id="actor-retire",
        occurred_at=NOW + timedelta(minutes=2),
        idempotency_key="retire:key",
    )

    result = plan_guideline_retirement(command)

    assert result.expected_guideline_id == revision.guideline_id
    assert result.expected_head_revision == command.current_head.head_revision
    assert result.expected_revision_id == revision.revision_id
    assert result.expected_revision_number == revision.revision_number
    assert result.expected_semantic_version == revision.semantic_version
    assert result.expected_revision_digest == revision.revision_digest
    assert result.retirement.retired_revision_digest == revision.revision_digest
    assert result.retirement.status is GuidelineLifecycleStatus.RETIRED

    with pytest.raises(GuidelineLifecycleError, match="guideline_is_terminal"):
        plan_guideline_retirement(
            command,
            current_retirement=result.retirement,
        )
    with pytest.raises(GuidelineLifecycleError, match="guideline_is_terminal"):
        execute_guideline_patch(
            _patch_command(
                revision,
                GuidelineRevisionPatch(content="Post-retirement edit."),
            ),
            retirement=result.retirement,
        )
