"""SK-B B04 pure guideline lifecycle and semantic-version contracts."""

from __future__ import annotations

import hashlib
import json
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
    SemanticVersion,
    classify_guideline_change,
    execute_guideline_patch,
    guideline_request_digest_v1,
    guideline_revision_content_digest_v1,
    plan_guideline_binding_transition,
    plan_guideline_creation,
    plan_guideline_patch,
    plan_guideline_retirement,
    validate_binding_transition,
)
from okto_pulse.core.domain.guideline_policy import (
    AdoptedGuidelineRevisionRef,
    BoardGuidelineBinding,
    GuidelineBindingProvenance,
    GuidelineBindingState,
    GuidelineEnforcement,
    GuidelineHead,
    GuidelineLifecycleStatus,
    GuidelinePolicyContractError,
    GuidelinePredicate,
    GuidelineRevision,
    GuidelineRule,
    GuidelineRuleOperator,
    GuidelineScope,
    PolicyEntityType,
)


NOW = datetime(2026, 7, 29, 12, tzinfo=timezone.utc)
_MISSING = object()


def _predicate(
    operator: str = "exists",
    fact: str = "resource_gate_ready",
    *,
    value: object = _MISSING,
) -> GuidelinePredicate:
    parameters: list[tuple[str, object]] = [("fact", fact)]
    if value is not _MISSING:
        parameters.append(("value", value))
    return GuidelinePredicate(operator, parameters)


def _rule(index: int = 1, **overrides) -> GuidelineRule:
    values = {
        "rule_id": f"rule-{index}",
        "code": f"policy.rule_{index}",
        "title": f"Rule {index}",
        "description": f"Deterministic rule {index}.",
        "target_entity_types": (PolicyEntityType.SPEC,),
        "predicates": (_predicate(),),
        "enforcement": GuidelineEnforcement.ADVISORY,
        "operator": GuidelineRuleOperator.ALL,
        "waivable": True,
        "policy_class": "standard",
    }
    values.update(overrides)
    return GuidelineRule(**values)


def _revision(
    *,
    semantic_version: str = "1.2.3",
    title: str = "Engineering policy",
    content: str = "Keep policy evidence current.",
    tags: tuple[str, ...] = ("architecture", "security"),
    rules: tuple[GuidelineRule, ...] | None = None,
    revision_id: str = "revision-3",
    revision_number: int = 3,
) -> GuidelineRevision:
    resolved_rules = (_rule(),) if rules is None else rules
    return GuidelineRevision(
        revision_id=revision_id,
        guideline_id="guideline-1",
        revision_number=revision_number,
        semantic_version=semantic_version,
        title=title,
        content=content,
        content_digest=guideline_revision_content_digest_v1(
            title=title,
            content=content,
            tags=tags,
            rules=resolved_rules,
        ),
        rules=resolved_rules,
        tags=tags,
        created_by="actor-previous",
        created_at=NOW,
        parent_revision_id=(None if revision_number == 1 else "revision-previous"),
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
    occurred_at: datetime = NOW + timedelta(minutes=1),
) -> GuidelinePatchCommand:
    return GuidelinePatchCommand(
        current_revision=revision,
        current_head=_head(revision),
        patch=patch,
        next_revision_id="revision-next",
        actor_id="actor-next",
        occurred_at=occurred_at,
        idempotency_key="patch:key",
        declared_semantic_version=declared,
    )


def _active_binding_command(
    *,
    expected: int | None,
    occurred_at: datetime,
    revision_id: str = "revision-3",
    semantic_version: str = "1.2.3",
    revision_digest: str = "a" * 64,
    priority: int = 2,
    enforcement: GuidelineEnforcement = GuidelineEnforcement.ADVISORY,
) -> GuidelineBindingTransitionCommand:
    return GuidelineBindingTransitionCommand(
        binding_id="binding-1",
        board_id="board-1",
        guideline_id="guideline-1",
        state=GuidelineBindingState.ACTIVE,
        actor_id="actor-1",
        occurred_at=occurred_at,
        idempotency_key=f"binding:{expected}:{priority}",
        expected_binding_revision=expected,
        revision_id=revision_id,
        semantic_version=semantic_version,
        revision_digest=revision_digest,
        priority=priority,
        default_enforcement=enforcement,
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
        actor_id="actor-1",
        occurred_at=occurred_at,
        idempotency_key=f"unlink:{expected}",
        expected_binding_revision=expected,
    )


def test_contract_versions_are_explicit() -> None:
    assert GUIDELINE_LIFECYCLE_CONTRACT_VERSION == "guideline-lifecycle/v1"
    assert GUIDELINE_REVISION_DIGEST_CONTRACT_VERSION == "guideline-revision-digest/v1"
    assert GUIDELINE_REQUEST_DIGEST_CONTRACT_VERSION == "guideline-request-digest/v1"


@pytest.mark.parametrize(
    ("left", "right"),
    (
        ("1.0.0-alpha", "1.0.0-alpha.1"),
        ("1.0.0-alpha.1", "1.0.0-alpha.beta"),
        ("1.0.0-alpha.beta", "1.0.0-beta"),
        ("1.0.0-beta", "1.0.0-beta.2"),
        ("1.0.0-beta.2", "1.0.0-beta.11"),
        ("1.0.0-beta.11", "1.0.0-rc.1"),
        ("1.0.0-rc.1", "1.0.0"),
    ),
)
def test_semver_uses_strict_semver_precedence(left: str, right: str) -> None:
    assert SemanticVersion.parse(left) < SemanticVersion.parse(right)


@pytest.mark.parametrize(
    "value",
    (
        "1",
        "1.0",
        "01.0.0",
        "1.01.0",
        "1.0.01",
        "1.0.0-01",
        "1.0.0-rc.01",
        "1١.0.0",
        "١.0.0",
        f"{'9' * 129}.0.0",
        f"{'9' * 5000}.0.0",
        f"1.0.0-{'9' * 129}",
        " 1.0.0",
        "1.0.0 ",
        "v1.0.0",
    ),
    ids=(
        "missing-minor-patch",
        "missing-patch",
        "major-leading-zero",
        "minor-leading-zero",
        "patch-leading-zero",
        "prerelease-leading-zero",
        "prerelease-component-leading-zero",
        "mixed-unicode-digit",
        "unicode-major",
        "oversized-major",
        "python-int-limit-major",
        "oversized-numeric-prerelease",
        "leading-space",
        "trailing-space",
        "v-prefix",
    ),
)
def test_semver_rejects_non_semver_and_numeric_prerelease_zeroes(
    value: str,
) -> None:
    with pytest.raises(GuidelineLifecycleError, match="guideline_semver_invalid"):
        SemanticVersion.parse(value)


def test_semver_direct_components_are_strict_tuples_and_hash_matches_equality() -> None:
    for prerelease, build in (("rc", ()), (("rc",), "build"), ((1,), ())):
        with pytest.raises(
            GuidelineLifecycleError,
            match="guideline_semver_invalid",
        ):
            SemanticVersion(1, 0, 0, prerelease, build)  # type: ignore[arg-type]

    with pytest.raises(
        GuidelineLifecycleError,
        match="guideline_semver_invalid",
    ):
        SemanticVersion(10**128, 0, 0)

    first = SemanticVersion.parse("1.2.3+first")
    second = SemanticVersion.parse("1.2.3+second")
    assert first == second
    assert hash(first) == hash(second)
    assert {first, second} == {first}


def test_b01_semver_contract_also_rejects_numeric_prerelease_zeroes() -> None:
    with pytest.raises(
        GuidelinePolicyContractError,
        match="guideline_revision_semantic_version_invalid",
    ):
        _revision(semantic_version="1.2.3-01")


def test_minimum_bump_classifier_covers_patch_minor_and_major_matrix() -> None:
    base_rule = _rule()
    current = _revision(rules=(base_rule,))

    patch_cases = (
        {
            "title": "Changed",
            "content": current.content,
            "tags": current.tags,
            "rules": current.rules,
        },
        {
            "title": current.title,
            "content": "Changed",
            "tags": current.tags,
            "rules": current.rules,
        },
        {
            "title": current.title,
            "content": current.content,
            "tags": ("new",),
            "rules": current.rules,
        },
        {
            "title": current.title,
            "content": current.content,
            "tags": current.tags,
            "rules": (replace(base_rule, title="Changed"),),
        },
    )
    for values in patch_cases:
        assert (
            classify_guideline_change(current, **values) is GuidelineVersionBump.PATCH
        )

    minor_rules = (
        (base_rule, _rule(2)),
        (
            replace(
                base_rule,
                target_entity_types=(PolicyEntityType.SPEC, PolicyEntityType.CARD),
            ),
        ),
        (
            replace(
                base_rule,
                enforcement=GuidelineEnforcement.ADVISORY,
            ),
        ),
        (replace(base_rule, waivable=True),),
    )
    minor_currents = (
        current,
        current,
        _revision(
            rules=(
                replace(
                    base_rule,
                    enforcement=GuidelineEnforcement.BLOCKING,
                ),
            )
        ),
        _revision(rules=(replace(base_rule, waivable=False),)),
    )
    for prior, rules in zip(minor_currents, minor_rules, strict=True):
        assert (
            classify_guideline_change(
                prior,
                title=prior.title,
                content=prior.content,
                tags=prior.tags,
                rules=rules,
            )
            is GuidelineVersionBump.MINOR
        )

    major_rule_sets = (
        (),
        (_rule(2, enforcement=GuidelineEnforcement.BLOCKING), base_rule),
        (replace(base_rule, predicates=(_predicate("exists", "status"),)),),
        (replace(base_rule, operator=GuidelineRuleOperator.ANY),),
        (replace(base_rule, policy_class="custom"),),
        (base_rule,),
        (
            replace(
                base_rule,
                enforcement=GuidelineEnforcement.BLOCKING,
            ),
        ),
        (replace(base_rule, waivable=False),),
    )
    major_currents = (
        current,
        current,
        current,
        current,
        current,
        _revision(
            rules=(
                replace(
                    base_rule,
                    target_entity_types=(PolicyEntityType.SPEC, PolicyEntityType.CARD),
                ),
            )
        ),
        current,
        current,
    )
    for prior, rules in zip(major_currents, major_rule_sets, strict=True):
        assert (
            classify_guideline_change(
                prior,
                title=prior.title,
                content=prior.content,
                tags=prior.tags,
                rules=rules,
            )
            is GuidelineVersionBump.MAJOR
        )


def test_mixed_change_uses_maximum_severity() -> None:
    current = _revision()
    blocking = _rule(2, enforcement=GuidelineEnforcement.BLOCKING)
    assert (
        classify_guideline_change(
            current,
            title="Text changed",
            content=current.content,
            tags=("new",),
            rules=(*current.rules, blocking),
        )
        is GuidelineVersionBump.MAJOR
    )


def test_canonical_trim_and_order_turn_equivalent_patch_into_noop() -> None:
    first = _rule(
        1,
        predicates=(
            _predicate("exists", "status"),
            _predicate("exists", "validation_state"),
        ),
    )
    second = _rule(2)
    current = _revision(rules=(first, second))
    patch = GuidelineRevisionPatch(
        title=f"  {current.title}  ",
        content=f"\n{current.content}\t",
        tags=(" security ", "architecture"),
        rules=(
            second,
            replace(first, predicates=tuple(reversed(first.predicates))),
        ),
    )

    low_level = plan_guideline_patch(
        current,
        patch,
        requested_semantic_version="9.0.0",
    )
    result = execute_guideline_patch(_patch_command(current, patch, declared="9.0.0"))

    assert low_level.is_noop is True
    assert low_level.semantic_version == current.semantic_version
    assert isinstance(result, GuidelinePatchNoop)
    assert result.revision is None
    assert result.head is None


def test_under_bump_returns_typed_zero_output_with_exact_fences() -> None:
    current = _revision()
    proposed_rules = (
        *current.rules,
        _rule(2, enforcement=GuidelineEnforcement.BLOCKING),
    )
    result = execute_guideline_patch(
        _patch_command(
            current,
            GuidelineRevisionPatch(rules=proposed_rules),
            declared="1.3.0",
        )
    )

    assert isinstance(result, GuidelinePatchRejected)
    assert result.code == "guideline_semver_below_minimum"
    assert result.minimum_bump is GuidelineVersionBump.MAJOR
    assert result.minimum_semantic_version == "2.0.0"
    assert result.revision is None
    assert result.head is None
    assert result.expected_head_revision == _head(current).head_revision
    assert result.expected_revision_digest == current.content_digest


def test_declared_version_needs_greater_precedence_and_required_core_bump() -> None:
    current = _revision()
    patch = GuidelineRevisionPatch(content="Changed content.")

    build_only = execute_guideline_patch(
        _patch_command(current, patch, declared="1.2.3+build")
    )
    prerelease_patch = execute_guideline_patch(
        _patch_command(current, patch, declared="1.2.4-alpha.1")
    )
    major_prerelease = execute_guideline_patch(
        _patch_command(
            current,
            GuidelineRevisionPatch(
                rules=(
                    *current.rules,
                    _rule(2, enforcement=GuidelineEnforcement.BLOCKING),
                )
            ),
            declared="2.0.0-rc.1",
        )
    )

    assert isinstance(build_only, GuidelinePatchRejected)
    assert isinstance(prerelease_patch, GuidelinePatchApplied)
    assert prerelease_patch.revision.semantic_version == "1.2.4-alpha.1"
    assert isinstance(major_prerelease, GuidelinePatchApplied)
    assert major_prerelease.revision.semantic_version == "2.0.0-rc.1"


def test_automatic_patch_always_increments_core_even_from_prerelease() -> None:
    current = _revision(semantic_version="1.2.3-rc.1")
    result = execute_guideline_patch(
        _patch_command(
            current,
            GuidelineRevisionPatch(content="Changed content."),
        )
    )
    assert isinstance(result, GuidelinePatchApplied)
    assert result.revision.semantic_version == "1.2.4"


def test_revision_digest_preserves_b03_v1_bytes_and_predicate_order() -> None:
    predicates = (
        _predicate("exists", "status"),
        _predicate("exists", "validation_state"),
    )
    rule = _rule(predicates=predicates)
    title = "  Keep legacy whitespace  "
    content = "line one\r\nline two"
    payload = {
        "contract": "guideline-revision-digest/v1",
        "title": title,
        "content": content,
        "tags": ["a", "z"],
        "rules": [
            {
                "rule_id": rule.rule_id,
                "code": rule.code,
                "title": rule.title,
                "description": rule.description,
                "target_entity_types": ["spec"],
                "predicates": [
                    {
                        "predicate_code": predicate.predicate_code,
                        "parameters": [
                            [key, value] for key, value in predicate.parameters
                        ],
                    }
                    for predicate in predicates
                ],
                "enforcement": "advisory",
                "operator": "all",
                "waivable": True,
                "policy_class": "standard",
            }
        ],
    }
    expected = hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()

    actual = guideline_revision_content_digest_v1(
        title=title,
        content=content,
        tags=(" z ", "a"),
        rules=(rule,),
    )
    reversed_digest = guideline_revision_content_digest_v1(
        title=title,
        content=content,
        tags=("a", "z"),
        rules=(replace(rule, predicates=tuple(reversed(predicates))),),
    )

    assert actual == expected
    assert reversed_digest != actual


def test_public_request_digest_is_order_stable_and_scope_fenced() -> None:
    first = guideline_request_digest_v1(
        operation="patch",
        scope_id="guideline-1",
        payload={"b": 2, "a": 1},
    )
    reordered = guideline_request_digest_v1(
        operation="patch",
        scope_id="guideline-1",
        payload={"a": 1, "b": 2},
    )
    other_scope = guideline_request_digest_v1(
        operation="patch",
        scope_id="guideline-2",
        payload={"a": 1, "b": 2},
    )
    assert first == reordered
    assert first != other_scope


def test_create_command_emits_canonical_1_0_0_bundle_and_digest() -> None:
    command = GuidelineCreateCommand(
        guideline_id="guideline-new",
        revision_id="revision-new",
        owner_id="owner-1",
        scope=GuidelineScope.GLOBAL,
        title="  Engineering policy  ",
        content="\nKeep evidence current.\t",
        tags=(" security ", "architecture"),
        rules=(_rule(2), _rule(1)),
        created_by="actor-1",
        created_at=NOW,
        idempotency_key="create:key",
    )
    result = plan_guideline_creation(command)

    assert result.expected_head_revision == 0
    assert result.revision.semantic_version == "1.0.0"
    assert result.head.semantic_version == "1.0.0"
    assert result.revision.title == "Engineering policy"
    assert result.revision.content == "Keep evidence current."
    assert result.revision.tags == ("architecture", "security")
    assert [rule.code for rule in result.revision.rules] == [
        "policy.rule_1",
        "policy.rule_2",
    ]
    assert result.revision.content_digest == guideline_revision_content_digest_v1(
        title=result.revision.title,
        content=result.revision.content,
        tags=result.revision.tags,
        rules=result.revision.rules,
    )
    assert len(result.request_digest) == 64


def test_patch_applied_result_is_complete_snapshot_with_expected_fences() -> None:
    current = _revision()
    result = execute_guideline_patch(
        _patch_command(
            current,
            GuidelineRevisionPatch(
                content="  Changed content.  ",
                tags=("new", "architecture"),
            ),
        )
    )

    assert isinstance(result, GuidelinePatchApplied)
    assert result.minimum_bump is GuidelineVersionBump.PATCH
    assert result.revision.parent_revision_id == current.revision_id
    assert result.revision.revision_number == current.revision_number + 1
    assert result.head.head_revision == _head(current).head_revision + 1
    assert result.expected_head_revision == _head(current).head_revision
    assert result.expected_revision_id == current.revision_id
    assert result.expected_revision_digest == current.content_digest
    assert result.revision.content == "Changed content."


def test_patch_and_retirement_reject_backdated_event_clocks() -> None:
    current = _revision()
    with pytest.raises(
        GuidelineLifecycleError,
        match="guideline_patch_time_not_monotonic",
    ):
        _patch_command(
            current,
            GuidelineRevisionPatch(content="Backdated."),
            occurred_at=_head(current).updated_at,
        )

    with pytest.raises(
        GuidelineLifecycleError,
        match="guideline_retirement_time_not_monotonic",
    ):
        GuidelineRetirementCommand(
            current_revision=current,
            current_head=_head(current),
            retirement_id="retirement-backdated",
            status=GuidelineLifecycleStatus.RETIRED,
            reason="Backdated.",
            actor_id="actor-1",
            occurred_at=_head(current).updated_at,
            idempotency_key="retire:backdated",
        )


def test_retirement_freezes_exact_head_and_supersedence_is_terminal() -> None:
    current = _revision()
    command = GuidelineRetirementCommand(
        current_revision=current,
        current_head=_head(current),
        retirement_id="retirement-1",
        status=GuidelineLifecycleStatus.SUPERSEDED,
        reason="Replaced by the platform policy.",
        actor_id="actor-1",
        occurred_at=NOW + timedelta(minutes=2),
        idempotency_key="retire:key",
        superseded_by_guideline_id="guideline-2",
    )
    result = plan_guideline_retirement(command)
    retirement = result.retirement

    assert retirement.retired_revision_id == current.revision_id
    assert retirement.retired_revision_number == current.revision_number
    assert retirement.retired_semantic_version == current.semantic_version
    assert retirement.retired_revision_digest == current.content_digest
    assert retirement.retired_head_revision == _head(current).head_revision
    assert result.expected_head_revision == _head(current).head_revision
    assert len(result.request_digest) == 64

    with pytest.raises(GuidelineLifecycleError, match="guideline_is_terminal"):
        execute_guideline_patch(
            _patch_command(
                current,
                GuidelineRevisionPatch(content="Forbidden."),
            ),
            retirement=retirement,
        )
    with pytest.raises(GuidelineLifecycleError, match="guideline_is_terminal"):
        plan_guideline_retirement(command, current_retirement=retirement)


def test_result_bundles_fail_closed_when_replaced_with_incoherent_evidence() -> None:
    current = _revision()
    applied = execute_guideline_patch(
        _patch_command(
            current,
            GuidelineRevisionPatch(content="Changed."),
        )
    )
    assert isinstance(applied, GuidelinePatchApplied)
    with pytest.raises(
        GuidelineLifecycleError,
        match="guideline_patch_result_bundle_mismatch",
    ):
        replace(
            applied,
            head=replace(applied.head, guideline_id="other-guideline"),
        )

    rejected = execute_guideline_patch(
        _patch_command(
            current,
            GuidelineRevisionPatch(
                rules=(
                    *current.rules,
                    _rule(2, enforcement=GuidelineEnforcement.BLOCKING),
                )
            ),
            declared="1.3.0",
        )
    )
    assert isinstance(rejected, GuidelinePatchRejected)
    with pytest.raises(GuidelineLifecycleError):
        replace(rejected, request_digest="not-a-digest")

    retirement_result = plan_guideline_retirement(
        GuidelineRetirementCommand(
            current_revision=current,
            current_head=_head(current),
            retirement_id="retirement-tamper",
            status=GuidelineLifecycleStatus.RETIRED,
            reason="Complete.",
            actor_id="actor-1",
            occurred_at=NOW + timedelta(minutes=1),
            idempotency_key="retire:tamper",
        )
    )
    with pytest.raises(
        GuidelineLifecycleError,
        match="guideline_retirement_result_fence_mismatch",
    ):
        replace(
            retirement_result,
            retirement=replace(
                retirement_result.retirement,
                retired_semantic_version="9.0.0",
            ),
        )


def test_initial_priority_unlink_and_relink_are_append_only() -> None:
    initial = plan_guideline_binding_transition(
        _active_binding_command(expected=None, occurred_at=NOW),
        current=None,
    )
    assert isinstance(initial, GuidelineBindingApplied)
    assert initial.binding.state is GuidelineBindingState.ACTIVE
    assert initial.binding.binding_revision == 1

    priority = plan_guideline_binding_transition(
        _active_binding_command(
            expected=1,
            occurred_at=NOW + timedelta(minutes=1),
            priority=9,
        ),
        current=initial.binding,
    )
    assert isinstance(priority, GuidelineBindingApplied)
    assert priority.binding.priority == 9
    assert priority.binding.binding_revision == 2

    unlink = plan_guideline_binding_transition(
        _unlink_command(
            expected=2,
            occurred_at=NOW + timedelta(minutes=2),
        ),
        current=priority.binding,
    )
    assert isinstance(unlink, GuidelineBindingApplied)
    assert unlink.binding.state is GuidelineBindingState.UNLINKED
    assert unlink.binding.binding_revision == 3
    for field_name in (
        "revision_id",
        "semantic_version",
        "revision_digest",
        "priority",
        "default_enforcement",
    ):
        assert getattr(unlink.binding, field_name) == getattr(
            priority.binding,
            field_name,
        )

    repeated_unlink = plan_guideline_binding_transition(
        _unlink_command(
            expected=3,
            occurred_at=NOW + timedelta(minutes=3),
        ),
        current=unlink.binding,
    )
    assert isinstance(repeated_unlink, GuidelineBindingNoop)
    assert repeated_unlink.binding is None

    relink = plan_guideline_binding_transition(
        _active_binding_command(
            expected=3,
            occurred_at=NOW + timedelta(minutes=4),
            priority=9,
        ),
        current=unlink.binding,
    )
    assert isinstance(relink, GuidelineBindingApplied)
    assert relink.binding.state is GuidelineBindingState.ACTIVE
    assert relink.binding.binding_revision == 4


def test_binding_results_reject_invalid_noop_and_applied_envelopes() -> None:
    initial = plan_guideline_binding_transition(
        _active_binding_command(expected=None, occurred_at=NOW),
        current=None,
    )
    assert isinstance(initial, GuidelineBindingApplied)
    with pytest.raises(
        GuidelineLifecycleError,
        match="guideline_binding_result_command_mismatch",
    ):
        replace(
            initial,
            binding=replace(initial.binding, board_id="board-other"),
        )

    noop = plan_guideline_binding_transition(
        _active_binding_command(
            expected=1,
            occurred_at=NOW + timedelta(minutes=1),
        ),
        current=initial.binding,
    )
    assert isinstance(noop, GuidelineBindingNoop)
    with pytest.raises(GuidelineLifecycleError):
        replace(noop, expected_binding_revision=0)
    with pytest.raises(GuidelineLifecycleError):
        replace(noop, request_digest="f" * 63)


def test_unlink_cannot_change_snapshot_and_new_event_time_is_monotonic() -> None:
    current = BoardGuidelineBinding(
        binding_id="binding-1",
        board_id="board-1",
        guideline_id="guideline-1",
        revision_id="revision-3",
        semantic_version="1.2.3",
        revision_digest="a" * 64,
        priority=2,
        binding_revision=1,
        adopted_by="actor-1",
        adopted_at=NOW,
    )
    changed_unlink = replace(
        current,
        state=GuidelineBindingState.UNLINKED,
        priority=3,
        binding_revision=2,
        adopted_at=NOW + timedelta(minutes=1),
    )
    with pytest.raises(
        GuidelineLifecycleError,
        match="guideline_binding_unlink_snapshot_changed",
    ):
        validate_binding_transition(current, changed_unlink)

    with pytest.raises(
        GuidelineLifecycleError,
        match="guideline_binding_time_not_monotonic",
    ):
        plan_guideline_binding_transition(
            _active_binding_command(
                expected=1,
                occurred_at=NOW,
                priority=3,
            ),
            current=current,
        )

    with pytest.raises(
        GuidelineLifecycleError,
        match="guideline_binding_unlink_snapshot_forbidden",
    ):
        GuidelineBindingTransitionCommand(
            binding_id="binding-1",
            board_id="board-1",
            guideline_id="guideline-1",
            state=GuidelineBindingState.UNLINKED,
            actor_id="actor-1",
            occurred_at=NOW + timedelta(minutes=1),
            idempotency_key="bad-unlink",
            expected_binding_revision=1,
            priority=3,
        )


def test_retired_guideline_can_unlink_but_cannot_link_or_relink() -> None:
    current_revision = _revision()
    retirement = plan_guideline_retirement(
        GuidelineRetirementCommand(
            current_revision=current_revision,
            current_head=_head(current_revision),
            retirement_id="retirement-1",
            status=GuidelineLifecycleStatus.RETIRED,
            reason="No longer applicable.",
            actor_id="actor-1",
            occurred_at=NOW + timedelta(minutes=1),
            idempotency_key="retire:key",
        )
    ).retirement
    initial = plan_guideline_binding_transition(
        _active_binding_command(expected=None, occurred_at=NOW),
        current=None,
    )
    assert isinstance(initial, GuidelineBindingApplied)
    with pytest.raises(GuidelineLifecycleError, match="guideline_is_terminal"):
        plan_guideline_binding_transition(
            _active_binding_command(
                expected=1,
                priority=9,
                occurred_at=NOW + timedelta(minutes=2),
            ),
            current=initial.binding,
            retirement=retirement,
        )
    unlinked = plan_guideline_binding_transition(
        _unlink_command(
            expected=1,
            occurred_at=NOW + timedelta(minutes=2),
        ),
        current=initial.binding,
        retirement=retirement,
    )
    assert isinstance(unlinked, GuidelineBindingApplied)

    with pytest.raises(GuidelineLifecycleError, match="guideline_is_terminal"):
        plan_guideline_binding_transition(
            _active_binding_command(
                expected=2,
                occurred_at=NOW + timedelta(minutes=3),
            ),
            current=unlinked.binding,
            retirement=retirement,
        )
    with pytest.raises(GuidelineLifecycleError, match="guideline_is_terminal"):
        plan_guideline_binding_transition(
            _active_binding_command(
                expected=None,
                occurred_at=NOW + timedelta(minutes=3),
            ),
            current=None,
            retirement=retirement,
        )


def test_unlinked_binding_cannot_be_projected_as_adopted_policy() -> None:
    active = plan_guideline_binding_transition(
        _active_binding_command(expected=None, occurred_at=NOW),
        current=None,
    )
    assert isinstance(active, GuidelineBindingApplied)
    unlinked = plan_guideline_binding_transition(
        _unlink_command(
            expected=1,
            occurred_at=NOW + timedelta(minutes=1),
        ),
        current=active.binding,
    )
    assert isinstance(unlinked, GuidelineBindingApplied)

    with pytest.raises(
        GuidelinePolicyContractError,
        match="adopted_guideline_binding_inactive",
    ):
        AdoptedGuidelineRevisionRef.from_binding(unlinked.binding)


def test_binding_transition_rejects_provenance_mismatch() -> None:
    current = BoardGuidelineBinding(
        binding_id="binding-1",
        board_id="board-1",
        guideline_id="guideline-1",
        revision_id="revision-3",
        semantic_version="1.2.3",
        revision_digest="a" * 64,
        priority=2,
        binding_revision=1,
        adopted_by="system",
        adopted_at=NOW,
        source_kind=GuidelineBindingProvenance.DEFAULT_MATERIALIZATION,
    )

    with pytest.raises(
        GuidelineLifecycleError,
        match="guideline_binding_origin_immutable",
    ):
        plan_guideline_binding_transition(
            _active_binding_command(
                expected=1,
                occurred_at=NOW + timedelta(minutes=1),
                priority=3,
            ),
            current=current,
        )
