"""Public semantic guideline-domain/v2 contract tests."""

from __future__ import annotations

import ast
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from pathlib import Path

import pytest

from okto_pulse.core.domain import guideline_policy
from okto_pulse.core.domain import guideline_lifecycle
from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_BINDING_CONFIGURATION_CONTRACT_VERSION,
    GUIDELINE_DOMAIN_CONTRACT_VERSION,
    GUIDELINE_IMPACT_CONTRACT_VERSION,
    GUIDELINE_REVISION_DIGEST_CONTRACT_VERSION,
    BoardGuidelineBinding,
    Guideline,
    GuidelineBindingProvenance,
    GuidelineBindingState,
    GuidelineContextScope,
    GuidelineEnforcement,
    GuidelineMetric,
    GuidelineMetricDirection,
    GuidelinePolicyContractError,
    GuidelineRevision,
    GuidelineScope,
    PolicyEntityType,
    PolicySubjectRef,
    guideline_binding_configuration_digest_v1,
    guideline_revision_digest_v2,
)


NOW = datetime(2026, 7, 29, 12, tzinfo=timezone.utc)
DOMAIN_PATH = (
    Path(__file__).parents[1]
    / "src"
    / "okto_pulse"
    / "core"
    / "domain"
    / "guideline_policy.py"
)


def _metric(
    *,
    metric_id: str = "metric-segregation",
    code: str = "segregation",
    targets: tuple[PolicyEntityType, ...] = (PolicyEntityType.SPEC,),
    direction: GuidelineMetricDirection = GuidelineMetricDirection.MINIMUM,
    threshold: int = 80,
) -> GuidelineMetric:
    return GuidelineMetric(
        metric_id=metric_id,
        code=code,
        title="Segregation",
        description="Measures technical and business concern segregation.",
        evaluation_rubric="0 is mixed; 100 is completely isolated.",
        target_entity_types=targets,
        direction=direction,
        default_threshold=threshold,
    )


def _revision(
    *,
    revision_id: str = "revision-1",
    revision_number: int = 1,
    semantic_version: str = "1.0.0",
    metrics: tuple[GuidelineMetric, ...] = (_metric(),),
    revision_digest: str | None = None,
    parent_revision_id: str | None = None,
) -> GuidelineRevision:
    return GuidelineRevision(
        revision_id=revision_id,
        guideline_id="guideline-1",
        revision_number=revision_number,
        semantic_version=semantic_version,
        title="Hexagonal architecture",
        content="Business rules must not depend on adapters.",
        metrics=metrics,
        created_by="agent-1",
        created_at=NOW,
        revision_digest=revision_digest,
        parent_revision_id=parent_revision_id,
        tags=("architecture",),
    )


def _binding(
    revision: GuidelineRevision,
    **changes: object,
) -> BoardGuidelineBinding:
    values: dict[str, object] = {
        "binding_id": "binding-1",
        "board_id": "board-1",
        "guideline_id": revision.guideline_id,
        "revision_id": revision.revision_id,
        "semantic_version": revision.semantic_version,
        "revision_digest": revision.revision_digest,
        "priority": 10,
        "binding_revision": 1,
        "adopted_by": "agent-1",
        "adopted_at": NOW,
        "enforcement": GuidelineEnforcement.BLOCKING,
        "minimum_confidence": 80,
        "metric_threshold_overrides": {"segregation": 90},
        "state": GuidelineBindingState.ACTIVE,
        "source_kind": GuidelineBindingProvenance.NATIVE,
    }
    values.update(changes)
    return BoardGuidelineBinding(**values)


def _import_roots(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            roots.add(node.module.split(".", 1)[0])
    return roots


def test_contract_literals_and_closed_semantic_enums_are_frozen() -> None:
    assert GUIDELINE_DOMAIN_CONTRACT_VERSION == "guideline-domain/v2"
    assert GUIDELINE_REVISION_DIGEST_CONTRACT_VERSION == (
        "guideline-revision-digest/v2"
    )
    assert GUIDELINE_BINDING_CONFIGURATION_CONTRACT_VERSION == (
        "guideline-binding-configuration/v1"
    )
    assert GUIDELINE_IMPACT_CONTRACT_VERSION == "guideline-impact/v2"
    assert {item.value for item in GuidelineMetricDirection} == {
        "minimum",
        "maximum",
    }
    assert {item.value for item in GuidelineEnforcement} == {
        "advisory",
        "blocking",
    }
    assert {item.value for item in PolicyEntityType} == {
        "ideation",
        "refinement",
        "spec",
        "sprint",
        "card",
        "test_scenario",
    }


def test_domain_module_is_stdlib_only_and_active_public_surface_is_v2() -> None:
    assert _import_roots(DOMAIN_PATH) <= {
        "__future__",
        "dataclasses",
        "datetime",
        "enum",
        "hashlib",
        "json",
        "math",
        "re",
        "types",
        "typing",
        "unicodedata",
    }
    public = set(guideline_policy.__all__)
    assert {
        "GuidelineMetric",
        "GuidelineMetricDirection",
        "GuidelineRevision",
        "BoardGuidelineBinding",
        "guideline_revision_digest_v2",
        "guideline_binding_configuration_digest_v1",
        "guideline_impact_digest_v2",
    } <= public
    retired = {
        "GuidelinePredicate",
        "GuidelineRule",
        "GuidelineRuleOperator",
        "PolicyEvaluationInput",
        "PolicyEvaluationResult",
        "PolicyComplianceFinding",
        "PolicyComplianceReceipt",
        "guideline_impact_digest_v1",
    }
    assert public.isdisjoint(retired)
    # Transitional internals remain explicitly importable until all adapters
    # finish migration, but wildcard/public exports cannot advertise policy/v1.
    assert all(hasattr(guideline_policy, name) for name in retired)
    assert "guideline_revision_content_digest_v2" in (
        guideline_lifecycle.__all__
    )
    assert {
        "guideline_revision_content_digest",
        "guideline_revision_content_digest_v1",
    }.isdisjoint(guideline_lifecycle.__all__)
    assert hasattr(
        guideline_lifecycle,
        "guideline_revision_content_digest_v1",
    )


def test_guideline_identity_is_contextual_and_board_scope_is_closed() -> None:
    global_guideline = Guideline(
        guideline_id="guideline-1",
        owner_id="owner-1",
        scope=GuidelineScope.GLOBAL,
        context_scope=GuidelineContextScope.ALL,
        created_at=NOW,
    )
    assert global_guideline.board_id is None
    with pytest.raises(
        GuidelinePolicyContractError,
        match="inline_guideline_board_id_required",
    ):
        Guideline(
            guideline_id="guideline-2",
            owner_id="owner-1",
            scope=GuidelineScope.INLINE,
            created_at=NOW,
        )


@pytest.mark.parametrize("threshold", (-1, 101, True))
def test_metric_score_range_is_deterministically_closed(threshold: object) -> None:
    with pytest.raises(
        GuidelinePolicyContractError,
        match="guideline_metric_default_threshold_invalid",
    ):
        _metric(threshold=threshold)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("changes", "code"),
    (
        ({"metric_id": "confidence"}, "guideline_metric_confidence_reserved"),
        ({"code": "confidence"}, "guideline_metric_confidence_reserved"),
        ({"code": "not a code"}, "guideline_metric_code_invalid"),
        ({"targets": ()}, "guideline_metric_target_entity_types_required"),
        (
            {"targets": (PolicyEntityType.SPEC, PolicyEntityType.SPEC)},
            "guideline_metric_target_entity_types_duplicate",
        ),
    ),
)
def test_metric_identity_targets_and_reserved_confidence_fail_closed(
    changes: dict[str, object],
    code: str,
) -> None:
    with pytest.raises(GuidelinePolicyContractError, match=code):
        _metric(**changes)  # type: ignore[arg-type]


def test_revision_preserves_metric_order_and_seals_normative_digest() -> None:
    first = _metric()
    second = _metric(
        metric_id="metric-dependency",
        code="dependency_direction",
    )
    mutable_metrics = [first, second]
    revision = _revision(metrics=mutable_metrics)  # type: ignore[arg-type]
    mutable_metrics.reverse()

    assert revision.metrics == (first, second)
    assert revision.revision_digest == guideline_revision_digest_v2(
        semantic_version="1.0.0",
        title=revision.title,
        content=revision.content,
        metrics=(first, second),
        tags=("architecture",),
    )
    assert not revision.context_only
    with pytest.raises(FrozenInstanceError):
        revision.title = "Changed"  # type: ignore[misc]
    with pytest.raises(
        GuidelinePolicyContractError,
        match="guideline_revision_digest_mismatch",
    ):
        _revision(revision_digest="a" * 64)


def test_context_only_and_duplicate_metric_id_or_code_are_explicit() -> None:
    assert _revision(metrics=()).context_only
    with pytest.raises(
        GuidelinePolicyContractError,
        match="guideline_revision_duplicate_metric_id",
    ):
        _revision(metrics=(_metric(), _metric(code="other")))
    with pytest.raises(
        GuidelinePolicyContractError,
        match="guideline_revision_duplicate_metric_code",
    ):
        _revision(
            metrics=(
                _metric(),
                _metric(metric_id="metric-other", code="SEGREGATION"),
            )
        )


@pytest.mark.parametrize(
    "semantic_version",
    (
        "01.0.0",
        "1\u0661.0.0",
        "\u0661.0.0",
        f"{'9' * 129}.0.0",
        f"1.0.0-{'9' * 129}",
    ),
    ids=(
        "leading-zero",
        "mixed-unicode-digit",
        "unicode-major",
        "oversized-major",
        "oversized-prerelease",
    ),
)
def test_revision_semver_rejects_noncanonical_or_oversized_values(
    semantic_version: str,
) -> None:
    with pytest.raises(
        GuidelinePolicyContractError,
        match="guideline_revision_semantic_version_invalid",
    ):
        _revision(semantic_version=semantic_version)


def test_revision_lineage_is_exact() -> None:
    with pytest.raises(
        GuidelinePolicyContractError,
        match="guideline_initial_revision_parent_forbidden",
    ):
        _revision(parent_revision_id="previous")
    with pytest.raises(
        GuidelinePolicyContractError,
        match="guideline_revision_parent_required",
    ):
        _revision(
            revision_id="revision-2",
            revision_number=2,
            semantic_version="1.1.0",
        )


def test_binding_seals_exact_semantic_configuration_and_freezes_overrides() -> None:
    revision = _revision()
    binding = _binding(revision)
    assert binding.configuration_digest == (
        guideline_binding_configuration_digest_v1(
            binding_id=binding.binding_id,
            board_id=binding.board_id,
            guideline_id=binding.guideline_id,
            revision_id=binding.revision_id,
            revision_digest=binding.revision_digest,
            priority=binding.priority,
            enforcement=binding.enforcement,
            minimum_confidence=binding.minimum_confidence,
            metric_threshold_overrides={"segregation": 90},
        )
    )
    with pytest.raises(TypeError):
        binding.metric_threshold_overrides["segregation"] = 0
    with pytest.raises(
        GuidelinePolicyContractError,
        match="guideline_binding_configuration_digest_mismatch",
    ):
        _binding(revision, configuration_digest="b" * 64)


@pytest.mark.parametrize(
    ("changes", "code"),
    (
        (
            {"minimum_confidence": 101},
            "guideline_binding_minimum_confidence_invalid",
        ),
        (
            {"metric_threshold_overrides": {"segregation": -1}},
            "guideline_binding_metric_threshold_overrides_invalid",
        ),
        (
            {
                "metric_threshold_overrides": {
                    "segregation": 80,
                    "SEGREGATION": 90,
                }
            },
            "guideline_binding_metric_threshold_overrides_invalid",
        ),
    ),
)
def test_binding_semantic_configuration_rejects_invalid_values(
    changes: dict[str, object],
    code: str,
) -> None:
    with pytest.raises(GuidelinePolicyContractError, match=code):
        _binding(_revision(), **changes)


def test_subject_reference_binds_board_type_id_and_version() -> None:
    subject = PolicySubjectRef(
        board_id="board-1",
        entity_type=PolicyEntityType.SPEC,
        subject_id="spec-1",
        subject_version=3,
    )
    assert (
        subject.board_id,
        subject.entity_type,
        subject.subject_id,
        subject.subject_version,
    ) == ("board-1", PolicyEntityType.SPEC, "spec-1", 3)
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_subject_version_invalid",
    ):
        PolicySubjectRef(
            board_id="board-1",
            entity_type=PolicyEntityType.SPEC,
            subject_id="spec-1",
            subject_version=0,
        )
