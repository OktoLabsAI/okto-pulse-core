from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsFoundationQuery,
    AnalyticsUtcWindow,
)
from okto_pulse.core.services.policy_resource_readiness_read_model import (
    build_policy_resource_readiness_projection,
)
from okto_pulse.core.services.spec_readiness_read_model import (
    build_spec_readiness_projection,
)


NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


def _query():
    return AnalyticsFoundationQuery(
        "board-1",
        "actor:user-1",
        AnalyticsUtcWindow(NOW - timedelta(days=30), NOW),
        as_of=NOW,
    )


def _validation(*, canonical=True, outcome="success", edition=3):
    row = {
        "id": "validation-1",
        "validation_id": "validation-1",
        "edition": edition,
        "validation_edition": edition,
        "is_current": True,
        "outcome": outcome,
        "created_at": (NOW - timedelta(hours=2)).isoformat(),
        "assertiveness": 82,
        "ambiguity": 12,
    }
    if canonical:
        row.update(confidence=80, clarity=81, decidability=83)
    else:
        row["completeness"] = 99
    return row


def _spec(*, validation=None, status="validated"):
    return SimpleNamespace(
        id="spec-1",
        edition=3,
        status=status,
        archived=False,
        current_validation_id="validation-1" if validation else None,
        validations=[validation] if validation else [],
    )


def test_spec_readiness_preserves_canonical_measures_and_attempts():
    projection = build_spec_readiness_projection(
        query=_query(), as_of=NOW, specs=(_spec(validation=_validation()),)
    )

    row = projection.specs[0]
    assert row.validation.state.value == "current"
    assert row.validation.measures.clarity == 81
    assert row.validation.attempts.attempts == 1
    assert row.validation.lifecycle_ready is True
    assert row.spec_pending_validation is False


def test_legacy_completeness_never_invents_canonical_readiness():
    projection = build_spec_readiness_projection(
        query=_query(),
        as_of=NOW,
        specs=(_spec(validation=_validation(canonical=False)),),
    )

    row = projection.specs[0]
    assert row.validation.measures.legacy_completeness == 99
    assert row.validation.measures.confidence is None
    assert row.validation.lifecycle_ready is False
    assert row.spec_pending_validation is True


def test_policy_and_resources_keep_l1_l2_and_cancelled_only_evidence_separate():
    spec = _spec(validation=_validation())
    active = SimpleNamespace(
        id="card-active",
        spec_id="spec-1",
        status="in_progress",
        archived=False,
        screen_mockups=[{"id": "mockup-1"}],
        knowledge_bases=[],
    )
    cancelled = SimpleNamespace(
        id="card-cancelled",
        spec_id="spec-1",
        status="cancelled",
        archived=False,
        screen_mockups=[],
        knowledge_bases=[],
    )
    architecture = SimpleNamespace(
        id="arch-1",
        spec_id=None,
        card_id="card-cancelled",
    )
    knowledge = SimpleNamespace(id="kb-1", spec_id="spec-1")

    projection = build_policy_resource_readiness_projection(
        query=_query(),
        as_of=NOW,
        specs=(spec,),
        cards=(active, cancelled),
        architecture_designs=(architecture,),
        spec_knowledge_bases=(knowledge,),
        not_applicable=(),
    )

    row = projection.specs[0]
    assert row.policy_totals.native_pass == 1
    by_type = {item.resource_type.value: item for item in row.resources_l2}
    assert by_type["mockup"].state.value == "covered"
    assert by_type["architecture"].state.value == "uncovered"
    assert by_type["architecture"].covered_only_by_cancelled_task is True
    l1 = {item.resource_type.value: item for item in row.resources_l1}
    assert l1["knowledge_base"].state.value == "provided"


def test_missing_validation_is_blocking_pending_not_native_pass():
    projection = build_policy_resource_readiness_projection(
        query=_query(),
        as_of=NOW,
        specs=(_spec(validation=None, status="approved"),),
        cards=(),
        architecture_designs=(),
        spec_knowledge_bases=(),
        not_applicable=(),
    )

    fact = projection.specs[0].policies[0]
    assert fact.state.value == "blocking_pending"
    assert projection.specs[0].policy_totals.native_pass == 0
