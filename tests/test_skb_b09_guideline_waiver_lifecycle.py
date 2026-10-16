"""Retirement ratchets for the policy/v1 waiver milestone.

SK-B3 replaces executable-rule waivers with semantic metric waivers.  The
complete lifecycle contract lives in ``test_skb3_semantic_guideline_exceptions``;
these checks keep the old B09 surface from silently becoming authoritative
again.
"""

from __future__ import annotations

import pytest

import okto_pulse.core.domain.guideline_lifecycle as guideline_lifecycle
from okto_pulse.core.domain.guideline_semantic_exceptions import (
    SemanticMetricWaiverEventType,
    SemanticMetricWaiverExpireReason,
    SemanticMetricWaiverStatus,
)


def test_b09_executable_rule_digest_is_absent() -> None:
    assert not hasattr(guideline_lifecycle, "guideline_revision_content_digest_v1")
    assert not hasattr(guideline_lifecycle, "guideline_revision_content_digest")


@pytest.mark.parametrize(
    ("enum_type", "wire_value"),
    [
        (SemanticMetricWaiverStatus, "requested"),
        (SemanticMetricWaiverStatus, "approved"),
        (SemanticMetricWaiverStatus, "rejected"),
        (SemanticMetricWaiverStatus, "revoked"),
        (SemanticMetricWaiverStatus, "expired"),
        (SemanticMetricWaiverEventType, "request"),
        (SemanticMetricWaiverEventType, "approve"),
        (SemanticMetricWaiverEventType, "reject"),
        (SemanticMetricWaiverEventType, "revoke"),
        (SemanticMetricWaiverEventType, "expire"),
        (SemanticMetricWaiverEventType, "revalidate"),
        (SemanticMetricWaiverExpireReason, "scheduled_expiry"),
        (SemanticMetricWaiverExpireReason, "subject_scope_changed"),
        (SemanticMetricWaiverExpireReason, "guideline_revision_changed"),
        (SemanticMetricWaiverExpireReason, "binding_configuration_changed"),
        (SemanticMetricWaiverExpireReason, "metric_result_changed"),
    ],
)
def test_b09_semantic_metric_waiver_wire_value_is_closed(
    enum_type: type[
        SemanticMetricWaiverStatus
        | SemanticMetricWaiverEventType
        | SemanticMetricWaiverExpireReason
    ],
    wire_value: str,
) -> None:
    member = enum_type(wire_value)

    assert member.value == wire_value
