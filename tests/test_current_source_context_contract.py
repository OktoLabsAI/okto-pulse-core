"""Native source-context snapshots refuse imported classification overlays."""
from copy import deepcopy

import pytest

from okto_pulse.core.domain.code_traceability import (
    CodeTraceabilityContractError,
    DeliveryContext,
    RefinementDeliveryContextProvenance,
    RefinementSourceContextManifestV2,
    build_source_context_summary_v2,
    parse_refinement_source_context_manifest_v2,
)
from okto_pulse.core.domain.mcp_permission_registry import MCP_TOOL_PERMISSION_POLICIES


@pytest.mark.parametrize('obsolete', ['classification_fence', 'classification_state', 'uncategorized_legacy_count'])
def test_native_manifest_roundtrip_is_exact_and_old_overlay_is_refused(obsolete):
    summary = build_source_context_summary_v2(
        delivery_context=DeliveryContext.GREENFIELD,
        delivery_context_provenance=RefinementDeliveryContextProvenance(
            value=DeliveryContext.GREENFIELD,
            source_refinement_id='refinement', source_refinement_version=1,
        ),
        current_investigation_outcomes=(), evidence=(),
    )
    manifest = RefinementSourceContextManifestV2(
        refinement_id='refinement', refinement_version=1,
        summary=summary, current_receipts=(),
    )
    payload = manifest.as_dict()
    assert 'classification_fence' not in payload
    assert parse_refinement_source_context_manifest_v2(payload) == manifest
    original = deepcopy(payload)
    incompatible = deepcopy(payload)
    if obsolete == 'uncategorized_legacy_count':
        incompatible['role_counts'][obsolete] = 0
    else:
        incompatible[obsolete] = {}
    with pytest.raises(CodeTraceabilityContractError):
        parse_refinement_source_context_manifest_v2(incompatible)
    assert payload == original


def test_removed_classification_tool_has_no_permission_policy():
    assert 'okto_pulse_classify_legacy_code_evidence' not in {
        policy.tool_name for policy in MCP_TOOL_PERMISSION_POLICIES
    }
