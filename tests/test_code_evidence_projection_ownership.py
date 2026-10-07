"""Closed Evidence ownership must not become prefix or writer authority."""
import pytest

from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker
from okto_pulse.core.ports.code_evidence_projection import (
    CODE_EVIDENCE_LINK_FAMILY as FAMILY, CODE_EVIDENCE_LINK_RULE,
    CODE_TRACEABILITY_SPEC_ENDPOINTS,
)
from test_code_traceability_events_kg import _evidence


@pytest.mark.parametrize("kind,section", CODE_TRACEABILITY_SPEC_ENDPOINTS.values())
def test_native_spec_endpoint_belongs_only_to_exact_evidence(kind, section):
    target = "spec:spec" + (f":{section}:child" if section else "")
    args = dict(owner_id="owner", source_type="Entity", source_ref="code_evidence:owner",
        target_type=kind, target_ref=target)
    assert FAMILY.owns_endpoints(**args)
    for field, value in (("source_ref", "code_evidence:owner-other"),
            ("source_ref", "card:owner"), ("source_type", "Decision"),
            ("target_ref", target + ":suffix"), ("target_ref", "spec: spec"),
            ("target_type", "Unknown")):
        assert not FAMILY.owns_endpoints(**{**args, field: value})


@pytest.mark.parametrize("rule,layer,writer", [
    ("supports/code_traceability_spec_link@v9", "deterministic", "worker_layer1"),
    (CODE_EVIDENCE_LINK_RULE, "cognitive", "worker_layer1"),
    (CODE_EVIDENCE_LINK_RULE, "deterministic", "human"),
])
def test_foreign_provenance_never_grants_cleanup(rule, layer, writer):
    assert not FAMILY.owns_writer(rule_id=rule, layer=layer, created_by=writer)


@pytest.mark.parametrize("case", ["missing", "null", "mapping"])
def test_incomplete_source_cannot_authorize_empty_active_set(case):
    value = dict(_evidence())
    value.pop("spec_links")
    if case != "missing":
        value["spec_links"] = None if case == "null" else {}
    with pytest.raises(ValueError, match="code_traceability_spec_links_incomplete"):
        DeterministicWorker().process_code_evidence(value)


def test_complete_empty_source_declares_retraction_without_losing_evidence():
    value = {**_evidence(), "spec_links": []}
    result = DeterministicWorker().process_code_evidence(value)
    intent, = result.relational_projection_active_set_intents
    assert (intent.owner_type, intent.owner_id, intent.namespace) == (
        "code_evidence", value["id"], "code_evidence_spec_links")
    assert intent.active_refs == intent.active_edges == ()
    assert any(node.source_artifact_ref == f"code_evidence:{value['id']}" for node in result.nodes)
