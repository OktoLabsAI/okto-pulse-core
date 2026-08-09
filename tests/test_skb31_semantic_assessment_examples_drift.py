"""Versioned semantic-assessment MCP example drift gates."""

import json

from okto_pulse.core.mcp.semantic_assessment_examples_generator import (
    EXAMPLES_DIR,
    POLICY_RESOURCE_PATH,
    render_examples,
    render_policy_compliance_resource,
)


def test_versioned_semantic_assessment_examples_are_current() -> None:
    rendered = render_examples()

    assert set(rendered) == {
        "semantic-guideline-assessment-v1.json",
        "semantic-guideline-assessment-v2.json",
    }
    for name, expected in rendered.items():
        assert (EXAMPLES_DIR / name).read_text(encoding="utf-8") == expected


def test_v2_example_has_explicit_discriminators_and_human_pinpoint() -> None:
    payload = json.loads(
        (EXAMPLES_DIR / "semantic-guideline-assessment-v2.json").read_text(
            encoding="utf-8"
        )
    )
    metric = payload["metric_results"][0]
    pinpoint = metric["pinpoints"][0]

    assert payload["contract_version"] == "v2"
    assert metric["contract_version"] == "v2"
    assert pinpoint["contract_version"] == "v2"
    assert pinpoint["title"]
    assert pinpoint["detail"]
    assert pinpoint["remediation"]
    assert pinpoint["anchor"]["anchor_ref"]


def test_policy_compliance_rollout_resource_is_generated_and_current() -> None:
    source = POLICY_RESOURCE_PATH.read_text(encoding="utf-8")

    assert render_policy_compliance_resource(source) == source
    assert source.count("okto_pulse_record_semantic_guideline_assessment_v2") == 1
    assert "SEMANTIC_ASSESSMENT_V2_READERS_READY=true" in source
    assert "SEMANTIC_ASSESSMENT_V2_WRITER_ENABLED=true" in source
