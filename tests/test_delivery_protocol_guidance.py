"""The published composed handoff example must parse as the real closed command."""

import json
from pathlib import Path
import re

import pytest
from pydantic import ValidationError

from okto_pulse.core.models.delivery_evidence import card_delivery_command


def test_documented_composed_handoff_uses_the_native_closed_report_contract():
    resource = Path(__file__).resolve().parents[1] / (
        "src/okto_pulse/core/mcp/resources/reference/tool-docs/code-traceability.md")
    section = resource.read_text(encoding="utf-8").split("### Minimal composed handoff", 1)[1]
    example = json.loads(re.search(r"```json\n(.*?)\n```", section, re.S).group(1))
    example["evidence"]["batch"]["entries"][0]["bindings"][0]["obligation_ref"] = "ac:ac_example"
    command = card_delivery_command(**example)
    assert command.contract_version == "card-delivery-report/v1"
    assert command.report.status == "validation"
    assert command.batch.entries[0].execution_submission.target_id == "<native-target-id>"
    # The example cannot turn into a client-authored test/approval attestation.
    example["evidence"]["verified"] = True
    with pytest.raises(ValidationError):
        card_delivery_command(**example)
