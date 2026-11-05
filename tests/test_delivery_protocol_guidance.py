"""Published handoff examples must use the real closed command contracts."""

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


def _exact_result_batch():
    resource = Path(__file__).resolve().parents[1] / (
        "src/okto_pulse/core/mcp/resources/reference/tool-docs/card.md")
    section = resource.read_text(encoding="utf-8").split(
        "#### Minimal exact-result association batch", 1)[1]
    return json.loads(re.search(r"```json\n(.*?)\n```", section, re.S).group(1))


@pytest.mark.parametrize('explicit_selection', [True, False])
def test_documented_test_batch_preserves_independent_associations(explicit_selection):
    example = _exact_result_batch()
    if not explicit_selection:
        for entry in example['evidence']['entries']:
            entry.pop('accepted_result_id')
    command = card_delivery_command(**example)
    assert command.contract_version == 'card-delivery-batch/v1'
    assert command.expected_delivery_revision == 7
    first, second = command.entries
    assert first.kind == second.kind == 'test'
    assert first.scenario_id == second.scenario_id
    assert first.implementation_ids != second.implementation_ids
    assert first.obligation_refs != second.obligation_refs
    if explicit_selection:
        assert first.accepted_result_id == '<current-accepted-result-a>'
        assert second.accepted_result_id == '<current-accepted-result-b>'
    else:
        assert first.accepted_result_id is second.accepted_result_id is None


@pytest.mark.parametrize('untrusted_field', ['verified', 'test_receipt', 'verification_basis_sha256'])
def test_documented_test_batch_cannot_assert_its_own_verification(untrusted_field):
    example = _exact_result_batch()
    example['evidence']['entries'][1][untrusted_field] = 'client-asserted'
    with pytest.raises(ValidationError) as error:
        card_delivery_command(**example)
    assert any(item['type'] == 'extra_forbidden' and item['loc'][-1] == untrusted_field
        for item in error.value.errors())
