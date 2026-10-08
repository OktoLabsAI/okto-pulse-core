"""Current Path B evidence, read through the public persistence port.

The scenario collection's authoritative epoch prevents an operational reset
followed by restoration of identical evidence from reviving an old attestation.
No historical confirmation is rewritten by these reads.
"""

from dataclasses import replace
import hashlib
import json

from okto_pulse.core.ports.application_persistence import get_application_persistence_port
from okto_pulse.core.services.bug_regression_scenarios import (
    AmendmentLineageFact,
    CoverageBasis,
)
from okto_pulse.core.services.test_scenario_lifecycle import (
    compute_test_scenario_semantic_sha256,
    reexecutable_evidence_reference,
    scenario_has_authenticated_required_evidence,
)


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                    ensure_ascii=False, allow_nan=False).encode()).hexdigest()


async def current_coverage_basis(context, *, amendment, task_id: str,
                                 scenario_id: str, scenario_spec_id: str) -> CoverageBasis | None:
    """Resolve the exact bound source, never a similarly named Board scenario."""
    port = get_application_persistence_port()
    task = await port.get(context, entity="card", record_id=task_id)
    spec = await port.get(context, entity="spec", record_id=scenario_spec_id)
    if (task is None or spec is None
            or task.board_id != amendment.board_id or spec.board_id != amendment.board_id
            or getattr(task.status, "value", task.status) != "done"
            or getattr(task.card_type, "value", task.card_type) != "test"
            or getattr(task, "archived", False)
            or scenario_id not in (task.test_scenario_ids or ())):
        return None
    matches = [item for item in (spec.test_scenarios or ())
               if isinstance(item, dict) and item.get("id") == scenario_id]
    if len(matches) != 1:
        return None
    scenario = matches[0]
    if scenario.get("status") not in {"passed", "automated"}:
        return None
    epoch = getattr(spec, "test_scenario_policy_epoch", None)
    edition = getattr(spec, "edition", None)
    if type(epoch) is not int or epoch < 1 or type(edition) is not int or edition < 1:
        return None
    criteria = list(spec.acceptance_criteria or [])
    if not scenario_has_authenticated_required_evidence(
            board_id=amendment.board_id, spec_id=spec.id,
            scenario=scenario, acceptance_criteria=criteria):
        return None
    evidence_ref = reexecutable_evidence_reference(scenario)
    if not evidence_ref:
        return None
    try:
        semantic = compute_test_scenario_semantic_sha256(
            board_id=amendment.board_id, spec_id=spec.id,
            scenario=scenario, acceptance_criteria=criteria)
        amendment_source = {
            field: getattr(amendment, field, None) for field in (
                "id", "board_id", "original_spec_id", "origin_bug_id", "revision_spec_id",
                "origin_task_ids", "affected_task_ids", "regression_scenario_ids",
                "regression_test_task_ids", "automated_regression_refs", "status", "lineage_state")
        }
        return CoverageBasis(
            scenario_spec_id=spec.id, spec_edition=edition, scenario_epoch=epoch,
            semantic_sha256=semantic, evidence_sha256=_digest(scenario.get("evidence")),
            amendment_sha256=_digest(amendment_source), evidence_ref=evidence_ref,
        )
    except (ValueError, TypeError):
        return None


async def current_amendment_facts(context, rows) -> list[AmendmentLineageFact]:
    facts = []
    for row in rows:
        fact = AmendmentLineageFact.from_row(row)
        confirmation = fact.coverage_confirmation
        basis = None
        if confirmation is not None and confirmation.basis is not None:
            basis = await current_coverage_basis(
                context, amendment=row, task_id=confirmation.regression_test_task_id,
                scenario_id=confirmation.regression_scenario_id,
                scenario_spec_id=confirmation.basis.scenario_spec_id)
        facts.append(replace(fact, current_coverage_basis=basis))
    return facts
