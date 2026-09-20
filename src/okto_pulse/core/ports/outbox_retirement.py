"""Conservative classification of source-exclusive historical outbox work.

The caller supplies facts from one original relational snapshot and a verified
Board removal plan. This contract neither reads storage nor authorizes cutover.
"""

from dataclasses import dataclass
from typing import Literal, Mapping

from okto_pulse.core.ports.retirement_graph import GraphRetirementPlan


@dataclass(frozen=True, slots=True)
class OutboxRetirementDisposition:
    action: Literal["supersede", "preserve", "review"]
    reason: str


def classify_outbox_retirement(*, event: Mapping, audit: Mapping | None,
        references: tuple[Mapping, ...], board_plan: GraphRetirementPlan,
        archived_origin_ids: frozenset[str]) -> OutboxRetirementDisposition:
    """Only the currently understood consolidation producer can be retired.

    A session label is not ownership. Every recorded node must also be in the
    retained source-owned graph selection. A mixed session remains processable.
    Update/supersession counters cannot be proved from the producer's add-only
    node refs, so those cases require investigation rather than invented facts.
    """
    def result(action, reason):
        return OutboxRetirementDisposition(action, reason)

    if event.get("board_id") != board_plan.board_id:
        return result("review", "outbox_board_mismatch")
    if event.get("processed_at") is not None:
        return result("preserve", "completed_outbox_history")
    if audit is None:
        return result("review", "outbox_consolidation_origin_missing")
    if (audit.get("board_id") != board_plan.board_id
            or audit.get("session_id") != event.get("session_id")
            or any(ref.get("board_id") != board_plan.board_id
                or ref.get("session_id") != audit.get("session_id") for ref in references)):
        return result("review", "outbox_session_scope_mismatch")
    if audit.get("artifact_type") != "sprint":
        return result("preserve", "surviving_outbox_origin")
    if audit.get("artifact_id") not in archived_origin_ids:
        return result("review", "outbox_origin_not_archived")
    if audit.get("undo_status") != "none" or audit.get("committed_at") is None:
        return result("review", "outbox_consolidation_not_current")
    payload = event.get("payload")
    counts = ("nodes_added", "nodes_updated", "nodes_superseded", "edges_added")
    if (event.get("event_type") != "consolidation_committed" or not isinstance(payload, dict)
            or set(payload) != {"session_id", "artifact_id", *counts}
            or payload.get("session_id") != audit.get("session_id")
            or payload.get("artifact_id") != audit.get("artifact_id")
            or any(type(payload.get(key)) is not int or not 0 <= payload[key] <= 100_000
                or type(audit.get(key)) is not int or payload[key] != audit[key] for key in counts)):
        return result("review", "outbox_consolidation_contract_drift")
    if payload["nodes_updated"] or payload["nodes_superseded"]:
        return result("review", "outbox_unproved_update_effects")
    if (not references or len(references) > 100_000
            or any(ref.get("operation") != "add" for ref in references)):
        return result("review", "outbox_node_references_incomplete")
    keys = tuple((ref.get("kuzu_node_type"), ref.get("kuzu_node_id")) for ref in references)
    if (any(type(kind) is not str or type(key) is not str for kind, key in keys)
            or len(set(keys)) != len(keys) or len(keys) != payload["nodes_added"]):
        return result("review", "outbox_node_reference_census_mismatch")
    if not set(keys) <= set(board_plan.node_keys):
        # These references are outside our deletion proof. Keeping the event
        # does not certify that every referenced node still physically exists.
        return result("preserve", "references_outside_retired_selection")
    if type(event.get("retry_count")) is not int or event["retry_count"] < -1:
        return result("review", "outbox_retry_state_requires_review")
    return result("supersede", "exclusive_archived_sprint_consolidation")
