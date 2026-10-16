"""Pure selection of native Decision identities for an exact rebuild."""
from okto_pulse.core.ports.kg_cognitive_source import latest_cognitive_source_records


def select_decision_heads(board_id, candidates, records):
    refs = {candidate.source_artifact_ref for candidate in candidates.values()
            if str(getattr(candidate.node_type, "value", candidate.node_type)) == "Decision"}
    grouped = {}
    for record in latest_cognitive_source_records(records):
        if record.board_id != board_id:
            raise ValueError("cognitive_rebuild_source_cross_board")
        if record.node_type == "Decision" and record.payload.get("source_artifact_ref") in refs:
            grouped.setdefault(record.payload["source_artifact_ref"], []).append(record)
    selected = {}
    for ref, history in grouped.items():
        active = [record for record in history if not record.payload.get("superseded_by")]
        if len(active) != 1:
            raise ValueError("cognitive_rebuild_active_identity_ambiguous")
        head, = active
        if head.payload.get("generation") != head.generation:
            raise ValueError("cognitive_rebuild_generation_mismatch")
        selected[ref] = head
    return selected


def matches_literal_decision(candidate, record):
    payload = record.payload
    if not payload.get("human_curated") and any(
        (getattr(candidate, key, None) or "") != (payload.get(key) or "")
        for key in ("title", "content", "context", "justification")
    ):
        return False
    return all(getattr(candidate, key) == payload.get(key)
               for key in ("graph_layer", "maturity_status"))


def require_existing_decision_matches(record, actual):
    fields = ("source_artifact_ref", "generation", "superseded_by",
              "title", "content", "context", "justification", "human_curated",
              "graph_layer", "maturity_status")
    if any(actual.get(key) != record.payload.get(key) for key in fields):
        raise ValueError("cognitive_rebuild_existing_identity_conflict")
