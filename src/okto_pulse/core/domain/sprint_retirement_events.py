"""Historical F2/F3 event classification; no dispatcher or mutation authority.

Only explicitly understood, exclusively Sprint facts may be superseded by a
future archived cutover. Classification never rewrites stored event payloads.
"""

from dataclasses import dataclass
from typing import Literal

SUPERSEDED_WORK_STATUS = "superseded"

@dataclass(frozen=True, slots=True)
class SprintEventDisposition:
    action: Literal["supersede", "preserve", "review"]
    sprint_ids: tuple[str, ...]
    reason: str


_EXCLUSIVE_FIELDS = {
    "sprint.created": frozenset({"sprint_id", "spec_id"}),
    "sprint.moved": frozenset({"sprint_id", "from_status", "to_status"}),
    "sprint.closed": frozenset({"sprint_id"}),
}
_ARCHIVE_FIELDS = frozenset({"artifact_type", "artifact_id", "archived"})
_CARD_CREATED_FIELDS = frozenset({"card_id", "spec_id", "sprint_id", "card_type", "priority"})
_EXCLUSIVE_HANDLERS = {
    "sprint.created": frozenset({"ConsolidationEnqueuer"}),
    "sprint.moved": frozenset({"ConsolidationEnqueuer", "SourceCancellationLifecycleHandler"}),
    "sprint.closed": frozenset({"ConsolidationEnqueuer"}),
    "artifact.archive_changed": frozenset({"ConsolidationEnqueuer", "SourceArchiveLifecycleHandler"}),
}


def _identity(value: object) -> bool:
    return isinstance(value, str) and bool(value) and value.strip() == value


def _references(payload: dict) -> tuple[str, ...]:
    """Discover structured historical references without substring deletion rules."""
    references: set[str] = set()
    stack = [(payload, 0)]
    visited = 0
    while stack:
        value, depth = stack.pop()
        visited += 1
        if visited > 5000 or depth > 32:
            raise ValueError("sprint_retirement_event_structure_limit")
        if isinstance(value, dict):
            for key, item in value.items():
                if key in {"sprint_id", "origin_sprint_id"} and item is not None:
                    if not _identity(item):
                        raise ValueError("sprint_retirement_event_reference_invalid")
                    references.add(item)
            for prefix in ("artifact", "subject", "entity", "source"):
                if value.get(f"{prefix}_type") == "sprint":
                    identity = value.get(f"{prefix}_id")
                    if not _identity(identity):
                        raise ValueError("sprint_retirement_event_reference_invalid")
                    references.add(identity)
            stack.extend((item, depth + 1) for item in value.values())
        elif isinstance(value, list):
            stack.extend((item, depth + 1) for item in value)
        elif isinstance(value, str) and value.startswith("sprint:"):
            identity = value.split(":", 2)[1]
            if not _identity(identity):
                raise ValueError("sprint_retirement_event_reference_invalid")
            references.add(identity)
    return tuple(sorted(references))


def classify_historical_sprint_event(event_type: str, payload: object) -> SprintEventDisposition:
    """Classify only the stored payload (common envelope fields are separate).

    Unknown fields on a putatively exclusive event can hide surviving effects;
    they require investigation. A mixed Card fact must survive unchanged. Unknown
    event contracts mentioning Sprint also require investigation, not guessing.
    """
    if not isinstance(payload, dict) or not _identity(event_type):
        raise ValueError("sprint_retirement_event_envelope_invalid")
    references = _references(payload)
    expected = _EXCLUSIVE_FIELDS.get(event_type)
    if expected is not None:
        if set(payload) != expected or not all(_identity(payload[key]) for key in expected):
            return SprintEventDisposition("review", references, "exclusive_contract_drift")
        if references != (payload["sprint_id"],):
            return SprintEventDisposition("review", references, "exclusive_reference_drift")
        return SprintEventDisposition("supersede", references, "exclusive_sprint_event")
    if event_type.startswith("sprint."):
        return SprintEventDisposition("review", references, "unknown_sprint_event")
    if event_type == "artifact.archive_changed" and payload.get("artifact_type") == "sprint":
        if (set(payload) != _ARCHIVE_FIELDS or type(payload.get("archived")) is not bool
                or references != (payload.get("artifact_id"),)):
            return SprintEventDisposition("review", references, "archive_contract_drift")
        return SprintEventDisposition("supersede", references, "exclusive_sprint_archive_event")
    if references:
        if (event_type == "card.created" and set(payload) <= _CARD_CREATED_FIELDS
                and _identity(payload.get("card_id"))
                and references == (payload.get("sprint_id"),)):
            return SprintEventDisposition("preserve", references, "mixed_card_event")
        return SprintEventDisposition("review", references, "unclassified_sprint_reference")
    return SprintEventDisposition("preserve", (), "no_sprint_reference")


def classify_historical_sprint_execution(
    event_type: str, disposition: SprintEventDisposition, *, handler_name: str, status: str,
) -> tuple[str, str]:
    """Plan pending work without declaring it processed or changing its history."""
    if status == SUPERSEDED_WORK_STATUS:
        return "preserve", "superseded_execution_history"
    if status == "done":
        return "preserve", "completed_execution_history"
    if status == "processing":
        return "review", "in_flight_execution"
    if status not in {"pending", "failed", "dlq"}:
        return "review", "unknown_execution_status"
    if disposition.action != "supersede":
        return disposition.action, disposition.reason
    if handler_name not in _EXCLUSIVE_HANDLERS.get(event_type, ()):
        return "review", "unclassified_handler_effects"
    return "supersede", "pending_exclusive_sprint_work"


def classify_historical_sprint_queue(
    *, artifact_type: str, artifact_id: str, work_kind: str, status: str, payload: object,
) -> SprintEventDisposition:
    if payload is not None and not isinstance(payload, dict):
        raise ValueError("sprint_retirement_queue_payload_invalid")
    refs = _references({} if payload is None else payload)
    if artifact_type == "sprint":
        if not _identity(artifact_id):
            raise ValueError("sprint_retirement_queue_reference_invalid")
        refs = tuple(sorted(set(refs) | {artifact_id}))
    if not refs:
        return SprintEventDisposition("preserve", (), "no_sprint_reference")
    if status == SUPERSEDED_WORK_STATUS:
        return SprintEventDisposition("preserve", refs, "superseded_queue_history")
    if status == "done":
        return SprintEventDisposition("preserve", refs, "completed_queue_history")
    if status == "claimed":
        return SprintEventDisposition("review", refs, "in_flight_queue")
    if (artifact_type == "sprint" and work_kind in {"consolidate", "stale_reconcile"}
            and status in {"pending", "paused", "failed"} and payload in (None, {})):
        return SprintEventDisposition("supersede", refs, "pending_exclusive_sprint_queue")
    return SprintEventDisposition("review", refs, "unclassified_queue_effects")
