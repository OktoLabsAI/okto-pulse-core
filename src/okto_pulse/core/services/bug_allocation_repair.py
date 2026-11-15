"""Owned, fail-closed repair of the retired Bug allocation inheritance defect.

This is a repair of a proven append, never a scope editor. Native creation
intent AND an exact pre-creation requirement image must corroborate the old
algorithm. Neither an absent contribution plan nor origin lineage alone proves
an allocation was automatic. The caller owns authorization and the transaction.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import uuid

from okto_pulse.core.ports.application_persistence import (
    ApplicationFilter as F,
    ApplicationQuery,
    ApplicationRecord,
    get_application_persistence_port,
)


INHERITED_FIELDS = (
    "test_scenarios",
    "business_rules",
    "api_contracts",
    "integration_requirements",
    "observability_requirements",
    "technical_requirements",
    "decisions",
)
SCOPE_FIELDS = (*INHERITED_FIELDS, "functional_requirements", "acceptance_criteria")
HISTORY_LIMIT = 200
LINK_LIMIT = 200
ACTION = "bug_allocation_repaired"


class BugAllocationRepairError(ValueError):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)

    def to_dict(self):
        return {
            "error": self.code,
            "code": self.code,
            "message": self.message,
            "retryable": False,
            "next_action": "Review the current repair preview before retrying.",
        }


def _digest(value):
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
            ensure_ascii=False,
        ).encode()
    ).hexdigest()


def _time(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return (
        value.replace(tzinfo=timezone.utc)
        if value.tzinfo is None
        else value.astimezone(timezone.utc)
    )


def _value(value):
    return getattr(value, "value", value)


def _before_image(field, item_id, creation, history):
    """Only an unambiguous full image preceding creation proves the old scope."""
    candidates = []
    for row in history:
        for change in row.changes or []:
            changed_field = str(change.get("field", ""))
            if changed_field != field and not changed_field.startswith(field + "."):
                continue
            if _time(row.created_at) >= _time(creation.created_at):
                if row.action == ACTION and changed_field == field:
                    old = [
                        item
                        for item in change.get("old", [])
                        if isinstance(item, dict) and item.get("id") == item_id
                    ]
                    new = [
                        item
                        for item in change.get("new", [])
                        if isinstance(item, dict) and item.get("id") == item_id
                    ]
                    if len(old) == len(new) == 1 and old == new:
                        continue  # an owned repair of another item is not new scope
                return None  # later explicit/manual scope or unresolvable same-second order
            if changed_field == field and isinstance(change.get("new"), list):
                images = [
                    item
                    for item in change["new"]
                    if isinstance(item, dict) and item.get("id") == item_id
                ]
                if len(images) == 1:
                    candidates.append((row, images[0]))
    if not candidates:
        return None
    newest = max(_time(row.created_at) for row, _ in candidates)
    latest = [
        (row, image) for row, image in candidates if _time(row.created_at) == newest
    ]
    if len(latest) != 1:
        return None
    return latest[0]


def project_repair(spec, bug, origin, history, activities):
    """Read-only projection; every returned eligible row has affirmative proof."""
    creations = [a for a in activities if a.action == "card_created"]
    creation = creations[0] if len(creations) == 1 else None
    creation_valid = bool(
        creation
        and creation.details
        and creation.details.get("lineage_policy") is None
        and creation.details.get("traceability")
        == {"requested": [], "changed": [], "idempotent": True}
        and _time(creation.created_at).replace(microsecond=0)
        == _time(bug.created_at).replace(microsecond=0)
    )
    # A later deliberate link, even an idempotent one, is manual intent. It
    # cannot be undone by inferring what the user might have meant.
    explicit_link = any(a.action == "card_traceability_linked" for a in activities)
    lineage_changed = any(
        a.action == "card_updated"
        and any(
            c.get("field") in {"spec_id", "origin_task_id"}
            for c in (a.details or {}).get("changes", [])
        )
        for a in activities
    )
    rows = []
    for field in SCOPE_FIELDS:
        for item in getattr(spec, field, None) or []:
            if not isinstance(item, dict) or bug.id not in (
                item.get("linked_task_ids") or []
            ):
                continue
            key = f"{field}:{item.get('id', '')}"
            row = {
                "link_id": key,
                "field": field,
                "item_id": item.get("id"),
                "bug_id": bug.id,
                "origin_task_id": origin.id,
                "state": "requires_scope_review",
                "reason": "Automatic inheritance is not proven.",
            }
            proof = (
                _before_image(field, item.get("id"), creation, history)
                if creation
                else None
            )
            if (
                creation_valid
                and not explicit_link
                and not lineage_changed
                and field in INHERITED_FIELDS
                and proof
            ):
                source, before = proof
                links = before.get("linked_task_ids")
                expected = deepcopy(before)
                if (
                    isinstance(links, list)
                    and origin.id in links
                    and bug.id not in links
                ):
                    expected["linked_task_ids"] = [*links, bug.id]
                    if expected == item:
                        row.update(
                            state="eligible",
                            reason="Exact legacy automatic append confirmed.",
                            before_linked_task_ids=item["linked_task_ids"],
                            after_linked_task_ids=links,
                            before_sha256=_digest(item),
                            after_sha256=_digest(before),
                            original_implementation_plan=before.get(
                                "implementation_plan"
                            ),
                            proof={
                                "creation_activity_id": creation.id,
                                "spec_history_id": source.id,
                                "creation_sha256": _digest(creation.values),
                                "history_sha256": _digest(source.values),
                            },
                        )
            rows.append(row)
    if len(rows) > LINK_LIMIT or len({r["link_id"] for r in rows}) != len(rows):
        raise BugAllocationRepairError(
            "repair_scope_unavailable",
            "Repair scope is oversized or has duplicate identifiers.",
        )
    scope = {field: getattr(spec, field, None) for field in SCOPE_FIELDS}
    facts = {
        "scope": scope,
        "spec_id": spec.id,
        "edition": spec.edition,
        "version": spec.version,
        "status": _value(spec.status),
        "bug": {
            k: bug.values.get(k)
            for k in (
                "id",
                "board_id",
                "spec_id",
                "origin_task_id",
                "policy_version",
                "status",
                "test_scenario_ids",
            )
        },
        "origin": {
            k: origin.values.get(k) for k in ("id", "spec_id", "board_id", "card_type")
        },
        "history": [h.values for h in history],
        "activities": [a.values for a in activities],
    }
    return {
        "contract": "bug-allocation-repair/v1",
        "board_id": spec.board_id,
        "spec_id": spec.id,
        "spec_edition": spec.edition,
        "spec_version": spec.version,
        "bug_id": bug.id,
        "origin_task_id": origin.id,
        "scope_digest": _digest(facts),
        "state": "ready"
        if any(r["state"] == "eligible" for r in rows)
        else "requires_scope_review",
        "links": rows,
        "historical_evidence_changed": False,
        "next_action": "Select only eligible links to remove. Explicit and ambiguous allocations remain unchanged.",
    }


class BugAllocationRepairService:
    def __init__(self, db):
        self.db = db
        self.store = get_application_persistence_port()

    async def _get(self, entity, identifier):
        return await self.store.get(self.db, entity=entity, record_id=identifier)

    async def _bounded(self, entity, filters):
        query = ApplicationQuery(
            entity=entity,
            filters=tuple(filters),
            limit=HISTORY_LIMIT,
            order_by=(("created_at", False), ("id", False)),
        )
        if await self.store.count(self.db, query) > HISTORY_LIMIT:
            raise BugAllocationRepairError(
                "repair_provenance_unavailable",
                "Complete provenance exceeds the bounded repair window; no changes were made.",
            )
        return await self.store.list(self.db, query)

    async def _load(self, board_id, spec_id, bug_id):
        spec, bug = await self._get("spec", spec_id), await self._get("card", bug_id)
        if (
            not spec
            or not bug
            or spec.board_id != board_id
            or bug.board_id != board_id
            or bug.spec_id != spec_id
            or _value(bug.card_type) != "bug"
        ):
            raise BugAllocationRepairError(
                "repair_subject_not_found",
                "Repair subjects were not found on this board.",
            )
        origin = (
            await self._get("card", bug.origin_task_id) if bug.origin_task_id else None
        )
        if not origin or origin.board_id != board_id or origin.spec_id != spec_id:
            raise BugAllocationRepairError(
                "repair_provenance_unavailable",
                "Bug origin is unavailable or has changed.",
            )
        history = await self._bounded("spec_history", [F("spec_id", "eq", spec_id)])
        activities = await self._bounded(
            "activity_log", [F("board_id", "eq", board_id), F("card_id", "eq", bug_id)]
        )
        return spec, bug, origin, history, activities

    async def preview(self, *, board_id, spec_id, bug_id):
        return project_repair(*await self._load(board_id, spec_id, bug_id))

    async def apply(
        self,
        *,
        board_id,
        spec_id,
        bug_id,
        actor,
        expected_spec_version,
        expected_scope_digest,
        selected_link_ids,
        idempotency_key,
    ):
        selected = sorted(set(selected_link_ids))
        if (
            not selected
            or len(selected) > LINK_LIMIT
            or len(selected) != len(selected_link_ids)
            or not idempotency_key.strip()
            or len(idempotency_key) > 160
        ):
            raise BugAllocationRepairError(
                "repair_selection_invalid",
                "Select distinct eligible links and supply a stable idempotency key.",
            )
        request_digest = _digest(
            {
                "actor_id": actor.actor_id,
                "board_id": board_id,
                "spec_id": spec_id,
                "bug_id": bug_id,
                "version": expected_spec_version,
                "scope": expected_scope_digest,
                "selected": selected,
            }
        )
        audit_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"pulse:bug-allocation-repair:{board_id}:{spec_id}:{actor.actor_id}:{idempotency_key}",
            )
        )
        # Serialize repairs and perform replay lookup before evaluating stale
        # preview guards. Never acquire a graph lease inside this SQL owner.
        if not await self.store.fence(
            self.db, entity="board", record_id=board_id, expected_values={}
        ):
            raise BugAllocationRepairError(
                "repair_conflict", "The board changed; refresh the preview."
            )
        previous = await self._get("spec_history", audit_id)
        if previous:
            receipt = (previous.changes or [{}])[0].get("new", {})
            if (
                previous.spec_id != spec_id
                or previous.action != ACTION
                or receipt.get("request_digest") != request_digest
            ):
                raise BugAllocationRepairError(
                    "repair_idempotency_conflict",
                    "This key already identifies a different repair.",
                )
            return {**receipt["response"], "replayed": True}
        spec, bug, origin, history, activities = await self._load(
            board_id, spec_id, bug_id
        )
        if spec.version != expected_spec_version or not await self.store.fence(
            self.db,
            entity="spec",
            record_id=spec_id,
            expected_values={
                "board_id": board_id,
                "version": expected_spec_version,
                "edition": spec.edition,
            },
        ):
            raise BugAllocationRepairError(
                "repair_conflict", "The Spec changed; refresh the preview."
            )
        preview = project_repair(spec, bug, origin, history, activities)
        if preview["scope_digest"] != expected_scope_digest:
            raise BugAllocationRepairError(
                "repair_conflict",
                "The repair scope or provenance changed; refresh the preview.",
            )
        eligible = {
            row["link_id"]: row
            for row in preview["links"]
            if row["state"] == "eligible"
        }
        if not set(selected) <= eligible.keys():
            raise BugAllocationRepairError(
                "requires_scope_review",
                "One or more selected links lack affirmative automatic provenance; no changes were made.",
            )
        changes = []
        for field in SCOPE_FIELDS:
            old = getattr(spec, field, None)
            new = deepcopy(old)
            for item in new or []:
                key = f"{field}:{item.get('id', '')}" if isinstance(item, dict) else ""
                if key in selected:
                    item["linked_task_ids"] = eligible[key]["after_linked_task_ids"]
            if old != new:
                setattr(spec, field, new)
                spec.mark_dirty(field)
                changes.append({"field": field, "old": old, "new": new})
        await self.store.flush(self.db)
        spec = await self.store.refresh(self.db, spec)
        response = {
            "contract": "bug-allocation-repair/v1",
            "board_id": board_id,
            "spec_id": spec_id,
            "bug_id": bug_id,
            "audit_id": audit_id,
            "spec_edition": spec.edition,
            "spec_version": spec.version,
            "removed_link_ids": selected,
            "replayed": False,
            "historical_evidence_changed": False,
        }
        receipt = {
            "request_digest": request_digest,
            "preview_digest": expected_scope_digest,
            "selected": [eligible[key] for key in selected],
            "response": response,
        }
        await self.store.add(
            self.db,
            ApplicationRecord(
                entity="spec_history",
                values={
                    "id": audit_id,
                    "spec_id": spec_id,
                    "action": ACTION,
                    "actor_id": actor.actor_id,
                    "actor_type": "agent" if actor.actor_kind == "agent" else "user",
                    "actor_name": actor.actor_name or actor.actor_id,
                    "version": spec.version,
                    "summary": "Removed proven legacy automatic Bug allocations; explicit scope and historical evidence preserved.",
                    "changes": [
                        {"field": "bug_allocation_repair", "old": None, "new": receipt},
                        *changes,
                    ],
                },
            ),
        )
        from okto_pulse.core.events import publish
        from okto_pulse.core.events.types import SpecSemanticChanged

        await publish(
            SpecSemanticChanged(
                board_id=board_id,
                actor_id=actor.actor_id,
                spec_id=spec_id,
                changed_fields=[c["field"] for c in changes],
                projection_card_ids=[bug_id, origin.id],
            ),
            session=self.db,
        )
        return response
