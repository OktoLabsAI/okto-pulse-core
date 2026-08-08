"""Structured child-entity mutations for specs.

This service is the single mutation boundary for spec child entities. It keeps
the current JSON-backed storage model, but gives UI/API/MCP callers a typed
operation surface instead of whole-list replacement.
"""

from __future__ import annotations

import copy
import hashlib
import json
import secrets
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from pydantic import ValidationError

from okto_pulse.core.events import publish as event_publish
from okto_pulse.core.events.types import (
    SpecSemanticChanged,
    SpecVersionBumped,
    StructuredSpecEntityCreated,
    StructuredSpecEntityRevoked,
    StructuredSpecEntityUpdated,
)
from okto_pulse.core.infra.permissions import PermissionSet
from okto_pulse.core.models.schemas import (
    ApiContract,
    BusinessRule,
    Decision,
    IntegrationRequirement,
    ObservabilityRequirement,
)
from okto_pulse.core.ports.structured_spec import (
    StructuredSpecRecord,
    get_structured_spec_store,
)
from okto_pulse.core.ports.requirement_lint import RequirementLintWriter
from okto_pulse.core.services.requirement_lint_writer import (
    stage_spec_requirement_lint,
)
from okto_pulse.core.services.main import (
    SpecLockedError,
    SpecService,
    _require_spec_unlocked,
    _validate_spec_linked_refs,
    resolve_actor_name,
)
# spec_child_text / spec_child_id now live in the leaf canonicalization module
# (spec 9d66847f) so SpecService.create_spec/update_spec can import the
# canonicalizer without an import cycle. Re-exported here for existing callers.
from okto_pulse.core.services.spec_entity_canonicalization import (  # noqa: F401
    SPEC_REQUIREMENT_FIELDS,
    canonicalize_spec_children,
    canonicalize_spec_requirement_fields,
    spec_child_id,
    spec_child_text,
)


STRUCTURED_SPEC_ENTITY_FIELDS: dict[str, str] = {
    "functional_requirement": "functional_requirements",
    "business_rule": "business_rules",
    "technical_requirement": "technical_requirements",
    "decision": "decisions",
    "acceptance_criterion": "acceptance_criteria",
    "api_contract": "api_contracts",
    "integration_requirement": "integration_requirements",
    "observability_requirement": "observability_requirements",
}

STRUCTURED_SPEC_ENTITY_OPERATIONS: set[str] = {
    "create",
    "update",
    "revoke",
    "supersede",
    "restore",
    "reorder",
    "link_task",
    "unlink_task",
}

_TEXT_ENTITY_TYPES = {"functional_requirement", "acceptance_criterion"}
_LEGACY_MATERIALIZED_ENTITY_TYPES = _TEXT_ENTITY_TYPES | {"technical_requirement"}
_STRUCTURED_MODEL_BY_TYPE = {
    "business_rule": BusinessRule,
    "decision": Decision,
    "api_contract": ApiContract,
    "integration_requirement": IntegrationRequirement,
    "observability_requirement": ObservabilityRequirement,
}
# api_contract is validated with a write context so its http strictness (the
# real-verb method enum + required path) fires on create/update through this
# service (F9). Every read-back path constructs WITHOUT it and stays tolerant of
# pre-existing legacy contracts.
_API_CONTRACT_WRITE_CONTEXT = {"on_write": True}


def _api_contract_validation_message(exc: ValidationError) -> str:
    """Canonical api-contract error string with NO errors.pydantic.dev URL (F10).

    ``errors(include_url=False)`` drops the Pydantic library URL surface so the
    agent receives an actionable, vocabulary-aligned message rather than a raw
    library dump.
    """
    parts = []
    for err in exc.errors(include_url=False):
        loc = ".".join(str(p) for p in err.get("loc", ()))
        msg = err.get("msg", "invalid value")
        parts.append(f"{loc}: {msg}" if loc else str(msg))
    return "invalid_api_contract: " + "; ".join(parts)
_ID_PREFIX_BY_TYPE = {
    "functional_requirement": "fr",
    "business_rule": "br",
    "technical_requirement": "tr",
    "decision": "dec",
    "acceptance_criterion": "ac",
    "api_contract": "api",
    "integration_requirement": "ir",
    "observability_requirement": "or",
}
_TECHNICAL_REQUIREMENT_FIELDS = {
    "id",
    "text",
    "title",
    "locale",
    "linked_task_ids",
    "status",
    "notes",
}
_LIFECYCLE_OPERATION_STATUS = {
    "revoke": "revoked",
    "supersede": "superseded",
    "restore": "active",
}
_DESTRUCTIVE_LIKE_OPERATIONS = {"revoke", "supersede", "reorder"}
_ACK_TTL = timedelta(minutes=5)
_SEMANTIC_FIELDS = {
    "decisions",
    "business_rules",
    "api_contracts",
    "integration_requirements",
    "observability_requirements",
}


# spec_child_text and spec_child_id are imported (re-exported) from
# spec_entity_canonicalization at the top of this module — see spec 9d66847f.


def canonical_spec_child_ref(spec_id: str, entity_type: str, entity_id: str) -> str:
    return f"spec:{spec_id}:{entity_type}:{entity_id}"


def is_active_spec_child(item: Any) -> bool:
    if not isinstance(item, dict):
        return True
    return str(item.get("status") or "active") == "active"


def filter_active_spec_children(items: list[Any] | None, *, include_inactive: bool = False) -> list[Any]:
    values = list(items or [])
    if include_inactive:
        return values
    return [item for item in values if is_active_spec_child(item)]


def _utcnow() -> datetime:
    return datetime.now(UTC)


class UnsupportedSpecEntityOperation(ValueError):
    """Raised when an operation is invalid for a specific entity type."""


# ---------------------------------------------------------------------------
# FR5 — lazy ref migration helpers (spec c61569b2, IMPL-4)
#
# Called by SpecService.update_spec whenever FR/AC lists are touched (i.e.
# any call to update_spec that includes functional_requirements or
# acceptance_criteria). When those lists contain legacy string items (or
# dict items without an id), canonicalize_fr_ac assigns fresh fr_/ac_ ids.
# These helpers then rewrite index/text refs in linked_requirements /
# linked_criteria fields so that downstream analytics and the referential-
# integrity gate find canonical ids instead of positional indices.
#
# Guard: the existing READ resolvers (resolve_linked_fr_indices,
# resolve_linked_criteria_to_ids) remain untouched — specs not touched by
# update_spec keep resolving correctly via index/text tolerance.  No batch
# migration; no one-shot tool; only lazy on-touch.
# ---------------------------------------------------------------------------


def _build_materialization_mappings(
    old_items: list[Any],
    new_items: list[Any],
) -> list[dict[str, str]]:
    """Build index→fr_id/ac_id mappings for items materialized by canonicalize_fr_ac.

    An item is considered *newly materialized* when the old item at the same
    position lacked a structured id (was a legacy string or an id-less dict)
    and the new item carries one.  Only those positions produce mapping
    entries; items that already had ids are skipped (no migration needed).

    Returns a list of ``{"index": str, "text": str, "id": str}`` dicts, the
    same shape consumed by ``_legacy_replacement_map`` inside
    ``StructuredSpecEntityService``.
    """
    mappings: list[dict[str, str]] = []
    for idx, (old, new) in enumerate(zip(old_items, new_items)):
        old_id = spec_child_id(old)
        new_id = spec_child_id(new)
        if old_id is not None or new_id is None:
            # old already had an id → no migration needed for this slot
            continue
        text = spec_child_text(new)
        mappings.append({"index": str(idx), "text": text, "id": new_id})
    return mappings


def _apply_ref_replacement(
    collection: list[Any],
    replacements: dict[str, str],
    ref_field: str,
) -> tuple[list[Any], bool]:
    """Replace legacy refs in *ref_field* of each dict in *collection*.

    Returns the (possibly mutated) collection and a boolean indicating
    whether any replacement was made.  The input list is deep-copied
    internally so the caller's original is never mutated.
    """
    result = copy.deepcopy(collection)
    changed = False
    for item in result:
        if not isinstance(item, dict):
            continue
        refs = item.get(ref_field) or []
        next_refs: list[str] = []
        item_changed = False
        for ref in refs:
            ref_str = str(ref)
            replacement = replacements.get(ref_str, ref_str)
            if replacement != ref_str:
                item_changed = True
            if replacement not in next_refs:
                next_refs.append(replacement)
        if item_changed:
            item[ref_field] = next_refs
            changed = True
    return result, changed


def migrate_legacy_fr_refs(
    old_frs: list[Any],
    new_frs: list[Any],
    collections: dict[str, list[Any]],
) -> dict[str, list[Any]]:
    """Rewrite index/text-based linked_requirements refs to canonical fr_ids.

    Called by ``SpecService.update_spec`` after ``canonicalize_fr_ac`` runs on
    ``functional_requirements``.  Only items that transitioned from legacy
    (string/id-less dict) to structured (dict-with-fr_id) produce mapping
    entries; already-structured items are skipped.

    ``collections`` maps field_name → current list for each dependent field
    (business_rules, api_contracts, integration_requirements,
    observability_requirements, decisions).  The caller is responsible for
    merging the returned updates back into update_data.

    Returns a dict of ``{field_name: updated_list}`` for fields that changed.
    An empty dict means no migration was needed.
    """
    mappings = _build_materialization_mappings(old_frs, new_frs)
    if not mappings:
        return {}
    replacements: dict[str, str] = {}
    for m in mappings:
        replacements[m["index"]] = m["id"]
        if m["text"]:
            replacements[m["text"]] = m["id"]
    updates: dict[str, list[Any]] = {}
    for field_name, collection in collections.items():
        updated, changed = _apply_ref_replacement(collection, replacements, "linked_requirements")
        if changed:
            updates[field_name] = updated
    return updates


def migrate_legacy_ac_refs(
    old_acs: list[Any],
    new_acs: list[Any],
    scenarios: list[Any],
) -> list[Any] | None:
    """Rewrite index/text-based linked_criteria refs in test_scenarios to canonical ac_ids.

    Called by ``SpecService.update_spec`` after ``canonicalize_fr_ac`` runs on
    ``acceptance_criteria``.  Returns the updated scenarios list if any refs
    were rewritten, or ``None`` if no migration was needed.
    """
    mappings = _build_materialization_mappings(old_acs, new_acs)
    if not mappings:
        return None
    replacements: dict[str, str] = {}
    for m in mappings:
        replacements[m["index"]] = m["id"]
        if m["text"]:
            replacements[m["text"]] = m["id"]
    updated, changed = _apply_ref_replacement(scenarios, replacements, "linked_criteria")
    return updated if changed else None


class StructuredSpecEntityNotFound(ValueError):
    """Raised when a requested child entity cannot be found in the spec."""


class StructuredSpecEntityErrorCode:
    UNSUPPORTED_ENTITY_TYPE = "unsupported_entity_type"
    UNSUPPORTED_OPERATION = "unsupported_operation"
    VALIDATION_FAILED = "validation_failed"
    AUTHORIZATION_DENIED = "authorization_denied"
    VERSION_CONFLICT = "version_conflict"
    IMPACT_ACK_REQUIRED = "impact_ack_required"
    IMPACT_ACK_INVALID = "impact_ack_invalid"
    ENTITY_NOT_FOUND = "entity_not_found"
    LINK_TARGET_INVALID = "link_target_invalid"
    SPEC_LOCKED = "spec_locked"
    SPEC_NOT_FOUND = "spec_not_found"


@dataclass(slots=True)
class StructuredSpecEntityMetricEvent:
    name: str
    labels: dict[str, str | int | bool | None]


class StructuredSpecEntityMetricsSink(Protocol):
    def record(self, event: StructuredSpecEntityMetricEvent) -> None:
        """Record one structured entity operation metric."""


@dataclass(slots=True)
class InMemoryStructuredSpecEntityMetricsSink:
    events: list[StructuredSpecEntityMetricEvent] = field(default_factory=list)

    def record(self, event: StructuredSpecEntityMetricEvent) -> None:
        self.events.append(event)


@dataclass(slots=True)
class StructuredSpecEntityImpactRef:
    target_type: str
    target_id: str
    target_ref: str
    severity: str
    reason: str
    blocking: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "target_type": self.target_type,
            "target_id": self.target_id,
            "target_ref": self.target_ref,
            "severity": self.severity,
            "reason": self.reason,
            "blocking": self.blocking,
        }


@dataclass(slots=True)
class StructuredSpecEntityImpactReport:
    impacted_refs: list[StructuredSpecEntityImpactRef]
    counts_by_type: dict[str, int]
    ack_token: str | None = None
    expires_at: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "impacted_refs": [ref.as_dict() for ref in self.impacted_refs],
            "counts_by_type": self.counts_by_type,
            "ack_token": self.ack_token,
            "expires_at": self.expires_at,
        }


@dataclass(slots=True)
class StructuredSpecEntityAckRecord:
    board_id: str
    spec_id: str
    entity_type: str
    entity_id: str
    operation: str
    expected_spec_version: int
    impact_fingerprint: str
    expires_at: datetime
    materialized_field_name: str | None = None
    materialized_items: list[Any] | None = None
    related_updates: dict[str, Any] = field(default_factory=dict)


class StructuredSpecEntityAckStore(Protocol):
    def issue(self, record: StructuredSpecEntityAckRecord) -> str:
        """Persist a one-time acknowledgement token."""

    def peek(self, token: str) -> StructuredSpecEntityAckRecord | None:
        """Read an acknowledgement token without consuming it."""

    def consume(self, token: str) -> StructuredSpecEntityAckRecord | None:
        """Consume an acknowledgement token exactly once."""


class InMemoryStructuredSpecEntityAckStore:
    def __init__(self) -> None:
        self._records: dict[str, StructuredSpecEntityAckRecord] = {}

    def issue(self, record: StructuredSpecEntityAckRecord) -> str:
        token = secrets.token_urlsafe(24)
        self._records[token] = record
        return token

    def peek(self, token: str) -> StructuredSpecEntityAckRecord | None:
        record = self._records.get(token)
        if record and record.expires_at <= _utcnow():
            self._records.pop(token, None)
            return None
        return record

    def consume(self, token: str) -> StructuredSpecEntityAckRecord | None:
        record = self.peek(token)
        if record is not None:
            self._records.pop(token, None)
        return record


_DEFAULT_ACK_STORE = InMemoryStructuredSpecEntityAckStore()


@dataclass(slots=True)
class StructuredSpecEntityCommand:
    spec_id: str
    actor_id: str
    entity_type: str
    operation: str
    payload: dict[str, Any] = field(default_factory=dict)
    board_id: str | None = None
    entity_id: str | None = None
    expected_spec_version: int | None = None
    position: int | None = None
    task_id: str | None = None
    ack_token: str | None = None
    preview_only: bool = False
    permission_set: PermissionSet | None = None


@dataclass(slots=True)
class StructuredSpecEntityResult:
    success: bool
    entity_type: str
    operation: str
    spec_id: str
    entity_id: str | None = None
    child_ref: str | None = None
    spec_version: int | None = None
    changed_fields: list[str] = field(default_factory=list)
    error_code: str | None = None
    error_message: str | None = None
    required_permission: str | None = None
    impact_report: dict[str, Any] | None = None
    ack_token: str | None = None
    expires_at: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "entity_type": self.entity_type,
            "operation": self.operation,
            "spec_id": self.spec_id,
            "entity_id": self.entity_id,
            "child_ref": self.child_ref,
            "spec_version": self.spec_version,
            "changed_fields": self.changed_fields,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "required_permission": self.required_permission,
            "impact_report": self.impact_report,
            "ack_token": self.ack_token,
            "expires_at": self.expires_at,
        }


class StructuredSpecEntityService:
    """Mutates spec child entities through one typed, permissioned boundary."""

    def __init__(
        self,
        db: Any,
        *,
        metrics_sink: StructuredSpecEntityMetricsSink | None = None,
        ack_store: StructuredSpecEntityAckStore | None = None,
    ) -> None:
        self.db = db
        self.metrics_sink = metrics_sink
        self.ack_store = ack_store or _DEFAULT_ACK_STORE

    async def mutate(self, command: StructuredSpecEntityCommand) -> StructuredSpecEntityResult:
        if command.entity_type not in STRUCTURED_SPEC_ENTITY_FIELDS:
            return self._failure(command, StructuredSpecEntityErrorCode.UNSUPPORTED_ENTITY_TYPE)
        if command.operation not in STRUCTURED_SPEC_ENTITY_OPERATIONS:
            return self._failure(command, StructuredSpecEntityErrorCode.UNSUPPORTED_OPERATION)

        spec = await get_structured_spec_store().get(
            self.db,
            spec_id=command.spec_id,
        )
        if spec is None:
            return self._failure(command, StructuredSpecEntityErrorCode.SPEC_NOT_FOUND)
        if command.board_id is not None and spec.board_id != command.board_id:
            return self._failure(
                command,
                StructuredSpecEntityErrorCode.VALIDATION_FAILED,
                "Spec does not belong to the requested board.",
            )
        if command.board_id is None:
            command.board_id = spec.board_id
        if getattr(spec, "archived", False):
            return self._failure(
                command,
                StructuredSpecEntityErrorCode.VALIDATION_FAILED,
                "This spec is archived. Restore it first before making changes.",
            )

        permission = self._required_permission(command)
        if command.permission_set is not None:
            status = getattr(spec.status, "value", spec.status)
            err = command.permission_set.check_with_state(
                permission,
                entity="spec",
                status=str(status),
            )
            if err is not None:
                return self._failure(
                    command,
                    StructuredSpecEntityErrorCode.AUTHORIZATION_DENIED,
                    err,
                    required_permission=permission,
                )

        if (
            command.expected_spec_version is not None
            and command.expected_spec_version != spec.version
        ):
            return self._failure(
                command,
                StructuredSpecEntityErrorCode.VERSION_CONFLICT,
                f"Expected spec version {command.expected_spec_version}, found {spec.version}.",
            )

        try:
            await _require_spec_unlocked(self.db, spec.id)
        except SpecLockedError as exc:
            return self._failure(command, StructuredSpecEntityErrorCode.SPEC_LOCKED, str(exc))

        if command.preview_only and command.operation not in _DESTRUCTIVE_LIKE_OPERATIONS:
            return self._failure(
                command,
                StructuredSpecEntityErrorCode.UNSUPPORTED_OPERATION,
                "impact preview is only supported for revoke, supersede and reorder.",
            )

        field_name = STRUCTURED_SPEC_ENTITY_FIELDS[command.entity_type]
        current_items = copy.deepcopy(getattr(spec, field_name, None) or [])
        related_updates: dict[str, Any] = {}
        ack_record = self.ack_store.peek(command.ack_token) if command.ack_token else None
        if command.entity_type in _LEGACY_MATERIALIZED_ENTITY_TYPES:
            if (
                ack_record is not None
                and ack_record.materialized_field_name == field_name
                and ack_record.materialized_items is not None
            ):
                current_items = copy.deepcopy(ack_record.materialized_items)
                related_updates.update(copy.deepcopy(ack_record.related_updates))
            else:
                current_items, materialized = self._materialize_legacy_entities(
                    command.entity_type,
                    current_items,
                )
                related_updates.update(
                    self._migrate_materialized_legacy_refs(spec, command.entity_type, materialized)
                )
        try:
            if command.operation in _DESTRUCTIVE_LIKE_OPERATIONS:
                impact_result = self._handle_impact_ack(
                    spec=spec,
                    field_name=field_name,
                    command=command,
                    current_items=current_items,
                    related_updates=related_updates,
                    ack_record=ack_record,
                )
                if impact_result is not None:
                    return impact_result

            new_items, entity_id = await self._apply_operation(
                spec=spec,
                field_name=field_name,
                command=command,
                current_items=current_items,
            )
            update_data = {field_name: new_items, **related_updates}

            # Every structured semantic writer crosses the same final-state
            # FR/TR/AC canonicalization boundary as bulk create/update. This
            # also lazily materializes untouched legacy collections and
            # validates cross-collection ID uniqueness before any mutation.
            requirement_fields_before = {
                requirement_field: copy.deepcopy(
                    getattr(spec, requirement_field, None)
                )
                for requirement_field, _ in SPEC_REQUIREMENT_FIELDS
            }
            requirement_fields_final = {
                requirement_field: (
                    update_data[requirement_field]
                    if requirement_field in update_data
                    else current_value
                )
                for (
                    requirement_field,
                    current_value,
                ) in requirement_fields_before.items()
            }
            canonical_requirements = canonicalize_spec_requirement_fields(
                requirement_fields_final,
                existing_fields=requirement_fields_before,
            )
            for requirement_field, canonical_value in (
                canonical_requirements.items()
            ):
                if (
                    requirement_field in update_data
                    or canonical_value
                    != requirement_fields_before[requirement_field]
                ):
                    update_data[requirement_field] = canonical_value

            if command.operation == "create" and field_name in canonical_requirements:
                created = canonical_requirements[field_name] or []
                entity_id = spec_child_id(created[-1]) if created else entity_id

            old_frs = list(
                requirement_fields_before["functional_requirements"] or []
            )
            new_frs = list(
                canonical_requirements["functional_requirements"] or []
            )
            if old_frs != new_frs:
                fr_dependencies = {
                    dependent_field: list(
                        update_data.get(
                            dependent_field,
                            getattr(spec, dependent_field, None) or [],
                        )
                        or []
                    )
                    for dependent_field in (
                        "business_rules",
                        "api_contracts",
                        "integration_requirements",
                        "observability_requirements",
                        "decisions",
                    )
                }
                update_data.update(
                    migrate_legacy_fr_refs(
                        old_frs,
                        new_frs,
                        fr_dependencies,
                    )
                )

            old_acs = list(
                requirement_fields_before["acceptance_criteria"] or []
            )
            new_acs = list(canonical_requirements["acceptance_criteria"] or [])
            if old_acs != new_acs:
                migrated_scenarios = migrate_legacy_ac_refs(
                    old_acs,
                    new_acs,
                    list(
                        update_data.get(
                            "test_scenarios",
                            getattr(spec, "test_scenarios", None) or [],
                        )
                        or []
                    ),
                )
                if migrated_scenarios is not None:
                    update_data["test_scenarios"] = migrated_scenarios
            await _validate_spec_linked_refs(self.db, spec, update_data)
        except UnsupportedSpecEntityOperation as exc:
            return self._failure(command, StructuredSpecEntityErrorCode.UNSUPPORTED_OPERATION, str(exc))
        except StructuredSpecEntityNotFound as exc:
            return self._failure(command, StructuredSpecEntityErrorCode.ENTITY_NOT_FOUND, str(exc))
        except ValueError as exc:
            code = (
                StructuredSpecEntityErrorCode.LINK_TARGET_INVALID
                if "linked_" in str(exc) or "Card" in str(exc)
                else StructuredSpecEntityErrorCode.VALIDATION_FAILED
            )
            return self._failure(command, code, str(exc))

        old_version = spec.version
        old_values = {field_name: copy.deepcopy(getattr(spec, field_name, None) or [])}
        for changed_field, value in update_data.items():
            old_values.setdefault(changed_field, copy.deepcopy(getattr(spec, changed_field, None) or []))
            setattr(spec, changed_field, value)
        spec.version = old_version + 1

        changed_fields = list(update_data.keys())
        await event_publish(
            SpecVersionBumped(
                board_id=spec.board_id,
                actor_id=command.actor_id,
                spec_id=spec.id,
                old_version=old_version,
                new_version=spec.version,
                changed_fields=changed_fields,
            ),
            session=self.db,
        )
        semantic_changed = [changed_field for changed_field in changed_fields if changed_field in _SEMANTIC_FIELDS]
        if semantic_changed:
            await event_publish(
                SpecSemanticChanged(
                    board_id=spec.board_id,
                    actor_id=command.actor_id,
                    spec_id=spec.id,
                    changed_fields=semantic_changed,
                ),
                session=self.db,
            )
        child_ref = canonical_spec_child_ref(spec.id, command.entity_type, entity_id) if entity_id else None
        if child_ref and entity_id:
            structured_event_cls = self._structured_event_class(command.operation)
            event_changed_fields = self._event_changed_fields(
                entity_type=command.entity_type,
                field_name=field_name,
                entity_id=entity_id,
                operation=command.operation,
                payload=command.payload,
                changed_fields=changed_fields,
            )
            await event_publish(
                structured_event_cls(
                    board_id=spec.board_id,
                    actor_id=command.actor_id,
                    spec_id=spec.id,
                    entity_type=command.entity_type,
                    entity_id=entity_id,
                    child_ref=child_ref,
                    operation=command.operation,
                    changed_fields=event_changed_fields,
                    spec_version=spec.version,
                ),
                session=self.db,
            )
            self._record_metric(
                "spec_structured_entity_event_emitted_total",
                {
                    "board_id": spec.board_id,
                    "spec_id": spec.id,
                    "entity_type": command.entity_type,
                    "operation": command.operation,
                    "event_type": structured_event_cls.event_type,
                    "outcome": "emitted",
                },
            )

        actor_name = await resolve_actor_name(self.db, command.actor_id, spec.board_id)
        service = SpecService(self.db)
        changes = service._compute_diff(
            old_values,
            update_data,
            changed_fields,
        )
        await service._record_history(
            spec_id=spec.id,
            action=f"structured_{command.operation}",
            actor_id=command.actor_id,
            actor_name=actor_name,
            changes=changes,
            version=spec.version,
            summary=(
                f"{command.operation} {command.entity_type} "
                f"{entity_id or command.entity_id or ''}".strip()
            ),
        )
        await get_structured_spec_store().save(
            self.db,
            spec,
            changed_fields=changed_fields,
        )
        await stage_spec_requirement_lint(
            self.db,
            spec,
            actor_id=command.actor_id,
            writer=RequirementLintWriter.STRUCTURED_CRUD,
            changed_fields=tuple(changed_fields),
        )

        result = StructuredSpecEntityResult(
            success=True,
            entity_type=command.entity_type,
            operation=command.operation,
            spec_id=spec.id,
            entity_id=entity_id,
            child_ref=child_ref,
            spec_version=spec.version,
            changed_fields=changed_fields,
        )
        self._emit(command, "success", None)
        return result

    async def apply(self, command: StructuredSpecEntityCommand) -> StructuredSpecEntityResult:
        """Compatibility alias for API/MCP callers named in the spec contract."""
        return await self.mutate(command)

    def _handle_impact_ack(
        self,
        *,
        spec: StructuredSpecRecord,
        field_name: str,
        command: StructuredSpecEntityCommand,
        current_items: list[Any],
        related_updates: dict[str, Any],
        ack_record: StructuredSpecEntityAckRecord | None,
    ) -> StructuredSpecEntityResult | None:
        entity_id = self._operation_scope_entity_id(command, current_items)
        impact = self._build_impact_report(
            spec=spec,
            command=command,
            field_name=field_name,
            current_items=current_items,
            related_updates=related_updates,
            entity_id=entity_id,
        )
        if not impact.impacted_refs:
            if command.preview_only:
                return StructuredSpecEntityResult(
                    success=True,
                    entity_type=command.entity_type,
                    operation=command.operation,
                    spec_id=spec.id,
                    entity_id=entity_id,
                    child_ref=canonical_spec_child_ref(spec.id, command.entity_type, entity_id),
                    spec_version=spec.version,
                    impact_report=impact.as_dict(),
                )
            return None

        fingerprint = self._impact_fingerprint(impact)
        expected_version = command.expected_spec_version if command.expected_spec_version is not None else spec.version
        board_id = command.board_id or spec.board_id

        if not command.ack_token:
            expires_at = _utcnow() + _ACK_TTL
            record = StructuredSpecEntityAckRecord(
                board_id=board_id,
                spec_id=spec.id,
                entity_type=command.entity_type,
                entity_id=entity_id,
                operation=command.operation,
                expected_spec_version=expected_version,
                impact_fingerprint=fingerprint,
                expires_at=expires_at,
                materialized_field_name=field_name if command.entity_type in _TEXT_ENTITY_TYPES else None,
                materialized_items=copy.deepcopy(current_items) if command.entity_type in _TEXT_ENTITY_TYPES else None,
                related_updates=copy.deepcopy(related_updates),
            )
            token = self.ack_store.issue(record)
            impact.ack_token = token
            impact.expires_at = expires_at.isoformat()
            self._emit(command, "impact_ack_required", StructuredSpecEntityErrorCode.IMPACT_ACK_REQUIRED)
            return StructuredSpecEntityResult(
                success=bool(command.preview_only),
                entity_type=command.entity_type,
                operation=command.operation,
                spec_id=spec.id,
                entity_id=entity_id,
                child_ref=canonical_spec_child_ref(spec.id, command.entity_type, entity_id)
                if entity_id != "__collection__"
                else canonical_spec_child_ref(spec.id, command.entity_type, entity_id),
                spec_version=spec.version,
                error_code=None if command.preview_only else StructuredSpecEntityErrorCode.IMPACT_ACK_REQUIRED,
                error_message=None if command.preview_only else "Impact acknowledgement is required before applying this operation.",
                impact_report=impact.as_dict(),
                ack_token=token,
                expires_at=impact.expires_at,
            )

        if ack_record is None:
            return self._failure(
                command,
                StructuredSpecEntityErrorCode.IMPACT_ACK_INVALID,
                "Acknowledgement token is invalid or expired.",
            )

        if (
            ack_record.board_id != board_id
            or ack_record.spec_id != spec.id
            or ack_record.entity_type != command.entity_type
            or ack_record.entity_id != entity_id
            or ack_record.operation != command.operation
            or ack_record.expected_spec_version != expected_version
            or ack_record.impact_fingerprint != fingerprint
        ):
            return self._failure(
                command,
                StructuredSpecEntityErrorCode.IMPACT_ACK_INVALID,
                "Acknowledgement token does not match the current operation, version or impact set.",
            )

        consumed = self.ack_store.consume(command.ack_token)
        if consumed is None:
            return self._failure(
                command,
                StructuredSpecEntityErrorCode.IMPACT_ACK_INVALID,
                "Acknowledgement token is invalid or expired.",
            )
        self._emit(command, "impact_acknowledged", None)
        return None

    def _operation_scope_entity_id(
        self,
        command: StructuredSpecEntityCommand,
        current_items: list[Any],
    ) -> str:
        if command.operation == "reorder":
            return "__collection__"
        index = self._find_index(command.entity_type, current_items, command.entity_id)
        entity_id = self._entity_id(command.entity_type, current_items[index])
        return entity_id or str(index)

    def _build_impact_report(
        self,
        *,
        spec: StructuredSpecRecord,
        command: StructuredSpecEntityCommand,
        field_name: str,
        current_items: list[Any],
        related_updates: dict[str, Any],
        entity_id: str,
    ) -> StructuredSpecEntityImpactReport:
        refs: list[StructuredSpecEntityImpactRef] = []
        impacted_keys: set[tuple[str, str, str]] = set()

        def add_ref(target_type: str, target_id: str, target_ref: str, reason: str, severity: str = "high") -> None:
            key = (target_type, target_id, reason)
            if key in impacted_keys:
                return
            impacted_keys.add(key)
            refs.append(
                StructuredSpecEntityImpactRef(
                    target_type=target_type,
                    target_id=target_id,
                    target_ref=target_ref,
                    severity=severity,
                    reason=reason,
                    blocking=True,
                )
            )

        target_ids = self._reorder_target_ids(command, current_items) if command.operation == "reorder" else [entity_id]
        target_id_set = set(target_ids)
        target_ref_set = self._target_ref_aliases(command.entity_type, current_items, target_id_set)

        for item in current_items:
            item_id = self._entity_id(command.entity_type, item)
            if item_id not in target_id_set:
                continue
            for task_id in self._linked_task_ids(item):
                add_ref(
                    "card",
                    task_id,
                    f"card:{task_id}",
                    f"{command.operation}_affects_linked_task",
                    "medium",
                )

        for target_field, target_type, link_field in self._downstream_link_specs(command.entity_type):
            collection = copy.deepcopy(related_updates.get(target_field, getattr(spec, target_field, None) or []))
            for item in collection:
                if not isinstance(item, dict):
                    continue
                raw_links = item.get(link_field)
                if isinstance(raw_links, list):
                    links = [str(ref) for ref in raw_links]
                elif raw_links in (None, ""):
                    links = []
                else:
                    links = [str(raw_links)]
                matched = sorted(target_ref_set.intersection(links))
                if not matched:
                    continue
                target_id = spec_child_id(item) or item.get("title") or item.get("path") or "unknown"
                add_ref(
                    target_type,
                    str(target_id),
                    canonical_spec_child_ref(spec.id, target_type, str(target_id)),
                    f"{command.operation}_affects_{link_field}",
                )

        counts: dict[str, int] = {}
        for ref in refs:
            counts[ref.target_type] = counts.get(ref.target_type, 0) + 1
        return StructuredSpecEntityImpactReport(impacted_refs=refs, counts_by_type=counts)

    def _target_ref_aliases(
        self,
        entity_type: str,
        current_items: list[Any],
        target_id_set: set[str],
    ) -> set[str]:
        refs = set(target_id_set)
        if entity_type not in _TEXT_ENTITY_TYPES:
            return refs
        for index, item in enumerate(current_items):
            item_id = self._entity_id(entity_type, item)
            if item_id not in target_id_set:
                continue
            refs.add(str(index))
            text = spec_child_text(item).strip()
            if text:
                refs.add(text)
        return refs

    def _impact_fingerprint(self, impact: StructuredSpecEntityImpactReport) -> str:
        payload = sorted(
            (ref.as_dict() for ref in impact.impacted_refs),
            key=lambda item: (item["target_type"], item["target_id"], item["reason"]),
        )
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def _downstream_link_specs(self, entity_type: str) -> list[tuple[str, str, str]]:
        return {
            "functional_requirement": [
                ("business_rules", "business_rule", "linked_requirements"),
                ("api_contracts", "api_contract", "linked_requirements"),
                ("integration_requirements", "integration_requirement", "linked_requirements"),
                ("observability_requirements", "observability_requirement", "linked_requirements"),
                ("decisions", "decision", "linked_requirements"),
            ],
            "acceptance_criterion": [
                ("test_scenarios", "test_scenario", "linked_criteria"),
            ],
            "business_rule": [
                ("api_contracts", "api_contract", "linked_rules"),
            ],
            "api_contract": [
                ("integration_requirements", "integration_requirement", "linked_api_contracts"),
            ],
            "integration_requirement": [
                ("observability_requirements", "observability_requirement", "linked_integration_requirements"),
            ],
            "decision": [
                ("decisions", "decision", "supersedes_decision_id"),
            ],
        }.get(entity_type, [])

    def _linked_task_ids(self, item: Any) -> list[str]:
        if not isinstance(item, dict):
            return []
        linked = item.get("linked_task_ids") or []
        if not isinstance(linked, list):
            return []
        return [str(task_id) for task_id in linked if str(task_id)]

    def _reorder_target_ids(self, command: StructuredSpecEntityCommand, current_items: list[Any]) -> list[str]:
        ordered = command.payload.get("ordered_entity_ids")
        if not isinstance(ordered, list) or not ordered:
            raise ValueError("ordered_entity_ids must be a non-empty full ordered entity id list.")
        ordered_ids = [str(entity_id) for entity_id in ordered]
        current_ids = [self._entity_id(command.entity_type, item) for item in current_items]
        if any(entity_id in (None, "") for entity_id in current_ids):
            raise ValueError("All entities must have stable ids before reorder.")
        current_id_set = {str(entity_id) for entity_id in current_ids}
        ordered_id_set = set(ordered_ids)
        if len(ordered_ids) != len(ordered_id_set):
            raise ValueError("ordered_entity_ids must not contain duplicates.")
        if ordered_id_set != current_id_set:
            missing = sorted(current_id_set - ordered_id_set)
            unknown = sorted(ordered_id_set - current_id_set)
            raise ValueError(f"ordered_entity_ids must contain exactly all entity ids; missing={missing}, unknown={unknown}.")
        return ordered_ids

    async def _apply_operation(
        self,
        *,
        spec: StructuredSpecRecord,
        field_name: str,
        command: StructuredSpecEntityCommand,
        current_items: list[Any],
    ) -> tuple[list[Any], str | None]:
        operation = command.operation
        if operation == "create":
            item = self._validate_payload_for_create(command.entity_type, command.payload)
            if command.entity_type in _LEGACY_MATERIALIZED_ENTITY_TYPES:
                canonical = canonicalize_spec_children(
                    command.entity_type,
                    [*current_items, item],
                    existing_items=current_items,
                )
                assert canonical is not None
                item = canonical[-1]
            entity_id = self._entity_id(command.entity_type, item)
            self._reject_duplicate_id(command.entity_type, current_items, entity_id)
            return [*current_items, item], entity_id or str(len(current_items))

        if operation == "reorder":
            ordered_ids = self._reorder_target_ids(command, current_items)
            by_id = {
                str(self._entity_id(command.entity_type, item)): item
                for item in current_items
            }
            return [by_id[entity_id] for entity_id in ordered_ids], "__collection__"

        index = self._find_index(command.entity_type, current_items, command.entity_id)
        item = copy.deepcopy(current_items[index])

        if operation == "update":
            item = self._validate_payload_for_update(command.entity_type, item, command.payload)
        elif operation in {"link_task", "unlink_task"}:
            item = self._apply_task_link(command, item)
        elif operation in _LIFECYCLE_OPERATION_STATUS:
            item = self._apply_lifecycle(command, item)
        else:
            raise ValueError(f"Unsupported operation: {operation}")

        current_items[index] = item
        entity_id = self._entity_id(command.entity_type, item) or command.entity_id
        return current_items, entity_id

    def _validate_payload_for_create(self, entity_type: str, payload: dict[str, Any]) -> Any:
        payload = copy.deepcopy(payload or {})
        if entity_type in _TEXT_ENTITY_TYPES:
            self._ensure_only_keys(
                payload,
                {
                    "text",
                    "title",
                    "id",
                    "locale",
                    "status",
                    "notes",
                    "linked_task_ids",
                },
            )
            text = str(payload.get("text") or payload.get("title") or "").strip()
            if not text:
                raise ValueError("text is required.")
            return {
                **({"id": payload["id"]} if payload.get("id") else {}),
                "text": text,
                "status": payload.get("status") or "active",
                **(
                    {"locale": payload["locale"]}
                    if payload.get("locale") is not None
                    else {}
                ),
                **({"notes": payload["notes"]} if payload.get("notes") is not None else {}),
                **({"linked_task_ids": payload["linked_task_ids"]} if payload.get("linked_task_ids") is not None else {}),
            }
        if entity_type == "technical_requirement":
            self._validate_technical_requirement_payload(payload, creating=True)
            return payload

        model = _STRUCTURED_MODEL_BY_TYPE[entity_type]
        payload.setdefault("id", self._new_id(entity_type))
        self._ensure_only_keys(payload, set(model.model_fields))
        # api_contract validates with the on_write context (F9 http strictness)
        # and returns a canonical message with no errors.pydantic.dev URL (F10).
        _ctx = _API_CONTRACT_WRITE_CONTEXT if entity_type == "api_contract" else None
        try:
            return model.model_validate(payload, context=_ctx).model_dump()
        except ValidationError as exc:
            if entity_type == "api_contract":
                raise ValueError(_api_contract_validation_message(exc)) from exc
            raise ValueError(str(exc)) from exc

    def _validate_payload_for_update(self, entity_type: str, item: Any, payload: dict[str, Any]) -> Any:
        payload = copy.deepcopy(payload or {})
        if not payload:
            raise ValueError("payload must contain at least one field for update.")
        if entity_type in _TEXT_ENTITY_TYPES:
            item_dict = self._as_dict(item)
            payload.pop("id", None)
            self._ensure_only_keys(
                payload,
                {
                    "text",
                    "title",
                    "locale",
                    "status",
                    "notes",
                    "linked_task_ids",
                },
            )
            if "text" in payload or "title" in payload:
                text = str(payload.get("text") or payload.get("title") or "").strip()
                if not text:
                    raise ValueError("text is required.")
                item_dict["text"] = text
            for key in ("locale", "status", "notes", "linked_task_ids"):
                if key in payload:
                    item_dict[key] = payload[key]
            if "linked_task_ids" in item_dict and item_dict["linked_task_ids"] is not None:
                if not isinstance(item_dict["linked_task_ids"], list):
                    raise ValueError("linked_task_ids must be a list.")
            return item_dict
        if entity_type == "technical_requirement":
            merged = self._as_dict(item)
            payload.pop("id", None)
            self._ensure_only_keys(payload, _TECHNICAL_REQUIREMENT_FIELDS - {"id"})
            merged.update(payload)
            self._validate_technical_requirement_payload(merged, creating=False)
            return merged

        model = _STRUCTURED_MODEL_BY_TYPE[entity_type]
        merged = self._as_dict(item)
        payload.pop("id", None)
        self._ensure_only_keys(payload, set(model.model_fields) - {"id"})
        merged.update(payload)
        # api_contract validates with the on_write context (F9 http strictness)
        # and returns a canonical message with no errors.pydantic.dev URL (F10).
        _ctx = _API_CONTRACT_WRITE_CONTEXT if entity_type == "api_contract" else None
        try:
            return model.model_validate(merged, context=_ctx).model_dump()
        except ValidationError as exc:
            if entity_type == "api_contract":
                raise ValueError(_api_contract_validation_message(exc)) from exc
            raise ValueError(str(exc)) from exc

    def _apply_task_link(self, command: StructuredSpecEntityCommand, item: Any) -> Any:
        if not command.task_id:
            raise ValueError("task_id is required.")
        item_dict = self._as_dict(item)
        linked = list(item_dict.get("linked_task_ids") or [])
        if command.operation == "link_task" and command.task_id not in linked:
            linked.append(command.task_id)
        if command.operation == "unlink_task":
            linked = [task_id for task_id in linked if task_id != command.task_id]
        item_dict["linked_task_ids"] = linked
        return item_dict

    def _apply_lifecycle(self, command: StructuredSpecEntityCommand, item: Any) -> Any:
        item_dict = self._as_dict(item)
        item_dict["status"] = _LIFECYCLE_OPERATION_STATUS[command.operation]
        if command.operation == "supersede" and command.entity_type == "decision":
            supersedes = command.payload.get("supersedes_decision_id")
            if supersedes:
                item_dict["supersedes_decision_id"] = supersedes
        return self._validate_payload_for_update(command.entity_type, item, item_dict)

    def _find_index(self, entity_type: str, items: list[Any], entity_id: str | None) -> int:
        if not entity_id:
            raise ValueError("entity_id is required.")
        if entity_type in _TEXT_ENTITY_TYPES and entity_id.isdigit():
            index = int(entity_id)
            if 0 <= index < len(items):
                return index
        for index, item in enumerate(items):
            if self._entity_id(entity_type, item) == entity_id:
                return index
        raise StructuredSpecEntityNotFound(f"Entity not found: {entity_id}")

    def _entity_id(self, entity_type: str, item: Any) -> str | None:
        if isinstance(item, dict):
            return spec_child_id(item)
        return getattr(item, "id", None)

    def _materialize_legacy_entities(
        self,
        entity_type: str,
        items: list[Any],
    ) -> tuple[list[Any], list[dict[str, str]]]:
        canonical = canonicalize_spec_children(
            entity_type,
            items,
            existing_items=items,
        )
        assert canonical is not None
        return canonical, _build_materialization_mappings(items, canonical)

    def _migrate_materialized_legacy_refs(
        self,
        spec: StructuredSpecRecord,
        entity_type: str,
        mappings: list[dict[str, str]],
    ) -> dict[str, Any]:
        if not mappings:
            return {}
        if entity_type == "functional_requirement":
            return self._migrate_functional_requirement_refs(spec, mappings)
        if entity_type == "acceptance_criterion":
            return self._migrate_acceptance_criterion_refs(spec, mappings)
        return {}

    def _migrate_functional_requirement_refs(
        self,
        spec: StructuredSpecRecord,
        mappings: list[dict[str, str]],
    ) -> dict[str, Any]:
        replacements = self._legacy_replacement_map(mappings)
        updates: dict[str, Any] = {}
        for field_name in (
            "business_rules",
            "api_contracts",
            "integration_requirements",
            "observability_requirements",
            "decisions",
        ):
            collection = copy.deepcopy(getattr(spec, field_name, None) or [])
            changed = False
            for item in collection:
                if not isinstance(item, dict):
                    continue
                refs, did_change = self._replace_refs(item.get("linked_requirements") or [], replacements)
                if did_change:
                    item["linked_requirements"] = refs
                    changed = True
            if changed:
                updates[field_name] = collection
        return updates

    def _migrate_acceptance_criterion_refs(
        self,
        spec: StructuredSpecRecord,
        mappings: list[dict[str, str]],
    ) -> dict[str, Any]:
        replacements = self._legacy_replacement_map(mappings)
        scenarios = copy.deepcopy(getattr(spec, "test_scenarios", None) or [])
        changed = False
        for item in scenarios:
            if not isinstance(item, dict):
                continue
            refs, did_change = self._replace_refs(item.get("linked_criteria") or [], replacements)
            if did_change:
                item["linked_criteria"] = refs
                changed = True
        return {"test_scenarios": scenarios} if changed else {}

    def _legacy_replacement_map(self, mappings: list[dict[str, str]]) -> dict[str, str]:
        replacements: dict[str, str] = {}
        for item in mappings:
            replacements[item["index"]] = item["id"]
            if item["text"]:
                replacements[item["text"]] = item["id"]
        return replacements

    def _replace_refs(self, refs: list[Any], replacements: dict[str, str]) -> tuple[list[str], bool]:
        changed = False
        next_refs: list[str] = []
        for ref in refs:
            ref_str = str(ref)
            replacement = replacements.get(ref_str, ref_str)
            if replacement != ref_str:
                changed = True
            if replacement not in next_refs:
                next_refs.append(replacement)
        return next_refs, changed

    def _reject_duplicate_id(self, entity_type: str, items: list[Any], entity_id: str | None) -> None:
        if not entity_id:
            return
        if any(self._entity_id(entity_type, item) == entity_id for item in items):
            raise ValueError(f"Duplicate {entity_type} id: {entity_id}")

    def _validate_technical_requirement_payload(self, payload: dict[str, Any], *, creating: bool) -> None:
        self._ensure_only_keys(payload, _TECHNICAL_REQUIREMENT_FIELDS)
        if not creating and not str(payload.get("id") or "").strip():
            raise ValueError("id is required.")
        text = str(payload.get("text") or payload.get("title") or "").strip()
        if not text:
            raise ValueError("text is required.")
        if "linked_task_ids" in payload and payload["linked_task_ids"] is not None:
            if not isinstance(payload["linked_task_ids"], list):
                raise ValueError("linked_task_ids must be a list.")

    def _ensure_only_keys(self, payload: dict[str, Any], allowed: set[str]) -> None:
        extra = sorted(set(payload) - allowed)
        if extra:
            raise ValueError(f"Unknown fields: {', '.join(extra)}")

    def _as_dict(self, item: Any) -> dict[str, Any]:
        if isinstance(item, dict):
            return copy.deepcopy(item)
        if hasattr(item, "model_dump"):
            return item.model_dump()
        raise ValueError("Structured object is required for this operation.")

    def _new_id(self, entity_type: str) -> str:
        return f"{_ID_PREFIX_BY_TYPE[entity_type]}_{uuid.uuid4().hex[:8]}"

    def _required_permission(self, command: StructuredSpecEntityCommand) -> str:
        return f"spec.structured_entity.{command.entity_type}.{command.operation}"

    def _structured_event_class(self, operation: str):
        if operation == "create":
            return StructuredSpecEntityCreated
        if operation in {"revoke", "supersede"}:
            return StructuredSpecEntityRevoked
        return StructuredSpecEntityUpdated

    def _event_changed_fields(
        self,
        *,
        entity_type: str,
        field_name: str,
        entity_id: str,
        operation: str,
        payload: dict[str, Any],
        changed_fields: list[str],
    ) -> list[str]:
        if operation == "reorder":
            return [field_name]
        prefix = f"{field_name}.{entity_id}"
        if operation in {"link_task", "unlink_task"}:
            return [f"{prefix}.linked_task_ids"]
        if operation in {"revoke", "supersede", "restore"}:
            return [f"{prefix}.status"]
        if payload:
            return [f"{prefix}.{key}" for key in sorted(payload)]
        if entity_type in _TEXT_ENTITY_TYPES:
            return [f"{prefix}.text"]
        return changed_fields or [prefix]

    def _failure(
        self,
        command: StructuredSpecEntityCommand,
        code: str,
        message: str | None = None,
        *,
        required_permission: str | None = None,
    ) -> StructuredSpecEntityResult:
        self._emit(command, "failure", code)
        return StructuredSpecEntityResult(
            success=False,
            entity_type=command.entity_type,
            operation=command.operation,
            spec_id=command.spec_id,
            entity_id=command.entity_id,
            error_code=code,
            error_message=message or code,
            required_permission=required_permission,
        )

    def _emit(
        self,
        command: StructuredSpecEntityCommand,
        outcome: str,
        reason: str | None,
    ) -> None:
        labels = {
            "board_id": command.board_id or "unknown",
            "spec_id": command.spec_id,
            "entity_type": command.entity_type,
            "operation": command.operation,
            "outcome": outcome,
            "reason": reason,
        }
        self._record_metric("spec_structured_entity_operation_total", labels)
        self._record_metric("spec_structured_entity_mutation_total", labels)
        if reason == StructuredSpecEntityErrorCode.VALIDATION_FAILED:
            self._record_metric("spec_structured_entity_validation_failure_total", labels)
        elif reason == StructuredSpecEntityErrorCode.AUTHORIZATION_DENIED:
            self._record_metric("spec_structured_entity_authorization_denied_total", labels)
        elif reason == StructuredSpecEntityErrorCode.VERSION_CONFLICT:
            self._record_metric("spec_structured_entity_version_conflict_total", labels)
        elif reason in {
            StructuredSpecEntityErrorCode.IMPACT_ACK_REQUIRED,
            StructuredSpecEntityErrorCode.IMPACT_ACK_INVALID,
        }:
            self._record_metric("spec_structured_entity_impact_total", labels)

    def _record_metric(
        self,
        name: str,
        labels: dict[str, str | int | bool | None],
    ) -> None:
        if self.metrics_sink is None:
            return
        try:
            self.metrics_sink.record(
                StructuredSpecEntityMetricEvent(
                    name=name,
                    labels=labels,
                )
            )
        except Exception:
            return
