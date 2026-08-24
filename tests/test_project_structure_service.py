from __future__ import annotations

import copy
import json

import pytest

from okto_pulse.core.domain.project_structure import validate_project_structure
from okto_pulse.core.domain.human_validation_cycle import SubjectEditRequiresDraftError
from okto_pulse.core.ports.structured_spec import (
    ProjectStructureMutationPersistenceResult,
    ProjectStructureMutationPersistenceState,
    StructuredSpecRecord,
)
from okto_pulse.core.services.spec_structured_entities import (
    StructuredSpecEntityCommand,
    StructuredSpecEntityErrorCode,
    StructuredSpecEntityService,
)


def _record(project_structure, *, version: int = 4) -> StructuredSpecRecord:
    return StructuredSpecRecord(
        id="spec-1",
        board_id="board-1",
        status="draft",
        version=version,
        archived=False,
        functional_requirements=None,
        business_rules=None,
        technical_requirements=None,
        decisions=None,
        acceptance_criteria=None,
        api_contracts=None,
        integration_requirements=None,
        observability_requirements=None,
        test_scenarios=None,
        project_structure=project_structure,
        project_structure_revision=2,
    )


class _AtomicStore:
    def __init__(
        self,
        state: ProjectStructureMutationPersistenceState,
        record: StructuredSpecRecord | None = None,
    ) -> None:
        self.state = state
        self.record = record
        self.calls = []

    async def get(self, context, *, spec_id):
        return self.record

    async def get_project_structure_receipt(self, *args, **kwargs):
        return None

    async def save_project_structure_mutation(
        self,
        context,
        record,
        *,
        expected_spec_version,
        expected_project_structure_revision,
        bump_spec_version,
        changed_fields,
        receipt,
    ):
        self.calls.append(
            {
                "expected_spec_version": expected_spec_version,
                "expected_structure_revision": expected_project_structure_revision,
                "bump_spec_version": bump_spec_version,
                "record_version": record.version,
                "changed_fields": list(changed_fields),
                "receipt": receipt,
                "nodes": copy.deepcopy(record.project_structure),
            }
        )
        return ProjectStructureMutationPersistenceResult(self.state)


def test_batch_authorization_expands_every_concrete_permission_leaf() -> None:
    service = StructuredSpecEntityService(object())
    command = StructuredSpecEntityCommand(
        board_id="board-1",
        spec_id="spec-1",
        actor_id="agent-1",
        entity_type="project_structure_node",
        operation="batch",
        payload={
            "operations": [
                {"operation": "create", "payload": {}},
                {
                    "operation": "link_evidence",
                    "entity_id": "psn_root",
                    "evidence_id": "evidence-1",
                },
            ]
        },
    )
    assert service._required_permissions(command) == [
        "spec.structured_entity.project_structure_node.create",
        "spec.structured_entity.project_structure_node.link_evidence",
    ]


async def _allow_domain_prechecks(monkeypatch: pytest.MonkeyPatch, store) -> None:
    from okto_pulse.core.services import spec_structured_entities as module

    async def unlocked(*args, **kwargs):
        return None

    async def valid_refs(*args, **kwargs):
        return None

    monkeypatch.setattr(module, "get_structured_spec_store", lambda: store)
    monkeypatch.setattr(module, "_require_spec_unlocked", unlocked)
    monkeypatch.setattr(module, "_validate_spec_linked_refs", valid_refs)


@pytest.mark.asyncio
async def test_noop_still_claims_idempotency_key_atomically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nodes = validate_project_structure(
        [
            {
                "id": "psn_file",
                "position": 0,
                "kind": "file",
                "name": "main.py",
                "classification": "as_is",
                "task_references": [{"task_id": "task-1", "role": "modify"}],
            }
        ]
    )
    spec = _record(nodes)
    store = _AtomicStore(ProjectStructureMutationPersistenceState.APPLIED)
    await _allow_domain_prechecks(monkeypatch, store)

    result = await StructuredSpecEntityService(object())._mutate_project_structure(
        spec,
        StructuredSpecEntityCommand(
            board_id="board-1",
            spec_id="spec-1",
            actor_id="agent-1",
            entity_type="project_structure_node",
            operation="link_task",
            entity_id="psn_file",
            task_id="task-1",
            task_role="modify",
            expected_spec_version=4,
            expected_structure_revision=2,
            idempotency_key="idem-noop",
        ),
    )

    assert result.success is True
    assert result.changed_fields == []
    assert store.calls[0]["expected_spec_version"] == 4
    assert store.calls[0]["expected_structure_revision"] == 2
    assert store.calls[0]["bump_spec_version"] is False
    assert store.calls[0]["changed_fields"] == []
    assert store.calls[0]["receipt"].result["details"]["nodes"]


@pytest.mark.asyncio
async def test_atomic_cas_conflict_restores_memory_and_emits_no_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.services import spec_structured_entities as module

    spec = _record([])
    store = _AtomicStore(ProjectStructureMutationPersistenceState.VERSION_CONFLICT)
    await _allow_domain_prechecks(monkeypatch, store)
    emitted = []

    async def publish(*args, **kwargs):
        emitted.append(args)

    monkeypatch.setattr(module, "event_publish", publish)
    result = await StructuredSpecEntityService(object())._mutate_project_structure(
        spec,
        StructuredSpecEntityCommand(
            board_id="board-1",
            spec_id="spec-1",
            actor_id="agent-1",
            entity_type="project_structure_node",
            operation="create",
            payload={
                "id": "psn_root",
                "position": 0,
                "kind": "folder",
                "name": "src",
                "classification": "to_be",
                "note": "must never leak into history on conflict",
            },
            expected_spec_version=4,
            expected_structure_revision=2,
            idempotency_key="idem-cas",
        ),
    )

    assert result.success is False
    assert result.error_code == StructuredSpecEntityErrorCode.VERSION_CONFLICT
    assert spec.version == 4
    assert spec.project_structure == []
    assert spec.project_structure_revision == 2
    assert emitted == []
    assert store.calls[0]["record_version"] == 5


@pytest.mark.asyncio
async def test_atomic_idempotency_race_returns_deterministic_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = _record([])
    store = _AtomicStore(ProjectStructureMutationPersistenceState.IDEMPOTENCY_CONFLICT)
    await _allow_domain_prechecks(monkeypatch, store)
    result = await StructuredSpecEntityService(object())._mutate_project_structure(
        spec,
        StructuredSpecEntityCommand(
            board_id="board-1",
            spec_id="spec-1",
            actor_id="agent-1",
            entity_type="project_structure_node",
            operation="create",
            payload={
                "id": "psn_root",
                "position": 0,
                "kind": "folder",
                "name": "src",
                "classification": "to_be",
            },
            expected_spec_version=4,
            expected_structure_revision=2,
            idempotency_key="idem-race",
        ),
    )
    assert result.success is False
    assert result.error_code == StructuredSpecEntityErrorCode.IDEMPOTENCY_CONFLICT
    assert spec.project_structure == []


@pytest.mark.asyncio
async def test_history_records_only_ids_digests_and_revisions_not_note_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.services import spec_structured_entities as module

    spec = _record([])
    store = _AtomicStore(ProjectStructureMutationPersistenceState.APPLIED)
    await _allow_domain_prechecks(monkeypatch, store)
    history_payloads = []

    async def publish(*args, **kwargs):
        return None

    async def actor_name(*args, **kwargs):
        return "Agent"

    async def record_history(self, **kwargs):
        history_payloads.append(kwargs)

    monkeypatch.setattr(module, "event_publish", publish)
    monkeypatch.setattr(module, "resolve_actor_name", actor_name)
    monkeypatch.setattr(module.SpecService, "_record_history", record_history)
    result = await StructuredSpecEntityService(object())._mutate_project_structure(
        spec,
        StructuredSpecEntityCommand(
            board_id="board-1",
            spec_id="spec-1",
            actor_id="agent-1",
            entity_type="project_structure_node",
            operation="create",
            payload={
                "id": "psn_secret",
                "position": 0,
                "kind": "file",
                "name": "secret.py",
                "classification": "to_be",
                "note": "sensitive implementation note",
            },
            expected_spec_version=4,
            expected_structure_revision=2,
            idempotency_key="idem-history",
        ),
    )
    assert result.success is True
    rendered = json.dumps(history_payloads, default=str)
    assert "sensitive implementation note" not in rendered
    assert "psn_secret" in rendered
    assert "digest" in rendered


@pytest.mark.asyncio
async def test_operational_task_link_advances_only_structure_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.services import spec_structured_entities as module

    nodes = validate_project_structure(
        [
            {
                "id": "psn_file",
                "position": 0,
                "kind": "file",
                "name": "main.py",
                "classification": "as_is",
            }
        ]
    )
    spec = _record(nodes)
    spec.status = "approved"
    store = _AtomicStore(ProjectStructureMutationPersistenceState.APPLIED, spec)
    await _allow_domain_prechecks(monkeypatch, store)
    events = []

    async def publish(event, **kwargs):
        events.append(type(event).__name__)

    async def actor_name(*args, **kwargs):
        return "Agent"

    async def record_history(self, **kwargs):
        return None

    monkeypatch.setattr(module, "event_publish", publish)
    monkeypatch.setattr(module, "resolve_actor_name", actor_name)
    monkeypatch.setattr(module.SpecService, "_record_history", record_history)
    result = await StructuredSpecEntityService(object()).mutate(
        StructuredSpecEntityCommand(
            board_id="board-1",
            spec_id="spec-1",
            actor_id="agent-1",
            entity_type="project_structure_node",
            operation="link_task",
            entity_id="psn_file",
            task_id="task-1",
            task_role="modify",
            expected_spec_version=4,
            expected_structure_revision=2,
            idempotency_key="idem-operational",
        )
    )
    assert result.success is True
    assert result.spec_version == 4
    assert result.structure_revision == 3
    assert spec.version == 4
    assert store.calls[0]["bump_spec_version"] is False
    assert events == ["StructuredSpecEntityUpdated"]


@pytest.mark.asyncio
async def test_mixed_operational_batch_remains_draft_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = _record([])
    spec.status = "approved"
    store = _AtomicStore(ProjectStructureMutationPersistenceState.APPLIED, spec)
    await _allow_domain_prechecks(monkeypatch, store)
    with pytest.raises(SubjectEditRequiresDraftError):
        await StructuredSpecEntityService(object()).mutate(
            StructuredSpecEntityCommand(
                board_id="board-1",
                spec_id="spec-1",
                actor_id="agent-1",
                entity_type="project_structure_node",
                operation="batch",
                payload={
                    "operations": [
                        {
                            "operation": "link_task",
                            "entity_id": "psn_file",
                            "task_id": "task-1",
                            "task_role": "read",
                        },
                        {
                            "operation": "update",
                            "entity_id": "psn_file",
                            "payload": {"note": "semantic change"},
                        },
                    ]
                },
                expected_spec_version=4,
                expected_structure_revision=2,
                idempotency_key="idem-mixed",
            )
        )


@pytest.mark.asyncio
async def test_relation_write_rejects_non_operational_spec_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = _record([])
    spec.status = "cancelled"
    store = _AtomicStore(ProjectStructureMutationPersistenceState.APPLIED, spec)
    await _allow_domain_prechecks(monkeypatch, store)
    result = await StructuredSpecEntityService(object()).mutate(
        StructuredSpecEntityCommand(
            board_id="board-1",
            spec_id="spec-1",
            actor_id="agent-1",
            entity_type="project_structure_node",
            operation="link_test",
            entity_id="psn_file",
            test_id="test-1",
            test_role="target",
            expected_spec_version=4,
            expected_structure_revision=2,
            idempotency_key="idem-cancelled",
        )
    )
    assert result.success is False
    assert (
        result.error_code
        == StructuredSpecEntityErrorCode.PROJECT_STRUCTURE_RELATION_STATUS_CONFLICT
    )
