from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from okto_pulse.core.application.effective_knowledge_read import (
    attach_effective_knowledge,
    load_effective_card_knowledge,
    load_effective_spec_knowledge,
)
from okto_pulse.core.ports.application_persistence import ApplicationRecord
from okto_pulse.core.services.knowledge_propagation import (
    KnowledgePropagationServiceError,
)
from okto_pulse.core.services.reference_resolution import (
    resolve_spec_references,
    resolve_task_context_references,
)


class _KnowledgeRead:
    def __init__(
        self,
        result: Any = None,
        *,
        error: Exception | None = None,
    ) -> None:
        self.result = result
        self.error = error
        self.targets: list[Any] = []

    async def read(self, target: Any) -> Any:
        self.targets.append(target)
        if self.error is not None:
            raise self.error
        return self.result


class _ResourceGate:
    def __init__(self, resources: list[dict[str, Any]]) -> None:
        self.resources = resources
        self.calls: list[tuple[str, str, str]] = []

    async def get_effective_resources(
        self,
        board_id: str,
        entity_type: str,
        entity_id: str,
    ) -> dict[str, Any]:
        self.calls.append((board_id, entity_type, entity_id))
        return {"resources": {"knowledge_base": self.resources}}


def _card() -> SimpleNamespace:
    return SimpleNamespace(
        id="card-1",
        board_id="board-1",
        knowledge_bases=[
            {
                "id": "legacy-copy",
                "title": "Physical legacy history",
                "content": "must not leak once v2 is active",
            }
        ],
    )


@pytest.mark.asyncio
async def test_v1_read_keeps_physical_card_projection() -> None:
    gate = _ResourceGate([])
    services = SimpleNamespace(
        knowledge_propagation=_KnowledgeRead(
            SimpleNamespace(v2_active=False)
        ),
        resource_gate=gate,
    )

    result = await load_effective_card_knowledge(services, _card())

    assert [item["id"] for item in result] == ["legacy-copy"]
    assert gate.calls == []


@pytest.mark.asyncio
async def test_missing_knowledge_port_is_not_downgraded_to_legacy() -> None:
    services = SimpleNamespace(
        knowledge_propagation=_KnowledgeRead(
            error=RuntimeError("knowledge_propagation_port_not_configured")
        ),
        resource_gate=_ResourceGate([]),
    )

    with pytest.raises(
        RuntimeError,
        match="knowledge_propagation_port_not_configured",
    ):
        await load_effective_card_knowledge(services, _card())


@pytest.mark.asyncio
async def test_v2_read_uses_only_hydrated_effective_projection() -> None:
    gate = _ResourceGate(
        [
            {
                "id": "root-selected",
                "hydrated": True,
                "resource": {
                    "id": "kb-current",
                    "title": "Current source",
                    "content": "current bytes",
                },
                "ref": {
                    "knowledge_assignment_id": "assignment-1",
                    "knowledge_assignment_mode": "reference",
                    "knowledge_assignment_state": "active",
                    "knowledge_assignment_stale": False,
                    "origin_class": "v2",
                },
            }
        ]
    )
    services = SimpleNamespace(
        knowledge_propagation=_KnowledgeRead(
            SimpleNamespace(v2_active=True)
        ),
        resource_gate=gate,
    )

    result = await load_effective_card_knowledge(services, _card())

    assert [item["id"] for item in result] == ["kb-current"]
    assert result[0]["knowledge_assignment"] == {
        "assignment_id": "assignment-1",
        "mode": "reference",
        "state": "active",
        "stale": False,
        "origin_class": "v2",
    }
    assert gate.calls == [("board-1", "card", "card-1")]


@pytest.mark.asyncio
async def test_v2_unhydrated_effective_assignment_fails_closed() -> None:
    services = SimpleNamespace(
        knowledge_propagation=_KnowledgeRead(
            SimpleNamespace(v2_active=True)
        ),
        resource_gate=_ResourceGate(
            [
                {
                    "id": "root-selected",
                    "hydrated": False,
                    "resource": None,
                    "hydration_error": "source_deleted",
                }
            ]
        ),
    )

    with pytest.raises(KnowledgePropagationServiceError) as caught:
        await load_effective_card_knowledge(services, _card())

    assert caught.value.code == "knowledge_propagation_effective_read_failed"
    assert caught.value.details["resource_id"] == "root-selected"


@pytest.mark.asyncio
async def test_configured_scope_failure_is_not_downgraded_to_legacy() -> None:
    services = SimpleNamespace(
        knowledge_propagation=_KnowledgeRead(
            error=RuntimeError("configured_port_failed")
        ),
        resource_gate=_ResourceGate([]),
    )

    with pytest.raises(RuntimeError, match="configured_port_failed"):
        await load_effective_card_knowledge(services, _card())


@pytest.mark.asyncio
async def test_v2_spec_projection_excludes_physical_history_and_sets_parent() -> None:
    spec = SimpleNamespace(
        id="spec-1",
        board_id="board-1",
        title="Selected spec",
        knowledge_bases=[
            {
                "id": "physical-history",
                "title": "Must not leak",
                "content": "grandfathered bytes",
            }
        ],
    )
    gate = _ResourceGate(
        [
            {
                "id": "root-current",
                "hydrated": True,
                "resource": {
                    "id": "kb-current",
                    "title": "Current source",
                    "description": None,
                    "content": "current bytes",
                    "mime_type": "text/markdown",
                },
                "ref": {
                    "knowledge_assignment_id": "assignment-spec-1",
                    "knowledge_assignment_mode": "snapshot",
                    "knowledge_assignment_state": "active",
                    "knowledge_assignment_stale": False,
                    "origin_class": "v2",
                },
            }
        ]
    )
    services = SimpleNamespace(
        knowledge_propagation=_KnowledgeRead(
            SimpleNamespace(v2_active=True)
        ),
        resource_gate=gate,
    )

    result = await load_effective_spec_knowledge(services, spec)

    assert [item["id"] for item in result] == ["kb-current"]
    assert result[0]["spec_id"] == "spec-1"
    assert result[0]["knowledge_assignment"]["assignment_id"] == (
        "assignment-spec-1"
    )
    assert gate.calls == [("board-1", "spec", "spec-1")]


def test_application_record_projection_is_read_only() -> None:
    record = ApplicationRecord(
        entity="card",
        values={
            "id": "card-1",
            "board_id": "board-1",
            "knowledge_bases": [{"id": "physical-history"}],
        },
    )

    projected = attach_effective_knowledge(
        record,
        [{"id": "kb-current", "content": "current bytes"}],
    )

    assert projected is record
    assert record.knowledge_bases == [
        {"id": "kb-current", "content": "current bytes"}
    ]
    assert record.dirty_fields == set()


def test_full_context_reference_resolvers_use_only_projected_knowledge() -> None:
    physical_card = _card()
    physical_card.title = "Task"
    physical_spec = SimpleNamespace(
        id="spec-1",
        board_id="board-1",
        title="Spec",
        knowledge_bases=[{"id": "spec-physical", "content": "must not leak"}],
    )
    projected_card = attach_effective_knowledge(
        physical_card,
        [{"id": "card-current", "content": "selected card bytes"}],
    )
    projected_spec = attach_effective_knowledge(
        physical_spec,
        [{"id": "spec-current", "content": "selected spec bytes"}],
    )

    task_refs = resolve_task_context_references(
        projected_card,
        projected_spec,
    )
    spec_refs = resolve_spec_references(projected_spec)

    assert {
        item["id"] for item in task_refs["knowledge_bases"]
    } == {"card-current", "spec-current"}
    assert [item["id"] for item in spec_refs["knowledge_bases"]] == [
        "spec-current"
    ]
