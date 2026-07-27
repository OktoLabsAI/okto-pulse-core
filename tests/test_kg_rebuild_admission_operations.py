from __future__ import annotations

import pytest

from okto_pulse.core.application.kg_operations import CoreKnowledgeGraphOperations
from okto_pulse.core.application.kg_rebuild import refuse_rebuild_if_quarantined


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("graph_state", "should_refuse"),
    [("healthy", False), ("quarantined", True)],
)
async def test_rebuild_admission_uses_operation_health_probe(
    monkeypatch: pytest.MonkeyPatch,
    graph_state: str,
    should_refuse: bool,
) -> None:
    relational_context = object()
    scheduler_control = object()
    calls: list[tuple[str, object, object | None]] = []

    async def fake_health(
        board_id: str,
        context: object,
        *,
        scheduler_control: object | None = None,
    ) -> dict[str, object]:
        calls.append((board_id, context, scheduler_control))
        return {"graph_state": graph_state}

    monkeypatch.setattr(
        "okto_pulse.core.services.kg_health_service.get_kg_health",
        fake_health,
    )
    operations = CoreKnowledgeGraphOperations(relational_context)

    result = await operations.invoke_rebuild_admission(
        refuse_rebuild_if_quarantined,
        "board-e2e",
        scheduler_control=scheduler_control,
    )

    assert (result is not None) is should_refuse
    assert calls == [("board-e2e", relational_context, scheduler_control)]
