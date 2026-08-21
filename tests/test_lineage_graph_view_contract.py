from __future__ import annotations

from inspect import signature

import pytest

from okto_pulse.core.application.service_catalog import (
    CoreApplicationServiceCatalog,
)
from okto_pulse.core.ports.traceability import (
    TraceabilityReadError,
    TraceabilityReadPort,
)
from okto_pulse.core.services import traceability as traceability_service


class _CapturingTraceabilityAdapter:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def build_lineage_graph(
        self,
        context: object,
        board_id: str,
        *,
        entity_type: str,
        entity_id: str,
        include_artifacts: bool = True,
        view: str = "lineage",
    ) -> dict[str, object]:
        call = {
            "context": context,
            "board_id": board_id,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "include_artifacts": include_artifacts,
            "view": view,
        }
        self.calls.append(call)
        return call


class _LegacyTraceabilityAdapter:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def build_lineage_graph(
        self,
        context: object,
        board_id: str,
        *,
        entity_type: str,
        entity_id: str,
        include_artifacts: bool = True,
    ) -> dict[str, object]:
        call = {
            "context": context,
            "board_id": board_id,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "include_artifacts": include_artifacts,
        }
        self.calls.append(call)
        return call


def test_traceability_port_keeps_lineage_as_the_default_view() -> None:
    parameter = signature(TraceabilityReadPort.build_lineage_graph).parameters["view"]

    assert parameter.default == "lineage"


@pytest.mark.asyncio
async def test_traceability_service_uses_legacy_signature_for_default_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _LegacyTraceabilityAdapter()
    monkeypatch.setattr(
        traceability_service,
        "resolve_traceability_adapter",
        lambda: adapter,
    )
    context = object()

    result = await traceability_service.build_lineage_graph(
        context,
        "board-1",
        entity_type="spec",
        entity_id="spec-1",
        include_artifacts=False,
    )

    assert result == {
        "context": context,
        "board_id": "board-1",
        "entity_type": "spec",
        "entity_id": "spec-1",
        "include_artifacts": False,
    }
    assert adapter.calls == [result]


@pytest.mark.asyncio
async def test_traceability_service_forwards_dependency_view(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _CapturingTraceabilityAdapter()
    monkeypatch.setattr(
        traceability_service,
        "resolve_traceability_adapter",
        lambda: adapter,
    )
    context = object()

    await traceability_service.build_lineage_graph(
        context,
        "board-1",
        entity_type="task",
        entity_id="task-1",
        include_artifacts=False,
        view="dependency",
    )

    assert [call["view"] for call in adapter.calls] == ["dependency"]


@pytest.mark.asyncio
async def test_traceability_service_rejects_invalid_view_before_adapter_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _CapturingTraceabilityAdapter()
    monkeypatch.setattr(
        traceability_service,
        "resolve_traceability_adapter",
        lambda: adapter,
    )

    with pytest.raises(TraceabilityReadError) as raised:
        await traceability_service.build_lineage_graph(
            object(),
            "board-1",
            entity_type="task",
            entity_id="task-1",
            view="sideways",  # type: ignore[arg-type]
        )

    assert raised.value.code == "invalid_lineage_graph_view"
    assert raised.value.status_code == 400
    assert adapter.calls == []


@pytest.mark.asyncio
async def test_application_service_catalog_forwards_the_view(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    async def _build_lineage_graph(
        context: object,
        board_id: str,
        **kwargs: object,
    ) -> dict[str, object]:
        captured.update(context=context, board_id=board_id, **kwargs)
        return captured

    monkeypatch.setattr(
        traceability_service,
        "build_lineage_graph",
        _build_lineage_graph,
    )
    context = object()
    catalog = CoreApplicationServiceCatalog(context)

    lineage_result = await catalog.build_lineage_graph(
        "board-1",
        entity_type="spec",
        entity_id="spec-1",
        include_artifacts=False,
    )
    assert "view" not in lineage_result

    captured.clear()
    dependency_result = await catalog.build_lineage_graph(
        "board-1",
        entity_type="task",
        entity_id="task-1",
        include_artifacts=False,
        view="dependency",
    )

    assert dependency_result == {
        "context": context,
        "board_id": "board-1",
        "entity_type": "task",
        "entity_id": "task-1",
        "include_artifacts": False,
        "view": "dependency",
    }


@pytest.mark.asyncio
async def test_application_service_catalog_rejects_invalid_view(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dispatched = False

    async def _build_lineage_graph(*args: object, **kwargs: object) -> object:
        nonlocal dispatched
        dispatched = True
        return {"args": args, "kwargs": kwargs}

    monkeypatch.setattr(
        traceability_service,
        "build_lineage_graph",
        _build_lineage_graph,
    )

    with pytest.raises(TraceabilityReadError) as raised:
        await CoreApplicationServiceCatalog(object()).build_lineage_graph(
            "board-1",
            entity_type="spec",
            entity_id="spec-1",
            include_artifacts=False,
            view="sideways",  # type: ignore[arg-type]
        )

    assert raised.value.code == "invalid_lineage_graph_view"
    assert dispatched is False
