from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any

import pytest

from okto_pulse.core.models.db import SpecStatus
from okto_pulse.core.services import discovery_executor


class _ScalarResult:
    def __init__(self, value: Any = None, values: list[Any] | None = None) -> None:
        self._value = value
        self._values = values or []

    def scalar_one_or_none(self) -> Any:
        return self._value

    def scalars(self) -> "_ScalarResult":
        return self

    def all(self) -> list[Any]:
        return self._values


class _SequenceSession:
    def __init__(self, results: list[_ScalarResult]) -> None:
        self._results = list(results)

    async def execute(self, _stmt: Any) -> _ScalarResult:
        assert self._results, "unexpected execute call"
        return self._results.pop(0)


def _selector_intent() -> SimpleNamespace:
    return SimpleNamespace(
        id="intent-selector",
        name="coverage_for_fr",
        tool_binding="okto_pulse_list_test_scenarios",
        params_schema={
            "fr_id": {
                "type": "spec_child_selector",
                "required": True,
                "child_types": ["functional_requirement"],
            }
        },
    )


def _card_selector_intent() -> SimpleNamespace:
    return SimpleNamespace(
        id="intent-card-selector",
        name="dependencies_of_card",
        tool_binding="okto_pulse_get_card_dependencies",
        params_schema={
            "card_id": {
                "type": "entity_selector",
                "entity_type": "card",
                "required": True,
                "label": "Card",
            }
        },
    )


def _spec(**overrides: Any) -> SimpleNamespace:
    defaults = {
        "id": "spec-1",
        "board_id": "board-1",
        "title": "Selector Spec",
        "status": SpecStatus.REVIEW,
        "functional_requirements": ["FR1 — Adicionar handler"],
        "technical_requirements": [],
        "acceptance_criteria": [],
        "test_scenarios": [],
        "business_rules": [],
        "api_contracts": [],
        "integration_requirements": [],
        "observability_requirements": [],
        "decisions": [],
        "skip_rules_coverage": False,
        "skip_test_coverage": False,
        "skip_trs_coverage": False,
        "skip_contract_coverage": False,
        "skip_ir_coverage": False,
        "skip_or_coverage": False,
        "skip_decisions_coverage": False,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _card(**overrides: Any) -> SimpleNamespace:
    defaults = {
        "id": "card-1",
        "board_id": "board-1",
        "title": "Dependency source card",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


@pytest.mark.asyncio
async def test_invalid_selector_is_rejected_before_tool_binding(monkeypatch):
    async def fail_if_called(*_args: Any, **_kwargs: Any) -> dict:
        raise AssertionError("tool binding must not run for invalid selector payloads")

    monkeypatch.setattr(discovery_executor, "_exec_test_scenarios", fail_if_called)

    with pytest.raises(discovery_executor.DiscoverySelectorExecutionError) as exc:
        await discovery_executor.execute_intent(
            db=None,
            user_id="user-1",
            board_id="board-1",
            intent=_selector_intent(),
            params={"fr_id": "not-a-child-ref"},
        )

    assert exc.value.code == "invalid_child_ref"
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_cross_board_selector_ref_is_forbidden(monkeypatch):
    async def load_other_board_spec(_db: Any, _spec_id: str) -> SimpleNamespace:
        return _spec(
            id="spec-other",
            board_id="board-other",
            title="Secret Other Board Spec",
            functional_requirements=["Secret FR label"],
        )

    monkeypatch.setattr(discovery_executor, "_load_spec_by_id", load_other_board_spec)

    with pytest.raises(discovery_executor.DiscoverySelectorExecutionError) as exc:
        await discovery_executor.execute_intent(
            db=None,
            user_id="user-1",
            board_id="board-1",
            intent=_selector_intent(),
            params={"fr_id": "spec:spec-other:functional_requirement:0"},
        )

    assert exc.value.code == "selector_reference_forbidden"
    assert exc.value.status_code == 403
    error_text = str(exc.value)
    assert "spec-other" not in error_text
    assert "Secret Other Board Spec" not in error_text
    assert "Secret FR label" not in error_text
    assert "spec:spec-other:functional_requirement:0" not in error_text


@pytest.mark.asyncio
async def test_selector_denial_metric_omits_raw_payload(caplog, monkeypatch):
    async def load_other_board_spec(_db: Any, _spec_id: str) -> SimpleNamespace:
        return _spec(
            id="spec-secret",
            board_id="board-secret",
            title="Secret Other Board Spec",
            functional_requirements=["Secret FR label"],
        )

    monkeypatch.setattr(discovery_executor, "_load_spec_by_id", load_other_board_spec)
    caplog.set_level(
        logging.INFO,
        logger="okto_pulse.core.services.discovery_executor",
    )

    with pytest.raises(discovery_executor.DiscoverySelectorExecutionError):
        await discovery_executor.execute_intent(
            db=None,
            user_id="user-1",
            board_id="board-1",
            intent=_selector_intent(),
            params={
                "fr_id": {
                    "child_ref": "spec:spec-secret:functional_requirement:0",
                    "spec_id": "spec-secret",
                    "child_type": "functional_requirement",
                    "child_id": "0",
                    "label": "Secret FR label",
                    "description": "full payload must not leak",
                }
            },
        )

    metric_records = [
        record
        for record in caplog.records
        if getattr(record, "metric_name", None)
        == "discovery_selector_access_denied_total"
    ]
    assert metric_records, "denial path must emit a safe selector metric"
    for record in metric_records:
        assert record.board_id == "board-1"
        assert record.param_name == "fr_id"
        assert record.child_type == "functional_requirement"
        assert record.outcome == "forbidden"
        rendered = repr(record.__dict__).casefold()
        assert "spec-secret" not in rendered
        assert "spec:spec-secret:functional_requirement:0" not in rendered
        assert "secret fr label" not in rendered
        assert "full payload" not in rendered
        assert "description" not in rendered


@pytest.mark.asyncio
async def test_valid_selector_executes_with_exact_fr_coverage(monkeypatch):
    spec = _spec(
        test_scenarios=[
            {
                "id": "ts-fr10",
                "title": "FR10 scenario",
                "linked_criteria": ["FR10 — not selected"],
                "linked_task_ids": ["card-10"],
            },
            {
                "id": "ts-fr1",
                "title": "FR1 scenario",
                "linked_criteria": ["FR1 — selected"],
                "linked_task_ids": ["card-1"],
            },
        ],
    )

    async def load_spec(_db: Any, _spec_id: str) -> SimpleNamespace:
        return spec

    async def can_read(_db: Any, _user_id: str, _spec: Any) -> bool:
        return True

    monkeypatch.setattr(discovery_executor, "_load_spec_by_id", load_spec)
    monkeypatch.setattr(discovery_executor, "_can_read_selector_spec", can_read)
    db = _SequenceSession([_ScalarResult(values=[spec])])

    out = await discovery_executor.execute_intent(
        db=db,
        user_id="user-1",
        board_id="board-1",
        intent=_selector_intent(),
        params={
            "fr_id": {
                "child_ref": "spec:spec-1:functional_requirement:0",
                "spec_id": "spec-1",
                "child_type": "functional_requirement",
                "child_id": "0",
            }
        },
    )

    assert out["total"] == 1
    assert out["rows"][0]["id"] == "ts-fr1"
    meta = out["rows"][0]["meta"]
    assert meta["entity_type"] == "spec"
    assert meta["entity_id"] == "spec-1"
    assert meta["spec_id"] == "spec-1"
    assert meta["selected_child_ref"] == "spec:spec-1:functional_requirement:0"
    assert meta["child_ref"] == "spec:spec-1:functional_requirement:0"
    assert meta["child_type"] == "functional_requirement"
    assert meta["child_id"] == "0"
    assert meta["child_index"] == 0


@pytest.mark.asyncio
async def test_card_entity_selector_executes_dependency_lookup(monkeypatch):
    async def load_card(_db: Any, _card_id: str) -> SimpleNamespace:
        return _card()

    monkeypatch.setattr(discovery_executor, "_load_card_by_id", load_card)
    db = _SequenceSession([_ScalarResult(values=[])])

    out = await discovery_executor.execute_intent(
        db=db,
        user_id="user-1",
        board_id="board-1",
        intent=_card_selector_intent(),
        params={
            "card_id": {
                "entity_type": "card",
                "card_id": "card-1",
                "entity_id": "card-1",
                "id": "card-1",
            }
        },
    )

    assert out["tool_binding"] == "okto_pulse_get_card_dependencies"
    assert out["params_echo"] == {"card_id": "card-1"}
    assert out["rows"] == []


@pytest.mark.asyncio
async def test_uncovered_requirements_include_first_class_structured_children():
    spec = _spec(
        functional_requirements=[],
        acceptance_criteria=[],
        technical_requirements=[],
        business_rules=[
            {"id": "br-1", "title": "Unlinked BR", "linked_task_ids": []},
            {"id": "br-2", "title": "Linked BR", "linked_task_ids": ["card-1"]},
        ],
        api_contracts=[
            {"id": "api-1", "method": "GET", "path": "/v1/items", "linked_task_ids": []}
        ],
        integration_requirements=[
            {"id": "ir-1", "title": "Unlinked IR", "linked_task_ids": []}
        ],
        observability_requirements=[
            {"id": "or-1", "title": "Unlinked OR", "linked_task_ids": []}
        ],
        decisions=[
            {"id": "dec-1", "title": "Unlinked decision", "linked_task_ids": []},
            {"id": "dec-2", "title": "Superseded decision", "status": "superseded"},
        ],
    )
    board = SimpleNamespace(settings={})
    card = SimpleNamespace(id="card-1", spec_id="spec-1", status=SimpleNamespace(value="done"))
    db = _SequenceSession(
        [
            _ScalarResult(value=board),
            _ScalarResult(values=[spec]),
            _ScalarResult(values=[card]),
        ]
    )

    out = await discovery_executor._exec_uncovered_requirements(db, "board-1")

    by_type = {row["type"]: row for row in out["rows"]}
    assert set(by_type) == {
        "UncoveredBR",
        "UncoveredAPIContract",
        "UncoveredIR",
        "UncoveredOR",
        "UncoveredDecision",
    }
    assert by_type["UncoveredBR"]["meta"]["child_ref"] == (
        "spec:spec-1:business_rule:br-1"
    )
    assert by_type["UncoveredAPIContract"]["meta"]["child_type"] == "api_contract"
    assert by_type["UncoveredDecision"]["meta"]["child_id"] == "dec-1"
