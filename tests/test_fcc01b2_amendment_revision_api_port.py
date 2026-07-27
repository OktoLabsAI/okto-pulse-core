from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from okto_pulse.core.domain.amendment_eligibility import (
    AmendmentLineageState,
    AmendmentRevisionStatus,
)
from okto_pulse.core.domain.enums import CardType, SpecStatus
from okto_pulse.core.services.amendment_revision_api import AmendmentRevisionApiService


class FakeAmendmentRevisionApiBackend:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.bug = SimpleNamespace(
            id="bug-1",
            board_id="board-1",
            card_type=CardType.BUG,
            spec_id="spec-1",
            origin_task_id="task-1",
        )
        self.spec = SimpleNamespace(
            id="spec-1",
            board_id="board-1",
            status=SpecStatus.DONE,
            current_validation_id=None,
        )
        self.amendment = None
        self.created_kwargs = None

    async def get_bug(self, board_id: str, bug_id: str):
        self.calls.append("get_bug")
        return self.bug if board_id == self.bug.board_id and bug_id == self.bug.id else None

    async def get_spec(self, board_id: str, spec_id: str):
        self.calls.append("get_spec")
        return self.spec if board_id == self.spec.board_id and spec_id == self.spec.id else None

    async def create_amendment(self, **kwargs):
        self.calls.append("create_amendment")
        self.created_kwargs = kwargs
        now = datetime.now(timezone.utc)
        self.amendment = SimpleNamespace(
            id="amd-1",
            board_id=kwargs["board_id"],
            original_spec_id=kwargs["original_spec_id"],
            origin_bug_id=kwargs["origin_bug_id"],
            revision_spec_id=kwargs.get("revision_spec_id"),
            status=AmendmentRevisionStatus.DRAFT,
            lineage_state=AmendmentLineageState.INCOMPLETE,
            origin_task_ids=list(kwargs.get("origin_task_ids") or []),
            affected_task_ids=list(kwargs.get("affected_task_ids") or []),
            regression_scenario_ids=list(kwargs.get("regression_scenario_ids") or []),
            regression_test_task_ids=list(kwargs.get("regression_test_task_ids") or []),
            automated_regression_refs=list(kwargs.get("automated_regression_refs") or []),
            created_at=now,
            updated_at=now,
        )
        return self.amendment

    async def get_amendment(self, amendment_id: str):
        self.calls.append("get_amendment")
        return self.amendment if self.amendment and amendment_id == self.amendment.id else None

    async def list_amendments_for_bug(self, **kwargs):
        self.calls.append("list_amendments_for_bug")
        return [self.amendment] if self.amendment else []

    async def associate_artifacts(self, amendment_id: str, **kwargs):
        self.calls.append("associate_artifacts")
        return self.amendment

    async def set_lineage_state(self, amendment_id: str, lineage_state, actor: str):
        self.calls.append("set_lineage_state")
        self.amendment.lineage_state = lineage_state
        return self.amendment

    async def set_status(self, amendment_id: str, new_status, actor: str):
        self.calls.append("set_status")
        self.amendment.status = new_status
        return self.amendment

    async def path_b_resolution(self, **kwargs):
        self.calls.append("path_b_resolution")
        return {
            "available": True,
            "coverage_state": "not_applicable",
            "missing_links": [],
            "safe_next_actions": [],
        }

    def eligibility(self, amendment):
        self.calls.append("eligibility")
        return SimpleNamespace(
            lineage_eligible=False,
            canonicalization_candidate=False,
            blocked=True,
            reason_code="blocking_status",
        )


def test_amendment_revision_api_service_has_no_direct_relational_imports():
    source = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "services"
        / "amendment_revision_api.py"
    ).read_text(encoding="utf-8")

    for forbidden in (
        "sqlalchemy.ext.asyncio",
        "AsyncSession",
        "okto_pulse.core.models.db",
    ):
        assert forbidden not in source


@pytest.mark.asyncio
async def test_create_runs_with_fake_backend_without_relational_session():
    backend = FakeAmendmentRevisionApiBackend()
    service = AmendmentRevisionApiService(backend)

    result = await service.create(
        board_id="board-1",
        bug_id="bug-1",
        author="agent-1",
        origin_task_ids=["task-1"],
        regression_scenario_ids=["ts-1"],
    )

    assert result["id"] == "amd-1"
    assert result["status"] == "draft"
    assert result["origin_bug_id"] == "bug-1"
    assert result["original_spec_id"] == "spec-1"
    assert result["regression_scenario_ids"] == ["ts-1"]
    assert result["eligibility"]["blocked"] is True
    assert backend.created_kwargs is not None
    assert "validation_metadata" not in backend.created_kwargs
    assert backend.calls == [
        "get_bug",
        "get_spec",
        "create_amendment",
        "eligibility",
    ]
