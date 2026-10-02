"""The native Checklist application boundary has one command and read contract."""

from types import SimpleNamespace
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases.checklist import (
    StartChecklistExecutionCommand,
    SubmitChecklistExecutionCommand,
    _preflight,
)
from okto_pulse.core.domain.checklist import (
    ChecklistBinding,
    ChecklistMode,
    ChecklistSpecSnapshot,
)


@pytest.mark.parametrize("field", ["expected_spec_edition", "idempotency_key", "binding_digest"])
def test_start_rejects_removed_command_fields(field):
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        StartChecklistExecutionCommand(
            board_id="board", spec_id="spec", expected_spec_version=3,
            spec_edition=2, binding_version=1, **{field: "old-input"},
        )


@pytest.mark.parametrize("field", ["expected_execution_revision", "items", "idempotency_key"])
def test_submit_rejects_removed_command_fields(field):
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        SubmitChecklistExecutionCommand(
            board_id="board", spec_id="spec", execution_id="execution",
            expected_spec_version=3, spec_edition=2, item_results=(),
            **{field: "old-input"},
        )


@pytest.mark.parametrize("field", ["spec_edition", "binding_version"])
@pytest.mark.parametrize("value", [None, 0, -1, True, "1"])
def test_start_requires_positive_native_fences(field, value):
    fields = dict(board_id="board", spec_id="spec", expected_spec_version=3,
                  spec_edition=2, binding_version=1)
    fields[field] = value
    with pytest.raises(ValueError, match="checklist_.*_invalid"):
        StartChecklistExecutionCommand(**fields)


def subject(edition):
    return ChecklistSpecSnapshot(
        board_id="board", spec_id="spec", spec_version=3,
        spec_edition=edition, content_digest="a" * 64, input_digest="b" * 64,
        status="approved",
    )


@pytest.mark.asyncio
async def test_editionless_port_subject_cannot_fall_back_to_live_binding():
    persistence = SimpleNamespace(
        get_spec_snapshot=AsyncMock(return_value=subject(None)),
        get_binding=AsyncMock(), get_validation_binding=AsyncMock(),
        get_current=AsyncMock(),
    )
    with pytest.raises(RuntimeError, match="checklist_subject_edition_required"):
        await _preflight(SimpleNamespace(services=SimpleNamespace(checklists=persistence)),
                         board_id="board", spec_id="spec")
    persistence.get_binding.assert_not_called()
    persistence.get_validation_binding.assert_not_called()
    persistence.get_current.assert_not_called()


@pytest.mark.asyncio
async def test_current_read_type_error_is_not_retried_without_edition():
    persistence = SimpleNamespace(
        get_spec_snapshot=AsyncMock(return_value=subject(2)),
        get_validation_binding=AsyncMock(return_value=ChecklistBinding(
            board_id="board", mode=ChecklistMode.BLOCKING, version=1,
        )),
        get_current=AsyncMock(side_effect=TypeError("adapter defect")),
    )
    with pytest.raises(TypeError, match="adapter defect"):
        await _preflight(SimpleNamespace(services=SimpleNamespace(checklists=persistence)),
                         board_id="board", spec_id="spec")
    persistence.get_current.assert_awaited_once()
    assert persistence.get_current.await_args.kwargs["spec_edition"] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["approved", "validated", "in_progress", "done"])
async def test_missing_validation_snapshot_cannot_use_live_governance(status):
    persistence = SimpleNamespace(
        get_spec_snapshot=AsyncMock(return_value=replace(subject(2), status=status)),
        get_validation_binding=AsyncMock(return_value=None),
        get_binding=AsyncMock(), get_current=AsyncMock(),
    )
    with pytest.raises(RuntimeError, match="snapshot_missing"):
        await _preflight(SimpleNamespace(services=SimpleNamespace(checklists=persistence)),
                         board_id="board", spec_id="spec")
    persistence.get_binding.assert_not_called()
    persistence.get_current.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["draft", "review", "cancelled"])
async def test_prevalidation_preview_reads_board_without_creating_snapshot(status):
    binding = ChecklistBinding(board_id="board", mode=ChecklistMode.BLOCKING, version=1)
    persistence = SimpleNamespace(
        get_spec_snapshot=AsyncMock(return_value=replace(subject(2), status=status)),
        get_validation_binding=AsyncMock(return_value=None),
        get_binding=AsyncMock(return_value=binding),
        get_current=AsyncMock(return_value=None),
    )
    result = await _preflight(SimpleNamespace(services=SimpleNamespace(checklists=persistence)),
                              board_id="board", spec_id="spec")
    assert result.binding == binding
    persistence.get_binding.assert_awaited_once()
