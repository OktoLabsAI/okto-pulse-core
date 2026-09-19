from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.delivery_progress import (
    DeliveryProgress, progress_blocks_execution, progress_change_scope,
)

NOW = datetime(2026, 9, 19, tzinfo=timezone.utc)


def progress(**changes):
    return DeliveryProgress.model_validate({
        "contract_version": "delivery-progress/v2", "material_change": "targets",
        "target_ids": ["t1"], "remaining": "Retest changed code",
        "source_state": dict(workspace_state="dirty", recoverability="external_workspace"),
        **changes,
    })


def blocked(value, *, target="t1", source="source", observed=NOW):
    return progress_blocks_execution(value, target_id=target, source_ref=source,
        checkpoint_received_at=NOW, execution_observed_at=observed)


def test_material_change_is_scoped_and_requires_strictly_later_observation():
    value = progress()
    assert blocked(value)
    assert blocked(value, observed=NOW - timedelta(days=1))
    assert blocked(value, observed=None)
    assert not blocked(value, target="t2")
    assert not blocked(value, observed=NOW + timedelta(microseconds=1))
    assert blocked(value, observed=NOW.replace(tzinfo=None))


def test_source_and_unknown_scope_never_fabricate_coverage():
    value = progress(material_change="source", target_ids=[], source_state=dict(
        source_ref="source", workspace_state="clean", recoverability="unknown"))
    assert blocked(value, target="any")
    assert not blocked(value, source="other")
    unknown = progress(material_change="unknown", target_ids=[])
    assert blocked(unknown, target="any", source="other")
    assert not blocked(progress(material_change="unknown"), target="other")


def test_context_note_does_not_invalidate_earlier_proof_even_with_external_dirty_state():
    assert not blocked(progress(material_change="none"))


@pytest.mark.parametrize("changes", [
    {"material_change": None},
    {"material_change": "current"},
    {"material_change": "targets", "target_ids": []},
    {"material_change": "source"},
    {"contract_version": "delivery-progress/v1"},
    {"material_change": "none", "impact_delta": {"files": [dict(repo="core", path="a.py", change_kind="modified")]}},
])
def test_closed_change_contract_rejects_missing_scope_or_contradiction(changes):
    with pytest.raises(ValidationError):
        progress(**changes)


def test_legacy_dirty_is_uncertain_scoped_work_without_rewriting_its_payload():
    data = progress().model_dump()
    data["contract_version"] = "delivery-progress/v1"
    data.pop("material_change")
    value = DeliveryProgress.model_validate(data)
    assert value.model_dump() == data
    assert progress_change_scope(value) == "targets"
    assert blocked(value) and not blocked(value, target="other")
    data["source_state"]["workspace_state"] = "unknown"
    assert progress_change_scope(DeliveryProgress.model_validate(data)) == "none"
    data["impact_delta"] = {"files": [dict(repo="core", path="a.py", change_kind="modified")]}
    assert blocked(DeliveryProgress.model_validate(data))
