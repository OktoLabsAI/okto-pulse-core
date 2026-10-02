from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.delivery_progress import (
    DeliveryProgress, progress_blocks_execution,
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


@pytest.mark.parametrize("version", ["delivery-progress/v1", "delivery-progress/v2"])
def test_missing_declaration_is_rejected_without_inference(version):
    data = progress().model_dump()
    data["contract_version"] = version
    data.pop("material_change")
    with pytest.raises(ValidationError):
        DeliveryProgress.model_validate(data)


def test_current_checkpoint_round_trip_has_canonical_defaults():
    value = progress()
    assert value.model_dump()["impact_base_revision"] is None
    assert DeliveryProgress.model_validate_json(value.model_dump_json()) == value
