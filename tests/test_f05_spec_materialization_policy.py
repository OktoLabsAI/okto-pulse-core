from __future__ import annotations

from types import SimpleNamespace

from okto_pulse.core.domain.spec_materialization import (
    plan_legacy_fr_ac_materialization,
)


def _spec(frs, acs):  # noqa: ANN001, ANN201
    return SimpleNamespace(functional_requirements=frs, acceptance_criteria=acs)


def test_f05_materialization_plan_is_atomic_and_idempotent() -> None:
    legacy = _spec(["FR one"], ["AC one"])
    canonical = _spec(
        [{"id": "fr_existing", "text": "FR", "status": "active"}],
        [{"id": "ac_existing", "text": "AC", "status": "active"}],
    )

    first = plan_legacy_fr_ac_materialization([legacy, canonical])
    assert first.scanned == 2
    assert first.changed == 1
    assert first.skipped == 1
    assert first.errors == 0
    assert {field for field, _value in first.changes[0].fields} == {
        "functional_requirements",
        "acceptance_criteria",
    }

    for field, value in first.changes[0].fields:
        setattr(legacy, field, value)
    second = plan_legacy_fr_ac_materialization([legacy, canonical])
    assert second.changed == 0
    assert second.skipped == 2


def test_f05_materialization_plan_does_not_mutate_corrupt_spec() -> None:
    duplicate = [
        {"id": "fr_same", "text": "one", "status": "active"},
        {"id": "fr_same", "text": "two", "status": "active"},
    ]
    corrupt = _spec(duplicate, ["legacy AC"])

    plan = plan_legacy_fr_ac_materialization([corrupt])

    assert plan.errors == 1
    assert plan.changed == 0
    assert corrupt.functional_requirements is duplicate
    assert corrupt.acceptance_criteria == ["legacy AC"]
