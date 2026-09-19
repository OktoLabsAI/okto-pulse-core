"""BASE F2B: characterize policy information lost by removing the Sprint input.

This exercises the current pure resolver, not a proposed migration or a claim
that a lifecycle transition is authorized. Replace the loss assertions with
migration equivalence once a faithful per-card representation is authorized.
"""

from types import SimpleNamespace

from okto_pulse.core.services import CardService


def _resolve(*, card, spec, sprint, board):
    # The current resolver is pure; self owns no part of the calculation.
    return CardService._resolve_validation_config(
        None, card, spec, sprint, board
    )


def test_distinct_sprint_policies_cannot_collapse_to_one_spec_policy():
    spec = SimpleNamespace(id="same-spec", require_task_validation=True)
    cards = [SimpleNamespace(id=f"card-{n}", spec_id=spec.id) for n in (1, 2)]
    sprints = [
        SimpleNamespace(validation_min_confidence=90),
        SimpleNamespace(validation_min_confidence=60),
    ]
    board = {"min_confidence": 70, "min_completeness": 80, "max_drift": 50}
    before = [
        _resolve(card=card, spec=spec, sprint=sprint, board=board)
        for card, sprint in zip(cards, sprints, strict=True)
    ]
    without_sprint = [
        _resolve(card=card, spec=spec, sprint=None, board=board)
        for card in cards
    ]

    assert [value["min_confidence"] for value in before] == [90, 60]
    assert [value["min_confidence"] for value in without_sprint] == [70, 70]
    assert all(value["resolved_sources"]["min_confidence"] == "sprint" for value in before)
    assert all(value["resolved_sources"]["min_completeness"] == "board" for value in before)
    assert all(value["resolved_sources"]["required"] == "spec" for value in before)


def test_false_and_zero_are_overrides_but_null_keeps_independent_inheritance():
    card = SimpleNamespace(id="card", spec_id="spec")
    spec = SimpleNamespace(require_task_validation=True, validation_min_completeness=85)
    sprint = SimpleNamespace(
        require_task_validation=False,
        validation_min_confidence=0,
        validation_min_completeness=None,
        validation_max_drift=0,
    )
    result = _resolve(card=card, spec=spec, sprint=sprint, board={})

    assert result == {
        "required": False,
        "min_confidence": 0,
        "min_completeness": 85,
        "max_drift": 0,
        "resolved_from": "sprint",
        "resolved_sources": {
            "required": "sprint",
            "min_confidence": "sprint",
            "min_completeness": "spec",
            "max_drift": "sprint",
        },
    }
