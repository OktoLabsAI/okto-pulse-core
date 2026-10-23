"""Legacy normalization reaches the regular retryable projection dispatcher."""

from okto_pulse.core.events.handlers.consolidation_enqueuer import (
    ConsolidationEnqueuer,
)
from okto_pulse.core.events.registry import registered_handlers
from okto_pulse.core.events.types import (
    CARD_STATUS_MIGRATION_ID,
    CardStatusNormalized,
    resolve_event_class,
)


def test_normalization_event_projects_card_then_parent_spec() -> None:
    event = CardStatusNormalized(
        board_id="board", card_id="card", spec_id="spec",
        old_position=1, new_position=8,
        old_policy_version=4, new_policy_version=5, archived=False,
    )
    assert resolve_event_class(event.event_type) is CardStatusNormalized
    assert event.migration_id == CARD_STATUS_MIGRATION_ID
    assert ConsolidationEnqueuer in registered_handlers(event.event_type)
    assert ConsolidationEnqueuer()._map_targets(event) == [
        ("card", "card"), ("spec", "spec")
    ]


def test_orphan_normalization_has_only_card_target() -> None:
    event = CardStatusNormalized(
        board_id="board", card_id="orphan", spec_id=None,
        old_position=1, new_position=2,
        old_policy_version=1, new_policy_version=2, archived=True,
    )
    assert ConsolidationEnqueuer()._map_targets(event) == [("card", "orphan")]
