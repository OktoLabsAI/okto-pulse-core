"""Native deterministic population for analytics aggregation and scoped reads."""

from datetime import datetime, timedelta, timezone

from okto_pulse.core.domain.enums import (
    CardStatus,
    CardType,
    IdeationStatus,
    RefinementStatus,
    SpecStatus,
    StoryStatus,
)
from okto_pulse.core.ports.analytics_read import AnalyticsFact
from spec_validation_fixtures import native_validation
from task_validation_native_fixtures import native_entry

NOW = datetime(2026, 9, 21, 12, tzinfo=timezone.utc)


class FixedClock(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW if tz else NOW.replace(tzinfo=None)


def population():
    def fact(id, **values):
        return AnalyticsFact(
            {
                "id": id,
                "board_id": "board-a",
                "title": id,
                "archived": False,
                "created_at": NOW - timedelta(days=3),
                "updated_at": NOW,
                "validations": [],
                "evaluations": [],
                "conclusions": [],
                "business_rules": [],
                "api_contracts": [],
                "test_scenarios": [],
                "functional_requirements": [],
                "technical_requirements": [],
                "acceptance_criteria": [],
                "spec_id": "spec-a",
                "ideation_id": None,
                "refinement_id": None,
                "topic_id": None,
                **values,
            }
        )

    def task_validation(card_id, *, success):
        return {
            **native_entry(),
            "id": f"{card_id}-{'success' if success else 'failed'}",
            "card_id": card_id, "board_id": "board-a",
            "outcome": "success" if success else "failed",
            "validation_outcome": "success" if success else "failed",
            "completion_outcome": "completed" if success else "rejected",
            "card_status": "done" if success else "rejected",
            "confidence": 95 if success else 61,
            "estimated_completeness": 96 if success else 70,
            "estimated_drift": 2 if success else 14,
            "recommendation": "approve" if success else "reject",
            "created_at": NOW.isoformat(),
            "threshold_violations": [] if success else ["confidence below minimum"],
        }

    spec_failure = native_validation(
        "spec-failed", spec_id="spec-a", board_id="board-a",
        confidence=61, clarity=72, decidability=91, assertiveness=60, ambiguity=30,
        outcome="failed", recommendation="reject", created_at=NOW.isoformat(),
        threshold_violations=["confidence below minimum"],
    )
    spec_success = native_validation(
        "spec-success", spec_id="spec-a", board_id="board-a",
        confidence=95, clarity=94, decidability=95, assertiveness=60, ambiguity=30,
        created_at=NOW.isoformat(),
    )
    spec = fact(
        "spec-a",
        status=SpecStatus.DONE,
        validations=[spec_failure, spec_success],
        evaluations=[
            {"recommendation": "request_changes", "overall_score": 65, "spec_edition": 1},
            {"recommendation": "approve", "overall_score": 95, "spec_edition": 1},
        ],
        business_rules=[{"id": "br-1", "text": "Keep approval"}],
    )
    return {
        "board": [
            fact("board-a", owner_id="owner", name="Visible"),
            fact("board-b", owner_id="other", name="Foreign"),
        ],
        "story": [fact("story-a", status=StoryStatus.CONVERTED)],
        "ideation": [fact("idea-a", status=IdeationStatus.DONE)],
        "refinement": [fact("ref-a", status=RefinementStatus.DONE)],
        "spec": [
            spec,
            fact("spec-foreign", board_id="board-b", status=SpecStatus.DRAFT),
        ],
        "card": [
            fact(
                "card-normal",
                status=CardStatus.DONE,
                card_type=CardType.NORMAL,
                validations=[task_validation("card-normal", success=False),
                             task_validation("card-normal", success=True)],
                conclusions=[{"completeness": 88, "drift": 8}],
            ),
            fact(
                "card-test",
                status=CardStatus.DONE,
                card_type=CardType.TEST,
                created_at=NOW - timedelta(days=1),
                validations=[task_validation("card-test", success=True)],
            ),
            fact(
                "card-bug",
                status=CardStatus.IN_PROGRESS,
                card_type=CardType.BUG,
                severity="critical",
                linked_test_task_ids=["card-test"],
                validations=[task_validation("card-bug", success=False)],
            ),
            fact(
                "card-archived",
                status=CardStatus.DONE,
                card_type=CardType.NORMAL,
                archived=True,
            ),
            fact(
                "card-foreign",
                board_id="board-b",
                spec_id="spec-foreign",
                status=CardStatus.DONE,
                card_type=CardType.BUG,
            ),
        ],
        "activity_log": [
            fact(
                "event-spec",
                action="spec_moved",
                details={"new_status": "done"},
                created_at=NOW,
            ),
        ],
        "topic": [],
        "story_ideation_link": [],
    }


class PopulationReader:
    def __init__(self):
        self.rows = population()
        self.queries = []

    async def list(self, context, query):
        self.queries.append(query)
        assert query.entity != "sprint", "native aggregate queried retired Sprint"
        assert all(f.value != "sprint_moved" for f in query.filters)
        rows = self.rows[query.entity]
        for clause in query.filters:

            def matches(row):
                actual = getattr(row, clause.field)
                expected = clause.value
                match clause.operator:
                    case "eq":
                        return actual == expected
                    case "in":
                        return actual in expected
                    case "is_false":
                        return actual is False
                    case "gte":
                        return actual >= expected
                    case "lt":
                        return actual < expected
                    case _:
                        raise AssertionError(clause)

            rows = [row for row in rows if matches(row)]
        return tuple(rows[query.offset :][: query.limit])

    async def count(self, context, query):
        return len(await self.list(context, query))


CASES = [
    (reader, window)
    for reader in (
        "funnel",
        "overview",
        "validations",
        "mcp",
        "velocity_day",
        "velocity_week",
    )
    for window in ("all", "recent", "empty")
]


async def evaluate_case(service, reader, window):
    dates = (
        {}
        if window == "all"
        else {
            "dt_from": NOW - timedelta(days=2)
            if window == "recent"
            else NOW + timedelta(days=1),
            "dt_to": NOW + timedelta(days=2),
        }
    )
    if reader == "mcp":
        return await service.compute_mcp_board_analytics(None, "board-a", **dates)
    if reader == "overview":
        return await service.compute_overview(None, "owner", **dates)
    if reader.startswith("velocity_"):
        return await service.compute_velocity(
            None, "board-a", granularity=reader.split("_")[1], **dates
        )
    return await getattr(service, "compute_" + reader)(None, "board-a", **dates)


def assert_no_retired_fields(value):
    """Removed metrics may not reappear even as synthetic zero counts."""
    retired = {"sprints", "sprint", "total_sprints", "sprint_count",
               "sprint_status_breakdown", "sprint_evaluation", "sprint_done", "sprint_id"}
    if isinstance(value, dict):
        assert not retired.intersection(value)
        for item in value.values():
            assert_no_retired_fields(item)
    elif isinstance(value, list):
        for item in value:
            assert_no_retired_fields(item)
