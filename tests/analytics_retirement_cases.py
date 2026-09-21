"""Deterministic mixed population for the F5 aggregate retirement baseline.

The baseline was captured from the installed pre-retirement pair, not recomputed
from the new implementation. This reader exercises the public analytics port;
SQL and transport behavior remain covered by the relational integration tests.
"""

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
                "sprint_id": "old-a",
                "ideation_id": None,
                "refinement_id": None,
                "topic_id": None,
                **values,
            }
        )

    failure = {
        "outcome": "failed",
        "confidence": 61,
        "completeness": 70,
        "assertiveness": 60,
        "ambiguity": 30,
        "drift": 14,
        "recommendation": "reject",
        "created_at": NOW.isoformat(),
        "threshold_violations": ["confidence below minimum"],
    }
    success = {
        **failure,
        "outcome": "success",
        "confidence": 95,
        "completeness": 96,
        "drift": 2,
        "recommendation": "approve",
        "threshold_violations": [],
    }
    spec = fact(
        "spec-a",
        status=SpecStatus.DONE,
        validations=[failure, success],
        evaluations=[
            {"recommendation": "request_changes", "overall_score": 65},
            {"recommendation": "approve", "overall_score": 95},
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
                validations=[failure, success],
                conclusions=[{"completeness": 88, "drift": 8}],
            ),
            fact(
                "card-test",
                status=CardStatus.DONE,
                card_type=CardType.TEST,
                created_at=NOW - timedelta(days=1),
                validations=[success],
            ),
            fact(
                "card-bug",
                status=CardStatus.IN_PROGRESS,
                card_type=CardType.BUG,
                severity="critical",
                linked_test_task_ids=["card-test"],
                validations=[failure],
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
        "sprint": [fact("old-a", status="closed", evaluations=[{"overall_score": 30}])],
        "activity_log": [
            fact(
                "event-spec",
                action="spec_moved",
                details={"new_status": "done"},
                created_at=NOW,
            ),
            fact(
                "event-old",
                action="sprint_moved",
                details={"new_status": "closed"},
                created_at=NOW,
            ),
        ],
        "topic": [],
        "story_ideation_link": [],
    }


class PopulationReader:
    def __init__(self, *, forbid_retired=False):
        self.rows = population()
        self.forbid_retired = forbid_retired
        self.queries = []

    async def list(self, context, query):
        self.queries.append(query)
        if self.forbid_retired:
            assert query.entity != "sprint", "live aggregate queried retired Sprint"
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


def surviving_fields(value):
    """The authorized delta is an explicit set of retired metric keys only."""
    retired = {
        "sprints",
        "sprint",
        "total_sprints",
        "sprint_count",
        "sprint_status_breakdown",
        "sprint_evaluation",
        "sprint_done",
        "sprint_id",
    }
    if isinstance(value, dict):
        return {
            key: surviving_fields(item)
            for key, item in value.items()
            if key not in retired
        }
    if isinstance(value, list):
        return [surviving_fields(item) for item in value]
    return value
