"""Public policy contract for archived work retirement, never simulated delivery.

Superseded is terminal and cannot be requeued. Its provenance is verified by
the edition's fenced migration journal; classification alone is not that proof.
"""

from okto_pulse.core.domain.sprint_retirement_events import (
    SUPERSEDED_WORK_STATUS,
    SprintEventDisposition,
    classify_historical_sprint_event,
    classify_historical_sprint_execution,
    classify_historical_sprint_queue,
)

WORK_RETIRED_ORIGIN_EVENT = "migration.work_origin_retired"
WORK_RETIREMENT_FORMAT = "archived-work-retirement/v1"

__all__ = ["SUPERSEDED_WORK_STATUS", "WORK_RETIRED_ORIGIN_EVENT", "WORK_RETIREMENT_FORMAT", "SprintEventDisposition", "classify_historical_sprint_event",
    "classify_historical_sprint_execution", "classify_historical_sprint_queue"]
