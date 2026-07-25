from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from okto_pulse.core.domain.datetime_utils import (
    isoformat_utc,
    normalize_utc_datetime,
)
from okto_pulse.core.mcp.cancellation_projection import project_cancellation


def test_legacy_naive_cancellation_timestamp_projects_as_explicit_utc():
    entity = SimpleNamespace(
        cancellation_reason="No longer needed",
        cancelled_by="actor-1",
        cancelled_at=datetime(2026, 7, 25, 10, 30, 0),
    )

    assert project_cancellation(entity)["cancelled_at"] == (
        "2026-07-25T10:30:00+00:00"
    )


def test_offset_cancellation_timestamp_is_normalized_to_utc():
    local = datetime(
        2026,
        7,
        25,
        10,
        30,
        0,
        tzinfo=timezone(timedelta(hours=-3)),
    )

    assert isoformat_utc(local) == "2026-07-25T13:30:00+00:00"
    assert normalize_utc_datetime(local).tzinfo == timezone.utc
