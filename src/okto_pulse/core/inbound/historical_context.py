"""Lazy historical follow-up; no source discovery or authority grant."""

from typing import Literal

def historical_context_follow_up(board_id: str, target_kind: Literal["spec", "card"], target_id: str) -> dict:
    # Presence of this pointer says nothing about existence or visibility of any
    # source. Only the separate read checks current target and archived authority.
    return {"availability": "not_queried", "tool": "okto_pulse_get_historical_context",
        "arguments": {"board_id": board_id, "target_kind": target_kind, "target_id": target_id,
            "offset": 0, "limit": 50},
        "pagination": "read_until_next_offset_null", "historical_only": True}
