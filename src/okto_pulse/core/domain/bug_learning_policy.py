"""KG §7.4: durable Learning admission, independent of legacy graph holds."""

from collections.abc import Mapping
from typing import Any, Literal


BugLearningCloseoutMode = Literal["advisory", "blocking"]
LEARNING_CAPTURE_REQUIRED = "bug_learning_capture_required"
LEARNING_CAPTURE_PRECONDITION = "valid_durable_learning_capture"


def bug_learning_closeout_mode(settings: Mapping[str, Any] | None) -> BugLearningCloseoutMode:
    """Omission is advisory; corrupt persisted policy never becomes a waiver."""
    value = (settings or {}).get("bug_learning_closeout", "advisory")
    if value not in ("advisory", "blocking"):
        raise ValueError("bug_learning_closeout_policy_invalid")
    return value


def requires_bug_learning_capture(settings: Mapping[str, Any] | None, card_type: Any) -> bool:
    if getattr(card_type, "value", card_type) != "bug":
        return False
    return bug_learning_closeout_mode(settings) == "blocking"
