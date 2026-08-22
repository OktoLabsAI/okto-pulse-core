"""Immutable authority for Sprint commitment at activation time."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Protocol, runtime_checkable

from okto_pulse.core.ports.analytics_foundation import require_utc_datetime
from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)


SPRINT_ACTIVATION_BASELINE_CONTRACT_VERSION = "1"
MAX_SPRINT_ACTIVATION_MEMBERS = 10_000
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_RUNTIME_KEY = "ports.sprint_activation_baseline.store"


def _text(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"sprint_activation_baseline_{field}_required")
    return value.strip()


def _utc_text(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


class SprintCommitmentState(str, Enum):
    AVAILABLE = "available"
    UNAVAILABLE_LEGACY = "unavailable_legacy"


@dataclass(frozen=True, slots=True, order=True)
class SprintActivationMember:
    card_id: str
    card_type: str
    card_version: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "card_id", _text(self.card_id, field="card_id"))
        object.__setattr__(
            self, "card_type", _text(self.card_type, field="card_type").lower()
        )
        if (
            isinstance(self.card_version, bool)
            or not isinstance(self.card_version, int)
            or self.card_version < 1
        ):
            raise ValueError("sprint_activation_baseline_card_version_invalid")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "card_id": self.card_id,
            "card_type": self.card_type,
            "card_version": self.card_version,
        }


@dataclass(frozen=True, slots=True)
class SprintActivationBaseline:
    board_id: str
    sprint_id: str
    spec_id: str
    sprint_version: int
    activated_at: datetime
    activated_by: str
    members: tuple[SprintActivationMember, ...]
    baseline_ref: str | None = None

    def __post_init__(self) -> None:
        for field in ("board_id", "sprint_id", "spec_id", "activated_by"):
            object.__setattr__(self, field, _text(getattr(self, field), field=field))
        if (
            isinstance(self.sprint_version, bool)
            or not isinstance(self.sprint_version, int)
            or self.sprint_version < 1
        ):
            raise ValueError("sprint_activation_baseline_sprint_version_invalid")
        object.__setattr__(
            self,
            "activated_at",
            require_utc_datetime(self.activated_at, field="sprint_activated_at"),
        )
        if not isinstance(self.members, tuple) or any(
            not isinstance(item, SprintActivationMember) for item in self.members
        ):
            raise ValueError("sprint_activation_baseline_members_invalid")
        if not self.members or len(self.members) > MAX_SPRINT_ACTIVATION_MEMBERS:
            raise ValueError("sprint_activation_baseline_member_count_invalid")
        if tuple(sorted(self.members)) != self.members:
            raise ValueError("sprint_activation_baseline_members_out_of_order")
        if len({item.card_id for item in self.members}) != len(self.members):
            raise ValueError("sprint_activation_baseline_member_duplicate")
        expected = self.derive_ref()
        if self.baseline_ref is None:
            object.__setattr__(self, "baseline_ref", expected)
        elif not isinstance(self.baseline_ref, str) or self.baseline_ref != expected:
            raise ValueError("sprint_activation_baseline_ref_invalid")

    @property
    def member_count(self) -> int:
        return len(self.members)

    def _digest_payload(self) -> dict[str, object]:
        return {
            "contract_version": SPRINT_ACTIVATION_BASELINE_CONTRACT_VERSION,
            "board_id": self.board_id,
            "sprint_id": self.sprint_id,
            "spec_id": self.spec_id,
            "sprint_version": self.sprint_version,
            "activated_at": _utc_text(self.activated_at),
            "activated_by": self.activated_by,
            "members": [item.canonical_dict() for item in self.members],
        }

    def derive_ref(self) -> str:
        encoded = json.dumps(
            self._digest_payload(), sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return f"sprint_activation_baseline:{hashlib.sha256(encoded).hexdigest()}"

    def canonical_dict(self) -> dict[str, object]:
        return {**self._digest_payload(), "baseline_ref": self.baseline_ref}


@runtime_checkable
class SprintActivationBaselineStore(Protocol):
    async def get(
        self, context: object, *, board_id: str, sprint_id: str
    ) -> SprintActivationBaseline | None: ...

    async def save_if_absent(
        self, context: object, baseline: SprintActivationBaseline
    ) -> SprintActivationBaseline: ...


def register_sprint_activation_baseline_store(
    store: SprintActivationBaselineStore,
) -> None:
    if not isinstance(store, SprintActivationBaselineStore):
        raise TypeError("sprint_activation_baseline_store_invalid")
    register_runtime_value(_RUNTIME_KEY, store)


def get_sprint_activation_baseline_store() -> SprintActivationBaselineStore:
    return require_runtime_value(
        _RUNTIME_KEY, "sprint_activation_baseline_store_not_configured"
    )


def reset_sprint_activation_baseline_store_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "MAX_SPRINT_ACTIVATION_MEMBERS",
    "SPRINT_ACTIVATION_BASELINE_CONTRACT_VERSION",
    "SprintActivationBaseline",
    "SprintActivationBaselineStore",
    "SprintActivationMember",
    "SprintCommitmentState",
    "get_sprint_activation_baseline_store",
    "register_sprint_activation_baseline_store",
    "reset_sprint_activation_baseline_store_for_tests",
]
