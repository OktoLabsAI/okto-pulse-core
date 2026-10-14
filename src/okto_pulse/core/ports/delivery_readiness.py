"""Readiness facts from one authorized, read-only edition snapshot.

These are internal facts, not a transport response or an authorization token.
The use case reauthorizes access and filters detail visibility before rendering.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from okto_pulse.core.domain.delivery_evidence import DeliveryEvidenceSnapshot
from okto_pulse.core.domain.enums import CardStatus, CardType


class ReadinessReadError(ValueError):
    def __init__(self, code: str, *, native_code: str | None = None):
        super().__init__(code)
        self.code = code
        self.native_code = native_code


class ReadinessClock(Protocol):
    def now(self) -> datetime: ...


@dataclass(frozen=True, slots=True)
class ReadinessCard:
    id: str
    card_type: CardType
    status: CardStatus
    version: int
    scenario_ids: tuple[str, ...] | None
    assignee_id: str | None


@dataclass(frozen=True, slots=True)
class ReadinessScenarioProcedure:
    id: str
    title: str | None
    verification_method: str | None
    given: str | None
    when: str | None
    then: str | None
    notes: str | None
    criterion_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ReadinessSnapshot:
    facts: DeliveryEvidenceSnapshot
    snapshot_id: str
    evaluated_at: datetime
    spec_version: int
    spec_status: str
    cards: tuple[ReadinessCard, ...]
    scenarios: tuple[ReadinessScenarioProcedure, ...] = ()


class DeliveryReadinessReadPort(Protocol):
    async def load(self, *, board_id: str, spec_id: str, card_id: str | None = None) -> ReadinessSnapshot:
        """Load all related facts in one consistent batch, including other Cards.

        Caller must authorize evidence.read and Board/Spec/Card access first.
        A requested foreign, missing or archived Card is a scope error, never
        an empty successful population. No writes, commits or lock_scope fence.
        Preserve native authentication, selection and reviewer authority. Retry
        a generation race at most once; never combine different generations.
        Fingerprint semantic inputs and effective validity, excluding clock time.
        """
        ...
