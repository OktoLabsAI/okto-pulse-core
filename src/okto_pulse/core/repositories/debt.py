"""Terminal ORM-return policy and retired strangler history.

Core no longer owns SQLAlchemy mappings and no Core repository is allowed to
return an edition-owned ORM type. The historical entries remain as audit data;
only ``ORM_RETURN_DEBT`` is consulted by active boundary gates.
"""

from __future__ import annotations

from dataclasses import dataclass

#: Verified starting point of the relational strangler. Historical only.
HISTORICAL_ORM_BASE_CLASS_BASELINE = 59

#: Terminal active budget: mappings are edition-owned, never Core-owned.
ORM_BASE_CLASS_BASELINE = 0


@dataclass(frozen=True)
class OrmReturnDebt:
    """A registered, time-boxed exception allowing a repository to return ORM."""

    aggregate: str
    repository: str
    orm_type: str
    owner: str
    deadline: str
    withdrawal_criterion: str

#: Retired first-cut exceptions retained only for architecture history.
HISTORICAL_ORM_RETURN_DEBT: tuple[OrmReturnDebt, ...] = (
    OrmReturnDebt(
        aggregate="board",
        repository="okto_pulse.core.repositories.interfaces.repositories.BoardRepository",
        orm_type="okto_pulse.core.models.db.Board",
        owner="core-refactor",
        deadline="F01 Community mapping extraction",
        withdrawal_criterion="Core ORM mappings and all mapped consumers are removed",
    ),
    OrmReturnDebt(
        aggregate="ideation",
        repository="okto_pulse.core.repositories.interfaces.repositories.IdeationRepository",
        orm_type="okto_pulse.core.models.db.Ideation",
        owner="core-refactor",
        deadline="F01 Community mapping extraction",
        withdrawal_criterion="Core ORM mappings and all mapped consumers are removed",
    ),
    OrmReturnDebt(
        aggregate="spec",
        repository="okto_pulse.core.repositories.interfaces.repositories.SpecRepository",
        orm_type="okto_pulse.core.models.db.Spec",
        owner="core-refactor",
        deadline="F01 Community mapping extraction",
        withdrawal_criterion="Core ORM mappings and all mapped consumers are removed",
    ),
)

#: Active exceptions. Terminal by design: Core ports return neutral records or
#: domain entities, and Community maps those contracts to SQLAlchemy.
ORM_RETURN_DEBT: tuple[OrmReturnDebt, ...] = ()

_EXCEPTED_ORM_TYPES = frozenset(entry.orm_type for entry in ORM_RETURN_DEBT)
_EXCEPTED_ORM_PAIRS = frozenset(
    (entry.repository, entry.orm_type) for entry in ORM_RETURN_DEBT
)


def is_orm_return_excepted(orm_type: str, repository: str | None = None) -> bool:
    """True if an ORM return is a registered transitional debt.

    When ``repository`` is given the exception is keyed by the EXACT
    ``(repository, orm_type)`` pair — so a non-migrated repository returning an
    already-excepted type (e.g. a future ``CardRepository`` returning ``Board``)
    is correctly NOT excepted (the boundary gate must block it). With
    ``repository`` omitted it falls back to a type-only check. A return that is
    NOT excepted is a new, unregistered ORM leak.
    """
    if repository is not None:
        return (repository, orm_type) in _EXCEPTED_ORM_PAIRS
    return orm_type in _EXCEPTED_ORM_TYPES


@dataclass(frozen=True)
class TransitionalDebt:
    """A registered, time-boxed transitional shortcut other than an ORM return."""

    kind: str
    location: str
    owner: str
    deadline: str
    withdrawal_criterion: str
