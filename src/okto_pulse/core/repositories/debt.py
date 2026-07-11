"""Transitional ORM/Base debt ledger for the relational strangler (spec #04,
tr_cd0631cf / fr_802078e4).

Records the baseline of ORM Base classes and the repositories that are ALLOWED
to return ORM models during the transition — each with owner, deadline,
affected aggregate and a withdrawal criterion. A repository may return an ORM
type ONLY if it is registered here; the RelationalBoundaryGate (spec #04 card
b37786d9) consumes ``is_orm_return_excepted`` to block any new, unregistered ORM
return in a migrated use case.
"""

from __future__ import annotations

from dataclasses import dataclass

#: Base-derived ORM classes in ``core/models/db.py`` at the start of the
#: strangler. Verified on `main` (== spec #04 ac_cddd871d baseline). This is a
#: debt baseline to draw down, NOT a blind total-removal target this phase.
ORM_BASE_CLASS_BASELINE = 59


@dataclass(frozen=True)
class OrmReturnDebt:
    """A registered, time-boxed exception allowing a repository to return ORM."""

    aggregate: str
    repository: str
    orm_type: str
    owner: str
    deadline: str
    withdrawal_criterion: str

#: Repositories permitted to return the existing ORM models during the
#: transition (the first-cut aggregates). New ORM returns NOT listed here are
#: blocked by the RelationalBoundaryGate.
ORM_RETURN_DEBT: tuple[OrmReturnDebt, ...] = (
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
