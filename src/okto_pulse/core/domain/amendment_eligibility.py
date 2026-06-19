"""Pure Path B AmendmentHotfixRevision lifecycle + eligibility policy
(spec 7ea1e4be, card f5dca74a).

Leaf module — stdlib + domain dataclasses only. It imports NO database/session
code, so the bug-regression resolver/gate (cards ead17e4d / c9cf9781) can read
the eligibility predicate without pulling the SQLAlchemy store. ``models/db.py``
(the ORM table), ``models/schemas.py`` and ``services/amendment_revision.py``
import from HERE; never the reverse.

Eligibility is LAYERED (codex card #1 refinement — do not collapse the names):
* ``lineage_eligible`` — a non-blocking status (approved/done) AND
  ``lineage_state == complete``. Before ``done`` the amendment is still
  working-only, so this is NOT ``closure_ready``/``canonical_ready``: final bug
  closure additionally depends on the shared Path A/B predicate +
  ``coverage_confirmed`` delivered by later cards.
* ``canonicalization_candidate`` — the ``done`` + complete subset that card #2
  (74a6d258) promotes to canonical amendment facts.
Everything else fails closed with a stable reason code (unknown status and
unknown/missing lineage included).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum as PyEnum


class AmendmentRevisionStatus(str, PyEnum):
    """Lifecycle of a Path B amendment/hotfix revision (fr_eb9d83c9)."""

    DRAFT = "draft"
    REVIEW = "review"
    APPROVED = "approved"
    DONE = "done"
    CANCELLED = "cancelled"
    SUPERSEDED = "superseded"


class AmendmentLineageState(str, PyEnum):
    """Whether the bug -> original spec -> revision/evidence lineage is complete."""

    INCOMPLETE = "incomplete"
    COMPLETE = "complete"


#: The only statuses from which an amendment MAY grant eligibility (fr_09119273).
#: Every other status — draft/review and any unknown value — is blocking.
NON_BLOCKING_STATUSES: frozenset[AmendmentRevisionStatus] = frozenset(
    {AmendmentRevisionStatus.APPROVED, AmendmentRevisionStatus.DONE}
)

# Stable, bounded reason codes (safe for API payloads + metric labels).
REASON_OK = "ok"
REASON_UNKNOWN_STATUS = "amendment_status_unknown"
REASON_STATUS_BLOCKING = "amendment_status_blocking"
REASON_CANCELLED = "amendment_cancelled"
REASON_SUPERSEDED = "amendment_superseded"
REASON_LINEAGE_INCOMPLETE = "amendment_lineage_incomplete"


@dataclass(frozen=True)
class AmendmentEligibility:
    """Deterministic verdict for a (status, lineage_state) pair.

    ``lineage_eligible`` never implies closure on its own; it only means the
    amendment's lineage qualifies. ``canonicalization_candidate`` is the
    done+complete subset card #2 canonicalizes. ``blocked`` + ``reason_code``
    carry the fail-closed explanation for every other combination.
    """

    lineage_eligible: bool
    canonicalization_candidate: bool
    blocked: bool
    reason_code: str


def _coerce_status(value: object) -> AmendmentRevisionStatus | None:
    if isinstance(value, AmendmentRevisionStatus):
        return value
    try:
        return AmendmentRevisionStatus(value)
    except (ValueError, TypeError):
        return None


def _coerce_lineage(value: object) -> AmendmentLineageState | None:
    if isinstance(value, AmendmentLineageState):
        return value
    try:
        return AmendmentLineageState(value)
    except (ValueError, TypeError):
        return None


def amendment_status_is_blocking(status: object) -> bool:
    """True when the status ALONE blocks eligibility — i.e. it is not one of the
    non-blocking statuses (approved/done). Unknown/cancelled/superseded/draft/
    review all block. Fail-closed on unknown."""
    coerced = _coerce_status(status)
    return coerced is None or coerced not in NON_BLOCKING_STATUSES


def evaluate_amendment_eligibility(
    status: object, lineage_state: object
) -> AmendmentEligibility:
    """Fail-closed (status, lineage_state) verdict (fr_09119273 / G3 / AC5).

    Matrix:
      draft/review + complete       -> blocked (status_blocking)   [G3]
      approved + incomplete         -> blocked (lineage_incomplete)
      approved + complete           -> lineage_eligible, NOT canonical
      done + complete               -> lineage_eligible + canonicalization_candidate
      done + incomplete             -> blocked (lineage_incomplete)
      cancelled                     -> blocked (cancelled)
      superseded                    -> blocked (superseded)
      unknown status / unknown lineage -> blocked (fail-closed)
    """
    coerced = _coerce_status(status)
    if coerced is None:
        return AmendmentEligibility(False, False, True, REASON_UNKNOWN_STATUS)
    if coerced is AmendmentRevisionStatus.CANCELLED:
        return AmendmentEligibility(False, False, True, REASON_CANCELLED)
    if coerced is AmendmentRevisionStatus.SUPERSEDED:
        return AmendmentEligibility(False, False, True, REASON_SUPERSEDED)
    if coerced not in NON_BLOCKING_STATUSES:
        # draft / review — blocking even with complete lineage (G3).
        return AmendmentEligibility(False, False, True, REASON_STATUS_BLOCKING)
    if _coerce_lineage(lineage_state) is not AmendmentLineageState.COMPLETE:
        # incomplete OR unknown/missing lineage -> fail closed.
        return AmendmentEligibility(False, False, True, REASON_LINEAGE_INCOMPLETE)
    # Non-blocking status + complete lineage = lineage_eligible.
    is_done = coerced is AmendmentRevisionStatus.DONE
    return AmendmentEligibility(
        lineage_eligible=True,
        canonicalization_candidate=is_done,
        blocked=False,
        reason_code=REASON_OK,
    )


__all__ = [
    "AmendmentRevisionStatus",
    "AmendmentLineageState",
    "NON_BLOCKING_STATUSES",
    "AmendmentEligibility",
    "amendment_status_is_blocking",
    "evaluate_amendment_eligibility",
    "REASON_OK",
    "REASON_UNKNOWN_STATUS",
    "REASON_STATUS_BLOCKING",
    "REASON_CANCELLED",
    "REASON_SUPERSEDED",
    "REASON_LINEAGE_INCOMPLETE",
]
