"""Pure domain policy/predicate leaves (no DB/session/SQLAlchemy imports).

Modules here are safe to import from models, services and the bug-regression
resolver/gate without pulling heavy database dependencies.
"""

from okto_pulse.core.domain.entities import Board, Ideation, Spec
from okto_pulse.core.domain.realm import (
    LOCAL_REALM_ID,
    MissingRealmScope,
    RealmIsolationViolation,
    RealmScope,
    require_realm_scope,
)

__all__ = [
    "Board",
    "Ideation",
    "LOCAL_REALM_ID",
    "MissingRealmScope",
    "RealmIsolationViolation",
    "RealmScope",
    "Spec",
    "require_realm_scope",
]
