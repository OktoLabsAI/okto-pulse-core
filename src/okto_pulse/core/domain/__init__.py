"""Pure domain policy/predicate leaves (no DB/session/SQLAlchemy imports).

Modules here are safe to import from models, services and the bug-regression
resolver/gate without pulling heavy database dependencies.
"""

from okto_pulse.core.domain.entities import Board, Ideation, Spec
from okto_pulse.core.domain.knowledge_governance import (
    KnowledgeGovernanceInvalidMetadata,
    KnowledgeGovernanceMetadataV1,
    normalize_knowledge_governance_metadata,
    parse_knowledge_governance_metadata,
    project_knowledge_governance,
)
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
    "KnowledgeGovernanceInvalidMetadata",
    "KnowledgeGovernanceMetadataV1",
    "LOCAL_REALM_ID",
    "MissingRealmScope",
    "RealmIsolationViolation",
    "RealmScope",
    "Spec",
    "normalize_knowledge_governance_metadata",
    "parse_knowledge_governance_metadata",
    "project_knowledge_governance",
    "require_realm_scope",
]
