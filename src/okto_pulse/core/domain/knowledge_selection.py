"""Storage-neutral contracts for selective Knowledge Base propagation.

The values in this module preserve the difference between an omitted selector,
an explicitly empty selector, and an explicit set of Knowledge Base ids.  They
also describe durable assignments without owning persistence, transport, or
lineage identity.  Revision identity is delegated to the public
``ResourceRevisionStamp`` contract.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
import re
from typing import Any, TypeVar, cast

from okto_pulse.core.domain.resource_revision import ResourceRevisionStamp


KNOWLEDGE_PROPAGATION_CONTRACT_VERSION = 2
_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")


class KnowledgePropagationContractError(ValueError):
    """A selective-propagation value violates its public contract."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(code)

    def as_dict(self) -> dict[str, str]:
        return {"code": self.code, "detail": self.detail}

    def to_error_dict(self) -> dict[str, str]:
        return self.as_dict()


class KnowledgeSelectionState(str, Enum):
    OMITTED = "omitted"
    EXPLICIT_EMPTY = "explicit_empty"
    EXPLICIT_IDS = "explicit_ids"


class KnowledgePropagationMode(str, Enum):
    REFERENCE = "reference"
    SNAPSHOT = "snapshot"
    DROP = "drop"


class KnowledgeAssignmentState(str, Enum):
    ACTIVE = "active"
    STALE = "stale"
    SOURCE_DELETED = "source_deleted"
    DROPPED = "dropped"
    INACTIVE = "inactive"


class KnowledgeOriginClass(str, Enum):
    V2 = "v2"
    LEGACY_ALL = "legacy_all"
    SELECTED_LEGACY = "selected_legacy"
    LEGACY_UNRESOLVED = "legacy_unresolved"


class KnowledgeRelevanceEntityType(str, Enum):
    FUNCTIONAL_REQUIREMENT = "functional_requirement"
    ACCEPTANCE_CRITERION = "acceptance_criterion"
    TEST_SCENARIO = "test_scenario"


class KnowledgeTargetType(str, Enum):
    SPEC = "spec"
    CARD = "card"


_EnumT = TypeVar("_EnumT", bound=Enum)


def _coerce_enum(value: object, enum_type: type[_EnumT], field: str) -> _EnumT:
    if isinstance(value, enum_type):
        return value
    if isinstance(value, str):
        try:
            return enum_type(value.strip())
        except ValueError:
            pass
    raise KnowledgePropagationContractError(
        f"invalid_{field}",
        f"{field} must be one of: "
        + ", ".join(str(item.value) for item in enum_type),
    )


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise KnowledgePropagationContractError(
            f"invalid_{field}", f"{field} must be a string"
        )
    normalized = value.strip()
    if not normalized:
        raise KnowledgePropagationContractError(
            f"empty_{field}", f"{field} must not be empty"
        )
    return normalized


def _optional_text(value: object | None, field: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, field)


def _optional_blank_text(value: object | None, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise KnowledgePropagationContractError(
            f"invalid_{field}", f"{field} must be a string"
        )
    normalized = value.strip()
    return normalized or None


def _canonical_ids(values: Sequence[str], field: str) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise KnowledgePropagationContractError(
            f"invalid_{field}", f"{field} must be an ordered sequence of strings"
        )
    canonical: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = _required_text(value, field)
        if normalized not in seen:
            canonical.append(normalized)
            seen.add(normalized)
    return tuple(sorted(canonical))


@dataclass(frozen=True, slots=True)
class KnowledgeRelevanceLink:
    """A functional entity that explains why one assignment is relevant."""

    entity_type: KnowledgeRelevanceEntityType | str
    entity_id: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "entity_type",
            _coerce_enum(
                self.entity_type,
                KnowledgeRelevanceEntityType,
                "relevance_entity_type",
            ),
        )
        object.__setattr__(
            self, "entity_id", _required_text(self.entity_id, "relevance_entity_id")
        )

    def as_dict(self) -> dict[str, str]:
        return {
            "entity_type": cast(
                KnowledgeRelevanceEntityType, self.entity_type
            ).value,
            "entity_id": self.entity_id,
        }

    def to_dict(self) -> dict[str, str]:
        return self.as_dict()


@dataclass(frozen=True, slots=True)
class KnowledgeSelection:
    """Canonical tri-state selection received by a propagation operation."""

    selection_state: KnowledgeSelectionState | str
    knowledge_ids: tuple[str, ...] = ()
    mode: KnowledgePropagationMode | str | None = None

    def __post_init__(self) -> None:
        state = _coerce_enum(
            self.selection_state, KnowledgeSelectionState, "selection_state"
        )
        mode = (
            None
            if self.mode is None
            else _coerce_enum(self.mode, KnowledgePropagationMode, "propagation_mode")
        )
        knowledge_ids = _canonical_ids(self.knowledge_ids, "knowledge_ids")

        if state is KnowledgeSelectionState.OMITTED:
            if knowledge_ids or mode is not None:
                raise KnowledgePropagationContractError(
                    "omitted_selection_must_be_empty",
                    "omitted selection requires no ids and no propagation mode",
                )
        elif state is KnowledgeSelectionState.EXPLICIT_EMPTY:
            if knowledge_ids or mode is not KnowledgePropagationMode.DROP:
                raise KnowledgePropagationContractError(
                    "explicit_empty_requires_drop",
                    "explicit_empty selection requires no ids and mode=drop",
                )
        elif not knowledge_ids or mode is None:
            raise KnowledgePropagationContractError(
                "explicit_ids_require_ids_and_mode",
                "explicit_ids selection requires at least one id and a mode",
            )

        object.__setattr__(self, "selection_state", state)
        object.__setattr__(self, "knowledge_ids", knowledge_ids)
        object.__setattr__(self, "mode", mode)

    @classmethod
    def omitted(cls) -> "KnowledgeSelection":
        return cls(selection_state=KnowledgeSelectionState.OMITTED)

    @classmethod
    def explicit_empty(cls) -> "KnowledgeSelection":
        return cls(
            selection_state=KnowledgeSelectionState.EXPLICIT_EMPTY,
            mode=KnowledgePropagationMode.DROP,
        )

    @classmethod
    def explicit_ids(
        cls,
        knowledge_ids: Sequence[str],
        *,
        mode: KnowledgePropagationMode | str,
    ) -> "KnowledgeSelection":
        return cls(
            selection_state=KnowledgeSelectionState.EXPLICIT_IDS,
            knowledge_ids=knowledge_ids,  # type: ignore[arg-type]
            mode=mode,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract_version": KNOWLEDGE_PROPAGATION_CONTRACT_VERSION,
            "selection_state": cast(
                KnowledgeSelectionState, self.selection_state
            ).value,
            "knowledge_ids": list(self.knowledge_ids),
            "mode": (
                cast(KnowledgePropagationMode, self.mode).value
                if self.mode
                else None
            ),
        }

    def to_dict(self) -> dict[str, Any]:
        return self.as_dict()


@dataclass(frozen=True, slots=True)
class KnowledgeAssignment:
    """One durable relationship between a target and a Knowledge Base root."""

    assignment_id: str
    board_id: str
    target_type: KnowledgeTargetType | str
    target_id: str
    source_knowledge_id: str
    revision_stamp: ResourceRevisionStamp
    mode: KnowledgePropagationMode | str
    state: KnowledgeAssignmentState | str
    origin_class: KnowledgeOriginClass | str
    actor_id: str
    revision: int
    justification: str | None = None
    relevance_links: tuple[KnowledgeRelevanceLink, ...] = ()

    def __post_init__(self) -> None:
        for field in (
            "assignment_id",
            "board_id",
            "target_id",
            "source_knowledge_id",
            "actor_id",
        ):
            object.__setattr__(
                self, field, _required_text(getattr(self, field), field)
            )

        target_type = _coerce_enum(
            self.target_type, KnowledgeTargetType, "target_type"
        )
        mode = _coerce_enum(self.mode, KnowledgePropagationMode, "propagation_mode")
        state = _coerce_enum(self.state, KnowledgeAssignmentState, "assignment_state")
        origin_class = _coerce_enum(
            self.origin_class, KnowledgeOriginClass, "origin_class"
        )
        if (mode is KnowledgePropagationMode.DROP) != (
            state is KnowledgeAssignmentState.DROPPED
        ):
            raise KnowledgePropagationContractError(
                "drop_assignment_state_mismatch",
                "mode=drop and state=dropped must occur together",
            )
        if (
            state is KnowledgeAssignmentState.STALE
            and mode is not KnowledgePropagationMode.SNAPSHOT
        ):
            raise KnowledgePropagationContractError(
                "stale_assignment_requires_snapshot",
                "only snapshot assignments may become stale",
            )

        if not isinstance(self.revision_stamp, ResourceRevisionStamp):
            raise KnowledgePropagationContractError(
                "invalid_revision_stamp",
                "revision_stamp must use the public ResourceRevisionStamp contract",
            )
        stamp = ResourceRevisionStamp(
            root_id=_required_text(self.revision_stamp.root_id, "root_id"),
            immediate_parent_id=_optional_text(
                self.revision_stamp.immediate_parent_id, "immediate_parent_id"
            ),
            source_revision=_optional_text(
                self.revision_stamp.source_revision, "source_revision"
            ),
            source_content_sha256=_optional_text(
                self.revision_stamp.source_content_sha256,
                "source_content_sha256",
            ),
        )
        if (
            stamp.source_content_sha256 is not None
            and _SHA256_HEX.fullmatch(stamp.source_content_sha256) is None
        ):
            raise KnowledgePropagationContractError(
                "invalid_source_content_sha256",
                "source_content_sha256 must be a lowercase SHA-256 hex digest",
            )
        if origin_class is KnowledgeOriginClass.V2 and (
            stamp.source_revision is None or stamp.source_content_sha256 is None
        ):
            raise KnowledgePropagationContractError(
                "v2_assignment_revision_evidence_required",
                "v2 assignments require source_revision and source_content_sha256",
            )

        if type(self.revision) is not int or self.revision < 0:
            raise KnowledgePropagationContractError(
                "invalid_assignment_revision",
                "revision must be a non-negative integer",
            )

        if isinstance(self.relevance_links, (str, bytes)) or not isinstance(
            self.relevance_links, Sequence
        ):
            raise KnowledgePropagationContractError(
                "invalid_relevance_links",
                "relevance_links must be an ordered sequence of "
                "KnowledgeRelevanceLink",
            )
        links: list[KnowledgeRelevanceLink] = []
        seen_links: set[tuple[str, str]] = set()
        for link in self.relevance_links:
            if not isinstance(link, KnowledgeRelevanceLink):
                raise KnowledgePropagationContractError(
                    "invalid_relevance_link",
                    "every relevance link must be a KnowledgeRelevanceLink",
                )
            identity = (
                cast(KnowledgeRelevanceEntityType, link.entity_type).value,
                link.entity_id,
            )
            if identity in seen_links:
                raise KnowledgePropagationContractError(
                    "duplicate_relevance_link",
                    "relevance links must not contain duplicates",
                )
            links.append(link)
            seen_links.add(identity)

        object.__setattr__(self, "revision_stamp", stamp)
        object.__setattr__(self, "target_type", target_type)
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "origin_class", origin_class)
        links.sort(
            key=lambda item: (
                cast(KnowledgeRelevanceEntityType, item.entity_type).value,
                item.entity_id,
            )
        )

        justification = _optional_blank_text(self.justification, "justification")
        if mode is KnowledgePropagationMode.DROP and justification is None:
            raise KnowledgePropagationContractError(
                "knowledge_drop_justification_required",
                "drop assignments require a justification",
            )
        if origin_class is KnowledgeOriginClass.V2 and justification is None:
            raise KnowledgePropagationContractError(
                "knowledge_assignment_justification_required",
                "v2 assignments require a justification",
            )
        object.__setattr__(self, "justification", justification)
        object.__setattr__(self, "relevance_links", tuple(links))

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract_version": KNOWLEDGE_PROPAGATION_CONTRACT_VERSION,
            "assignment_id": self.assignment_id,
            "board_id": self.board_id,
            "target_type": cast(KnowledgeTargetType, self.target_type).value,
            "target_id": self.target_id,
            "source_knowledge_id": self.source_knowledge_id,
            "revision_stamp": self.revision_stamp.to_dict(),
            "mode": cast(KnowledgePropagationMode, self.mode).value,
            "state": cast(KnowledgeAssignmentState, self.state).value,
            "origin_class": cast(KnowledgeOriginClass, self.origin_class).value,
            "actor_id": self.actor_id,
            "revision": self.revision,
            "justification": self.justification,
            "relevance_links": [link.as_dict() for link in self.relevance_links],
        }

    def to_dict(self) -> dict[str, Any]:
        return self.as_dict()


__all__ = [
    "KNOWLEDGE_PROPAGATION_CONTRACT_VERSION",
    "KnowledgeAssignment",
    "KnowledgeAssignmentState",
    "KnowledgeOriginClass",
    "KnowledgePropagationContractError",
    "KnowledgePropagationMode",
    "KnowledgeRelevanceEntityType",
    "KnowledgeRelevanceLink",
    "KnowledgeSelection",
    "KnowledgeSelectionState",
    "KnowledgeTargetType",
]
