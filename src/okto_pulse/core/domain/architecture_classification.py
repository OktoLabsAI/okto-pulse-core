"""Strict authored classification intent and deterministic source preflight.

This module never creates an IR, fetches a schema reference or grants authority.
The application coordinator checks all permissions and locks before persisting
its plan and the structured writer's prepared IRs in one transaction.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Annotated, Any, Literal, Mapping

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StrictInt,
    StringConstraints,
    model_validator,
)
from typing_extensions import Required, TypedDict

from okto_pulse.core.models.schemas import IntegrationRequirementType

from okto_pulse.core.domain.architecture_candidates import (
    ArchitectureCandidate,
    ArchitectureCandidatePopulation,
)


from okto_pulse.core.domain.requirement_verification import RequirementVerification

MAX_CLASSIFICATION_BYTES = 256 * 1024
Nonempty = Annotated[str, StringConstraints(strict=True, min_length=1, pattern=r"\S")]
Digest = Annotated[str, StringConstraints(strict=True, pattern=r"^[0-9a-f]{64}$")]


class ArchitectureClassificationError(ValueError):
    """Stable code, with no provider diagnostics or source contract in errors."""


class AuthoredIntegrationRequirement(TypedDict, total=False):
    """Closed create input; relational links still use the shared IR preflight.

    Preserve omitted fields rather than inferring roles, protocol or HTTP data.
    data_contract is JSON data, never an authority/policy input.
    """

    __pydantic_config__ = ConfigDict(extra="forbid")

    id: Nonempty
    title: Required[Nonempty]
    integration_type: Required[IntegrationRequirementType]
    description: str
    provider: str | None
    consumer: str | None
    contract_ref: str | None
    endpoint: str | None
    method: str | None
    data_contract: dict[str, JsonValue] | None
    linked_requirements: list[Nonempty] | None
    linked_api_contracts: list[Nonempty] | None
    linked_task_ids: list[Nonempty] | None
    status: Literal["active"]
    notes: str | None
    verification: RequirementVerification | None


class ArchitectureDecisionIntent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    candidate_ref: Nonempty
    expected_source_digest: Digest
    disposition: Literal["promote_to_ir", "associate_existing_ir", "context_only"]
    integration_requirements: tuple[AuthoredIntegrationRequirement, ...] = ()
    integration_requirement_refs: tuple[Nonempty, ...] = ()
    # RFC6901 paths through named object members. Selecting an array is fine;
    # selecting a positional array element would not be a stable fragment.
    scope_paths: tuple[str, ...] = ("",)
    reason: Nonempty | None = None
    remainder_reason: Nonempty | None = None

    @model_validator(mode="after")
    def coherent_disposition(self):
        if not self.scope_paths or len(set(self.scope_paths)) != len(self.scope_paths):
            raise ValueError("architecture_classification_scope_invalid")
        for path in self.scope_paths:
            _scope_tokens(path)
        if self.disposition == "promote_to_ir":
            if (
                not self.integration_requirements
                or self.integration_requirement_refs
                or self.reason
            ):
                raise ValueError("architecture_classification_promotion_invalid")
            for payload in self.integration_requirements:
                # Do not inherit the legacy IR model's HTTP/API default from
                # an event, MCP or unknown architecture contract.
                if (
                    not payload.get("integration_type")
                    or payload.get("status", "active") != "active"
                ):
                    raise ValueError(
                        "architecture_classification_authored_ir_type_required"
                    )
        elif self.disposition == "associate_existing_ir":
            if (
                not self.integration_requirement_refs
                or self.integration_requirements
                or self.reason
            ):
                raise ValueError("architecture_classification_association_invalid")
            if len(set(self.integration_requirement_refs)) != len(
                self.integration_requirement_refs
            ):
                raise ValueError("architecture_classification_duplicate_ir_ref")
        elif (
            not self.reason
            or self.integration_requirements
            or self.integration_requirement_refs
        ):
            raise ValueError("architecture_classification_context_reason_required")
        return self


class ArchitectureClassificationBatch(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    expected_spec_version: Annotated[StrictInt, Field(ge=1)]
    expected_spec_edition: Annotated[StrictInt, Field(ge=1)]
    idempotency_key: Annotated[Nonempty, Field(max_length=255)]
    decisions: Annotated[
        tuple[ArchitectureDecisionIntent, ...], Field(min_length=1, max_length=50)
    ]

    @model_validator(mode="before")
    @classmethod
    def bounded_json(cls, value):
        try:
            encoded = _canonical_json(value).encode("utf-8")
        except (TypeError, ValueError):
            raise ValueError("architecture_classification_json_required") from None
        if len(encoded) > MAX_CLASSIFICATION_BYTES:
            raise ValueError("architecture_classification_payload_too_large")
        return value

    @model_validator(mode="after")
    def coherent_candidate_scopes(self):
        grouped: dict[str, list[ArchitectureDecisionIntent]] = defaultdict(list)
        for item in self.decisions:
            grouped[item.candidate_ref].append(item)
        for items in grouped.values():
            if len({item.expected_source_digest for item in items}) != 1:
                raise ValueError("architecture_classification_source_conflict")
            paths = [_scope_tokens(path) for item in items for path in item.scope_paths]
            for index, path in enumerate(paths):
                if any(
                    path[: len(other)] == other or other[: len(path)] == path
                    for other in paths[:index]
                ):
                    raise ValueError("architecture_classification_scope_overlap")
            reasons = {
                item.remainder_reason
                for item in items
                if item.remainder_reason is not None
            }
            if paths == [()]:
                if reasons:
                    raise ValueError("architecture_classification_no_remainder")
            elif len(reasons) != 1:
                raise ValueError(
                    "architecture_classification_remainder_reason_required"
                )
        return self

    def request_digest(self, *, board_id: str, spec_id: str, actor_id: str) -> str:
        payload = {
            "contract_version": "architecture-classification/v1",
            "board_id": board_id,
            "spec_id": spec_id,
            "actor_id": actor_id,
            "request": self.model_dump(mode="json"),
        }
        return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _scope_tokens(path: str) -> tuple[str, ...]:
    if path == "":
        return ()
    if not path.startswith("/") or re.search(r"~(?![01])", path):
        raise ValueError("architecture_classification_scope_invalid")
    return tuple(
        part.replace("~1", "/").replace("~0", "~") for part in path[1:].split("/")
    )


@dataclass(frozen=True, slots=True)
class ResolvedArchitectureDecision:
    intent: ArchitectureDecisionIntent
    candidate: ArchitectureCandidate


def resolve_architecture_classification(
    batch: ArchitectureClassificationBatch,
    *,
    spec_id: str,
    population: ArchitectureCandidatePopulation,
    integration_requirements: tuple[Mapping[str, Any], ...],
) -> tuple[ResolvedArchitectureDecision, ...]:
    """Resolve explicit IDs only; never bulk-classify unseen future candidates."""
    if not population.source_complete:
        raise ArchitectureClassificationError("architecture_sources_unavailable")
    by_candidate: dict[str, list[ArchitectureCandidate]] = defaultdict(list)
    for candidate in population.candidates:
        by_candidate[candidate.id].append(candidate)
    irs: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for item in integration_requirements:
        identity = item.get("id")
        if isinstance(identity, str) and identity.strip():
            irs[identity].append(item)
    resolved = []
    for intent in batch.decisions:
        matches = by_candidate.get(intent.candidate_ref, [])
        if len(matches) != 1 or any(
            issue.candidate_id == intent.candidate_ref for issue in population.issues
        ):
            raise ArchitectureClassificationError("architecture_candidate_unresolved")
        candidate = matches[0]
        if (
            candidate.spec_id != spec_id
            or candidate.spec_edition != batch.expected_spec_edition
            or candidate.source_digest != intent.expected_source_digest
        ):
            raise ArchitectureClassificationError(
                "architecture_candidate_source_changed"
            )
        for path in intent.scope_paths:
            value = candidate.contract
            for key in _scope_tokens(path):
                if not isinstance(value, dict) or key not in value:
                    raise ArchitectureClassificationError(
                        "architecture_classification_scope_unresolved"
                    )
                value = value[key]
            if value is None:
                raise ArchitectureClassificationError(
                    "architecture_classification_scope_unresolved"
                )
        for ir_id in intent.integration_requirement_refs:
            items = irs.get(ir_id, [])
            if len(items) != 1 or items[0].get("status", "active") != "active":
                raise ArchitectureClassificationError(
                    "architecture_classification_ir_not_active_in_spec"
                )
        resolved.append(ResolvedArchitectureDecision(intent, candidate))
    return tuple(resolved)
