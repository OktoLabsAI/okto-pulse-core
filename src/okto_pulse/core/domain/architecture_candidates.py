"""Pure candidate projection over an authorized, complete adopted design set.

The caller owns access checks and effective-resource resolution. A projection
never follows schema_ref, generates interface IDs, refreshes adopted snapshots,
or creates an Integration Requirement. Classifications belong to a Spec edition;
the logical identity and semantic digest deliberately exclude physical copies,
design revisions and diagram layout.
"""

from __future__ import annotations

import copy
from collections import Counter
from dataclasses import dataclass
import hashlib
import json
from typing import Any, Mapping


_CONTRACT_FIELDS = (
    "request_schema", "response_schema", "event_schema", "error_contract",
)
_SEMANTIC_FIELDS = (
    "name", "endpoint", "description", "participants", "direction", "protocol",
    "contract_type", *_CONTRACT_FIELDS, "schema_ref", "notes",
)


class ArchitectureCandidateReadError(ValueError):
    """Public, bounded read-contract errors; provider failures are not exposed."""


@dataclass(frozen=True, slots=True)
class AdoptedArchitectureDesign:
    board_id: str
    design_id: str
    root_design_id: str
    revision: int
    interfaces: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True, slots=True)
class ArchitectureCandidate:
    id: str
    spec_id: str
    spec_edition: int
    root_design_id: str
    interface_id: str
    source_digest: str
    contract_json: str
    # Each equivalent adopted physical snapshot retains its own provenance.
    adopted_sources: tuple[tuple[str, int], ...]
    signals: tuple[str, ...]

    @property
    def contract(self) -> dict[str, Any]:
        """A fresh view cannot mutate the content associated with the digest."""
        return json.loads(self.contract_json)


@dataclass(frozen=True, slots=True)
class ArchitectureCandidateIssue:
    code: str
    design_id: str | None = None
    interface_index: int | None = None
    candidate_id: str | None = None


@dataclass(frozen=True, slots=True)
class ArchitectureCandidatePopulation:
    candidates: tuple[ArchitectureCandidate, ...]
    issues: tuple[ArchitectureCandidateIssue, ...]
    source_complete: bool

    @property
    def resolved(self) -> bool:
        """Population integrity only; this does not authorize starting a Spec."""
        return self.source_complete and not self.issues


def architecture_candidate_read_projection(
    population: ArchitectureCandidatePopulation, *, board_id: str, spec_id: str,
    spec_version: int, spec_edition: int,
    offset: int = 0, limit: int = 25, candidate_id: str | None = None,
    source_digest: str | None = None,
) -> dict[str, Any]:
    """One transport-neutral read envelope; integrity is not start permission.

    Conflicting semantic variants share a candidate identity and remain visible.
    Unknown enumeration has no authoritative total, even when nothing was read.
    """
    if type(offset) is not int or not 0 <= offset <= 2**63 - 1:
        raise ArchitectureCandidateReadError("architecture_candidates_invalid_offset")
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ArchitectureCandidateReadError("architecture_candidates_invalid_limit")
    if bool(candidate_id) != bool(source_digest):
        raise ArchitectureCandidateReadError("architecture_candidate_identity_and_digest_required")
    detail = bool(candidate_id)
    selected = population.candidates[offset:offset + limit]
    if detail:
        selected = tuple(item for item in population.candidates
                         if item.id == candidate_id and item.source_digest == source_digest)
        if population.source_complete and len(selected) != 1:
            raise ArchitectureCandidateReadError("architecture_candidate_source_changed")
    issue_counts = Counter(issue.code for issue in population.issues)
    identities_complete = population.source_complete and not any(
        issue.candidate_id is None for issue in population.issues
    )
    return {
        "contract_version": "architecture-candidates/v1",
        "board_id": board_id, "spec_id": spec_id,
        "spec_version": spec_version, "spec_edition": spec_edition,
        "source_complete": population.source_complete,
        "population_state": (
            "unavailable" if not population.source_complete
            else "complete" if population.resolved else "unresolved"
        ),
        "total": len({item.id for item in population.candidates}) if identities_complete else None,
        "total_variants": len(population.candidates) if population.source_complete else None,
        "offset": 0 if detail else offset, "limit": 1 if detail else limit,
        "has_more": not detail and offset + limit < len(population.candidates),
        "profile": "detail" if detail else "summary",
        "issue_counts": dict(sorted(issue_counts.items())),
        "issues_truncated": len(population.issues) > 100,
        "candidates": [{
            "id": item.id, "root_design_id": item.root_design_id,
            "interface_id": item.interface_id, "source_digest": item.source_digest,
            "name": item.contract.get("name"),
            "contract_type": item.contract.get("contract_type"),
            "direction": item.contract.get("direction"),
            "protocol": item.contract.get("protocol"),
            **({"contract": item.contract} if detail else {}),
            "adopted_sources": [
                {"design_id": design_id, "revision": revision}
                for design_id, revision in item.adopted_sources
            ],
            "signals": list(item.signals),
        } for item in selected],
        "issues": [{
            "code": issue.code, "design_id": issue.design_id,
            "interface_index": issue.interface_index, "candidate_id": issue.candidate_id,
        } for issue in population.issues[:100]],
    }


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def declares_architecture_contract(interface: Mapping[str, Any]) -> bool:
    return bool(
        _text(interface.get("contract_type"))
        or _text(interface.get("schema_ref"))
        or any(interface.get(field) is not None for field in _CONTRACT_FIELDS)
    )


def _digest(value: Any) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def architecture_candidate_identity(spec_id: str, root_design_id: str, interface_id: str) -> str:
    """Stable logical identity shared by candidates and stored decision witnesses."""
    return "arqc_" + _digest([spec_id, root_design_id, interface_id])


def architecture_contract_digest(contract: Any) -> str:
    """Digest a normalized contract or selected JSON value without coercion."""
    return _digest(contract)


def _contract(interface: Mapping[str, Any]) -> dict[str, Any]:
    result = {field: copy.deepcopy(interface.get(field)) for field in _SEMANTIC_FIELDS}
    # Participants are an unordered collection of identities, not role slots.
    participants = result.get("participants")
    if participants is not None:
        if not isinstance(participants, (list, tuple)) or not all(
            isinstance(item, str) for item in participants
        ):
            raise ValueError("architecture_contract_invalid_participants")
        result["participants"] = sorted(set(participants))
    else:
        result["participants"] = []
    return result


def project_architecture_candidates(
    *, board_id: str, spec_id: str, spec_edition: int,
    designs: tuple[AdoptedArchitectureDesign, ...], source_complete: bool,
) -> ArchitectureCandidatePopulation:
    """Enumerate the whole supplied population, retaining conflicting revisions.

    Missing identity is an unresolved source, never a freshly generated ID or
    a silently discarded contract. Incomplete enumeration is not known empty.
    Cross-board input fails before exposing candidate content or counts.
    """
    if not board_id or not spec_id or type(spec_edition) is not int or spec_edition < 1:
        raise ValueError("architecture_candidate_scope_invalid")
    if type(source_complete) is not bool:
        raise ValueError("architecture_candidate_completeness_required")
    if any(design.board_id != board_id for design in designs):
        raise ValueError("architecture_candidate_scope_unavailable")

    issues: list[ArchitectureCandidateIssue] = []
    grouped: dict[str, dict[str, Any]] = {}
    if not source_complete:
        issues.append(ArchitectureCandidateIssue("architecture_sources_unavailable"))
    for design in designs:
        seen: set[str] = set()
        for index, interface in enumerate(design.interfaces):
            if not isinstance(interface, Mapping):
                issues.append(ArchitectureCandidateIssue(
                    "architecture_contract_unresolved", design.design_id, index,
                ))
                continue
            if not declares_architecture_contract(interface):
                continue
            interface_id = interface.get("id")
            if (
                not _text(interface_id) or not _text(design.root_design_id)
                or not _text(design.design_id)
                or type(design.revision) is not int or design.revision < 1
            ):
                issues.append(ArchitectureCandidateIssue(
                    "architecture_contract_identity_required", design.design_id, index,
                ))
                continue
            candidate_id = architecture_candidate_identity(spec_id, design.root_design_id, interface_id)
            if interface_id in seen:
                issues.append(ArchitectureCandidateIssue(
                    "architecture_interface_identity_duplicate", design.design_id,
                    index, candidate_id,
                ))
            seen.add(interface_id)
            try:
                contract = _contract(interface)
                digest = architecture_contract_digest(contract)
            except (TypeError, ValueError):
                issues.append(ArchitectureCandidateIssue(
                    "architecture_contract_unresolved", design.design_id,
                    index, candidate_id,
                ))
                continue
            group = grouped.setdefault(candidate_id, {
                "root": design.root_design_id, "interface": interface_id, "revisions": {},
            })
            variant = group["revisions"].setdefault(digest, {"contract": contract, "sources": set()})
            variant["sources"].add((design.design_id, design.revision))

    candidates: list[ArchitectureCandidate] = []
    for candidate_id, group in sorted(grouped.items()):
        if len(group["revisions"]) > 1:
            issues.append(ArchitectureCandidateIssue(
                "architecture_contract_revision_conflict", candidate_id=candidate_id,
            ))
        for digest, variant in sorted(group["revisions"].items()):
            contract = variant["contract"]
            signals = []
            if any(contract[field] == {} for field in _CONTRACT_FIELDS):
                signals.append("unrestricted_schema")
            if not any(contract[field] is not None for field in _CONTRACT_FIELDS):
                signals.append("reference_only" if _text(contract["schema_ref"]) else "contract_content_missing")
            candidates.append(ArchitectureCandidate(
                candidate_id, spec_id, spec_edition, group["root"], group["interface"],
                digest, json.dumps(contract, ensure_ascii=False, sort_keys=True,
                                   separators=(",", ":"), allow_nan=False),
                tuple(sorted(variant["sources"])), tuple(signals),
            ))
    return ArchitectureCandidatePopulation(tuple(candidates), tuple(issues), source_complete)
