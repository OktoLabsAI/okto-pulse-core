"""Read-only currentness over complete adopted sources and relational decisions.

Currentness is local classification structure, not semantic approval, rollout
adoption, requirement readiness or permission to begin execution. No writes or
external schema fetches occur here, including when a source has disappeared.
"""

import copy
import json
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Literal, Mapping

from okto_pulse.core.domain.architecture_candidates import (
    ArchitectureCandidatePopulation,
    architecture_candidate_identity,
    architecture_contract_digest,
)
from okto_pulse.core.domain.architecture_classification import (
    ArchitectureClassificationBatch,
)
from okto_pulse.core.ports.architecture_classification import ArchitectureDecisionRecord
from okto_pulse.core.domain.architecture_promotion_suggestion import (
    architecture_promotion_suggestion,
)

ArchitectureReviewState = Literal[
    "pending", "current", "review_required", "unresolved", "retired", "unavailable"
]
REVIEW_STATES = (
    "pending",
    "current",
    "review_required",
    "unresolved",
    "retired",
    "unavailable",
)


class ArchitectureClassificationReadError(ValueError):
    """Safe public code only, never a storage/provider diagnostic."""


def _tokens(path: str) -> tuple[str, ...]:
    return (
        tuple(
            token.replace("~1", "/").replace("~0", "~") for token in path[1:].split("/")
        )
        if path
        else ()
    )


def _value(contract: dict, path: str) -> tuple[bool, Any]:
    value: Any = contract
    for key in _tokens(path):
        if not isinstance(value, dict) or key not in value:
            return False, None
        value = value[key]
    return True, value


def _same_scope(before: dict, after: dict, path: str) -> bool:
    left_found, left = _value(before, path)
    right_found, right = _value(after, path)
    return (
        left_found
        and right_found
        and architecture_contract_digest(left) == architecture_contract_digest(right)
    )


def _remainder(contract: dict, paths: tuple[str, ...]) -> dict:
    remaining = copy.deepcopy(contract)
    for path in paths:
        keys = _tokens(path)
        if not keys:
            return {}
        parent = remaining
        for key in keys[:-1]:
            if not isinstance(parent, dict) or key not in parent:
                break
            parent = parent[key]
        else:
            if isinstance(parent, dict):
                parent.pop(keys[-1], None)
    return remaining


def _changed_paths(before: dict, after: dict) -> tuple[list[str], bool]:
    """Bounded diagnostics; arrays are atomic, never positional fragment IDs."""
    stack = [("", before, after, 0)]
    changed: list[str] = []
    size = 0
    while stack:
        path, left, right, depth = stack.pop()
        if architecture_contract_digest(left) == architecture_contract_digest(right):
            continue
        if isinstance(left, dict) and isinstance(right, dict) and depth < 64:
            for key in sorted(left.keys() | right.keys(), reverse=True):
                child = f"{path}/{key.replace('~', '~0').replace('/', '~1')}"
                if key not in left or key not in right:
                    # Distinguish absence from an explicitly stored JSON null.
                    stack.append(
                        (
                            child,
                            [key in left, left.get(key)],
                            [key in right, right.get(key)],
                            depth + 1,
                        )
                    )
                else:
                    stack.append((child, left[key], right[key], depth + 1))
            continue
        size += len(path.encode("utf-8"))
        if len(changed) >= 100 or size > 16 * 1024:
            return changed, True
        changed.append(path)
    return changed, False


def _stored_contract(
    records: tuple[ArchitectureDecisionRecord, ...],
    *,
    spec_id: str,
    spec_edition: int,
    spec_version: int,
) -> dict:
    """Reject storage drift; never repair or pick one incompatible fragment."""
    first = records[0]
    contract = json.loads(first.source_contract_json)
    if (
        not isinstance(contract, dict)
        or architecture_contract_digest(contract) != first.source_digest
    ):
        raise ValueError("decision_contract_drift")
    if len({record.id for record in records}) != len(records):
        raise ValueError("decision_identity_drift")
    intents = []
    for record in records:
        if (
            record.spec_id != spec_id
            or record.spec_edition != spec_edition
            or type(record.spec_version) is not int
            or not 1 <= record.spec_version <= spec_version
            or record.spec_version != first.spec_version
            or record.source_digest != first.source_digest
            or record.source_contract_json != first.source_contract_json
            or record.actor_id != first.actor_id
            or record.classified_at != first.classified_at
            or not isinstance(record.id, str)
            or not record.id.strip()
            or not isinstance(record.classified_at, datetime)
            or not isinstance(record.actor_id, str)
            or not record.actor_id.strip()
            or not isinstance(record.root_design_id, str)
            or not record.root_design_id.strip()
            or not isinstance(record.interface_id, str)
            or not record.interface_id.strip()
            or record.candidate_id
            != architecture_candidate_identity(
                spec_id, record.root_design_id, record.interface_id
            )
            or not record.adopted_sources
            or record.adopted_sources != first.adopted_sources
            or any(
                not isinstance(identity, str)
                or not identity.strip()
                or type(revision) is not int
                or revision < 1
                for identity, revision in record.adopted_sources
            )
        ):
            raise ValueError("decision_scope_drift")
        intents.append(
            {
                "candidate_ref": record.candidate_id,
                "expected_source_digest": record.source_digest,
                # A persisted promotion has IR IDs, not create payloads. Validate
                # its existing bindings with the same association/scope contract.
                "disposition": "associate_existing_ir"
                if record.disposition == "promote_to_ir"
                else record.disposition,
                "integration_requirement_refs": list(
                    record.integration_requirement_ids
                ),
                "scope_paths": list(record.scope_paths),
                "reason": record.reason,
                "remainder_reason": record.remainder_reason,
            }
        )
    ArchitectureClassificationBatch.model_validate(
        {
            "expected_spec_version": first.spec_version,
            "expected_spec_edition": spec_edition,
            "idempotency_key": "stored-classification-review",
            "decisions": intents,
        }
    )
    if any(
        not _value(contract, path)[0] or _value(contract, path)[1] is None
        for record in records
        for path in record.scope_paths
    ):
        raise ValueError("decision_scope_drift")
    return contract


def architecture_classification_review(
    *,
    board_id: str,
    spec_id: str,
    spec_edition: int,
    spec_version: int,
    population: ArchitectureCandidatePopulation,
    decisions: tuple[ArchitectureDecisionRecord, ...],
    integration_requirements: tuple[Mapping[str, Any], ...],
    offset: int = 0,
    limit: int = 25,
    candidate_id: str | None = None,
    source_digest: str | None = None,
    state: ArchitectureReviewState | None = None,
) -> dict[str, Any]:
    if type(offset) is not int or not 0 <= offset <= 2**63 - 1:
        raise ArchitectureClassificationReadError(
            "architecture_classification_invalid_offset"
        )
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ArchitectureClassificationReadError(
            "architecture_classification_invalid_limit"
        )
    if state is not None and state not in REVIEW_STATES:
        raise ArchitectureClassificationReadError(
            "architecture_classification_invalid_state"
        )
    if bool(candidate_id) != bool(source_digest):
        raise ArchitectureClassificationReadError(
            "architecture_classification_identity_and_digest_required"
        )
    by_candidate: dict[str, list] = defaultdict(list)
    by_decision: dict[str, list] = defaultdict(list)
    global_issues = {
        issue.code for issue in population.issues if issue.candidate_id is None
    }
    for candidate in population.candidates:
        if candidate.spec_id != spec_id or candidate.spec_edition != spec_edition:
            raise ArchitectureClassificationReadError(
                "architecture_classification_scope_unavailable"
            )
        by_candidate[candidate.id].append(candidate)
    for record in decisions:
        if (
            record.spec_id != spec_id
            or record.spec_edition != spec_edition
            or not isinstance(record.candidate_id, str)
            or not record.candidate_id.strip()
        ):
            global_issues.add("architecture_classification_history_unavailable")
            continue  # Do not expose identifiers from a corrupt foreign row.
        by_decision[record.candidate_id].append(record)
    irs: dict[str, list] = defaultdict(list)
    for ir in integration_requirements:
        identity = ir.get("id")
        if isinstance(identity, str) and identity.strip():
            irs[identity].append(ir)

    rows = []
    for identity in sorted(by_candidate.keys() | by_decision.keys()):
        variants = by_candidate[identity]
        records = tuple(by_decision[identity])
        issue_codes = {
            issue.code for issue in population.issues if issue.candidate_id == identity
        }
        stored = None
        if records:
            try:
                stored = _stored_contract(
                    records,
                    spec_id=spec_id,
                    spec_edition=spec_edition,
                    spec_version=spec_version,
                )
            except (ValueError, TypeError, AttributeError):
                issue_codes.add("architecture_classification_history_unavailable")
        if records and stored is None and not variants:
            global_issues.add("architecture_classification_history_unavailable")
            continue
        representative = variants[0] if variants else records[0]
        row = {
            "candidate_id": identity,
            "root_design_id": representative.root_design_id,
            "interface_id": representative.interface_id,
            "current_source_digest": variants[0].source_digest
            if len(variants) == 1
            else None,
            "analyzed_source_digest": records[0].source_digest
            if stored is not None
            else None,
            "source_variant_count": len(variants),
            "source_digests": [item.source_digest for item in variants[:25]],
            "source_digests_truncated": len(variants) > 25,
            "decision_count": len(records) if stored is not None else None,
            "dispositions": sorted({item.disposition for item in records})
            if stored is not None
            else [],
            "name": variants[0].contract.get("name")
            if len(variants) == 1
            else stored.get("name")
            if stored
            else None,
        }
        decision_states = []
        remainder_state = None
        if not population.source_complete:
            row_state = "unavailable"
            issue_codes.add("architecture_sources_unavailable")
        elif issue_codes or len(variants) > 1:
            row_state = "unresolved"
            issue_codes.add("architecture_contract_unresolved")
        elif not variants:
            row_state = "unavailable" if global_issues else "retired"
            issue_codes.add(
                "architecture_sources_unavailable"
                if global_issues
                else "architecture_candidate_retired"
            )
        elif not records:
            row_state = "pending"
            issue_codes.add("architecture_candidates_pending")
        else:
            current = variants[0].contract
            paths = tuple(path for record in records for path in record.scope_paths)
            for record in records:
                valid_refs = all(
                    len(irs[ref]) == 1
                    and irs[ref][0].get("status", "active") == "active"
                    for ref in record.integration_requirement_ids
                )
                scope_current = all(
                    _same_scope(stored, current, path) for path in record.scope_paths
                )
                decision_states.append(
                    "current" if scope_current and valid_refs else "review_required"
                )
                if not valid_refs:
                    issue_codes.add("architecture_classification_ir_not_active_in_spec")
                if not scope_current:
                    issue_codes.add("architecture_candidate_outdated")
            if "" not in paths:
                remainder_current = architecture_contract_digest(
                    _remainder(stored, paths)
                ) == architecture_contract_digest(_remainder(current, paths))
                remainder_state = "current" if remainder_current else "review_required"
                if not remainder_current:
                    issue_codes.add("architecture_candidate_outdated")
            row_state = (
                "review_required"
                if "review_required" in decision_states
                or remainder_state == "review_required"
                else "current"
            )
        row.update(
            state=row_state, issues=sorted(issue_codes), remainder_state=remainder_state
        )
        if identity == candidate_id:
            chosen = [item for item in variants if item.source_digest == source_digest]
            if len(chosen) != 1 and not (
                not variants
                and stored is not None
                and records[0].source_digest == source_digest
            ):
                raise ArchitectureClassificationReadError(
                    "architecture_candidate_source_changed"
                )
            row["current_contract"] = chosen[0].contract if chosen else None
            row["promotion_suggestion"] = (
                architecture_promotion_suggestion(chosen[0].contract)
                if len(variants) == 1
                and chosen
                and row_state in {"pending", "current", "review_required"}
                else None
            )
            row["analyzed_contract"] = stored
            row["decisions"] = (
                [
                    {
                        "decision_id": record.id,
                        "spec_version": record.spec_version,
                        "actor_id": record.actor_id,
                        "classified_at": record.classified_at.isoformat(),
                        "disposition": record.disposition,
                        "integration_requirement_refs": list(
                            record.integration_requirement_ids
                        ),
                        "scope_paths": list(record.scope_paths),
                        "reason": record.reason,
                        "remainder_reason": record.remainder_reason,
                        "state": decision_states[index]
                        if decision_states
                        else row_state,
                        "adopted_sources": [
                            {"design_id": design_id, "revision": revision}
                            for design_id, revision in record.adopted_sources
                        ],
                    }
                    for index, record in enumerate(records)
                ]
                if stored is not None
                else []
            )
            changed, truncated = (
                _changed_paths(stored, chosen[0].contract)
                if stored is not None and len(chosen) == 1
                else ([], False)
            )
            row["changed_paths"], row["changed_paths_truncated"] = changed, truncated
        rows.append(row)

    counts = Counter(row["state"] for row in rows)
    blocking_ids = [row["candidate_id"] for row in rows if row["state"] not in {"current", "retired"}]
    enumeration_complete = population.source_complete and not global_issues
    selected = [row for row in rows if state is None or row["state"] == state]
    if candidate_id:
        selected = [row for row in rows if row["candidate_id"] == candidate_id]
        if not selected:
            raise ArchitectureClassificationReadError(
                "architecture_candidate_source_changed"
            )
    complete = (
        enumeration_complete
        and not population.issues
        and not any(
            counts[item] for item in REVIEW_STATES if item not in {"current", "retired"}
        )
    )
    return {
        "contract_version": "architecture-classification-review/v1",
        "board_id": board_id,
        "spec_id": spec_id,
        "spec_edition": spec_edition,
        "spec_version": spec_version,
        "source_complete": population.source_complete,
        "enumeration_complete": enumeration_complete,
        "classification_complete": complete,
        # Global diagnostics precede presentation filters/pagination. Bounded
        # IDs never substitute for source completeness or admission evaluation.
        "blocking_candidate_ids": blocking_ids[:25],
        "blocking_candidate_count": len(blocking_ids),
        "blocking_candidates_truncated": len(blocking_ids) > 25,
        "admission_evaluated": False,
        "semantic_review_evaluated": False,
        "rollout_evaluated": False,
        "observed_total": len(rows),
        "total": len(selected) if enumeration_complete else None,
        "state_counts": {item: counts[item] for item in REVIEW_STATES},
        "counts_scope": "complete" if enumeration_complete else "observed",
        "issues": sorted(global_issues),
        "state_filter": state,
        "profile": "detail" if candidate_id else "summary",
        "offset": 0 if candidate_id else offset,
        "limit": 1 if candidate_id else limit,
        "has_more": not candidate_id and offset + limit < len(selected),
        "items": selected if candidate_id else selected[offset : offset + limit],
    }
