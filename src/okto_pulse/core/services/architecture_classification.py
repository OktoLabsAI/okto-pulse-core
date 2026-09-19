"""One transactional coordinator for authored architecture classifications.

The use case owns board access and complete-batch authorization before entering
this service. The caller holds a write UoW through source resolution, the
structured IR preflight, the atomic store, history/events, and commit.
"""

from __future__ import annotations

import copy
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from okto_pulse.core.domain.architecture_classification import (
    ArchitectureClassificationBatch,
    ArchitectureClassificationError,
    resolve_architecture_classification,
)
from okto_pulse.core.domain.human_validation_cycle import require_draft_mutation
from okto_pulse.core.events import publish
from okto_pulse.core.events.types import (
    SpecSemanticChanged,
    SpecVersionBumped,
    StructuredSpecEntityCreated,
)
from okto_pulse.core.ports.architecture_classification import (
    ArchitectureClassificationPersistenceState as State,
    ArchitectureClassificationReceipt,
    ArchitectureDecisionRecord,
)
from okto_pulse.core.ports.permission_policy import PermissionSet
from okto_pulse.core.ports.structured_spec import get_structured_spec_store
from okto_pulse.core.services.architecture_candidates import (
    load_spec_architecture_candidates,
)
from okto_pulse.core.services.main import SpecService, resolve_actor_name
from okto_pulse.core.services.spec_structured_entities import (
    PreparedIntegrationRequirementCreates,
    StructuredSpecEntityCommand,
    StructuredSpecEntityService,
    canonical_spec_child_ref,
)


class ArchitectureClassificationService:
    def __init__(self, context: Any):
        self.context = context

    async def apply(
        self,
        *,
        board_id: str,
        spec_id: str,
        batch: ArchitectureClassificationBatch,
        actor_id: str,
        permissions: PermissionSet,
        actor_name: str | None = None,
        actor_kind: str = "unknown",
    ) -> dict[str, Any]:
        store = get_structured_spec_store()
        digest = batch.request_digest(
            board_id=board_id, spec_id=spec_id, actor_id=actor_id
        )
        existing = await store.get_architecture_classification_receipt(
            self.context,
            spec_id=spec_id,
            idempotency_key=batch.idempotency_key,
        )
        if existing is not None:
            if (
                existing.actor_id != actor_id
                or existing.board_id != board_id
                or existing.request_digest != digest
            ):
                raise ArchitectureClassificationError(
                    "architecture_classification_idempotency_conflict"
                )
            return {**copy.deepcopy(existing.result), "replayed": True}

        spec = await store.get(self.context, spec_id=spec_id)
        if spec is None or spec.board_id != board_id or spec.archived:
            raise ArchitectureClassificationError(
                "architecture_classification_spec_unavailable"
            )
        require_draft_mutation(spec, subject_type="spec")
        structured = StructuredSpecEntityService(self.context)
        command = StructuredSpecEntityCommand(
            board_id=board_id,
            spec_id=spec_id,
            actor_id=actor_id,
            entity_type="integration_requirement",
            operation="create",
            expected_spec_version=batch.expected_spec_version,
            expected_spec_edition=batch.expected_spec_edition,
            permission_set=permissions,
        )
        # Same edition/version/content-lock check as the existing writer, also
        # required for batches with only associations or context decisions.
        failure = await structured._check_semantic_fence(spec, command)
        if failure is not None:
            raise ArchitectureClassificationError(
                f"architecture_classification_{failure.error_code}"
            )
        population = await load_spec_architecture_candidates(
            self.context, board_id=board_id, spec_id=spec_id
        )
        resolved = resolve_architecture_classification(
            batch,
            spec_id=spec_id,
            population=population,
            integration_requirements=tuple(spec.integration_requirements or ()),
        )
        payloads = [
            payload
            for item in batch.decisions
            for payload in item.integration_requirements
        ]
        updates: dict[str, Any] = {}
        created_ids: tuple[str, ...] = ()
        if payloads:
            prepared = await structured.prepare_integration_requirement_creates(
                command, payloads
            )
            if not isinstance(prepared, PreparedIntegrationRequirementCreates):
                raise ArchitectureClassificationError(
                    f"architecture_classification_{prepared.error_code}"
                )
            updates = prepared.update_data
            created_ids = prepared.entity_ids

        new_version = batch.expected_spec_version + 1
        classified_at = datetime.now(UTC)
        decisions = []
        created_offset = 0
        for item in resolved:
            intent, candidate = item.intent, item.candidate
            ir_ids = intent.integration_requirement_refs
            if intent.disposition == "promote_to_ir":
                count = len(intent.integration_requirements)
                ir_ids = created_ids[created_offset : created_offset + count]
                created_offset += count
            decisions.append(
                ArchitectureDecisionRecord(
                    id=str(uuid4()),
                    spec_id=spec_id,
                    spec_edition=batch.expected_spec_edition,
                    spec_version=new_version,
                    candidate_id=candidate.id,
                    source_digest=candidate.source_digest,
                    root_design_id=candidate.root_design_id,
                    interface_id=candidate.interface_id,
                    source_contract_json=candidate.contract_json,
                    adopted_sources=candidate.adopted_sources,
                    actor_id=actor_id,
                    classified_at=classified_at,
                    disposition=intent.disposition,
                    integration_requirement_ids=tuple(ir_ids),
                    scope_paths=intent.scope_paths,
                    reason=intent.reason,
                    remainder_reason=intent.remainder_reason,
                )
            )
        result = {
            "contract_version": "architecture-classification/v1",
            "board_id": board_id,
            "spec_id": spec_id,
            "spec_edition": batch.expected_spec_edition,
            "spec_version": new_version,
            "idempotency_key": batch.idempotency_key,
            "created_ir_ids": list(created_ids),
            "decisions": [
                {
                    "decision_id": item.id,
                    "candidate_ref": item.candidate_id,
                    "source_digest": item.source_digest,
                    "disposition": item.disposition,
                    "integration_requirement_refs": list(
                        item.integration_requirement_ids
                    ),
                    "scope_paths": list(item.scope_paths),
                }
                for item in decisions
            ],
            "pending_checks": [
                "requirement_readiness_and_spec_start_gates_not_evaluated"
            ],
        }
        receipt = ArchitectureClassificationReceipt(
            board_id=board_id,
            spec_id=spec_id,
            actor_id=actor_id,
            idempotency_key=batch.idempotency_key,
            request_digest=digest,
            result=result,
        )
        old_values = {key: copy.deepcopy(getattr(spec, key)) for key in updates}
        for key, value in updates.items():
            setattr(spec, key, copy.deepcopy(value))
        spec.version = new_version
        persisted = await store.save_architecture_classification(
            self.context,
            spec,
            expected_spec_version=batch.expected_spec_version,
            expected_spec_edition=batch.expected_spec_edition,
            changed_fields=tuple(updates),
            decisions=tuple(decisions),
            receipt=receipt,
        )
        if persisted.state == State.REPLAYED:
            return {**copy.deepcopy(persisted.receipt.result), "replayed": True}
        if persisted.state != State.APPLIED:
            raise ArchitectureClassificationError(
                f"architecture_classification_{persisted.state.value}"
            )

        changed_fields = [*updates, "architecture_classifications"]
        actor_type = actor_kind if actor_kind in {"agent", "user", "system"} else "user"
        await publish(
            SpecVersionBumped(
                board_id=board_id,
                actor_id=actor_id,
                actor_type=actor_type,
                spec_id=spec_id,
                old_version=batch.expected_spec_version,
                new_version=new_version,
                changed_fields=changed_fields,
            ),
            session=self.context,
        )
        await publish(
            SpecSemanticChanged(
                board_id=board_id,
                actor_id=actor_id,
                actor_type=actor_type,
                spec_id=spec_id,
                changed_fields=changed_fields,
            ),
            session=self.context,
        )
        for ir_id in created_ids:
            await publish(
                StructuredSpecEntityCreated(
                    board_id=board_id,
                    actor_id=actor_id,
                    actor_type=actor_type,
                    spec_id=spec_id,
                    entity_type="integration_requirement",
                    entity_id=ir_id,
                    child_ref=canonical_spec_child_ref(
                        spec_id, "integration_requirement", ir_id
                    ),
                    operation="create",
                    changed_fields=["integration_requirements"],
                    spec_version=new_version,
                ),
                session=self.context,
            )
        service = SpecService(self.context)
        changes = service._compute_diff(old_values, updates, list(updates))
        changes.append(
            {
                "field": "architecture_classifications",
                "old": None,
                "new": result["decisions"],
            }
        )
        await service._record_history(
            spec_id,
            "architecture_classified",
            actor_id,
            actor_name or await resolve_actor_name(self.context, actor_id, board_id),
            actor_type=actor_type,
            changes=changes,
            summary=f"Classified {len({item.candidate_id for item in decisions})} architecture candidates",
            version=new_version,
        )
        return {**result, "replayed": False}
