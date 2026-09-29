"""Server-owned linkage of an admitted capture to one authorized Bug closeout.

No content is reauthored here. The capture remains in the cognitive source
ledger; the Card retains only transition identity and source fingerprints.
Structural validation and hashes are not signatures or lifecycle authority.
"""

from dataclasses import fields, replace
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
from okto_pulse.core.ports.bug_cognitive_context import (
    BugCognitiveContext, qualify_bug_semantic_context,
)
from okto_pulse.core.ports.kg_cognitive_source import (
    CognitiveSourceRecord, latest_cognitive_source_records,
)
from okto_pulse.core.ports.learning_capture import validate_learning_capture_payload


class LearningCaptureSelection(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True, frozen=True)
    learning_id: str = Field(min_length=1, max_length=4096)
    generation: int = Field(ge=0)
    fingerprint: str = Field(pattern=r'^[0-9a-f]{64}$')

    @field_validator('learning_id')
    @classmethod
    def meaningful_identity(cls, value):
        if not value.strip():
            raise ValueError('learning_capture_selection_invalid')
        return value


class LearningCloseoutBinding(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True, frozen=True)
    contract_version: Literal['learning-closeout-binding/v1'] = 'learning-closeout-binding/v1'
    transition_id: str = Field(min_length=1, max_length=255)
    board_id: str = Field(min_length=1, max_length=4096)
    bug_id: str = Field(min_length=1, max_length=4096)
    actor_id: str = Field(min_length=1, max_length=4096)
    bound_at: str = Field(min_length=1, max_length=40)
    capture: LearningCaptureSelection
    capture_revision: int = Field(ge=0)
    before_digest: str = Field(pattern=r'^[0-9a-f]{64}$')
    before_version: int = Field(ge=1)
    closed_digest: str = Field(pattern=r'^[0-9a-f]{64}$')
    closed_version: int = Field(ge=1)
    operation: Literal['move_card', 'submit_task_validation']
    sha256: str = Field(pattern=r'^[0-9a-f]{64}$')

    @field_validator('bound_at')
    @classmethod
    def zoned_timestamp(cls, value):
        if 'T' not in value or datetime.fromisoformat(value.replace('Z', '+00:00')).utcoffset() is None:
            raise ValueError('learning_closeout_timestamp_invalid')
        return value


def verify_learning_closeout_binding(value) -> LearningCloseoutBinding:
    binding = LearningCloseoutBinding.model_validate(value)
    if (binding.closed_version < binding.before_version
            or binding.sha256 != canonical_sha256(binding.model_dump(exclude={'sha256'}))):
        raise ValueError('learning_closeout_binding_integrity_invalid')
    return binding


def bind_learning_capture_to_closed_source(
    *, capture: CognitiveSourceRecord, before: BugCognitiveContext,
    closed: BugCognitiveContext, transition_id: str, actor_id: str,
    bound_at: datetime, operation: Literal['move_card', 'submit_task_validation'],
    appended_validations: tuple = (),
) -> LearningCloseoutBinding:
    """Link only the caller's explicit append delta and lifecycle consequence.

    The caller must have revalidated the selected capture/evidence under the
    write fence and authorized the transition. It supplies server-constructed
    review entries, never arbitrary delta fields from a client. A new conclusion
    must already be part of the capture's admitted basis, not this closing delta.
    Every other source fact must be unchanged. Persist this value with the transition
    and outbox before releasing that UOW; this pure function persists nothing.
    """
    before = qualify_bug_semantic_context(before)
    closed = qualify_bug_semantic_context(closed)
    if (not before.verified or not closed.verified or before.status == 'done'
            or closed.status != 'done' or closed.source_policy_version < before.source_policy_version):
        raise ValueError('learning_closeout_source_invalid')
    latest_cognitive_source_records((capture,))
    payload = dict(capture.payload)
    if not validate_learning_capture_payload(payload, board_id=before.board_id,
            node_type=capture.node_type, node_id=capture.node_id,
            generation=capture.generation, evidence_refs=capture.evidence_refs):
        raise ValueError('learning_closeout_capture_invalid')
    if (capture.board_id != before.board_id or payload['source']['bug_id'] != before.bug_id
            or payload['source']['digest'] != before.source_digest
            or payload['source']['policy_version'] != before.source_policy_version
            or payload['intent']['kind'] != 'create'):
        raise ValueError('learning_closeout_capture_basis_mismatch')
    if type(appended_validations) is not tuple:
        raise ValueError('learning_closeout_delta_invalid')
    if operation == 'move_card' and appended_validations:
        raise ValueError('learning_closeout_delta_invalid')
    if operation == 'submit_task_validation' and len(appended_validations) != 1:
        raise ValueError('learning_closeout_delta_invalid')
    expected = replace(before, status='done', source_policy_version=closed.source_policy_version,
        validations=before.validations + appended_validations, source_digest=None)
    expected = qualify_bug_semantic_context(expected)
    if not expected.verified or expected.source_digest != closed.source_digest:
        raise ValueError('learning_closeout_unexpected_source_change')
    # Comparing qualified fields too makes the intended allowance explicit;
    # neither a status-only comparison nor the Bug version covers related facts.
    if any(getattr(expected, field.name) != getattr(closed, field.name) for field in fields(expected)):
        raise ValueError('learning_closeout_unexpected_source_change')
    value = dict(transition_id=transition_id, board_id=before.board_id, bug_id=before.bug_id,
        actor_id=actor_id, bound_at=bound_at.isoformat(),
        capture=LearningCaptureSelection(learning_id=capture.node_id, generation=capture.generation,
            fingerprint=capture.record_fingerprint), capture_revision=capture.source_revision,
        before_digest=before.source_digest, before_version=before.source_policy_version,
        closed_digest=closed.source_digest, closed_version=closed.source_policy_version, operation=operation)
    draft = LearningCloseoutBinding(**value, sha256='0' * 64)
    return verify_learning_closeout_binding({**draft.model_dump(),
        'sha256': canonical_sha256(draft.model_dump(exclude={'sha256'}))})


def closeout_binding_is_current(value, source: BugCognitiveContext) -> bool:
    """Current applicability only; historical linkage never proves new work."""
    binding = verify_learning_closeout_binding(value)
    source = qualify_bug_semantic_context(source)
    if not source.verified:
        raise ValueError('learning_closeout_source_unavailable')
    return (source.status == 'done' and source.board_id == binding.board_id
        and source.bug_id == binding.bug_id and source.source_digest == binding.closed_digest
        and source.source_policy_version == binding.closed_version)


def append_learning_closeout_binding(history, value) -> list[dict]:
    """Append a server binding, preserving history and exact transition retry.

    No storage/lock is acquired here. The lifecycle writer must retain its
    serialization fence and persist the returned list in the same UOW.
    """
    binding = verify_learning_closeout_binding(value)
    if history is not None and type(history) is not list:
        raise ValueError('learning_closeout_history_invalid')
    records = []
    identities = set()
    replay = False
    for item in history or []:
        prior = verify_learning_closeout_binding(item)
        if (prior.board_id != binding.board_id or prior.bug_id != binding.bug_id
                or prior.transition_id in identities):
            raise ValueError('learning_closeout_history_invalid')
        identities.add(prior.transition_id)
        if prior.transition_id == binding.transition_id:
            if prior != binding:
                raise ValueError('learning_closeout_transition_conflict')
            replay = True
        records.append(prior.model_dump())
    if not replay:
        records.append(binding.model_dump())
    return records
