"""One authored Learning submitted with an execution report.

The initial digest fences the user's observed Bug. Only the server's exact
report append and requested lifecycle change may become the capture's basis.
Neither this comparison nor a replay digest authenticates evidence or grants
permission. The writer retains the source fence and owns one atomic UOW.
"""

from dataclasses import fields, replace

from pydantic import BaseModel, ConfigDict, Field, field_validator

from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
from okto_pulse.core.domain.learning_intent import LearningIntentRequest
from okto_pulse.core.ports.bug_cognitive_context import qualify_bug_semantic_context


class LearningSubmission(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True, frozen=True)
    capture_id: str = Field(min_length=1, max_length=255)
    expected_source_digest: str = Field(pattern=r'^[0-9a-f]{64}$')
    expected_source_version: int = Field(ge=1)
    content: str = Field(min_length=1, max_length=65536)
    context: str = Field(min_length=1, max_length=65536)
    applicability: str = Field(min_length=1, max_length=65536)
    scenario_ids: list[str] = Field(min_length=1, max_length=128)
    # None is omitted from legacy request digests (exclude_none=True).
    intent: LearningIntentRequest | None = None

    @field_validator('capture_id', 'content', 'context', 'applicability')
    @classmethod
    def meaningful_text(cls, value):
        if not value.strip():
            raise ValueError('learning_submission_invalid')
        return value

    @field_validator('scenario_ids')
    @classmethod
    def unique_scenarios(cls, value):
        if len(set(value)) != len(value) or any(not item.strip() or len(item) > 4096 for item in value):
            raise ValueError('learning_submission_invalid')
        return value


class LearningSubmissionReceipt(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True, frozen=True)
    contract_version: str = Field(pattern=r'^learning-submission/v1$', default='learning-submission/v1')
    capture_id: str = Field(min_length=1, max_length=255)
    request_digest: str = Field(pattern=r'^[0-9a-f]{64}$')
    from_status: str = Field(min_length=1, max_length=40)
    to_status: str = Field(pattern=r'^(validation|done)$')


def learning_submission_request_digest(*, board_id, bug_id, actor_id, move):
    return canonical_sha256({'board_id': board_id, 'bug_id': bug_id,
        'actor_id': actor_id, 'move': move.model_dump(mode='json', exclude_none=True)})


def learning_submission_replay(conclusions, *, actor_id, capture_id, request_digest):
    """Recognize the server-owned receipt persisted with the compound write.

    Replay returns current Card state without repeating mutation or claiming
    applicability to a later edition. Receipt presence is not Learning proof.
    """
    matches = []
    for row in conclusions or ():
        if not isinstance(row, dict) or row.get('author_id') != actor_id or 'learning_submission' not in row:
            continue
        receipt = LearningSubmissionReceipt.model_validate(row['learning_submission'])
        if receipt.capture_id == capture_id:
            matches.append(receipt)
    if len(matches) > 1:
        raise ValueError('learning_submission_history_invalid')
    if matches and matches[0].request_digest != request_digest:
        raise ValueError('learning_submission_idempotency_conflict')
    return matches[0] if matches else None


def qualify_learning_submission_basis(*, initial, captured, conclusion, capture_status):
    """Accept only the explicit report/status delta under the caller's fence."""
    initial = qualify_bug_semantic_context(initial)
    captured = qualify_bug_semantic_context(captured)
    if (not initial.verified or not captured.verified or initial.status == 'done'
            or capture_status not in {initial.status, 'validation'}
            or captured.source_policy_version < initial.source_policy_version
            or type(conclusion) is not dict):
        raise ValueError('learning_submission_source_invalid')
    expected = qualify_bug_semantic_context(replace(initial, status=capture_status,
        conclusions=initial.conclusions + (conclusion,),
        source_policy_version=captured.source_policy_version, source_digest=None))
    if (not expected.verified or expected.source_digest != captured.source_digest
            or any(getattr(expected, item.name) != getattr(captured, item.name) for item in fields(expected))):
        raise ValueError('learning_submission_unexpected_source_change')
    return captured
