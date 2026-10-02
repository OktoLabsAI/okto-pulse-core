"""Seed immutable guideline revisions through native domain plans and the port.

These helpers create fixture state; permission/transport behavior is exercised
by the policy governance suites, not bypassed in product code.
"""
from datetime import timedelta
from uuid import uuid4

from okto_pulse.core.domain.guideline_lifecycle import (
    GuidelinePatchApplied, GuidelinePatchCommand, GuidelineRevisionPatch,
    GuidelineRetirementCommand, execute_guideline_patch, plan_guideline_retirement,
)
from okto_pulse.core.domain.guideline_policy import GuidelineLifecycleStatus
from okto_pulse.core.ports.relational_application import require_relational_application_adapter
from okto_pulse.core.services.main import GuidelineService


async def append_revision(db, guideline_id, actor_id, **fields):
    port = require_relational_application_adapter().guideline_policy(db)
    head = await port.get_head(guideline_id=guideline_id)
    current = await port.get_revision(guideline_id=guideline_id, revision_id=head.revision_id)
    key = str(uuid4())
    plan = execute_guideline_patch(GuidelinePatchCommand(
        current_revision=current, current_head=head, patch=GuidelineRevisionPatch(**fields),
        next_revision_id=key, actor_id=actor_id, occurred_at=head.updated_at + timedelta(seconds=1),
        idempotency_key=key,
    ), retirement=await port.get_retirement(guideline_id=guideline_id))
    assert isinstance(plan, GuidelinePatchApplied), plan
    await port.append_revision_cas(revision=plan.revision, next_head=plan.head,
        expected_head_revision=plan.expected_head_revision, idempotency_key=plan.idempotency_key,
        request_digest=plan.request_digest)
    return await GuidelineService(db).get_guideline(guideline_id, owner_id=actor_id)


async def retire_guideline(db, guideline_id, actor_id):
    port = require_relational_application_adapter().guideline_policy(db)
    head = await port.get_head(guideline_id=guideline_id)
    current = await port.get_revision(guideline_id=guideline_id, revision_id=head.revision_id)
    key = str(uuid4())
    plan = plan_guideline_retirement(GuidelineRetirementCommand(
        current_revision=current, current_head=head, retirement_id=key,
        status=GuidelineLifecycleStatus.RETIRED, reason='Explicit fixture retirement',
        actor_id=actor_id, occurred_at=head.updated_at + timedelta(seconds=1), idempotency_key=key,
    ), current_retirement=None)
    await port.retire_guideline_cas(retirement=plan.retirement,
        expected_head_revision=plan.expected_head_revision, idempotency_key=plan.idempotency_key,
        request_digest=plan.request_digest, actor_type='user')


def exact_pin(guideline, priority=0):
    return {'guideline_id': guideline.id, 'priority': priority,
        'revision_id': guideline.revision_id, 'revision_number': guideline.version,
        'semantic_version': guideline.semantic_version, 'revision_digest': guideline.revision_digest}
