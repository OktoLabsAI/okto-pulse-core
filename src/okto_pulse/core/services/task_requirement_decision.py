"""Core policy for a human selection, preview fence and immutable successor."""
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from okto_pulse.core.domain.delivery_inventory import delivery_digest
from okto_pulse.core.domain.task_requirement_decision import (
    TaskRequirementDecisionRecord, resolve_task_requirement,
)


def task_requirement_scenario_digest(*, board_id, spec_id, scenario, acceptance_criteria):
    """Bind the complete native, namespaced Evidence V2 scenario contract.

    The native contract is `sha256:<hex>`, while responsibility scopes use
    unprefixed digests. Hash the namespaced value; never truncate or reinterpret
    an arbitrary source digest supplied by a caller.
    """
    from okto_pulse.core.services.test_scenario_lifecycle import compute_test_scenario_semantic_sha256
    return delivery_digest({'native_scenario_semantic_sha256': compute_test_scenario_semantic_sha256(
        board_id=board_id, spec_id=spec_id, scenario=scenario, acceptance_criteria=acceptance_criteria)})


@dataclass(frozen=True)
class TaskRequirementDecisionState:
    board_id: str
    spec_id: str
    edition: int
    spec_version: int
    spec_status: str
    inventory: object
    cards: tuple[dict, ...]
    histories: dict
    scenario_digests: dict
    dependencies: tuple[dict, ...]
    authority_sha256: str


def task_requirement_preview(state, *, card_ids, actor_id, allowed_decisions):
    if len(set(card_ids)) != len(card_ids) or not 1 <= len(card_ids) <= 50:
        raise ValueError('task_requirement_selection_invalid')
    cards = {card['id']: card for card in state.cards}
    if not set(card_ids) <= cards.keys():
        raise ValueError('task_requirement_card_not_found')
    rows, scopes, heads = [], {}, []
    for identity in card_ids:
        card = cards[identity]
        if card.get('spec_id') is None:
            raise ValueError('task_requirement_unsupported_scope')
        scope = state.inventory.task_requirement_scope(board_id=state.board_id, spec_id=state.spec_id,
            edition=state.edition, card=card, scenario_digests=state.scenario_digests)
        resolved = resolve_task_requirement(scope, state.histories.get(identity, ()))
        scopes[identity] = scope
        frozen = state.spec_status in {'done', 'cancelled'} or card['status'] in {'done', 'cancelled', 'validation', 'rejected'}
        rows.append(dict(card_id=identity, title=card['title'], lifecycle_status=card['status'],
            decision=resolved.decision, decision_id=resolved.record.id if resolved.record else None,
            scope_sha256=scope.digest, mutable=not frozen,
            reason_code='reopen_required' if frozen else None,
            history_count=len(state.histories.get(identity, ())),
            user_note=resolved.record.user_note if resolved.record else None))
        heads.append(resolved.history_head.to_dict() if resolved.history_head else None)
    basis = delivery_digest(dict(contract_version='task-requirement-basis/v1', actor_id=actor_id,
        board_id=state.board_id, spec_id=state.spec_id, edition=state.edition,
        spec_version=state.spec_version, spec_status=state.spec_status,
        inventory=state.inventory.snapshot_sha256, cards=state.cards,
        scopes=[scopes[identity].to_dict() for identity in card_ids], heads=heads,
        dependencies=state.dependencies, authority=state.authority_sha256))
    return dict(contract_version='task-requirement-options/v1', board_id=state.board_id,
        spec_id=state.spec_id, edition=state.edition, spec_version=state.spec_version, basis=basis,
        cards=rows, allowed_decisions=list(allowed_decisions), note_optional=True,
        can_apply=bool(allowed_decisions) and all(row['mutable'] for row in rows),
        effects=dict(lifecycle_unchanged=True, proof_credit=False,
            dependencies_unchanged=True, dependent_tasks=list(state.dependencies),
            test_scenarios_remain_governed=any(card['card_type'] == 'test' for card in state.cards))), scopes


def prepare_task_requirement_decision(state, command, *, actor_id, actor_kind,
                                      now=None, id_factory=None):
    if actor_kind not in {'user', 'human'}:
        raise ValueError('task_requirement_human_authorization_required')
    preview, scopes = task_requirement_preview(state, card_ids=command.card_ids,
        actor_id=actor_id, allowed_decisions=(command.decision,))
    if preview['basis'] != command.basis:
        raise ValueError('task_requirement_stale_basis')
    if not preview['can_apply']:
        raise ValueError('task_requirement_reopen_required')
    now = now or datetime.now(timezone.utc)
    identify = id_factory or (lambda prefix: prefix + uuid4().hex)
    operation_id = identify('task_requirement_op_')
    records = []
    for identity in command.card_ids:
        scope = scopes[identity]
        head = resolve_task_requirement(scope, state.histories.get(identity, ())).history_head
        records.append(TaskRequirementDecisionRecord(id=identify('task_requirement_'),
            operation_id=operation_id, scope=scope, revision=head.revision + 1 if head else 1,
            decision=command.decision, previous_decision=head.decision if head else 'required',
            supersedes=head.id if head else None, actor_id=actor_id, recorded_at=now,
            user_note=command.note))
    response = dict(contract_version='task-requirement-operation/v1', operation_id=operation_id,
        status='applied', edition=state.edition, decision_ids=[record.id for record in records],
        affected_cards=list(command.card_ids), decision=command.decision, replayed=False,
        # This receipt confirms the atomic decision. Independent delivery gates
        # are deliberately not evaluated again on the human save path.
        remaining_blockers=dict(evaluated=False, count=None))
    return tuple(records), response
