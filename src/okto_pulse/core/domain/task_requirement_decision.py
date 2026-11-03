"""Human decisions about a task's current responsibility, separate from proof."""
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from okto_pulse.core.domain.delivery_inventory import delivery_digest

RequirementDecision = Literal['required', 'optional', 'waived']
DECISIONS = frozenset({'required', 'optional', 'waived'})


def _text(value, name, maximum=512):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError('task_requirement_' + name + '_invalid')
    return value


def _digest(value):
    if not isinstance(value, str) or len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
        raise ValueError('task_requirement_digest_invalid')
    return value


def normalize_user_note(value):
    if value is not None and (not isinstance(value, str) or len(value) > 20000):
        raise ValueError('task_requirement_note_invalid')
    return value.strip() or None if value is not None else None


@dataclass(frozen=True, slots=True)
class TaskRequirementScope:
    board_id: str
    spec_id: str
    edition: int
    card_id: str
    card_type: Literal['normal', 'bug', 'test']
    # (obligation/scenario ref, definition digest, contribution digest).
    contributions: tuple[tuple[str, str, str], ...]

    def __post_init__(self):
        for name in ('board_id', 'spec_id', 'card_id'):
            _text(getattr(self, name), name)
        if type(self.edition) is not int or self.edition < 1:
            raise ValueError('task_requirement_edition_invalid')
        if self.card_type not in {'normal', 'bug', 'test'}:
            raise ValueError('task_requirement_card_type_invalid')
        if not isinstance(self.contributions, tuple) or not 1 <= len(self.contributions) <= 5000:
            raise ValueError('task_requirement_scope_unavailable')
        refs = []
        for item in self.contributions:
            if not isinstance(item, tuple) or len(item) != 3:
                raise ValueError('task_requirement_scope_invalid')
            refs.append(_text(item[0], 'obligation'))
            _digest(item[1]); _digest(item[2])
        if len(set(refs)) != len(refs) or tuple(sorted(self.contributions)) != self.contributions:
            raise ValueError('task_requirement_scope_invalid')

    @property
    def digest(self):
        return delivery_digest(self.to_dict())

    def to_dict(self):
        return dict(board_id=self.board_id, spec_id=self.spec_id, edition=self.edition,
                    card_id=self.card_id, card_type=self.card_type,
                    contributions=[list(row) for row in self.contributions])

    @classmethod
    def from_dict(cls, value):
        if not isinstance(value, dict) or set(value) != {
            'board_id', 'spec_id', 'edition', 'card_id', 'card_type', 'contributions',
        } or not isinstance(value['contributions'], list):
            raise ValueError('task_requirement_scope_invalid')
        return cls(**{**value, 'contributions': tuple(tuple(row) for row in value['contributions'])})


def task_requirement_scope(*, inventory, board_id, spec_id, edition, card,
                           scenario_digests: Mapping[str, str] | None = None):
    """Use complete native allocation; never broaden a task to an obligation ID."""
    if (not isinstance(card, Mapping) or card.get('board_id') != board_id
            or card.get('spec_id') != spec_id or card.get('archived') is not False):
        raise ValueError('task_requirement_card_not_found')
    identity, kind = card.get('id'), card.get('card_type')
    _text(identity, 'card_id')
    entries = []
    if kind == 'test':
        scenarios = card.get('test_scenario_ids')
        if (not isinstance(scenarios, (list, tuple)) or not scenarios
                or any(not isinstance(item, str) or not item.strip() for item in scenarios)
                or len(set(scenarios)) != len(scenarios) or scenario_digests is None):
            raise ValueError('task_requirement_scope_unavailable')
        for scenario in scenarios:
            definition = _digest(scenario_digests.get(scenario))
            entries.append(('scenario:' + scenario, definition, delivery_digest({
                'board_id': board_id, 'spec_id': spec_id, 'edition': edition,
                'card_id': identity, 'scenario_id': scenario, 'definition': definition,
            })))
    elif kind in {'normal', 'bug'}:
        if inventory.population_complete is not True or inventory.issues:
            raise ValueError('task_requirement_scope_unavailable')
        for row in inventory.card_obligations(identity):
            owned = [fact for fact in row.contributions if fact.card_id == identity]
            if row.blockers or len(owned) != 1:
                raise ValueError('task_requirement_scope_unavailable')
            entries.append((row.binding.obligation_ref, row.binding.semantic_sha256, owned[0].scope_sha256))
    else:
        raise ValueError('task_requirement_card_type_invalid')
    return TaskRequirementScope(board_id, spec_id, edition, identity, kind, tuple(sorted(entries)))


@dataclass(frozen=True, slots=True)
class TaskRequirementDecisionRecord:
    id: str
    operation_id: str
    scope: TaskRequirementScope
    revision: int
    decision: RequirementDecision
    previous_decision: RequirementDecision
    supersedes: str | None
    actor_id: str
    recorded_at: datetime
    user_note: str | None = None

    def __post_init__(self):
        for name in ('id', 'operation_id', 'actor_id'):
            _text(getattr(self, name), name)
        if not isinstance(self.scope, TaskRequirementScope):
            raise ValueError('task_requirement_scope_invalid')
        if (not isinstance(self.decision, str) or self.decision not in DECISIONS
                or not isinstance(self.previous_decision, str) or self.previous_decision not in DECISIONS):
            raise ValueError('task_requirement_decision_invalid')
        if type(self.revision) is not int or self.revision < 1:
            raise ValueError('task_requirement_revision_invalid')
        if ((self.revision == 1 and (self.supersedes is not None or self.previous_decision != 'required'))
                or (self.revision > 1 and not self.supersedes)):
            raise ValueError('task_requirement_history_invalid')
        if self.supersedes is not None:
            _text(self.supersedes, 'supersedes')
        if (not isinstance(self.recorded_at, datetime) or self.recorded_at.tzinfo is None
                or self.recorded_at.utcoffset() is None):
            raise ValueError('task_requirement_timestamp_invalid')
        object.__setattr__(self, 'user_note', normalize_user_note(self.user_note))

    def to_dict(self):
        return dict(contract_version='task-requirement-decision/v1', id=self.id,
                    operation_id=self.operation_id, scope=self.scope.to_dict(),
                    scope_sha256=self.scope.digest, revision=self.revision,
                    decision=self.decision, previous_decision=self.previous_decision,
                    supersedes=self.supersedes, actor_id=self.actor_id,
                    recorded_at=self.recorded_at.isoformat(), user_note=self.user_note)

    @classmethod
    def from_dict(cls, value):
        fields = {'contract_version', 'id', 'operation_id', 'scope', 'scope_sha256',
                  'revision', 'decision', 'previous_decision', 'supersedes',
                  'actor_id', 'recorded_at', 'user_note'}
        if not isinstance(value, dict) or set(value) != fields or value['contract_version'] != 'task-requirement-decision/v1':
            raise ValueError('task_requirement_record_invalid')
        scope = TaskRequirementScope.from_dict(value['scope'])
        if value['scope_sha256'] != scope.digest:
            raise ValueError('task_requirement_record_invalid')
        data = {key: item for key, item in value.items() if key not in {'contract_version', 'scope_sha256'}}
        return cls(**{**data, 'scope': scope, 'recorded_at': datetime.fromisoformat(value['recorded_at'])})


@dataclass(frozen=True, slots=True)
class ResolvedTaskRequirement:
    decision: RequirementDecision
    record: TaskRequirementDecisionRecord | None
    history_head: TaskRequirementDecisionRecord | None

    @property
    def required(self):
        return self.decision == 'required'


def resolve_task_requirement(scope, history):
    """Only an intact current head can remove responsibility; no old fallback."""
    rows = tuple(history)
    if any(not isinstance(row, TaskRequirementDecisionRecord) or
           (row.scope.board_id, row.scope.spec_id, row.scope.edition, row.scope.card_id) !=
           (scope.board_id, scope.spec_id, scope.edition, scope.card_id) for row in rows):
        raise ValueError('task_requirement_history_invalid')
    rows = sorted(rows, key=lambda row: row.revision)
    if len({row.id for row in rows}) != len(rows):
        raise ValueError('task_requirement_history_invalid')
    previous = None
    for revision, row in enumerate(rows, 1):
        if (row.revision != revision or row.supersedes != (previous.id if previous else None)
                or row.previous_decision != (previous.decision if previous else 'required')
                or (previous and row.recorded_at < previous.recorded_at)):
            raise ValueError('task_requirement_history_invalid')
        previous = row
    current = previous if previous and previous.scope == scope else None
    return ResolvedTaskRequirement(current.decision if current else 'required', current, previous)


@dataclass(frozen=True, slots=True)
class TaskRequirementAllocation:
    obligation_ref: str
    required_card_ids: tuple[str, ...]
    optional_card_ids: tuple[str, ...]
    waived_card_ids: tuple[str, ...]
    decision_ids: tuple[str, ...]
    not_required: bool


def resolve_task_requirement_allocation(*, inventory, board_id, spec_id, edition, cards, histories):
    """Project applicability without changing allocation or granting proof.

    Every current scope is rebuilt from native contributions. Test Card choices
    affect their execution only and cannot remove another task's criterion.
    Unallocated/malformed obligations never acquire N/A from an unrelated choice.
    """
    current = {}
    selected = {card['id']: card for card in cards if isinstance(card, Mapping)
                and card.get('board_id') == board_id and card.get('spec_id') == spec_id}
    for card_id, history in histories.items():
        card = selected.get(card_id)
        if card is None or card.get('card_type') == 'test':
            continue
        scope = task_requirement_scope(inventory=inventory, board_id=board_id,
            spec_id=spec_id, edition=edition, card=card)
        current[card_id] = resolve_task_requirement(scope, history)
    result = []
    for row in inventory.rows:
        if row.family == 'decision':
            continue
        groups = {key: [] for key in DECISIONS}
        ids = []
        for contribution in row.contributions:
            resolution = current.get(contribution.card_id)
            choice = resolution.decision if resolution else 'required'
            groups[choice].append(contribution.card_id)
            if resolution and resolution.record and choice != 'required':
                ids.append(resolution.record.id)
        result.append(TaskRequirementAllocation(row.binding.obligation_ref,
            tuple(sorted(groups['required'])), tuple(sorted(groups['optional'])),
            tuple(sorted(groups['waived'])), tuple(sorted(set(ids))),
            bool(row.contributions) and not row.blockers and not groups['required']))
    return tuple(result)
