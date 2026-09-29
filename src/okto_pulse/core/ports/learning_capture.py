"""Durable Learning capture format; structural validity grants no admission.

The existing cognitive source fingerprint seals this entire payload. Only an
authorized application writer may attest source/evidence currentness. A capture
is not a literal graph node and cannot be restored as canonical knowledge.
"""

from datetime import datetime
from dataclasses import dataclass, field
import json
import re
from typing import Protocol, runtime_checkable
from urllib.parse import quote, unquote

from okto_pulse.core.ports.kg_cognitive_source import CognitiveSourceRecord

LEARNING_CAPTURE_FORMAT = 'learning-capture/v1'
LEARNING_SCOPED_CAPTURE_FORMAT = 'learning-capture/v2'
LEARNING_CAPTURE_MAX_BYTES = 256 * 1024


class LearningCaptureTargetConflict(ValueError):
    """A stale explicit target; current identity is server-observed, not authority."""
    def __init__(self, current: CognitiveSourceRecord | None):
        super().__init__('learning_capture_target_changed')
        self.current_target = (None if current is None else {
            'learning_id': current.node_id, 'generation': current.generation,
            'source_revision': current.source_revision, 'fingerprint': current.record_fingerprint})


@dataclass(frozen=True, slots=True)
class LearningCaptureHistoryPage:
    records: tuple[CognitiveSourceRecord, ...]
    next_cursor: str | None


@runtime_checkable
class LearningCaptureIdentityReservation(Protocol):
    async def reserve_capture_identity_in_context(
        self, context: object, *, board_id: str, author_id: str, capture_id: str,
    ) -> CognitiveSourceRecord | None:
        """Serialize one author request identity through the caller's UOW.

        Audit complete histories of matching captures across target identities.
        Return the one original capture, or None while retaining the reservation
        until caller commit/rollback. Duplicate or corrupt identities fail.
        This creates no source, graph node, policy or separate memory store.
        """
        ...


@runtime_checkable
class LearningCaptureHistoryReader(Protocol):
    async def read_capture_history_in_context(
        self, context: object, *, board_id: str, bug_id: str,
        cursor: str | None = None, limit: int = 20,
    ) -> LearningCaptureHistoryPage:
        """Read a page of source identities containing captures of this Bug.

        Verify every revision of each selected identity, including revisions
        not returned as captures. Preserve older captures when the head changes.
        Return only matching capture records, never unrelated Board cognition.
        Bound identity count (1..50), revision count (200) and returned payload
        bytes (8 MiB); fail explicitly rather than truncate a history. Cursor is
        a stable ordered identity selector, not authorization or a snapshot.
        No graph, commit, repair or inference of semantic applicability.
        """
        ...


@dataclass(frozen=True, slots=True)
class LearningCaptureIntent:
    kind: str = 'create'
    target_node_id: str | None = None
    target_generation: int | None = None
    expected_fingerprint: str | None = None
    reason: str | None = None
    # Explicitly replaces applicability for this capture's source Bug only.
    # None preserves the original v1 format; legacy supersede captures do not
    # acquire a scope or new historical effect merely by being read.
    scope: str | None = None

    def __post_init__(self):
        if self.kind == 'create':
            valid = all(value is None for value in (self.target_node_id, self.target_generation,
                self.expected_fingerprint, self.reason))
        else:
            valid = (self.kind in ('reuse', 'supersede') and _text(self.target_node_id)
                and type(self.target_generation) is int and self.target_generation >= 0
                and _digest(self.expected_fingerprint) and _text(self.reason, 16384))
        valid = valid and (self.scope is None or (self.kind == 'supersede' and self.scope == 'source_bug'))
        if not valid:
            raise ValueError('learning_capture_intent_invalid')


@dataclass(frozen=True, slots=True)
class CreateLearningCapture:
    board_id: str
    bug_id: str
    capture_id: str
    expected_source_digest: str
    expected_source_version: int
    content: str
    context: str
    applicability: str
    scenario_ids: tuple[str, ...]
    intent: LearningCaptureIntent = field(default_factory=LearningCaptureIntent)

    def __post_init__(self):
        if (any(not _text(value) for value in (self.board_id, self.bug_id, self.capture_id))
                or not _digest(self.expected_source_digest)
                or type(self.expected_source_version) is not int or self.expected_source_version < 1
                or any(not _text(value, 65536) for value in (self.content, self.context, self.applicability))
                or type(self.scenario_ids) is not tuple or not 1 <= len(self.scenario_ids) <= 128
                or any(not _text(value) for value in self.scenario_ids)
                or len(set(self.scenario_ids)) != len(self.scenario_ids)
                or type(self.intent) is not LearningCaptureIntent):
            raise ValueError('learning_capture_request_invalid')


def _text(value, limit=4096):
    return type(value) is str and bool(value.strip()) and len(value) <= limit


def _digest(value):
    return type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None


def learning_capture_intent_payload(intent: LearningCaptureIntent) -> dict:
    """Preserve v1 bytes/meaning when the caller has not declared a scope."""
    value = dict(kind=intent.kind, target_node_id=intent.target_node_id,
        target_generation=intent.target_generation, expected_fingerprint=intent.expected_fingerprint,
        reason=intent.reason)
    if intent.scope is not None:
        value['scope'] = intent.scope
    return value


@dataclass(frozen=True, slots=True)
class LearningCaptureSourceRef:
    """Reference to durable authored intent, not standalone supersedence proof.

    A target revision may cite this source alongside its prior evidence. The
    domain must resolve, within the enclosing Board, the exact capture, target fingerprint, scope and its
    committed successor before assigning any effective relationship. Parsing
    this reference grants no authority and infers no applicability.
    """
    node_id: str
    generation: int
    fingerprint: str

    def __post_init__(self):
        if (not _text(self.node_id) or type(self.generation) is not int
                or self.generation < 0 or not _digest(self.fingerprint)):
            raise ValueError('learning_capture_source_ref_invalid')

    def encode(self) -> str:
        return f'learning-capture-source/v1:{quote(self.node_id, safe="")}:{self.generation}:{self.fingerprint}'


def parse_learning_capture_source_ref(value: str) -> LearningCaptureSourceRef | None:
    prefix = 'learning-capture-source/v1:'
    if type(value) is not str or not value.startswith('learning-capture-source/'):
        return None
    if not value.startswith(prefix) or len(value) > 16384:
        raise ValueError('learning_capture_source_ref_invalid')
    parts = value[len(prefix):].split(':')
    if len(parts) != 3 or not re.fullmatch(r'0|[1-9][0-9]*', parts[1]):
        raise ValueError('learning_capture_source_ref_invalid')
    reference = LearningCaptureSourceRef(unquote(parts[0]), int(parts[1]), parts[2])
    if reference.encode() != value:
        raise ValueError('learning_capture_source_ref_invalid')
    return reference


def validate_learning_capture_payload(payload, *, board_id, node_type, node_id, generation, evidence_refs):
    """Return whether this is a capture, rejecting malformed/unknown formats.

    Legacy property maps have no ``capture_format`` key and are left unchanged.
    No authority, truth, evidence admission or permission follows from this
    validation. The caller must also verify the enclosing source fingerprint.
    """
    if type(payload) is not dict or 'capture_format' not in payload:
        return False
    invalid = ValueError('learning_capture_payload_invalid')
    if (set(payload) != {'capture_format', 'capture_id', 'author_id', 'captured_at',
            'content', 'context', 'applicability', 'source', 'intent'}
            or payload['capture_format'] not in (LEARNING_CAPTURE_FORMAT, LEARNING_SCOPED_CAPTURE_FORMAT)
            or node_type != 'Learning' or not _text(node_id)
            or type(generation) is not int or generation < 0
            or not _text(payload['capture_id']) or not _text(payload['author_id'])
            or any(not _text(payload[name], 65536) for name in ('content', 'context', 'applicability'))):
        raise invalid
    stamp = payload['captured_at']
    try:
        if (type(stamp) is not str or len(stamp) > 40 or 'T' not in stamp
                or datetime.fromisoformat(stamp.replace('Z', '+00:00')).utcoffset() is None):
            raise invalid
    except (ValueError, TypeError) as exc:
        raise invalid from exc
    source, intent = payload['source'], payload['intent']
    if (type(source) is not dict or set(source) != {'board_id', 'bug_id', 'policy_version', 'digest', 'evidence_refs'}
            or not _text(board_id) or source['board_id'] != board_id or not _text(source['bug_id'])
            or type(source['policy_version']) is not int or source['policy_version'] < 1
            or not _digest(source['digest']) or type(source['evidence_refs']) is not list
            or not 1 <= len(source['evidence_refs']) <= 128
            or any(not _text(ref) for ref in source['evidence_refs'])
            or len(set(source['evidence_refs'])) != len(source['evidence_refs'])
            or type(evidence_refs) not in (list, tuple)
            or tuple(source['evidence_refs']) != tuple(evidence_refs)):
        raise invalid
    scoped = payload['capture_format'] == LEARNING_SCOPED_CAPTURE_FORMAT
    intent_keys = {'kind', 'target_node_id', 'target_generation', 'expected_fingerprint', 'reason'}
    if scoped:
        intent_keys.add('scope')
    if type(intent) is not dict or set(intent) != intent_keys:
        raise invalid
    if scoped and (intent['kind'] != 'supersede' or intent['scope'] != 'source_bug'):
        raise invalid
    if intent['kind'] == 'create':
        if any(intent[key] is not None for key in ('target_node_id', 'target_generation', 'expected_fingerprint', 'reason')):
            raise invalid
    elif intent['kind'] in ('reuse', 'supersede'):
        if (not _text(intent['target_node_id']) or type(intent['target_generation']) is not int
                or intent['target_generation'] < 0 or not _digest(intent['expected_fingerprint'])
                or not _text(intent['reason'], 16384)):
            raise invalid
        same_identity = (intent['target_node_id'], intent['target_generation']) == (node_id, generation)
        if same_identity != (intent['kind'] == 'reuse'):
            raise invalid
    else:
        raise invalid
    try:
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')
    except (TypeError, ValueError, UnicodeError) as exc:
        raise invalid from exc
    if len(encoded) > LEARNING_CAPTURE_MAX_BYTES:
        raise ValueError('learning_capture_payload_limit')
    return True
