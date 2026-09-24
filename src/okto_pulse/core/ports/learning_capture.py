"""Durable Learning capture format; structural validity grants no admission.

The existing cognitive source fingerprint seals this entire payload. Only an
authorized application writer may attest source/evidence currentness. A capture
is not a literal graph node and cannot be restored as canonical knowledge.
"""

from datetime import datetime
from dataclasses import dataclass
import json
import re

LEARNING_CAPTURE_FORMAT = 'learning-capture/v1'
LEARNING_CAPTURE_MAX_BYTES = 256 * 1024


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

    def __post_init__(self):
        if (any(not _text(value) for value in (self.board_id, self.bug_id, self.capture_id))
                or not _digest(self.expected_source_digest)
                or type(self.expected_source_version) is not int or self.expected_source_version < 1
                or any(not _text(value, 65536) for value in (self.content, self.context, self.applicability))
                or type(self.scenario_ids) is not tuple or not 1 <= len(self.scenario_ids) <= 128
                or any(not _text(value) for value in self.scenario_ids)
                or len(set(self.scenario_ids)) != len(self.scenario_ids)):
            raise ValueError('learning_capture_request_invalid')


def _text(value, limit=4096):
    return type(value) is str and bool(value.strip()) and len(value) <= limit


def _digest(value):
    return type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None


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
            or payload['capture_format'] != LEARNING_CAPTURE_FORMAT
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
    if type(intent) is not dict or set(intent) != {'kind', 'target_node_id', 'target_generation', 'expected_fingerprint', 'reason'}:
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
