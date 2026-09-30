"""Closed internal references for independently replayable capture work."""
from dataclasses import dataclass
import re
from urllib.parse import quote, unquote

_MARKER = ':learning:capture-v1:'
_PATTERN = re.compile(r'^bug:([^:]+):learning:capture-v1:([^:]+):(0|[1-9][0-9]*)$')
_MARKER_V2 = ':learning:capture-v2:'
_PATTERN_V2 = re.compile(r'^bug:([^:]+):learning:capture-v2:([^:]+):(0|[1-9][0-9]*):([0-9a-f]{64})$')


@dataclass(frozen=True)
class LearningCaptureWorkRef:
    bug_id: str
    learning_id: str
    generation: int
    fingerprint: str | None = None

    def encode(self) -> str:
        if (not isinstance(self.bug_id, str) or not self.bug_id.strip()
                or not isinstance(self.learning_id, str) or not self.learning_id.strip()
                or max(len(self.bug_id), len(self.learning_id)) > 4096
                or type(self.generation) is not int or self.generation < 0):
            raise ValueError('learning_capture_work_reference_invalid')
        if self.fingerprint is not None:
            if not isinstance(self.fingerprint, str) or re.fullmatch(r'[0-9a-f]{64}', self.fingerprint) is None:
                raise ValueError('learning_capture_work_reference_invalid')
            return (f'bug:{quote(self.bug_id, safe="")}{_MARKER_V2}'
                f'{quote(self.learning_id, safe="")}:{self.generation}:{self.fingerprint}')
        return f'bug:{quote(self.bug_id, safe="")}{_MARKER}{quote(self.learning_id, safe="")}:{self.generation}'


def parse_learning_capture_work_ref(value: str) -> LearningCaptureWorkRef | None:
    if _MARKER not in value and _MARKER_V2 not in value:
        return None
    version_two = _MARKER_V2 in value
    match = (_PATTERN_V2 if version_two else _PATTERN).fullmatch(value)
    if match is None or len(value) > 25000:
        raise ValueError('learning_capture_work_reference_invalid')
    bug, learning, generation = match.groups()[:3]
    result = LearningCaptureWorkRef(unquote(bug, errors='strict'), unquote(learning, errors='strict'),
        int(generation), match.group(4) if version_two else None)
    if result.encode() != value:
        raise ValueError('learning_capture_work_reference_invalid')
    return result
