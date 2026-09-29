"""Closed internal references for independently replayable capture work."""
from dataclasses import dataclass
import re
from urllib.parse import quote, unquote

_MARKER = ':learning:capture-v1:'
_PATTERN = re.compile(r'^bug:([^:]+):learning:capture-v1:([^:]+):(0|[1-9][0-9]*)$')


@dataclass(frozen=True)
class LearningCaptureWorkRef:
    bug_id: str
    learning_id: str
    generation: int

    def encode(self) -> str:
        if (not isinstance(self.bug_id, str) or not self.bug_id.strip()
                or not isinstance(self.learning_id, str) or not self.learning_id.strip()
                or max(len(self.bug_id), len(self.learning_id)) > 4096
                or type(self.generation) is not int or self.generation < 0):
            raise ValueError('learning_capture_work_reference_invalid')
        return f'bug:{quote(self.bug_id, safe="")}{_MARKER}{quote(self.learning_id, safe="")}:{self.generation}'


def parse_learning_capture_work_ref(value: str) -> LearningCaptureWorkRef | None:
    if _MARKER not in value:
        return None
    match = _PATTERN.fullmatch(value)
    if match is None or len(value) > 25000:
        raise ValueError('learning_capture_work_reference_invalid')
    bug, learning, generation = match.groups()
    result = LearningCaptureWorkRef(unquote(bug, errors='strict'), unquote(learning, errors='strict'), int(generation))
    if result.encode() != value:
        raise ValueError('learning_capture_work_reference_invalid')
    return result
