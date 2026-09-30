"""Source-reference diagnostics; these records confer no delivery authority.

Identity excludes the current reason and source fingerprint: reevaluation can
change or close a finding without inventing a second defect for the same link.
"""
from dataclasses import dataclass
import hashlib
import json


def _text(value, name):
    if (type(value) is not str or not value or value.strip() != value
            or any(ord(character) < 32 or ord(character) == 127 for character in value)):
        raise ValueError('projection_finding_' + name + '_invalid')


@dataclass(frozen=True, slots=True)
class ProjectionReferenceFinding:
    board_id: str
    owner_type: str
    owner_id: str
    namespace: str
    source_selector: str
    target_ref: str | None
    reason_code: str

    def __post_init__(self):
        for name in ('board_id', 'owner_type', 'owner_id', 'namespace', 'source_selector', 'reason_code'):
            _text(getattr(self, name), name)
        if self.target_ref is not None:
            _text(self.target_ref, 'target_ref')

    @property
    def finding_id(self) -> str:
        identity = ['projection-reference-finding/v1', self.board_id, self.owner_type,
                    self.owner_id, self.namespace, self.source_selector, self.target_ref]
        return hashlib.sha256(json.dumps(identity, separators=(',', ':'), ensure_ascii=True).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class ProjectionFindingSnapshot:
    """A complete source evaluation, including an explicitly empty finding set.

    No snapshot means unobserved/unavailable, never a clean bill of health.
    A fingerprint is internal projection currentness, not a Spec evaluation.
    """
    board_id: str
    owner_type: str
    owner_id: str
    namespace: str
    source_fingerprint: str
    findings: tuple[ProjectionReferenceFinding, ...]

    def __post_init__(self):
        for name in ('board_id', 'owner_type', 'owner_id', 'namespace'):
            _text(getattr(self, name), name)
        if (type(self.source_fingerprint) is not str or len(self.source_fingerprint) != 64
                or any(character not in '0123456789abcdef' for character in self.source_fingerprint)):
            raise ValueError('projection_finding_fingerprint_invalid')
        if type(self.findings) is not tuple:
            raise ValueError('projection_finding_collection_invalid')
        identity = self.board_id, self.owner_type, self.owner_id, self.namespace
        observed = set()
        for finding in self.findings:
            if (type(finding) is not ProjectionReferenceFinding
                    or (finding.board_id, finding.owner_type, finding.owner_id, finding.namespace) != identity):
                raise ValueError('projection_finding_scope_mismatch')
            if finding.finding_id in observed:
                raise ValueError('projection_finding_identity_duplicate')
            observed.add(finding.finding_id)
