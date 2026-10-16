"""Source-reference diagnostics; these records confer no delivery authority.

Identity excludes the current reason and source fingerprint: reevaluation can
change or close a finding without inventing a second defect for the same link.
"""
from dataclasses import asdict, dataclass
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

    def to_payload(self) -> dict:
        """Versioned durable representation, independent of historical audit DTOs."""
        payload = asdict(self)
        payload['schema_version'] = 1
        payload['findings'] = [asdict(item) | {'finding_id': item.finding_id}
                               for item in self.findings]
        return payload

    @classmethod
    def from_payload(cls, payload: object) -> 'ProjectionFindingSnapshot':
        if (type(payload) is not dict
                or set(payload) != set(cls.__dataclass_fields__) | {'schema_version'}
                or type(payload['schema_version']) is not int or payload['schema_version'] != 1
                or type(payload['findings']) is not list):
            raise ValueError('projection_finding_payload_invalid')
        findings = []
        for item in payload['findings']:
            if (type(item) is not dict
                    or set(item) != set(ProjectionReferenceFinding.__dataclass_fields__) | {'finding_id'}):
                raise ValueError('projection_finding_payload_invalid')
            finding = ProjectionReferenceFinding(**{key: value for key, value in item.items()
                                                    if key != 'finding_id'})
            if item['finding_id'] != finding.finding_id:
                raise ValueError('projection_finding_identity_mismatch')
            findings.append(finding)
        return cls(**{key: value for key, value in payload.items()
                      if key not in {'schema_version', 'findings'}}, findings=tuple(findings))


def validate_audit_finding_snapshot(snapshot, *, board_id, artifact_type, artifact_id, agent_id):
    """Diagnostics are internal worker facts, never client authority or approval."""
    if snapshot is None:
        return
    if (type(snapshot) is not ProjectionFindingSnapshot
            or (snapshot.board_id, snapshot.owner_type, snapshot.owner_id)
            != (board_id, artifact_type, artifact_id)
            or agent_id != 'system:historical_consolidation'):
        raise ValueError('projection_finding_audit_scope_invalid')
