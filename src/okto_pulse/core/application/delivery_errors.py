"""Public application error contract for delivery-gated commands.

Transport adapters consume this typed result without importing service internals.
HTTP mapping remains owned by the edition.
"""
from dataclasses import asdict

from okto_pulse.core.domain.delivery_assessment import aggregate_delivery_metrics
from okto_pulse.core.domain.delivery_inventory import delivery_digest
from okto_pulse.core.models.delivery_readiness import DeliveryMetric

class DeliveryGateBlocked(ValueError):
    """Typed assessed deficits retained for native error envelopes/diagnostics."""
    code = 'delivery_evidence_incomplete'
    def __init__(self, assessment, gate):
        self.assessment, self.gate = assessment, gate
        codes = sorted({code for blocker in gate.blockers for reason in blocker.reasons
            for code in (reason.native_reason_codes or (reason.code,))})
        missing = sorted({b.binding.obligation_ref for b in gate.blockers if b.binding})
        super().__init__('delivery_evidence_incomplete: ' + ', '.join(codes)
            + '; obligations=' + ', '.join(missing[:20]))

    def to_error_dict(self):
        assessment, gate = self.assessment, self.gate
        metrics = aggregate_delivery_metrics(assessment)
        query = {'board_id': assessment.scope.board_id, 'spec_id': assessment.scope.spec_id,
            'card_id': assessment.card_id, 'view': 'readiness'}
        # Identify precisely the assessment that rejected this mutation. This
        # fingerprint is NOT a readiness cursor snapshot: the follow-up read
        # reauthorizes and establishes its own current snapshot after rollback.
        summary = {'assessment_snapshot_id': delivery_digest({'assessment': asdict(assessment), 'gate': asdict(gate)}),
            'spec_edition': assessment.scope.edition,
            'implementation': DeliveryMetric(**asdict(metrics.implementation)).model_dump(mode='json'),
            'verification': DeliveryMetric(**asdict(metrics.verification)).model_dump(mode='json'), 'delivery_gate_status': gate.status,
            'blocker_count': len(gate.blockers), 'snapshot_refresh_required': True}
        return {'code': self.code, 'message': 'Delivery requirements are not satisfied. Read readiness for current causes and actions.',
            'details': {'readiness_summary': summary, 'readiness_query': query},
            'remediation': [{'action': 'review_delivery_readiness', 'tool': 'okto_pulse_get_delivery_evidence',
                'arguments': query}]}
