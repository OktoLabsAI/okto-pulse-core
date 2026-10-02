"""Complete records for native Task Validation test scenarios."""

def native_entry():
    return dict(
        id='validation', card_id='card', board_id='board', reviewer_id='reviewer',
        reviewer_name='Reviewer', confidence=90, confidence_justification='Checked evidence',
        estimated_completeness=95, completeness_justification='Checked scope',
        estimated_drift=0, drift_justification='No scope drift',
        general_justification='Current implementation satisfies the requirements',
        recommendation='approve', outcome='success', validation_outcome='success',
        completion_outcome='completed', threshold_violations=[], completion_gate_failures=[],
        resolved_thresholds={'min_confidence': 70, 'min_completeness': 80, 'max_drift': 50},
        reviewer_separation={'mode': 'enforce', 'allowed': True, 'conflicts': []},
        created_at='2026-10-02T12:00:00Z', card_status='done',
        expected_subject_version=1, subject_version=2,
        request_digest='private', idempotency_key='private',
    )

