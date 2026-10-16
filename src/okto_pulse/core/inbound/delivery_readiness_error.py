"""Shared safe read-error projection; HTTP status belongs to the REST adapter."""
from okto_pulse.core.domain.delivery_assessment import ReadinessIntegrityError
from okto_pulse.core.domain.delivery_diagnostics import DiagnosticIntegrityError
from okto_pulse.core.ports.delivery_readiness import ReadinessAccessDenied, ReadinessReadError

READINESS_ERRORS = (ReadinessReadError, ReadinessAccessDenied, ReadinessIntegrityError, DiagnosticIntegrityError)
_MESSAGES = {
    'readiness_query_invalid': 'The readiness query is invalid. Check its scope, filters and cursor.',
    'readiness_scope_mismatch': 'The requested readiness scope is unavailable to this actor.',
    'readiness_cursor_stale': 'Readiness changed. Restart the query without a cursor or snapshot.',
    'readiness_snapshot_changed': 'Readiness changed during the read. Retry the query.',
    'readiness_limit_exceeded': 'Readiness exceeds the supported query capacity.',
    'readiness_assessment_unavailable': 'The current readiness assessment is unavailable.',
    'readiness_permission_denied': 'Readiness read permission is required.',
    'readiness_diagnostic_integrity': 'The readiness diagnostic could not be composed consistently.',
}


def project_readiness_error(error):
    if isinstance(error, ReadinessAccessDenied):
        code, native = 'readiness_permission_denied', None
    elif isinstance(error, ReadinessReadError):
        code, native = error.code, error.native_code
    elif isinstance(error, (ReadinessIntegrityError, DiagnosticIntegrityError)):
        code, native = 'readiness_diagnostic_integrity', str(error)
    else:
        raise TypeError('unsupported_readiness_error')
    # Only stable native machine codes are public; never error prose, actor
    # identities, source contents, query payloads or cursor signing material.
    details = {'native_code': native} if native and native.replace('_', '').isalnum() else {}
    return dict(code=code, message=_MESSAGES.get(code, _MESSAGES['readiness_assessment_unavailable']),
        details=details, retryable=code in {'readiness_cursor_stale', 'readiness_snapshot_changed',
            'readiness_assessment_unavailable'}, remediation=[{
                'action': 'restart_readiness_query' if code in {'readiness_cursor_stale', 'readiness_snapshot_changed'}
                    else 'review_readiness_query', 'tool': 'okto_pulse_get_delivery_evidence'}])
