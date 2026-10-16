"""Reuse the delivery evaluator for analytics; no graph-derived approvals."""

from okto_pulse.core.domain.delivery_evidence import DeliveryScope, evaluate_delivery_coverage
from okto_pulse.core.services.delivery_evidence import delivery_store
from okto_pulse.core.ports.relational_application import RelationalApplicationAdapterMissing


async def decision_delivery_rows(session, specs):
    result = {}
    for spec in specs:
        if not any(d.get('status', 'active') == 'active' for d in (getattr(spec, 'decisions', None) or [])):
            continue
        try:
            snapshot = await delivery_store(session).load_snapshot(DeliveryScope(spec.board_id, spec.id, int(spec.edition)))
        except (ValueError, TimeoutError, RelationalApplicationAdapterMissing):
            result[str(spec.id)] = None
            continue
        if not snapshot.complete:
            result[str(spec.id)] = None
            continue
        evaluation = evaluate_delivery_coverage(snapshot)
        result[str(spec.id)] = {row.obligation.binding.obligation_ref.removeprefix('decision:'): row
            for row in evaluation.rows if getattr(row, 'decision_verification_status', None) is not None}
    return result
