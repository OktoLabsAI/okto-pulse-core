"""Impact variant of the existing traceability facade."""
import json
from okto_pulse.core.application.use_cases.base import EntityNotFoundError, PermissionDeniedError
from okto_pulse.core.application.use_cases.decision_impact import DecisionImpactCommand, DecisionImpactUseCase
from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
from okto_pulse.core.kg.interfaces.graph_errors import GraphError, GraphQueryTimeout
from okto_pulse.core.models.decision_impact import DecisionImpactRequest, DecisionImpactResponse


async def query_decision_impact(board_id, query, *, context, uow_factory):
    try:
        query = DecisionImpactRequest.model_validate(query)
        _, spec_id, _, decision_id = query.subject_ref.split(':')
        actor = MCPAdapterContract.actor(context, board_id=board_id)
        command = DecisionImpactCommand(board_id, spec_id, decision_id, query.limit,
            query.cursor, query.max_depth, query.timeout_ms)
        async with uow_factory(actor=actor) as uow:
            result = await DecisionImpactUseCase().execute(command, actor=actor, uow=uow)
        return DecisionImpactResponse.model_validate(result).model_dump_json()
    except EntityNotFoundError:
        code, message = 'decision_not_found', 'Decision not found or unavailable'
    except PermissionDeniedError:
        code, message = 'permission_denied', 'Permission denied for Decision impact'
    except GraphQueryTimeout:
        code, message = 'graph_query_timeout', 'Impact query timed out'
    except GraphError:
        code, message = 'decision_impact_unavailable', 'Impact query unavailable'
    except ValueError as exc:
        code = str(exc)
        if code in {'decision_impact_cursor_stale', 'spec_coverage_source_changed'}:
            message = 'The query scope changed. Reload the first page.'
        elif code == 'decision_impact_cursor_invalid':
            message = 'Invalid impact cursor'
        else:
            code, message = 'decision_impact_unavailable', 'Impact query unavailable'
    return json.dumps({'error': message, 'code': code})
