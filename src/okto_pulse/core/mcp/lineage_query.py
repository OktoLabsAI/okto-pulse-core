"""Lineage variant of the existing traceability facade."""
import json
from okto_pulse.core.application.use_cases.base import EntityNotFoundError, PermissionDeniedError
from okto_pulse.core.application.use_cases.lineage_query import LineageCommand, LineageUseCase
from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
from okto_pulse.core.kg.interfaces.graph_errors import GraphError, GraphQueryTimeout
from okto_pulse.core.models.lineage_query import LineageRequest, LineageResponse
from okto_pulse.core.ports.traceability import TraceabilityReadError


async def query_lineage(board_id, query, *, context, uow_factory):
    try:
        query = LineageRequest.model_validate(query)
        actor = MCPAdapterContract.actor(context, board_id=board_id)
        command = LineageCommand(board_id, query.subject_ref, query.limit, query.cursor, query.max_depth, query.timeout_ms)
        async with uow_factory(actor=actor) as uow:
            result = await LineageUseCase().execute(command, actor=actor, uow=uow)
        return LineageResponse.model_validate(result).model_dump_json()
    except EntityNotFoundError:
        code, message = 'lineage_subject_not_found', 'Lineage subject not found or unavailable'
    except PermissionDeniedError:
        code, message = 'permission_denied', 'Permission denied for this lineage scope'
    except GraphQueryTimeout:
        code, message = 'graph_query_timeout', 'Lineage query timed out'
    except GraphError:
        code, message = 'lineage_unavailable', 'Lineage query unavailable'
    except TraceabilityReadError as exc:
        code, message = ('lineage_subject_not_found', 'Lineage subject not found or unavailable') if exc.status_code == 404 else (
            'lineage_unavailable', 'Lineage query unavailable')
    except ValueError as exc:
        code = str(exc)
        if code in {'lineage_cursor_stale', 'lineage_source_changed'}:
            message = 'The source changed. Reload the first page.'
        elif code == 'lineage_cursor_invalid':
            message = 'Invalid lineage cursor'
        elif code in {'lineage_source_node_bound', 'lineage_source_edge_bound', 'lineage_summary_payload_bound', 'lineage_row_payload_bound'}:
            message = 'The lineage scope exceeds the query resource limit.'
        else:
            code, message = 'lineage_unavailable', 'Lineage query unavailable'
    return json.dumps({'error': message, 'code': code})
