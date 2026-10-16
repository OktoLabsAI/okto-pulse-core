"""Coverage variant of the existing traceability facade."""
import json
from okto_pulse.core.application.use_cases.base import EntityNotFoundError, PermissionDeniedError
from okto_pulse.core.application.use_cases.spec_coverage_query import SpecCoverageCommand, SpecCoverageUseCase
from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
from okto_pulse.core.kg.interfaces.graph_errors import GraphError, GraphQueryTimeout
from okto_pulse.core.models.spec_coverage_query import SpecCoverageRequest, SpecCoverageResponse


async def query_spec_coverage(board_id, query, *, context, uow_factory):
    try:
        query = SpecCoverageRequest.model_validate(query)
        actor = MCPAdapterContract.actor(context, board_id=board_id)
        command = SpecCoverageCommand(board_id, query.subject_ref[5:], query.limit, query.cursor, query.timeout_ms)
        async with uow_factory(actor=actor) as uow:
            result = await SpecCoverageUseCase().execute(command, actor=actor, uow=uow)
        return SpecCoverageResponse.model_validate(result).model_dump_json()
    except EntityNotFoundError:
        code, message = 'spec_not_found', 'Spec not found or unavailable'
    except PermissionDeniedError:
        code, message = 'permission_denied', 'Permission denied for Spec coverage'
    except GraphQueryTimeout:
        code, message = 'graph_query_timeout', 'Coverage query timed out'
    except GraphError:
        code, message = 'spec_coverage_unavailable', 'Coverage query unavailable'
    except ValueError as exc:
        code = str(exc)
        if code in {'spec_coverage_cursor_stale', 'spec_coverage_source_changed'}:
            message = 'The query scope changed. Reload the first page.'
        elif code == 'spec_coverage_cursor_invalid':
            message = 'Invalid coverage cursor'
        else:
            code, message = 'spec_coverage_unavailable', 'Coverage query unavailable'
    return json.dumps({'error': message, 'code': code})
