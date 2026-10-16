"""Bug variant of the existing traceability facade; no new tool family."""
import json
from datetime import datetime, timezone

from okto_pulse.core.application.use_cases.base import EntityNotFoundError, PermissionDeniedError
from okto_pulse.core.application.use_cases.bug_clusters import BugClustersCommand, BugClustersUseCase
from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
from okto_pulse.core.kg.interfaces.graph_errors import GraphError, GraphQueryTimeout
from okto_pulse.core.models.bug_clusters import BugClustersRequest, BugClustersResponse


async def query_bug_clusters(board_id, query, *, context, uow_factory):
    try:
        query = BugClustersRequest.model_validate(query)
        actor = MCPAdapterContract.actor(context, board_id=board_id)
        command = BugClustersCommand(board_id, query.window(datetime.now(timezone.utc)),
            query.group_by, query.status, query.severity, query.limit, query.cursor, query.timeout_ms)
        async with uow_factory(actor=actor) as uow:
            result = await BugClustersUseCase().execute(command, actor=actor, uow=uow)
        return BugClustersResponse.model_validate(result).model_dump_json(by_alias=True)
    except EntityNotFoundError:
        code, message = 'board_not_found', 'Board not found'
    except PermissionDeniedError:
        code, message = 'permission_denied', 'Permission denied for this grouping'
    except GraphQueryTimeout:
        code, message = 'graph_query_timeout', 'Cluster query timed out'
    except GraphError:
        code, message = 'bug_clusters_unavailable', 'Cluster query unavailable'
    except ValueError as exc:
        code = str(exc)
        if code in {'bug_clusters_cursor_stale', 'bug_clusters_source_changed'}:
            message = 'The query scope changed. Reload the first page.'
        elif code in {'bug_clusters_cursor_invalid', 'bug_clusters_cursor_window_required',
            'bug_clusters_window_invalid', 'analytics_temporal_query_invalid', 'bug_clusters_filter_invalid'}:
            message = 'Invalid cluster query'
        else:
            code, message = 'bug_clusters_unavailable', 'Cluster query unavailable'
    return json.dumps({'error': message, 'code': code})
