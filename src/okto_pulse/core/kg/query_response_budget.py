"""Bound the complete public query data envelope, independently of row count."""
import json
from typing import Any

from okto_pulse.core.kg.interfaces.graph_errors import GraphQueryResourceLimit


MAX_QUERY_PAYLOAD_BYTES = 4 * 1024 * 1024


def enforce_query_response_budget(payload: dict[str, Any]) -> dict[str, Any]:
    """Refuse oversized data without publishing incomplete aggregates.

    Count the UTF-8 JSON data envelope, including its query metadata. ASCII
    escaping and standard separators conservatively cover the MCP JSON form
    and compact REST form. Transport framing is separate. Streaming avoids
    allocating a second full serialized payload solely to check its size.
    ``observed`` is the count at refusal, not a claimed full-result size.
    """
    observed = 0
    for chunk in json.JSONEncoder(ensure_ascii=True, default=str).iterencode(payload):
        observed += len(chunk.encode('utf-8'))
        if observed > MAX_QUERY_PAYLOAD_BYTES:
            raise GraphQueryResourceLimit('Serialized query payload exceeds its resource limit.', details={
                'resource': 'serialized_payload_bytes', 'limit': MAX_QUERY_PAYLOAD_BYTES,
                'observed': observed,
            })
    return payload
