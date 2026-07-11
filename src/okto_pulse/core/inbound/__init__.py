"""Transport-neutral MCP adapter contracts.

Thin transport→application bridges: build a transport-neutral ``ActorContext``
and a command from a REST request or MCP tool call, invoke the shared use case,
and map the result/errors back to the transport's response shape. Business rules
live in the use cases, never here. These modules ARE the transport layer, so —
unlike ``application/use_cases`` — they project MCP envelopes without owning a
server runtime. The FastAPI adapter is Community-owned.
"""
