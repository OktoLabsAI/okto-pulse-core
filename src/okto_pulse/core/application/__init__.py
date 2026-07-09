"""Application layer for the agnostic core (SaaS Refactor spec #09).

Houses transport-free use cases, command/result DTOs, the transport-neutral
``ActorContext`` and the ``ApplicationPurityGate``. Nothing under
``application/use_cases`` may import a transport framework (FastAPI, Starlette,
MCP) or transport-coupled symbols (``Request``, ``Depends``, ``ContextVar``,
``_active_api_key``) — the purity gate enforces this. Application use cases
receive relational access only through UnitOfWork-shaped objects; remaining
adapter/service transitional work belongs outside this package boundary.
"""
