# MCP Resources — Reference

Cada arquivo `.md` neste diretório é exposto via `@mcp.resource(uri="okto-pulse://reference/<nome>")` e contém material de referência consultado esporadicamente (errors, multi-value formats, destructive ops, card types, spec gates).

Estes resources são linkados a partir de docstrings de tools MCP via `resource://okto-pulse://reference/<nome>.md` — agente lê on-demand quando precisa do detalhe.

Cada arquivo deve começar com header `version: X.Y` para detecção de drift via smoke test CI.
