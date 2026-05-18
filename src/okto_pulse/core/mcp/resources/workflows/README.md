# MCP Resources — Workflows

Cada arquivo `.md` neste diretório é exposto via `@mcp.resource(uri="okto-pulse://workflows/<nome>")` e contém o workflow operacional de uma entidade do SDLC (cards, specs, ideations, refinements, sprints, kg, stories).

Estes resources são consumidos sob demanda pelo agente via `resources/read`, em vez de carregados eagerly no system prompt — parte da iniciativa P0.A (Resources MCP para instructions).

Cada arquivo deve começar com header `version: X.Y` para detecção de drift via smoke test CI.
