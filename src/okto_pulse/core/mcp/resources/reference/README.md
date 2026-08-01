---
version: "1.0"
---
# MCP Resources — Reference

Cada arquivo `.md` neste diretório é exposto via
`@mcp.resource(uri="okto-pulse://reference/<nome>")` e contém material de
referência consultado sob demanda (errors, knowledge governance, multi-value
formats, destructive ops, card types, spec gates, Quality e avaliações
semânticas de guidelines).

Entradas operacionais canônicas:

- Quality assessments/pinpointing:
  `okto-pulse://reference/quality-assessments`, com contratos completos em
  `okto-pulse://reference/tool-docs/quality`.
- Guidelines versionadas, adoção, métricas e avaliações semânticas:
  `okto-pulse://reference/policy-compliance`, com assinaturas das ferramentas
  em `okto-pulse://reference/tool-docs/guideline`.

Estes resources são linkados a partir de docstrings de tools MCP pela URI `okto-pulse://reference/<nome>` — sem prefixo `resource://` e sem sufixo `.md`. O agente lê on-demand quando precisa do detalhe. Atenção ao mapeamento hífen/underscore: a URI pode usar hífen enquanto o arquivo usa underscore (ex.: `okto-pulse://reference/projection-profiles` → `projection_profiles.md`); o mapeamento canônico é o registro `@mcp.resource` em `server.py`.

Cada arquivo deve começar com frontmatter `---` / `version: "X.Y"` / `---` para detecção de drift.
