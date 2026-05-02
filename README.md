# okto-pulse-core

Core engine for [Okto Pulse](https://github.com/OktoLabsAI/okto-pulse) — shared models, services, API routes, and MCP server.

> **You probably want to install [`okto-pulse`](https://pypi.org/project/okto-pulse/) instead.**
> This package is the internal engine. The `okto-pulse` package provides the CLI, frontend, and everything you need to get started.

## What's inside

- **26 SQLAlchemy models** — Boards, Cards, Specs, Ideations, Refinements, Sprints, Agents, Knowledge, Mockups, Validations, etc. (Skills entity dropped in 0.1.13.)
- **17 service classes** — Full business logic with governance rules (Skills service dropped in 0.1.13.)
- **11 API route modules** — FastAPI REST endpoints
- **150+ MCP tools** — Complete Model Context Protocol server for AI agent integration, including:
  - Pipeline CRUD (Ideation, Refinement, Spec, Sprint, Card)
  - Q&A and choice questions across every entity
  - Mockups (HTML+Tailwind, sanitised) and Knowledge Bases at spec/refinement/card scope
  - Decisions with supersedence and coverage gates
  - Per-card Knowledge attachment lifecycle (`add_card_knowledge` and friends)
  - 22 Knowledge Graph tools (consolidation, query primario/power, health, dead-letter, schema-migrate, decay tick controllability)
- **App factory** — `create_app()` with dependency injection for auth and storage providers
- **Embedded Knowledge Graph** — per-board Kùzu instance + global discovery meta-graph, deterministic + cognitive workers, 11 node types and 10 relationship types

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Single Python process  (single Kùzu lock holder)        │
│                                                          │
│   ┌─────────────────┐         ┌──────────────────┐       │
│   │  uvicorn :api   │         │  uvicorn :mcp    │       │
│   │   FastAPI app   │         │  MCP ASGI app    │       │
│   │   /api/v1/*     │         │  /mcp            │       │
│   │   /static, SPA  │         │  streamable-http │       │
│   └────────┬────────┘         └────────┬─────────┘       │
│            │                           │                  │
│            └─────────┬─────────────────┘                  │
│                      │                                    │
│           shared module-level state:                      │
│           - Mongo session factory (init via lifespan)     │
│           - _global_db (Kùzu cache)                       │
│           - _active_api_key (ContextVar)                  │
└─────────────────────────────────────────────────────────┘
```

`build_mcp_asgi_app()` and `mount_mcp(app)` are the two helpers exposed from `okto_pulse.core.mcp` — pick `build_mcp_asgi_app()` to drive a separate uvicorn `Server` (the community edition does this for the `--mcp-port` listener) or `mount_mcp(app)` to mount the MCP sub-app under an arbitrary path on an existing FastAPI app.

## Release Notes

### 0.1.13 — current

#### Fix C: single-process, dual-port serve (Kùzu lock contention)

`okto-pulse serve` now runs API/UI **and** MCP from a **single Python process** but on **two different ports** (`--api-port` defaults to 8100, `--mcp-port` defaults to 8101). Two `uvicorn.Server` instances run concurrently inside one `asyncio.gather` — the embedded Kùzu DB is owned by exactly one OS process (no inter-process lock contention), and the two listeners share the module-level state (the registered session factory, the `_global_db` Kùzu cache, the `_active_api_key` `ContextVar`).

What you get:
- **No Kùzu file-lock thrash** — the embedded DB does not support multiple writers, so a single Python process is the only safe topology. The `kg.db_open.lock_retry path=... attempt=N/5` warnings disappear.
- **Independent ports** — keep `:8100` for the SPA fetches and `:8101` for the MCP HTTP transport, unchanged from earlier releases.
- **One lifespan** — `init_db`, KG worker startup, scheduler boot, and `register_session_factory` all run once on the API listener; the MCP sub-app picks up the registered factory automatically.

Public surface:
- `okto_pulse.core.mcp.build_mcp_asgi_app()` — returns the MCP ASGI app wrapped in the `ApiKeySessionMiddleware` (handles `?api_key=` / `X-API-Key` / `Authorization: Bearer` and binds the key to the request `ContextVar`).
- `okto_pulse.core.mcp.mount_mcp(app, mount_path="/mcp")` — mounts the same ASGI app onto an existing FastAPI app at the given path (community does this when an embedded mount is needed).
- `okto_pulse.core.mcp.register_session_factory(factory)` — call from the API lifespan so the MCP sub-app finds the DB. Idempotent.

#### Spec Skills entity removed in its entirety

The experimental "skills" feature on the spec entity is gone. Adoption was zero in real boards and knowledge entries already cover the reusable-context use case more naturally — the dedicated tab, MCP tools, REST endpoints and ORM table were paying recurring maintenance cost without return.

What goes away:
- **5 MCP tools removed** — `okto_pulse_create_spec_skill`, `okto_pulse_delete_spec_skill`, `okto_pulse_spec_skill_retrieve`, `okto_pulse_spec_skill_inspect`, `okto_pulse_spec_skill_load`.
- **4 REST endpoints removed** — `GET / POST / PATCH / DELETE /api/v1/specs/{spec_id}/skills` (and the `{skill_id}` variants).
- **5 permission flags removed** — `spec.skills.{read,load,create,delete,recall}` from the registry and from every preset.
- **Database table dropped** — `spec_skills`. Migration is idempotent (`DROP TABLE IF EXISTS`); no downgrade — the data is gone.
- **Pydantic schemas removed** — `SkillSectionSchema`, `SpecSkillCreate`, `SpecSkillUpdate`, `SpecSkillResponse`, `SpecSkillSummary`. The `skills` field is gone from `SpecResponse`.
- **`agent_instructions.md` scrubbed** — Quick Navigation, the dedicated Spec Skills section, the spec-authoring workflow step and the destructive-operations row no longer reference skills.

Reader-side defensive handling: `BaseSchema` now sets `extra="ignore"` so historical payloads still carrying a `skills` field validate silently — no warning, no log, no error. There is nothing to migrate; the field is dropped on read.

Use **knowledge entries** (`spec_knowledge`, `card_knowledge`) and **decisions** for the same use case.

#### Agent instructions overhaul

`agent_instructions.md` was reviewed end-to-end. Three behavioural sections were added in response to repeated drift patterns observed across production sessions:

- **§ 2.1a Ambiguity-killer protocol** — at ideation, the agent must scan the user's request against a table of ambiguity symptoms (vague verbs, undefined nouns, multiple plausible interpretations, implicit success criteria, implicit scope) and post Q&A items for every gap before advancing the ideation. "Just make a reasonable choice" is permission, not silence — it must be recorded explicitly.
- **§ 2.2a Investigação profunda obrigatória (refinement)** — refinement is research, not paraphrasing. The agent must exhaust all applicable sources (project files, source code, KE, Knowledge Graph, mockups, web docs, online discussions, runtime evidence, stakeholder context) and the refinement body must cite each finding with `path:line`, KE titles, KG node ids or URLs.
- **§ 2.8 Card-level artifact attachment (MANDATORY)** — every card must be self-contained. KE/mockup dependencies must be attached **directly to the card** via `copy_knowledge_to_card` / `copy_mockups_to_card` / `add_card_knowledge` / `add_screen_mockup(entity_type="card")`. Vague references to "see the spec" are a protocol violation.

Cleanup:
- Quick Navigation header *Multi-value Parameters — Two Accepted Formats* corrected to *Three Input Shapes* (the section was extended to native `list[str]` in 0.1.4).
- Obsolete `delete_task_validation` reference removed from the *Available Tools → Evaluations & Validations* table (the tool never shipped).
- `okto_pulse_create_sprint` parameter list aligned with the schema (`objective?` and `expected_outcome?` were missing).
- Duplicate "Startup Protocol" subsection deleted — Pre-Flight Checklist is the single source of truth.

#### Other improvements

- **MCP `ApiKeySessionMiddleware`** rewritten on top of `ContextVar` — required because the FastAPI process serves multiple concurrent requests and the previous module-level global would leak identities across requests. Token-based set/reset pattern protects against exception leaks.
- **`run_mcp_server`** retained for legacy callers, now defers to `build_mcp_asgi_app` for ASGI-mode embeds.

To upgrade an existing install: `pip install -U okto-pulse okto-pulse-core` and then `okto-pulse init --agents` to regenerate `.mcp.json` (the URL still points at port 8101 by default; override with `--mcp-port` if you remapped). No downstream contract changes for MCP clients — the wire protocol and tool catalog (sans 5 skills tools) are unchanged.

### 0.1.3 — previous stable (PyPI)

First hardening pass on the card lifecycle, the analytics contract, and the MCP instruction set.

- **`CardService.delete_card` cascades** through every spec-side JSON list (`test_scenarios[].linked_task_ids`, `business_rules[]`, `api_contracts[]`, `technical_requirements[]`, `decisions[]`) and through bug cards' `linked_test_task_ids`. The transactional cascade unblocks the delete→recreate flow that previously tripped `_validate_spec_linked_refs`.
- **Analytics card-type classifier** uses enum identity instead of `str(card.card_type).endswith(...)`. `total_cards_impl/test/bug`, `task_validation_gate.total_submitted`, `velocity[].test/bug`, and `bug_rate_per_spec` now report real counts.
- **`parse_multi_value` helper** consolidated the scattered `.split("|")` pattern; pipe-separated and JSON-array inputs are autodetected.
- **MCP agent instructions** rewritten (1830 → 2050 lines) with new sections for Multi-value Parameters, Destructive Operations, Versioning & Concurrent Edits, Security, Analytics-Driven Closure.

### 0.1.1 — initial PyPI release

26+1 SQLAlchemy models, 17+1 service classes, 11 API route modules, 119 MCP tools, embedded Kùzu Knowledge Graph with deterministic workers. (Spec Skills shipped here and was removed in 0.1.13.)

(Version 0.1.2 was published to TestPyPI only as a release candidate for 0.1.3.)

## License

[Elastic License 2.0](./LICENSE) — free for personal and commercial use. Cannot be offered as a hosted/managed service.
