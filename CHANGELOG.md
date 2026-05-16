# Changelog

All notable changes to `okto-pulse-core` are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Fixed — P0.B post-release polish (3 bugs from evaluation 2026-05-16)

After the v0.2.1 cut, an empirical evaluation against the E2E board exposed three discrepancies between the spec/API contracts and the actual handler implementations. All three were fixed as docs-or-tiny-code follow-ups in the same `Unreleased` cycle:

1. **Signature alignment (docs)** — The spec P0.B (`ec70c5f7`) and API contracts (`api_c963da56`, `api_ef747629`, `api_0b42caa1`) document idealised shapes that drop `board_id` and use `parent_type/parent_id` for `list_knowledge`. The actual implementations require `board_id` everywhere (needed for ACL/auth) and use a uniform `entity_type/entity_id` naming. The implementation is the source of truth; the affected docstrings are now authoritative. Bug card: `75535b71`.

2. **`filters` accepts JSON string** — `okto_pulse_list_by_board`, `okto_pulse_list_qa`, and `okto_pulse_list_knowledge` now accept `filters` as either a Python dict OR a JSON-encoded string (e.g. `'{"status":"done"}'`). MCP transports often serialise complex params as strings; without this, agents that followed the JSON-RPC convention hit a `dict_type` Pydantic error. The signature is now `filters: dict[str, Any] | str | None = None` and each handler decodes the string with `json.loads` (returning a `structured_error` on `JSONDecodeError`). Bug card: `eb782ae4`.

3. **`limit` default unified to 100** — `okto_pulse_list_specs`, `okto_pulse_list_ideations`, and `okto_pulse_list_refinements` previously defaulted to `limit=50` while the new consolidated `okto_pulse_list_by_board` defaults to `limit=100`. Clients migrating between the two would silently double their page size. The three deprecated handlers now also default to `limit=100`, aligning the surface during the deprecation window. Bug card: `717e9915`.

### Authoritative handler signatures (P0.B)

| Handler | Signature |
|---|---|
| `okto_pulse_list_by_board` | `(board_id, entity_type, filters?, limit=100, offset=0)` |
| `okto_pulse_list_qa` | `(board_id, entity_type, entity_id, filters?)` |
| `okto_pulse_list_knowledge` | `(board_id, entity_type, entity_id, filters?)` |
| `okto_pulse_list_snapshots` | `(board_id, entity_type, entity_id)` |

All four return `structured_error` with keys `error`, `error_code`, `supported`, `detail` when invoked with an unsupported `entity_type` or invalid filter.

### Added — P0.B: 4 polymorphic list handlers

Four new consolidated MCP tools that replace 15 entity-specific `list_*` tools. Each handler delegates to the same existing service layer — behavior is preserved while the surface area is reduced.

| New handler | Replaces | Scope |
|---|---|---|
| `okto_pulse_list_by_board` | `list_specs`, `list_ideations`, `list_refinements`, `list_sprints`, `list_stories`, `list_topics` | Top-level board entities |
| `okto_pulse_list_qa` | `list_spec_qa`, `list_ideation_qa`, `list_refinement_qa` | Q&A items per entity |
| `okto_pulse_list_knowledge` | `list_spec_knowledge`, `list_ideation_knowledge`, `list_refinement_knowledge`, `list_card_knowledge` | Knowledge base items per parent (listing only — create/update/delete remain entity-specific) |
| `okto_pulse_list_snapshots` | `list_ideation_snapshots`, `list_refinement_snapshots` | Version snapshots per entity |

Server-side filter validation is enforced via the new `okto_pulse.core.mcp.filters.validate_filters()`; invalid combinations return a structured error with keys `error`, `error_code`, `supported`, `suggested_tool` (preserving the `error` key for backwards compatibility with existing clients).

### Added — P0.A: MCP resources for instructions

Twelve MCP resources (`okto-pulse://workflows/...` and `okto-pulse://reference/...`) now expose workflow and reference material on demand, allowing the root `agent_instructions.md` to shrink from ~3,403 lines to ~271 lines. Clients that support `resources/read` fetch detailed content lazily; clients that do not continue to function unchanged (graceful degradation).

### Added — P2: Runtime schema generation (pilot)

New module `okto_pulse.core.mcp.schema_generator` derives MCP tool input schemas from Pydantic v2 models at server startup via `model.model_json_schema()` + `model.model_fields`. Pilot covers five card CRUD tools (`get_card`, `update_card`, `move_card`, `delete_card`, `list_cards_by_status`); `create_card` is intentionally excluded from the pilot. Coverage of `Field(description=...)` is 100% on the 15 pilot models.

### Deprecated — 15 legacy `list_*` tools (removal target: v0.3.0)

The 15 entity-specific list tools are still functional in v0.2.x but emit a `_deprecation_warning` key on every response, identifying the replacement handler. A sample log (rate configurable via `OKTO_PULSE_DEPRECATION_SAMPLE_RATE`, default 1%) captures invocation patterns to validate adoption before removal.

**Deprecation window**: one minor release. Concrete removal target: **v0.3.0** (target date to be set by the release manager when the v0.3.x branch is cut; minimum 8 weeks from the v0.2.1 release that introduced the replacements).

**Migration guide**: for each call to a deprecated tool, replace it with the corresponding consolidated handler from the table above. The new handlers accept the same logical arguments (board scope + entity discriminator + optional filters) and return payloads that are field-equivalent to the legacy responses, minus the legacy-only `_deprecation_warning` key. Server-side filter validation may reject filter shapes that the legacy tools silently accepted — when in doubt, omit unsupported filters and re-validate the response shape.

The full mapping plus naming rationale lives in spec `ec70c5f7-1af6-4830-90e6-e9fd9a6e0b63` on the Okto Pulse 0.2.1 board.

---

## [0.2.1] — 2026-05-16

Initial 0.2.1 release. See git tag `v0.2.1` for the full diff against `v0.2.0`.
