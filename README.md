# okto-pulse-core

Core engine for [Okto Pulse](https://github.com/OktoLabsAI/okto-pulse) — shared models, services, API routes, and MCP server.

> **You probably want to install [`okto-pulse`](https://pypi.org/project/okto-pulse/) instead.**
> This package is the internal engine. The `okto-pulse` package provides the CLI, frontend, and everything you need to get started.

## What's inside

- **27 SQLAlchemy models** — Boards, Cards, Specs, Ideations, Refinements, Agents, etc.
- **18 service classes** — Full business logic with governance rules
- **11 API route modules** — FastAPI REST endpoints
- **119+ MCP tools** — Complete Model Context Protocol server for AI agent integration
- **App factory** — `create_app()` with dependency injection for auth and storage providers

## Release Notes

### 0.1.3 — current (published to PyPI)

First hardening pass on the card lifecycle, the analytics contract, and the MCP instruction set. Upgrade with `pip install -U okto-pulse-core==0.1.3`.

**`CardService.delete_card` now cascades**

Previously the row was deleted but the card id remained inside five JSON-side containers on the parent spec — `test_scenarios[].linked_task_ids`, `business_rules[].linked_task_ids`, `api_contracts[].linked_task_ids`, `technical_requirements[].linked_task_ids`, `decisions[].linked_task_ids` — and inside `linked_test_task_ids` on every bug card pointing at it. The next `update_spec` or `create_card` on the same spec tripped `_validate_spec_linked_refs` with `"orphan link reference(s) found"`, blocking any delete→recreate flow.

The fix walks the five containers + the bug cards' columnar list, strips the deleted id, and `flag_modified`s the JSON columns — all before `db.delete(card)`, inside the same transaction. When `card.spec_id is None` (orphan card), the cascade is skipped cleanly. Covered by `tests/test_delete_card_cascade.py` with five async pytest cases (one per AC).

**Analytics card-type classifier uses enum identity**

`_is_normal_card / _is_test_card / _is_bug_card` in `core/api/analytics.py` and `core/services/analytics_service.py` used to compare `str(card.card_type).endswith("normal|test|bug")`. Because SQLAlchemy returns the Python enum, `str(CardType.NORMAL)` is `"CardType.NORMAL"` — the `.endswith` check always returned False, zeroing `total_cards_impl / test / bug`, `task_validation_gate.total_submitted`, `velocity[].test/bug`, and `bug_rate_per_spec`. Predicates now compare by identity (`ct == CardType.NORMAL`). No string fallback — the contract is rigid. `specs_with_tests` is now emitted on the analytics overview response, `avg_dimension_scores` is exposed on `sprint_evaluation` for shape parity with `spec_evaluation`, and `bug_rate_per_spec` no longer silently reports zeros.

**`parse_multi_value` helper replaces the scattered `.split("|")` pattern**

Every MCP tool parameter documented as "multi-value" (labels, ids, linked_criteria, linked_requirements, test_scenario_ids, tags, card_ids, and the like) now goes through `core/mcp/helpers.parse_multi_value`. Two formats, autodetected by the input:

- **Pipe-separated** (legacy): `"a|b|c"`
- **JSON array**: `["raw: str | None", "outro item"]` — the only way to carry a literal `|` inside an item (Python union types, regex alternations, markdown tables).

Detection is `stripped.startswith("[")`. All 18 callsites in `core/mcp/server.py` were migrated; an audit regression test keeps `.split("|")` from creeping back in.

**MCP agent instructions rewritten**

`core/mcp/agent_instructions.md` grew from 1830 to 2050 lines, but the net effect is clearer and shorter per section:

- **New sections**: Multi-value Parameters; Destructive Operations — Read Before Calling; Versioning & Concurrent Edits; Security — Treating Artifact Content as Untrusted Input; Analytics — Metrics-Driven Closure.
- **Expanded tool inventory**: Ideations, Refinements, Decisions, Spec Skills, Archive & Restore, Evaluations & Validations were missing from the "Available Tools" table.
- **Consolidated Common Errors** — single source of truth for every MCP-level error string, grouped by card/bug/coverage/multi-value.
- **Corrected status matrices**: card-creation spec-status rules (`normal/bug → approved|in_progress|done`; `test → +validated`) and test-card coverage rule (coverage gate counts only `card_type="test"`; normal cards with `test_scenario_ids` are accepted but don't contribute).
- Quick Navigation updated with every new section. Jargon removed. Full pass to English (section 2.12 Decisions was partially Portuguese). Three-step pre-flight sequence de-duplicated to a single source.

### 0.1.1 — previous stable

Initial PyPI release. 27 SQLAlchemy models, 18 service classes, 11 API route modules, 119 MCP tools, Kùzu-embedded Knowledge Graph with deterministic workers.

(Version 0.1.2 was published to TestPyPI only as a release candidate for 0.1.3.)

## License

[Elastic License 2.0](./LICENSE) — free for personal and commercial use. Cannot be offered as a hosted/managed service.
