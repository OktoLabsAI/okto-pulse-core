---
version: "1.0"
---

# Multi-Value Parameters — Supported Input Shapes

Most MCP tool arguments documented as multi-value (labels, ids, linked_criteria, linked_requirements, test_scenario_ids, tags, card_ids, and the like) are parsed by `okto_pulse.core.mcp.helpers.coerce_to_list_str`. Migrated tools declare their parameters as `list[str] | str = ""` so FastMCP's Pydantic Union dispatch picks the right shape at the framework boundary. **For migrated strict tools, comma-only input is rejected.**

**Expanded migrated cluster (spec d41c7209 — R3a).** The `list[str] | str` shape now covers the full multi-value cluster across refinement, spec, decision and choice tools, not just `labels`. The `tools/list` schema for each of these declares `anyOf [array-of-string, string]` (identical to `labels`), so the contract the agent reads matches what the server accepts:

- `okto_pulse_create_refinement` / `okto_pulse_update_refinement`: `in_scope`, `out_of_scope`, `decisions`
- `okto_pulse_create_spec` / `okto_pulse_update_spec`: `functional_requirements`, `technical_requirements`, `acceptance_criteria`
- `okto_pulse_add_decision` / `okto_pulse_update_decision`: `alternatives_considered`
- `okto_pulse_add_api_contract` / `okto_pulse_update_api_contract`: `linked_rules`
- `okto_pulse_add_integration_requirement`: `linked_api_contracts`; `okto_pulse_add_observability_requirement`: `linked_integration_requirements`
- `okto_pulse_create_guideline` / `okto_pulse_update_guideline`: `tags`
- the choice/answer tools: `options`, `selected`

All of them accept the same four input shapes below and reject comma-only input through `coerce_to_list_str`.

The structured object fields (`request_body_json`, `response_success_json`, `data_contract_json`, `payload_json` → `dict | str`; `response_errors_json` → `list[dict] | str`) follow the **object/array** counterpart of this rule — see the JSON-field note at the bottom of this page.

| Shape | Example input | Status | When to use |
|---|---|---|---|
| **Native list[str]** (PREFERRED) | `["bug", "frontend"]` | Canonical | New MCP clients; the FastMCP wire format is a JSON array; handler receives a Python `list`. |
| **JSON-array string** (legacy compat) | `'["str | None", "outro item"]'` | Accepted | Older MCP clients that can only send strings; required when any item contains a literal `|` that would be silently split. |
| **Pipe-separated string** (legacy compat) | `"a|b|c"` | Accepted | Older MCP clients with simple atomic values that never contain `|`. |
| **Bare single string** | `"1"` | Accepted | Convenience for a single id/value. The handler treats it as `["1"]`. |
| **Comma-only string** | `"a,b,c"` | **REJECTED** | Ambiguous under strict mode. Use a native list, JSON-array string, or pipe-separated string. |

**Detection rules:**
- If the parameter is delivered as a Python `list`, items are validated as strings then stripped and de-duped of empties — no parsing.
- If delivered as a string and trimmed input starts with `[`, JSON path.
- If delivered as a string and contains `|`, pipe path.
- If delivered as a string and contains `,` under `strict_mode=True`, reject as ambiguous comma-separated input.
- Otherwise, return a single-item list.

**Error behaviour — uniform envelope (spec d41c7209 — R3a).** Migrated multi-value handlers no longer leak a raw `ValueError` to the transport. A parse failure is caught and returned as the uniform JSON envelope `{"error": "invalid_multi_value_input", "detail": "<message>"}` (the same `{error, detail}` shape as `_canonical_api_contract_error`), where `detail` carries the underlying message:
- Malformed JSON → `"malformed JSON for multi-value param: <reason> (at pos N)"`.
- JSON decoded to a non-list (e.g. `{"a": 1}`) → `"malformed multi-value: expected list, got dict"`.
- JSON array with a non-string item (e.g. `["ok", 42]`) → `"malformed multi-value: expected string items, got int at index 1"`.
- Comma-only string under `strict_mode=True` → `"multi-value input must be a JSON array ... or pipe-separated ..."`.

This closes the NC-3/G-2 leak where comma-only prose raised an uncaught exception at the MCP boundary. The legacy `auth`/`permission` errors and other non-multi-value sites keep their existing error shapes — only the multi-value cluster uses this envelope.

**Rule of thumb:**
1. Send a **native list** whenever you can — it never needs parsing and never needs escaping.
2. If you must send a string, send a **JSON array** when items contain `|` or `,` or any punctuation that risks being a separator.
3. Pipe is a convenience — never a contract.

## Ideation Domain Examples

`okto_pulse_create_ideation.labels` and `okto_pulse_update_ideation.labels` accept native list:

```python
okto_pulse_create_ideation(
    board_id="...", title="New idea",
    labels=["product, ux", "discovery"],   # native list — commas inside survive
)
```

## Refinement Domain Examples

`okto_pulse_create_refinement.labels` and `okto_pulse_update_refinement.labels` accept native list:

```python
okto_pulse_create_refinement(
    board_id="...", ideation_id="...", title="API design refinement",
    labels=["api, REST", "design"],   # native list — commas inside survive
)
```

## Spec Domain Examples

Four tools migrated: `okto_pulse_create_spec.labels`, `okto_pulse_update_spec.labels`, `okto_pulse_copy_mockups_to_card.screen_ids`, `okto_pulse_copy_knowledge_to_card.knowledge_ids`.

```python
okto_pulse_create_spec(
    board_id="...",
    title="Auth refactor",
    labels=["security, OAuth2", "backend"],   # native list — commas inside survive
)
okto_pulse_copy_mockups_to_card(
    board_id="...", spec_id="...", card_id="...",
    screen_ids=["scr_a", "scr_b"],            # native list — preferred
)
```

## Card Domain Examples

Both `okto_pulse_create_card` and `okto_pulse_update_card` now accept native list for `labels`, `test_scenario_ids` and (update only) `linked_test_task_ids`:

```python
# Native list — PREFERRED
okto_pulse_create_card(
    board_id="...",
    title="My card",
    spec_id="...",
    labels=["bug, regression", "frontend (React, Vite)"],   # commas inside survive
    test_scenario_ids=["ts_abc", "ts_def"],
)

# Legacy string (JSON array) — works for older clients
okto_pulse_create_card(
    board_id="...",
    title="My card",
    spec_id="...",
    labels='["bug, regression", "frontend"]',               # commas inside survive
    test_scenario_ids='["ts_abc", "ts_def"]',
)

# Legacy string (pipe) — works when no item needs `|` or `,` inside
okto_pulse_create_card(
    board_id="...",
    title="My card",
    spec_id="...",
    labels="bug|frontend",
    test_scenario_ids="ts_abc|ts_def",
)
```

## Structured JSON Fields (object / array) — `dict | str` and `list[dict] | str`

The `*_json` parameters carry a structured **object** or **array of objects**, not a list of strings, so they use a different Union than the multi-value cluster above (spec d41c7209 — R3a):

| Parameter | Declared type | Schema (`tools/list`) | Domain |
|---|---|---|---|
| `request_body_json`, `response_success_json` (api_contract) | `dict \| str = ""` | `anyOf [object, string]` | single object |
| `data_contract_json` (integration_requirement) | `dict \| str = ""` | `anyOf [object, string]` | single object |
| `payload_json` (update_spec_entity / update_spec_api_contract) | `dict \| str = ""` | `anyOf [object, string]` | single object |
| `response_errors_json` (api_contract) | `list[dict] \| str = ""` | `anyOf [array-of-object, string]` | list of error objects |

The **LIST-vs-OBJECT asymmetry is intentional** (`response_errors` is a list; `request_body`/`response_success`/`data_contract`/`payload` are single objects) and is preserved by the schema. Each handler accepts a **native** `dict`/`list` (an `isinstance` branch skips `json.loads`) **or** the legacy JSON-string — both produce the same persisted structure. Parse errors on these fields keep the existing `{"error": "Invalid <param>: <exc>"}` shape (distinct from the multi-value `invalid_multi_value_input` envelope).

```python
# Native dict — PREFERRED (no json.dumps round-trip)
okto_pulse_add_api_contract(
    board_id="...", spec_id="...", name="Create user", method="POST", path="/users",
    request_body_json={"type": "object", "properties": {"email": {"type": "string"}}},
    response_errors_json=[{"code": 400, "message": "bad request"}],   # list of objects
)

# Legacy JSON-string — still accepted (additive)
okto_pulse_add_api_contract(
    board_id="...", spec_id="...", name="Create user", method="POST", path="/users",
    request_body_json='{"type": "object"}',
    response_errors_json='[{"code": 400}]',
)
```
