---
version: "1.0"
---

# Multi-Value Parameters — Supported Input Shapes

Most MCP tool arguments documented as multi-value (labels, ids, linked_criteria, linked_requirements, test_scenario_ids, tags, card_ids, and the like) are parsed by `okto_pulse.core.mcp.helpers.coerce_to_list_str`. Migrated tools declare their parameters as `list[str] | str = ""` so FastMCP's Pydantic Union dispatch picks the right shape at the framework boundary. **For migrated strict tools, comma-only input is rejected.**

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

**Error behaviour (returned as `{"error": "Invalid <param>: <message>"}`):**
- Malformed JSON → `"malformed JSON for multi-value param: <reason> (at pos N)"`.
- JSON decoded to a non-list (e.g. `{"a": 1}`) → `"malformed multi-value: expected list, got dict"`.
- JSON array with a non-string item (e.g. `["ok", 42]`) → `"malformed multi-value: expected string items, got int at index 1"`.
- Comma-only string under `strict_mode=True` → `"multi-value input must be a JSON array ... or pipe-separated ..."`.

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
