---
version: "1.1"
---

# Tool docs — `api-contract`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## Data model — `contract_type`, the http method enum, and JSON field shapes

`ApiContract` carries a `contract_type` discriminator: one of `http` (default), `in_process`, `grpc`, or `event`. It controls the shape rules:

- **`contract_type: "http"` (the default)** — `method` is REQUIRED and must be a real HTTP verb (one of `GET, HEAD, POST, PUT, DELETE, CONNECT, OPTIONS, TRACE, PATCH`) and `path` is REQUIRED. A non-verb method (e.g. `"CALL"`) is rejected at the write boundary, not silently stored.
- **`contract_type != "http"` (`in_process` / `grpc` / `event`)** — `method` and `path` are OPTIONAL and the http verb enum does NOT apply. This lets you model an in-process function or an event contract without inventing a fake method/path.

**Non-HTTP interactions:** pass `contract_type="in_process"`, `"grpc"`, or `"event"` explicitly. Method tokens never infer or convert the interaction type.

**Per-field JSON shapes are ASYMMETRIC by design (documented, not normalized):**
- `request_body_json` → an OBJECT, e.g. `'{"name": "string"}'`.
- `response_success_json` → an OBJECT, e.g. `'{"id": "uuid"}'`.
- `response_errors_json` → a LIST, e.g. `'[{"status": 400, "detail": "..."}]'`. In short: **`response_errors` is a LIST while `request_body` and `response_success` are OBJECTs.**

**Errors:** a malformed contract shape returns the canonical `invalid_api_contract` error — never a raw Pydantic / `errors.pydantic.dev` surface. Reads and writes enforce the same shape. Invalid stored contracts are refused without conversion or repair.

## `okto_pulse_add_api_contract`

Add an API contract to a spec. API contracts define endpoints, request/response
shapes, and link to requirements and business rules.

Args:
    board_id: Board ID
    spec_id: Spec ID
    contract_type: http (default), in_process, grpc, or event
    method: Real HTTP verb when contract_type=http; optional for non-HTTP interactions.
    path: Endpoint path (required for http) or identifier; optional for non-http contracts (e.g. "/api/v1/users")
    description: What this endpoint does (optional)
    request_body_json: JSON string for request body schema (optional). Example: '{"name": "string", "email": "string"}'
    response_success_json: JSON string for success response schema (optional)
    response_errors_json: JSON string for error responses array (optional). Example: '[{"status": 400, "detail": "..."}]'
    linked_requirements: Pipe-separated exact FR/TR IDs.
        Example: "fr_login|tr_audit_events"
    linked_rules: Pipe-separated business rule IDs. Example: "br_abc123|br_def456"
    notes: Additional notes (optional)

Returns:
    JSON with the created API contract

## `okto_pulse_list_api_contracts`

List all API contracts for a spec with linked business rules resolved.

Args:
    board_id: Board ID
    spec_id: Spec ID

Returns:
    JSON array of API contracts with resolved linked rules and requirements

## `okto_pulse_remove_spec_entity` (`target_type="api_contract"`)

Use `okto_pulse_remove_spec_entity` with `target_type="api_contract"` and `entity_id` naming the target.
See `okto-pulse://reference/tool-families/spec_entity_remove` for parameters, authorization and results.

## `okto_pulse_update_api_contract`

Update an existing API contract on a spec.

Args:
    board_id: Board ID
    spec_id: Spec ID
    contract_id: API contract ID (e.g. "api_abc12345")
    contract_type: New explicit interaction type (optional, omitted = no change)
    method: New HTTP verb (optional, empty = no change)
    path: New path (optional)
    description: New description (optional, "CLEAR" to remove)
    request_body_json: New request body JSON (optional, "CLEAR" to remove)
    response_success_json: New success response JSON (optional, "CLEAR" to remove)
    response_errors_json: New error responses JSON (optional, "CLEAR" to remove)
    linked_requirements: Pipe-separated exact FR/TR IDs.
        "CLEAR" to remove all. Empty = no change.
    linked_rules: Pipe-separated rule IDs. "CLEAR" to remove all. Empty = no change.
    notes: New notes (optional, "CLEAR" to remove)

Returns:
    JSON with the updated API contract

## `okto_pulse_update_spec_api_contract`

Thin API Contract structured mutation wrapper.

This wrapper owns no authorization, persistence, impact or event logic; it only fixes
entity_type=api_contract and delegates to StructuredSpecEntityService.
