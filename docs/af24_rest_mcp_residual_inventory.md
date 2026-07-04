# AF24 REST/MCP Residual Inventory

owner: core

AF24 reviewed REST/MCP task-to-scenario linking, activity summaries, and coverage
metrics for duplicated transport behavior. The rule is: application behavior
belongs in core use cases/services; REST and MCP adapters only map auth,
permissions, input/output envelopes, and transport-specific error shapes.

## Inventory

| Surface | Status | Owner | Evidence | Follow-up |
| --- | --- | --- | --- | --- |
| `src/okto_pulse/core/api/specs.py:545` REST task-to-scenario link | Delegated | core | Calls `LinkTaskToScenarioUseCase` through the REST adapter. | no open AF24 residual |
| `src/okto_pulse/core/mcp/server.py:_link_task_to_scenario_internal` MCP task-to-scenario link | Delegated by AF24 | core | Calls `LinkTaskToScenarioUseCase`; MCP preserves the legacy JSON envelope and saturation response. | no open AF24 residual |
| `src/okto_pulse/core/services/activity_log.py:67` activity summaries | Already shared | core | `activity_log_summary` is the shared deterministic summary builder; REST/MCP consumers do not own separate summary rules. | no open AF24 residual |
| `src/okto_pulse/core/services/analytics_service.py:737` coverage summaries | Already shared | core | `spec_coverage_summary` is the canonical coverage/saturation source for REST/MCP projections. | no open AF24 residual |

## Result

REST/MCP residuals found in AF24 are either delegated to shared core services or
documented above with file:line and owner. No additional AF24 follow-up is open
for `activity_log_summary` or `spec_coverage_summary`.
