---
version: "1.0"
---

# Tool docs — `analytics`

Full long-form documentation (args, returns, examples, enum prose) for `okto_pulse_*` tools in this family. The `tools/list` surface carries only the compact summary; read here on demand.

## `okto_pulse_get_analytics`

Get analytics data for a board. Supports multiple metric types.

Args:
    board_id: Board ID
    metric_type: Type of analytics — one of: overview, funnel, quality, velocity, coverage, agents
    from_date: Start date filter (ISO format, optional)
    to_date: End date filter (ISO format, optional)

Returns:
    JSON with analytics data for the requested metric type
