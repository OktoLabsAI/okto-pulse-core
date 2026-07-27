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
    from_date: Start bound (ISO date or timestamp, optional). A date-only value
        is interpreted as 00:00:00 UTC.
    to_date: Exclusive end bound (ISO date or timestamp, optional). A date-only
        value is normalized to 00:00:00 UTC on the following day; no
        23:59:59.999999 sentinel is used. A full timestamp preserves its exact
        instant. The selected interval is always half-open: ``[from, to)``.

Returns:
    JSON with analytics data for the requested metric type. Card counts use one
    disjoint category per card with precedence ``bug → test → implementation``;
    therefore implementation + test + bug always equals the selected total.
