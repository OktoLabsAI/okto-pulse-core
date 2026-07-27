"""Telemetry store module — R10-E Pass 2 FULFILLED.

``LocalTelemetryStore`` and its helpers (``parse_iso`` / ``ensure_inside`` /
``add_guided_help_counts``) have been REMOVED from core. The concrete append-only
JSONL store is now owned exclusively by the Community edition:
``okto_pulse.community.adapters.telemetry_store.CommunityLocalTelemetryStore``.

The pure helpers were absorbed into
``okto_pulse.community.adapters._telemetry_helpers`` in R10-E Pass 1.
The core runtime obtains the store through the
``event_store_registry.get_telemetry_event_store`` factory.
"""
