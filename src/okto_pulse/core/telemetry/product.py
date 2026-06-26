"""Product-aggregation module — R10-E Pass 2 FULFILLED.

``ProductTelemetryAggregator`` and its SQLite helpers have been REMOVED from core.
The concrete sqlite3 product aggregator is now owned exclusively by the Community
edition:
``okto_pulse.community.adapters.product_telemetry.CommunityProductTelemetryAggregator``.

The pure helpers were absorbed into
``okto_pulse.community.adapters._telemetry_helpers`` in R10-E Pass 1.
The core runtime obtains the aggregator through the
``product_aggregator_registry.get_product_aggregator`` factory.

The PURE family-vocabulary constants (``PRODUCT_AGGREGATE_FAMILIES`` /
``PRODUCT_METRIC_KEYS``) are a stable port contract that lives in
``okto_pulse.core.ports.telemetry`` and is re-exported here for backwards
compatibility with any consumer that previously imported them from this module.
"""

from __future__ import annotations

# Re-export the PURE vocabulary (no sqlite3 / FS / Community dependency).
from okto_pulse.core.ports.telemetry import (  # noqa: F401  (re-export)
    PRODUCT_AGGREGATE_FAMILIES,
    PRODUCT_METRIC_KEYS,
)

__all__ = [
    "PRODUCT_AGGREGATE_FAMILIES",
    "PRODUCT_METRIC_KEYS",
]
