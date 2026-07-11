"""Product-aggregation factory registry (spec R10-D/R10-E fulfilled).

The composition root (the Community edition) registers a factory that builds a
:class:`~okto_pulse.core.ports.telemetry.ProductAggregationPort` for a given
settings object and opaque state reference. The core telemetry runtime obtains the aggregator
ONLY through :func:`get_product_aggregator` — it never references the concrete
``ProductTelemetryAggregator`` (removed in R10-E Pass 2; Community owns it as
``community.adapters.product_telemetry.CommunityProductTelemetryAggregator``).

R10-E Pass 2 fulfilled: the register-before-remove fallback that instantiated
``ProductTelemetryAggregator`` is REMOVED (``_CoreProductAggregatorShim`` also
removed). The registry is now FAIL-CLOSED: no factory registered → structured
error, no concrete instantiation.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from okto_pulse.core.ports.telemetry import ProductAggregationPort
from okto_pulse.core.runtime_context import register_runtime_value, reset_runtime_values, resolve_runtime_value

logger = logging.getLogger("okto_pulse.telemetry.product_aggregator")

#: A factory: ``(settings, state_ref: str) -> ProductAggregationPort``.
ProductAggregatorFactory = Callable[[Any, str], ProductAggregationPort]

_RUNTIME_KEY = "telemetry.product_aggregator.factory"


def register_product_aggregator_factory(factory: ProductAggregatorFactory) -> None:
    """Register the edition's product-aggregator factory (composition root).
    Idempotent overwrite; thread-safe."""
    register_runtime_value(_RUNTIME_KEY, factory)


def get_product_aggregator(settings: Any, state_ref: Any) -> ProductAggregationPort:
    """Build the product aggregator via the registered factory (fail-closed: R10-E Pass 2).

    R10-E Pass 2: the register-before-remove fallback that built
    ``ProductTelemetryAggregator`` is removed. The composition root MUST register
    a factory via :func:`register_product_aggregator_factory` before any aggregation.
    Calling without a registered factory raises ``RuntimeError`` and emits a
    structured signal (secret-free, bounded).
    """
    factory = resolve_runtime_value(_RUNTIME_KEY)
    if factory is not None:
        return factory(settings, str(state_ref))
    # R10-E Pass 2 fail-closed: no provider → structured error (never instantiates
    # a concrete class). The composition root (Community) registers before any use.
    logger.error(
        "product aggregator registry has no factory — composition root must "
        "register before use (R10-E Pass 2 fulfilled)",
        extra={
            "metric_name": "product_aggregator_no_provider_total",
            "component": "product_aggregator_registry",
            "outcome": "fail_closed",
            "reason": "no_factory_registered",
        },
    )
    raise RuntimeError(
        "No ProductAggregationPort factory registered. "
        "Call register_product_aggregator_factory before any aggregation. "
        "(R10-E Pass 2: ProductTelemetryAggregator removed from core; Community owns it.)"
    )


def reset_product_aggregator_factory_for_tests() -> None:
    """Drop the registered factory (tests only)."""
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "ProductAggregatorFactory",
    "register_product_aggregator_factory",
    "get_product_aggregator",
    "reset_product_aggregator_factory_for_tests",
]
