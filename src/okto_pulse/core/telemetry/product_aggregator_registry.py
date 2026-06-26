"""Product-aggregation factory registry (spec R10-D/R10-E fulfilled).

The composition root (the Community edition) registers a factory that builds a
:class:`~okto_pulse.core.ports.telemetry.ProductAggregationPort` for a given
``settings`` / ``metrics_dir``. The core telemetry runtime obtains the aggregator
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
import threading
from pathlib import Path
from typing import Any, Callable

from okto_pulse.core.ports.telemetry import ProductAggregationPort

logger = logging.getLogger("okto_pulse.telemetry.product_aggregator")

#: A factory: ``(settings, metrics_dir: Path) -> ProductAggregationPort``.
ProductAggregatorFactory = Callable[[Any, Path], ProductAggregationPort]

_product_aggregator_factory: ProductAggregatorFactory | None = None
_lock = threading.Lock()


def register_product_aggregator_factory(factory: ProductAggregatorFactory) -> None:
    """Register the edition's product-aggregator factory (composition root).
    Idempotent overwrite; thread-safe."""
    global _product_aggregator_factory
    with _lock:
        _product_aggregator_factory = factory


def get_product_aggregator(settings: Any, metrics_dir: Any) -> ProductAggregationPort:
    """Build the product aggregator via the registered factory (fail-closed: R10-E Pass 2).

    R10-E Pass 2: the register-before-remove fallback that built
    ``ProductTelemetryAggregator`` is removed. The composition root MUST register
    a factory via :func:`register_product_aggregator_factory` before any aggregation.
    Calling without a registered factory raises ``RuntimeError`` and emits a
    structured signal (secret-free, bounded).
    """
    base = Path(metrics_dir)
    if _product_aggregator_factory is not None:
        return _product_aggregator_factory(settings, base)
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
    global _product_aggregator_factory
    with _lock:
        _product_aggregator_factory = None


__all__ = [
    "ProductAggregatorFactory",
    "register_product_aggregator_factory",
    "get_product_aggregator",
    "reset_product_aggregator_factory_for_tests",
]
