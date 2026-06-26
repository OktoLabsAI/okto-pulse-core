"""LayerResolver current-tree-v2 — maps the physical source tree to layers.

Spec #12, tr_c3f8fa6d + contract ``api_24add273``. Canonical mapping of the
*current* repository layout (NOT an aspirational one): ``core/domain`` and
``core/application`` are REAL layers; ``core/services`` / ``core/commands`` are
legacy/transitional debt and must never absorb the new architecture; only an
ABSENT ``core/ports`` is ``future_target``. Anything unmapped is
``unclassified`` and must surface with owner/promotion_criteria — never a silent
pass.

Pure ``pathlib`` string logic; importable in isolation.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath

# Layer identifiers (contract api_24add273 LayerResolver keys).
DOMAIN = "domain"
APPLICATION = "application"
INBOUND = "inbound"
OUTBOUND = "outbound"
EVENT_RUNTIME_MESSAGING = "event_runtime_messaging"
COMPOSITION = "composition"
PACKAGING_OPS_EDITION = "packaging_ops_edition"
LEGACY_APPLICATION_TRANSITIONAL_DEBT = "legacy_application_transitional_debt"
FUTURE_TARGET = "future_target"
UNCLASSIFIED = "unclassified"

LAYER_RESOLVER_VERSION = "current-tree-v2"

#: Exact-file rules win over prefix rules (e.g. ``core/app.py`` is composition,
#: not outbound). Ordered most-specific-first.
_EXACT_FILE_LAYERS: tuple[tuple[str, str], ...] = (
    ("okto_pulse/core/app.py", COMPOSITION),
    ("okto_pulse/core/composition.py", COMPOSITION),
    ("okto_pulse/core/discovery_params_schema.py", OUTBOUND),
    ("pyproject.toml", PACKAGING_OPS_EDITION),
)

#: Prefix rules, ordered most-specific-first so ``core/application`` is matched
#: before ``core`` and ``core/ports`` before any broader fallback.
_PREFIX_LAYERS: tuple[tuple[str, str], ...] = (
    ("okto_pulse/core/domain/", DOMAIN),
    ("okto_pulse/core/application/", APPLICATION),
    ("okto_pulse/core/ports/", FUTURE_TARGET),
    ("okto_pulse/core/inbound/", INBOUND),
    ("okto_pulse/core/api/", INBOUND),
    ("okto_pulse/core/mcp/", INBOUND),
    ("okto_pulse/core/repositories/", OUTBOUND),
    ("okto_pulse/core/infra/", OUTBOUND),
    ("okto_pulse/core/kg/", OUTBOUND),
    ("okto_pulse/core/telemetry/", OUTBOUND),
    ("okto_pulse/core/models/", OUTBOUND),
    ("okto_pulse/core/events/", EVENT_RUNTIME_MESSAGING),
    ("okto_pulse/core/services/", LEGACY_APPLICATION_TRANSITIONAL_DEBT),
    ("okto_pulse/core/commands/", LEGACY_APPLICATION_TRANSITIONAL_DEBT),
    ("okto_pulse/tools/", PACKAGING_OPS_EDITION),
    ("dist-info/", PACKAGING_OPS_EDITION),
)


@dataclass(frozen=True)
class LayerResolution:
    """The layer a path resolves to, plus governance for the soft layers."""

    layer: str
    #: True for ``future_target`` / ``unclassified`` — caller must attach
    #: owner/promotion_criteria and never promote silently.
    requires_governance: bool


class LayerResolver:
    """current-tree-v2 resolver. Stateless; ``resolve`` is pure."""

    version = LAYER_RESOLVER_VERSION

    @staticmethod
    def _normalise(path: str) -> str:
        text = PurePosixPath(str(path).replace("\\", "/")).as_posix().lstrip("./")
        # Drop a leading ``src/`` so wheel-relative and repo-relative paths agree.
        if text.startswith("src/"):
            text = text[len("src/") :]
        return text

    def resolve(self, path: str) -> LayerResolution:
        """Resolve a repo/wheel-relative path to a :class:`LayerResolution`."""
        norm = self._normalise(path)
        for prefix, layer in _EXACT_FILE_LAYERS:
            if norm == prefix:
                return LayerResolution(layer, requires_governance=False)
        for prefix, layer in _PREFIX_LAYERS:
            if norm.startswith(prefix):
                return LayerResolution(
                    layer, requires_governance=(layer == FUTURE_TARGET)
                )
        return LayerResolution(UNCLASSIFIED, requires_governance=True)

    def resolve_module(self, dotted: str) -> LayerResolution:
        """Resolve a dotted import (``okto_pulse.core.kg.x``) to a layer.

        Only ``okto_pulse`` internal modules are classifiable; third-party roots
        return ``unclassified`` (the import-boundary gate treats those via the
        framework allow/deny lists, not the layer matrix).
        """
        if not dotted.startswith("okto_pulse"):
            return LayerResolution(UNCLASSIFIED, requires_governance=False)
        as_path = dotted.replace(".", "/") + ".py"
        res = self.resolve(as_path)
        if res.layer is UNCLASSIFIED:
            # A package (``okto_pulse.core.kg``) has no ``.py`` suffix match; retry
            # as a directory prefix.
            return self.resolve(dotted.replace(".", "/") + "/")
        return res
