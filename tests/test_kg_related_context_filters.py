"""Tests for kg_get_related_context filter knobs (spec a5278df8).

Validates:
- `direction` parameter accepts {both, incoming, outgoing} and rejects other.
- `max_depth` accepts {1, 2} and rejects other.
- `rel_types` filters flow through to the store layer.
- When no filters are passed, the legacy `find_by_artifact` path is preserved
  (cache key parity with pre-change behavior).
"""

import inspect
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest

from okto_pulse.core.kg.kg_service import KGService


class TestSignatureContract:
    def test_service_signature(self):
        sig = inspect.signature(KGService.get_related_context)
        params = sig.parameters
        assert "rel_types" in params
        assert "direction" in params
        assert "max_depth" in params
        assert params["direction"].default == "both"
        assert params["max_depth"].default == 2


class TestValidation:
    def test_rejects_invalid_direction(self):
        svc = KGService()
        with pytest.raises(ValueError, match="direction"):
            svc.get_related_context("bid", "aid", direction="sideways")

    def test_rejects_invalid_max_depth(self):
        svc = KGService()
        with pytest.raises(ValueError, match="max_depth"):
            svc.get_related_context("bid", "aid", max_depth=5)


class TestRoutingToFilteredPath:
    """When ANY filter departs from defaults, the filtered store method is
    invoked; otherwise the legacy path is preserved."""

    def _setup_fake_store(self, calls):
        class FakeStore:
            def find_by_artifact(self, board_id, artifact_id, filters):
                calls["legacy"] += 1
                return []

            def find_by_artifact_filtered(
                self, board_id, artifact_id, filters,
                *, rel_types=None, direction="both", max_depth=2,
            ):
                calls["filtered"] += 1
                calls["last_rel_types"] = rel_types
                calls["last_direction"] = direction
                calls["last_max_depth"] = max_depth
                return []

        return FakeStore()

    def test_defaults_route_to_legacy(self):
        calls = {"legacy": 0, "filtered": 0}
        store = self._setup_fake_store(calls)

        import okto_pulse.core.kg.kg_service as kg_service_mod
        original = kg_service_mod._get_graph_store
        kg_service_mod._get_graph_store = lambda: store
        try:
            svc = KGService()
            svc.get_related_context("bid", "aid")
            assert calls["legacy"] == 1
            assert calls["filtered"] == 0
        finally:
            kg_service_mod._get_graph_store = original

    def test_rel_types_routes_to_filtered(self):
        calls = {"legacy": 0, "filtered": 0}
        store = self._setup_fake_store(calls)

        import okto_pulse.core.kg.kg_service as kg_service_mod
        original = kg_service_mod._get_graph_store
        kg_service_mod._get_graph_store = lambda: store
        try:
            svc = KGService()
            svc.get_related_context("bid", "aid", rel_types=["supersedes"])
            assert calls["filtered"] == 1
            assert calls["last_rel_types"] == ["supersedes"]
        finally:
            kg_service_mod._get_graph_store = original

    def test_direction_routes_to_filtered(self):
        calls = {"legacy": 0, "filtered": 0}
        store = self._setup_fake_store(calls)

        import okto_pulse.core.kg.kg_service as kg_service_mod
        original = kg_service_mod._get_graph_store
        kg_service_mod._get_graph_store = lambda: store
        try:
            svc = KGService()
            svc.get_related_context("bid", "aid", direction="outgoing")
            assert calls["filtered"] == 1
            assert calls["last_direction"] == "outgoing"
        finally:
            kg_service_mod._get_graph_store = original

    def test_max_depth_1_routes_to_filtered(self):
        calls = {"legacy": 0, "filtered": 0}
        store = self._setup_fake_store(calls)

        import okto_pulse.core.kg.kg_service as kg_service_mod
        original = kg_service_mod._get_graph_store
        kg_service_mod._get_graph_store = lambda: store
        try:
            svc = KGService()
            svc.get_related_context("bid", "aid", max_depth=1)
            assert calls["filtered"] == 1
            assert calls["last_max_depth"] == 1
        finally:
            kg_service_mod._get_graph_store = original


class TestStoreMethodSignature:
    def test_kuzu_store_exposes_method(self):
        from okto_pulse.core.kg.providers.embedded.kuzu_graph_store import KuzuGraphStore
        assert hasattr(KuzuGraphStore, "find_by_artifact_filtered")
        sig = inspect.signature(KuzuGraphStore.find_by_artifact_filtered)
        assert set(sig.parameters) >= {
            "self", "board_id", "artifact_id", "filters",
            "rel_types", "direction", "max_depth",
        }
