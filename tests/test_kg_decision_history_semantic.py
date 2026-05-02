"""Tests for kg_get_decision_history semantic enrichment (spec ab8f6cd6).

Validates:
- `use_semantic=True` (default) surfaces decisions whose titles don't match
  the topic literally but whose embeddings are close.
- `use_semantic=False` preserves legacy title-CONTAINS behavior.
- Semantic enrichment never reduces result set or reorders text-match hits.
- Failure of the semantic path does not block text-match results.
"""

import inspect
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


from okto_pulse.core.kg.kg_service import KGService


class TestSignatureContract:
    """The MCP tool signature exposes both knobs the spec promises."""

    def test_get_decision_history_accepts_use_semantic(self):
        sig = inspect.signature(KGService.get_decision_history)
        assert "use_semantic" in sig.parameters
        assert sig.parameters["use_semantic"].default is True

    def test_get_decision_history_accepts_min_similarity(self):
        sig = inspect.signature(KGService.get_decision_history)
        assert "min_similarity" in sig.parameters
        assert isinstance(sig.parameters["min_similarity"].default, float)


class TestStoreFindByTopicSemantic:
    """The new store method is part of the embedded provider surface."""

    def test_kuzu_store_exposes_method(self):
        from okto_pulse.core.kg.providers.embedded.kuzu_graph_store import KuzuGraphStore
        assert hasattr(KuzuGraphStore, "find_by_topic_semantic")
        sig = inspect.signature(KuzuGraphStore.find_by_topic_semantic)
        assert set(sig.parameters) >= {
            "self", "board_id", "node_type", "query_vec", "filters", "min_similarity"
        }


class TestMergePreservesTextOrderWhenSemanticFails:
    """Spec invariant: when semantic path fails (no index, lock, etc.),
    the returned list equals the text-only legacy result."""

    def test_merge_returns_text_rows_when_semantic_empty(self):
        # Unit-level verification of the merge logic: build a fake service
        # with a monkey-patched store that raises on semantic and returns a
        # known list on text.
        class FakeStore:
            def find_by_topic(self, board_id, node_type, topic, f):
                return [
                    ["dec-1", "Title", "Content", "ts", 0.9, 0.8, None],
                    ["dec-2", "Other", "Content", "ts", 0.9, 0.7, None],
                ]

            def find_by_topic_semantic(self, *a, **kw):
                raise RuntimeError("vector index missing — simulated")

        # Patch the service's _get_graph_store hook via module-level function
        # used inside kg_service.py.
        import okto_pulse.core.kg.kg_service as kg_service_mod

        original = kg_service_mod._get_graph_store
        kg_service_mod._get_graph_store = lambda: FakeStore()
        try:
            svc = KGService()
            # bypass auth via manual call — the service method doesn't call ACL
            out = svc.get_decision_history("bid", "foo", use_semantic=True)
            assert len(out) == 2
            assert [d["id"] for d in out] == ["dec-1", "dec-2"]
        finally:
            kg_service_mod._get_graph_store = original

    def test_merge_dedups_by_id(self):
        class FakeStore:
            def find_by_topic(self, board_id, node_type, topic, f):
                return [["dec-1", "T", "C", "ts", 0.9, 0.8, None]]

            def find_by_topic_semantic(self, *a, **kw):
                # Same dec-1 surfaced via semantic must not double
                return [
                    ["dec-1", "T", "C", "ts", 0.9, 0.8, None],
                    ["dec-42", "Paraphrase", "C", "ts", 0.9, 0.7, None],
                ]

        import okto_pulse.core.kg.kg_service as kg_service_mod

        original = kg_service_mod._get_graph_store
        kg_service_mod._get_graph_store = lambda: FakeStore()
        try:
            svc = KGService()
            out = svc.get_decision_history("bid", "foo", use_semantic=True)
            ids = [d["id"] for d in out]
            assert ids == ["dec-1", "dec-42"]
            assert len(ids) == len(set(ids))
        finally:
            kg_service_mod._get_graph_store = original


class TestUseSemanticFalseDisablesPath:
    def test_semantic_not_called_when_flag_off(self):
        calls = {"semantic": 0, "text": 0}

        class FakeStore:
            def find_by_topic(self, *a, **kw):
                calls["text"] += 1
                return [["dec-1", "T", "C", "ts", 0.9, 0.8, None]]

            def find_by_topic_semantic(self, *a, **kw):
                calls["semantic"] += 1
                return []

        import okto_pulse.core.kg.kg_service as kg_service_mod

        original = kg_service_mod._get_graph_store
        kg_service_mod._get_graph_store = lambda: FakeStore()
        try:
            svc = KGService()
            svc.get_decision_history("bid", "foo", use_semantic=False)
            assert calls["text"] == 1
            assert calls["semantic"] == 0
        finally:
            kg_service_mod._get_graph_store = original


class TestSemanticSkippedWhenMaxRowsReached:
    def test_no_semantic_when_text_fills_budget(self):
        calls = {"semantic": 0}

        class FakeStore:
            def find_by_topic(self, board_id, node_type, topic, f):
                # Fill up to max_rows
                return [
                    [f"dec-{i}", "T", "C", "ts", 0.9, 0.8, None]
                    for i in range(f.max_rows)
                ]

            def find_by_topic_semantic(self, *a, **kw):
                calls["semantic"] += 1
                return []

        import okto_pulse.core.kg.kg_service as kg_service_mod

        original = kg_service_mod._get_graph_store
        kg_service_mod._get_graph_store = lambda: FakeStore()
        try:
            svc = KGService()
            out = svc.get_decision_history("bid", "foo", max_rows=5)
            assert len(out) == 5
            assert calls["semantic"] == 0
        finally:
            kg_service_mod._get_graph_store = original
