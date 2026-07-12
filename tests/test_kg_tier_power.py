"""Test suite for Tier Power — Cypher safety, rate limit, NL search, schema info."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from okto_pulse.core.kg.tier_power import (
    TierPowerError,
    _apply_canonical_projection,
    _auto_bound_var_length_path,
    _auto_inject_limit,
    check_rate_limit,
    clamp_max_rows,
    clamp_timeout,
    compute_pattern_hash,
    execute_natural_query,
    get_schema_info,
    reset_rate_limiter_for_tests,
    validate_cypher_read_only,
)
from kg_registry_testing import RealBoardCypherExecutorForTests, configure_test_kg_registry


@pytest.fixture(autouse=True)
def _reset_rate():
    # reset_rate_limiter_for_tests() resets the whole KG registry; R-P2-03 no
    # longer lazy-builds defaults, so re-configure the embedded fakes explicitly
    # (this autouse fixture runs after the conftest one, so it must restore a
    # configured registry for the tests that read get_kg_registry()).
    reset_rate_limiter_for_tests()
    configure_test_kg_registry(cypher_executor=RealBoardCypherExecutorForTests())


class TestCypherSafety:
    def test_valid_read_passes(self):
        validate_cypher_read_only("MATCH (n) RETURN n LIMIT 10")

    def test_create_rejected(self):
        with pytest.raises(TierPowerError) as exc:
            validate_cypher_read_only("CREATE (n:X {id: 'a'})")
        assert exc.value.code == "unsafe_cypher"

    def test_delete_rejected(self):
        with pytest.raises(TierPowerError):
            validate_cypher_read_only("MATCH (n) DELETE n")

    def test_set_rejected(self):
        with pytest.raises(TierPowerError):
            validate_cypher_read_only("MATCH (n) SET n.x = 1")

    def test_merge_rejected(self):
        with pytest.raises(TierPowerError):
            validate_cypher_read_only("MERGE (n:X {id: 'a'})")

    def test_comment_stripping(self):
        validate_cypher_read_only("MATCH (n) // CREATE\nRETURN n")

    def test_block_comment_stripping(self):
        validate_cypher_read_only("MATCH (n) /* DELETE */ RETURN n")

    def test_injection_in_string_literal_safe(self):
        validate_cypher_read_only("MATCH (n) WHERE n.title = 'CREATE' RETURN n")


class TestSafetyRails:
    def test_auto_inject_limit(self):
        q = _auto_inject_limit("MATCH (n) RETURN n", 500)
        assert "LIMIT 500" in q

    def test_no_double_limit(self):
        q = _auto_inject_limit("MATCH (n) RETURN n LIMIT 10", 500)
        assert "LIMIT 500" not in q

    def test_auto_bound_path(self):
        q = _auto_bound_var_length_path("(a)-[*]->(b)")
        assert "*..20" in q

    def test_auto_bound_path_preserves_count_star(self):
        q = _auto_bound_var_length_path("MATCH (n) RETURN count(*) AS total")
        assert "count(*)" in q
        assert "count(*..20)" not in q

    def test_auto_bound_path_preserves_aggregate_variants(self):
        q = _auto_bound_var_length_path(
            "MATCH (a)-[r]->(b) RETURN count(r), count(DISTINCT r), collect(r), collect(DISTINCT r)"
        )
        assert "count(r)" in q
        assert "count(DISTINCT r)" in q
        assert "collect(r)" in q
        assert "collect(DISTINCT r)" in q

    def test_auto_bound_path_preserves_bounded_relationship_path(self):
        q = _auto_bound_var_length_path("MATCH p=(a)-[*1..3]->(b) RETURN p")
        assert "[*1..3]" in q

    def test_auto_bound_path_caps_open_upper_relationship_path(self):
        q = _auto_bound_var_length_path("MATCH p=(a)-[*1..]->(b) RETURN p")
        assert "[*1..20]" in q

    def test_auto_bound_path_does_not_rewrite_star_in_string_literal(self):
        q = _auto_bound_var_length_path("MATCH (n) WHERE n.title = '[*]' RETURN count(*)")
        assert "'[*]'" in q
        assert "count(*)" in q

    def test_auto_inject_limit_ignores_limit_in_string_literal(self):
        q = _auto_inject_limit("MATCH (n) WHERE n.title = 'LIMIT' RETURN n", 500)
        assert q.endswith("LIMIT 500")

    def test_clamp_timeout(self):
        assert clamp_timeout(None) == 5000
        assert clamp_timeout(100) == 1000
        assert clamp_timeout(50000) == 30000

    def test_clamp_max_rows(self):
        assert clamp_max_rows(None) == 1000
        assert clamp_max_rows(20000) == 10000

    def test_canonical_projection_omits_working_rows_when_layer_visible(self):
        result = _apply_canonical_projection(
            {
                "rows": [
                    {"id": "c1", "graph_layer": "canonical"},
                    {"id": "w1", "graph_layer": "working"},
                    {"id": "legacy"},
                ],
                "row_count": 3,
            },
            include_working=False,
        )
        assert [row["id"] for row in result["rows"]] == ["c1", "legacy"]
        assert result["query_state"] == "canonical_only"
        assert result["canonical_filter_enforced"] is True
        assert result["working_omitted_count"] == 1

    def test_canonical_projection_can_include_working_explicitly(self):
        result = _apply_canonical_projection(
            {"rows": [{"id": "w1", "graph_layer": "working"}], "row_count": 1},
            include_working=True,
        )
        assert result["row_count"] == 1
        assert result["query_state"] == "canonical_and_working"

    def test_cypher_rewrite_filters_named_nodes_before_executor(self):
        from okto_pulse.core.kg.tier_power import execute_cypher_read_only

        class FakeExecutor:
            seen = ""

            def execute_read_only(self, board_id, cypher, params=None, *, max_rows=1000):
                self.seen = cypher
                return {"rows": [[{"id": "n1"}]], "row_count": 1}

        fake = FakeExecutor()
        configure_test_kg_registry(cypher_executor=fake)

        result = execute_cypher_read_only("board-x", "MATCH (n) RETURN n")

        assert fake.seen == (
            "MATCH (n) WHERE n.graph_layer = 'canonical' RETURN n\nLIMIT 1000"
        )
        assert result["canonical_filter_enforced"] is True
        assert result["canonical_filter_mode"] == "cypher_rewrite"

    def test_cypher_rewrite_preserves_existing_where_with_clause_spacing(self):
        from okto_pulse.core.kg.tier_power import execute_cypher_read_only

        class FakeExecutor:
            seen = ""

            def execute_read_only(self, board_id, cypher, params=None, *, max_rows=1000):
                self.seen = cypher
                return {"rows": [], "row_count": 0}

        fake = FakeExecutor()
        configure_test_kg_registry(cypher_executor=fake)

        execute_cypher_read_only(
            "board-x",
            "MATCH (n) WHERE n.title = 'x' RETURN n",
        )

        assert fake.seen == (
            "MATCH (n) WHERE n.graph_layer = 'canonical' "
            "AND (n.title = 'x') RETURN n\nLIMIT 1000"
        )

    def test_cypher_rewrite_preserves_starts_with_operator(self):
        from okto_pulse.core.kg.tier_power import execute_cypher_read_only

        class FakeExecutor:
            seen = ""

            def execute_read_only(self, board_id, cypher, params=None, *, max_rows=1000):
                self.seen = cypher
                return {"rows": [], "row_count": 0}

        fake = FakeExecutor()
        configure_test_kg_registry(cypher_executor=fake)

        execute_cypher_read_only(
            "board-x",
            "MATCH (n) WHERE n.source_artifact_ref STARTS WITH 'spec:abc' RETURN n",
        )

        assert fake.seen == (
            "MATCH (n) WHERE n.graph_layer = 'canonical' "
            "AND (n.source_artifact_ref STARTS WITH 'spec:abc') RETURN n\nLIMIT 1000"
        )

    def test_cypher_rewrite_fails_closed_for_anonymous_nodes(self):
        from okto_pulse.core.kg.tier_power import execute_cypher_read_only

        with pytest.raises(TierPowerError) as exc:
            execute_cypher_read_only("board-x", "MATCH (:Spec) RETURN count(*)")

        assert exc.value.code == "canonical_filter_unenforceable"

    def test_cypher_rewrite_fails_closed_for_variable_length_paths(self):
        from okto_pulse.core.kg.tier_power import execute_cypher_read_only

        with pytest.raises(TierPowerError) as exc:
            execute_cypher_read_only("board-x", "MATCH p=(a)-[*]->(b) RETURN p")

        assert exc.value.code == "canonical_filter_unenforceable"

    def test_cypher_rewrite_can_include_working_without_filter(self):
        from okto_pulse.core.kg.tier_power import execute_cypher_read_only

        class FakeExecutor:
            seen = ""

            def execute_read_only(self, board_id, cypher, params=None, *, max_rows=1000):
                self.seen = cypher
                return {
                    "rows": [{"id": "w1", "graph_layer": "working"}],
                    "row_count": 1,
                }

        fake = FakeExecutor()
        configure_test_kg_registry(cypher_executor=fake)

        result = execute_cypher_read_only(
            "board-x",
            "MATCH (:Spec) RETURN count(*)",
            include_working=True,
        )

        assert "graph_layer = 'canonical'" not in fake.seen
        assert result["query_state"] == "canonical_and_working"


class TestRateLimit:
    def test_allows_30_then_rejects(self):
        for _ in range(30):
            check_rate_limit("agent-rl")
        with pytest.raises(TierPowerError) as exc:
            check_rate_limit("agent-rl")
        assert exc.value.code == "rate_limited"
        assert "retry_after" in exc.value.details

    def test_different_agents_independent(self):
        for _ in range(30):
            check_rate_limit("agent-a")
        check_rate_limit("agent-b")


class TestPatternHash:
    def test_same_shape_same_hash(self):
        h1 = compute_pattern_hash("MATCH (n) WHERE n.id = 'abc' RETURN n")
        h2 = compute_pattern_hash("MATCH (n) WHERE n.id = 'xyz' RETURN n")
        assert h1 == h2

    def test_different_shape_different_hash(self):
        h1 = compute_pattern_hash("MATCH (n) RETURN n")
        h2 = compute_pattern_hash("MATCH (n)-[r]->(m) RETURN m")
        assert h1 != h2


class TestSchemaInfo:
    def test_stable_types_count(self):
        info = get_schema_info("board-x")
        assert len(info["stable_node_types"]) == 11
        # 10 REL_TYPES single-pair entries + 6 MULTI_REL_TYPES names (implements,
        # relates_to, belongs_to, originates_from, covered_by, supersedes).
        # S-KG-01 added the additive `relates_to` Learning taxonomy entry;
        # spec MKG-D-S1 promoted `supersedes` to a universal multi-pair edge
        # (walkable chain for all node types, +1).
        assert len(info["stable_rel_types"]) == 16
        rel_names = {rel["name"] for rel in info["stable_rel_types"]}
        assert {"belongs_to", "originates_from", "covered_by"} <= rel_names

    def test_vector_indexes_count(self):
        info = get_schema_info("board-x")
        assert len(info["vector_indexes"]) == 9

    def test_internal_hidden_by_default(self):
        info = get_schema_info("board-x")
        assert "internal_node_types" not in info

    def test_internal_exposed_with_flag(self):
        info = get_schema_info("board-x", include_internal=True)
        assert "internal_node_types" in info
        assert info["internal_node_types"][0]["name"] == "BoardMeta"


class TestNLQuery:
    def test_query_returns_dict(self):
        import tempfile
        os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_tp_"))
        from kg_schema_testing import bootstrap_board_graph
        bootstrap_board_graph("board-nl-test")
        result = execute_natural_query("board-nl-test", "test query")
        assert "nodes" in result
        assert "total_matches" in result

    def test_query_exact_fallback_finds_bug_without_vector_index_hit(self):
        import tempfile
        os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_tp_"))
        from kg_schema_testing import bootstrap_board_graph, open_board_connection

        board_id = "board-nl-bug-test"
        bootstrap_board_graph(board_id)
        with open_board_connection(board_id) as (_db, conn):
            conn.execute(
                "CREATE (n:Bug {id: 'bug_exact_1', title: 'Exact bug title', "
                "content: 'Observed failure', source_artifact_ref: 'bug:exact-1', "
                "source_confidence: 1.0, relevance_score: 0.5})"
            )

        # This case exercises the exact-match fallback retrieval, not the layer
        # filter. The fixture Bug is created without a graph_layer (legacy/
        # un-stamped), so under the spec e2598178 contract it is legacy_unknown
        # and surfaces only under graph_layer='all' (default canonical fails
        # closed on un-stamped nodes — no silent fallback to old behavior).
        result = execute_natural_query(
            board_id,
            "Exact bug title",
            min_confidence=0.0,
            graph_layer="all",
        )

        assert any(
            node["node_id"] == "bug_exact_1" and node["node_type"] == "Bug"
            for node in result["nodes"]
        )


class TestMCPRegistration:
    def test_power_tools_registered(self):
        import inspect
        from okto_pulse.core.mcp import kg_power_tools
        src = inspect.getsource(kg_power_tools.register_kg_power_tools)
        # 5 originais + okto_pulse_kg_provenance_drift (spec MKG-B-S1 FR7).
        assert src.count("@mcp.tool()") == 6
