"""Test suite for Tier Power — Cypher safety, rate limit, NL search, schema info."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from okto_pulse.core.kg.tier_power import (
    TierPowerError,
    _auto_bound_var_length_path,
    _auto_inject_limit,
    _strip_comments,
    check_rate_limit,
    clamp_max_rows,
    clamp_timeout,
    compute_pattern_hash,
    execute_natural_query,
    get_schema_info,
    reset_rate_limiter_for_tests,
    validate_cypher_read_only,
)


@pytest.fixture(autouse=True)
def _reset_rate():
    reset_rate_limiter_for_tests()


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

    def test_clamp_timeout(self):
        assert clamp_timeout(None) == 5000
        assert clamp_timeout(100) == 1000
        assert clamp_timeout(50000) == 30000

    def test_clamp_max_rows(self):
        assert clamp_max_rows(None) == 1000
        assert clamp_max_rows(20000) == 10000


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
        assert len(info["stable_rel_types"]) == 10

    def test_vector_indexes_count(self):
        info = get_schema_info("board-x")
        assert len(info["vector_indexes"]) == 5

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
        from okto_pulse.core.kg.schema import bootstrap_board_graph
        bootstrap_board_graph("board-nl-test")
        result = execute_natural_query("board-nl-test", "test query")
        assert "nodes" in result
        assert "total_matches" in result


class TestMCPRegistration:
    def test_3_power_tools(self):
        import inspect
        from okto_pulse.core.mcp import kg_power_tools
        src = inspect.getsource(kg_power_tools.register_kg_power_tools)
        assert src.count("@mcp.tool()") == 3
