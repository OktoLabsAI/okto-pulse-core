"""Storage-independent contracts retained after removing the retired graph-runtime fixtures."""

from types import SimpleNamespace

import pytest

from okto_pulse.core.kg import cypher_templates as tpl, kg_service, search


def test_supersedence_template_default_and_invalid_type():
    assert tpl.supersedence_chain_template("Decision") == tpl.GET_SUPERSEDENCE_CHAIN
    with pytest.raises(ValueError):
        tpl.supersedence_chain_template("DROP TABLE x")


def test_chain_uses_the_requested_neutral_node_type(monkeypatch):
    calls = []

    def traverse(board, current, *, node_type):
        calls.append((board, current, node_type))
        target = {"learning_v3": "learning_v2", "learning_v2": "learning_v1"}.get(
            current
        )
        return [[target, target, None, None, None]] if target else []

    monkeypatch.setattr(
        kg_service,
        "_get_graph_store",
        lambda: SimpleNamespace(traverse_supersedence=traverse),
    )
    result = kg_service.KGService().get_supersedence_chain(
        "board", "learning_v3", node_type="Learning"
    )
    assert result["depth"] == 2
    assert [row["id"] for row in result["chain"]] == ["learning_v2", "learning_v1"]
    assert calls == [
        ("board", node, "Learning")
        for node in ["learning_v3", "learning_v2", "learning_v1"]
    ]


def test_empty_chain_keeps_the_decision_default(monkeypatch):
    calls = []

    def traverse(board, current, *, node_type):
        calls.append((board, current, node_type))
        return []

    monkeypatch.setattr(
        kg_service,
        "_get_graph_store",
        lambda: SimpleNamespace(traverse_supersedence=traverse),
    )
    assert kg_service.KGService().get_supersedence_chain("board", "absent") == {
        "chain": [],
        "depth": 0,
        "current_active": "absent",
    }
    assert calls == [("board", "absent", "Decision")]


def test_invalid_chain_type_refuses_before_resolving_a_provider(monkeypatch):
    monkeypatch.setattr(
        kg_service,
        "_get_graph_store",
        lambda: pytest.fail("unexpected provider access"),
    )
    with pytest.raises(kg_service.KGToolError) as caught:
        kg_service.KGService().get_supersedence_chain(
            "board", "x", node_type="NotAType"
        )
    assert caught.value.code == "invalid_node_type"


@pytest.mark.parametrize(
    "include_superseded,top_k,expected",
    [
        (False, 5, [1, 3, 5, 7, 9]),
        (True, 4, [0, 1, 2, 3]),
    ],
)
def test_semantic_port_preserves_active_default_and_opt_in(
    monkeypatch, include_superseded, top_k, expected
):
    calls = []

    def vector_search(**kwargs):
        calls.append(kwargs)
        return [
            {
                "node_id": f"learning_{i:03d}",
                "node_type": kwargs["node_type"],
                "title": f"t{i}",
                "source_artifact_ref": None,
                "similarity": 1.0 - i * 0.05,
            }
            for i in range(10)
            if kwargs["include_superseded"] or i % 2 == 1
        ][: kwargs["top_k"]]

    provider = SimpleNamespace(graph_store=SimpleNamespace(vector_search=vector_search))
    monkeypatch.setattr(search, "get_kg_registry", lambda: provider)
    options = {"include_superseded": True} if include_superseded else {}
    result = search.find_similar_nodes_by_type(
        "board", "Learning", [0.0] * 384, top_k=top_k, min_similarity=0.0, **options
    )
    assert calls[0]["include_superseded"] is include_superseded
    assert [row.graph_node_id for row in result] == [
        f"learning_{i:03d}" for i in expected
    ]


def test_active_filter_semantics_are_independent_of_the_storage_provider():
    assert (
        tpl.superseded_filter_clause("n")
        == "($include_superseded = true OR n.superseded_by IS NULL)"
    )
    assert tpl.ACTIVE_READ_TOMBSTONE_REASONS == {
        "policy_constraint_guideline_retired",
        "policy_constraint_guideline_superseded",
        "policy_constraint_rebuild_not_adopted",
        "policy_constraint_rule_removed",
        "policy_constraint_unlinked",
        "source_deleted",
        "source_projection_removed",
    }
    assert tpl.is_visible_in_active_reads("revision_superseded") is True
    for reason in tpl.ACTIVE_READ_TOMBSTONE_REASONS:
        assert tpl.is_visible_in_active_reads(reason) is False
    assert tpl.active_read_filter_clause("n") == (
        "(NOT (coalesce(n.revocation_reason, '') IN "
        "['policy_constraint_guideline_retired', 'policy_constraint_guideline_superseded', "
        "'policy_constraint_rebuild_not_adopted', 'policy_constraint_rule_removed', "
        "'policy_constraint_unlinked', 'source_deleted', 'source_projection_removed']))"
    )


def test_active_templates_apply_tombstones_to_every_observed_alias():
    templates_and_aliases = (
        (tpl.GET_DECISION_HISTORY, ("d",)),
        (tpl.GET_RELATED_CONTEXT, ("center", "hop1", "hop2")),
        (tpl.GET_SUPERSEDENCE_CHAIN, ("current", "next")),
        (tpl.FIND_CONTRADICTIONS_BY_NODE, ("a", "b")),
        (tpl.FIND_CONTRADICTIONS_ALL, ("a", "b")),
        (tpl.FIND_SIMILAR_DECISIONS_TEXT_FALLBACK, ("d",)),
        (tpl.EXPLAIN_CONSTRAINT, ("c",)),
        (tpl.EXPLAIN_CONSTRAINT_ORIGINS, ("c", "origin")),
        (tpl.EXPLAIN_CONSTRAINT_VIOLATIONS, ("c", "bug")),
        (tpl.LIST_ALTERNATIVES, ("d", "alt")),
        (tpl.GET_LEARNING_FROM_BUGS, ("l", "b")),
        (tpl.GET_ALL_NODES, ("n",)),
        (tpl.GET_ALL_NODES_BY_TYPE, ("n",)),
        (tpl.GET_ALL_NODES_AFTER_CURSOR, ("n",)),
        (tpl.GET_ALL_NODES_BY_TYPE_AFTER_CURSOR, ("n",)),
        (tpl.COUNT_ALL_NODES, ("n",)),
        (tpl.COUNT_ALL_NODES_BY_TYPE, ("n",)),
        (tpl.supersedence_chain_template("Constraint"), ("current", "next")),
    )
    for template, aliases in templates_and_aliases:
        for alias in aliases:
            assert tpl.active_read_filter_clause(alias) in template
