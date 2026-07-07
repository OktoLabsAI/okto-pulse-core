"""Core-owned Discovery intent seed catalog."""

from __future__ import annotations

from typing import Any

DEFAULT_DISCOVERY_INTENTS: tuple[dict[str, Any], ...] = (

    # --- Coverage & Tracing ---
    {
        "name": "coverage_for_fr",
        "label": "What covers this FR?",
        "description": (
            "Lists cards, scenarios and rules that cover a given functional "
            "requirement on the current board."
        ),
        "category": "coverage_tracing",
        "tool_binding": "okto_pulse_list_test_scenarios",
        "params_schema": {
            "fr_id": {
                "type": "spec_child_selector",
                "required": True,
                "label": "Functional requirement",
                "child_types": ["functional_requirement"],
            }
        },
        "renderer": "table",
        "min_permission": "kg.query.global",
    },
    {
        "name": "uncovered_requirements",
        "label": "Which requirements are uncovered?",
        "description": (
            "Surfaces FRs / NFRs / TRs that still have no linked card or "
            "test scenario — the coverage gap list."
        ),
        "category": "coverage_tracing",
        # Remap (ideação d1783b03): the previous binding to
        # okto_pulse_get_spec_context required a spec_id that the intent
        # did not collect, so the aggregated "uncovered across the board"
        # question was impossible to answer. The new binding is an
        # aggregator that walks every spec.
        "tool_binding": "okto_pulse_list_uncovered_requirements",
        "params_schema": None,
        "renderer": "table",
        "min_permission": "kg.query.global",
    },
    {
        "name": "scenarios_without_tasks",
        "label": "Which test scenarios have no task?",
        "description": (
            "Lists TestScenario nodes that are not linked to any "
            "implementation card — likely blindspots in the sprint plan."
        ),
        "category": "coverage_tracing",
        "tool_binding": "okto_pulse_list_test_scenarios",
        "params_schema": None,
        "renderer": "table",
        "min_permission": "kg.query.global",
    },

    # --- Decisions & History ---
    {
        "name": "decisions_superseded",
        "label": "Which decisions were superseded?",
        "description": (
            "Shows the supersedence chain for decisions on this board — "
            "useful for auditing why a choice was replaced."
        ),
        "category": "decisions_history",
        # Remap (ideação d1783b03): the original binding to
        # okto_pulse_kg_get_supersedence_chain requires a decision_id, but
        # the intent is "show all chains" — the new aggregator walks all
        # spec.decisions on the board.
        "tool_binding": "okto_pulse_list_supersedence_chains",
        "params_schema": None,
        "renderer": "table",
        "min_permission": "kg.query.global",
    },
    {
        "name": "contradictions_in_kg",
        "label": "Where does the knowledge graph contradict itself?",
        "description": (
            "Runs the contradiction detector over the current board's KG "
            "so mismatched rules, decisions and requirements can be "
            "reconciled before they bite."
        ),
        "category": "decisions_history",
        "tool_binding": "okto_pulse_kg_find_contradictions",
        "params_schema": None,
        "renderer": "table",
        "min_permission": "kg.query.global",
    },
    {
        "name": "key_decisions",
        "label": "Key Decisions",
        "description": (
            "The most relevant decisions on this board, ranked by a "
            "blend of relevance score and graph connectivity (number "
            "of connections) — highest first."
        ),
        "category": "decisions_history",
        "tool_binding": "okto_pulse_kg_list_key_decisions",
        "params_schema": None,
        "renderer": "table",
        "min_permission": "kg.query.global",
    },
    {
        "name": "decisions_by_topic",
        "label": "Find decisions about a topic",
        "description": (
            "Semantic search across decisions on this board for a given "
            "topic or phrase — useful when onboarding or revisiting a "
            "corner of the product."
        ),
        "category": "decisions_history",
        "tool_binding": "okto_pulse_kg_find_similar_decisions",
        "params_schema": {
            "topic": {
                "type": "text",
                "required": True,
                "label": "Topic / phrase",
            }
        },
        "renderer": "table",
        "min_permission": "kg.query.global",
    },

    # --- Dependencies & Blockers ---
    {
        "name": "blockers_current_sprint",
        "label": "What is blocking the current sprint?",
        "description": (
            "Lists cards blocked by unresolved dependencies in the active "
            "sprint so the team can focus on unblocking them first."
        ),
        "category": "dependencies_blockers",
        "tool_binding": "okto_pulse_list_blockers",
        "params_schema": None,
        "renderer": "table",
        "min_permission": "kg.query.global",
    },
    {
        "name": "dependencies_of_card",
        "label": "Who depends on this card?",
        "description": (
            "Reverse-lookup for a given card: which other cards or specs "
            "are waiting on it to land before they can move forward."
        ),
        "category": "dependencies_blockers",
        "tool_binding": "okto_pulse_get_card_dependencies",
        "params_schema": {
            "card_id": {
                "type": "entity_selector",
                "entity_type": "card",
                "required": True,
                "label": "Card",
            }
        },
        "renderer": "table",
        "min_permission": "kg.query.global",
    },

    # --- Similarity & Reuse ---
    {
        "name": "similar_nodes_to_text",
        "label": "Find similar nodes for a phrase",
        "description": (
            "Returns the most semantically similar nodes on the board for "
            "an arbitrary phrase — handy for duplicate detection and "
            "cross-referencing."
        ),
        "category": "similarity_reuse",
        # Remap (ideação 803c1fe1): original binding was
        # okto_pulse_kg_get_similar_nodes which requires session_id +
        # candidate_id from an active consolidation — inadequate for
        # user-facing queries. kg_query_natural accepts a free-form
        # `nl_query` and runs the same HNSW search plus string fallback.
        "tool_binding": "okto_pulse_kg_query_natural",
        "params_schema": {
            "query": {
                "type": "text",
                "required": True,
                "label": "Phrase",
            }
        },
        "renderer": "table",
        "min_permission": "kg.query.global",
    },
    {
        "name": "learning_from_bugs",
        "label": "Learnings extracted from bugs",
        "description": (
            "Surfaces the Learning nodes the KG has distilled from closed "
            "Bug artifacts — the institutional memory of what broke and "
            "why."
        ),
        "category": "similarity_reuse",
        "tool_binding": "okto_pulse_kg_get_learning_from_bugs",
        "params_schema": None,
        "renderer": "table",
        "min_permission": "kg.query.global",
    },
    {
        "name": "learnings_by_relevance",
        "label": "Learnings by relevance",
        "description": (
            "Lists every Learning node on this board ordered by "
            "relevance score, highest first — the institutional memory "
            "ranked by how alive each lesson still is."
        ),
        "category": "similarity_reuse",
        "tool_binding": "okto_pulse_kg_list_learnings_by_relevance",
        "params_schema": None,
        "renderer": "table",
        "min_permission": "kg.query.global",
    },

    # --- Activity & Recency ---
    {
        "name": "recent_activity",
        "label": "What changed recently on this board?",
        "description": (
            "Rolls up the activity log for the current board — recent "
            "cards, status moves, consolidations, evaluations."
        ),
        "category": "activity_recency",
        "tool_binding": "okto_pulse_get_activity_log",
        "params_schema": None,
        "renderer": "table",
        "min_permission": "kg.query.global",
    },
    {
        "name": "my_mentions",
        "label": "Where was I mentioned?",
        "description": (
            "Lists the comments and QA items where the current user was "
            "@mentioned, so replies don't get lost."
        ),
        "category": "activity_recency",
        "tool_binding": "okto_pulse_list_my_mentions",
        "params_schema": None,
        "renderer": "table",
        "min_permission": "kg.query.global",
    },
)
