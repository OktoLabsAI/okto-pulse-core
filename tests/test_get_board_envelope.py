"""Unit tests for Story 2 get_board envelope helpers.

Covers:
- `_parse_include(include: str) -> set[str]` parser (server.py:1541)
- `_BOARD_INCLUDE_KEYS` whitelist (server.py:1538)
- `BoardResponse.counts` schema field (Optional dict[str, int])

The tool `okto_pulse_get_board` itself is integration-tested via manual smoke
(TC3a, see ts_33f1852e/ts_9549b8d9/ts_f2570f89). These unit tests target the
pure helpers that drive the envelope shape.
"""

from __future__ import annotations

import pytest

from okto_pulse.core.mcp.server import _BOARD_INCLUDE_KEYS, _parse_include


# ---------------------------------------------------------------------------
# _parse_include — parser unit tests
# ---------------------------------------------------------------------------


def test_empty_string_returns_empty_set():
    """No include token → no inlined collections (default minimal envelope)."""
    assert _parse_include("") == set()


def test_star_expands_to_all_keys():
    """`*` is the canonical opt-in for the full legacy shape (FR2)."""
    assert _parse_include("*") == set(_BOARD_INCLUDE_KEYS)


def test_star_with_surrounding_whitespace_still_expands():
    """Whitespace tolerance around the wildcard."""
    assert _parse_include("  *  ") == set(_BOARD_INCLUDE_KEYS)


def test_single_known_token_inlines_only_that_collection():
    """include='ideations' → {'ideations'} (FR3 subset semantic)."""
    assert _parse_include("ideations") == {"ideations"}


def test_csv_of_known_tokens_inlines_subset():
    """include='ideations,specs' → {'ideations','specs'}."""
    assert _parse_include("ideations,specs") == {"ideations", "specs"}


def test_csv_tolerates_whitespace_between_tokens():
    """`tok.strip()` allows ` ideations , specs ` style input."""
    assert _parse_include(" ideations , specs ") == {"ideations", "specs"}


def test_unknown_tokens_are_silently_dropped():
    """include='foo' → set() (intersect with whitelist, FR4 + BR Tokens dropped)."""
    assert _parse_include("foo") == set()


def test_mixed_known_and_unknown_tokens_keeps_only_known():
    """include='foo,ideations,bar' → {'ideations'}."""
    assert _parse_include("foo,ideations,bar") == {"ideations"}


def test_empty_tokens_in_csv_are_ignored():
    """include=',,,' → set() (empty tokens skipped by `if tok.strip()`)."""
    assert _parse_include(",,,") == set()


def test_duplicate_tokens_are_deduplicated():
    """include='ideations,ideations,specs' → {'ideations','specs'} (set semantics)."""
    assert _parse_include("ideations,ideations,specs") == {"ideations", "specs"}


def test_all_four_keys_in_csv_equivalent_to_star():
    """include='ideations,specs,cards,agents' must equal include='*'."""
    full_csv = ",".join(_BOARD_INCLUDE_KEYS)
    assert _parse_include(full_csv) == _parse_include("*")


@pytest.mark.parametrize("token", _BOARD_INCLUDE_KEYS)
def test_each_whitelist_key_round_trips(token):
    """Sanity: every key in the whitelist must parse to its singleton set."""
    assert _parse_include(token) == {token}


# ---------------------------------------------------------------------------
# _BOARD_INCLUDE_KEYS — whitelist contract
# ---------------------------------------------------------------------------


def test_whitelist_is_immutable_tuple():
    """Whitelist must be a tuple — additive-only ordering contract."""
    assert isinstance(_BOARD_INCLUDE_KEYS, tuple)


def test_whitelist_contains_exactly_four_canonical_collections():
    """The 4 canonical board collections (FR1)."""
    assert set(_BOARD_INCLUDE_KEYS) == {"ideations", "specs", "cards", "agents"}


def test_whitelist_has_no_duplicates():
    """Each collection name appears once."""
    assert len(_BOARD_INCLUDE_KEYS) == len(set(_BOARD_INCLUDE_KEYS))


# ---------------------------------------------------------------------------
# BoardResponse.counts — schema field
# ---------------------------------------------------------------------------


def test_board_response_counts_field_is_optional_with_none_default():
    """FR7: schema gains `counts: dict[str, int] | None = None` (non-breaking)."""
    from okto_pulse.core.models.schemas import BoardResponse

    field = BoardResponse.model_fields.get("counts")
    assert field is not None, "BoardResponse must declare a `counts` field"
    assert field.default is None, "counts must default to None for backward compat"


def test_board_response_accepts_minimal_payload_without_counts():
    """Legacy serializers that don't populate counts must still validate."""
    from datetime import datetime, timezone

    from okto_pulse.core.models.schemas import BoardResponse

    minimal = BoardResponse(
        id="b1",
        name="X",
        description=None,
        owner_id="u1",
        settings={},
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    assert minimal.counts is None


def test_board_response_accepts_counts_payload():
    """When the server populates counts (MCP default), the model accepts it."""
    from datetime import datetime, timezone

    from okto_pulse.core.models.schemas import BoardResponse

    with_counts = BoardResponse(
        id="b1",
        name="X",
        description=None,
        owner_id="u1",
        settings={},
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        counts={"ideations": 23, "specs": 28, "cards": 247, "agents": 1},
    )
    assert with_counts.counts == {"ideations": 23, "specs": 28, "cards": 247, "agents": 1}


def test_board_response_normalizes_historical_card_conclusion_shapes():
    """Full board payloads must tolerate executor/MCP conclusion variants."""
    from datetime import datetime, timezone

    from okto_pulse.core.models.schemas import BoardResponse

    now = datetime.now(timezone.utc)
    board = BoardResponse.model_validate(
        {
            "id": "b1",
            "name": "Board",
            "description": None,
            "owner_id": "u1",
            "settings": {},
            "created_at": now,
            "updated_at": now,
            "cards": [
                {
                    "id": "c1",
                    "board_id": "b1",
                    "title": "Task",
                    "description": None,
                    "details": None,
                    "status": "done",
                    "subject_version": 1,
                    "priority": "none",
                    "position": 0,
                    "assignee_id": None,
                    "created_by": "u1",
                    "created_at": now,
                    "updated_at": now,
                    "due_date": None,
                    "labels": None,
                    "conclusions": [
                        {
                            "description": "Executor summary",
                            "author": "executor",
                            "created_at": now.isoformat(),
                            "completeness": 95,
                            "drift": 2,
                        },
                        {
                            "body": "Structured execution body",
                            "author_agent_id": "agent-executor",
                            "kind": "execution_summary",
                            "created_at": now.isoformat(),
                        },
                    ],
                }
            ],
        }
    )

    first, second = board.cards[0].conclusions or []
    assert first.text == "Executor summary"
    assert first.author_id == "executor"
    assert first.completeness == 95
    assert first.drift == 2
    assert second.text == "Structured execution body"
    assert second.author_id == "agent-executor"
