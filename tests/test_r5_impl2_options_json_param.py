"""R5 IMPL-2 — options_json param for the 4 choice-question MCP handlers
(spec bfb30e51, FR2/FR3, TR2/TR3).

Behavioural tests: TEST-C (ts_5d87035e, ts_eeddb715) and TEST-D (ts_a8a5e1cb,
ts_b09f8515).

Strategy
--------
The 4 handler functions (okto_pulse_add_choice_comment,
okto_pulse_ask_ideation_choice_question,
okto_pulse_ask_refinement_choice_question,
okto_pulse_ask_spec_choice_question) require an authenticated agent context
and an async DB session — spinning the full MCP stack is out of scope for a
unit suite.  The logic under test lives in two places:

1. ``parse_options_json`` (helpers.py) — the dedicated parser for
   object-typed option items.  Tests for AC2/AC3/AC4/AC5 that exercise the
   "then" branch (parser + *ChoiceOption construction) call this function and
   instantiate the *ChoiceOption classes directly, which is exactly what each
   handler does internally.

2. Structural invariants on server.py — lightweight assertions that confirm
   each handler (a) declares the new ``options_json`` parameter, (b) calls
   ``parse_options_json``, and (c) keeps calling ``parse_multi_value`` for the
   legacy ``options`` fallback.  These guard against future regressions without
   requiring DB/auth.

3. One end-to-end behavioural precedence test (AC3, ts_eeddb715) drives the
   REAL ``okto_pulse_add_choice_comment`` handler with BOTH ``options`` and
   ``options_json`` present (stubbed auth + the conftest DB factory) and asserts
   the persisted choices come from ``options_json`` — exercising the production
   ``if parsed_objects is not None`` selector through CommentService, not a
   re-implemented branch.

AC cross-reference
------------------
- AC2 (ts_5d87035e): options_json [{label:A, recommended:true, tradeoff:t}]
  → choice has label A, recommended True, tradeoff t, id opt_0.
- AC3 (ts_eeddb715): BOTH options and options_json present → choices come
  from options_json (precedence), NOT from options.
- AC4 (ts_a8a5e1cb): options_json with item missing label → ValueError.
- AC5 (ts_b09f8515): options_json [{label:A}] (no recommended/tradeoff)
  → recommended=False, tradeoff=None.
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import re
from pathlib import Path

import pytest

from okto_pulse.core.mcp.helpers import parse_options_json

SERVER_PY = (
    Path(__file__).resolve().parent.parent
    / "src" / "okto_pulse" / "core" / "mcp" / "server.py"
)

CHOICE_HANDLERS = (
    "okto_pulse_add_choice_comment",
    "okto_pulse_ask_ideation_choice_question",
    "okto_pulse_ask_refinement_choice_question",
    "okto_pulse_ask_spec_choice_question",
)


@pytest.fixture(scope="module")
def server_source() -> str:
    return SERVER_PY.read_text(encoding="utf-8")


def _slice_handler(source: str, tool_name: str) -> str:
    """Return the source slice from the tool's ``async def`` to the next one."""
    start_marker = f"async def {tool_name}("
    start = source.find(start_marker)
    assert start != -1, f"handler not found: {tool_name}"
    next_start = source.find("\nasync def ", start + 1)
    if next_start == -1:
        next_start = len(source)
    return source[start:next_start]


# ---------------------------------------------------------------------------
# AC2 (ts_5d87035e) — options_json with label/recommended/tradeoff
# ---------------------------------------------------------------------------


def test_ac2_parse_options_json_single_full_item():
    """AC2 — parse_options_json with one full item returns the expected dict."""
    raw = '[{"label": "A", "recommended": true, "tradeoff": "t"}]'
    result = parse_options_json(raw)
    assert result is not None
    assert len(result) == 1
    item = result[0]
    assert item["label"] == "A"
    assert item["recommended"] is True
    assert item["tradeoff"] == "t"


def test_ac2_choice_option_constructed_from_parsed_item():
    """AC2 (ChoiceOption) — choice built from parse_options_json output has
    label A, recommended True, tradeoff 't', and id 'opt_0'.

    Mirrors the synthesis path inside okto_pulse_add_choice_comment."""
    from okto_pulse.core.models.schemas import ChoiceOption

    raw = '[{"label": "A", "recommended": true, "tradeoff": "t"}]'
    parsed = parse_options_json(raw)
    assert parsed is not None
    choice_list = [
        ChoiceOption(id=f"opt_{i}", label=obj["label"], recommended=obj["recommended"], tradeoff=obj["tradeoff"])
        for i, obj in enumerate(parsed)
    ]
    assert len(choice_list) == 1
    c = choice_list[0]
    assert c.id == "opt_0"
    assert c.label == "A"
    assert c.recommended is True
    assert c.tradeoff == "t"


def test_ac2_ideation_choice_option_from_options_json():
    """AC2 (IdeationQAChoiceOption) — synthesis path for ask_ideation handler."""
    from okto_pulse.core.models.schemas import IdeationQAChoiceOption

    raw = '[{"label": "A", "recommended": true, "tradeoff": "t"}]'
    parsed = parse_options_json(raw)
    assert parsed is not None
    opts = [
        IdeationQAChoiceOption(id=f"opt_{i}", label=obj["label"], recommended=obj["recommended"], tradeoff=obj["tradeoff"])
        for i, obj in enumerate(parsed)
    ]
    assert opts[0].id == "opt_0"
    assert opts[0].label == "A"
    assert opts[0].recommended is True
    assert opts[0].tradeoff == "t"


def test_ac2_refinement_choice_option_from_options_json():
    """AC2 (RefinementQAChoiceOption) — synthesis path for ask_refinement handler."""
    from okto_pulse.core.models.schemas import RefinementQAChoiceOption

    raw = '[{"label": "A", "recommended": true, "tradeoff": "t"}]'
    parsed = parse_options_json(raw)
    assert parsed is not None
    opts = [
        RefinementQAChoiceOption(id=f"opt_{i}", label=obj["label"], recommended=obj["recommended"], tradeoff=obj["tradeoff"])
        for i, obj in enumerate(parsed)
    ]
    assert opts[0].id == "opt_0"
    assert opts[0].recommended is True
    assert opts[0].tradeoff == "t"


def test_ac2_spec_choice_option_from_options_json():
    """AC2 (SpecQAChoiceOption) — synthesis path for ask_spec handler."""
    from okto_pulse.core.models.schemas import SpecQAChoiceOption

    raw = '[{"label": "A", "recommended": true, "tradeoff": "t"}]'
    parsed = parse_options_json(raw)
    assert parsed is not None
    opts = [
        SpecQAChoiceOption(id=f"opt_{i}", label=obj["label"], recommended=obj["recommended"], tradeoff=obj["tradeoff"])
        for i, obj in enumerate(parsed)
    ]
    assert opts[0].id == "opt_0"
    assert opts[0].recommended is True
    assert opts[0].tradeoff == "t"


# ---------------------------------------------------------------------------
# AC3 (ts_eeddb715) — options_json takes precedence over options when both
# are present.
# ---------------------------------------------------------------------------


def test_ac3_options_json_precedence_over_options():
    """AC3 — when options_json is non-empty, the parser returns its items.
    The caller (handler) uses those items instead of the label-only options list.

    We test the handler logic directly: if parse_options_json returns non-None,
    the choice_list comes from the parsed objects (not from parse_multi_value).
    """
    from okto_pulse.core.models.schemas import ChoiceOption

    options_json_raw = '[{"label": "From JSON"}]'

    parsed_objects = parse_options_json(options_json_raw)
    assert parsed_objects is not None, "options_json should produce non-None"

    # When parsed_objects is not None, handler uses it — mirrors server.py logic.
    choice_list = [
        ChoiceOption(id=f"opt_{i}", label=obj["label"], recommended=obj["recommended"], tradeoff=obj["tradeoff"])
        for i, obj in enumerate(parsed_objects)
    ]

    # Must contain only the options_json item, NOT the label-only options.
    assert len(choice_list) == 1
    assert choice_list[0].label == "From JSON"


async def test_ac3_handler_options_json_wins_over_options_behavioral():
    """AC3 (behavioural, ts_eeddb715 ``then``) — call the REAL
    ``okto_pulse_add_choice_comment`` handler with BOTH ``options`` (label-only)
    AND ``options_json`` present, and assert the persisted choices come from
    ``options_json``, NOT from ``options``.

    This drives the production selector (server.py:3238-3253
    ``if parsed_objects is not None: [options_json] else: [options]``) end-to-end
    through ``CommentService.create_comment`` — the real precedence branch, not a
    re-implementation. If ``options`` ever won, "From Options A/B" would appear.
    """
    import json
    import uuid
    from unittest.mock import AsyncMock, patch

    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.mcp import server as mcp_server
    from sqlalchemy_test_models import (
        Board,
        Card,
        CardStatus,
        CardType,
        Spec,
        SpecStatus,
    )

    board_id = "r5-impl2-prec-board"
    user_id = "r5-impl2-prec-agent"
    spec_id = str(uuid.uuid4())
    card_id = str(uuid.uuid4())

    factory = get_session_factory()
    async with factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="R5 IMPL-2 precedence", owner_id=user_id))
            await db.flush()
        db.add(Spec(
            id=spec_id, board_id=board_id, title="R5 IMPL-2 prec spec",
            status=SpecStatus.APPROVED, created_by=user_id,
            functional_requirements=["FR1"], acceptance_criteria=["AC1"],
            test_scenarios=[], business_rules=[], api_contracts=[],
        ))
        db.add(Card(
            id=card_id, board_id=board_id, spec_id=spec_id,
            title="prec card", status=CardStatus.NOT_STARTED,
            card_type=CardType.NORMAL, created_by=user_id,
        ))
        await db.commit()

    ctx = type("Ctx", (), {
        "agent_id": user_id,
        "agent_name": "prec-agent",
        "permissions": ["*"],
    })()

    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=ctx)), \
         patch.object(mcp_server, "check_permission", return_value=None):
        raw = await mcp_server.okto_pulse_add_choice_comment.fn(
            board_id=board_id,
            card_id=card_id,
            question="Which approach?",
            options="From Options A|From Options B",
            options_json='[{"label": "From JSON", "recommended": true, "tradeoff": "more cost"}]',
        )

    payload = json.loads(raw)
    assert payload.get("success") is True, payload

    choices = payload["comment"]["choices"]
    labels = [c["label"] for c in choices]
    # Precedence: choices come from options_json, NOT the label-only options.
    assert labels == ["From JSON"], labels
    assert "From Options A" not in labels
    assert "From Options B" not in labels
    # The structured options_json fields survived end-to-end through the handler.
    assert choices[0]["id"] == "opt_0"
    assert choices[0]["recommended"] is True
    assert choices[0]["tradeoff"] == "more cost"


def test_ac3_options_path_used_when_options_json_absent():
    """AC3 (complement) — when options_json is absent/empty, parse_options_json
    returns None and the handler falls back to the legacy options param."""
    from okto_pulse.core.mcp.helpers import parse_multi_value

    options_json_raw = ""  # absent — handler passes None
    options_labels_raw = "Legacy A|Legacy B"

    parsed_objects = parse_options_json(options_json_raw or None)
    assert parsed_objects is None, "empty options_json should signal legacy fallback"

    # Handler falls back to parse_multi_value(options).
    option_labels = parse_multi_value(options_labels_raw)
    assert option_labels == ["Legacy A", "Legacy B"]


def test_ac3_options_path_used_when_options_json_empty_json_array():
    """AC3 (complement 2) — an empty JSON array '[]' is treated as absent
    (parse_options_json returns None), so the legacy path is used."""
    parsed_objects = parse_options_json("[]")
    assert parsed_objects is None, "empty JSON array should signal legacy fallback"


# ---------------------------------------------------------------------------
# AC4 (ts_a8a5e1cb) — options_json with item missing label → ValueError
# ---------------------------------------------------------------------------


def test_ac4_missing_label_raises_value_error():
    """AC4 — an item with no 'label' key raises ValueError (label is required)."""
    raw = '[{"recommended": true, "tradeoff": "t"}]'
    with pytest.raises(ValueError, match="missing a non-empty 'label'"):
        parse_options_json(raw)


def test_ac4_empty_string_label_raises_value_error():
    """AC4 — an item with label='' (empty string) raises ValueError."""
    raw = '[{"label": "", "recommended": false}]'
    with pytest.raises(ValueError, match="missing a non-empty 'label'"):
        parse_options_json(raw)


def test_ac4_whitespace_only_label_raises_value_error():
    """AC4 — an item with label='   ' (whitespace only) raises ValueError."""
    raw = '[{"label": "   "}]'
    with pytest.raises(ValueError, match="missing a non-empty 'label'"):
        parse_options_json(raw)


def test_ac4_null_label_raises_value_error():
    """AC4 — an item with label=null raises ValueError."""
    raw = '[{"label": null}]'
    with pytest.raises(ValueError, match="missing a non-empty 'label'"):
        parse_options_json(raw)


# ---------------------------------------------------------------------------
# AC5 (ts_b09f8515) — options_json [{label:A}] without recommended/tradeoff
# → recommended=False, tradeoff=None
# ---------------------------------------------------------------------------


def test_ac5_parse_options_json_defaults_for_missing_fields():
    """AC5 — item with only 'label' gets recommended=False and tradeoff=None."""
    raw = '[{"label": "A"}]'
    result = parse_options_json(raw)
    assert result is not None
    assert len(result) == 1
    assert result[0]["recommended"] is False
    assert result[0]["tradeoff"] is None


def test_ac5_choice_option_has_defaults():
    """AC5 (ChoiceOption) — choice constructed from parse_options_json output
    with only 'label' has recommended=False and tradeoff=None."""
    from okto_pulse.core.models.schemas import ChoiceOption

    raw = '[{"label": "A"}]'
    parsed = parse_options_json(raw)
    assert parsed is not None
    opts = [
        ChoiceOption(id=f"opt_{i}", label=obj["label"], recommended=obj["recommended"], tradeoff=obj["tradeoff"])
        for i, obj in enumerate(parsed)
    ]
    assert opts[0].recommended is False
    assert opts[0].tradeoff is None


def test_ac5_all_four_option_types_have_defaults():
    """AC5 — all 4 *ChoiceOption types accept recommended=False, tradeoff=None
    constructed via parse_options_json output."""
    from okto_pulse.core.models.schemas import (
        ChoiceOption,
        IdeationQAChoiceOption,
        RefinementQAChoiceOption,
        SpecQAChoiceOption,
    )

    raw = '[{"label": "A"}]'
    parsed = parse_options_json(raw)
    assert parsed is not None
    obj = parsed[0]

    for cls in (ChoiceOption, IdeationQAChoiceOption, RefinementQAChoiceOption, SpecQAChoiceOption):
        instance = cls(id="opt_0", label=obj["label"], recommended=obj["recommended"], tradeoff=obj["tradeoff"])
        assert instance.recommended is False, f"{cls.__name__} recommended should be False"
        assert instance.tradeoff is None, f"{cls.__name__} tradeoff should be None"


# ---------------------------------------------------------------------------
# Structural invariants — each handler declares options_json + calls
# parse_options_json + retains parse_multi_value for legacy fallback.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("tool_name", CHOICE_HANDLERS)
def test_handler_declares_options_json_param(server_source: str, tool_name: str):
    """FR2 — each handler's signature must include 'options_json'."""
    body = _slice_handler(server_source, tool_name)
    assert "options_json" in body, (
        f"{tool_name} must declare 'options_json' parameter (FR2/TR2)"
    )


@pytest.mark.parametrize("tool_name", CHOICE_HANDLERS)
def test_options_param_is_optional_when_options_json_is_used(server_source: str, tool_name: str):
    """Regression: strict MCP clients must be able to omit legacy ``options``
    when they provide ``options_json``."""
    body = _slice_handler(server_source, tool_name)
    assert re.search(r"options:\s*list\[str\]\s*\|\s*str\s*=\s*\"\"", body), (
        f"{tool_name} must default options to an empty string so options_json-only "
        "calls are valid in the MCP schema"
    )


@pytest.mark.parametrize("tool_name", CHOICE_HANDLERS)
def test_handler_calls_parse_options_json(server_source: str, tool_name: str):
    """FR3 — each handler must call parse_options_json (the dedicated parser)."""
    body = _slice_handler(server_source, tool_name)
    assert "parse_options_json(" in body, (
        f"{tool_name} must call parse_options_json (FR3/TR3)"
    )


@pytest.mark.parametrize("tool_name", CHOICE_HANDLERS)
def test_handler_retains_parse_multi_value_for_legacy_fallback(server_source: str, tool_name: str):
    """Retrocompat — each handler must still call parse_multi_value or
    coerce_to_list_str (which routes through parse_multi_value internally)
    for the legacy options fallback path when options_json is absent.
    Updated by R3a IMPL-1 (FR1/FR2): the Union-typed params now use
    coerce_to_list_str as the canonical entry point."""
    body = _slice_handler(server_source, tool_name)
    assert "parse_multi_value(" in body or "coerce_to_list_str(" in body, (
        f"{tool_name} must still call parse_multi_value or coerce_to_list_str for legacy options fallback"
    )


@pytest.mark.parametrize("tool_name", CHOICE_HANDLERS)
def test_handler_docstring_mentions_options_json(server_source: str, tool_name: str):
    """FR2 — docstring must document the new options_json parameter."""
    body = _slice_handler(server_source, tool_name)
    docstring_end = body.find('"""', body.find('"""') + 3)
    docstring = body[:docstring_end]
    assert "options_json" in docstring, (
        f"{tool_name} docstring must mention 'options_json' parameter"
    )


# ---------------------------------------------------------------------------
# Additional parse_options_json unit tests (edge cases)
# ---------------------------------------------------------------------------


def test_parse_options_json_none_returns_none():
    assert parse_options_json(None) is None


def test_parse_options_json_empty_string_returns_none():
    assert parse_options_json("") is None


def test_parse_options_json_whitespace_returns_none():
    assert parse_options_json("   ") is None


def test_parse_options_json_empty_array_returns_none():
    """An empty JSON array signals 'no options' — caller uses legacy path."""
    assert parse_options_json("[]") is None


def test_parse_options_json_multiple_items():
    raw = '[{"label": "Opt1", "recommended": false}, {"label": "Opt2", "recommended": true, "tradeoff": "risk"}]'
    result = parse_options_json(raw)
    assert result is not None
    assert len(result) == 2
    assert result[0]["label"] == "Opt1"
    assert result[0]["recommended"] is False
    assert result[0]["tradeoff"] is None
    assert result[1]["label"] == "Opt2"
    assert result[1]["recommended"] is True
    assert result[1]["tradeoff"] == "risk"


def test_parse_options_json_malformed_json_raises():
    with pytest.raises(ValueError, match="malformed JSON"):
        parse_options_json('[{"label": "A"')


def test_parse_options_json_non_list_raises():
    with pytest.raises(ValueError, match="must decode to a list"):
        parse_options_json('{"label": "A"}')


def test_parse_options_json_item_not_dict_raises():
    with pytest.raises(ValueError, match="must be an object"):
        parse_options_json('["just a string"]')


def test_parse_options_json_ids_assigned_by_caller():
    """parse_options_json returns raw dicts without 'id'; caller assigns id=f'opt_{i}'."""
    raw = '[{"label": "A"}, {"label": "B"}]'
    result = parse_options_json(raw)
    assert result is not None
    for item in result:
        assert "id" not in item, "parse_options_json must NOT assign id — caller does that"
