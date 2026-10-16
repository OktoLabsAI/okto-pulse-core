"""Current structured choice metadata, including real handler persistence."""
from mcp_runtime_testing import register_mcp_test_runtime

async def test_native_choice_objects_persist_through_real_handler():
    """Native object options preserve labels and metadata through persistence."""
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
            functional_requirements=[{"id": "fr_1", "text": "FR1"}],
            acceptance_criteria=[{"id": "ac_1", "text": "AC1"}],
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
            options=[{"label": "A, B | C", "recommended": True, "tradeoff": "more cost"}],
        )

    payload = json.loads(raw)
    assert payload.get("success") is True, payload

    choices = payload["comment"]["choices"]
    labels = [c["label"] for c in choices]
    assert labels == ["A, B | C"], labels
    # The native option fields survived end-to-end through the handler.
    assert choices[0]["id"] == "opt_0"
    assert choices[0]["recommended"] is True
    assert choices[0]["tradeoff"] == "more cost"
