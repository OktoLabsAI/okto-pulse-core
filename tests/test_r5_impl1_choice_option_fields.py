"""R5 IMPL-1 — Choice-option recommended/tradeoff fields (spec bfb30e51).

Covers:
- AC1: model_dump of all 4 ChoiceOption classes contains recommended + tradeoff
- AC6: deserializing a legacy {id, label} dict produces recommended=False, tradeoff=None
- AC7: a *Response schema containing a structured choice exposes recommended/tradeoff

FR1/TR1: fields added to all 4 classes
FR4/TR4: retrocompat — defaults handle missing keys from legacy JSON rows
FR5: projection — *Response schemas expose the new fields unchanged
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# AC1 — model_dump contains recommended and tradeoff for all 4 classes
# ---------------------------------------------------------------------------


async def test_ac1_choice_option_fields_present():
    """AC1 — ChoiceOption.model_dump() contains recommended (default False) and
    tradeoff (default None)."""
    from okto_pulse.core.models.schemas import ChoiceOption

    opt = ChoiceOption(id="o1", label="Option 1")
    dumped = opt.model_dump()
    assert "recommended" in dumped, "ChoiceOption must have 'recommended' key"
    assert "tradeoff" in dumped, "ChoiceOption must have 'tradeoff' key"
    assert dumped["recommended"] is False
    assert dumped["tradeoff"] is None


async def test_ac1_ideation_qa_choice_option_fields_present():
    """AC1 — IdeationQAChoiceOption.model_dump() contains recommended and tradeoff."""
    from okto_pulse.core.models.schemas import IdeationQAChoiceOption

    opt = IdeationQAChoiceOption(id="o1", label="Option 1")
    dumped = opt.model_dump()
    assert "recommended" in dumped
    assert "tradeoff" in dumped
    assert dumped["recommended"] is False
    assert dumped["tradeoff"] is None


async def test_ac1_refinement_qa_choice_option_fields_present():
    """AC1 — RefinementQAChoiceOption.model_dump() contains recommended and tradeoff."""
    from okto_pulse.core.models.schemas import RefinementQAChoiceOption

    opt = RefinementQAChoiceOption(id="o1", label="Option 1")
    dumped = opt.model_dump()
    assert "recommended" in dumped
    assert "tradeoff" in dumped
    assert dumped["recommended"] is False
    assert dumped["tradeoff"] is None


async def test_ac1_spec_qa_choice_option_fields_present():
    """AC1 — SpecQAChoiceOption.model_dump() contains recommended and tradeoff."""
    from okto_pulse.core.models.schemas import SpecQAChoiceOption

    opt = SpecQAChoiceOption(id="o1", label="Option 1")
    dumped = opt.model_dump()
    assert "recommended" in dumped
    assert "tradeoff" in dumped
    assert dumped["recommended"] is False
    assert dumped["tradeoff"] is None


async def test_ac1_set_recommended_and_tradeoff():
    """AC1 — Fields can be set to non-default values."""
    from okto_pulse.core.models.schemas import SpecQAChoiceOption

    opt = SpecQAChoiceOption(id="o2", label="Preferred Option", recommended=True, tradeoff="Higher cost")
    dumped = opt.model_dump()
    assert dumped["recommended"] is True
    assert dumped["tradeoff"] == "Higher cost"


# ---------------------------------------------------------------------------
# AC6 — deserializing a legacy {id, label} dict gives recommended=False, tradeoff=None
# ---------------------------------------------------------------------------


async def test_ac6_legacy_dict_deserialization_choice_option():
    """AC6 — ChoiceOption built from legacy {id, label} dict (no recommended/tradeoff)
    gets recommended=False and tradeoff=None via Pydantic defaults."""
    from okto_pulse.core.models.schemas import ChoiceOption

    legacy = {"id": "opt1", "label": "Legacy Option"}
    opt = ChoiceOption.model_validate(legacy)
    assert opt.recommended is False
    assert opt.tradeoff is None


async def test_ac6_legacy_dict_deserialization_ideation_qa():
    """AC6 — IdeationQAChoiceOption from legacy dict gets defaults."""
    from okto_pulse.core.models.schemas import IdeationQAChoiceOption

    legacy = {"id": "opt1", "label": "Legacy Option"}
    opt = IdeationQAChoiceOption.model_validate(legacy)
    assert opt.recommended is False
    assert opt.tradeoff is None


async def test_ac6_legacy_dict_deserialization_refinement_qa():
    """AC6 — RefinementQAChoiceOption from legacy dict gets defaults."""
    from okto_pulse.core.models.schemas import RefinementQAChoiceOption

    legacy = {"id": "opt1", "label": "Legacy Option"}
    opt = RefinementQAChoiceOption.model_validate(legacy)
    assert opt.recommended is False
    assert opt.tradeoff is None


async def test_ac6_legacy_dict_deserialization_spec_qa():
    """AC6 — SpecQAChoiceOption from legacy dict gets defaults."""
    from okto_pulse.core.models.schemas import SpecQAChoiceOption

    legacy = {"id": "opt1", "label": "Legacy Option"}
    opt = SpecQAChoiceOption.model_validate(legacy)
    assert opt.recommended is False
    assert opt.tradeoff is None


async def test_ac6_legacy_choices_list_via_response_schema():
    """AC6 — A list of legacy dicts in *Response schema choices field
    is deserialized to ChoiceOption instances with recommended=False/tradeoff=None."""
    from datetime import datetime, timezone
    from okto_pulse.core.models.schemas import IdeationQAResponse

    now = datetime.now(timezone.utc)
    # Simulate legacy DB row: choices stored as list of {id, label} dicts
    response = IdeationQAResponse.model_validate({
        "id": "qa-1",
        "ideation_id": "ideation-1",
        "question": "Which approach?",
        "question_type": "choice",
        "choices": [
            {"id": "a", "label": "Approach A"},
            {"id": "b", "label": "Approach B"},
        ],
        "allow_free_text": False,
        "answer": None,
        "selected": None,
        "asked_by": "user-1",
        "answered_by": None,
        "created_at": now,
        "answered_at": None,
    })
    assert response.choices is not None
    assert len(response.choices) == 2
    for choice in response.choices:
        assert choice.recommended is False
        assert choice.tradeoff is None


# ---------------------------------------------------------------------------
# AC7 — Response schema with structured choice exposes recommended/tradeoff
# ---------------------------------------------------------------------------


async def test_ac7_response_schema_includes_recommended_tradeoff_in_payload():
    """AC7 — A *Response schema containing a structured choice (with recommended/tradeoff set)
    includes those fields in model_dump() output — they are NOT filtered out."""
    from datetime import datetime, timezone
    from okto_pulse.core.models.schemas import SpecQAResponse

    now = datetime.now(timezone.utc)
    response = SpecQAResponse.model_validate({
        "id": "qa-2",
        "spec_id": "spec-1",
        "question": "Which design pattern?",
        "question_type": "choice",
        "choices": [
            {"id": "a", "label": "Factory", "recommended": True, "tradeoff": "More boilerplate"},
            {"id": "b", "label": "Singleton", "recommended": False, "tradeoff": "Harder to test"},
        ],
        "allow_free_text": False,
        "answer": None,
        "selected": None,
        "asked_by": "user-1",
        "answered_by": None,
        "created_at": now,
        "answered_at": None,
    })

    payload = response.model_dump()
    assert payload["choices"] is not None
    factory_opt = next(c for c in payload["choices"] if c["id"] == "a")
    singleton_opt = next(c for c in payload["choices"] if c["id"] == "b")

    assert factory_opt["recommended"] is True
    assert factory_opt["tradeoff"] == "More boilerplate"
    assert singleton_opt["recommended"] is False
    assert singleton_opt["tradeoff"] == "Harder to test"


async def test_ac7_refinement_response_schema_includes_fields():
    """AC7 — RefinementQAResponse model_dump includes recommended/tradeoff in choices."""
    from datetime import datetime, timezone
    from okto_pulse.core.models.schemas import RefinementQAResponse

    now = datetime.now(timezone.utc)
    response = RefinementQAResponse.model_validate({
        "id": "qa-3",
        "refinement_id": "ref-1",
        "question": "Architecture choice?",
        "question_type": "multi_choice",
        "choices": [
            {"id": "x", "label": "Microservices", "recommended": True, "tradeoff": "Operational overhead"},
        ],
        "allow_free_text": True,
        "answer": None,
        "selected": None,
        "asked_by": "user-2",
        "answered_by": None,
        "created_at": now,
        "answered_at": None,
    })

    payload = response.model_dump()
    assert payload["choices"][0]["recommended"] is True
    assert payload["choices"][0]["tradeoff"] == "Operational overhead"
