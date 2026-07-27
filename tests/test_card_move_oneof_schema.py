"""Card C8 — CardMove oneOf publication and runtime agreement (matriz v13).

Proves, on RAW payloads, that the PUBLISHED schema (``model_json_schema`` —
what FastAPI copies into ``/openapi.json`` components) and the RUNTIME
validator agree case by case: every valid payload matches EXACTLY ONE oneOf
variant and constructs; every invalid payload matches ZERO variants and
raises at parse time (422 at the REST boundary). Also pins the
null-tolerance representation: excluded fields use ``{"type": "null"}``
(never ``{"const": null}``, which Pydantic drops into accept-anything).
"""

from __future__ import annotations

import jsonschema
import pytest
from pydantic import ValidationError

from okto_pulse.core.models.schemas import CardMove

SCHEMA = CardMove.model_json_schema()

VALID_PAYLOADS = [
    # (payload sem status — o oneOf rege apenas os seletores)
    {"position": None, "before_id": "card-x"},          # relative (explicit null pos)
    {"placement": "end", "position": None},             # global
    {"placement": "start"},                             # global (absent others)
    {"position": 0, "before_id": None},                 # positional (explicit null anchor)
    {"position": -1},                                   # positional legacy end
    {},                                                 # all absent -> positional
    {"position": None, "before_id": None, "after_id": None, "placement": None},
    {"after_id": "card-y"},                             # relative after
    # Draft 2020-12: mathematically integral floats ARE "integer" — the
    # runtime normalizes them to int (C8 round-3, val_bb8b593d).
    {"position": 1.0},
    {"position": -1.0},
    {"position": -0.0},
]

INVALID_PAYLOADS = [
    {"before_id": "a", "after_id": "b"},                # both anchors
    {"before_id": "a", "position": 2},                  # anchor + real position
    {"placement": "end", "position": 0},                # placement + real position
    {"position": -2},                                   # below -1 (authorized narrowing)
    {"before_id": ""},                                  # blank anchor
    {"placement": "bogus"},                             # unknown placement
    {"placement": "start", "before_id": "a"},           # placement + anchor
    # C8 round-2 repros (val_288739ce): coercion killers + whitespace anchor —
    # schema (0 variants) and runtime (before-validator / strip) must BOTH reject.
    {"position": "0"},                                  # string position
    {"position": True},                                 # bool position
    {"before_id": "   "},                               # whitespace anchor
    {"position": 1.5},                                  # fractional float
]


def _matching_variants(payload: dict) -> int:
    count = 0
    for variant in SCHEMA["oneOf"]:
        try:
            jsonschema.validate(payload, variant)
        except jsonschema.ValidationError:
            continue
        count += 1
    return count


def test_schema_publishes_null_tolerant_oneof() -> None:
    assert len(SCHEMA["oneOf"]) == 3
    # Null tolerance is {"type": "null"} — {"const": null} would have been
    # dropped by the serializer and accept anything.
    positional = SCHEMA["oneOf"][0]
    assert positional["properties"]["before_id"] == {"type": "null"}
    assert '"const"' not in str(SCHEMA["oneOf"])


@pytest.mark.parametrize("payload", VALID_PAYLOADS)
def test_valid_payloads_match_exactly_one_variant_and_construct(payload: dict) -> None:
    assert _matching_variants(payload) == 1, payload
    CardMove(status="started", **payload)  # runtime agrees: constructs


@pytest.mark.parametrize("payload", INVALID_PAYLOADS)
def test_invalid_payloads_match_zero_variants_and_reject(payload: dict) -> None:
    assert _matching_variants(payload) == 0, payload
    with pytest.raises(ValidationError):  # runtime agrees: 422 at the boundary
        CardMove(status="started", **payload)
