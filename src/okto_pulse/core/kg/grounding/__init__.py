"""Grounding verification for KG-backed agent answers (ideação d3dfdab8).

Post-answer verifier. Given an answer and the retrieved rows that
informed it, returns a decoupled verdict: is the answer grounded,
which entities were hallucinated, which claims are unsupported, and
which source nodes attribute each claim.

Usage::

    from okto_pulse.core.kg.grounding import verify_grounding

    result = verify_grounding(
        answer_text=agent_answer,
        retrieved_rows=kg_hits,
        extractor_fn=my_claim_extractor,
        grounder_fn=my_llm_grounder,
    )
    if not result.overall_grounded:
        # Caller decides enforcement — disclaim, retry, block, ...
        pass
"""

from .grounding import (
    Claim,
    GroundingResult,
    check_entities_present,
    score_semantic_grounding,
    verify_grounding,
)

__all__ = [
    "Claim",
    "GroundingResult",
    "check_entities_present",
    "score_semantic_grounding",
    "verify_grounding",
]
