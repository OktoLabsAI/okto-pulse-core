"""Grounding verification implementation (ideação d3dfdab8)."""

from __future__ import annotations

import logging
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Callable

logger = logging.getLogger("okto_pulse.kg.grounding")


# ===========================================================================
# Data contracts
# ===========================================================================


@dataclass(frozen=True)
class Claim:
    """An atomic assertion extracted from an answer.

    ``mentioned_entities`` is a tuple of entity name strings the
    extractor flagged — these are checked against the retrieved rows
    for presence (exact normalized or Jaccard fallback).
    """

    text: str
    mentioned_entities: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class GroundingResult:
    """Verdict of verify_grounding.

    - ``overall_grounded`` is False when any entity is hallucinated,
      average confidence is below 0.5, or any claim is unsupported.
    - ``confidence`` is the simple mean of per-claim confidences.
    - ``hallucinated_entities`` names that appear in claims but not
      in retrieved rows (after normalization + Jaccard fallback).
    - ``unsupported_claims`` claims the grounder marked as not grounded.
    - ``attribution_map`` one entry per claim, linking to source node
      ids that support it (from the grounder's supporting_ids).
    """

    overall_grounded: bool
    confidence: float
    hallucinated_entities: tuple[str, ...] = field(default_factory=tuple)
    unsupported_claims: tuple[dict, ...] = field(default_factory=tuple)
    attribution_map: tuple[dict, ...] = field(default_factory=tuple)


# Callable contracts — caller injects the LLM provider.
ExtractorFn = Callable[[str], list[Claim]]
GrounderFn = Callable[[list[Claim], list[dict]], list[dict]]
# GrounderFn returns one dict per claim:
#   {"grounded": bool, "confidence": float, "supporting_ids": list[str]}


# ===========================================================================
# Entity presence check
# ===========================================================================


def _normalize_entity(text: str) -> str:
    """Case + diacritic + whitespace insensitive normalization.

    Uses NFKD decomposition then drops non-ASCII combining chars. The
    result is lowercase, trimmed, suitable for exact match across
    Portuguese / English / common accented scripts.
    """
    if not text:
        return ""
    decomposed = unicodedata.normalize("NFKD", text)
    ascii_bytes = decomposed.encode("ASCII", "ignore")
    return ascii_bytes.decode("ASCII").strip().lower()


def _jaccard_overlap(a_tokens: frozenset[str], b_tokens: frozenset[str]) -> float:
    """Jaccard overlap = |intersection| / |union| (0 when either is empty)."""
    if not a_tokens or not b_tokens:
        return 0.0
    inter = len(a_tokens & b_tokens)
    if inter == 0:
        return 0.0
    return inter / len(a_tokens | b_tokens)


def check_entities_present(
    entities: list[str],
    retrieved_rows: list[dict],
    *,
    threshold: float = 0.7,
) -> tuple[set[str], set[str]]:
    """Verify each entity appears in retrieved rows.

    Strategy: exact normalized match first (O(N·M)); if no row matches,
    fallback to Jaccard overlap of tokens with the given threshold
    (default 0.7). Any entity matching nothing goes to the
    ``hallucinated`` set.

    Returns (present, hallucinated) as string sets preserving the
    original (non-normalized) entity names.
    """
    if not entities:
        return set(), set()

    row_titles = [str(r.get("title", "")) for r in retrieved_rows]
    norm_titles = [_normalize_entity(t) for t in row_titles]
    title_tokens = [frozenset(t.split()) for t in norm_titles if t]

    present: set[str] = set()
    hallucinated: set[str] = set()

    for entity in entities:
        if not isinstance(entity, str) or not entity.strip():
            continue
        norm_ent = _normalize_entity(entity)

        # Exact match against any normalized title.
        if any(norm_ent == nt for nt in norm_titles if nt):
            present.add(entity)
            continue

        # Jaccard fallback.
        ent_tokens = frozenset(norm_ent.split())
        matched = any(
            _jaccard_overlap(ent_tokens, tt) >= threshold
            for tt in title_tokens
        )
        if matched:
            present.add(entity)
        else:
            hallucinated.add(entity)

    return present, hallucinated


# ===========================================================================
# Semantic grounding (batch LLM call)
# ===========================================================================


def _default_score() -> dict[str, Any]:
    return {"grounded": False, "confidence": 0.0, "supporting_ids": []}


def score_semantic_grounding(
    claims: list[Claim],
    retrieved_rows: list[dict],
    grounder_fn: GrounderFn,
) -> list[dict]:
    """Invoke grounder_fn EXACTLY once with all claims, return a
    sanitized list with exactly ``len(claims)`` entries.

    - If grounder returns fewer items, the tail is padded with
      default scores (grounded=False, confidence=0.0, supporting_ids=[]).
    - If grounder returns more items, the excess is discarded.
    - If grounder returns a non-list, returns all defaults.
    """
    if not claims:
        return []

    raw = grounder_fn(claims, retrieved_rows)
    if not isinstance(raw, list):
        return [_default_score() for _ in claims]

    out: list[dict] = []
    for idx in range(len(claims)):
        if idx < len(raw) and isinstance(raw[idx], dict):
            score = raw[idx]
            # Validate shape — missing keys fall back to defaults.
            out.append({
                "grounded": bool(score.get("grounded", False)),
                "confidence": float(score.get("confidence", 0.0) or 0.0),
                "supporting_ids": list(score.get("supporting_ids", []) or []),
            })
        else:
            out.append(_default_score())
    return out


# ===========================================================================
# Orchestrator
# ===========================================================================


def _failure_result(reason: str) -> GroundingResult:
    logger.warning("verify_grounding.failed reason=%s", reason)
    return GroundingResult(
        overall_grounded=False,
        confidence=0.0,
        hallucinated_entities=(),
        unsupported_claims=(),
        attribution_map=(),
    )


def verify_grounding(
    answer_text: str,
    retrieved_rows: list[dict],
    *,
    extractor_fn: ExtractorFn,
    grounder_fn: GrounderFn,
    entity_match_threshold: float = 0.7,
) -> GroundingResult:
    """Orchestrate: extract claims → check entities → score grounding.

    Never raises. Any exception from extractor_fn or grounder_fn is
    logged as a warning and translated into a safe GroundingResult
    with ``overall_grounded=False, confidence=0.0``.
    """
    # 1. Extract claims.
    try:
        claims = extractor_fn(answer_text)
    except Exception as e:  # noqa: BLE001
        return _failure_result(f"extractor_error:{type(e).__name__}")

    if not isinstance(claims, list) or not claims:
        # Nothing to verify — trivially grounded (vacuous truth) if no
        # claims but low confidence since nothing was checked.
        return GroundingResult(
            overall_grounded=True,
            confidence=1.0,
            hallucinated_entities=(),
            unsupported_claims=(),
            attribution_map=(),
        )

    # 2. Collect all entities and check presence.
    all_entities: list[str] = []
    for c in claims:
        for e in c.mentioned_entities:
            if isinstance(e, str) and e.strip():
                all_entities.append(e)

    present, hallucinated = check_entities_present(
        all_entities,
        retrieved_rows,
        threshold=entity_match_threshold,
    )

    # 3. Semantic grounding via batch LLM call.
    try:
        scores = score_semantic_grounding(claims, retrieved_rows, grounder_fn)
    except Exception as e:  # noqa: BLE001
        return _failure_result(f"grounder_error:{type(e).__name__}")

    # 4. Build output fields.
    unsupported: list[dict] = []
    attribution: list[dict] = []
    confidences: list[float] = []

    for claim, score in zip(claims, scores):
        conf = float(score.get("confidence", 0.0) or 0.0)
        confidences.append(conf)
        supporting_ids = tuple(
            str(sid) for sid in score.get("supporting_ids", [])
        )
        attribution.append({
            "claim": claim.text,
            "source_node_ids": supporting_ids,
        })
        if not score.get("grounded", False):
            unsupported.append({
                "claim": claim.text,
                "reason": f"confidence={conf:.2f}",
            })

    avg_confidence = (
        sum(confidences) / len(confidences) if confidences else 0.0
    )

    overall = (
        not hallucinated
        and avg_confidence >= 0.5
        and not unsupported
    )

    return GroundingResult(
        overall_grounded=overall,
        confidence=round(avg_confidence, 4),
        hallucinated_entities=tuple(sorted(hallucinated)),
        unsupported_claims=tuple(unsupported),
        attribution_map=tuple(attribution),
    )
