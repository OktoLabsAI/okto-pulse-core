"""Extract Alternative nodes from spec.context "## Analysis" + Q&A
(card 14cd6bd9, spec f565115d).

Pattern: scans for sentences containing any of
``alternativa | considerado | poderia | em vez de | optei por ... ao invés de``
and builds one Alternative per occurrence. Each extraction carries
`reasoning_against` (why it was NOT chosen) and an optional link hint
`decision_chosen_hint` so the orchestrator can wire the `relates_to` edge
to the winning Decision later.

Regex-driven on purpose — deterministic, cheap, easy to audit. The
cognitive LLM is NOT involved in this extractor; it only reviews the
extracted candidates and decides whether to promote them to Alternative
nodes via add_node_candidate.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

# Pattern words (Portuguese + common English variants). Case-insensitive.
_MARKERS = (
    r"alternativ[ao]s?",
    r"considerad[ao]s?",
    r"poderi[ao]",
    r"em\s+vez\s+de",
    r"ao\s+inv[eé]s\s+de",
    r"instead\s+of",
    r"rejected",
    r"discarded",
    r"not\s+chosen",
)
_MARKER_RE = re.compile(
    r"(?P<sentence>[^.\n!?]*(?:" + "|".join(_MARKERS) + r")[^.\n!?]*[.!?])",
    re.IGNORECASE,
)

# Heading of the analysis section. Matches ## Analysis, # ANALYSIS, etc.
_ANALYSIS_HEADER = re.compile(r"^\s*#+\s*analysis\s*$", re.IGNORECASE | re.MULTILINE)
_NEXT_HEADER = re.compile(r"^\s*#+\s+", re.MULTILINE)


@dataclass(frozen=True)
class AlternativeExtraction:
    """One Alternative candidate awaiting agent review."""

    title: str
    reasoning_against: str  # the full sentence explaining rejection
    source_section: str  # "analysis" | "qa" | "context"
    source_ref: str  # spec:<id> / ideation:<id> / refinement:<id>
    raw_match: str = field(default="")
    decision_chosen_hint: str | None = None  # optional decision_id if caller can infer


def _extract_analysis_section(context: str) -> str:
    """Return the text between the `## Analysis` header and the next `##`
    header, or empty if the section is absent."""
    if not context:
        return ""
    m = _ANALYSIS_HEADER.search(context)
    if not m:
        return ""
    start = m.end()
    after = context[start:]
    nm = _NEXT_HEADER.search(after)
    return after[: nm.start()] if nm else after


def _candidate_title(sentence: str) -> str:
    """Trim a sentence into a concise node title.

    Keeps up to 120 chars (matches EmittedNode shape) and strips markdown
    bullet glyphs so UI doesn't render double-bullets.
    """
    cleaned = sentence.strip().lstrip("-*• ").strip()
    return cleaned[:120]


def extract_alternatives(
    *,
    spec_context: str = "",
    qa_texts: list[str] | None = None,
    source_ref: str = "",
) -> list[AlternativeExtraction]:
    """Return Alternative candidates parsed from spec.context + Q&A.

    `spec_context` is scanned INSIDE the `## Analysis` section only — per the
    card description. `qa_texts` is scanned whole-text because Q&A rarely
    has section headers.

    The function is tolerant of missing sections — returns [] rather than
    raising, so callers can skip silently when the artifact has neither.
    """
    out: list[AlternativeExtraction] = []
    analysis = _extract_analysis_section(spec_context)
    if analysis:
        for m in _MARKER_RE.finditer(analysis):
            sentence = m.group("sentence").strip()
            out.append(AlternativeExtraction(
                title=_candidate_title(sentence),
                reasoning_against=sentence,
                source_section="analysis",
                source_ref=source_ref,
                raw_match=sentence,
            ))

    for qa in qa_texts or []:
        if not qa:
            continue
        for m in _MARKER_RE.finditer(qa):
            sentence = m.group("sentence").strip()
            out.append(AlternativeExtraction(
                title=_candidate_title(sentence),
                reasoning_against=sentence,
                source_section="qa",
                source_ref=source_ref,
                raw_match=sentence,
            ))

    return out
