"""Extract Assumption nodes from spec.context + Q&A (spec 3d907a87, FR4 / D2).

Closes the gap where the `Assumption` node type was listed in
``NODE_TYPES`` but had no extractor — only Learning and Alternative did.
Symmetric to ``alternatives.py``: regex-driven, deterministic, no LLM,
returns candidates that the cognitive agent (or the auto-extraction
handler) can promote via ``add_node_candidate``.

Markers cover Portuguese ("assumindo que", "presume-se", "assume-se",
"pressupõe-se") + English ("assuming", "we assume", "assumed that") +
conditional patterns ("caso X então", "if X then"). Each match becomes
one Assumption candidate carrying the full sentence as evidence.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

# Marker words. Case-insensitive. Conditional patterns are intentionally
# loose ("caso X então") to catch both "caso X então Y" and "caso X, então Y".
_MARKERS = (
    r"assumindo\s+que",
    r"presume-se",
    r"assume-se",
    r"pressup[oõ]e-se",
    r"assuming",
    r"we\s+assume",
    r"assumed\s+that",
    r"caso\s+.+?\s+ent[ãa]o",
    r"if\s+.+?\s+then",
)
_MARKER_RE = re.compile(
    r"(?P<sentence>[^.\n!?]*(?:" + "|".join(_MARKERS) + r")[^.\n!?]*[.!?])",
    re.IGNORECASE,
)

# Heading of the analysis section (mirrors alternatives.py).
_ANALYSIS_HEADER = re.compile(r"^\s*#+\s*analysis\s*$", re.IGNORECASE | re.MULTILINE)
_NEXT_HEADER = re.compile(r"^\s*#+\s+", re.MULTILINE)


@dataclass(frozen=True)
class AssumptionExtraction:
    """One Assumption candidate awaiting agent review."""

    title: str
    body: str  # the full sentence — same field semantics as AlternativeExtraction.reasoning_against
    source_section: str  # "analysis" | "qa" | "context"
    source_ref: str  # spec:<id> / refinement:<id>
    raw_match: str = field(default="")


def _extract_analysis_section(context: str) -> str:
    """Return the text between the `## Analysis` header and the next `##`
    header, or empty if the section is absent.

    Identical to ``alternatives._extract_analysis_section`` — kept inline
    rather than imported to avoid a forward dependency between sibling
    extractors. If a third sibling appears, lift this into a shared helper.
    """
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
    """Trim a sentence into a concise node title (≤120 chars, bullets stripped)."""
    cleaned = sentence.strip().lstrip("-*• ").strip()
    return cleaned[:120]


def extract_assumptions(
    *,
    spec_context: str = "",
    qa_texts: list[str] | None = None,
    source_ref: str = "",
) -> list[AssumptionExtraction]:
    """Return Assumption candidates parsed from spec.context + Q&A.

    ``spec_context`` is scanned INSIDE the ``## Analysis`` section only —
    same convention as Alternative. ``qa_texts`` is scanned whole-text.

    Tolerant of missing sections: returns ``[]`` rather than raising so
    callers can skip silently when the artifact has neither analysis nor Q&A.
    """
    out: list[AssumptionExtraction] = []
    analysis = _extract_analysis_section(spec_context)
    if analysis:
        for m in _MARKER_RE.finditer(analysis):
            sentence = m.group("sentence").strip()
            out.append(AssumptionExtraction(
                title=_candidate_title(sentence),
                body=sentence,
                source_section="analysis",
                source_ref=source_ref,
                raw_match=sentence,
            ))

    for qa in qa_texts or []:
        if not qa:
            continue
        for m in _MARKER_RE.finditer(qa):
            sentence = m.group("sentence").strip()
            out.append(AssumptionExtraction(
                title=_candidate_title(sentence),
                body=sentence,
                source_section="qa",
                source_ref=source_ref,
                raw_match=sentence,
            ))

    return out
