"""Deterministic, content-addressed node identity policy (spec MKG-A-S1).

Replaces random uuid4 minting in the consolidation commit path so the SAME
logical node gets the SAME id in every process, session and graph
generation (marginalia ADR 0016 adapted with a generation component —
spec decision D1).

Recipe (FR1):
    node_id = f"{node_type.lower()}_" + sha256(
        board_id \\x00 node_type \\x00 natural_key \\x00 str(generation)
    ).hexdigest()[:24]

Natural key (FR2, decision D2):
    - ``source_artifact_ref`` when non-empty (aligned with the NC-8 dedup
      key in primitives);
    - otherwise ``"content:" + sha256(node_type \\x00 NFKC-casefolded
      title).hexdigest()[:16]`` — cognitive nodes frequently carry an
      empty ref, which is exactly where stable identity matters most.

Generation (FR3, decision D1):
    - fresh CREATE mints generation=0;
    - SUPERSEDE mints generation = superseded generation + 1, so the
      successor NEVER collides with the superseded node's id while both
      remain deterministic across re-execution.

Pure functions, stdlib only, no I/O — safe inside the per-board commit
critical section of ``_do_graph_commit`` (TR1). Legacy uuid4-style ids
(12-hex suffix) remain valid; this policy is applied to minting only,
never to reads (TR6). New ids use a 24-hex suffix and are therefore
visually distinguishable from legacy ids.
"""

from __future__ import annotations

import hashlib
import unicodedata

_ID_HASH_CHARS = 24
_CONTENT_KEY_HASH_CHARS = 16
_SEP = b"\x00"

__all__ = ["derive_natural_key", "mint_node_id", "normalize_text"]


def normalize_text(value: str | None) -> str:
    """Canonical NFKC-casefold-strip normalization (spec MKG-A-S1 FR2).

    Shared by the content natural key AND the NC-8 identity-change
    criterion (spec MKG-D-S1 FR8/TR6) so the two rules can never drift.
    """

    return unicodedata.normalize("NFKC", value or "").casefold().strip()


def derive_natural_key(
    source_artifact_ref: str | None,
    node_type: str,
    title: str | None,
) -> str:
    """Canonical natural key for a node (FR2).

    Returns ``source_artifact_ref`` verbatim when non-empty (whitespace
    trimmed); otherwise a content key derived from the NFKC-casefolded
    title, prefixed with ``content:`` so the two key families can never
    collide.
    """

    ref = (source_artifact_ref or "").strip()
    if ref:
        return ref
    normalized_title = normalize_text(title)
    digest = hashlib.sha256()
    digest.update(node_type.encode("utf-8"))
    digest.update(_SEP)
    digest.update(normalized_title.encode("utf-8"))
    return "content:" + digest.hexdigest()[:_CONTENT_KEY_HASH_CHARS]


def mint_node_id(
    board_id: str,
    node_type: str,
    natural_key: str,
    generation: int = 0,
) -> str:
    """Deterministic node id (FR1).

    Same inputs always produce the same id; changing any component
    (board, type, key or generation) produces a different id. Raises
    ``ValueError`` for a negative generation — the supersedence chain
    only ever moves forward (FR3).
    """

    if generation < 0:
        raise ValueError("generation must be >= 0")
    digest = hashlib.sha256()
    digest.update(board_id.encode("utf-8"))
    digest.update(_SEP)
    digest.update(node_type.encode("utf-8"))
    digest.update(_SEP)
    digest.update(natural_key.encode("utf-8"))
    digest.update(_SEP)
    digest.update(str(generation).encode("ascii"))
    return f"{node_type.lower()}_{digest.hexdigest()[:_ID_HASH_CHARS]}"
