"""Per-concept ``source_artifact_ref`` resolver for cognitive nodes.

Spec ``eca49df9`` (Cognitive Dedup Granularity, SUPERSEDE Wiring & Counted
Merge) — FR1/FR3, contract ``api_1d7b8fa5`` (``cognitive_source_ref``).

The cognitive over-dedup bug came from stamping a single non-unique
``spec:{id}`` ref on every Alternative/Assumption of a spec, so the
commit's ``(node_type, source_artifact_ref)`` dedup collapsed N distinct
concepts to one node. This module discriminates the ref PER CONCEPT via a
content hash. It is deterministic and stable against reorder/insertion:
the same normalised content always yields the same ref, and a ref does
NOT depend on a concept's position in the extraction list.

Scope: ``Alternative`` and ``Assumption`` are the spec-driven cognitive
types (``cognitive_extraction.py`` has no ``_extract_learnings`` — Learning
is bug-driven and out of scope for the spec-driven producer). The
structural ``_spec_child_ref`` worker is untouched: per-concept
granularity is exclusive to cognitive nodes.
"""

from __future__ import annotations

import hashlib
import re

# Cognitive types that have a spec-driven producer. Learning-from-spec is
# out of scope (no _extract_learnings); rejected here so a caller can't
# silently mint a Learning ref the producer never creates.
_VALID_COGNITIVE_TYPES = frozenset({"alternative", "assumption"})

_WS_RE = re.compile(r"\s+")


class CognitiveSourceRefError(ValueError):
    """Raised by :func:`cognitive_source_ref` on contract violations.

    ``code`` mirrors the ``api_1d7b8fa5`` response_errors:
    ``invalid_cognitive_type`` | ``missing_source_content`` |
    ``unstable_or_empty_source_key``.
    """

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


def _normalize(text: str) -> str:
    """Documented normalisation: trim, lowercase, collapse runs of
    whitespace to a single space. Two semantically-equivalent texts
    normalise to the same key — the basis of reorder/insertion stability.
    """
    return _WS_RE.sub(" ", (text or "").strip().lower())


def content_hash8(content: str) -> str:
    """First 8 hex chars of sha256 over the normalised content.

    Deterministic and position-independent: equivalent content -> same
    hash regardless of where the concept sits in the list.
    """
    return hashlib.sha256(_normalize(content).encode("utf-8")).hexdigest()[:8]


def per_concept_source_ref(base_source_ref: str, type_token: str, content: str) -> str:
    """Build ``{base_source_ref}:{type_token}:{content_hash8}``.

    ``base_source_ref`` is the artifact ref produced upstream (e.g.
    ``spec:<spec_id>``); the live extractors receive it and append the
    per-concept discriminator. Non-raising — extractor sentences are always
    non-empty; an empty content still yields the stable hash of the empty
    string. Use :func:`cognitive_source_ref` when contract-level validation
    of the inputs is required (e.g. the MCP add_node_candidate path).
    """
    return f"{base_source_ref}:{type_token.strip().lower()}:{content_hash8(content)}"


def cognitive_source_ref(
    cognitive_type: str,
    source_entity_type: str,
    source_entity_id: str,
    content: str,
) -> str:
    """Contract resolver (``api_1d7b8fa5``).

    Returns ``{source_entity_type}:{source_entity_id}:{type_token}:{content_hash8}``
    e.g. ``spec:<spec_id>:alternative:<hash8>``.

    Raises :class:`CognitiveSourceRefError` with the contract error codes:
      - ``invalid_cognitive_type``: not Alternative/Assumption.
      - ``missing_source_content``: content is None/empty.
      - ``unstable_or_empty_source_key``: normalised content (or the
        source_entity_id) is empty — would collapse distinct concepts.
    """
    type_token = (cognitive_type or "").strip().lower()
    if type_token not in _VALID_COGNITIVE_TYPES:
        raise CognitiveSourceRefError(
            "invalid_cognitive_type",
            f"cognitive_type must be one of {sorted(_VALID_COGNITIVE_TYPES)}, "
            f"got {cognitive_type!r}",
        )
    if content is None or content == "":
        raise CognitiveSourceRefError("missing_source_content", "content is empty")
    if not _normalize(content):
        raise CognitiveSourceRefError(
            "unstable_or_empty_source_key", "normalised content is empty"
        )
    if not (source_entity_id or "").strip():
        raise CognitiveSourceRefError(
            "unstable_or_empty_source_key", "source_entity_id is empty"
        )
    base = f"{source_entity_type}:{source_entity_id}"
    return per_concept_source_ref(base, type_token, content)
