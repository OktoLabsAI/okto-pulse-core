"""Parent artifact resolution for KG retrieval (ideação fe55ff7c).

Each KG node carries a ``source_artifact_ref`` string in the format
``"type:uuid"`` (type ∈ {spec, sprint, card}) set by the
DeterministicWorker when the node was consolidated. Granular nodes
(Decision, Criterion, TestScenario, BusinessRule, ...) lose their
parent context in retrieval — the agent receives fragments without
knowing which spec/card they came from. This module resolves the
parent artifact from the ref and injects title + status so the agent
can orient itself.

Public API:
- ``parse_artifact_ref(ref)`` — validate and decompose the ref.
- ``resolve_parent_artifacts(db, refs)`` — batch lookup, one query
  per artifact type. Orphans and malformed refs are silently omitted
  from the returned dict (callers interpret "not in dict" as "no
  parent found" and keep the row with parent_artifact=None).
"""

from .parent_doc import parse_artifact_ref, resolve_parent_artifacts

__all__ = ["parse_artifact_ref", "resolve_parent_artifacts"]
