"""Declarative per-operation curation autonomy policy (spec MKG-C-S1 — FR5, D3).

Business rule owned by the CORE (board guideline: rules live in the core;
enforcement lives in the entrypoints — community CLI / MCP use cases —
following decision_8506b9471f48: classification and enforcement are
separate mechanisms).

The rule that shapes every level (BR2, marginalia ADR 0009): REVERSIBILITY,
not confidence, defines the default —

* ``auto``          — deterministic, non-destructive; may run unattended
                      (the daily decay tick recomputes scores, destroys
                      nothing).
* ``propose_only``  — irreversible or state-mutating; requires an explicit
                      confirmation artifact (CLI ``--confirm``/``--approve``,
                      the rebuild's preflight→confirm token, a DLQ item id
                      listed first). This is the FAIL-CLOSED DEFAULT for any
                      operation not classified below.
* ``forbidden``     — never runs through this surface (physical hard-delete
                      with bulk edge re-point — the mutation class behind
                      marginalia ADR 0007 / the KGD-01 corruption).

Pure data + pure functions: no I/O, no imports beyond stdlib.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Mapping

__all__ = [
    "CURATION_LEVEL_AUTO",
    "CURATION_LEVEL_FORBIDDEN",
    "CURATION_LEVEL_PROPOSE_ONLY",
    "CURATION_POLICY",
    "CurationPolicyError",
    "require_curation_allowed",
    "resolve_curation_level",
]

CURATION_LEVEL_AUTO = "auto"
CURATION_LEVEL_PROPOSE_ONLY = "propose_only"
CURATION_LEVEL_FORBIDDEN = "forbidden"

# The REAL inventory of curation operations (refinement MKG-C-R1 — the
# hypothetical 'quarantine purge' does not exist as a standalone surface
# and is deliberately absent). Immutable: levels derive from reversibility,
# not preference — mutating this at runtime is not supported.
CURATION_POLICY: Mapping[str, str] = MappingProxyType(
    {
        # Merge/curation writes — reversible only via the equivalence
        # ledger flow, so the write itself needs explicit confirmation.
        "kg_dedup_entities": CURATION_LEVEL_PROPOSE_ONLY,
        "kg_unmerge": CURATION_LEVEL_PROPOSE_ONLY,
        # Offline maintenance surfaces (community CLI).
        "kg_backfill_apply": CURATION_LEVEL_PROPOSE_ONLY,
        "kg_restore": CURATION_LEVEL_PROPOSE_ONLY,
        "kg_reset": CURATION_LEVEL_PROPOSE_ONLY,
        # DLQ reprocessing mutates graph state from listed items — the
        # list→reprocess(id) pair IS the propose→confirm artifact.
        "kg_dlq_reprocess": CURATION_LEVEL_PROPOSE_ONLY,
        "kg_connectivity_dlq_reprocess": CURATION_LEVEL_PROPOSE_ONLY,
        # Rebuild already carries the strongest confirm-gate in the tree
        # (preflight→confirm→run with immutable manifest hash).
        "kg_rebuild_run": CURATION_LEVEL_PROPOSE_ONLY,
        # Deterministic, non-destructive score recompute.
        "kg_decay_tick": CURATION_LEVEL_AUTO,
        # Physical delete + bulk edge re-point: the corruption class of
        # ADR 0007/KGD-01 — never through this surface (D2).
        "kg_dedup_hard_delete": CURATION_LEVEL_FORBIDDEN,
    }
)


class CurationPolicyError(Exception):
    """Structured policy refusal — stable code ``curation_policy_violation``.

    Carries operation + resolved level + remediation so entrypoints can
    render an actionable error without string-matching.
    """

    code = "curation_policy_violation"

    def __init__(self, operation: str, level: str, remediation: str) -> None:
        self.operation = operation
        self.level = level
        self.remediation = remediation
        super().__init__(
            f"curation_policy_violation: operation={operation} level={level} "
            f"remediation={remediation}"
        )


def resolve_curation_level(operation: str) -> str:
    """Resolve the autonomy level for ``operation``.

    Unclassified operations resolve to ``propose_only`` — the fail-closed
    default (BR2): a new destructive surface must OPT INTO ``auto``
    explicitly by earning a classification here, never by omission.
    """

    return CURATION_POLICY.get(operation, CURATION_LEVEL_PROPOSE_ONLY)


def require_curation_allowed(operation: str, *, confirmed: bool = False) -> str:
    """Pure gate consulted by entrypoints before a curation mutation.

    Returns the resolved level when the call may proceed. Raises
    :class:`CurationPolicyError` when:

    * the level is ``forbidden`` — always;
    * the level is ``propose_only`` and ``confirmed`` is False — the caller
      must present an explicit confirmation artifact (``--confirm``,
      ``--approve <id>``, confirm token, listed DLQ id).

    No I/O — safe to call anywhere, trivially testable.
    """

    level = resolve_curation_level(operation)
    if level == CURATION_LEVEL_FORBIDDEN:
        raise CurationPolicyError(
            operation,
            level,
            remediation=(
                "This operation is forbidden by the curation policy "
                "(irreversible physical mutation). Use the reversible "
                "default (tombstone + equivalence ledger); physical "
                "materialization happens only inside the deterministic "
                "rebuild."
            ),
        )
    if level == CURATION_LEVEL_PROPOSE_ONLY and not confirmed:
        raise CurationPolicyError(
            operation,
            level,
            remediation=(
                "This operation requires an explicit confirmation artifact. "
                "Run with --dry-run/--propose first to inspect the plan, "
                "then re-run with --confirm (or --approve <proposal_id>)."
            ),
        )
    return level
