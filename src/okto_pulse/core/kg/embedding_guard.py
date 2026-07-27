"""Embedding compatibility guard — pure decision contract (spec MKG-D-S1).

A board graph whose vectors were produced by one embedding model MUST NOT
be silently queried with another: same-dimension model swaps corrupt every
similarity ranking without a single error (the worst failure mode), and
different-dimension swaps only explode later at write time. The physical
open (community ``ensure_board_graph_bootstrapped``) persists the effective
provider metadata on BoardMeta and consults this pure contract on every
first-open-per-process.

Verdicts (spec FR2/FR3, decisions D1/D2):
  * ``ok``            — persisted and effective metadata match; proceed.
  * ``stamp``         — nothing (valid) persisted yet; stamp the effective
                        values (first open / legacy board).
  * ``indeterminate`` — the EFFECTIVE side is not trustworthy (stub
                        provider, no metadata, dimension 0, registry not
                        configured): log and proceed, NEVER re-stamp.
  * ``mismatch``      — both sides valid and different: the open MUST be
                        refused with :class:`EmbeddingIncompatibleError`.

Pure: stdlib only. The concrete model value and the BoardMeta persistence
belong to the community edition (board decisions 281f7b278d6b /
e93eb07935d8); this module owns only the business rule.
"""

from __future__ import annotations

__all__ = [
    "VERDICT_INDETERMINATE",
    "VERDICT_MISMATCH",
    "VERDICT_OK",
    "VERDICT_STAMP",
    "EmbeddingIncompatibleError",
    "compare_embedding_compat",
]

VERDICT_OK = "ok"
VERDICT_STAMP = "stamp"
VERDICT_INDETERMINATE = "indeterminate"
VERDICT_MISMATCH = "mismatch"


class EmbeddingIncompatibleError(Exception):
    """Structured, fail-closed open refusal (stable code
    ``kg_embedding_incompatible`` — spec AC2)."""

    code = "kg_embedding_incompatible"

    def __init__(
        self,
        *,
        board_id: str,
        persisted_model: str | None,
        persisted_dimension: int | None,
        effective_model: str | None,
        effective_dimension: int | None,
    ) -> None:
        self.board_id = board_id
        self.persisted_model = persisted_model
        self.persisted_dimension = persisted_dimension
        self.effective_model = effective_model
        self.effective_dimension = effective_dimension
        self.remediation = (
            "The board graph was embedded with "
            f"'{persisted_model}' (dim={persisted_dimension}) but the "
            f"effective provider is '{effective_model}' "
            f"(dim={effective_dimension}). Restore the original provider, "
            "or run a sanctioned rebuild/re-embed of the board graph before "
            "opening it with the new provider."
        )
        super().__init__(
            f"kg_embedding_incompatible board={board_id} "
            f"persisted={persisted_model}/{persisted_dimension} "
            f"effective={effective_model}/{effective_dimension}"
        )


def _valid(model: object, dimension: object) -> bool:
    try:
        return bool(model) and int(dimension or 0) > 0
    except (TypeError, ValueError):
        return False


def compare_embedding_compat(
    persisted_model: str | None,
    persisted_dimension: int | None,
    effective_model: str | None,
    effective_dimension: int | None,
    *,
    effective_is_stub: bool = False,
) -> str:
    """Pure verdict for the open-time embedding guard (contract api_5f426482).

    The INDETERMINATE check runs first: an untrustworthy effective side can
    neither confirm a mismatch nor justify a stamp (spec D2 — stub
    test-envs keep working and never dirty a valid stamp).
    """

    if effective_is_stub or not _valid(effective_model, effective_dimension):
        return VERDICT_INDETERMINATE
    if not _valid(persisted_model, persisted_dimension):
        return VERDICT_STAMP
    if str(persisted_model) == str(effective_model) and int(
        persisted_dimension or 0
    ) == int(effective_dimension or 0):
        return VERDICT_OK
    return VERDICT_MISMATCH
