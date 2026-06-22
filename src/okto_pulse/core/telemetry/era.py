"""Canonical era / semantics / trust_state vocabulary (spec R3A — owner).

Business rule ``br_ade18c8a`` makes R3A the single owner of this contract, and
decision ``dec_ab2ff16b`` centralizes it here so discovery (R2), backfill (R3B)
and reporting (R4/R6) classify the same historical vs steady-state cuts with ONE
vocabulary instead of inventing divergent names.

* ``era`` — which side of the watermark/delta fix produced the data.
* ``semantics`` — how the metric values compose (a delta batch must be summed,
  a cumulative/snapshot must NOT be summed as if it were a delta).
* ``trust_state`` — the composed classification a consumer (R4) uses to decide
  whether a record may be aggregated as a trusted post-fix delta.

The steady-state publisher (R3A-B) stamps each ``/v1/usage`` batch with
``era=post_fix`` + ``semantics=delta`` (:data:`POST_FIX_DELTA_MARKER`). Reports
and backfill MUST read these explicit markers and never infer semantics from a
date or path alone (``br_8d26d92e``); :func:`classify_trust_state` is the
canonical derivation they share.
"""

from __future__ import annotations

from okto_pulse.core.telemetry.schema import CURRENT_SCHEMA_VERSION

# --- era ∈ {pre_fix, post_fix} (br_ade18c8a) -------------------------------
ERA_PRE_FIX = "pre_fix"  # data captured before the watermark/delta fix
ERA_POST_FIX = "post_fix"  # data captured by the steady-state delta publisher
VALID_ERAS = frozenset({ERA_PRE_FIX, ERA_POST_FIX})

# --- semantics ∈ {delta, cumulative, snapshot, unknown} (br_ade18c8a) ------
SEMANTICS_DELTA = "delta"  # only events not yet confirmed; safe to sum across batches
SEMANTICS_CUMULATIVE = "cumulative"  # legacy running totals; summing double-counts
SEMANTICS_SNAPSHOT = "snapshot"  # point-in-time; report latest, never sum
SEMANTICS_UNKNOWN = "unknown"  # un-markable legacy/pre-fix payloads
VALID_SEMANTICS = frozenset(
    {SEMANTICS_DELTA, SEMANTICS_CUMULATIVE, SEMANTICS_SNAPSHOT, SEMANTICS_UNKNOWN}
)

# --- trust_state ∈ {trusted_delta, untrusted, excluded, failed} (br_ade18c8a)
TRUST_TRUSTED_DELTA = "trusted_delta"  # post_fix delta on the current schema → summable
TRUST_UNTRUSTED = "untrusted"  # markable but not a trusted post-fix delta
TRUST_EXCLUDED = "excluded"  # deliberately kept out of trusted aggregates (pre-fix)
TRUST_FAILED = "failed"  # could not be classified (missing/invalid markers)
VALID_TRUST_STATES = frozenset(
    {TRUST_TRUSTED_DELTA, TRUST_UNTRUSTED, TRUST_EXCLUDED, TRUST_FAILED}
)

# The marker every steady-state delta batch carries (fr_169be135 / ir_d7bcef31).
POST_FIX_DELTA_MARKER: dict[str, str] = {
    "era": ERA_POST_FIX,
    "semantics": SEMANTICS_DELTA,
}

# The marker the product-telemetry SNAPSHOT path carries (R3A-F): cumulative /
# point-in-time data marked explicitly as snapshot so a consumer NEVER sums it as
# a trusted_delta. It must travel on a separate path, never inside a delta batch.
POST_FIX_SNAPSHOT_MARKER: dict[str, str] = {
    "era": ERA_POST_FIX,
    "semantics": SEMANTICS_SNAPSHOT,
}


def classify_trust_state(
    *,
    era: str | None,
    semantics: str | None,
    schema_version: str | None,
    current_schema_version: str = CURRENT_SCHEMA_VERSION,
) -> str:
    """Canonical trust classification consumed by R3B/R4/R6 — never re-defined.

    A record is :data:`TRUST_TRUSTED_DELTA` only when it is a post-fix delta on
    the CURRENT schema — i.e. produced by this watermark-based publisher. Pre-fix
    data is :data:`TRUST_EXCLUDED` (kept separate so R4 never sums it as a
    trusted delta with post-fix data — ``br_660cdac7``); anything whose markers
    are missing/invalid is :data:`TRUST_FAILED`; a validly-marked but
    non-delta/old-schema record is :data:`TRUST_UNTRUSTED`.
    """
    if era not in VALID_ERAS or semantics not in VALID_SEMANTICS:
        return TRUST_FAILED
    if era == ERA_PRE_FIX:
        return TRUST_EXCLUDED
    if semantics == SEMANTICS_DELTA and schema_version == current_schema_version:
        return TRUST_TRUSTED_DELTA
    return TRUST_UNTRUSTED
