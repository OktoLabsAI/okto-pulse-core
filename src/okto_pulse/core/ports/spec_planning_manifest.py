"""Transaction-bound freshness of the inputs to Spec planning conclusions.

These revisions fence a request; they do not decide Current/Previous, replace
authorization or grant approval. Concrete storage and revision tracking belong
to the edition. Callers authorize the subject before reading this port.
"""

from typing import Protocol

from okto_pulse.core.domain.spec_planning import SpecPlanningManifest
from okto_pulse.core.domain.spec_validation import SpecValidationConflictError


class SpecPlanningManifestConflict(SpecValidationConflictError):
    code = "spec_planning_manifest_conflict"


class SpecPlanningManifestUnavailable(RuntimeError):
    code = "spec_planning_manifest_unavailable"


class SpecPlanningManifestPort(Protocol):
    """Read 1..50 manifests from the caller's authoritative transaction.

    Return in requested order. Missing source/revision/realm authority is an
    error, never revision zero. The caller must hold the edition's write fence
    before checking a manifest for mutation. Reads must see its prior writes.
    """

    async def read_many(
        self, *, board_id: str, spec_ids: tuple[str, ...],
    ) -> tuple[SpecPlanningManifest, ...]: ...

    async def require_current(self, expected: SpecPlanningManifest) -> None: ...
