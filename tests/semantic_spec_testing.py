"""Current semantic payload fixtures; no conversion or identity allocation."""
import copy
from okto_pulse.core.domain.quality_canonicalization import SEMANTIC_FIELD_MANIFEST_V1

def semantic_spec_payload(spec):
    return {**{field: copy.deepcopy(getattr(spec, field, None))
               for field in SEMANTIC_FIELD_MANIFEST_V1["spec"]},
            "id": spec.id, "board_id": spec.board_id, "version": spec.version}
