"""Public application contract for embedding compatibility decisions."""

from okto_pulse.core.kg.embedding_guard import (
    VERDICT_INDETERMINATE,
    VERDICT_MISMATCH,
    VERDICT_OK,
    VERDICT_STAMP,
    EmbeddingIncompatibleError,
    compare_embedding_compat,
)

__all__ = [
    "VERDICT_INDETERMINATE",
    "VERDICT_MISMATCH",
    "VERDICT_OK",
    "VERDICT_STAMP",
    "EmbeddingIncompatibleError",
    "compare_embedding_compat",
]
