"""Public KG6.5 query policy DTO and bounded call contract for editions."""

from collections.abc import Mapping
from dataclasses import dataclass

DEFAULT_QUERY_TIMEOUT_MS = 15_000
MAX_QUERY_TIMEOUT_MS = 30_000
DEFAULT_QUERY_ROWS = 200
MAX_QUERY_ROWS = 1000


@dataclass(frozen=True)
class KGQueryPolicy:
    timeout_ms: int = DEFAULT_QUERY_TIMEOUT_MS

    def __post_init__(self):
        if type(self.timeout_ms) is not int or not 1 <= self.timeout_ms <= MAX_QUERY_TIMEOUT_MS:
            raise ValueError("invalid_kg_query_timeout_ms")

    @classmethod
    def from_settings(cls, settings: Mapping | None):
        if settings is not None and not isinstance(settings, Mapping):
            raise ValueError("invalid_board_query_settings")
        return cls((settings or {}).get("kg_query_timeout_ms", DEFAULT_QUERY_TIMEOUT_MS))

    def effective_timeout(self, requested: int | None = None) -> int:
        if requested is None:
            return self.timeout_ms
        if type(requested) is not int or requested < 1:
            raise ValueError("invalid_query_timeout_ms")
        return min(requested, self.timeout_ms)


def query_row_limit(requested: int | None = None) -> int:
    if requested is None or requested == 0 and type(requested) is int:
        return DEFAULT_QUERY_ROWS
    if type(requested) is not int or not 1 <= requested <= MAX_QUERY_ROWS:
        raise ValueError("query_rows_requires_1_to_1000")
    return requested
