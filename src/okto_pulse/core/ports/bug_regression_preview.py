"""Read boundary for bug regression preview facts."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from dataclasses import dataclass
from typing import Any, Protocol, Sequence


@dataclass(frozen=True, slots=True)
class RegressionCardFact:
    id: str
    board_id: str
    spec_id: str | None
    origin_task_id: str | None
    card_type: str
    test_scenario_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RegressionSpecFact:
    id: str
    board_id: str
    test_scenarios: tuple[dict[str, Any], ...]


class BugRegressionPreviewReadPort(Protocol):
    async def get_card(
        self,
        context: Any,
        *,
        card_id: str,
    ) -> RegressionCardFact | None: ...

    async def get_spec(
        self,
        context: Any,
        *,
        spec_id: str,
    ) -> RegressionSpecFact | None: ...

    async def candidate_spec_ids(
        self,
        context: Any,
        *,
        board_id: str,
        candidate_scenario_ids: Sequence[str],
    ) -> dict[str, str]: ...


_RUNTIME_KEY = "ports.bug_regression_preview.reader"


def register_bug_regression_preview_read_port(
    reader: BugRegressionPreviewReadPort,
) -> None:
    register_runtime_value(_RUNTIME_KEY, reader)


def get_bug_regression_preview_read_port() -> BugRegressionPreviewReadPort:
    return require_runtime_value(_RUNTIME_KEY, "bug_regression_preview_read_port_not_configured")


def reset_bug_regression_preview_read_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "BugRegressionPreviewReadPort",
    "RegressionCardFact",
    "RegressionSpecFact",
    "get_bug_regression_preview_read_port",
    "register_bug_regression_preview_read_port",
    "reset_bug_regression_preview_read_port_for_tests",
]
