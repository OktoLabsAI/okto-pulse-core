"""Transport-free application use cases (SaaS Refactor spec #09).

First cut: ``create_board`` (REST-only), ``move_ideation`` and
``submit_spec_validation`` (paired REST/MCP). Each use case delegates to the
existing service layer to preserve Community behavior; inbound adapters
(card 77188b6f) build the command/actor and map the result/errors back to the
transport. Nothing here may import a transport framework — see
``okto_pulse.core.application.purity_gate``.
"""

from __future__ import annotations

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    CommandValidationError,
    EntityNotFoundError,
    Source,
    UseCase,
    UseCaseError,
    commit,
    session_of,
)
from okto_pulse.core.application.use_cases.create_board import (
    CreateBoardCommand,
    CreateBoardResult,
    CreateBoardUseCase,
)
from okto_pulse.core.application.use_cases.move_ideation import (
    MoveIdeationCommand,
    MoveIdeationResult,
    MoveIdeationUseCase,
)
from okto_pulse.core.application.use_cases.submit_spec_validation import (
    SubmitSpecValidationCommand,
    SubmitSpecValidationResult,
    SubmitSpecValidationUseCase,
)

__all__ = [
    # base
    "ActorContext",
    "Source",
    "UseCase",
    "UseCaseError",
    "CommandValidationError",
    "EntityNotFoundError",
    "session_of",
    "commit",
    # create_board
    "CreateBoardCommand",
    "CreateBoardResult",
    "CreateBoardUseCase",
    # move_ideation
    "MoveIdeationCommand",
    "MoveIdeationResult",
    "MoveIdeationUseCase",
    # submit_spec_validation
    "SubmitSpecValidationCommand",
    "SubmitSpecValidationResult",
    "SubmitSpecValidationUseCase",
]
