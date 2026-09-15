from __future__ import annotations


def test_public_board_erasure_contention_contract_preserves_exception_identity() -> None:
    """An edition can map contention without importing private governance."""

    from okto_pulse.core.kg.governance import BoardErasureLockContention as private
    from okto_pulse.core.ports.board_erasure_control import (
        BoardErasureLockContention as public,
    )

    assert public is private
