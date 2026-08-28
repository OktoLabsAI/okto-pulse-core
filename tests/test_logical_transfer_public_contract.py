from __future__ import annotations

from okto_pulse.core.application.boundary.public_contract_manifest import (
    PUBLIC_CORE_CONTRACT_SURFACES,
    is_public_core_contract,
)


def test_logical_transfer_is_public_without_weakening_private_boundaries() -> None:
    logical_transfer = "okto_pulse.core.kg.logical_transfer"

    assert logical_transfer in PUBLIC_CORE_CONTRACT_SURFACES
    assert is_public_core_contract(logical_transfer)
    assert is_public_core_contract(f"{logical_transfer}.LogicalSnapshotSource")
    assert not is_public_core_contract(
        "okto_pulse.core.kg.workers.recovery.InternalWorker"
    )
