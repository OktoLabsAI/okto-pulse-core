"""Deployment cadence validation and typed internal tick failures after public tuning retirement."""

from __future__ import annotations


import pytest

from coordination_fakes import FakeLeaseProvider, FakeWriteLockPort
from okto_pulse.core.ports.coordination import (
    register_coordination_providers,
    reset_coordination_providers_for_tests,
)


pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _coordination_ports():
    reset_coordination_providers_for_tests()
    register_coordination_providers(
        lease_provider=FakeLeaseProvider(),
        write_lock_port=FakeWriteLockPort(),
    )
    yield
    reset_coordination_providers_for_tests()


async def test_ts3_put_outside_range_rejected_by_pydantic():
    """TS3 — RuntimeSettingsPayload rejeita valor abaixo do mínimo (5).

    Validação acontece na própria classe Pydantic, antes do request chegar
    no service. Verificamos diretamente via Pydantic ValidationError.
    """
    from pydantic import ValidationError
    from okto_pulse.community.config import CommunitySettings

    with pytest.raises(ValidationError) as exc_info:
        CommunitySettings(_env_file=None, kg_decay_tick_interval_minutes=4)
    msg = str(exc_info.value)
    assert "greater than or equal to 5" in msg or "ge=5" in msg or "5" in msg

    with pytest.raises(ValidationError):
        CommunitySettings(_env_file=None, kg_decay_tick_interval_minutes=10081)


async def test_ts6_reset_last_recomputed_at_reports_typed_failure_for_empty_scope():
    """TS6 — `reset_last_recomputed_at` fecha o escopo por board com um erro
    TIPADO quando o board-alvo não tem grafo local, em vez de vazar o erro
    cru do runtime de grafo. Exercita o caminho de iteração + o registro de
    falha por board sem depender do estado global acumulado pela suíte.

    Validação completa do comportamento force_full_rebuild=true requer uma
    fixture do runtime de grafo da Community (Grafx) com nodes pré-existentes
    — deferred para integration test em sessão futura.
    """
    from okto_pulse.core.application.kg_tick import (
        KGTickFullRebuildResetFailed,
        reset_last_recomputed_at,
    )

    # Per-board scope com board inexistente tenta o GraphTransaction composto;
    # a falha é agregada num erro tipado que nomeia o board afetado.
    with pytest.raises(KGTickFullRebuildResetFailed) as excinfo:
        await reset_last_recomputed_at(board_id="board-does-not-exist-uuid")

    assert "board-does-not-exist-uuid" in str(excinfo.value)
