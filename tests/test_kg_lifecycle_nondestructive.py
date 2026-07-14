"""Spec 3d89c192 — lifecycle de durabilidade não-destrutivo no hot path.

Automatiza os test scenarios ts_d4f7b001..ts_d4f7b006:
- worker commit (checkpoint/flush/fsync) nunca fecha o Database compartilhado
- corrida leitor contínuo vs close exclusivo (close guard FR-5)
- durabilidade sobrevive a kill abrupto do processo (CHECKPOINT real)
- rebuild lane preserva o close_reopen_probe destrutivo
- falha de CHECKPOINT bloqueia o ACK da queue (BR-3)
- FLUSH/FSYNC não-destrutivos com leitor ativo
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from contextvars import copy_context
from pathlib import Path

import pytest

import kg_schema_testing as schema
from okto_pulse.core.kg.safe_write_lifecycle import (
    DEFAULT_REQUIRED_STEPS,
    STEP_CHECKPOINT,
    STEP_CLOSE_REOPEN_PROBE,
    STEP_FLUSH,
    STEP_FSYNC,
)
from kg_schema_testing import (
    apply_ladybug_lifecycle_step,
    bootstrap_board_graph,
    close_all_connections,
    close_board_db_cache,
    open_board_connection,
)

kg_runtime = pytest.importorskip(
    "okto_pulse.community.adapters.kg_runtime",
    reason="AF-04 Community integration test requires the Community KG runtime adapter.",
)

SRC_DIR = str(Path(__file__).resolve().parents[1] / "src")


@pytest.fixture
def nd_board():
    bid = f"board-ndlc-{os.urandom(3).hex()}"
    bootstrap_board_graph(bid)
    yield bid
    close_all_connections(bid)


class _CheckpointSpyConnection:
    """Proxy de kuzu.Connection que grava as queries executadas."""

    def __init__(self, inner, log: list[str]):
        self._inner = inner
        self._log = log

    def execute(self, query, *args, **kwargs):
        self._log.append(str(query))
        return self._inner.execute(query, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def _spy_board_connection(monkeypatch, executed: list[str], *, fail_checkpoint: bool = False):
    orig_bc = kg_runtime.BoardConnection

    class SpyBC(orig_bc):
        def __init__(self, board_id: str) -> None:
            super().__init__(board_id)
            inner = self.conn
            if fail_checkpoint:
                class FailingConn(_CheckpointSpyConnection):
                    def execute(self, query, *args, **kwargs):
                        if "CHECKPOINT" in str(query).upper():
                            raise RuntimeError("forced checkpoint failure (ts_d4f7b005)")
                        return super().execute(query, *args, **kwargs)
                self.conn = FailingConn(inner, executed)
            else:
                self.conn = _CheckpointSpyConnection(inner, executed)

    monkeypatch.setattr(kg_runtime, "BoardConnection", SpyBC)


# ---------------------------------------------------------------------------
# ts_d4f7b001 — worker commit não fecha Database compartilhado (FR-1..FR-4)
# ---------------------------------------------------------------------------


def test_worker_steps_do_not_close_shared_database(nd_board, monkeypatch):
    close_calls: list[str] = []
    orig_cac = schema.close_all_connections
    orig_cbdc = schema.close_board_db_cache
    monkeypatch.setattr(
        schema, "close_all_connections",
        lambda *a, **k: (close_calls.append("close_all_connections"), orig_cac(*a, **k)),
    )
    monkeypatch.setattr(
        schema, "close_board_db_cache",
        lambda *a, **k: (close_calls.append("close_board_db_cache"), orig_cbdc(*a, **k)),
    )
    checkpoints: list[str] = []
    _spy_checkpoint(monkeypatch, checkpoints)

    for step in (STEP_CHECKPOINT, STEP_FLUSH, STEP_FSYNC):
        result = apply_ladybug_lifecycle_step(nd_board, "board_graph", step)
        assert result.ok is True, f"step {step} falhou: {result.detail}"

    assert close_calls == [], (
        f"subset do worker fechou o Database compartilhado: {close_calls}"
    )
    assert len(checkpoints) == 1, (
        "STEP_CHECKPOINT nao executou a instrucao CHECKPOINT real"
    )


# ---------------------------------------------------------------------------
# ts_d4f7b002 — corrida leitor contínuo vs close exclusivo (FR-5/BR-2)
# ---------------------------------------------------------------------------


def test_race_continuous_reader_vs_exclusive_close(nd_board):
    reader_errors: list[str] = []
    stop = threading.Event()

    def reader_loop():
        while not stop.is_set():
            try:
                with open_board_connection(nd_board) as (_db, conn):
                    res = conn.execute("MATCH (m:BoardMeta) RETURN count(m)")
                    res.get_next()
            except Exception as exc:  # noqa: BLE001 — qualquer erro é falha do teste
                reader_errors.append(f"{type(exc).__name__}: {exc}")
                break

    th = threading.Thread(
        target=copy_context().run,
        args=(reader_loop,),
        name="kg-race-reader",
    )
    th.start()
    try:
        # Deadline folgado + exigência mínima de 3 closes: sob carga externa
        # (suite rodando em paralelo com servidor/backfill) cada close drena
        # leitores e fica lento — o objetivo do teste é a CORRIDA, não a
        # vazão de closes.
        deadline = time.monotonic() + 8.0
        closes = 0
        while time.monotonic() < deadline and not reader_errors:
            close_board_db_cache(nd_board)
            closes += 1
            if closes >= 8:
                break
            time.sleep(0.05)
    finally:
        stop.set()
        th.join(timeout=15.0)

    assert closes >= 3, "teste nao exercitou closes suficientes"
    assert reader_errors == [], (
        f"use-after-close observado no leitor concorrente: {reader_errors[:3]}"
    )


def test_close_guard_fail_closed_defers_and_emits_warning(nd_board, monkeypatch, caplog):
    # KGD-01 FR6 (spec 26b46ef3, C6) — mudança de contrato INTENCIONAL.
    # ANTES: o close legítimo era FAIL-OPEN — após o timeout do dreno fechava
    # o Database com leitores vivos e emitia kg.close_guard.timeout (este
    # teste assertava esse warning). Esse fail-open é o produtor provável do
    # "escritor stale" que zera páginas interiores do WAL (KB1/H3).
    # AGORA: fail-closed — o close é ADIADO (kg.close_guard.deferred), o
    # Database permanece aberto/utilizável pelo leitor, e só o caminho de
    # shutdown pode forçar (force_after_drain_timeout=True →
    # kg.close_guard.forced_on_shutdown).
    # Encurta o dreno para o teste não custar 5s.
    monkeypatch.setattr(kg_runtime, "_CLOSE_DRAIN_TIMEOUT_S", 0.2)
    bc = schema.BoardConnection(nd_board)  # leitor "vazado" proposital
    try:
        with caplog.at_level("WARNING", logger="okto_pulse.kg.schema"):
            close_board_db_cache(nd_board)
        assert any(
            "kg.close_guard.deferred" in rec.message for rec in caplog.records
        ), "fail-closed nao emitiu o warning kg.close_guard.deferred"
        assert not any(
            "kg.close_guard.timeout" in rec.message for rec in caplog.records
        ), "caminho fail-open antigo (kg.close_guard.timeout) ainda ativo"
        # Fail-closed de verdade: o Database NAO foi fechado sob o leitor —
        # a conexao segue utilizavel (o fail-open antigo causava
        # use-after-close nativo aqui).
        res = bc.conn.execute("MATCH (m:BoardMeta) RETURN count(m)")
        res.get_next()
        res.close()
    finally:
        bc.close()


# ---------------------------------------------------------------------------
# ts_d4f7b003 — durabilidade sobrevive a kill do processo (FR-1/AC-3)
# ---------------------------------------------------------------------------


_WRITER_SCRIPT = r"""
import os, sys, time
sys.path.insert(0, os.environ["ND_SRC"])
# R-P2-03: this is a FRESH core subprocess with no composition root, so it must
# configure the KG registry (embedded fakes via the sanctioned test
# defaults_factory) before any board op reads get_kg_registry().config.
sys.path.insert(0, os.environ["ND_COMMUNITY_SRC"])
sys.path.insert(0, os.environ["ND_TESTS"])
from kg_registry_testing import configure_test_kg_registry
from okto_pulse.community.config import CommunitySettings
from okto_pulse.core.infra.config import configure_settings
configure_settings(CommunitySettings())
configure_test_kg_registry(graph_provider="real")
from kg_schema_testing import (
    BoardConnection, apply_ladybug_lifecycle_step, bootstrap_board_graph,
)
bid = os.environ["ND_BOARD"]
bootstrap_board_graph(bid)
bc = BoardConnection(bid)
bc.conn.execute("CREATE NODE TABLE IF NOT EXISTS KillT(id INT64, PRIMARY KEY(id))")
for i in range(25):
    bc.conn.execute(f"CREATE (:KillT {{id: {i}}})")
bc.close()
result = apply_ladybug_lifecycle_step(bid, "board_graph", "checkpoint")
assert result.ok, result.detail
print("READY", flush=True)
time.sleep(60)  # o parent mata o processo aqui — sem teardown
"""

_READER_SCRIPT = r"""
import os, sys
sys.path.insert(0, os.environ["ND_SRC"])
# R-P2-03: fresh core subprocess — compose the registry before board ops.
sys.path.insert(0, os.environ["ND_COMMUNITY_SRC"])
sys.path.insert(0, os.environ["ND_TESTS"])
from kg_registry_testing import configure_test_kg_registry
from okto_pulse.community.config import CommunitySettings
from okto_pulse.core.infra.config import configure_settings
configure_settings(CommunitySettings())
configure_test_kg_registry(graph_provider="real")
from kg_schema_testing import BoardConnection
bid = os.environ["ND_BOARD"]
bc = BoardConnection(bid)
res = bc.conn.execute("MATCH (k:KillT) RETURN count(k)")
print("COUNT", res.get_next()[0], flush=True)
bc.close()
"""


def test_kill_durability_checkpoint_survives_abrupt_death(nd_board, tmp_path):
    env = dict(os.environ)
    env["ND_SRC"] = SRC_DIR
    env["ND_COMMUNITY_SRC"] = str(
        Path(__file__).resolve().parents[2] / "okto_labs_pulse_community" / "src"
    )
    env["ND_TESTS"] = str(Path(__file__).resolve().parent)
    env["ND_BOARD"] = f"board-kill-{os.urandom(3).hex()}"
    # Reusa o KG_BASE_DIR do ambiente de teste (conftest) — escritor e leitor
    # compartilham o mesmo diretório, processos distintos.
    writer = subprocess.Popen(
        [sys.executable, "-c", _WRITER_SCRIPT],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        line = ""
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            line = writer.stdout.readline()
            if "READY" in line:
                break
        assert "READY" in line, (
            f"writer nunca ficou READY; stderr={writer.stderr.read()[:2000]}"
        )
    finally:
        writer.kill()  # TerminateProcess — sem flush/teardown de interpreter
        writer.wait(timeout=30)

    reader = subprocess.run(
        [sys.executable, "-c", _READER_SCRIPT],
        env=env, capture_output=True, text=True, timeout=120,
    )
    assert reader.returncode == 0, f"reader falhou: {reader.stderr[:2000]}"
    assert "COUNT [25]" in reader.stdout or "COUNT 25" in reader.stdout, (
        f"dados commitados nao sobreviveram ao kill: {reader.stdout!r}"
    )


# ---------------------------------------------------------------------------
# ts_d4f7b004 — rebuild lane preserva close_reopen_probe (AC-4)
# ---------------------------------------------------------------------------


def test_default_steps_keep_destructive_probe(nd_board, monkeypatch):
    assert DEFAULT_REQUIRED_STEPS == (
        STEP_CHECKPOINT, STEP_FLUSH, STEP_FSYNC, STEP_CLOSE_REOPEN_PROBE,
    ), "DEFAULT_REQUIRED_STEPS mudou — rebuild/recovery perderiam o probe"

    close_calls: list[bool] = []
    _spy_try_close(monkeypatch, close_calls)
    result = apply_ladybug_lifecycle_step(nd_board, "board_graph", STEP_CLOSE_REOPEN_PROBE)
    assert result.ok is True, f"probe falhou: {result.detail}"
    assert close_calls == [True], (
        "close_reopen_probe deixou de fechar handles — lane destrutiva quebrada"
    )


def test_destructive_probe_fails_closed_under_stuck_reader(nd_board, monkeypatch):
    """Review dcea02d (F4): o probe NÃO pode fechar o Database em fail-open
    com leitor ativo (use-after-close nativo → SIGSEGV). Com um leitor que
    não sai dentro do dreno, o probe falha explicitamente, sem fechar."""
    # Encurta o dreno do probe para o teste não esperar 30s.
    orig = kg_runtime.try_close_board_db

    def fast_probe_close(board_id, *, drain_timeout=None, fast_path=True):
        return orig(board_id, drain_timeout=0.05, fast_path=fast_path)

    monkeypatch.setattr(kg_runtime, "try_close_board_db", fast_probe_close)

    with open_board_connection(nd_board) as (_db, conn):
        before = conn.execute("MATCH (m:BoardMeta) RETURN count(m)").get_next()
        result = apply_ladybug_lifecycle_step(
            nd_board, "board_graph", STEP_CLOSE_REOPEN_PROBE
        )
        assert result.ok is False
        assert "active_readers" in (result.detail or "")
        # O leitor sobrevive — nada foi fechado por baixo dele.
        after = conn.execute("MATCH (m:BoardMeta) RETURN count(m)").get_next()
        assert after == before


def test_worker_subset_constant_excludes_probe():
    from okto_pulse.core.application.processors.consolidation import WORKER_COMMIT_LIFECYCLE_STEPS

    assert WORKER_COMMIT_LIFECYCLE_STEPS == (STEP_CHECKPOINT, STEP_FLUSH, STEP_FSYNC)
    assert STEP_CLOSE_REOPEN_PROBE not in WORKER_COMMIT_LIFECYCLE_STEPS


# ---------------------------------------------------------------------------
# ts_d4f7b005 — falha de CHECKPOINT bloqueia ACK da queue (BR-3)
# ---------------------------------------------------------------------------


def _spy_try_close(monkeypatch, calls: list[bool]):
    orig = kg_runtime.try_close_board_db

    def spy(board_id: str, **kwargs) -> bool:
        result = orig(board_id, **kwargs)
        calls.append(result)
        return result

    monkeypatch.setattr(kg_runtime, "try_close_board_db", spy)


def _spy_checkpoint(monkeypatch, calls: list[str], *, fail: bool = False):
    """Espia _execute_checkpoint_unguarded (o CHECKPOINT roda em janela
    exclusiva com conexão crua desde o fix do 6º crash — o SpyBC de
    BoardConnection não o vê mais)."""
    orig = kg_runtime._execute_checkpoint_unguarded

    def spy(path):
        calls.append(str(path))
        if fail:
            raise RuntimeError("forced checkpoint failure (ts_d4f7b005)")
        return orig(path)

    monkeypatch.setattr(kg_runtime, "_execute_checkpoint_unguarded", spy)


def test_periodic_buffer_hygiene_closes_every_kth_commit(nd_board, monkeypatch):
    """Campo 2026-06-10 (3 crashes): CHECKPOINTs sucessivos no mesmo Database
    aberto degradam o buffer do Ladybug até abort nativo. A cada K commits o
    step troca o CHECKPOINT pelo CLOSE (higiene do buffer pool)."""
    monkeypatch.setenv("KG_CHECKPOINT_CLOSE_INTERVAL", "3")
    kg_runtime._reset_checkpoint_counter(nd_board)
    close_calls: list[bool] = []
    _spy_try_close(monkeypatch, close_calls)
    checkpoints: list[str] = []
    _spy_checkpoint(monkeypatch, checkpoints)

    for _ in range(6):
        result = apply_ladybug_lifecycle_step(nd_board, "board_graph", STEP_CHECKPOINT)
        assert result.ok is True

    # K=3: commits 3 e 6 viram CLOSE; os demais (1,2,4,5) usam CHECKPOINT.
    assert close_calls == [True, True], (
        f"esperava 2 closes efetivos em 6 commits, veio {close_calls}"
    )
    assert len(checkpoints) == 4, f"esperava 4 CHECKPOINTs, veio {len(checkpoints)}"


def test_hygiene_close_skipped_under_active_reader(nd_board, monkeypatch):
    """Campo 2026-06-10 (4º crash): a higiene fechava o Database em fail-open
    com um health scan lendo o board → use-after-close nativo → exit 5.
    Com leitor ativo o close é PULADO (commit faz CHECKPOINT normal) e o
    contador fica re-armado: cada commit seguinte re-tenta até o leitor
    sair — só então o close acontece."""
    monkeypatch.setenv("KG_CHECKPOINT_CLOSE_INTERVAL", "3")
    monkeypatch.setattr(kg_runtime, "_HYGIENE_CLOSE_DRAIN_TIMEOUT_S", 0.05)
    kg_runtime._reset_checkpoint_counter(nd_board)
    close_calls: list[bool] = []
    _spy_try_close(monkeypatch, close_calls)
    checkpoints: list[str] = []
    _spy_checkpoint(monkeypatch, checkpoints)

    with open_board_connection(nd_board) as (_db, conn):
        before = conn.execute("MATCH (m:BoardMeta) RETURN count(m)").get_next()
        # K=3: commits 1-2 tentariam CHECKPOINT — ADIADO (6º crash:
        # CHECKPOINT sob leitor concorrente = SIGSEGV nativo); 3 = tentativa
        # de close (PULADA, leitor ativo); 4 = re-tentativa via re-arme
        # (PULADA). Sem o re-arme, o commit 4 nem tentaria o close.
        for _ in range(4):
            result = apply_ladybug_lifecycle_step(
                nd_board, "board_graph", STEP_CHECKPOINT
            )
            assert result.ok is True, result.detail
        assert close_calls == [False, False], (
            f"close deveria ser pulado 2x sob leitor ativo, veio {close_calls}"
        )
        assert checkpoints == [], (
            "CHECKPOINT nao pode rodar com leitor ativo (SIGSEGV nativo)"
        )
        # O leitor sobrevive intacto — prova de que nada foi fechado.
        after = conn.execute("MATCH (m:BoardMeta) RETURN count(m)").get_next()
        assert after == before

    # Leitor saiu: o próximo commit (contador re-armado) fecha de verdade.
    result = apply_ladybug_lifecycle_step(nd_board, "board_graph", STEP_CHECKPOINT)
    assert result.ok is True, result.detail
    assert close_calls == [False, False, True], (
        f"close deveria acontecer apos o leitor sair, veio {close_calls}"
    )


def test_checkpoint_failure_falls_back_to_close(nd_board, monkeypatch):
    """Campo 2026-06-10: CHECKPOINT esgotou o buffer manager do Ladybug sob
    backfill massivo e derrubava o processo. O fallback fecha o Database do
    board (libera o buffer pool + checkpoint implícito do close) e o step
    SUCEDE — a durabilidade fica garantida pelo close."""
    checkpoints: list[str] = []
    _spy_checkpoint(monkeypatch, checkpoints, fail=True)
    result = apply_ladybug_lifecycle_step(nd_board, "board_graph", STEP_CHECKPOINT)
    assert result.ok is True, f"fallback close deveria salvar o step: {result.detail}"
    assert checkpoints, "CHECKPOINT nem chegou a ser tentado"
    key = str(schema.board_kuzu_path(nd_board))
    with kg_runtime._board_db_cache_lock:
        assert key not in kg_runtime._board_db_cache, (
            "fallback nao fechou o Database (buffer pool nao liberado)"
        )


def test_checkpoint_skipped_under_active_reader_after_failure_setup(
    nd_board, monkeypatch
):
    """CHECKPOINT exige janela exclusiva: com leitor ativo o step é adiado
    SEM executar o CHECKPOINT (6º crash: SIGSEGV nativo) e SEM fechar nada
    — o commit já está no WAL e o STEP_FSYNC sincroniza os arquivos."""
    kg_runtime._reset_checkpoint_counter(nd_board)
    checkpoints: list[str] = []
    _spy_checkpoint(monkeypatch, checkpoints, fail=True)

    with open_board_connection(nd_board) as (_db, conn):
        before = conn.execute("MATCH (m:BoardMeta) RETURN count(m)").get_next()
        result = apply_ladybug_lifecycle_step(
            nd_board, "board_graph", STEP_CHECKPOINT
        )
        assert result.ok is True, f"step deveria suceder via WAL: {result.detail}"
        assert checkpoints == [], "CHECKPOINT nao pode rodar com leitor ativo"
        after = conn.execute("MATCH (m:BoardMeta) RETURN count(m)").get_next()
        assert after == before


def test_checkpoint_and_fallback_failure_blocks_queue_ack(nd_board, monkeypatch):
    """BR-3 preservada no caso terminal: se o CHECKPOINT falha E o fallback
    close também falha (falha REAL de close, não skip por leitor), o step
    falha e o worker NÃO ACKa o queue entry."""
    from okto_pulse.core.application.processors.consolidation import (
        _apply_board_graph_lifecycle_after_commit,
    )

    def _broken_close(*_a, **_k):
        raise RuntimeError("forced close failure (terminal)")

    monkeypatch.setattr(kg_runtime, "_close_cached_db_unguarded", _broken_close)
    checkpoints: list[str] = []
    _spy_checkpoint(monkeypatch, checkpoints, fail=True)
    with pytest.raises(RuntimeError) as exc_info:
        _apply_board_graph_lifecycle_after_commit(
            board_id=nd_board,
            owner_token="consolidation-worker:test-entry:deadbeef",
            mutation_ref="spec:test:session",
        )
    msg = str(exc_info.value)
    assert "board_graph_safe_lifecycle_failed" in msg
    assert "failed_step=checkpoint" in msg


# ---------------------------------------------------------------------------
# ts_d4f7b006 — FLUSH/FSYNC não-destrutivos com leitor ativo (FR-2/FR-3)
# ---------------------------------------------------------------------------


def test_flush_fsync_keep_active_reader_alive(nd_board):
    with open_board_connection(nd_board) as (_db, conn):
        before = conn.execute("MATCH (m:BoardMeta) RETURN count(m)").get_next()

        for step in (STEP_FLUSH, STEP_FSYNC):
            result = apply_ladybug_lifecycle_step(nd_board, "board_graph", step)
            assert result.ok is True, f"step {step} falhou: {result.detail}"

        # O leitor aberto DURANTE os steps continua plenamente funcional —
        # prova de que o Database compartilhado nunca foi fechado.
        after = conn.execute("MATCH (m:BoardMeta) RETURN count(m)").get_next()
        assert after == before
