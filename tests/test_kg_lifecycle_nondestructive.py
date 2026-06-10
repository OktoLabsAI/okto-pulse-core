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
from pathlib import Path

import pytest

from okto_pulse.core.kg import schema
from okto_pulse.core.kg.safe_write_lifecycle import (
    DEFAULT_REQUIRED_STEPS,
    STEP_CHECKPOINT,
    STEP_CLOSE_REOPEN_PROBE,
    STEP_FLUSH,
    STEP_FSYNC,
)
from okto_pulse.core.kg.schema import (
    apply_ladybug_lifecycle_step,
    bootstrap_board_graph,
    close_all_connections,
    close_board_db_cache,
    open_board_connection,
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
    orig_bc = schema.BoardConnection

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

    monkeypatch.setattr(schema, "BoardConnection", SpyBC)


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
    executed: list[str] = []
    _spy_board_connection(monkeypatch, executed)

    for step in (STEP_CHECKPOINT, STEP_FLUSH, STEP_FSYNC):
        result = apply_ladybug_lifecycle_step(nd_board, "board_graph", step)
        assert result.ok is True, f"step {step} falhou: {result.detail}"

    assert close_calls == [], (
        f"subset do worker fechou o Database compartilhado: {close_calls}"
    )
    assert any("CHECKPOINT" in q.upper() for q in executed), (
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

    th = threading.Thread(target=reader_loop, name="kg-race-reader")
    th.start()
    try:
        deadline = time.monotonic() + 4.0
        closes = 0
        while time.monotonic() < deadline and not reader_errors:
            close_board_db_cache(nd_board)
            closes += 1
            time.sleep(0.05)
    finally:
        stop.set()
        th.join(timeout=10.0)

    assert closes >= 5, "teste nao exercitou closes suficientes"
    assert reader_errors == [], (
        f"use-after-close observado no leitor concorrente: {reader_errors[:3]}"
    )


def test_close_guard_fail_open_emits_warning(nd_board, monkeypatch, caplog):
    # Encurta o dreno para o teste não custar 5s.
    monkeypatch.setattr(schema, "_CLOSE_DRAIN_TIMEOUT_S", 0.2)
    bc = schema.BoardConnection(nd_board)  # leitor "vazado" proposital
    try:
        with caplog.at_level("WARNING", logger="okto_pulse.kg.schema"):
            close_board_db_cache(nd_board)
        assert any("kg.close_guard.timeout" in rec.message for rec in caplog.records), (
            "fail-open nao emitiu o warning kg.close_guard.timeout"
        )
    finally:
        bc.close()


# ---------------------------------------------------------------------------
# ts_d4f7b003 — durabilidade sobrevive a kill do processo (FR-1/AC-3)
# ---------------------------------------------------------------------------


_WRITER_SCRIPT = r"""
import os, sys, time
sys.path.insert(0, os.environ["ND_SRC"])
from okto_pulse.core.kg.schema import (
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
from okto_pulse.core.kg.schema import BoardConnection
bid = os.environ["ND_BOARD"]
bc = BoardConnection(bid)
res = bc.conn.execute("MATCH (k:KillT) RETURN count(k)")
print("COUNT", res.get_next()[0], flush=True)
bc.close()
"""


def test_kill_durability_checkpoint_survives_abrupt_death(nd_board, tmp_path):
    env = dict(os.environ)
    env["ND_SRC"] = SRC_DIR
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

    close_calls: list[str] = []
    orig_cac = schema.close_all_connections
    monkeypatch.setattr(
        schema, "close_all_connections",
        lambda *a, **k: (close_calls.append("close_all_connections"), orig_cac(*a, **k)),
    )
    result = apply_ladybug_lifecycle_step(nd_board, "board_graph", STEP_CLOSE_REOPEN_PROBE)
    assert result.ok is True, f"probe falhou: {result.detail}"
    assert close_calls, "close_reopen_probe deixou de fechar handles — lane destrutiva quebrada"


def test_worker_subset_constant_excludes_probe():
    from okto_pulse.core.kg.workers.consolidation import WORKER_COMMIT_LIFECYCLE_STEPS

    assert WORKER_COMMIT_LIFECYCLE_STEPS == (STEP_CHECKPOINT, STEP_FLUSH, STEP_FSYNC)
    assert STEP_CLOSE_REOPEN_PROBE not in WORKER_COMMIT_LIFECYCLE_STEPS


# ---------------------------------------------------------------------------
# ts_d4f7b005 — falha de CHECKPOINT bloqueia ACK da queue (BR-3)
# ---------------------------------------------------------------------------


def test_checkpoint_failure_fails_step(nd_board, monkeypatch):
    executed: list[str] = []
    _spy_board_connection(monkeypatch, executed, fail_checkpoint=True)
    result = apply_ladybug_lifecycle_step(nd_board, "board_graph", STEP_CHECKPOINT)
    assert result.ok is False
    assert "forced checkpoint failure" in (result.detail or "")


def test_checkpoint_failure_blocks_queue_ack(nd_board, monkeypatch):
    from okto_pulse.core.kg.workers.consolidation import (
        _apply_board_graph_lifecycle_after_commit,
    )

    executed: list[str] = []
    _spy_board_connection(monkeypatch, executed, fail_checkpoint=True)
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
