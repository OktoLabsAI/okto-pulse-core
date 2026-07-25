"""Card 5: stale-canonical parity is complete and artifact-type safe."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from okto_pulse.core.application.rebuild_ports import BoardSourceSnapshot
from okto_pulse.core.kg import canonical_stale_reconciler as reconciler
from okto_pulse.core.kg import interfaces
from okto_pulse.core.kg.interfaces import SourceUnavailableError
from okto_pulse.core.kg.stale_canonical_parity import detect_board_graph_stale


@dataclass
class _SourceReader:
    snapshot: BoardSourceSnapshot

    def fetch(self, board_id: str) -> BoardSourceSnapshot:
        assert board_id == "board-card5-parity"
        return self.snapshot


class _CypherExecutor:
    def __init__(
        self,
        *,
        graph_ref: str | None = None,
        graph_layer: str = "canonical",
        maturity_status: str = "canonical_eligible",
        revocation_reason: str | None = None,
        relevance_score: float = 0.5,
    ) -> None:
        self.graph_ref = graph_ref
        self.graph_layer = graph_layer
        self.maturity_status = maturity_status
        self.revocation_reason = revocation_reason
        self.relevance_score = relevance_score
        self.calls: list[str] = []

    def execute_read_only(
        self,
        board_id: str,
        query: str,
        params: dict[str, Any],
        *,
        max_rows: int,
    ) -> dict[str, Any]:
        assert board_id == "board-card5-parity"
        assert params == {"c": "canonical", "w": "working"}
        assert max_rows == 10000
        self.calls.append(query)
        if self.graph_ref is not None and "MATCH (n:Requirement)" in query:
            return {
                "rows": [
                    (
                        "requirement-colliding-id",
                        self.graph_ref,
                        "system:layer1_worker",
                        self.graph_layer,
                        self.maturity_status,
                        self.revocation_reason,
                        self.relevance_score,
                    )
                ]
            }
        return {"rows": []}


@dataclass
class _Registry:
    reader: _SourceReader
    cypher_executor: _CypherExecutor

    def require_board_source_reader(self) -> _SourceReader:
        return self.reader


def _install_registry(
    monkeypatch: pytest.MonkeyPatch,
    *,
    snapshot: BoardSourceSnapshot,
    graph_ref: str | None = None,
    graph_layer: str = "canonical",
    maturity_status: str = "canonical_eligible",
    revocation_reason: str | None = None,
    relevance_score: float = 0.5,
) -> _CypherExecutor:
    executor = _CypherExecutor(
        graph_ref=graph_ref,
        graph_layer=graph_layer,
        maturity_status=maturity_status,
        revocation_reason=revocation_reason,
        relevance_score=relevance_score,
    )
    registry = _Registry(_SourceReader(snapshot), executor)
    monkeypatch.setattr(interfaces, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(reconciler, "get_kg_registry", lambda: registry)
    return executor


@pytest.mark.parametrize(
    "cause",
    ["db_missing", "table_missing", "realm_incomplete"],
)
def test_incomplete_source_snapshot_is_not_reported_as_healthy(
    monkeypatch: pytest.MonkeyPatch,
    cause: str,
) -> None:
    executor = _install_registry(
        monkeypatch,
        snapshot=BoardSourceSnapshot(
            complete=False,
            cause=cause,  # type: ignore[arg-type]
        ),
    )

    with pytest.raises(SourceUnavailableError, match="snapshot is incomplete"):
        detect_board_graph_stale("board-card5-parity")

    assert executor.calls == []


@pytest.mark.parametrize(
    ("graph_ref", "stale_type", "live_type"),
    [
        ("spec:shared-id", "spec", "card"),
        ("card:shared-id", "card", "spec"),
    ],
)
def test_same_id_in_another_artifact_type_cannot_mask_stale_source(
    monkeypatch: pytest.MonkeyPatch,
    graph_ref: str,
    stale_type: str,
    live_type: str,
) -> None:
    _install_registry(
        monkeypatch,
        snapshot=BoardSourceSnapshot(
            rows=(
                {
                    "id": "shared-id",
                    "artifact_type": stale_type,
                    "status": "draft",
                    "content_hash": "stale-content",
                },
                {
                    "id": "shared-id",
                    "artifact_type": live_type,
                    "status": "done",
                    "content_hash": "live-content",
                },
            ),
        ),
        graph_ref=graph_ref,
    )

    stale = detect_board_graph_stale("board-card5-parity")

    assert len(stale) == 1
    assert stale[0]["source_artifact_ref"] == graph_ref
    assert stale[0]["owning_source_id"] == "shared-id"
    assert stale[0]["current_source_status"] == "draft"


@pytest.mark.parametrize("graph_ref", ["card:card-id", "card_relationship_target:card-id"])
def test_task_source_row_matches_card_graph_reference_aliases(
    monkeypatch: pytest.MonkeyPatch,
    graph_ref: str,
) -> None:
    _install_registry(
        monkeypatch,
        snapshot=BoardSourceSnapshot(
            rows=(
                {
                    "id": "card-id",
                    "artifact_type": "task",
                    "status": "done",
                    "content_hash": "task-content",
                    "source_ref": "task:card-id",
                },
            ),
        ),
        graph_ref=graph_ref,
    )

    assert detect_board_graph_stale("board-card5-parity") == []


def test_deleted_working_source_without_tombstone_is_not_reported_healthy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_registry(
        monkeypatch,
        snapshot=BoardSourceSnapshot(),
        graph_ref="spec:deleted-spec",
        graph_layer="working",
        maturity_status="working_immature",
        revocation_reason=None,
        relevance_score=0.75,
    )

    stale = detect_board_graph_stale("board-card5-parity")

    assert len(stale) == 1
    assert stale[0]["reason_code"] == "source_deleted_tombstone_missing"
    assert stale[0]["current_graph_layer"] == "working"


def test_deleted_working_source_with_tombstone_is_converged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_registry(
        monkeypatch,
        snapshot=BoardSourceSnapshot(),
        graph_ref="spec:deleted-spec",
        graph_layer="working",
        maturity_status="working_stale",
        revocation_reason="source_deleted",
        relevance_score=0.0,
    )

    assert detect_board_graph_stale("board-card5-parity") == []
