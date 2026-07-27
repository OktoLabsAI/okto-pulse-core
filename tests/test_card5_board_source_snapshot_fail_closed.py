"""Card 5: incomplete source snapshots cannot masquerade as empty sequences."""

from __future__ import annotations

import pytest

from okto_pulse.core.application.rebuild_ports import (
    BoardSourceSnapshot,
    SourceUnavailableError,
)


@pytest.mark.parametrize(
    "consume",
    [
        len,
        list,
        lambda snapshot: snapshot[0],
        lambda snapshot: snapshot[:],
    ],
    ids=("len", "iterate", "index", "slice"),
)
def test_incomplete_snapshot_sequence_bridge_fails_closed(consume) -> None:
    snapshot = BoardSourceSnapshot(
        rows=(),
        complete=False,
        cause="realm_incomplete",
    )

    with pytest.raises(SourceUnavailableError) as caught:
        consume(snapshot)

    assert caught.value.code == "source_unavailable"
    assert caught.value.cause_type == "realm_incomplete"


def test_complete_snapshot_remains_sequence_compatible() -> None:
    row = {"id": "source-1", "artifact_type": "spec"}
    snapshot = BoardSourceSnapshot(rows=(row,))

    assert len(snapshot) == 1
    assert list(snapshot) == [row]
    assert snapshot[0] is row
