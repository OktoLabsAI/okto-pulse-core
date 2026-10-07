"""The current cognitive ledger never converts incompatible storage."""

from pathlib import Path

import pytest

from okto_pulse.core.kg.rebuild_audit import CognitiveConsolidationItemStore
from okto_pulse.core.kg.rebuild_generation import generate_kg_generation_id


@pytest.mark.parametrize("raw", ["[]", "null", "{broken", '{"pending_refs": []}'])
def test_incompatible_file_is_not_empty_or_replaced(tmp_path, raw):
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    generation = generate_kg_generation_id()
    key = store._record_key("board", generation)
    path = Path(store.artifact_store.reference(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(raw, encoding="utf-8")
    before = path.read_bytes()
    for operation in (
        lambda: store.list_items("board", generation),
        lambda: store.latest_generation("board"),
        lambda: store.read_completion_snapshot("board", generation),
        lambda: store.materialize_from_marker(
            board_id="board", kg_generation_id=generation, event_ref="new", source_set=[]),
    ):
        with pytest.raises(ValueError):
            operation()
        assert path.read_bytes() == before


def test_adapter_without_atomic_revision_is_refused_before_any_write():
    class IncompatibleAdapter:
        def __getattr__(self, name):
            if name == "replace_json_with_revision":
                raise AttributeError(name)
            raise AssertionError(f"unexpected storage access: {name}")

    store = CognitiveConsolidationItemStore(artifact_store=IncompatibleAdapter())
    with pytest.raises(ValueError, match="cognitive_revisioned_replace_unavailable"):
        store._replace_record_with_overlay_revision(
            key=store._record_key("board", generate_kg_generation_id()),
            transform=lambda current: {},
        )
