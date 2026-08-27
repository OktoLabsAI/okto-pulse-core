"""Transfer orchestration: one snapshot, bounded batches, certify then finalize."""

from __future__ import annotations

import pytest

from logical_transfer_testing import (
    RecordingSink,
    RecordingSnapshot,
    RecordingSource,
    sample_counts,
    sample_fingerprint,
    sample_nodes,
    sample_relations,
    sample_schema,
)
from okto_pulse.core.kg.logical_transfer import (
    CandidateCertificate,
    CertificationRefusedError,
    LogicalCounts,
    LogicalSchemaError,
    TransferFailedError,
    transfer_logical_graph,
)


def build(**sink_kwargs) -> tuple[RecordingSource, RecordingSink, RecordingSnapshot]:
    snapshot = RecordingSnapshot(
        sample_schema(), sample_nodes(), sample_relations()
    )
    return RecordingSource(snapshot), RecordingSink(**sink_kwargs), snapshot


class TestHappyPath:
    def test_a_clean_transfer_reports_what_it_moved(self) -> None:
        source, sink, _ = build()
        report = transfer_logical_graph(source, sink, batch_size=2)
        assert report.counts == sample_counts()
        assert report.fingerprint == sample_fingerprint()
        assert report.scope == "board"

    def test_exactly_one_snapshot_is_opened_and_closed_once(self) -> None:
        source, sink, snapshot = build()
        transfer_logical_graph(source, sink, batch_size=2)
        assert source.opened == 1
        assert snapshot.closed == 1

    def test_the_candidate_is_finalized_only_after_certification(self) -> None:
        source, sink, _ = build()
        transfer_logical_graph(source, sink, batch_size=2)
        assert sink.calls.index("certify") < sink.calls.index("finalize")
        assert "abort" not in sink.calls

    def test_checkpoint_happens_before_certification(self) -> None:
        source, sink, _ = build()
        transfer_logical_graph(source, sink, batch_size=2)
        assert sink.calls.index("checkpoint") < sink.calls.index("certify")

    def test_every_record_arrives_exactly_once(self) -> None:
        source, sink, _ = build()
        transfer_logical_graph(source, sink, batch_size=2)
        assert tuple(sink.nodes) == sample_nodes()
        assert tuple(sink.relations) == sample_relations()

    def test_identical_parallel_relations_both_arrive(self) -> None:
        source, sink, _ = build()
        transfer_logical_graph(source, sink, batch_size=2)
        assert len(sink.relations) == 3


class TestBatchBound:
    @pytest.mark.parametrize("batch_size", [1, 2, 3, 50])
    def test_no_batch_exceeds_the_bound(self, batch_size: int) -> None:
        source, sink, snapshot = build()
        transfer_logical_graph(source, sink, batch_size=batch_size)
        assert snapshot.batch_sizes
        assert max(snapshot.batch_sizes) <= batch_size
        assert max([*sink.node_batches, *sink.relation_batches]) <= batch_size

    def test_a_source_that_overshoots_the_bound_is_refused(self) -> None:
        snapshot = RecordingSnapshot(
            sample_schema(), sample_nodes(), sample_relations(), overshoot=True
        )
        source, sink = RecordingSource(snapshot), RecordingSink()
        with pytest.raises(TransferFailedError) as caught:
            transfer_logical_graph(source, sink, batch_size=1)
        assert caught.value.phase == "write"
        assert "abort" in sink.calls

    @pytest.mark.parametrize("batch_size", [0, -1, 2.5, None])
    def test_an_invalid_bound_is_refused_before_anything_opens(
        self, batch_size: object
    ) -> None:
        source, sink, _ = build()
        with pytest.raises(LogicalSchemaError):
            transfer_logical_graph(source, sink, batch_size=batch_size)
        assert source.opened == 0
        assert sink.calls == []


class TestFailureMatrix:
    @pytest.mark.parametrize(
        ("step", "phase"),
        [
            ("begin_candidate", "import"),
            ("write_nodes", "import"),
            ("write_relations", "import"),
            ("checkpoint", "checkpoint"),
            ("certify", "reopen"),
            ("finalize", "reopen"),
        ],
    )
    def test_each_failure_is_attributed_to_its_phase(
        self, step: str, phase: str
    ) -> None:
        source, sink, _ = build(fail_on=step)
        with pytest.raises(TransferFailedError) as caught:
            transfer_logical_graph(source, sink, batch_size=2)
        assert caught.value.phase == phase

    @pytest.mark.parametrize(
        "step",
        [
            "begin_candidate",
            "write_nodes",
            "write_relations",
            "checkpoint",
            "certify",
            "finalize",
        ],
    )
    def test_any_failure_abandons_the_candidate(self, step: str) -> None:
        source, sink, _ = build(fail_on=step)
        with pytest.raises(TransferFailedError):
            transfer_logical_graph(source, sink, batch_size=2)
        assert sink.calls[-1] == "abort"

    @pytest.mark.parametrize(
        "step", ["write_nodes", "checkpoint", "certify"]
    )
    def test_a_failed_transfer_never_finalizes(self, step: str) -> None:
        source, sink, _ = build(fail_on=step)
        with pytest.raises(TransferFailedError):
            transfer_logical_graph(source, sink, batch_size=2)
        assert "finalize" not in sink.calls

    def test_a_source_that_fails_mid_stream_is_a_write_failure(self) -> None:
        class ExplodingSnapshot(RecordingSnapshot):
            def iter_nodes(self, *, batch_size: int):
                yield sample_nodes()[:1]
                raise RuntimeError("source went away")

        snapshot = ExplodingSnapshot(
            sample_schema(), sample_nodes(), sample_relations()
        )
        source, sink = RecordingSource(snapshot), RecordingSink()
        with pytest.raises(TransferFailedError) as caught:
            transfer_logical_graph(source, sink, batch_size=1)
        assert caught.value.phase == "write"
        assert "abort" in sink.calls

    def test_a_source_that_cannot_open_is_a_write_failure(self) -> None:
        class DeadSource:
            def open_snapshot(self):
                raise RuntimeError("no snapshot")

        sink = RecordingSink()
        with pytest.raises(TransferFailedError) as caught:
            transfer_logical_graph(DeadSource(), sink, batch_size=2)
        assert caught.value.phase == "write"
        assert sink.calls == []

    def test_a_census_the_snapshot_contradicts_is_a_write_failure(self) -> None:
        snapshot = RecordingSnapshot(
            sample_schema(),
            sample_nodes(),
            sample_relations(),
            counts=LogicalCounts(nodes=99, relations=3, properties=11, vectors=1),
        )
        source, sink = RecordingSource(snapshot), RecordingSink()
        with pytest.raises(TransferFailedError) as caught:
            transfer_logical_graph(source, sink, batch_size=2)
        assert caught.value.phase == "write"

    def test_the_snapshot_is_released_even_when_the_transfer_fails(self) -> None:
        source, sink, snapshot = build(fail_on="checkpoint")
        with pytest.raises(TransferFailedError):
            transfer_logical_graph(source, sink, batch_size=2)
        assert snapshot.closed == 1

    def test_a_process_signal_abandons_the_candidate_and_propagates(self) -> None:
        source, sink, snapshot = build(
            fail_on="write_nodes", failure=KeyboardInterrupt()
        )
        with pytest.raises(KeyboardInterrupt):
            transfer_logical_graph(source, sink, batch_size=2)
        assert "abort" in sink.calls
        assert snapshot.closed == 1

    def test_an_abort_that_itself_fails_does_not_mask_the_real_failure(self) -> None:
        class BrittleSink(RecordingSink):
            def abort(self) -> None:
                self.calls.append("abort")
                raise RuntimeError("abort failed too")

        snapshot = RecordingSnapshot(
            sample_schema(), sample_nodes(), sample_relations()
        )
        sink = BrittleSink(fail_on="checkpoint")
        with pytest.raises(TransferFailedError) as caught:
            transfer_logical_graph(RecordingSource(snapshot), sink, batch_size=2)
        assert caught.value.phase == "checkpoint"


class TestCertification:
    def certificate(self, **overrides) -> CandidateCertificate:
        base = {
            "cold_reopen_completed": True,
            "verify_succeeded": True,
            "schema": sample_schema(),
            "counts": sample_counts(),
            "vector_spaces": ("card_embedding",),
            "fingerprint": sample_fingerprint(),
        }
        base.update(overrides)
        return CandidateCertificate(**base)

    @pytest.mark.parametrize(
        "missing",
        ["schema", "counts", "vector_spaces", "fingerprint"],
    )
    def test_an_omitted_claim_is_refused(self, missing: str) -> None:
        source, sink, _ = build(certificate=self.certificate(**{missing: None}))
        with pytest.raises(CertificationRefusedError) as caught:
            transfer_logical_graph(source, sink, batch_size=2)
        assert missing in str(caught.value)
        assert "finalize" not in sink.calls
        assert "abort" in sink.calls

    def test_a_candidate_never_reopened_from_cold_is_refused(self) -> None:
        source, sink, _ = build(
            certificate=self.certificate(cold_reopen_completed=False)
        )
        with pytest.raises(CertificationRefusedError) as caught:
            transfer_logical_graph(source, sink, batch_size=2)
        assert "cold_reopen_completed" in str(caught.value)

    def test_a_candidate_that_failed_verify_is_refused(self) -> None:
        source, sink, _ = build(certificate=self.certificate(verify_succeeded=False))
        with pytest.raises(CertificationRefusedError) as caught:
            transfer_logical_graph(source, sink, batch_size=2)
        assert "verify_succeeded" in str(caught.value)

    def test_a_divergent_census_is_refused(self) -> None:
        source, sink, _ = build(
            certificate=self.certificate(
                counts=LogicalCounts(nodes=1, relations=3, properties=11, vectors=1)
            )
        )
        with pytest.raises(CertificationRefusedError):
            transfer_logical_graph(source, sink, batch_size=2)

    def test_a_divergent_fingerprint_is_refused(self) -> None:
        source, sink, _ = build(certificate=self.certificate(fingerprint="0" * 64))
        with pytest.raises(CertificationRefusedError) as caught:
            transfer_logical_graph(source, sink, batch_size=2)
        assert "fingerprint" in str(caught.value)

    def test_divergent_vector_spaces_are_refused(self) -> None:
        source, sink, _ = build(
            certificate=self.certificate(vector_spaces=("other_space",))
        )
        with pytest.raises(CertificationRefusedError) as caught:
            transfer_logical_graph(source, sink, batch_size=2)
        assert "vector" in str(caught.value)

    def test_a_divergent_schema_is_refused(self) -> None:
        from okto_pulse.core.kg.logical_transfer import (
            LogicalNodeType,
            LogicalPropertyDef,
            LogicalSchema,
        )

        other = LogicalSchema(
            scope="board",
            node_types=(
                LogicalNodeType(
                    "Card", "id", (LogicalPropertyDef("id", "string", nullable=False),)
                ),
            ),
        )
        source, sink, _ = build(certificate=self.certificate(schema=other))
        with pytest.raises(CertificationRefusedError) as caught:
            transfer_logical_graph(source, sink, batch_size=2)
        assert "schema" in str(caught.value)

    def test_a_sink_that_returns_no_certificate_is_refused(self) -> None:
        class SilentSink(RecordingSink):
            def certify(self):
                self.calls.append("certify")
                return None

        snapshot = RecordingSnapshot(
            sample_schema(), sample_nodes(), sample_relations()
        )
        sink = SilentSink()
        with pytest.raises(CertificationRefusedError):
            transfer_logical_graph(RecordingSource(snapshot), sink, batch_size=2)
        assert "finalize" not in sink.calls
