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
    TRANSFER_PHASES,
    CandidateCertificate,
    CertificationRefusedError,
    LogicalCounts,
    LogicalNode,
    LogicalSchemaError,
    PhasedTransferError,
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
            "vector_spaces": ("card_embedding_idx",),
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


class TestEveryFailureIsClassifiable:
    """The matrix is finite, so every ending carries one of its four phases."""

    def test_a_refused_certificate_is_a_reopen_failure(self) -> None:
        source, sink, _ = build(
            certificate=CandidateCertificate(
                cold_reopen_completed=True,
                verify_succeeded=True,
                schema=sample_schema(),
                counts=sample_counts(),
                vector_spaces=("card_embedding_idx",),
                fingerprint="0" * 64,
            )
        )
        with pytest.raises(CertificationRefusedError) as caught:
            transfer_logical_graph(source, sink, batch_size=2)
        assert caught.value.phase == "reopen"
        assert isinstance(caught.value, PhasedTransferError)

    @pytest.mark.parametrize(
        "step", ["begin_candidate", "write_nodes", "checkpoint", "certify"]
    )
    def test_a_step_failure_carries_a_phase_from_the_matrix(self, step: str) -> None:
        source, sink, _ = build(fail_on=step)
        with pytest.raises(PhasedTransferError) as caught:
            transfer_logical_graph(source, sink, batch_size=2)
        assert caught.value.phase in TRANSFER_PHASES

    def test_a_record_that_violates_the_schema_is_a_write_failure(self) -> None:
        # Accounting for a record is source-side work, so a schema violation
        # discovered there belongs to write rather than leaking unclassified.
        bad = LogicalNode("Ghost", "g1", {"id": "g1"})
        snapshot = RecordingSnapshot(
            sample_schema(), (*sample_nodes(), bad), sample_relations()
        )
        source, sink = RecordingSource(snapshot), RecordingSink()
        with pytest.raises(PhasedTransferError) as caught:
            transfer_logical_graph(source, sink, batch_size=5)
        assert caught.value.phase == "write"
        assert "abort" in sink.calls

    def test_a_batch_with_no_length_is_a_write_failure(self) -> None:
        class LengthlessSnapshot(RecordingSnapshot):
            def iter_nodes(self, *, batch_size: int):
                yield object()  # no __len__

        snapshot = LengthlessSnapshot(
            sample_schema(), sample_nodes(), sample_relations()
        )
        source, sink = RecordingSource(snapshot), RecordingSink()
        with pytest.raises(PhasedTransferError) as caught:
            transfer_logical_graph(source, sink, batch_size=2)
        assert caught.value.phase == "write"


class TestCertificateClaimsMustBeRealBooleans:
    """A truthy placeholder is not a claim."""

    def certificate(self, **overrides) -> CandidateCertificate:
        base = {
            "cold_reopen_completed": True,
            "verify_succeeded": True,
            "schema": sample_schema(),
            "counts": sample_counts(),
            "vector_spaces": ("card_embedding_idx",),
            "fingerprint": sample_fingerprint(),
        }
        base.update(overrides)
        return CandidateCertificate(**base)

    @pytest.mark.parametrize("field", ["cold_reopen_completed", "verify_succeeded"])
    @pytest.mark.parametrize("truthy", [1, "yes", [1], 2.0])
    def test_a_truthy_non_bool_does_not_certify(
        self, field: str, truthy: object
    ) -> None:
        source, sink, _ = build(certificate=self.certificate(**{field: truthy}))
        with pytest.raises(CertificationRefusedError) as caught:
            transfer_logical_graph(source, sink, batch_size=2)
        assert field in str(caught.value)
        assert "finalize" not in sink.calls


class TestAMalformedCertificateStaysInTheMatrix:
    """A bad certificate must refuse, never leak a builtin from deep inside."""

    def certificate(self, **overrides) -> CandidateCertificate:
        base = {
            "cold_reopen_completed": True,
            "verify_succeeded": True,
            "schema": sample_schema(),
            "counts": sample_counts(),
            "vector_spaces": ("card_embedding_idx",),
            "fingerprint": sample_fingerprint(),
        }
        base.update(overrides)
        return CandidateCertificate(**base)

    @pytest.mark.parametrize("counts", [object(), "3", 3, [1, 2], {"nodes": 2}])
    def test_counts_that_are_not_a_census_are_refused(self, counts: object) -> None:
        # Reached as_mapping() before, and left as an AttributeError.
        source, sink, _ = build(certificate=self.certificate(counts=counts))
        with pytest.raises(CertificationRefusedError) as caught:
            transfer_logical_graph(source, sink, batch_size=2)
        assert caught.value.phase == "reopen"

    @pytest.mark.parametrize(
        "spaces", [("card_embedding_idx", None), ("x", 1), (1, 2), ("", "y")]
    )
    def test_vector_spaces_with_non_names_are_refused(self, spaces: tuple) -> None:
        # Reached sorted() before, and left as a TypeError.
        source, sink, _ = build(certificate=self.certificate(vector_spaces=spaces))
        with pytest.raises(CertificationRefusedError) as caught:
            transfer_logical_graph(source, sink, batch_size=2)
        assert caught.value.phase == "reopen"

    @pytest.mark.parametrize("spaces", [object(), 5, "card_embedding_idx"])
    def test_vector_spaces_that_are_not_a_collection_are_refused(
        self, spaces: object
    ) -> None:
        source, sink, _ = build(certificate=self.certificate(vector_spaces=spaces))
        with pytest.raises(CertificationRefusedError) as caught:
            transfer_logical_graph(source, sink, batch_size=2)
        assert caught.value.phase == "reopen"

    @pytest.mark.parametrize("schema", [object(), "board", 7, {"scope": "board"}])
    def test_a_schema_that_is_not_a_schema_is_refused(self, schema: object) -> None:
        source, sink, _ = build(certificate=self.certificate(schema=schema))
        with pytest.raises(CertificationRefusedError) as caught:
            transfer_logical_graph(source, sink, batch_size=2)
        assert caught.value.phase == "reopen"

    @pytest.mark.parametrize("fingerprint", [object(), 5, ["hex"], b"hex"])
    def test_a_fingerprint_that_is_not_a_digest_is_refused(
        self, fingerprint: object
    ) -> None:
        source, sink, _ = build(certificate=self.certificate(fingerprint=fingerprint))
        with pytest.raises(CertificationRefusedError) as caught:
            transfer_logical_graph(source, sink, batch_size=2)
        assert caught.value.phase == "reopen"

    def test_every_malformed_certificate_abandons_the_candidate(self) -> None:
        source, sink, _ = build(certificate=self.certificate(counts=object()))
        with pytest.raises(CertificationRefusedError):
            transfer_logical_graph(source, sink, batch_size=2)
        assert "abort" in sink.calls
        assert "finalize" not in sink.calls


class TestACertificateCannotRunCodeInsideTheService:
    """Materializing an arbitrary iterable is a risk the matrix cannot describe."""

    def certificate(self, spaces) -> CandidateCertificate:
        return CandidateCertificate(
            cold_reopen_completed=True,
            verify_succeeded=True,
            schema=sample_schema(),
            counts=sample_counts(),
            vector_spaces=spaces,
            fingerprint=sample_fingerprint(),
        )

    def test_a_generator_of_names_is_refused_not_consumed(self) -> None:
        consumed = []

        def spaces():
            consumed.append(1)
            yield "card_embedding_idx"

        source, sink, _ = build(certificate=self.certificate(spaces()))
        with pytest.raises(CertificationRefusedError) as caught:
            transfer_logical_graph(source, sink, batch_size=2)
        assert caught.value.phase == "reopen"
        assert consumed == []

    def test_an_exploding_iterable_is_refused_before_it_runs(self) -> None:
        class Hostile:
            def __iter__(self):
                raise RuntimeError("boom")

        source, sink, _ = build(certificate=self.certificate(Hostile()))
        with pytest.raises(CertificationRefusedError):
            transfer_logical_graph(source, sink, batch_size=2)

    def test_a_list_is_refused_because_the_dto_declares_a_tuple(self) -> None:
        source, sink, _ = build(certificate=self.certificate(["card_embedding_idx"]))
        with pytest.raises(CertificationRefusedError):
            transfer_logical_graph(source, sink, batch_size=2)

    def test_a_fingerprint_that_explodes_on_bool_is_refused(self) -> None:
        class Hostile:
            def __bool__(self):
                raise RuntimeError("boom")

        certificate = CandidateCertificate(
            cold_reopen_completed=True,
            verify_succeeded=True,
            schema=sample_schema(),
            counts=sample_counts(),
            vector_spaces=("card_embedding_idx",),
            fingerprint=Hostile(),
        )
        source, sink, _ = build(certificate=certificate)
        with pytest.raises(CertificationRefusedError) as caught:
            transfer_logical_graph(source, sink, batch_size=2)
        assert caught.value.phase == "reopen"
