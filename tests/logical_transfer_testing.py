"""Shared builders for the logical transfer suites.

One sample graph, defined once, so the codec, the fingerprint and the service
suites all reason about the same shape.  It deliberately contains every
distinction the format promises to preserve: an empty string, an explicit null,
an absent property, a timestamp, a vector, a self-loop and two byte-identical
parallel relations.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence

from okto_pulse.core.kg.logical_transfer import (
    LOGICAL_NULL,
    CandidateCertificate,
    LogicalCounts,
    LogicalNode,
    LogicalNodeType,
    LogicalPropertyDef,
    LogicalRelation,
    LogicalRelationLayout,
    LogicalSchema,
    LogicalTimestamp,
    LogicalVector,
    LogicalVectorSpace,
    count_graph,
    fingerprint_graph,
)


SAMPLE_MICROS = 1717171717000000


def sample_schema() -> LogicalSchema:
    return LogicalSchema(
        scope="board",
        node_types=(
            LogicalNodeType(
                name="Card",
                key="id",
                properties=(
                    LogicalPropertyDef("id", "string", nullable=False),
                    LogicalPropertyDef("title", "string"),
                    LogicalPropertyDef("rank", "int64"),
                    LogicalPropertyDef("score", "float64"),
                    LogicalPropertyDef("done", "bool"),
                    LogicalPropertyDef("created_at", "timestamp_us"),
                    LogicalPropertyDef("card_embedding", "vector"),
                ),
            ),
        ),
        relation_layouts=(
            LogicalRelationLayout(
                name="blocks",
                source_type="Card",
                target_type="Card",
                properties=(LogicalPropertyDef("note", "string"),),
            ),
        ),
        vector_spaces=(LogicalVectorSpace("card_embedding", "float32", 3),),
    )


def sample_nodes() -> tuple[LogicalNode, ...]:
    return (
        LogicalNode(
            type_name="Card",
            key="c1",
            properties={
                "id": "c1",
                # empty string, distinct from both null and absent
                "title": "",
                "rank": 7,
                "score": 0.1,
                "done": True,
                "created_at": LogicalTimestamp(SAMPLE_MICROS),
                "card_embedding": LogicalVector(
                    "card_embedding", "float32", (1.0, 2.5, -0.25)
                ),
            },
        ),
        LogicalNode(
            type_name="Card",
            key="c2",
            # 'title' is explicitly null; 'rank' is absent entirely
            properties={"id": "c2", "title": LOGICAL_NULL},
        ),
    )


def sample_relations() -> tuple[LogicalRelation, ...]:
    return (
        LogicalRelation("blocks", "c1", "c2", {"note": "x"}),
        # byte-identical parallel occurrence: both must survive
        LogicalRelation("blocks", "c1", "c2", {"note": "x"}),
        # self-loop
        LogicalRelation("blocks", "c1", "c1", {}),
    )


def sample_counts() -> LogicalCounts:
    return count_graph(sample_nodes(), sample_relations())


def sample_fingerprint() -> str:
    return fingerprint_graph(sample_schema(), sample_nodes(), sample_relations())


class RecordingSnapshot:
    """A snapshot that answers from fixed tuples and records how it was used."""

    def __init__(
        self,
        schema: LogicalSchema,
        nodes: Sequence[LogicalNode],
        relations: Sequence[LogicalRelation],
        *,
        counts: LogicalCounts | None = None,
        overshoot: bool = False,
    ) -> None:
        self._schema = schema
        self._nodes = tuple(nodes)
        self._relations = tuple(relations)
        self._counts = counts if counts is not None else count_graph(nodes, relations)
        self._overshoot = overshoot
        self.closed = 0
        self.batch_sizes: list[int] = []

    def schema(self) -> LogicalSchema:
        return self._schema

    def counts(self) -> LogicalCounts:
        return self._counts

    def iter_nodes(self, *, batch_size: int) -> Iterator[Sequence[LogicalNode]]:
        return self._batched(self._nodes, batch_size)

    def iter_relations(self, *, batch_size: int) -> Iterator[Sequence[LogicalRelation]]:
        return self._batched(self._relations, batch_size)

    def _batched(self, items: tuple, batch_size: int) -> Iterator[Sequence]:
        step = len(items) if self._overshoot else batch_size
        for start in range(0, len(items), max(step, 1)):
            batch = items[start : start + step]
            self.batch_sizes.append(len(batch))
            yield batch

    def close(self) -> None:
        self.closed += 1


class RecordingSource:
    """A source that hands out one snapshot and counts how often it was asked."""

    def __init__(self, snapshot: RecordingSnapshot) -> None:
        self._snapshot = snapshot
        self.opened = 0

    def open_snapshot(self) -> RecordingSnapshot:
        self.opened += 1
        return self._snapshot


class RecordingSink:
    """A sink that accepts everything and certifies truthfully by default.

    Any step can be told to fail, and the certificate can be overridden, so a
    test can put a failure in exactly one phase and assert what the service did
    about it.
    """

    def __init__(
        self,
        *,
        certificate: CandidateCertificate | None = None,
        fail_on: str | None = None,
        failure: BaseException | None = None,
    ) -> None:
        self.certificate = certificate
        self.fail_on = fail_on
        self.failure = failure or RuntimeError("injected backend failure")
        self.calls: list[str] = []
        self.nodes: list[LogicalNode] = []
        self.relations: list[LogicalRelation] = []
        self.node_batches: list[int] = []
        self.relation_batches: list[int] = []
        self.schema: LogicalSchema | None = None

    def _step(self, name: str) -> None:
        self.calls.append(name)
        if self.fail_on == name:
            raise self.failure

    def begin_candidate(self, schema: LogicalSchema) -> None:
        self.schema = schema
        self._step("begin_candidate")

    def write_nodes(self, nodes: Sequence[LogicalNode]) -> None:
        self._step("write_nodes")
        self.node_batches.append(len(nodes))
        self.nodes.extend(nodes)

    def write_relations(self, relations: Sequence[LogicalRelation]) -> None:
        self._step("write_relations")
        self.relation_batches.append(len(relations))
        self.relations.extend(relations)

    def checkpoint(self) -> None:
        self._step("checkpoint")

    def certify(self) -> CandidateCertificate:
        self._step("certify")
        if self.certificate is not None:
            return self.certificate
        return CandidateCertificate(
            cold_reopen_completed=True,
            verify_succeeded=True,
            schema=self.schema,
            counts=count_graph(self.nodes, self.relations),
            vector_spaces=tuple(
                space.name for space in (self.schema.vector_spaces if self.schema else ())
            ),
            fingerprint=(
                fingerprint_graph(self.schema, self.nodes, self.relations)
                if self.schema is not None
                else ""
            ),
        )

    def finalize(self) -> None:
        self._step("finalize")

    def abort(self) -> None:
        self.calls.append("abort")


__all__ = [
    "SAMPLE_MICROS",
    "RecordingSink",
    "RecordingSnapshot",
    "RecordingSource",
    "sample_counts",
    "sample_fingerprint",
    "sample_nodes",
    "sample_relations",
    "sample_schema",
]
