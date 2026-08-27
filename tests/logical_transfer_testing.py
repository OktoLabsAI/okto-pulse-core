"""Shared builders for the logical transfer suites.

One small sample graph, defined once, so the codec, the fingerprint, the
validation and the service suites all reason about the same shape.  It contains
every distinction the format promises to preserve: an empty string, an explicit
null, an absent property, a timestamp, a vector, a self-loop and two
byte-identical parallel relations.

Alongside it live reduced fixtures for the two real scopes.  They are literal
rather than imported so the suite pins what the format must represent even if a
schema module moves: eleven Board node types whose property is called
``embedding`` in all eleven cases and belongs to a different space in each, and
the four Global Discovery types with the same collision between ``Entity`` and
``DecisionDigest``.
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

# The real embedding geometry, shared by every Board and Global space.
EMBEDDING_DIMENSION = 384
EMBEDDING_METRIC = "cosine"
EMBEDDING_NORMALIZED = False
EMBEDDING_STORAGE_DTYPE = "float64"

# Board: eleven node types, each with a property literally named "embedding"
# bound to its own space. This is the mapping a name-derived space would lose.
BOARD_SPACE_BY_TYPE: dict[str, str] = {
    "Decision": "decision_embedding_idx",
    "Criterion": "criterion_embedding_idx",
    "Constraint": "constraint_embedding_idx",
    "Assumption": "assumption_embedding_idx",
    "Requirement": "requirement_embedding_idx",
    "Entity": "entity_embedding_idx",
    "APIContract": "apicontract_embedding_idx",
    "TestScenario": "testscenario_embedding_idx",
    "Bug": "bug_embedding_idx",
    "Learning": "learning_embedding_idx",
    "Alternative": "alternative_embedding_idx",
}

# Global Discovery: four types, four spaces. Entity and DecisionDigest both
# call the property "embedding" and land in different spaces.
GLOBAL_SPACE_BY_PROPERTY: dict[str, tuple[str, str]] = {
    "Board": ("summary_embedding", "board_summary_idx"),
    "Topic": ("centroid_embedding", "topic_centroid_idx"),
    "Entity": ("embedding", "entity_embedding_idx"),
    "DecisionDigest": ("embedding", "digest_embedding_idx"),
}

GLOBAL_LAYOUTS: tuple[tuple[str, str, str], ...] = (
    ("HAS_TOPIC", "Board", "Topic"),
    ("MENTIONS_ENTITY", "Board", "Entity"),
    ("CONTAINS_DECISION", "Board", "DecisionDigest"),
    ("TOPIC_RELATES_TO", "Topic", "Topic"),
    ("ENTITY_RELATES_TO", "Entity", "Entity"),
    ("DECISION_MENTIONS_ENTITY", "DecisionDigest", "Entity"),
    ("DECISION_DERIVES_FROM", "DecisionDigest", "DecisionDigest"),
)

GLOBAL_WEIGHTED = {("TOPIC_RELATES_TO", "Topic", "Topic"),
                   ("ENTITY_RELATES_TO", "Entity", "Entity")}


def embedding_space(name: str, dimension: int = EMBEDDING_DIMENSION):
    return LogicalVectorSpace(
        name=name,
        storage_dtype=EMBEDDING_STORAGE_DTYPE,
        dimension=dimension,
        metric=EMBEDDING_METRIC,
        normalized=EMBEDDING_NORMALIZED,
    )


def board_schema() -> LogicalSchema:
    """Eleven node types, eleven spaces, and one ambiguous layout name."""

    node_types = tuple(
        LogicalNodeType(
            name=node_type,
            key="id",
            properties=(
                LogicalPropertyDef("id", "string", nullable=False),
                LogicalPropertyDef("embedding", "vector", vector_space=space),
            ),
        )
        for node_type, space in BOARD_SPACE_BY_TYPE.items()
    )
    # "supersedes" exists twice, between different endpoint types. Keying
    # layouts by name alone would keep only one of them.
    layouts = (
        LogicalRelationLayout("supersedes", "Decision", "Decision"),
        LogicalRelationLayout("supersedes", "Alternative", "Alternative"),
    )
    spaces = tuple(embedding_space(name) for name in BOARD_SPACE_BY_TYPE.values())
    return LogicalSchema(
        scope="board",
        node_types=node_types,
        relation_layouts=layouts,
        vector_spaces=spaces,
    )


def global_schema() -> LogicalSchema:
    """Four node types, four spaces, seven directed layouts."""

    node_types = tuple(
        LogicalNodeType(
            name=node_type,
            key="id",
            properties=(
                LogicalPropertyDef("id", "string", nullable=False),
                LogicalPropertyDef(prop, "vector", vector_space=space),
            ),
        )
        for node_type, (prop, space) in GLOBAL_SPACE_BY_PROPERTY.items()
    )
    layouts = tuple(
        LogicalRelationLayout(
            name=name,
            source_type=source,
            target_type=target,
            properties=(
                (LogicalPropertyDef("weight", "float64"),)
                if (name, source, target) in GLOBAL_WEIGHTED
                else ()
            ),
        )
        for name, source, target in GLOBAL_LAYOUTS
    )
    spaces = tuple(
        embedding_space(space) for _, space in GLOBAL_SPACE_BY_PROPERTY.values()
    )
    return LogicalSchema(
        scope="global_discovery",
        node_types=node_types,
        relation_layouts=layouts,
        vector_spaces=spaces,
    )


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
                    LogicalPropertyDef(
                        "embedding", "vector", vector_space="card_embedding_idx"
                    ),
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
        vector_spaces=(embedding_space("card_embedding_idx", dimension=3),),
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
                "embedding": LogicalVector(
                    "card_embedding_idx",
                    EMBEDDING_STORAGE_DTYPE,
                    (1.0, 2.5, -0.25),
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
        LogicalRelation("blocks", "Card", "Card", "c1", "c2", {"note": "x"}),
        # byte-identical parallel occurrence: both must survive
        LogicalRelation("blocks", "Card", "Card", "c1", "c2", {"note": "x"}),
        # self-loop
        LogicalRelation("blocks", "Card", "Card", "c1", "c1", {}),
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
        schema = self.schema
        return CandidateCertificate(
            cold_reopen_completed=True,
            verify_succeeded=True,
            schema=schema,
            counts=count_graph(self.nodes, self.relations),
            vector_spaces=tuple(
                space.name for space in (schema.vector_spaces if schema else ())
            ),
            fingerprint=(
                fingerprint_graph(schema, self.nodes, self.relations)
                if schema is not None
                else ""
            ),
        )

    def finalize(self) -> None:
        self._step("finalize")

    def abort(self) -> None:
        self.calls.append("abort")


__all__ = [
    "BOARD_SPACE_BY_TYPE",
    "EMBEDDING_DIMENSION",
    "EMBEDDING_METRIC",
    "EMBEDDING_NORMALIZED",
    "EMBEDDING_STORAGE_DTYPE",
    "GLOBAL_LAYOUTS",
    "GLOBAL_SPACE_BY_PROPERTY",
    "SAMPLE_MICROS",
    "RecordingSink",
    "RecordingSnapshot",
    "RecordingSource",
    "board_schema",
    "embedding_space",
    "global_schema",
    "sample_counts",
    "sample_fingerprint",
    "sample_nodes",
    "sample_relations",
    "sample_schema",
]
