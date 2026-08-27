"""Neutral orchestration for one logical transfer.

The service moves a graph from exactly one snapshot into exactly one candidate,
in bounded batches, and refuses to call the result a success until the
destination has certified itself from a cold reopen.

Three rules shape the control flow, and each exists because of a way a transfer
can look finished while being wrong:

fail closed on the candidate
    Any failure abandons the candidate.  A partially imported generation that
    survived a failure is the one artifact nobody can safely reuse, and this
    milestone deliberately has no resume path -- an interrupted transfer is
    repeated, not continued.

certify before finalize
    ``finalize`` is called only after a typed certificate has been checked
    field by field.  A sink that finalized first and certified afterwards would
    be reporting on something already accepted.

absence is refusal
    A certificate that omits a claim is refused exactly like one that
    contradicts it.  Otherwise a sink could earn success by staying quiet.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import Any, Final, TypeVar

from .errors import (
    CertificationRefusedError,
    LogicalSchemaError,
    TransferFailedError,
    TransferPhase,
)
from .fingerprint import LogicalFingerprintAccumulator, schema_digest
from .model import LogicalCounts, LogicalSchema, LogicalScope
from .ports import (
    CandidateCertificate,
    LogicalCandidateSink,
    LogicalSnapshot,
    LogicalSnapshotSource,
)


DEFAULT_BATCH_SIZE: Final[int] = 500

_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class TransferReport:
    """What a completed transfer proved, in the destination's own terms."""

    scope: LogicalScope
    counts: LogicalCounts
    fingerprint: str
    schema_digest: str
    node_batches: int = 0
    relation_batches: int = 0


def transfer_logical_graph(
    source: LogicalSnapshotSource,
    sink: LogicalCandidateSink,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> TransferReport:
    """Move one logical graph from ``source`` into ``sink`` and certify it."""

    if type(batch_size) is not int or batch_size < 1:
        raise LogicalSchemaError(
            "batch_size must be a positive int", detail=repr(batch_size)
        )

    # Opening the snapshot is itself fallible and belongs inside the phase
    # classification: a source that cannot open has failed the write phase, not
    # produced an empty graph.
    snapshot = _guard("write", source.open_snapshot)
    started = False
    try:
        schema = _guard("write", snapshot.schema)
        declared = _guard("write", snapshot.counts)
        accumulator = LogicalFingerprintAccumulator.for_schema(schema)

        # Marked before the call, not after: a begin that raises part way may
        # already have allocated a candidate, and the only safe assumption is
        # that one exists to abandon. Aborting a candidate that was never begun
        # costs a swallowed no-op; skipping the abort orphans a generation.
        started = True
        _guard("import", sink.begin_candidate, schema)

        node_batches = _move(
            snapshot.iter_nodes,
            sink.write_nodes,
            batch_size,
            accumulator.add_node,
            "nodes",
        )
        relation_batches = _move(
            snapshot.iter_relations,
            sink.write_relations,
            batch_size,
            accumulator.add_relation,
            "relations",
        )

        observed = accumulator.counts()
        if observed != declared:
            raise TransferFailedError(
                "the snapshot produced a census other than the one it declared",
                phase="write",
                detail=(
                    f"declared={declared.as_mapping()} "
                    f"observed={observed.as_mapping()}"
                ),
            )
        fingerprint = accumulator.digest()

        _guard("checkpoint", sink.checkpoint)

        certificate = _guard("reopen", sink.certify)
        # Raised outside _guard so the refusal keeps its own type: a certificate
        # that does not hold up is not the same event as a sink that crashed.
        _require_certified(certificate, schema, observed, fingerprint)
        _guard("reopen", sink.finalize)
    except BaseException:
        # Process-control signals abandon the candidate too, then propagate
        # unchanged: a store does not swallow those because a batch landed.
        if started:
            _abandon(sink)
        raise
    finally:
        _release(snapshot)

    return TransferReport(
        scope=schema.scope,
        counts=observed,
        fingerprint=fingerprint,
        schema_digest=schema_digest(schema),
        node_batches=node_batches,
        relation_batches=relation_batches,
    )


def _move(
    produce: Callable[..., Iterator[Sequence[Any]]],
    consume: Callable[[Sequence[Any]], None],
    batch_size: int,
    account: Callable[[Any], None],
    what: str,
) -> int:
    """Move one section in bounded batches, counting each record exactly once."""

    batches = 0
    iterator = _guard("write", produce, batch_size=batch_size)
    while True:
        try:
            batch = next(iterator)
        except StopIteration:
            break
        except Exception as failure:
            raise TransferFailedError(
                f"the snapshot failed while producing {what}",
                phase="write",
                detail=str(failure),
            ) from failure
        if len(batch) > batch_size:
            # The bound is the contract, not a hint: a source that overshoots it
            # has already put an unbounded amount of the graph in memory.
            raise TransferFailedError(
                f"the snapshot produced a {what} batch larger than the bound",
                phase="write",
                detail=f"limit={batch_size} got={len(batch)}",
            )
        if not batch:
            continue
        for record in batch:
            account(record)
        _guard("import", consume, batch)
        batches += 1
    return batches


def _guard(
    phase: TransferPhase, call: Callable[..., _T], *args: Any, **kwargs: Any
) -> _T:
    """Run one fallible step and attribute any ordinary failure to ``phase``."""

    try:
        return call(*args, **kwargs)
    except Exception as failure:
        raise TransferFailedError(
            f"{getattr(call, '__name__', 'step')} failed",
            phase=phase,
            detail=str(failure) or type(failure).__name__,
        ) from failure


def _require_certified(
    certificate: CandidateCertificate,
    schema: LogicalSchema,
    counts: LogicalCounts,
    fingerprint: str,
) -> None:
    """Refuse the transfer unless every claim is present and matches."""

    if not isinstance(certificate, CandidateCertificate):
        raise CertificationRefusedError(
            "the sink did not return a certificate",
            detail=type(certificate).__name__,
        )
    if not certificate.cold_reopen_completed:
        raise CertificationRefusedError(
            "the candidate was not re-read from cold",
            detail="cold_reopen_completed",
        )
    if not certificate.verify_succeeded:
        raise CertificationRefusedError(
            "the candidate did not pass its own verification",
            detail="verify_succeeded",
        )
    if certificate.schema is None:
        raise CertificationRefusedError(
            "the certificate reports no schema", detail="schema"
        )
    if certificate.schema != schema:
        raise CertificationRefusedError(
            "the candidate schema differs from the snapshot schema",
            detail="schema",
        )
    if certificate.counts is None:
        raise CertificationRefusedError(
            "the certificate reports no counts", detail="counts"
        )
    if certificate.counts != counts:
        raise CertificationRefusedError(
            "the candidate census differs from the snapshot census",
            detail=(
                f"expected={counts.as_mapping()} "
                f"got={certificate.counts.as_mapping()}"
            ),
        )
    if certificate.vector_spaces is None:
        raise CertificationRefusedError(
            "the certificate reports no vector spaces", detail="vector_spaces"
        )
    expected_spaces = tuple(sorted(space.name for space in schema.vector_spaces))
    if tuple(sorted(certificate.vector_spaces)) != expected_spaces:
        raise CertificationRefusedError(
            "the candidate vector spaces differ from the snapshot schema",
            detail=f"expected={expected_spaces} got={certificate.vector_spaces}",
        )
    if not certificate.fingerprint:
        raise CertificationRefusedError(
            "the certificate reports no fingerprint", detail="fingerprint"
        )
    if certificate.fingerprint != fingerprint:
        raise CertificationRefusedError(
            "the candidate fingerprint differs from the snapshot fingerprint",
            detail="fingerprint",
        )


def _abandon(sink: LogicalCandidateSink) -> None:
    """Abort the candidate without letting cleanup mask the real failure."""

    try:
        sink.abort()
    except Exception:  # noqa: S110 - the original failure is the one to report
        # An abort that fails leaves a candidate nobody bound; reporting it here
        # would replace the reason the transfer stopped with a symptom of it.
        pass


def _release(snapshot: LogicalSnapshot) -> None:
    try:
        snapshot.close()
    except Exception:  # noqa: S110 - see _abandon
        pass


__all__ = [
    "DEFAULT_BATCH_SIZE",
    "TransferReport",
    "transfer_logical_graph",
]
