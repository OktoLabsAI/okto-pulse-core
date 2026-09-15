"""Neutral ports for a logical transfer: one snapshot source, one candidate sink.

Both names are deliberately free of backend vocabulary.  Core does not know
whether a snapshot is an MVCC read transaction or a frozen file, and does not
know whether a candidate is a database generation, a directory or a namespace.
It knows only that a snapshot answers questions about one fixed view of a
graph, and that a candidate accepts records and can be asked to certify itself.

What Core deliberately does NOT own, and what an adapter must therefore
guarantee on its side of these Protocols:

* the candidate handed to :meth:`LogicalCandidateSink.begin_candidate` is new,
  empty and unbound -- nothing reads from it while a transfer fills it;
* abandoning a candidate leaves the previously live generation exactly as it
  was, so a failed transfer costs nothing but the candidate;
* binding, activating or retiring a generation is not requested here and never
  happens as a side effect of a transfer.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from .model import LogicalCounts, LogicalNode, LogicalRelation, LogicalSchema


@dataclass(frozen=True, slots=True)
class CandidateCertificate:
    """What a sink claims about a candidate it has just re-read from cold.

    Every field is optional in the type and mandatory in effect: a sink that
    omits one has not proved it, and an unproved claim is refused exactly like a
    disproved one.  Reporting them separately is what makes the certificate
    observable instead of a single opaque boolean.
    """

    cold_reopen_completed: bool = False
    verify_succeeded: bool = False
    schema: LogicalSchema | None = None
    counts: LogicalCounts | None = None
    vector_spaces: tuple[str, ...] | None = None
    fingerprint: str | None = None


@runtime_checkable
class LogicalSnapshot(Protocol):
    """One fixed view of a source graph.

    Every method answers from the same instant.  A source that let its answers
    drift between the schema call and the last batch would produce an artifact
    that never existed.
    """

    def schema(self) -> LogicalSchema:
        """Return the logical schema of this snapshot."""

    def counts(self) -> LogicalCounts:
        """Return the census this snapshot will produce."""

    def iter_nodes(self, *, batch_size: int) -> Iterator[Sequence[LogicalNode]]:
        """Yield nodes in batches of at most ``batch_size``."""

    def iter_relations(
        self, *, batch_size: int
    ) -> Iterator[Sequence[LogicalRelation]]:
        """Yield relations in batches of at most ``batch_size``."""

    def close(self) -> None:
        """Release the snapshot; called exactly once, success or failure."""


@runtime_checkable
class LogicalSnapshotSource(Protocol):
    """Opens the single snapshot a transfer reads from."""

    def open_snapshot(self) -> LogicalSnapshot:
        """Open one snapshot. A transfer calls this exactly once."""


@runtime_checkable
class LogicalCandidateSink(Protocol):
    """Accepts a logical graph into a new, empty, unbound candidate."""

    def begin_candidate(self, schema: LogicalSchema) -> None:
        """Start a candidate for ``schema``; it must be new, empty and unbound."""

    def write_nodes(self, nodes: Sequence[LogicalNode]) -> None:
        """Import one bounded batch of nodes."""

    def write_relations(self, relations: Sequence[LogicalRelation]) -> None:
        """Import one bounded batch of relations."""

    def checkpoint(self) -> None:
        """Make everything imported so far durable."""

    def certify(self) -> CandidateCertificate:
        """Re-read the candidate from cold and report what it actually holds.

        This must be a genuine cold reopen.  A certificate answered from the
        writer's own warm state would prove that the process still remembers
        what it wrote, which is not the question.
        """

    def finalize(self) -> None:
        """Accept the candidate. Called only after certification succeeded."""

    def abort(self) -> None:
        """Abandon the candidate, leaving the previous generation untouched."""


__all__ = [
    "CandidateCertificate",
    "LogicalCandidateSink",
    "LogicalSnapshot",
    "LogicalSnapshotSource",
]
