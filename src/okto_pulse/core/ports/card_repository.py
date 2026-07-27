"""Card repository port — batch column resequencing (spec 8b33f9a8).

``CardRepositoryPort.resequence_columns`` is the architecture's contract for
card placement (matriz v13, item 5; refinement v17, item 7): a transactional
batch of :class:`ColumnResequenceOp` that atomically pre-validates EVERYTHING
before ANY write and rewrites every affected ``(board_id, status)`` column to
the density invariant — active cards ``0..n-1``, archived cards ``n..m``.

:class:`CoreCardResequencer` is the Core-owned default implementation built
directly on the :mod:`application_persistence` port (no service imports), so
editions may register a specialized adapter without touching the domain flow.
``CardService.move_card`` / ``ArchiveService`` call through the registry.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from okto_pulse.core.domain.enums import CardStatus
from okto_pulse.core.ports.application_persistence import (
    ApplicationFilter,
    ApplicationQuery,
    ApplicationRecord,
    get_application_persistence_port,
)
from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)

__all__ = [
    "CardRepositoryPort",
    "ColumnResequenceOp",
    "CoreCardResequencer",
    "get_card_repository_port",
    "register_card_repository_port",
    "reset_card_repository_port_for_tests",
]


@dataclass(frozen=True, slots=True)
class ColumnResequenceOp:
    """One card placement inside a batch column resequence (matriz v13, item 5).

    ``from``/``to`` model the FULL card lane state — ``(status, archived)`` —
    so archive/restore flows are ops too, not out-of-band renormalizations.
    Placement selectors are mutually exclusive; at most one of:

    - ``target_index`` — index into the target column's ACTIVE range (the
      normalized meaning of the legacy ``position >= 0``);
    - ``before_id`` / ``after_id`` — relative to an anchor card that must be
      an ACTIVE, non-moved card of the target column;
    - ``placement`` — ``"start"`` | ``"end"`` of the active range.

    No selector means "end of the active range" (legacy ``-1``/``None``).
    Ops never carry negative indices — the REST boundary rejects
    ``position < -1`` with 422 (authorized narrowing, QA 6afdc547). Archive
    ops (``to_archived=True``) accept NO selector: the card is relocated to
    the archived range preserving batch (tree-preorder) relative order.
    """

    card_id: str
    from_status: CardStatus
    to_status: CardStatus
    target_index: int | None = None
    from_archived: bool = field(default=False, kw_only=True)
    to_archived: bool = field(default=False, kw_only=True)
    before_id: str | None = field(default=None, kw_only=True)
    after_id: str | None = field(default=None, kw_only=True)
    placement: str | None = field(default=None, kw_only=True)


class CardRepositoryPort(Protocol):
    async def resequence_columns(
        self,
        context: Any,
        board_id: str,
        ops: list[ColumnResequenceOp],
        *,
        extra_columns: tuple[CardStatus, ...] = (),
        records: dict[str, ApplicationRecord] | None = None,
    ) -> int: ...


class CoreCardResequencer:
    """Default Core implementation of :class:`CardRepositoryPort`.

    Batch contract (refinement v17 item 7 + matriz v13 item 5): ALL ops are
    pre-validated before ANY write — a duplicate ``card_id``, more than one
    placement selector, a negative ``target_index``, an invalid ``placement``,
    an empty anchor id, a selector on an archive op, a missing card, a card
    from another board, a current ``(status, archived)`` different from the
    op's ``from`` state, or an invalid anchor (missing, other board, other
    column, archived, self, or itself moved in this batch) aborts the whole
    batch with ``ValueError`` (the adapter maps it like the other move-card
    409s). Ops are applied in list order — tree-walking callers supply
    preorder — and a later op landing on an already-inserted batch card steps
    PAST it (uniform rule across ALL selectors), so batches never reverse.
    Every affected column is rewritten from the deterministic order
    ``(position ASC, id DESC)``: actives dense ``0..n-1``, archived ``n..m``.
    Flushes through the persistence port and returns how many positions
    changed.
    """

    async def resequence_columns(
        self,
        context: Any,
        board_id: str,
        ops: list[ColumnResequenceOp],
        *,
        extra_columns: tuple[CardStatus, ...] = (),
        records: dict[str, ApplicationRecord] | None = None,
    ) -> int:
        port = get_application_persistence_port()

        # ---- structural pre-validation (no I/O) --------------------------
        seen: set[str] = set()
        for op in ops:
            if op.card_id in seen:
                raise ValueError(f"resequence_duplicate_card: {op.card_id}")
            seen.add(op.card_id)
            selectors = [
                selector
                for selector in (
                    op.target_index,
                    op.before_id,
                    op.after_id,
                    op.placement,
                )
                if selector is not None
            ]
            if len(selectors) > 1:
                raise ValueError(f"resequence_conflicting_placement: {op.card_id}")
            if op.target_index is not None and op.target_index < 0:
                raise ValueError(f"resequence_negative_index: {op.card_id}")
            if op.placement is not None and op.placement not in ("start", "end"):
                raise ValueError(f"resequence_invalid_placement: {op.card_id}")
            if op.to_archived and selectors:
                raise ValueError(f"resequence_archived_placement: {op.card_id}")
            for anchor_value in (op.before_id, op.after_id):
                # Empty/whitespace anchors are STRUCTURALLY invalid — they
                # must never slip past pre-validation into apply.
                if anchor_value is not None and not anchor_value.strip():
                    raise ValueError(
                        f"resequence_anchor_invalid: {op.card_id} anchor ''"
                    )
            anchor_id = op.before_id if op.before_id is not None else op.after_id
            if anchor_id is not None and anchor_id == op.card_id:
                raise ValueError(f"resequence_anchor_self: {op.card_id}")

        # ---- record pre-validation: whole batch or nothing ---------------
        moved: dict[str, ApplicationRecord] = {}
        for op in ops:
            record = (records or {}).get(op.card_id)
            if record is None:
                record = await port.get(context, entity="card", record_id=op.card_id)
            if record is None:
                raise ValueError(f"resequence_card_not_found: {op.card_id}")
            if record.board_id != board_id:
                raise ValueError(f"resequence_wrong_board: {op.card_id}")
            current = getattr(record.status, "value", record.status)
            expected = getattr(op.from_status, "value", op.from_status)
            if current != expected or bool(
                getattr(record, "archived", False)
            ) != bool(op.from_archived):
                raise ValueError(
                    f"resequence_stale_from: {op.card_id} is "
                    f"'{current}'/archived={bool(getattr(record, 'archived', False))}, "
                    f"expected '{expected}'/archived={bool(op.from_archived)}"
                )
            moved[op.card_id] = record

        # ---- anchor pre-validation ---------------------------------------
        for op in ops:
            anchor_id = op.before_id if op.before_id is not None else op.after_id
            if anchor_id is None:
                continue
            if anchor_id in moved:
                raise ValueError(f"resequence_anchor_in_batch: {op.card_id}")
            anchor = await port.get(context, entity="card", record_id=anchor_id)
            target_value = getattr(op.to_status, "value", op.to_status)
            if (
                anchor is None
                or anchor.board_id != board_id
                or getattr(anchor.status, "value", anchor.status) != target_value
                or bool(getattr(anchor, "archived", False))
            ):
                raise ValueError(
                    f"resequence_anchor_invalid: {op.card_id} anchor {anchor_id}"
                )

        # ---- load affected columns (moved cards excluded) ----------------
        statuses: dict[str, CardStatus] = {}
        for op in ops:
            for status in (op.from_status, op.to_status):
                statuses.setdefault(getattr(status, "value", status), status)
        for status in extra_columns:
            statuses.setdefault(getattr(status, "value", status), status)

        actives: dict[str, list[ApplicationRecord]] = {}
        archived: dict[str, list[ApplicationRecord]] = {}
        for value, status in statuses.items():
            rows = [
                row
                for row in await port.list(
                    context,
                    ApplicationQuery(
                        entity="card",
                        filters=(
                            ApplicationFilter("board_id", "eq", board_id),
                            ApplicationFilter("status", "eq", status),
                        ),
                    ),
                )
                if row.id not in moved
            ]
            # Deterministic per-column order: (position ASC, id DESC) — the
            # two-pass stable sort keeps id DESC inside equal positions.
            rows.sort(key=lambda item: item.id, reverse=True)
            rows.sort(
                key=lambda item: item.position if isinstance(item.position, int) else 0
            )
            actives[value] = [
                row for row in rows if not bool(getattr(row, "archived", False))
            ]
            archived[value] = [
                row for row in rows if bool(getattr(row, "archived", False))
            ]

        # ---- apply ops in list order (preorder for tree callers) ---------
        # Uniform stability rule: a computed insertion index steps PAST any
        # batch card inserted earlier, whatever selector produced it — so
        # start/index/after collisions can never reverse batch order.
        batch_inserted: set[str] = set()
        for op in ops:
            record = moved[op.card_id]
            target_value = getattr(op.to_status, "value", op.to_status)
            if op.to_archived:
                archived[target_value].append(record)
            else:
                lane = actives[target_value]
                if op.before_id is not None:
                    anchor_index = next(
                        (i for i, row in enumerate(lane) if row.id == op.before_id),
                        None,
                    )
                    if anchor_index is None:
                        raise ValueError(
                            f"resequence_anchor_invalid: {op.card_id} "
                            f"anchor {op.before_id}"
                        )
                    index = anchor_index
                elif op.after_id is not None:
                    anchor_index = next(
                        (i for i, row in enumerate(lane) if row.id == op.after_id),
                        None,
                    )
                    if anchor_index is None:
                        raise ValueError(
                            f"resequence_anchor_invalid: {op.card_id} "
                            f"anchor {op.after_id}"
                        )
                    index = anchor_index + 1
                elif op.placement == "start":
                    index = 0
                elif op.target_index is not None:
                    index = min(op.target_index, len(lane))
                else:  # placement == "end" or no selector: end of actives
                    index = len(lane)
                while index < len(lane) and lane[index].id in batch_inserted:
                    index += 1
                lane.insert(index, record)
            batch_inserted.add(record.id)
            if getattr(record.status, "value", record.status) != target_value:
                record.status = op.to_status
            if bool(getattr(record, "archived", False)) != bool(op.to_archived):
                record.archived = op.to_archived

        # ---- dense rewrite: actives 0..n-1, archived n..m ----------------
        changed = 0
        for value in statuses:
            position = 0
            for row in (*actives[value], *archived[value]):
                if row.position != position:
                    row.position = position
                    changed += 1
                position += 1
        await port.flush(context)
        return changed


_RUNTIME_KEY = "ports.card_repository.resequencer"


def register_card_repository_port(port: CardRepositoryPort) -> None:
    register_runtime_value(_RUNTIME_KEY, port)


def get_card_repository_port() -> CardRepositoryPort:
    """Return the registered port, falling back to the Core default."""
    try:
        return require_runtime_value(_RUNTIME_KEY, "card_repository_port_not_configured")
    except Exception:  # noqa: BLE001 — unregistered: serve the Core default
        return CoreCardResequencer()


def reset_card_repository_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)
