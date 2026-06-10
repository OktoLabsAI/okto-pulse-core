"""One-shot idempotent migrator: canonicalize legacy string FR/AC into
structured dicts with stable ids (spec 9d66847f, Phase 1).

Idempotent: a second run over already-canonical specs is a no-op (hash ids are
stable). Reuses ``canonicalize_fr_ac`` and the SpecService storage/session (no
unsafe bypass). Emits a structured summary; never logs FR/AC text.

CLI::

    python -m okto_pulse.core.commands.materialize_legacy_fr_ac --board-id=<UUID> --dry-run=false
"""

from __future__ import annotations

import argparse
import asyncio

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified

from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.models.db import Spec
from okto_pulse.core.services.spec_entity_canonicalization import (
    DuplicateSpecChildIdError,
    canonicalize_fr_ac,
)

# (storage field, canonicalization entity_type)
_FR_AC: tuple[tuple[str, str], ...] = (
    ("functional_requirements", "functional_requirement"),
    ("acceptance_criteria", "acceptance_criterion"),
)


async def materialize_legacy_fr_ac_board(
    db: AsyncSession, board_id: str, dry_run: bool = True
) -> dict:
    """Canonicalize FR/AC of every spec in ``board_id``.

    Returns a structured summary
    ``{board_id, dry_run, scanned, changed, skipped, errors}`` (no FR/AC text,
    only counters and the board id). Idempotent: a re-run produces the same ids
    and reports the specs as ``skipped``. On :class:`DuplicateSpecChildIdError`
    the spec is counted in ``errors`` and nothing is persisted for it.
    """
    result = await db.execute(select(Spec).where(Spec.board_id == board_id))
    specs = list(result.scalars().all())

    summary = {
        "board_id": board_id,
        "dry_run": dry_run,
        "scanned": len(specs),
        "changed": 0,
        "skipped": 0,
        "errors": 0,
    }

    for spec in specs:
        # Canonicalize ALL fields of the spec in memory FIRST. Only after every
        # field succeeds do we apply setattr/flag_modified. A spec with one
        # corrupted field (DuplicateSpecChildIdError) is therefore skipped
        # atomically — no field is ever partially persisted (fail-closed).
        try:
            pending: list[tuple[str, list]] = []
            for field_name, entity_type in _FR_AC:
                current = getattr(spec, field_name)
                canonical = canonicalize_fr_ac(
                    entity_type, current, existing_items=current
                )
                if canonical != current:
                    pending.append((field_name, canonical))
        except DuplicateSpecChildIdError:
            # Already-corrupted data (two children sharing an id) — surface it
            # and mutate NOTHING for this spec.
            summary["errors"] += 1
            continue

        if pending:
            summary["changed"] += 1
            if not dry_run:
                for field_name, canonical in pending:
                    setattr(spec, field_name, canonical)
                    flag_modified(spec, field_name)
        else:
            summary["skipped"] += 1

    if not dry_run:
        await db.commit()
    return summary


async def _amain(board_id: str, dry_run: bool) -> dict:
    async with get_session_factory()() as db:
        return await materialize_legacy_fr_ac_board(db, board_id, dry_run=dry_run)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Canonicalize legacy string FR/AC into structured dicts with stable "
            "ids (idempotent)."
        )
    )
    parser.add_argument("--board-id", required=True, help="Board UUID to migrate.")
    parser.add_argument(
        "--dry-run",
        default="true",
        help="'true' (default) only reports; 'false' persists the changes.",
    )
    args = parser.parse_args()
    dry_run = str(args.dry_run).strip().lower() not in ("false", "0", "no")
    summary = asyncio.run(_amain(args.board_id, dry_run))
    print(summary)


if __name__ == "__main__":
    main()
