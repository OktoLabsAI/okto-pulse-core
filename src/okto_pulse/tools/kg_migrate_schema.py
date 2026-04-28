"""CLI: force-apply KG schema migrations for legacy boards.

Spec 818748f2 — FR5. Use as a stand-in for ``okto-pulse kg migrate-schema``
since the project does not ship a Click-based CLI:

    python -m okto_pulse.tools.kg_migrate_schema --board <id>
    python -m okto_pulse.tools.kg_migrate_schema --all-boards

Idempotente. Safe to re-run. NUNCA delete graph.kuzu — use this script
ao ver ``Binder exception: Cannot find property X for n``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any


def _run_single_board(board_id: str) -> dict[str, Any]:
    from okto_pulse.core.kg.schema import migrate_schema_for_board
    return migrate_schema_for_board(board_id)


async def _list_local_boards() -> list[tuple[str, str]]:
    """Return [(board_id, board_name)] for every board in the local DB.

    Uses the same SQLite/PG handle the server uses — works offline
    against the file-backed default install (``~/.okto-pulse/db.sqlite3``)
    or against an external PG when ``OKTO_PULSE_DATABASE_URL`` is set.
    """
    from sqlalchemy import select

    from okto_pulse.core.infra.database import (
        get_session_factory,
        init_db,
    )
    from okto_pulse.core.models.db import Board

    await init_db()
    factory = get_session_factory()
    async with factory() as session:
        rows = (await session.execute(select(Board.id, Board.name))).all()
    return [(r[0], r[1]) for r in rows]


def _emit_single(summary: dict[str, Any]) -> int:
    print(json.dumps(summary, indent=2, default=str))
    return 0 if summary.get("migrated") and not summary.get("errors") else 1


def _emit_all(results: list[dict[str, Any]], names: dict[str, str]) -> int:
    failed = 0
    for r in results:
        bid = r["board_id"]
        bname = names.get(bid, "?")
        line = (
            f"{bid} ({bname}): migrated={r['migrated']} "
            f"columns_added_count={sum(len(v) for v in r['columns_added'].values())} "
            f"errors={len(r['errors'])} duration={r['duration_ms']}ms"
        )
        print(line)
        if r["errors"]:
            failed += 1
            for err in r["errors"]:
                print(f"  ERROR: {err}", file=sys.stderr)
    return 0 if failed == 0 else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m okto_pulse.tools.kg_migrate_schema",
        description=(
            "Force-apply KG schema migrations on a board (idempotent). "
            "Use to fix 'Binder exception: Cannot find property X for n' "
            "in consolidations on boards bootstrapped pre-v0.3.2."
        ),
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--board", dest="board_id", help="Board UUID to migrate")
    group.add_argument(
        "--all-boards",
        dest="all_boards",
        action="store_true",
        help="Migrate every board known to the server",
    )
    args = parser.parse_args(argv)

    if args.all_boards:
        pairs = asyncio.run(_list_local_boards())
        names = {bid: bname for bid, bname in pairs}
        results = [_run_single_board(bid) for bid, _ in pairs]
        return _emit_all(results, names)

    summary = _run_single_board(args.board_id)
    return _emit_single(summary)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
