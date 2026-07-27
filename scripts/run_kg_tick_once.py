"""One-shot trigger of the KG daily decay tick — debug/operator helper.

Invokes `_run_daily_tick()` from `core.events.handlers.kg_decay_tick`
exactly once against the local community SQLite database, populating
`kg_tick_runs` with a real row so the KGHealthView can show
"Last tick: 0h ago" instead of "Tick has never run".

Production tick is APScheduler cron at 03:00 UTC daily; this script
lets the operator force a run for validation or after a bulk import
without waiting for the cron window.

Usage:
    python scripts/run_kg_tick_once.py
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from pathlib import Path

# Use the same data dir the community CLI uses by default (DATA_DIR env
# honored, like CommunitySettings).
DATA_DIR = Path(os.environ.get("DATA_DIR") or (Path.home() / ".okto-pulse")).expanduser()
DB_PATH = DATA_DIR / "data" / "pulse.db"

if not DB_PATH.exists():
    print(f"ERROR: pulse.db not found at {DB_PATH}", file=sys.stderr)
    sys.exit(1)

# KGD-01 C6 (serve-lock): o tick abre grafos de board (Ladybug) — rodar com o
# servidor vivo criaria um segundo escritor no mesmo graph.lbug (produtor do
# "escritor stale" que corrompe o WAL). Mesmo guard fail-fast da CLI
# community: heartbeat fresco OU PID vivo no lock => recusa; takeover
# implícito só com heartbeat stale E PID comprovadamente morto. Import
# guardado: em ambiente core-only (sem a edição community) o check é pulado.
try:
    from okto_pulse.community.serve_lock import (  # type: ignore
        ServeAlreadyRunningError,
        assert_no_live_server,
    )
except ImportError:
    ServeAlreadyRunningError = None  # type: ignore[assignment]
    assert_no_live_server = None  # type: ignore[assignment]

if assert_no_live_server is not None:
    try:
        assert_no_live_server(DATA_DIR, operation="run_kg_tick_once")
    except ServeAlreadyRunningError as exc:  # type: ignore[misc]
        print(f"ERROR [serve-lock]: {exc}", file=sys.stderr)
        sys.exit(2)

# Force community-style URL so the engine matches what `okto-pulse serve`
# runs against. WAL mode means we can write while the server is reading.
os.environ.setdefault(
    "DATABASE_URL",
    f"sqlite+aiosqlite:///{DB_PATH.as_posix()}",
)


async def main() -> None:
    from okto_pulse.core.infra.config import CoreSettings, configure_settings
    from okto_pulse.core.infra.database import (
        create_database,
        get_session_factory,
        init_db,
        close_db,
    )
    from okto_pulse.core.events.handlers.kg_decay_tick import _run_daily_tick

    settings = CoreSettings(database_url=os.environ["DATABASE_URL"])
    configure_settings(settings)
    create_database(settings.database_url, echo=False)
    await init_db()

    factory = get_session_factory()
    tick_id = str(uuid.uuid4())
    print(f"Triggering tick {tick_id} ...")
    async with factory() as session:
        summary = await _run_daily_tick(tick_id=tick_id, session=session)
    print("Tick completed:")
    for k, v in summary.items():
        print(f"  {k} = {v}")

    await close_db()


if __name__ == "__main__":
    asyncio.run(main())
