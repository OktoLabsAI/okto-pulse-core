"""Database configuration and session management."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

# Base class for models — always available at import time
Base = declarative_base()

# Module-level singletons managed via create_database()
_engine = None
_session_factory = None


def create_database(url: str, *, echo: bool = False) -> None:
    """Create the async engine and session factory.

    Called once at application startup by the ecosystem bootstrap code.
    """
    global _engine, _session_factory

    engine_kwargs: dict = {
        "echo": echo,
        "future": True,
    }
    if url.startswith("postgresql"):
        engine_kwargs.update({
            "pool_size": 10,
            "max_overflow": 20,
            "pool_pre_ping": True,
        })

    _engine = create_async_engine(url, **engine_kwargs)
    _session_factory = async_sessionmaker(
        _engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )


def get_engine():
    """Return the async engine (asserts it has been initialised)."""
    assert _engine is not None, "Database not initialised. Call create_database() first."
    return _engine


def get_session_factory():
    """Return the async session factory (asserts it has been initialised)."""
    assert _session_factory is not None, "Database not initialised. Call create_database() first."
    return _session_factory


# ---------------------------------------------------------------------------
# Migration helpers
# ---------------------------------------------------------------------------


async def _migrate_agent_boards() -> None:
    """Migrate existing agents with board_id to the agent_boards junction table."""
    from sqlalchemy import text as sa_text

    dialect = get_engine().dialect.name
    if dialect == "postgresql":
        uuid_expr = "gen_random_uuid()::text"
    else:
        uuid_expr = (
            "lower(hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' ||"
            " substr(hex(randomblob(2)),2) || '-' ||"
            " substr('89ab', abs(random()) % 4 + 1, 1) ||"
            " substr(hex(randomblob(2)),2) || '-' ||"
            " hex(randomblob(6)))"
        )

    async with get_engine().begin() as conn:
        await conn.execute(sa_text(
            f"""
            INSERT INTO agent_boards (id, agent_id, board_id, granted_by, granted_at)
            SELECT
                {uuid_expr},
                a.id,
                a.board_id,
                a.created_by,
                a.created_at
            FROM agents a
            WHERE a.board_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM agent_boards ab
                WHERE ab.agent_id = a.id AND ab.board_id = a.board_id
              )
            """
        ))


async def _migrate_card_statuses() -> None:
    """Migrate card status enum values from Portuguese to English."""
    from sqlalchemy import text as sa_text

    status_map = {
        "nao_iniciado": "not_started",
        "iniciado": "started",
        "em_andamento": "in_progress",
        "em_pendencia": "on_hold",
        "finalizado": "done",
        "cancelado": "cancelled",
    }

    dialect = get_engine().dialect.name
    async with get_engine().begin() as conn:
        if dialect == "postgresql":
            table_check = await conn.execute(sa_text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'cards')"
            ))
            if not table_check.scalar():
                return
        else:
            try:
                await conn.execute(sa_text("SELECT 1 FROM cards LIMIT 0"))
            except Exception:
                return

        if dialect == "postgresql":
            col_check = await conn.execute(sa_text(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_name = 'cards' AND column_name = 'status'"
            ))
            row = col_check.first()
            if row and row[0] == 'USER-DEFINED':
                await conn.execute(sa_text(
                    "ALTER TABLE cards ALTER COLUMN status TYPE VARCHAR(50) USING status::text"
                ))
                try:
                    await conn.execute(sa_text("DROP TYPE IF EXISTS cardstatus"))
                except Exception:
                    pass

            for old_val, new_val in status_map.items():
                await conn.execute(sa_text(
                    f"UPDATE cards SET status = '{new_val}' WHERE LOWER(status) = '{old_val}'"
                ))
        else:
            for old_val, new_val in status_map.items():
                await conn.execute(sa_text(
                    f"UPDATE cards SET status = '{new_val}' WHERE LOWER(status) = '{old_val}'"
                ))


async def _migrate_add_priority_column() -> None:
    """Add priority column to cards table if it doesn't exist."""
    from sqlalchemy import text as sa_text

    dialect = get_engine().dialect.name
    async with get_engine().begin() as conn:
        if dialect == "postgresql":
            table_check = await conn.execute(sa_text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'cards')"
            ))
            if not table_check.scalar():
                return
            await conn.execute(sa_text(
                "ALTER TABLE cards ADD COLUMN IF NOT EXISTS priority VARCHAR(50) DEFAULT 'none' NOT NULL"
            ))
        else:
            try:
                await conn.execute(sa_text(
                    "ALTER TABLE cards ADD COLUMN priority VARCHAR(50) DEFAULT 'none' NOT NULL"
                ))
            except Exception:
                pass


async def _migrate_add_realm_id() -> None:
    """Add realm_id column to boards table if it doesn't exist."""
    from sqlalchemy import text as sa_text

    dialect = get_engine().dialect.name
    async with get_engine().begin() as conn:
        if dialect == "postgresql":
            table_check = await conn.execute(sa_text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'boards')"
            ))
            if not table_check.scalar():
                return
            await conn.execute(sa_text(
                "ALTER TABLE boards ADD COLUMN IF NOT EXISTS realm_id VARCHAR(255)"
            ))
            await conn.execute(sa_text(
                "CREATE INDEX IF NOT EXISTS ix_boards_realm_id ON boards (realm_id)"
            ))
        else:
            try:
                await conn.execute(sa_text(
                    "ALTER TABLE boards ADD COLUMN realm_id VARCHAR(255)"
                ))
            except Exception:
                pass


async def _migrate_add_comment_choice_columns() -> None:
    """Add choice board columns to comments table if they don't exist."""
    from sqlalchemy import text as sa_text

    dialect = get_engine().dialect.name
    async with get_engine().begin() as conn:
        if dialect == "postgresql":
            table_check = await conn.execute(sa_text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'comments')"
            ))
            if not table_check.scalar():
                return
            await conn.execute(sa_text(
                "ALTER TABLE comments ADD COLUMN IF NOT EXISTS comment_type VARCHAR(20) NOT NULL DEFAULT 'text'"
            ))
            await conn.execute(sa_text(
                "ALTER TABLE comments ADD COLUMN IF NOT EXISTS choices JSONB"
            ))
            await conn.execute(sa_text(
                "ALTER TABLE comments ADD COLUMN IF NOT EXISTS responses JSONB"
            ))
            await conn.execute(sa_text(
                "ALTER TABLE comments ADD COLUMN IF NOT EXISTS allow_free_text BOOLEAN NOT NULL DEFAULT false"
            ))
        else:
            for stmt in [
                "ALTER TABLE comments ADD COLUMN comment_type VARCHAR(20) NOT NULL DEFAULT 'text'",
                "ALTER TABLE comments ADD COLUMN choices JSON",
                "ALTER TABLE comments ADD COLUMN responses JSON",
                "ALTER TABLE comments ADD COLUMN allow_free_text BOOLEAN NOT NULL DEFAULT 0",
            ]:
                try:
                    await conn.execute(sa_text(stmt))
                except Exception:
                    pass


async def _migrate_add_bug_card_columns() -> None:
    """Add bug card columns to cards table if they don't exist."""
    from sqlalchemy import text as sa_text

    dialect = get_engine().dialect.name
    columns = [
        ("card_type", "VARCHAR(50) DEFAULT 'normal' NOT NULL"),
        ("origin_task_id", "VARCHAR(36)"),
        ("severity", "VARCHAR(50)"),
        ("expected_behavior", "TEXT"),
        ("observed_behavior", "TEXT"),
        ("steps_to_reproduce", "TEXT"),
        ("action_plan", "TEXT"),
        ("linked_test_task_ids", "JSON"),
    ]
    async with get_engine().begin() as conn:
        if dialect == "postgresql":
            table_check = await conn.execute(sa_text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'cards')"
            ))
            if not table_check.scalar():
                return
            for col_name, col_type in columns:
                await conn.execute(sa_text(
                    f"ALTER TABLE cards ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                ))
        else:
            for col_name, col_type in columns:
                try:
                    await conn.execute(sa_text(
                        f"ALTER TABLE cards ADD COLUMN {col_name} {col_type}"
                    ))
                except Exception:
                    pass


async def _migrate_add_skip_rules_coverage() -> None:
    """Add skip_rules_coverage column to specs table if it doesn't exist."""
    from sqlalchemy import text as sa_text

    dialect = get_engine().dialect.name
    async with get_engine().begin() as conn:
        if dialect == "postgresql":
            table_check = await conn.execute(sa_text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'specs')"
            ))
            if not table_check.scalar():
                return
            await conn.execute(sa_text(
                "ALTER TABLE specs ADD COLUMN IF NOT EXISTS skip_rules_coverage BOOLEAN DEFAULT false NOT NULL"
            ))
        else:
            try:
                await conn.execute(sa_text(
                    "ALTER TABLE specs ADD COLUMN skip_rules_coverage BOOLEAN DEFAULT 0 NOT NULL"
                ))
            except Exception:
                pass


async def _migrate_add_skip_trs_coverage() -> None:
    """Add skip_trs_coverage column to specs table if it doesn't exist."""
    from sqlalchemy import text as sa_text

    dialect = get_engine().dialect.name
    async with get_engine().begin() as conn:
        if dialect == "postgresql":
            table_check = await conn.execute(sa_text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'specs')"
            ))
            if not table_check.scalar():
                return
            await conn.execute(sa_text(
                "ALTER TABLE specs ADD COLUMN IF NOT EXISTS skip_trs_coverage BOOLEAN DEFAULT false NOT NULL"
            ))
        else:
            try:
                await conn.execute(sa_text(
                    "ALTER TABLE specs ADD COLUMN skip_trs_coverage BOOLEAN DEFAULT 0 NOT NULL"
                ))
            except Exception:
                pass


async def _migrate_add_decisions_columns() -> None:
    """Add decisions JSON column and skip_decisions_coverage flag to specs.

    Spec 0eb51d3e+decisions formalization — idempotent, defaults preserve
    backward-compat (skip=True means no gate change on existing specs).
    """
    from sqlalchemy import text as sa_text

    columns = [
        ("decisions", "JSON"),
        ("skip_decisions_coverage", "BOOLEAN DEFAULT true NOT NULL"),
    ]
    dialect = get_engine().dialect.name
    async with get_engine().begin() as conn:
        if dialect == "postgresql":
            table_check = await conn.execute(sa_text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'specs')"
            ))
            if not table_check.scalar():
                return
            for col_name, col_type in columns:
                await conn.execute(sa_text(
                    f"ALTER TABLE specs ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                ))
        else:
            for col_name, col_type in columns:
                try:
                    col_type_sqlite = col_type.replace("true", "1").replace("false", "0")
                    await conn.execute(sa_text(
                        f"ALTER TABLE specs ADD COLUMN {col_name} {col_type_sqlite}"
                    ))
                except Exception:
                    pass


async def _migrate_decisions_default_false() -> None:
    """Ideação #10 Fase 1: flip spec.skip_decisions_coverage default from True→False.

    Backward-compat: only NEW inserts get False; existing rows keep their
    current value (True for most pré-ideação #10 specs). On Postgres we
    ALTER COLUMN SET DEFAULT so raw SQL inserts honor the flip too. On
    SQLite, ALTER COLUMN DEFAULT is not supported — Python-side default
    (set on the SQLAlchemy model) handles future ORM inserts. Idempotent.
    """
    from sqlalchemy import text as sa_text

    dialect = get_engine().dialect.name
    if dialect != "postgresql":
        # SQLite handled via Python-side default on the Mapped column
        return
    async with get_engine().begin() as conn:
        table_check = await conn.execute(sa_text(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'specs')"
        ))
        if not table_check.scalar():
            return
        col_check = await conn.execute(sa_text(
            "SELECT column_default FROM information_schema.columns "
            "WHERE table_name = 'specs' AND column_name = 'skip_decisions_coverage'"
        ))
        current_default = col_check.scalar()
        if current_default and "false" in str(current_default).lower():
            return
        await conn.execute(sa_text(
            "ALTER TABLE specs ALTER COLUMN skip_decisions_coverage SET DEFAULT false"
        ))


async def _migrate_add_spec_validation_columns() -> None:
    """Add spec validation columns: skip_contract_coverage, skip_qualitative_validation, validation_threshold, evaluations."""
    from sqlalchemy import text as sa_text

    columns = [
        ("skip_contract_coverage", "BOOLEAN DEFAULT false NOT NULL"),
        ("skip_qualitative_validation", "BOOLEAN DEFAULT false NOT NULL"),
        ("validation_threshold", "INTEGER"),
        ("evaluations", "JSON"),
    ]
    dialect = get_engine().dialect.name
    async with get_engine().begin() as conn:
        if dialect == "postgresql":
            table_check = await conn.execute(sa_text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'specs')"
            ))
            if not table_check.scalar():
                return
            for col_name, col_type in columns:
                await conn.execute(sa_text(
                    f"ALTER TABLE specs ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                ))
        else:
            for col_name, col_type in columns:
                try:
                    col_type_sqlite = col_type.replace("false", "0")
                    await conn.execute(sa_text(
                        f"ALTER TABLE specs ADD COLUMN {col_name} {col_type_sqlite}"
                    ))
                except Exception:
                    pass


async def _migrate_heal_task_validation_field_names() -> None:
    """One-shot healing for pre-existing card.validations records that used legacy
    field names (estimated_completeness, estimated_drift, outcome, reviewer_id,
    general_justification) without the clean frontend aliases.

    Adds the clean aliases (completeness, drift, verdict, evaluator_id, summary)
    to every legacy record in-place. Also populates card.conclusions with a
    derived entry when a success validation exists but no conclusion was recorded
    (fixes the gap where submit_task_validation auto-routed to done without
    populating the Conclusion tab).

    Idempotent: safe to run multiple times. Records that already have the clean
    aliases are left untouched.
    """
    import json as _json
    from datetime import datetime, timezone
    from sqlalchemy import text as sa_text
    from sqlalchemy.orm.attributes import flag_modified as _flag_modified

    async with get_session_factory()() as db:
        # Load all cards that have any validations or might need healing.
        # Using raw SQL to avoid ORM overhead for this migration.
        try:
            result = await db.execute(sa_text(
                "SELECT id, validations, conclusions FROM cards WHERE validations IS NOT NULL"
            ))
            rows = result.fetchall()
        except Exception:
            # Table doesn't exist yet — nothing to heal
            return

        if not rows:
            return

        from okto_pulse.core.models.db import Card  # lazy import

        healed_count = 0
        for row in rows:
            card_id = row[0]
            raw_validations = row[1]
            raw_conclusions = row[2]

            # Parse JSON if stored as string (sqlite) vs dict (postgres)
            if isinstance(raw_validations, str):
                try:
                    validations = _json.loads(raw_validations)
                except Exception:
                    continue
            else:
                validations = raw_validations

            if not validations:
                continue

            modified = False
            latest_success_validation = None

            for v in validations:
                if not isinstance(v, dict):
                    continue
                # Add clean aliases if missing
                if "completeness" not in v and "estimated_completeness" in v:
                    v["completeness"] = v["estimated_completeness"]
                    modified = True
                if "drift" not in v and "estimated_drift" in v:
                    v["drift"] = v["estimated_drift"]
                    modified = True
                if "verdict" not in v and "outcome" in v:
                    v["verdict"] = "pass" if v["outcome"] == "success" else "fail"
                    modified = True
                if "evaluator_id" not in v and "reviewer_id" in v:
                    v["evaluator_id"] = v["reviewer_id"]
                    modified = True
                if "summary" not in v and "general_justification" in v:
                    v["summary"] = v["general_justification"]
                    modified = True
                # Track the latest success validation for conclusion auto-population
                if v.get("outcome") == "success" or v.get("verdict") == "pass":
                    latest_success_validation = v

            # Conclusion auto-population: if we have a success validation but no
            # conclusions, derive one from the validation.
            if isinstance(raw_conclusions, str):
                try:
                    conclusions = _json.loads(raw_conclusions) if raw_conclusions else []
                except Exception:
                    conclusions = []
            else:
                conclusions = raw_conclusions or []

            needs_conclusion = (
                latest_success_validation is not None
                and (not conclusions or len(conclusions) == 0)
            )
            if needs_conclusion:
                v = latest_success_validation
                conclusions = [{
                    "text": v.get("general_justification") or v.get("summary") or "",
                    "author_id": v.get("reviewer_id") or v.get("evaluator_id") or "",
                    "created_at": v.get("created_at") or datetime.now(timezone.utc).isoformat(),
                    "completeness": v.get("completeness", v.get("estimated_completeness", 0)),
                    "completeness_justification": v.get("completeness_justification", ""),
                    "drift": v.get("drift", v.get("estimated_drift", 0)),
                    "drift_justification": v.get("drift_justification", ""),
                    "source": "task_validation_heal",
                    "validation_id": v.get("id"),
                }]
                modified = True

            if modified:
                card = await db.get(Card, card_id)
                if card:
                    card.validations = validations
                    _flag_modified(card, "validations")
                    if needs_conclusion:
                        card.conclusions = conclusions
                        _flag_modified(card, "conclusions")
                    healed_count += 1

        if healed_count > 0:
            await db.commit()
            import logging
            logging.getLogger("okto_pulse.migrations").info(
                f"Task validation healing: patched {healed_count} card(s) with clean "
                f"aliases and/or auto-populated conclusions."
            )


async def _migrate_add_spec_validation_gate_columns() -> None:
    """Add Spec Validation Gate columns: validations (JSON history) and current_validation_id (pointer).

    Grandfathered: specs already in validated/in_progress/done status get validations=[] and
    current_validation_id=NULL — no retroactive lock applied.
    """
    from sqlalchemy import text as sa_text

    columns = [
        ("validations", "JSON"),
        ("current_validation_id", "VARCHAR(32)"),
    ]
    dialect = get_engine().dialect.name
    async with get_engine().begin() as conn:
        if dialect == "postgresql":
            table_check = await conn.execute(sa_text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'specs')"
            ))
            if not table_check.scalar():
                return
            for col_name, col_type in columns:
                await conn.execute(sa_text(
                    f"ALTER TABLE specs ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                ))
        else:
            for col_name, col_type in columns:
                try:
                    await conn.execute(sa_text(
                        f"ALTER TABLE specs ADD COLUMN {col_name} {col_type}"
                    ))
                except Exception:
                    pass


async def _migrate_add_archive_columns() -> None:
    """Add archived and pre_archive_status columns to ideations, refinements, specs, cards."""
    from sqlalchemy import text as sa_text

    dialect = get_engine().dialect.name
    tables = ["ideations", "refinements", "specs", "cards"]
    columns = [
        ("archived", "BOOLEAN DEFAULT false NOT NULL"),
        ("pre_archive_status", "VARCHAR(50)"),
    ]
    async with get_engine().begin() as conn:
        for table in tables:
            if dialect == "postgresql":
                table_check = await conn.execute(sa_text(
                    f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table}')"
                ))
                if not table_check.scalar():
                    continue
                for col_name, col_type in columns:
                    await conn.execute(sa_text(
                        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                    ))
            else:
                for col_name, col_type in columns:
                    try:
                        await conn.execute(sa_text(
                            f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}"
                        ))
                    except Exception:
                        pass


async def _migrate_status_renames() -> None:
    """Migrate old status values to new ones.

    - Ideation: 'refined' → 'done' (removed status)
    - Refinement: 'in_progress' → 'review' (renamed)
    """
    from sqlalchemy import text as sa_text

    async with get_engine().begin() as conn:
        # Ideation: 'refined' no longer exists — map to 'done'
        try:
            await conn.execute(sa_text(
                "UPDATE ideations SET status = 'done' WHERE status = 'refined'"
            ))
        except Exception:
            pass

        # Refinement: 'in_progress' renamed to 'review'
        try:
            await conn.execute(sa_text(
                "UPDATE refinements SET status = 'review' WHERE status = 'in_progress'"
            ))
        except Exception:
            pass


async def _migrate_add_permission_columns() -> None:
    """Add permission_flags and preset_id to agents, permission_overrides to agent_boards."""
    from sqlalchemy import text as sa_text

    agent_columns = [
        ("permission_flags", "JSON"),
        ("preset_id", "VARCHAR(36)"),
    ]
    board_columns = [
        ("permission_overrides", "JSON"),
    ]
    dialect = get_engine().dialect.name
    async with get_engine().begin() as conn:
        if dialect == "postgresql":
            for col_name, col_type in agent_columns:
                try:
                    await conn.execute(sa_text(
                        f"ALTER TABLE agents ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                    ))
                except Exception:
                    pass
            for col_name, col_type in board_columns:
                try:
                    await conn.execute(sa_text(
                        f"ALTER TABLE agent_boards ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                    ))
                except Exception:
                    pass
        else:
            for col_name, col_type in agent_columns:
                try:
                    await conn.execute(sa_text(
                        f"ALTER TABLE agents ADD COLUMN {col_name} {col_type}"
                    ))
                except Exception:
                    pass
            for col_name, col_type in board_columns:
                try:
                    await conn.execute(sa_text(
                        f"ALTER TABLE agent_boards ADD COLUMN {col_name} {col_type}"
                    ))
                except Exception:
                    pass


async def _migrate_add_card_sprint_id() -> None:
    """Add sprint_id FK column to cards table."""
    from sqlalchemy import text as sa_text

    dialect = get_engine().dialect.name
    async with get_engine().begin() as conn:
        if dialect == "postgresql":
            try:
                await conn.execute(sa_text(
                    "ALTER TABLE cards ADD COLUMN IF NOT EXISTS sprint_id VARCHAR(36) REFERENCES sprints(id) ON DELETE SET NULL"
                ))
            except Exception:
                pass
        else:
            try:
                await conn.execute(sa_text(
                    "ALTER TABLE cards ADD COLUMN sprint_id VARCHAR(36) REFERENCES sprints(id) ON DELETE SET NULL"
                ))
            except Exception:
                pass


async def _migrate_add_sprint_scope_fields() -> None:
    """Add objective and expected_outcome columns to sprints table."""
    from sqlalchemy import text as sa_text

    dialect = get_engine().dialect.name
    async with get_engine().begin() as conn:
        for col in ["objective", "expected_outcome"]:
            if dialect == "postgresql":
                try:
                    await conn.execute(sa_text(f"ALTER TABLE sprints ADD COLUMN IF NOT EXISTS {col} TEXT"))
                except Exception:
                    pass
            else:
                try:
                    await conn.execute(sa_text(f"ALTER TABLE sprints ADD COLUMN {col} TEXT"))
                except Exception:
                    pass


async def _migrate_add_card_knowledge_bases() -> None:
    """Add knowledge_bases JSON column to cards table."""
    from sqlalchemy import text as sa_text

    dialect = get_engine().dialect.name
    async with get_engine().begin() as conn:
        if dialect == "postgresql":
            try:
                await conn.execute(sa_text(
                    "ALTER TABLE cards ADD COLUMN IF NOT EXISTS knowledge_bases JSONB"
                ))
            except Exception:
                pass
        else:
            try:
                await conn.execute(sa_text(
                    "ALTER TABLE cards ADD COLUMN knowledge_bases JSON"
                ))
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


async def init_db() -> None:
    """Initialize database tables."""
    # Migrate enum type BEFORE create_all (avoids PG enum conflicts)
    await _migrate_card_statuses()
    await _migrate_add_priority_column()
    await _migrate_add_realm_id()
    await _migrate_add_comment_choice_columns()
    await _migrate_add_bug_card_columns()
    await _migrate_add_skip_rules_coverage()
    await _migrate_add_skip_trs_coverage()
    await _migrate_add_decisions_columns()
    await _migrate_decisions_default_false()
    await _migrate_add_archive_columns()
    await _migrate_add_spec_validation_columns()
    await _migrate_add_spec_validation_gate_columns()
    await _migrate_heal_task_validation_field_names()
    await _migrate_status_renames()
    await _migrate_add_permission_columns()
    await _migrate_add_event_tables()
    async with get_engine().begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await _migrate_add_card_sprint_id()
    await _migrate_add_card_knowledge_bases()
    await _migrate_add_sprint_scope_fields()
    await _migrate_agent_boards()
    await _migrate_add_task_validation_columns()
    await _seed_builtin_presets()
    await _migrate_agent_permissions()
    await _reconcile_builtin_presets()
    await _reconcile_agent_permission_flags()


async def _migrate_add_event_tables() -> None:
    """Create domain_events + domain_event_handler_executions tables.

    Idempotent: uses CREATE TABLE IF NOT EXISTS. Must run BEFORE
    Base.metadata.create_all so the two tables exist by the time the
    dispatcher starts consuming them.
    """
    from sqlalchemy import text as sa_text

    dialect = get_engine().dialect.name
    ts_type = "TIMESTAMP WITH TIME ZONE" if dialect == "postgresql" else "TIMESTAMP"
    json_type = "JSONB" if dialect == "postgresql" else "JSON"

    async with get_engine().begin() as conn:
        await conn.execute(sa_text(f"""
            CREATE TABLE IF NOT EXISTS domain_events (
                id VARCHAR(36) PRIMARY KEY,
                event_type VARCHAR(100) NOT NULL,
                board_id VARCHAR(36) NOT NULL,
                actor_id VARCHAR(36),
                actor_type VARCHAR(20) NOT NULL DEFAULT 'user',
                payload_json {json_type} NOT NULL,
                occurred_at {ts_type} NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (board_id) REFERENCES boards(id) ON DELETE CASCADE
            )
        """))
        await conn.execute(sa_text(
            "CREATE INDEX IF NOT EXISTS ix_domain_events_event_type "
            "ON domain_events(event_type)"
        ))
        await conn.execute(sa_text(
            "CREATE INDEX IF NOT EXISTS ix_domain_events_board_id "
            "ON domain_events(board_id)"
        ))
        await conn.execute(sa_text(
            "CREATE INDEX IF NOT EXISTS ix_domain_events_occurred_at "
            "ON domain_events(occurred_at)"
        ))

        await conn.execute(sa_text(f"""
            CREATE TABLE IF NOT EXISTS domain_event_handler_executions (
                id VARCHAR(36) PRIMARY KEY,
                event_id VARCHAR(36) NOT NULL,
                handler_name VARCHAR(100) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error VARCHAR(500),
                processed_at {ts_type},
                next_attempt_at {ts_type},
                FOREIGN KEY (event_id) REFERENCES domain_events(id) ON DELETE CASCADE,
                CONSTRAINT uq_deh_event_handler UNIQUE (event_id, handler_name)
            )
        """))
        await conn.execute(sa_text(
            "CREATE INDEX IF NOT EXISTS ix_deh_status_next_attempt "
            "ON domain_event_handler_executions(status, next_attempt_at)"
        ))


async def _migrate_add_task_validation_columns() -> None:
    """Add task validation gate columns to cards, specs, and sprints."""
    from sqlalchemy import text as sa_text

    dialect = get_engine().dialect.name

    # Cards: add validations JSON column
    card_columns = [
        ("validations", "JSON"),
    ]
    # Specs: add require_task_validation + threshold overrides
    spec_columns = [
        ("require_task_validation", "BOOLEAN"),
        ("validation_min_confidence", "INTEGER"),
        ("validation_min_completeness", "INTEGER"),
        ("validation_max_drift", "INTEGER"),
    ]
    # Sprints: same fields
    sprint_columns = [
        ("require_task_validation", "BOOLEAN"),
        ("validation_min_confidence", "INTEGER"),
        ("validation_min_completeness", "INTEGER"),
        ("validation_max_drift", "INTEGER"),
    ]

    migrations = [
        ("cards", card_columns),
        ("specs", spec_columns),
        ("sprints", sprint_columns),
    ]

    async with get_engine().begin() as conn:
        for table, columns in migrations:
            if dialect == "postgresql":
                table_check = await conn.execute(sa_text(
                    f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table}')"
                ))
                if not table_check.scalar():
                    continue
                for col_name, col_type in columns:
                    await conn.execute(sa_text(
                        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                    ))
            else:
                for col_name, col_type in columns:
                    try:
                        await conn.execute(sa_text(
                            f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}"
                        ))
                    except Exception:
                        pass


async def _migrate_agent_permissions() -> None:
    """Migrate agents from legacy flat permissions to granular permission_flags."""
    import logging
    logger = logging.getLogger("okto_pulse.migrations")

    import copy
    import json as _json
    from sqlalchemy import select as _select
    from sqlalchemy.orm.attributes import flag_modified

    async with get_session_factory()() as session:
        try:
            from okto_pulse.core.models.db import Agent as _Agent
            from okto_pulse.core.infra.permissions import (
                PERMISSION_REGISTRY, map_legacy_permissions
            )

            result = await session.execute(
                _select(_Agent).where(_Agent.permission_flags.is_(None))
            )
            agents = list(result.scalars().all())
            if not agents:
                return

            for agent in agents:
                old_perms = agent.permissions
                if old_perms is None:
                    new_flags = copy.deepcopy(PERMISSION_REGISTRY)
                else:
                    if isinstance(old_perms, str):
                        perm_list = _json.loads(old_perms)
                    else:
                        perm_list = old_perms
                    new_flags = map_legacy_permissions(perm_list)
                agent.permission_flags = new_flags
                flag_modified(agent, "permission_flags")
                logger.info(f"Migrated agent {agent.id[:8]} permissions")
            await session.commit()
            logger.info(f"Permission migration complete: {len(agents)} agent(s)")
        except Exception as e:
            logger.error(f"Permission migration failed: {e}")
            await session.rollback()


async def _seed_builtin_presets() -> None:
    """Seed built-in permission presets if they don't exist."""
    from sqlalchemy import text as sa_text

    try:
        from okto_pulse.core.infra.permissions import get_builtin_presets
        presets = get_builtin_presets()
    except Exception:
        return

    async with get_session_factory()() as session:
        try:
            from okto_pulse.core.models.db import PermissionPreset
            for preset_def in presets:
                # Check if preset already exists by name + is_builtin
                existing = await session.execute(
                    sa_text(
                        "SELECT id FROM permission_presets WHERE name = :name AND is_builtin = :builtin"
                    ).bindparams(name=preset_def["name"], builtin=True)
                )
                if existing.scalar():
                    continue
                import uuid
                import json
                preset = PermissionPreset(
                    id=str(uuid.uuid4()),
                    owner_id=None,
                    name=preset_def["name"],
                    description=preset_def["description"],
                    is_builtin=True,
                    base_preset_id=None,
                    flags=preset_def["flags"],
                )
                session.add(preset)
            await session.commit()
        except Exception:
            await session.rollback()


def _merge_missing_flags(stored: dict, registry: dict) -> tuple[dict, int]:
    """Deep-merge: add missing keys from registry to stored, preserve existing values.

    Registry leaves are booleans; for any key present in registry but missing
    in stored, the stored dict gets the key with default True. Existing leaf
    values in stored are never overwritten. Returns (merged_dict, added_count).

    Why default True? `PermissionSet.has()` already treats absent flags as
    allowed. Backfilling as False would silently DEMOTE existing agents when
    the registry grows — e.g. adding `sprint.entity.create` would deny it to
    every agent that predates the flag. Backfilling as True preserves the
    "absent = allowed" semantics that the rest of the system relies on.
    """
    added = 0
    for key, reg_val in registry.items():
        if key not in stored:
            # Entire subtree missing — copy structure with True leaves
            if isinstance(reg_val, dict):
                import copy as _copy
                subtree = _copy.deepcopy(reg_val)
                _set_all_leaves(subtree, True)
                stored[key] = subtree
                added += _count_leaves(subtree)
            else:
                stored[key] = True
                added += 1
        elif isinstance(reg_val, dict) and isinstance(stored[key], dict):
            _, sub_added = _merge_missing_flags(stored[key], reg_val)
            added += sub_added
    return stored, added


def _set_all_leaves(d: dict, value) -> None:
    for k, v in d.items():
        if isinstance(v, dict):
            _set_all_leaves(v, value)
        else:
            d[k] = value


def _count_leaves(d: dict) -> int:
    n = 0
    for v in d.values():
        if isinstance(v, dict):
            n += _count_leaves(v)
        else:
            n += 1
    return n


async def _reconcile_builtin_presets() -> None:
    """Refresh built-in preset flags from code definitions on every startup.

    Built-in presets are authoritative in code (get_builtin_presets()). When
    the registry grows (new entities or sub-flags), existing DB rows for
    built-in presets become stale. This rewrites their flags from the current
    definition, untouched for custom presets (is_builtin=False).
    """
    import logging
    import json as _json
    logger = logging.getLogger("okto_pulse.migrations")

    from sqlalchemy import text as sa_text

    try:
        from okto_pulse.core.infra.permissions import get_builtin_presets
        presets = get_builtin_presets()
    except Exception as e:
        logger.error(f"Built-in preset reconcile skipped (import failed): {e}")
        return

    async with get_session_factory()() as session:
        try:
            from okto_pulse.core.models.db import PermissionPreset
            from sqlalchemy import select, update
            refreshed = 0
            for preset_def in presets:
                query = select(PermissionPreset).where(
                    PermissionPreset.name == preset_def["name"],
                    PermissionPreset.is_builtin == True,
                )
                existing = (await session.execute(query)).scalar_one_or_none()
                if not existing:
                    continue
                new_flags_json = _json.dumps(preset_def["flags"], sort_keys=True)
                old_flags_json = _json.dumps(existing.flags or {}, sort_keys=True)
                if new_flags_json != old_flags_json:
                    await session.execute(
                        update(PermissionPreset)
                        .where(PermissionPreset.id == existing.id)
                        .values(flags=preset_def["flags"])
                    )
                    refreshed += 1
            if refreshed:
                await session.commit()
                logger.info(f"Refreshed {refreshed} built-in preset(s) from registry")
        except Exception as e:
            logger.error(f"Built-in preset reconcile failed: {e}")
            await session.rollback()


async def _reconcile_agent_permission_flags() -> None:
    """Backfill missing registry keys into agents' permission_flags on every startup.

    Non-destructive deep-merge: for each agent with a non-null permission_flags
    dict, walks the current PERMISSION_REGISTRY and adds any keys missing in
    the stored tree (default False). Existing leaf values are never overwritten
    — the user's customisations are preserved.
    """
    import logging
    import json as _json
    import copy as _copy
    logger = logging.getLogger("okto_pulse.migrations")

    from sqlalchemy import text as sa_text

    try:
        from okto_pulse.core.infra.permissions import PERMISSION_REGISTRY
    except Exception as e:
        logger.error(f"Agent permissions reconcile skipped (import failed): {e}")
        return

    async with get_session_factory()() as session:
        try:
            from okto_pulse.core.models.db import Agent as _Agent
            from sqlalchemy import select as _select
            from sqlalchemy.orm.attributes import flag_modified
            result = await session.execute(
                _select(_Agent).where(_Agent.permission_flags.is_not(None))
            )
            agents = list(result.scalars().all())
            updated = 0
            total_added = 0
            for agent in agents:
                if isinstance(agent.permission_flags, str):
                    stored_dict = _json.loads(agent.permission_flags)
                else:
                    stored_dict = _copy.deepcopy(agent.permission_flags or {})
                merged, added = _merge_missing_flags(stored_dict, PERMISSION_REGISTRY)
                if added > 0:
                    agent.permission_flags = merged
                    flag_modified(agent, "permission_flags")
                    updated += 1
                    total_added += added
            if updated:
                await session.commit()
                logger.info(
                    f"Reconciled {updated} agent(s) permission_flags "
                    f"(+{total_added} missing leaf keys backfilled as False)"
                )
        except Exception as e:
            logger.error(f"Agent permissions reconcile failed: {e}")
            await session.rollback()


async def close_db() -> None:
    """Close database connections."""
    await get_engine().dispose()


@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Get database session as async context manager."""
    async with get_session_factory()() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for database session."""
    async with get_session_factory()() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
