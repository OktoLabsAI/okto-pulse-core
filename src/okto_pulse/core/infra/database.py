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
    await _migrate_add_archive_columns()
    await _migrate_add_spec_validation_columns()
    await _migrate_status_renames()
    await _migrate_add_permission_columns()
    async with get_engine().begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await _migrate_add_card_sprint_id()
    await _migrate_add_card_knowledge_bases()
    await _migrate_agent_boards()
    await _seed_builtin_presets()
    await _migrate_agent_permissions()


async def _migrate_agent_permissions() -> None:
    """Migrate agents from legacy flat permissions to granular permission_flags."""
    import logging
    logger = logging.getLogger("okto_pulse.migrations")

    from sqlalchemy import text as sa_text

    async with get_session_factory()() as session:
        try:
            result = await session.execute(sa_text(
                "SELECT id, permissions, permission_flags FROM agents WHERE permission_flags IS NULL"
            ))
            agents = result.all()
            if not agents:
                return

            import json as _json
            from okto_pulse.core.infra.permissions import (
                PERMISSION_REGISTRY, map_legacy_permissions
            )
            import copy

            for agent_row in agents:
                agent_id = agent_row[0]
                old_perms = agent_row[1]

                if old_perms is None:
                    new_flags = copy.deepcopy(PERMISSION_REGISTRY)
                else:
                    if isinstance(old_perms, str):
                        perm_list = _json.loads(old_perms)
                    else:
                        perm_list = old_perms
                    new_flags = map_legacy_permissions(perm_list)

                flags_json = _json.dumps(new_flags)
                await session.execute(
                    sa_text(
                        "UPDATE agents SET permission_flags = :flags WHERE id = :id"
                    ).bindparams(flags=flags_json, id=agent_id)
                )
                logger.info(f"Migrated agent {agent_id[:8]} permissions ({len(flags_json)} bytes)")
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
