"""Test-only SQLAlchemy default board configuration store."""

import copy
from typing import Any

from sqlalchemy import func, select

from sqlalchemy_test_models import (
    DefaultBoardConfiguration,
    DefaultBoardConfigurationAudit,
    DesignSystem,
    Guideline,
)
from okto_pulse.core.ports.default_board_configuration import (
    DefaultBoardTemplateAudit,
    DefaultBoardTemplateRecord,
    DefaultDesignSystemFact,
    DefaultGuidelineFact,
)


def _record(row: Any) -> DefaultBoardTemplateRecord:
    return DefaultBoardTemplateRecord(
        id=str(row.id),
        version=int(row.version),
        status=str(row.status),
        is_active=bool(row.is_active),
        scope=str(row.scope),
        settings_payload=copy.deepcopy(row.settings_payload or {}),
        guideline_default_refs=copy.deepcopy(row.guideline_default_refs),
        design_system_default_ref=copy.deepcopy(row.design_system_default_ref),
        created_by=str(row.created_by),
        spec_checklist_mode=row.spec_checklist_mode,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _apply(row: Any, record: DefaultBoardTemplateRecord) -> None:
    for field_name in (
        "version",
        "status",
        "is_active",
        "scope",
        "settings_payload",
        "guideline_default_refs",
        "design_system_default_ref",
        "spec_checklist_mode",
        "created_by",
    ):
        setattr(row, field_name, copy.deepcopy(getattr(record, field_name)))


class TestSqlAlchemyDefaultBoardConfigurationStore:
    __test__ = False

    async def _authoritative_fact(
        self,
        context,
        *,
        guideline_id: str,
        revision_id: str | None = None,
        revision_number: int | None = None,
    ) -> DefaultGuidelineFact | None:
        try:
            from okto_pulse.core.ports.guideline_policy import (
                GuidelineRevisionListQuery,
            )
            from okto_pulse.core.ports.relational_application import (
                require_relational_application_adapter,
            )

            policy = require_relational_application_adapter().guideline_policy(context)
        except RuntimeError:
            return None
        identity = await policy.get_guideline(guideline_id=guideline_id)
        if identity is None:
            return None
        if revision_id is not None:
            revision = await policy.get_revision(
                guideline_id=guideline_id,
                revision_id=revision_id,
            )
        elif revision_number is not None:
            revision = None
            cursor = None
            while True:
                page = await policy.list_revisions(
                    GuidelineRevisionListQuery(
                        guideline_id=guideline_id,
                        limit=200,
                        cursor=cursor,
                    )
                )
                revision = next(
                    (
                        candidate
                        for candidate in page.items
                        if candidate.revision_number == revision_number
                    ),
                    None,
                )
                if revision is not None or not page.has_more:
                    break
                cursor = page.next_cursor
        else:
            head = await policy.get_head(guideline_id=guideline_id)
            revision = (
                await policy.get_revision(
                    guideline_id=guideline_id,
                    revision_id=head.revision_id,
                )
                if head is not None
                else None
            )
        if revision is None:
            return None
        retirement = await policy.get_retirement(guideline_id=guideline_id)
        return DefaultGuidelineFact(
            id=identity.guideline_id,
            title=revision.title,
            scope=identity.scope.value,
            board_id=identity.board_id,
            owner_id=identity.owner_id,
            version=revision.revision_number,
            revision_id=revision.revision_id,
            semantic_version=revision.semantic_version,
            revision_digest=revision.content_digest,
            revision_number=revision.revision_number,
            retired=retirement is not None,
        )

    async def resolve_active(
        self, context, *, scope: str
    ) -> DefaultBoardTemplateRecord | None:
        row = (
            (
                await context.execute(
                    select(DefaultBoardConfiguration)
                    .where(
                        DefaultBoardConfiguration.scope == scope,
                        DefaultBoardConfiguration.is_active.is_(True),
                    )
                    .order_by(DefaultBoardConfiguration.version.desc())
                )
            )
            .scalars()
            .first()
        )
        return _record(row) if row is not None else None

    async def get_template(
        self, context, *, template_id: str
    ) -> DefaultBoardTemplateRecord | None:
        row = await context.get(DefaultBoardConfiguration, template_id)
        return _record(row) if row is not None else None

    async def next_version(self, context, *, scope: str) -> int:
        value = await context.scalar(
            select(func.max(DefaultBoardConfiguration.version)).where(
                DefaultBoardConfiguration.scope == scope
            )
        )
        return int(value or 0) + 1

    async def create_template(
        self, context, record: DefaultBoardTemplateRecord
    ) -> DefaultBoardTemplateRecord:
        row = DefaultBoardConfiguration(id=record.id)
        _apply(row, record)
        context.add(row)
        await context.flush()
        await context.refresh(row)
        return _record(row)

    async def save_template(
        self, context, record: DefaultBoardTemplateRecord
    ) -> DefaultBoardTemplateRecord:
        row = await context.get(DefaultBoardConfiguration, record.id)
        if row is None:
            raise LookupError(f"Default board template {record.id!r} disappeared")
        _apply(row, record)
        await context.flush()
        await context.refresh(row)
        return _record(row)

    async def list_active_others(
        self, context, *, scope: str, exclude_template_id: str
    ) -> tuple[DefaultBoardTemplateRecord, ...]:
        rows = (
            (
                await context.execute(
                    select(DefaultBoardConfiguration).where(
                        DefaultBoardConfiguration.scope == scope,
                        DefaultBoardConfiguration.is_active.is_(True),
                        DefaultBoardConfiguration.id != exclude_template_id,
                    )
                )
            )
            .scalars()
            .all()
        )
        return tuple(_record(row) for row in rows)

    async def list_templates(
        self, context, *, scope: str
    ) -> tuple[DefaultBoardTemplateRecord, ...]:
        rows = (
            (
                await context.execute(
                    select(DefaultBoardConfiguration)
                    .where(DefaultBoardConfiguration.scope == scope)
                    .order_by(DefaultBoardConfiguration.version.desc())
                )
            )
            .scalars()
            .all()
        )
        return tuple(_record(row) for row in rows)

    async def get_guideline(
        self, context, *, guideline_id: str
    ) -> DefaultGuidelineFact | None:
        authoritative = await self._authoritative_fact(
            context,
            guideline_id=guideline_id,
        )
        if authoritative is not None:
            return authoritative
        row = await context.get(Guideline, guideline_id)
        if row is None:
            return None
        return DefaultGuidelineFact(
            id=str(row.id),
            title=str(row.title),
            scope=str(row.scope),
            board_id=str(row.board_id) if row.board_id else None,
            owner_id=str(row.owner_id) if row.owner_id else None,
            version=row.version,
        )

    async def get_guideline_revision(
        self,
        context,
        *,
        guideline_id: str,
        revision_id: str | None = None,
        revision_number: int | None = None,
    ) -> DefaultGuidelineFact | None:
        if (revision_id is None) == (revision_number is None):
            raise ValueError("exactly one guideline revision selector is required")
        authoritative = await self._authoritative_fact(
            context,
            guideline_id=guideline_id,
            revision_id=revision_id,
            revision_number=revision_number,
        )
        if authoritative is not None:
            return authoritative
        row = await context.get(Guideline, guideline_id)
        if row is None:
            return None
        # This legacy test schema predates immutable guideline revisions.  It
        # can resolve only its single mutable version and intentionally rejects
        # opaque revision ids.
        if revision_id is not None or int(revision_number or 0) != int(row.version):
            return None
        return DefaultGuidelineFact(
            id=str(row.id),
            title=str(row.title),
            scope=str(row.scope),
            board_id=str(row.board_id) if row.board_id else None,
            owner_id=str(row.owner_id) if row.owner_id else None,
            version=int(row.version),
            revision_number=int(row.version),
        )

    async def list_global_guidelines(
        self, context, *, owner_id: str | None
    ) -> tuple[DefaultGuidelineFact, ...]:
        statement = select(Guideline).where(
            Guideline.scope == "global", Guideline.board_id.is_(None)
        )
        if owner_id is not None:
            statement = statement.where(Guideline.owner_id == owner_id)
        rows = (
            (await context.execute(statement.order_by(Guideline.title))).scalars().all()
        )
        facts: list[DefaultGuidelineFact] = []
        for row in rows:
            fact = await self.get_guideline(
                context,
                guideline_id=str(row.id),
            )
            if fact is not None:
                facts.append(fact)
        return tuple(sorted(facts, key=lambda fact: (fact.title, fact.id)))

    async def get_design_system(
        self, context, *, design_system_id: str
    ) -> DefaultDesignSystemFact | None:
        row = await context.get(DesignSystem, design_system_id)
        if row is None:
            return None
        return DefaultDesignSystemFact(
            id=str(row.id),
            scope=str(row.scope),
            board_id=str(row.board_id) if row.board_id else None,
            status=str(row.status),
        )

    def add_audit(self, context, audit: DefaultBoardTemplateAudit) -> None:
        context.add(
            DefaultBoardConfigurationAudit(
                template_id=audit.template_id,
                template_version=audit.template_version,
                event_type=audit.event_type,
                actor_id=audit.actor_id,
                scope=audit.scope,
                payload=copy.deepcopy(audit.payload),
            )
        )


__all__ = ["TestSqlAlchemyDefaultBoardConfigurationStore"]
