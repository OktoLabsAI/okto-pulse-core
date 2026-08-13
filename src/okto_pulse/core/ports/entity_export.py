"""Transaction-bound reader port for canonical entity export bundles."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from okto_pulse.core.domain.entity_export import (
    EntityExportBundle,
    EntityExportDisclosure,
    EntityExportRequest,
)
from okto_pulse.core.domain.realm import RealmScope


@runtime_checkable
class EntityExportReadPort(Protocol):
    """Build one renderer-neutral bundle inside the surrounding UoW snapshot.

    An implementation must inspect ``disclosure`` before reading an optional
    section. A denied section is represented by an ``omitted`` manifest entry
    with reason ``permission_denied`` and no counts; its source tables must not
    be queried. The port never commits or starts an independent transaction.
    """

    async def build_bundle(
        self,
        *,
        request: EntityExportRequest,
        disclosure: EntityExportDisclosure,
        actor_id: str,
        realm_scope: RealmScope,
    ) -> EntityExportBundle: ...


__all__ = ["EntityExportReadPort"]
