"""Explicit submission bases: synthetic for pure contracts, native for writers."""
from okto_pulse.core.domain.spec_planning import SpecPlanningManifest


def manifest_payload(spec_id, board_id="board-1", edition=1, version=1):
    return SpecPlanningManifest(
        board_id=board_id, spec_id=spec_id, edition=edition, spec_version=version,
        subject_incarnation="a" * 32, subject_revision=1,
        authority_incarnation="b" * 32, authority_revision=1,
    ).model_dump(mode="json")


async def current_manifest(session, spec_id):
    from sqlalchemy import select
    from okto_pulse.core.domain.realm import RealmScope
    from okto_pulse.community.adapters.sqlalchemy_models import Spec
    from okto_pulse.community.adapters.sqlalchemy_spec_planning_manifest import CommunitySpecPlanningManifest

    board_id = (await session.execute(select(Spec.board_id).where(Spec.id == spec_id))).scalar_one()
    manifest, = await CommunitySpecPlanningManifest(session, RealmScope.local()).read_many(
        board_id=board_id, spec_ids=(spec_id,),
    )
    return manifest.model_dump(mode="json")


class ManifestDouble:
    """Only for tests that isolate non-persistence behavior."""
    def __init__(self):
        self.checked = []

    async def require_current(self, expected):
        self.checked.append(expected)
