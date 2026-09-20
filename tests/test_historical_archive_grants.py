"""Scoped archive revocations cannot widen the captured permission ceiling."""

from dataclasses import replace
from itertools import product

import pytest

from okto_pulse.core.ports.historical_archive import (
    ArchiveGrantState, ArchiveReadGrant, ArchiveReadSections, ArchiveSection,
    ArchiveSourceScope, archive_section_is_readable, revoke_archive_sections,
)


def state(sections=ArchiveReadSections(True, True, True, True)):
    grant = ArchiveReadGrant(ArchiveSourceScope("local", "board", "opaque", "origin"),
        "agent", "reader", sections)
    return ArchiveGrantState(grant, sections, "archive", "a" * 64, 1)


@pytest.mark.parametrize("decisions", list(product((False, True), repeat=4)))
@pytest.mark.parametrize("section", list(ArchiveSection))
def test_revocation_only_narrows_and_root_revocation_hides_every_section(decisions, section):
    before = state(ArchiveReadSections(*decisions))
    narrowed = revoke_archive_sections(before, (section,))
    after = replace(before, sections=narrowed, revision=2)
    for candidate in ArchiveSection:
        allowed = archive_section_is_readable(after.effective_grant(), scope=before.captured.scope,
            actor_kind="agent", actor_id="reader", section=candidate,
            current_board_access=True, current_section_permission=True)
        assert not allowed or before.captured.sections.allows(candidate)
        if candidate is section or section is ArchiveSection.CONTENT:
            assert not allowed
    assert before.revision == 1


@pytest.mark.parametrize("sections", [(), ("qa",), [ArchiveSection.QA], (ArchiveSection.QA, ArchiveSection.QA)])
def test_revocation_requires_closed_unique_typed_sections(sections):
    with pytest.raises(ValueError, match="revocation_invalid"):
        revoke_archive_sections(state(), sections)


def test_state_cannot_exceed_captured_denial_or_reenable_by_revoke():
    restricted = state(ArchiveReadSections(True, False, False, True))
    with pytest.raises(ValueError, match="exceeds_captured"):
        replace(restricted, sections=ArchiveReadSections(True, True, False, True))
    assert revoke_archive_sections(restricted, (ArchiveSection.QA,)) == restricted.sections


@pytest.mark.parametrize("changes", [{"revision": True}, {"revision": 0}, {"revision": 1.5},
    {"archive_id": " "}, {"archive_sha256": "f" * 63}, {"archive_sha256": "G" * 64}])
def test_persistent_state_rejects_invalid_revision_and_provenance(changes):
    with pytest.raises(ValueError, match="state_invalid"):
        replace(state(), **changes)
