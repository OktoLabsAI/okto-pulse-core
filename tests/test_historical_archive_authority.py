"""Historical grants preserve decisions and require current, exact authority."""

from dataclasses import asdict, replace
from itertools import product

import pytest

from okto_pulse.core.ports.historical_archive import (
    ArchiveReadGrant, ArchiveReadSections, ArchiveSection, ArchiveSourceScope,
    archive_section_is_readable, capture_archive_read_sections, parse_archive_read_grant,
)
from okto_pulse.core.ports.permission_policy import PermissionSet


AUTHORITIES = dict(content_permission="sprint.entity.read", qa_permission="sprint.qa.read",
    evaluations_permission="sprint.evaluations.read", history_permission="sprint.history_read")
SCOPE = ArchiveSourceScope("realm-a", "board-a", "opaque-origin", "original-id")


@pytest.mark.parametrize(("root", "qa", "evaluations", "history"), list(product((False, True), repeat=4)))
def test_source_root_and_each_section_are_preserved(root, qa, evaluations, history):
    permissions = PermissionSet({"board": {"read": True}, "sprint": {
        "entity": {"read": root}, "qa": {"read": qa},
        "evaluations": {"read": evaluations}, "history_read": history,
    }})
    sections = capture_archive_read_sections(permissions, **AUTHORITIES)
    assert sections == ArchiveReadSections(root, root and qa, root and evaluations, root and history)


def test_absent_legacy_leaves_use_existing_policy_while_explicit_denials_win():
    assert capture_archive_read_sections(PermissionSet({}), **AUTHORITIES) == ArchiveReadSections(True, True, True, True)
    sections = capture_archive_read_sections(PermissionSet({"sprint": {"qa": {"read": False}}}), **AUTHORITIES)
    assert sections == ArchiveReadSections(True, False, True, True)
    reviewed = PermissionSet({}, owner_review_required=True, review_reason="unknown_lineage")
    assert capture_archive_read_sections(reviewed, **AUTHORITIES) == ArchiveReadSections(False, False, False, False)


@pytest.mark.parametrize("changed", ["realm_id", "board_id", "origin_kind", "origin_id", "actor_id", "actor_kind", "grant", "current_board_access", "current_section_permission"])
def test_scope_identity_and_current_revocations_cannot_be_bypassed(changed):
    grant = ArchiveReadGrant(SCOPE, "agent", "reader", ArchiveReadSections(True, True, False, True))
    request = dict(grant=grant, scope=SCOPE, actor_kind="agent", actor_id="reader", section=ArchiveSection.QA,
        current_board_access=True, current_section_permission=True)
    assert archive_section_is_readable(**request)
    if changed in ("realm_id", "board_id", "origin_kind", "origin_id"):
        request["scope"] = replace(SCOPE, **{changed: "foreign"})
    else:
        request[changed] = {"actor_id": "other", "actor_kind": "human", "grant": None,
            "current_board_access": False, "current_section_permission": False}[changed]
    assert not archive_section_is_readable(**request)


def test_current_permission_never_overrides_a_captured_denial():
    grant = ArchiveReadGrant(SCOPE, "human", "owner", ArchiveReadSections(True, True, False, True))
    assert not archive_section_is_readable(grant, scope=SCOPE, actor_kind="human", actor_id="owner",
        section=ArchiveSection.EVALUATIONS, current_board_access=True, current_section_permission=True)
    assert parse_archive_read_grant(asdict(grant)) == grant


@pytest.mark.parametrize("value", [1, "true", None, [], {}])
def test_section_decisions_are_exact_booleans(value):
    with pytest.raises(ValueError, match="decision_invalid"):
        ArchiveReadSections(True, value, False, True)


def test_unresolved_authority_or_unknown_source_flag_is_not_a_full_access_sentinel():
    with pytest.raises(TypeError, match="resolved_permissions"):
        capture_archive_read_sections(None, **AUTHORITIES)
    with pytest.raises(ValueError, match="source_authority_unknown"):
        capture_archive_read_sections(PermissionSet({}), **{**AUTHORITIES, "qa_permission": "unknown.read"})
    record = asdict(ArchiveReadGrant(SCOPE, "human", "reader", ArchiveReadSections(True, True, True, True)))
    record["admin"] = True
    with pytest.raises(ValueError, match="grant_invalid"):
        parse_archive_read_grant(record)
