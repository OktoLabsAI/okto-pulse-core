"""Public Board membership policy preserves owner, scoped-agent and share gates."""

import pytest

from okto_pulse.core.ports.permission_policy import board_membership_allows_read


@pytest.mark.parametrize("owner,share,verified,roles,allowed", [
    ("reader", None, False, ("admin",), True),
    ("other", "viewer", False, None, True),
    ("other", "viewer", False, ("editor", "admin"), False),
    ("other", "editor", False, ("editor", "admin"), True),
    ("other", "admin", False, ("editor", "admin"), True),
    ("other", None, False, None, False),
    ("other", "viewer", False, (), False),
    ("other", None, True, ("admin",), True),
    ("other", None, 1, None, False),
    ("other", None, "true", None, False),
])
def test_membership_is_not_a_role_wildcard_and_share_restrictions_are_preserved(owner, share, verified, roles, allowed):
    assert board_membership_allows_read(owner_id=owner, actor_id="reader", share_permission=share,
        verified_agent_board_access=verified, allowed_share_permissions=roles) is allowed
