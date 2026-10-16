"""Explicit native grants for tests that need a closed permission tree."""
from okto_pulse.core.ports.permission_policy import (
    flatten_permission_flags, registered_permission_flags, set_permission_flag,
)


def native_permission_flags(*grants: str) -> dict:
    flags = registered_permission_flags()
    for leaf in flatten_permission_flags(flags):
        set_permission_flag(flags, leaf, False)
    for grant in grants:
        set_permission_flag(flags, grant, True)
    return flags
