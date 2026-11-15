"""Edition-facing graph write failures and acknowledgement ownership fence.

Adapters may translate the typed failure and require the existing Core-owned
global writer token before SQL acknowledgement. This surface grants neither
writer acquisition nor ownership transfer.
"""

from okto_pulse.core.kg.guarded_write import GuardedWriteError

__all__ = ["GuardedWriteError", "assert_global_discovery_writer_fence"]


def assert_global_discovery_writer_fence() -> None:
    """Fail closed unless the current global writer still owns its token."""
    from okto_pulse.core.kg.global_discovery_writer import (
        assert_global_discovery_writer_fence as assert_owned,
    )

    assert_owned()
