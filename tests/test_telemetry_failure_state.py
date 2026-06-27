"""Behavioral tests for the base failure-state schema (spec R1, card R1-A).

Covers test card 11c0ac9b / scenario ts_c5b11103 (legacy state.json without a
failure-state block migrates to safe defaults and never leaks secrets) plus the
structural secret-redaction invariant (BR br_89e39ee6 / FR fr_8ead6f5e).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from okto_pulse.core.telemetry.failure_state import (
    CONSENT_BLOCKED,
    CONSENT_GRANTED,
    CONSENT_UNKNOWN,
    FAILURE_STATE_KEY,
    PUBLIC_FAILURE_STATE_FIELDS,
    STATUS_DEGRADED,
    STATUS_UNKNOWN,
    FailureState,
    is_secret_key,
    merge,
    public_status_projection,
    read_failure_state,
    redact_secret_keys,
    write_failure_state,
)
from okto_pulse.core.telemetry.settings import load_state, save_state

# A legacy state.json exactly as described by ts_c5b11103: it carries install
# identity/token fields and next_batch_seq, but NO failure-state block.
_LEGACY_STATE = {
    "install_id": "11111111-2222-3333-4444-555555555555",
    "install_token": "super-secret-token-value",
    "token_hash": "deadbeefdeadbeef",
    "install_token_expires_at": "2026-07-01T00:00:00Z",
    "next_batch_seq": 7,
}

_SECRET_VALUES = {"super-secret-token-value", "deadbeefdeadbeef"}


def _write_legacy_state(tmp_path: Path, **extra) -> Path:
    metrics_dir = tmp_path / "metrics"
    state = {**_LEGACY_STATE, **extra}
    save_state(metrics_dir, state)
    return metrics_dir


def _assert_no_secret(obj) -> None:
    """No secret key or secret value appears anywhere in a (nested) structure."""
    blob = repr(obj)
    for value in _SECRET_VALUES:
        assert value not in blob
    if isinstance(obj, dict):
        for key in obj:
            assert not is_secret_key(key), f"secret key leaked: {key}"


def test_legacy_state_without_failure_block_migrates_to_safe_defaults(tmp_path: Path) -> None:
    """ts_c5b11103 — legacy state migrates with safe defaults, no secrets exposed."""
    metrics_dir = _write_legacy_state(tmp_path)

    state = load_state(metrics_dir)
    # The stored state still carries the token (it is needed to publish); only
    # the projection must omit it.
    assert state["install_token"] == "super-secret-token-value"
    assert FAILURE_STATE_KEY not in state

    fs = read_failure_state(state)
    # Safe, actionable defaults.
    assert fs.status == STATUS_UNKNOWN
    assert fs.reason_code is None
    assert fs.http_status is None
    assert fs.last_success_at is None
    assert fs.last_failure_at is None
    assert fs.next_retry_at is None
    assert fs.retry_count == 0
    assert fs.recovered_at is None
    # No recorded mode => consent unknown and publishing not assumed.
    assert fs.consent_state == CONSENT_UNKNOWN
    assert fs.publish_enabled is False

    projection = public_status_projection(state)
    # Exactly the 10 allowlisted fields, and no secret key/value.
    assert set(projection) == set(PUBLIC_FAILURE_STATE_FIELDS)
    _assert_no_secret(projection)


def test_legacy_anonymous_beacon_migrates_to_granted_consent(tmp_path: Path) -> None:
    """A legacy opted-in install migrates to granted consent + publish enabled,
    seeding last_success_at from the legacy last_send_at field."""
    metrics_dir = _write_legacy_state(
        tmp_path, mode="anonymous_beacon", last_send_at="2026-06-12T20:15:00Z"
    )

    fs = read_failure_state(load_state(metrics_dir))
    assert fs.consent_state == CONSENT_GRANTED
    assert fs.publish_enabled is True
    assert fs.status == STATUS_UNKNOWN
    assert fs.last_success_at == "2026-06-12T20:15:00Z"  # seeded from legacy field


def test_legacy_disabled_mode_migrates_to_blocked_consent(tmp_path: Path) -> None:
    metrics_dir = _write_legacy_state(tmp_path, mode="disabled")
    fs = read_failure_state(load_state(metrics_dir))
    assert fs.consent_state == CONSENT_BLOCKED
    assert fs.publish_enabled is False


def test_public_projection_is_allowlisted_even_with_injected_secret(tmp_path: Path) -> None:
    """A secret accidentally written into the failure_state block is dropped on
    read — the projection is allowlist-based, so it is structurally secret-free."""
    metrics_dir = tmp_path / "metrics"
    save_state(
        metrics_dir,
        {
            "mode": "anonymous_beacon",
            FAILURE_STATE_KEY: {
                "status": STATUS_DEGRADED,
                "reason_code": "USAGE_503",
                "retry_count": 2,
                # Hostile/buggy extra keys that must NOT survive a read:
                "install_token": "leaked-token",
                "token_hash": "leaked-hash",
            },
        },
    )

    state = load_state(metrics_dir)
    fs = read_failure_state(state)
    assert fs.status == STATUS_DEGRADED
    assert fs.reason_code == "USAGE_503"
    assert fs.retry_count == 2

    projection = public_status_projection(state)
    assert set(projection) == set(PUBLIC_FAILURE_STATE_FIELDS)
    assert "install_token" not in projection
    assert "token_hash" not in projection
    assert "leaked-token" not in repr(projection)


def test_write_failure_state_preserves_other_keys_and_omits_secrets() -> None:
    # R-P2-08: the FS persistence roundtrip is a Community concern; the core keeps
    # the PURE projection — write_failure_state must not disturb other state keys.
    state = {**_LEGACY_STATE, "mode": "anonymous_beacon"}

    fs = merge(
        read_failure_state(state),
        status=STATUS_DEGRADED,
        reason_code="USAGE_500",
        http_status=500,
        last_failure_at="2026-06-15T17:00:00Z",
        next_retry_at="2026-06-15T17:15:00Z",
        retry_count=1,
    )
    updated = write_failure_state(state, fs)

    # Round-trips identically through the pure projection.
    assert read_failure_state(updated) == fs

    # Other keys preserved (token still there for publishing).
    assert updated["install_token"] == "super-secret-token-value"
    assert updated["next_batch_seq"] == 7
    # The persisted failure_state block carries only allowlisted, non-secret keys.
    block = updated[FAILURE_STATE_KEY]
    assert set(block) == set(PUBLIC_FAILURE_STATE_FIELDS)
    _assert_no_secret(block)


def test_is_secret_key_covers_credentials_not_timestamps() -> None:
    for secret in ("install_token", "token_hash", "signature", "token", "refresh_token"):
        assert is_secret_key(secret) is True
    for safe in (
        "install_token_expires_at",
        "install_id",
        "next_batch_seq",
        "last_send_at",
        "status",
        "reason_code",
    ):
        assert is_secret_key(safe) is False


def test_redact_secret_keys_strips_credentials_keeps_safe_fields() -> None:
    redacted = redact_secret_keys({**_LEGACY_STATE, "mode": "anonymous_beacon"})
    assert "install_token" not in redacted
    assert "token_hash" not in redacted
    assert redacted["install_token_expires_at"] == "2026-07-01T00:00:00Z"
    assert redacted["install_id"] == _LEGACY_STATE["install_id"]
    _assert_no_secret(redacted)


def test_merge_validates_status_and_consent() -> None:
    base = FailureState()
    ok = merge(base, status=STATUS_DEGRADED, retry_count=3)
    assert ok.status == STATUS_DEGRADED and ok.retry_count == 3
    assert base.retry_count == 0  # frozen: original untouched

    with pytest.raises(ValueError):
        merge(base, status="not-a-real-status")
    with pytest.raises(ValueError):
        merge(base, consent_state="maybe")


def test_write_failure_state_is_pure_and_allowlisted() -> None:
    state = {"install_token": "secret", "mode": "anonymous_beacon"}
    out = write_failure_state(state, FailureState(status=STATUS_UNKNOWN))
    # original untouched (pure)
    assert FAILURE_STATE_KEY not in state
    # block is allowlisted
    assert set(out[FAILURE_STATE_KEY]) == set(PUBLIC_FAILURE_STATE_FIELDS)
    # untouched sibling keys preserved
    assert out["install_token"] == "secret"
