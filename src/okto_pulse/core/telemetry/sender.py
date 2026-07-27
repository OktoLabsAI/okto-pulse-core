"""Telemetry sender module — R10-E Pass 2 FULFILLED.

``TelemetryBeaconSender`` and its helpers (``sign_payload`` /
``get_or_create_install_id`` / ``install_id_path`` / backoff / circuit / HMAC /
token-lifecycle / watermark) have been REMOVED from core. The concrete
``requests``-based HTTP beacon sender is now owned exclusively by the Community
edition:
``okto_pulse.community.adapters.telemetry_sender.CommunityTelemetryBeaconSender``.

Community owns its own copies of all helpers (absorbed in R10-E Pass 1).
The beacon lifecycle (``community.main._metrics_beacon_loop``) obtains the sender
through the ``sender_registry.get_telemetry_sender`` factory.
"""
