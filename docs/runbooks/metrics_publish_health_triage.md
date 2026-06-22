# Metrics Publish Health — triage runbook (R5C-F)

**Use this FIRST.** Before opening AWS CLI, CloudWatch, or raw logs to ask "are
metrics flowing?", read the Metrics Publish Health surface. It is the cheap,
secret-free first triage: it tells you whether *this install's* publishing is
healthy and, when it is not, what the next action is.

**Guardrail — health does NOT replace CloudWatch.** The publish-health surface
reports the *client's* view (did this install publish, and what does each source
signal say). It is NOT the system of record for ingest volume, Firehose health,
or Athena freshness — those are CloudWatch/Athena. A source that is absent,
unwired, or expired is reported as `degraded` / `unavailable` / `stale`, **never
`healthy`** (a healthy local send does not prove AWS ingested — see R5C-C). So
"health is green" is a reason to *stop escalating*; "health is not green" tells
you *where* to escalate, not the full root cause.

## How to read it

- **MCP (agents):** call the `okto_pulse_get_publish_health` tool. No parameters;
  it is install-local, not board-scoped. This is the agent-facing twin of the
  endpoint (agents have no token/port).
- **REST (UI/operator):** `GET /metrics/publish-health` (community mounts it under
  `/api/v1/metrics/publish-health`). A `503` body with
  `error: HEALTH_SOURCE_UNAVAILABLE` means no source could be read.
- The response is the **redacted** DTO only (R5C-E): `redaction_applied` is always
  true, it never carries a secret, and the only install identifier is
  `install_id_redacted` (an `iid_…` hash).

### Reading order

Read the fields in this order — stop as soon as the state + reason answer your
question:

1. `status` and `source` — the headline (and which source(s) drove it).
2. `severity` — `none` / `info` / `warning` / `critical`.
3. `reason_code` (raw backend code) and `reason_category` (the classified bucket);
   `http_status` when the failure came from an HTTP response.
4. `freshness` — local `age_seconds` / `is_stale`; plus the per-source freshness
   in `sources[]` (`local`, `install_lifecycle`, `aws_ingest`, `report_athena`).
5. `last_success_at`, `last_failure_at`, `next_retry_at`, `retry_count` — when it
   last worked, last failed, and when it retries.
6. `message` — the bounded, actionable next-step text.
7. `install_id_redacted` — to correlate across logs WITHOUT a raw id.

## State interpretation matrix

| status | meaning | next action | escalate? | expected evidence |
| --- | --- | --- | --- | --- |
| `healthy` | last publish succeeded and is fresh | none | no — stop here | recent `last_success_at`, `freshness.is_stale=false`, all `sources` healthy |
| `recovering` | succeeded after a prior failure | watch the next cycles | no | recent `last_success_at` after an earlier `last_failure_at`, low/reset `retry_count` |
| `disabled` | telemetry off / consent not granted | enable anonymous telemetry to resume | no | `status=disabled`, `reason_category=disabled`, `message` says publishing is off, `redaction_applied=true` |
| `degraded` | transient failure OR an observability gap (AWS/report unwired) | read `reason_code` / `reason_category`; let the scheduled retry run | only if it persists past `next_retry_at` | `next_retry_at` set, `retry_count>=1`, or `source_gap` on `aws_ingest`/`report_athena` |
| `stale` | last success is old, or a source has not advanced | check the stale source's pipeline | yes if local-stale persists; for `stale_ingest`/`stale_report` go to CloudWatch/Athena | `freshness.is_stale=true` or `reason_category in {stale_ingest, stale_report}` |
| `failing` | actionable integrity/auth failure | manual action — this will NOT auto-recover | yes — investigate immediately | `reason_code=INVALID_SIGNATURE`, `severity=critical` |
| `unavailable` | no publish outcome yet, or a required source could not be read | confirm the install handshaked / the source is reachable | yes if a required source is down | `status=unavailable`, the `install_lifecycle` source not established, or the 503 `HEALTH_SOURCE_UNAVAILABLE` error body (an error body, NOT a success-DTO field) |

### Baseline vs incident (core standalone)

In a **core standalone** build there is no real AWS ingest / Athena report adapter
(R5C-C), so `aws_ingest` and `report_athena` are classified `source_gap` and the
overall `status` is `degraded`. **This is the EXPECTED BASELINE, not an incident**
— it is the honest "AWS freshness not confirmed from this client" signal, by
design (we never report `healthy` by proxy). Distinguish it from a real failure:

- **Baseline (no action):** `status=degraded`, `reason_category=source_gap` on
  `aws_ingest`/`report_athena`, while `local` and `install_lifecycle` are
  `healthy`. The local client published fine; only the AWS visibility is unwired.
- **Real local failure (act):** `status=degraded`/`failing`/`stale` with a LOCAL
  `reason_code` (e.g. `USAGE_503`, `UNKNOWN_INSTALL`, `TOKEN_EXPIRED`,
  `INVALID_SIGNATURE`) on the `local`/`install_lifecycle` source — the client
  itself is not publishing. Triage by reason below.

## Per-source triage (each source → a different action)

| source | a non-healthy signal means | next action |
| --- | --- | --- |
| `local` | the client's last publish failed (`USAGE_5…` transport, `INVALID_SIGNATURE`, etc.) | triage by `reason_code` below; let backoff retry, escalate on `failing` |
| `install_lifecycle` | the install handshake/token (e.g. `TOKEN_EXPIRED`, never handshaked) | none if `TOKEN_EXPIRED` (auto re-handshake); confirm handshake if `unavailable` |
| `aws_ingest` | AWS ingest gap / staleness (`source_gap`, `stale_ingest`) | baseline if unwired; else CloudWatch Firehose/ingest |
| `report_athena` | Athena report gap / staleness (`source_gap`, `stale_report`) | baseline if unwired; else re-run report after ingest is fresh |

## Reason / source interpretation

| reason_code / category | what happened | next action |
| --- | --- | --- |
| `UNKNOWN_INSTALL` | server does not recognize the install | none — the next cycle re-handshakes; escalate only if it persists |
| `TOKEN_EXPIRED` | install token expired and was dropped | none — re-handshake is automatic |
| `INVALID_SIGNATURE` | request signature rejected (integrity/auth) | escalate — check signing key / clock skew; will not auto-recover |
| transport / `5xx` (`USAGE_5…`, `USAGE_NETWORK`) | ingest endpoint returned a server/transport error | let the backoff retry run; escalate to CloudWatch if sustained |
| `disabled` | publishing turned off | enable telemetry to resume |
| `stale_ingest` (`aws_ingest`) | AWS ingest source has not advanced | go to CloudWatch Firehose / ingest alarms |
| `stale_report` (`report_athena`) | Athena report source is stale | re-run the report once ingest is fresh |
| `source_gap` / `source_unavailable` / `source_expired` (`aws_ingest`/`report_athena`) | the AWS summary / report adapter is unwired or its window expired | this is a visibility gap, NOT a healthy signal — confirm the downstream pipeline; never read as healthy |

## When to escalate (and when NOT to)

- **Do NOT escalate** when `status` is `healthy`, `recovering`, or `disabled`, or
  when a `degraded`/`stale` state has a scheduled `next_retry_at` that has not yet
  passed (transient, self-healing).
- **Escalate to CloudWatch / Athena** when `reason_category` is `stale_ingest` or
  `stale_report`, or when AWS/report sources are `unavailable`/`source_gap` and you
  need to confirm ingest/report health — health only tells you the *source* is not
  confirmed, CloudWatch tells you *why*.
- **Escalate immediately (manual)** when `status=failing` /
  `reason_code=INVALID_SIGNATURE` (auth/integrity — no auto-recovery), or when a
  required source is `unavailable` and publishing is blocked.
- **Reach for raw logs / AWS CLI only after** the health surface narrows the
  source and state — it is the triage filter, not a substitute for the system of
  record. When you do open CloudWatch / AWS CLI, **correlate only by
  `install_id_redacted` (the `iid_…` value)** — never by a token, signature, full
  nonce, sensitive payload, or a raw install id.

## Security guardrail

Never collect, log, or expose `install_token`, `token_hash`, `signature`, a full
`nonce`, sensitive payload, or a raw `install_id` while triaging. The health
surface is already redacted (R5C-E); the only identifier you correlate on is
`install_id_redacted`. Do not work around the redaction to fetch a raw value —
the redacted projection is sufficient for triage.

## Reference

- DTO + endpoint + MCP tool: R5C-A (`okto_pulse_get_publish_health`,
  `GET /metrics/publish-health`).
- State / reason classification: R5C-B.
- Real source composition (local / install_lifecycle / aws_ingest / report_athena)
  and the never-healthy-by-proxy rule: R5C-C.
- Redaction guardrail (this runbook references only the redacted DTO fields): R5C-E.
- UI panel: R5C-D (Metrics Health panel).
- Producer/consumer boundary: `docs/architecture/telemetry_r5a_r5c_boundary.md`.
