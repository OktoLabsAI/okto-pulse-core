# Internal Event Bus

The `core/events/` module is an **in-process outbox-pattern event bus** used by
okto-pulse services to react to domain changes (cards moved, specs versioned,
sprints closed, etc.) without cluttering `services/main.py` with cross-cutting
effects. Publishers emit typed `DomainEvent` instances atomically with their
data change; the `EventDispatcher` worker drains the outbox and invokes
registered handlers asynchronously.

Today's only subscriber is `ConsolidationEnqueuer`, which replaces the ad-hoc
`db.add(ConsolidationQueue(...))` calls that used to live scattered across the
services. Tomorrow's handlers (activity log, notifications, webhooks, metrics)
plug in exactly the same way — one decorator, one `handle()` method.

## Flow at a glance

```
┌──────────────────────────────────────────────────────────────────┐
│ Publisher side (services/main.py)                                │
│                                                                  │
│   async def create_card(...):                                    │
│       self.db.add(card)                                          │
│       await self.db.flush()                                      │
│       await event_publish(                                       │
│           CardCreated(board_id=..., card_id=card.id, ...),       │
│           session=self.db,                                       │
│       )                                                          │
│       # FastAPI request tx commits everything atomically          │
└──────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
          ┌─────────────────────────────────────────┐
          │  domain_events                          │   append-only
          │  domain_event_handler_executions        │   1 row per handler
          └─────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────┐
│ Dispatcher side (asyncio worker, same process)                   │
│                                                                  │
│   while running:                                                 │
│       SELECT pending executions ORDER BY occurred_at, id LIMIT 50│
│       for each: status=processing → handler.handle(event,session)│
│                 on success status=done                           │
│                 on exception attempts+=1, backoff or DLQ         │
│       wait asyncio.Event OR poll_timeout=5s                      │
└──────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
                    Handlers (isolated tx per execution)
                    ├── ConsolidationEnqueuer → ConsolidationQueue
                    ├── ActivityLogHandler       [phase 2+]
                    ├── MentionsNotifier         [phase 2+]
                    └── ...
```

## The 12 event types (MVP)

| Event | Publisher | Key payload fields |
|-------|-----------|--------------------|
| `card.created` | `CardService.create_card` | `card_id`, `spec_id`, `sprint_id`, `card_type`, `priority` |
| `card.moved` | `CardService.move_card` | `card_id`, `from_status`, `to_status` |
| `card.cancelled` | `CardService.move_card` (→ cancelled) | `card_id`, `previous_status` |
| `card.restored` | `CardService.move_card` (from cancelled) | `card_id`, `to_status` |
| `spec.created` | `SpecService.create_spec` | `spec_id`, `source`, `origin_id` |
| `spec.moved` | `SpecService.move_spec` | `spec_id`, `from_status`, `to_status` |
| `spec.version_bumped` | `SpecService.update_spec` | `spec_id`, `old_version`, `new_version`, `changed_fields` |
| `sprint.created` | `SprintService.create_sprint` | `sprint_id`, `spec_id` |
| `sprint.moved` | `SprintService.move_sprint` | `sprint_id`, `from_status`, `to_status` |
| `sprint.closed` | `SprintService.move_sprint` (→ closed) | `sprint_id` |
| `ideation.derived_to_spec` | `IdeationService.derive_spec` | `ideation_id`, `spec_id` |
| `refinement.derived_to_spec` | `RefinementService.derive_spec` | `refinement_id`, `spec_id` |

Every event also carries the common base fields: `event_id` (UUID),
`board_id`, `actor_id`, `actor_type`, `occurred_at` (UTC). These live in
dedicated columns on `domain_events` and are NOT duplicated inside
`payload_json`.

## Adding a new handler

Three steps — no wiring outside the module.

**1. Write the handler class.**

```python
# src/okto_pulse/core/events/handlers/notify_mentions.py
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.events.bus import register_handler
from okto_pulse.core.events.types import DomainEvent


@register_handler("card.created", "card.moved")
class NotifyMentionsHandler:
    async def handle(self, event: DomainEvent, session: AsyncSession) -> None:
        # Your side-effect here. Raises are retried up to MAX_ATTEMPTS
        # (5) with exponential backoff, then moved to status='dlq'.
        ...
```

Conventions:

- The decorator accepts N event_type strings; register once per event.
- `handle(event, session)` is async. The session is scoped to this
  handler's execution — writes commit together with the status update.
- Never call `session.commit()` or `session.rollback()` yourself — the
  dispatcher owns the transaction.
- Raise to trigger retry. Unrecoverable errors should raise a custom
  exception you log explicitly; after 5 failed attempts the row lands
  in the DLQ.

**2. Export from `handlers/__init__.py`.**

```python
# src/okto_pulse/core/events/handlers/__init__.py
from okto_pulse.core.events.handlers.notify_mentions import NotifyMentionsHandler  # noqa: F401
```

Importing the package at app startup triggers `@register_handler` so the
registry is populated BEFORE the dispatcher begins draining.

**3. That's it.** No service changes, no route changes. The handler
starts processing on the next drain cycle.

## Observability (SQL)

```sql
-- 1. Drain lag per event_type
SELECT e.event_type, COUNT(*) AS pending
FROM domain_event_handler_executions x
JOIN domain_events e ON e.id = x.event_id
WHERE x.status = 'pending'
GROUP BY e.event_type
ORDER BY pending DESC;

-- 2. DLQ inspection — events that tripped MAX_ATTEMPTS
SELECT
    x.handler_name,
    e.event_type,
    x.attempts,
    x.last_error,
    x.processed_at AS dlq_at,
    e.board_id,
    e.occurred_at
FROM domain_event_handler_executions x
JOIN domain_events e ON e.id = x.event_id
WHERE x.status = 'dlq'
ORDER BY x.processed_at DESC;

-- 3. Replay a handler for a period (e.g. reprocess the last day)
UPDATE domain_event_handler_executions
SET status = 'pending',
    attempts = 0,
    next_attempt_at = NULL,
    last_error = NULL
WHERE handler_name = 'ConsolidationEnqueuer'
  AND event_id IN (
      SELECT id FROM domain_events
      WHERE occurred_at > datetime('now', '-1 day')
  );

-- 4. Audit the history of a specific entity
SELECT event_type, occurred_at, actor_id, payload_json
FROM domain_events
WHERE board_id = 'BOARD_ID'
  AND (
      json_extract(payload_json, '$.card_id') = 'CARD_ID'
      OR json_extract(payload_json, '$.spec_id') = 'CARD_ID'
      OR json_extract(payload_json, '$.sprint_id') = 'CARD_ID'
  )
ORDER BY occurred_at ASC;
```

## Retention

The MVP does not run an automatic cleanup worker. Events accumulate
indefinitely; in practice `domain_event_handler_executions` is the hot
table while `domain_events` grows linearly with activity. For manual
purge:

```sql
-- Purge executions older than 90 days that completed successfully
DELETE FROM domain_event_handler_executions
WHERE status = 'done'
  AND processed_at < datetime('now', '-90 days');

-- Purge orphan event rows (no executions left)
DELETE FROM domain_events
WHERE id NOT IN (
    SELECT event_id FROM domain_event_handler_executions
);
```

DLQ rows are NEVER purged by this script — drain them manually after
investigating the root cause.

## Troubleshooting

### Events are stuck in `processing`

A dispatcher crash mid-handler leaves executions in `status='processing'`.
The dispatcher recovers automatically on startup via a `UPDATE … SET
status='pending'` statement that runs BEFORE the drain loop. If you
suspect the dispatcher never restarted, check the app logs for
`EventDispatcher started` and force a restart.

### DLQ is growing

The handler is raising on every retry. Inspect via the DLQ query above,
look at `last_error`, fix the root cause (code or data), then either:

- Replay the DLQ rows with the "replay a handler" SQL above (swapping
  the `event_id` filter for specific rows).
- Mark them as failed intentionally:
  `UPDATE … SET status='done', processed_at = datetime('now') WHERE …`.

### A handler isn't running

Double-check:

1. The handler class is decorated with `@register_handler("event.type")`.
2. It is imported from `handlers/__init__.py`.
3. `core/events/__init__.py` was imported at app startup (side effect
   of `from okto_pulse.core import events as _events` in `core/app.py`).
4. `EventBus._registry["event.type"]` contains your class at runtime.

### Latency is high

The dispatcher polls at `POLL_INTERVAL_SECONDS=5.0` as a fallback, but
every `publish()` sets the module-level `asyncio.Event` so the worker
wakes within milliseconds in the common case. If you're seeing multi-
second delays, you probably:

- Have many handlers in the registry, each taking >100ms to run.
- Are running on a thread that doesn't share the same event loop as
  the dispatcher — check that all publishes happen inside the FastAPI
  lifespan.

## Constants (dispatcher)

All live in `core/events/dispatcher.py` as module-level constants:

| Constant | Value | Why |
|----------|-------|-----|
| `MAX_ATTEMPTS` | 5 | Conservative upper bound before DLQ. |
| `BACKOFF_BASE` | 2 | Exponential multiplier: 2/4/8/16 s. |
| `BACKOFF_CAP_SECONDS` | 300 | Cap prevents 17-minute retries. |
| `DRAIN_BATCH_SIZE` | 50 | Balance throughput vs tx size. |
| `POLL_INTERVAL_SECONDS` | 5.0 | Fallback when wake signal is missed. |

These are intentionally hardcoded in the MVP — extract to config only
when you have evidence one of them matters in production.

## What's NOT here (fase 2+)

- **ActivityLogHandler** — migrate the `activity_log` table to be
  driven by the bus.
- **MentionsNotificationHandler** — scan `@mentions` in payloads and
  fan out notifications.
- **MetricsCounterHandler** — Prometheus counter per `event_type`.
- **WebhookDispatchHandler** — outbound POST to configured URLs.
- **GlobalLayerSyncHandler** — mirror events in the ecosystem edition
  meta-graph.
- **Automatic retention cleanup worker** — currently SQL as shown above.
- **Dashboard / UI** — SQL is the only observability surface in MVP.
- **Multi-instance coordination** (FOR UPDATE SKIP LOCKED) — SQLite
  single-writer makes this unnecessary today.

The architecture is intentionally minimal so adding any of the above
is a handler file + a row in `handlers/__init__.py`. No infrastructure
changes required.
