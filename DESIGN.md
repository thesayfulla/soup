# Design note

> Draft generated with AI assistance from the implemented code. Review it and rewrite it in your own words
> before you submit: you will have to defend every sentence in the live discussion.

**Storage: PostgreSQL.** With 8 worker processes plus an API all writing at once, I need row-level locks,
`FOR UPDATE SKIP LOCKED` for contention-free claiming, MVCC so reads don't block the claim path, and a single
authoritative clock (`now()`). SQLite gives one writer at a time across the whole file, has no `SKIP LOCKED`, and
its file locking through Docker volumes is a correctness risk of its own. SQLite would be simpler to run, but every
claim would serialize on the database lock, and the test would be of SQLite's lock, not of my design.

**How a worker claims a job.** One statement, one transaction (READ COMMITTED):

```sql
WITH next AS (SELECT id FROM jobs WHERE status='queued' ORDER BY created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED)
UPDATE jobs j SET status='running', lease_owner=$worker, lease_expires_at=now()+$lease,
       fencing_token=j.fencing_token+1, attempts=j.attempts+1
FROM next WHERE j.id=next.id RETURNING ...
```

The row lock guarantees that only one transaction can turn a given row from `queued` to `running`, and other
workers skip locked rows rather than waiting. In the same transaction the worker appends a row to `executions`,
so every attempt that *started* is recorded even if the worker dies a millisecond later.

**How I detect a dead worker.** By the lease, not by process liveness. A worker extends `lease_expires_at`
(DB time) with a heartbeat while it works. Every worker also runs a reaper:
`status='running' AND lease_expires_at < now()` → back to `queued`, or to `cancelled` if a cancel is pending, or
to `failed` if `attempts >= max_attempts`. A `kill -9`ed worker and a frozen worker look the same to the system.
Both stop heartbeating, and the job becomes available again after at most one lease (5 s). The cost is that a
job held by a dead worker waits up to 5 s before anyone else can take it.

**Zombies: fencing tokens.** Each claim increments `fencing_token`. Every write a worker makes (heartbeat,
finish) is `WHERE id=$id AND fencing_token=$mine AND status='running'`. A worker that comes back after losing
its lease updates zero rows. It learns it was fenced and drops its result (`stale_result_dropped` /
`lease_lost` in the logs). The clock never takes part in this decision, only the token does.

**Cancelling a running job.** `queued` → `cancelled` immediately (200). For a `running` job the API sets
`cancel_requested=true` (202). The worker sees the flag on its next heartbeat and finishes as `cancelled`. In
any case, `finish()` evaluates `CASE WHEN cancel_requested THEN 'cancelled'` under the row lock, so **an
accepted cancel always ends as `cancelled`, with no side effect**, and a cancel that arrives after
completion gets 409. If the owner is dead, the reaper turns a pending cancel into `cancelled`. Limitation: we
don't preempt work. A running job keeps burning compute until its next heartbeat.

**Guarantee: at-least-once execution, exactly-once effect.** A job can *run* more than once: a worker killed
mid-job is retried, and a frozen one may still be running when its replacement starts. The *effect* (the
`side_effects` row) is inserted in the **same transaction** as the fenced `running → succeeded` update, so
it commits if and only if the winning attempt commits. A `UNIQUE(job_id)` index stops a second effect even
if the state machine has a bug. This is honest only because the effect lives in the same database. A real
external effect (writing a checkpoint to S3, calling a GPU cluster) can't take part in that transaction.
It would drop to at-least-once and would need an idempotent consumer (for example, write to a path
derived from `job_id + fencing_token` and have readers accept only the token recorded in the DB).

**Idempotent create.** `INSERT … ON CONFLICT (idempotency_key) DO NOTHING RETURNING *`, then `SELECT`. A
concurrent insert with the same key blocks until the first one commits, so both callers get the same id. I
store a hash of the request body, so the same key with a different body is a 409, not a silent replay.

**Clock.** Every lease comparison uses Postgres `now()`, so worker clock skew doesn't matter. The
`local_clock` mutation shows what happens when it does. The DB clock jumping (failover to a replica with a different
clock, or an NTP step) still matters, and I don't defend against it.

**Weak spots I know about.**
- A worker frozen *inside* a transaction (between the claim's `UPDATE` and `COMMIT`) holds the row lock, and the
  reaper skips locked rows. Every connection now sets `idle_in_transaction_session_timeout` (10 s,
  `SOUP_IDLE_TX_TIMEOUT_MS`), so Postgres kills that session, rolls the claim back and releases the lock
  (`test_worker_frozen_inside_transaction_releases_lock`; the control test
  `test_without_idle_timeout_frozen_transaction_blocks_forever` shows the job stuck without it). The cost: such a job
  waits up to 10 s, not 5 s, before anyone else can take it.
- Workers poll (50 ms when idle). Under heavy load the claim query and the polling interval, not the API, set the throughput limit.
- Keyset pagination on `created_at` can miss a row whose transaction began before, but committed after, a page
  boundary. That's harmless here (inserts are single short statements) but not in general.
- Postgres is a single point of failure. There's no failover story.
- The heartbeat runs between slices of emulated work. Real blocking work would need a heartbeat thread, and then
  "heartbeat alive, work thread hung" becomes a new zombie class that fencing doesn't detect (the lease never expires).
