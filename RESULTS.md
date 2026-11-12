# Results

## 1. Spec chaos run

10,000 jobs, 8 workers, `kill -9` of a random worker every 2 s ± 0.5 s, all running at once:
- ~30% of keys POSTed 1–3 extra times (half at the same moment, half later)
- 5% of first POSTs time out on the client side after sending and are retried
- 1% re-sent with a different body
- ~10% of jobs cancelled (70% at a random time, 30% picked while observed `running`)
- 2% of jobs fail on purpose

| Run (`runs/…`) | Where | Result | kill -9 | Jobs run twice | Reaped | Dropped-response retries deduplicated | Cancels hitting `running` (202) | Wall time |
|---|---|---|---|---|---|---|---|---|
| `20261111T082405Z_baseline_s1001` | local | **PASS** 13/13 | 46 | 27 | 44 | n/a (see note) | 236 | 96 s |
| `20261111T082757Z_baseline_s2001` | docker compose | **PASS** 13/13 | 44 | 27 | 41 | 500/500 (500× 200 replay) | 257 | 92 s |
| `20261111T082929Z_baseline_s2002` | docker compose | **PASS** 13/13 | 45 | 30 | 42 | 500/500 (499× 200, 1× 201: the retry won the race) | 240 | 94 s |

Final states, s2001: 9,117 succeeded · 178 failed (emulated) · 705 cancelled. 9,349 executions and
9,117 side effects: exactly one effect per succeeded job, none for the rest.

Note on s1001: its "retry after timeout" traffic didn't do what it was meant to (only 2 timeouts in about
500 attempts). See AI_USAGE.md. The harness was fixed before s2001/s2002.

**Failed and invalid harness runs (kept in `runs/index.tsv`):**
- `…083103Z_baseline_s3001`: `HARNESS_ERROR: failed to resolve host 'postgres'`. The machine slept during the
  run and the Docker VM's DNS was gone when it woke up. No conclusion about the queue.
- `…120902Z_baseline_s3002`: `no_lost_jobs` (1000) and `no_unexpected_jobs` (400) failed. A second harness run
  (`…120938Z_baseline_s306232`) started 36 s later on the same `soup_chaos` database and reset it. Neither run
  says anything about the queue. They found a bug in the harness, which now holds a Postgres advisory lock
  and refuses to start while another run has it.

**Zombie baseline** (`--jobs 1000 --freeze-every 2 --freeze-for 7 --work-ms-min 500 --work-ms-max 2500`, docker):

| Run | Result | kill -9 | SIGSTOP 7 s | Executions | Runs per job | Reaped | Stale results dropped | Wall time |
|---|---|---|---|---|---|---|---|---|
| `20261111T121549Z_baseline_s3003` | **PASS** 13/13 | 189 | 191 | 1,286 for 1,000 jobs | 1×669, 2×196, 3×49, 4×12, 5×6 | 359 | 176 (+2 `lease_lost`) | 383 s |

Final states: 903 succeeded · 81 cancelled · 14 failed (emulated) · **2 failed with "lease expired; max attempts
reached"**. Those 2 jobs were healthy and lost 5 leases in a row to chaos. That's the designed behavior, but at
this chaos rate `max_attempts=5` turns infrastructure failures into user-visible job failures. Retries caused by
infrastructure probably shouldn't count against the job's own retry budget.

**Postgres crash runs** (harness on the host, `--db-crash-every N`). The crash is
`docker kill -s KILL soup-postgres-1 && docker start soup-postgres-1`: a real crash followed by WAL recovery,
not a clean shutdown. Worker `kill -9` every ~2 s at the same time.

| Run | Jobs | DB crashes | Overlapping POSTs? | Result | HTTP errors seen by client | Worker `db_error`s |
|---|---|---|---|---|---|---|
| `…_baseline_s4001` | 3,000 | 2 every 15 s | **no**: POSTs finished at 14 s, first crash at 16 s, so it doesn't test the API | PASS | 0 | 15 |
| `…_baseline_s4002` | 6,000 | 10 every 6 s | yes (6 during POSTs) | **PASS** 13/13 | 1× 500, 1× ReadError on POST, 1× 500 on cancel, all retried | 80 |

In s4002, `executions` = 5,682 but `claimed` log lines = 5,681. One claim committed, but the crash ate
the acknowledgement, so the worker never knew it held the job. The lease expired and the job was reaped and
rerun. That is the ambiguous-commit case, and it was handled correctly.
Changes that made this work: the API pool validates connections on checkout (`check_connection`), and harness
clients retry 5xx and transport errors with capped exponential backoff (about 15 s total).

**Largest run: 50,000 jobs** (docker, spec settings, `…123752Z_baseline_s5001`): **PASS** 13/13. 217 kill -9s,
2,500/2,500 dropped-response retries deduplicated, 1,295 cancels raced against running jobs, 152 jobs ran
twice, 212 reaps. 45,446 succeeded = 45,446 side effects. 475 s wall time, of which the invariant SQL took 38 s.
Here too `executions` (46,674) = `claimed` lines + 1, with no DB crash in this run: a worker was `kill -9`ed
after its claim committed and before it printed the log line. Execution rows are the source of truth; logs are not.

## 2. Can the harness actually see bugs? (mutation runs)

Each mutation switches off one safeguard. A harness that still passes is blind to that bug.

| Mutation | Scenario | Result | Caught by |
|---|---|---|---|
| (none, control) | + SIGSTOP zombies, 0.5–2.5 s jobs, 1000 jobs | PASS ✔ | 176 stale results correctly dropped (`…121549Z_baseline_s3003`) |
| `no_fencing` | kill -9 only, 20–120 ms jobs | **PASS: harness blind** | nothing (zombies woke up after the replacement had finished) |
| `no_fencing` | + SIGSTOP zombies, 0.5–2.5 s jobs | FAIL ✔ | `effect_written_by_current_lease` (24) |
| `racy_claim` | + zombies, short jobs | FAIL ✔ | `no_double_claim` (161), 182 stale results dropped |
| `naive_complete` | first attempt (index silently re-created) | invalid run | showed up only as `UniqueViolation` in workers.log |
| `naive_complete` | + zombies, long jobs | FAIL ✔ | `at_most_one_effect` (37), `accepted_cancel_ends_cancelled` (2), … |
| `local_clock`, skew +30 s on one worker | 600 long jobs, no kills | **PASS** (correct!) | nothing broken, but 1,411 executions for 600 jobs, 860 reaps, 568 `lease_lost`: the fast clock reaps everyone else's live leases |
| `local_clock,no_fencing`, skew +30 s | same | FAIL ✔ | `effect_written_by_current_lease` (357) |

Clock skew takeaway: **with fencing, skew is a performance bug, not a correctness bug.** A skewed lease decision
makes jobs run more often, and the fence still allows only the current owner to commit. Without the fence the
same skew silently commits stale results.

Takeaway: **`kill -9` alone never produces a zombie.** A killed process can't write anything afterwards,
so the spec scenario can't detect a missing fence. You need `SIGSTOP`/`SIGCONT` and jobs longer than the gap
between a zombie waking up and its replacement finishing.

## 3. Silent failures: how each was checked

| Failure | Defense | Deterministic test | Chaos evidence |
|---|---|---|---|
| Double claim | `FOR UPDATE SKIP LOCKED` in one UPDATE | `test_one_job_many_concurrent_claimers_exactly_one_wins` | `no_double_claim`; `racy_claim` mutation is caught |
| Zombie writes a stale result | fencing token on every write | `test_zombie_result_is_rejected_after_lease_expired`, `test_zombie_cannot_write_while_job_is_requeued` | `effect_written_by_current_lease`; `no_fencing` mutation is caught |
| Cancel/complete race | `CASE WHEN cancel_requested` under the row lock in `finish()` | `test_cancel_and_complete_race_is_consistent` (50 real races) | `accepted_cancel_ends_cancelled`, `succeeded_never_cancel_requested` |
| Duplicate on request retry | `ON CONFLICT DO NOTHING` + request hash | `test_concurrent_creates_same_key_yield_one_job` | `one_id_per_key`, `conflicting_body_rejected` |
| Isolation level | READ COMMITTED + explicit row locks; every decision is one locked statement | every test runs at the default READ COMMITTED | all chaos runs |
| Clock skew | only DB `now()` is compared | `test_lease_uses_db_clock_not_worker_clock` | `local_clock` mutation: see §2 |
| Frozen inside a transaction | `idle_in_transaction_session_timeout=10s` | `test_worker_frozen_inside_transaction_releases_lock` + control | not reachable by random SIGSTOP |
| DB crash / ambiguous commit | every write fenced; lease expiry recovers unknown outcomes | | s4002: 10 crashes, PASS |

**Why READ COMMITTED is enough here.** Every state change is a single `UPDATE … WHERE <precondition>`. Postgres
takes the row lock and then *re-checks the WHERE clause against the latest committed row version* (EvalPlanQual).
So a precondition such as `status='running' AND fencing_token=$mine` is evaluated on current data, never on a
stale snapshot. No code path reads a value and writes it back in a separate statement. The `racy_claim`
mutation is exactly that pattern, and it is caught. REPEATABLE READ / SERIALIZABLE would turn these conflicts
into `serialization_failure` errors that need retry loops. That's more failure paths and no extra safety.
The one read-then-write in the code, `create_job`'s `INSERT ON CONFLICT` followed by `SELECT`, *depends* on
READ COMMITTED (each statement in autocommit): the `SELECT` takes a fresh snapshot and sees the competing
insert. Wrapped in one REPEATABLE READ transaction, the `INSERT ON CONFLICT` itself would raise
`could not serialize access` whenever the conflicting row committed after the snapshot was taken.
(I'm relying on the Postgres docs for this; there's no test for it.)

## 4. What the harness does NOT catch
- Postgres **failover** to a replica: crash and restart of the same instance *is* tested now (s4002), but
  promotion to a replica with asynchronous replication can lose committed transactions, and nothing here covers that
- A network partition between a worker and the DB (the worker keeps running but can't fence or heartbeat)
- A worker frozen *inside* an open transaction: the chaos harness can't aim a SIGSTOP that precisely. It's covered
  by a deterministic test instead (fixed with `idle_in_transaction_session_timeout`).
- Bugs that need hours or millions of jobs (index bloat, vacuum, `fencing_token` growth). 50k jobs is the largest run.
- Anything about real external side effects: here the effect is in the same DB transaction
- Throughput or latency problems: correctness only. See the load test.
- Kill timing is random, not adversarial. Specific interleavings are covered by the deterministic tests instead.

## 5. Verdict at 100 RPS: **SHIP** (with the conditions below)

All load runs are on one 12-core laptop: API, 4 workers, Postgres and Locust share the CPU.

| Test (`runs/…`) | Achieved | Failures | p50 / p95 / p99 | Queue depth |
|---|---|---|---|---|
| `load_100rps` (your run, ~38 s) | 93 RPS | 0 | 2 / 5 / 14 ms | not sampled |
| `load_100rps_v2` (2 min) | **97.9 RPS** | **0** | 2 / 6 / 9 ms | **flat, 0–6 queued** (`load_100rps_v2_queue_depth.tsv`) |
| `load_1000rps` (10x, 90 s) | **942 RPS** | **0** | 2 / 14 / 46 ms | **grows ~275/s, 23,768 queued** after 90 s (`load_1000rps_queue_depth.tsv`) |
| `claim_ceiling.tsv` (preloaded, ~0 ms work) | 8 workers: 2,625 jobs/s · 16: 3,154 · 32: 3,473 | | | |

- **100 RPS** is about 40–50 new jobs/s. The API is far from its limit, the queue stays empty, and the claim path has
  ~50x headroom. Combined with every chaos run passing (kills, zombies, DB crashes, 50k jobs): **SHIP**.
- **What breaks first at 10x:** *worker capacity*, not the API or the queue. 4 workers × ~14 jobs/s
  (70 ms emulated work) can't keep up with ~400 new jobs/s (475 POST/s, ~20% of them replays). The backlog grows with no errors at all:
  every response is 2xx and fast, and jobs just wait longer. That is a silent failure, and only queue-depth monitoring shows it.
  Adding workers fixes it up to the claim ceiling (~3k jobs/s here). Beyond that, all workers compete for
  the head of the same index, and Postgres is a single node.

**Conditions for shipping (what I'd fix first, in order):**
1. **Alert on queue depth and oldest-queued age.** Today nothing signals the 10x backlog. Autoscale workers on it.
2. **Alert on "lease expired; max attempts reached"** failures and on the reap rate. A spike means workers are
   dying or freezing, or a poison-pill job.
3. **Retention:** finished jobs, `executions` and `side_effects` grow forever. The invariant SQL already took 38 s at 50k jobs.
4. **Postgres HA** with synchronous replication. A crash and restart is survived (s4002), but failover isn't tested.

## 6. What surprised me / what still concerns me
> **DRAFT written by the AI from the run data. Rewrite it in your own voice.** Soup asked for *your*
> paragraph, and they'll probe it in the live discussion.

What surprised me most is how easily a green harness lies. My first fencing-bug run passed because `kill -9`
can't create a zombie: a dead process writes nothing, so the spec's own chaos scenario can't see a missing
fence. Two more of my "tests" weren't doing what I thought. A mutation's dropped index was quietly re-created
by a startup migration, and a 0.5 ms client timeout fired 2 times in 500 because under load the response was
already buffered. In each case the fix only came from checking the counters, not the PASS line. The second
surprise was that clock skew, with fencing on, only costs throughput (1,411 executions for 600 jobs) and never
correctness. What still concerns me: (1) at 10x the system fails silently, with every request 2xx and fast
while the backlog grows by ~275 jobs a second; (2) the retry budget can't tell a poison-pill job from bad
luck, so heavy infrastructure churn turns healthy jobs into `failed`; (3) the exactly-once effect depends on
the effect living in the same Postgres transaction, which a real training job's output won't; and
(4) everything ran on one laptop with one Postgres, so failover and network partitions are untested.
