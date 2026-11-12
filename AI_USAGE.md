# AI usage

## What was generated
The initial scaffold was generated with Claude Code (Opus) from my task brief and a stack/design plan I
approved:
- schema, `app/queue.py` (claim / heartbeat / finish / reap / cancel SQL), API, worker, worker pool
- the chaos harness, the invariant checks, the mutation flags
- the pytest race tests, the locustfile, the first drafts of README / DESIGN / RESULTS

## Where the tool was wrong (caught while running it)
1. **The harness was blind to a fencing bug.** The first `no_fencing` mutation run **passed**
   (`runs/dev-iterations/20261111T081452Z_no_fencing_s2`). With 20–120 ms jobs, a frozen zombie only woke
   up after its replacement had already finished, so it never got a chance to overwrite. Fix: a zombie
   scenario with jobs longer than the freeze gap (`--work-ms-min 500 --work-ms-max 2500`). Then
   `no_fencing` fails `effect_written_by_current_lease` (`runs/dev-iterations/20261111T081848Z_no_fencing_s3`).
2. **A mutation run that didn't test what it claimed.** `naive_complete` drops the UNIQUE index on
   `side_effects`. The API's startup migration (`CREATE UNIQUE INDEX IF NOT EXISTS`) quietly recreated it, so
   the duplicates showed up as `UniqueViolation` errors in the worker log instead of as duplicate effects
   (`runs/dev-iterations/20261111T081528Z_naive_complete_s2`). Fix: drop the index after the API is up.

3. **The "retry after timeout" scenario barely happened.** The first version gave the first POST a 0.5 ms
   httpx read timeout. On its own that fires every time (checked: 20/20), but inside the harness it fired
   only 2 times in about 500 (`runs/20261111T082405Z_baseline_s1001`, `post_ReadTimeout: 2`). Under load the
   response was already buffered by the time the client coroutine read it. Fix: send the raw request on a
   socket and close it without reading. Then 50/50 dropped requests committed, and every retry got a 200 replay
   (`runs/dev-iterations/*_baseline_s7`).

4. **Two harness runs on one database silently corrupted each other.** My run and a run I started by hand at the
   same moment both reset `soup_chaos` (`runs/20261111T120902Z_baseline_s3002`: 1000 "lost" jobs, 400
   "unexpected"). Fix: the harness holds a Postgres advisory lock for the whole run and refuses to start
   without it. Verified by holding the lock and checking that a run refuses.
5. **The first DB-crash run didn't test what it claimed.** Both crashes happened after all POSTs had finished
   (`…_baseline_s4001`, zero HTTP errors), so the API was never under an outage. Re-ran with crashes every
   6 s overlapping the POSTs (`…_baseline_s4002`).
6. **The AI misread a number.** It first described the 10x backlog as "275 new jobs/s arriving". That was
   actually the *growth* rate. Arrivals were about 400/s. Corrected in RESULTS.md. Check every number against the files.

## Fixes made after the first review of weak spots
- `idle_in_transaction_session_timeout=10s` on every connection (a worker frozen mid-transaction can't hold a
  row lock forever), with a test and a control test.
- The API pool validates connections on checkout, so it survives a Postgres restart.
- Harness: Postgres crash injection (`--db-crash-every`), retries with backoff, the advisory-lock guard,
  `chaos/claim_ceiling.py` for raw claim throughput.

## Second session (2026-11-12): explanation and restoring lost write-ups
Also Claude Code (Opus):
- Walked me through the codebase (endpoints, decisions, worker lifecycle, zombie test, Locust) and translated it
  into Uzbek for my own preparation. That file is not part of the submission.
- `DESIGN.md`, `RESULTS.md` and this file were never committed and got deleted locally. Claude rebuilt them
  by replaying its own edits from the first session's transcript, then re-checked the key numbers against
  `runs/` (10x backlog peak 23,768, 1,295 cancel races and 46,673 `claimed` lines in the 50k run, 5,681 in s4002,
  2 jobs failed by the reaper in s3003). The first restore of `DESIGN.md` was made from an earlier version and
  was missing two weak spots (retry budget, ambiguous commit). They were added back once the final version was found.

## What I verified / changed / redid myself
- [ ] Read every SQL statement in `app/queue.py` and can explain its locking under READ COMMITTED
- [ ] Re-ran the chaos harness myself (list the runs)
- [ ] Rewrote DESIGN.md in my own words
- [ ] …
