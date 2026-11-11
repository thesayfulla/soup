"""Chaos harness.

    python -m chaos.run                         # spec run: 10k jobs, 8 workers, kill -9 every ~2s
    python -m chaos.run --freeze-every 3        # + zombies (SIGSTOP past the lease, then SIGCONT)
    python -m chaos.run --mutation no_fencing --freeze-every 3   # must FAIL
    python -m chaos.run --runs 5                # repeat with different seeds

It starts its own API (uvicorn) and its own worker pool against a separate
database (soup_chaos), so nothing else touches the data while it runs.
Every run writes runs/<ts>_<mutation>_s<seed>/ with harness.log, workers.log,
api.log and report.json, and appends one line to runs/index.tsv — pass or fail.
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import json
import os
import random
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx
import psycopg

from app import db
from chaos.invariants import ClientObservations, run_checks, stats
from worker.pool import ROOT, WorkerPool

_RUN_LOCK = 7_070_707
DEFAULT_CHAOS_URL = os.environ.get("CHAOS_DATABASE_URL", "postgresql://soup:soup@localhost:55432/soup_chaos")


class Tee:
    def __init__(self, path: Path):
        self.f = path.open("a", buffering=1)
        self.t0 = time.monotonic()

    def __call__(self, msg: str) -> None:
        line = f"[{time.monotonic() - self.t0:8.2f}s] {msg}"
        print(line, flush=True)
        self.f.write(line + "\n")


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--jobs", type=int, default=10_000)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--kill-every", type=float, default=2.0, help="seconds between kill -9 (0 = off)")
    ap.add_argument("--kill-jitter", type=float, default=0.5)
    ap.add_argument("--freeze-every", type=float, default=0.0, help="seconds between SIGSTOP zombies (0 = off)")
    ap.add_argument("--freeze-for", type=float, default=None, help="default: 2*lease + 1")
    ap.add_argument("--db-crash-every", type=float, default=0.0,
                    help="seconds between Postgres crashes (0 = off). Run the harness on the host for this.")
    ap.add_argument("--db-crash-cmd", default="docker kill -s KILL soup-postgres-1 && docker start soup-postgres-1",
                    help="shell command that crashes and restarts Postgres")
    ap.add_argument("--dup-ratio", type=float, default=0.3, help="share of keys POSTed again (1-3 extra times)")
    ap.add_argument("--retry-ratio", type=float, default=0.05,
                    help="share of keys whose first POST times out client-side, then retried")
    ap.add_argument("--conflict-ratio", type=float, default=0.01,
                    help="share of keys re-sent with a different body (expect 409)")
    ap.add_argument("--cancel-ratio", type=float, default=0.1)
    ap.add_argument("--fail-ratio", type=float, default=0.02, help="share of jobs whose emulated work fails")
    ap.add_argument("--concurrency", type=int, default=64)
    ap.add_argument("--lease", type=float, default=5.0)
    ap.add_argument("--heartbeat", type=float, default=1.0)
    ap.add_argument("--work-ms-min", type=float, default=20)
    ap.add_argument("--work-ms-max", type=float, default=120)
    ap.add_argument("--mutation", default="none", help="comma-separated, see app/config.py")
    ap.add_argument("--skew", type=float, default=0.0,
                    help="clock offset for worker slot 0 (only matters with local_clock)")
    ap.add_argument("--timeout", type=float, default=900.0, help="max seconds to wait for the queue to drain")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--port", type=int, default=8100)
    ap.add_argument("--db", default=DEFAULT_CHAOS_URL)
    ap.add_argument("--out", default=str(ROOT / "runs"))
    return ap.parse_args(argv)


async def wait_healthy(client: httpx.AsyncClient, timeout=30.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if (await client.get("/healthz")).status_code == 200:
                return
        except httpx.TransportError:
            pass
        await asyncio.sleep(0.2)
    raise RuntimeError("API did not become healthy")


async def chaos_run(args, seed: int, run_dir: Path) -> bool:
    out = Tee(run_dir / "harness.log")
    rng = random.Random(seed)
    freeze_for = args.freeze_for if args.freeze_for is not None else 2 * args.lease + 1
    out(f"config {json.dumps({**vars(args), 'seed': seed, 'freeze_for': freeze_for})}")

    # --- fresh database -----------------------------------------------------
    # Two harness runs on the same database silently wipe each other's data
    # (this happened: runs/20261111T120902Z_baseline_s3002). Hold a session
    # advisory lock for the whole run and refuse to start if someone else has it.
    db.ensure_database(args.db)
    lock_conn = db.connect(args.db)
    if not lock_conn.execute("SELECT pg_try_advisory_lock(%s) AS ok", (_RUN_LOCK,)).fetchone()["ok"]:
        lock_conn.close()
        raise RuntimeError(f"another chaos run holds the lock on {args.db}; refusing to reset its data")
    try:
        return await _chaos_run_locked(args, seed, run_dir, out, rng, freeze_for)
    finally:
        lock_conn.close()  # releases the advisory lock


async def _chaos_run_locked(args, seed, run_dir, out, rng, freeze_for) -> bool:
    with db.connect(args.db) as conn:
        db.reset_schema(conn)

    env = {
        "DATABASE_URL": args.db,
        "SOUP_MUTATION": args.mutation,
        "SOUP_LEASE_SECONDS": str(args.lease),
        "SOUP_HEARTBEAT_SECONDS": str(args.heartbeat),
        "SOUP_WORK_MS_MIN": str(args.work_ms_min),
        "SOUP_WORK_MS_MAX": str(args.work_ms_max),
        "PYTHONUNBUFFERED": "1",
    }

    # --- API + workers --------------------------------------------------------
    api_log = (run_dir / "api.log").open("a")
    api = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.api:app", "--host", "127.0.0.1", "--port", str(args.port),
         "--log-level", "warning"],
        cwd=ROOT, env={**os.environ, **env}, stdout=api_log, stderr=subprocess.STDOUT,
    )
    workers_log = (run_dir / "workers.log").open("a", buffering=1)
    slot_env = {0: {"SOUP_CLOCK_SKEW_S": str(args.skew)}} if args.skew else {}
    pool = WorkerPool(args.workers, env=env, slot_env=slot_env, log_file=workers_log)

    obs = ClientObservations()
    db_crashes = [0]
    http = collections.Counter()
    keys = [f"s{seed}-{i}" for i in range(args.jobs)]
    t_start = time.monotonic()

    limits = httpx.Limits(max_connections=args.concurrency + 16, max_keepalive_connections=args.concurrency + 16)
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{args.port}", timeout=15.0, limits=limits) as client:
        try:
            await wait_healthy(client)
            if "naive_complete" in args.mutation:
                # Must happen AFTER the API started: its startup migration
                # (CREATE UNIQUE INDEX IF NOT EXISTS) would silently re-create it.
                with db.connect(args.db) as conn:
                    conn.execute("DROP INDEX side_effects_job_uniq")
                out("mutation naive_complete: dropped UNIQUE index on side_effects.job_id")
            pool.start()
            out(f"api up, {args.workers} workers started: {[pool.worker_id(s) for s in range(args.workers)]}")

            stop_chaos = asyncio.Event()
            side_tasks: set[asyncio.Task] = set()
            cancel_budget = int(args.jobs * args.cancel_ratio)
            cancel_keys = set(rng.sample(keys, int(cancel_budget * 0.7)))  # rest: cancel jobs seen running
            conflict_keys = set(rng.sample(keys, int(args.jobs * args.conflict_ratio)))
            retry_keys = set(rng.sample(keys, int(args.jobs * args.retry_ratio)))
            payloads = {k: {"i": i, "fail": rng.random() < args.fail_ratio} for i, k in enumerate(keys)}

            # ---- request plan: primaries, concurrent dups (same slot), late dups
            plan = []
            for i, k in enumerate(keys):
                plan.append((i, 0, k, "primary"))
                if rng.random() < args.dup_ratio:
                    for _ in range(rng.randint(1, 3)):
                        if rng.random() < 0.5:
                            plan.append((i, 1, k, "dup_concurrent"))
                        else:
                            plan.append((i + rng.uniform(1, 500), 1, k, "dup_late"))
            plan.sort(key=lambda r: (r[0], r[1]))
            http["planned_posts"] = len(plan)

            def backoff(attempt: int) -> float:
                return min(1.0, 0.05 * 2 ** attempt)

            def spawn(coro):
                t = asyncio.create_task(coro)
                side_tasks.add(t)
                t.add_done_callback(side_tasks.discard)

            async def cancel(job_id: str, delay: float, why: str):
                await asyncio.sleep(delay)
                for attempt in range(20):
                    try:
                        r = await client.post(f"/jobs/{job_id}/cancel")
                    except httpx.TransportError:
                        http["cancel_transport_error"] += 1
                        await asyncio.sleep(backoff(attempt))
                        continue
                    http[f"cancel_{why}_{r.status_code}"] += 1
                    if r.status_code in (200, 202):
                        obs.cancel_accepted.add(job_id)
                    if r.status_code < 500:
                        return
                    await asyncio.sleep(backoff(attempt))

            async def conflict(key: str):
                body = {"payload": {**payloads[key], "conflict": True}}
                for attempt in range(20):
                    try:
                        r = await client.post("/jobs", json=body, headers={"Idempotency-Key": key})
                    except httpx.TransportError:
                        await asyncio.sleep(backoff(attempt))
                        continue
                    if r.status_code >= 500:
                        http["conflict_5xx"] += 1
                        await asyncio.sleep(backoff(attempt))
                        continue
                    obs.conflict_statuses[key] = r.status_code
                    return

            async def post_and_drop(key: str, body: dict):
                """Send a complete request, then close the socket without reading the
                response: the server commits, the client never learns the result.
                (A tiny httpx read timeout does NOT do this reliably — under load the
                response is already buffered when the client reads, so it never fires.)"""
                data = json.dumps(body).encode()
                head = (f"POST /jobs HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n"
                        f"Idempotency-Key: {key}\r\nContent-Length: {len(data)}\r\nConnection: close\r\n\r\n")
                try:
                    _, w = await asyncio.open_connection("127.0.0.1", args.port)
                    w.write(head.encode() + data)
                    await w.drain()
                    w.close()
                    await w.wait_closed()
                    http["post_response_dropped"] += 1
                except OSError:
                    http["post_drop_failed"] += 1

            async def post(key: str, kind: str):
                body = {"payload": payloads[key]}
                if kind == "primary" and key in retry_keys:
                    await post_and_drop(key, body)
                    await asyncio.sleep(rng.uniform(0, 0.05))
                    kind = "retry_after_drop"  # expect 200 (replay) if the dropped one committed first
                for attempt in range(20):
                    try:
                        r = await client.post("/jobs", json=body, headers={"Idempotency-Key": key})
                    except httpx.TransportError as e:
                        http[f"post_{type(e).__name__}"] += 1
                        await asyncio.sleep(backoff(attempt))
                        continue
                    http[f"post_{kind}_{r.status_code}"] += 1
                    if r.status_code >= 500:
                        await asyncio.sleep(backoff(attempt))
                        continue
                    if r.status_code in (200, 201):
                        job_id = r.json()["id"]
                        obs.ids_by_key.setdefault(key, set()).add(job_id)
                        if kind in ("primary", "retry_after_drop"):
                            if key in cancel_keys:
                                spawn(cancel(job_id, rng.uniform(0, 3.0), "scheduled"))
                            if key in conflict_keys:
                                spawn(conflict(key))
                    return
                http["post_gave_up"] += 1

            q: asyncio.Queue = asyncio.Queue()
            for item in plan:
                q.put_nowait(item)

            async def consumer():
                while True:
                    try:
                        _, _, key, kind = q.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    await post(key, kind)

            async def killer():
                if args.kill_every <= 0:
                    return
                while not stop_chaos.is_set():
                    await asyncio.sleep(max(0.05, args.kill_every + rng.uniform(-args.kill_jitter, args.kill_jitter)))
                    if stop_chaos.is_set():
                        return
                    r = pool.kill_random(rng)
                    if r:
                        out(f"kill -9 slot={r[0]} pid={r[1]} worker={r[2]} (total {pool.kills})")

            async def freezer():
                if args.freeze_every <= 0:
                    return
                while not stop_chaos.is_set():
                    await asyncio.sleep(args.freeze_every)
                    if stop_chaos.is_set():
                        return
                    r = pool.freeze(rng, freeze_for)
                    if r:
                        out(f"SIGSTOP slot={r[0]} worker={r[1]} for {freeze_for:.1f}s (total {pool.freezes})")

            async def running_canceller():
                """Cancel jobs we observe in 'running' to exercise the cancel/complete race."""
                budget = cancel_budget - len(cancel_keys)
                while budget > 0 and not stop_chaos.is_set():
                    await asyncio.sleep(0.2)
                    try:
                        r = await client.get("/jobs", params={"status": "running", "limit": 10})
                    except httpx.TransportError:
                        continue
                    if r.status_code != 200:
                        continue
                    for j in r.json()["items"][:3]:
                        if budget <= 0:
                            break
                        budget -= 1
                        spawn(cancel(j["id"], 0, "running"))

            def counts() -> dict | None:
                try:
                    with db.connect(args.db) as c:
                        return {r["status"]: r["n"] for r in c.execute(
                            "SELECT status, count(*) AS n FROM jobs GROUP BY status").fetchall()}
                except psycopg.OperationalError:
                    return None  # DB down (crash scenario)

            async def db_crasher():
                if args.db_crash_every <= 0:
                    return
                while not stop_chaos.is_set():
                    await asyncio.sleep(args.db_crash_every)
                    if stop_chaos.is_set():
                        return
                    t0 = time.monotonic()
                    proc = await asyncio.create_subprocess_shell(
                        args.db_crash_cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
                    _, err = await proc.communicate()
                    db_crashes[0] += 1
                    out(f"POSTGRES CRASH #{db_crashes[0]} rc={proc.returncode} "
                        f"{err.decode().strip()[:200]} ({time.monotonic() - t0:.1f}s)")

            async def progress():
                while not stop_chaos.is_set():
                    await asyncio.sleep(5)
                    for slot in pool.respawn_dead():
                        out(f"worker slot {slot} exited on its own; respawned")
                    out(f"progress {await asyncio.to_thread(counts)} posts_left={q.qsize()} "
                        f"kills={pool.kills} freezes={pool.freezes}")

            chaos_tasks = [asyncio.create_task(t())
                           for t in (killer, freezer, running_canceller, progress, db_crasher)]
            await asyncio.gather(*(consumer() for _ in range(args.concurrency)))
            out(f"all POSTs done in {time.monotonic() - t_start:.1f}s; waiting for cancels/conflicts")
            while side_tasks:
                await asyncio.gather(*list(side_tasks))

            # ---- drain (chaos keeps going while the queue drains)
            deadline = time.monotonic() + args.timeout
            drained = False
            while time.monotonic() < deadline:
                c = await asyncio.to_thread(counts)
                if c is not None and c.get("queued", 0) + c.get("running", 0) == 0:
                    drained = True
                    break
                await asyncio.sleep(1)
            out(f"drained={drained} after {time.monotonic() - t_start:.1f}s")
            stop_chaos.set()
            for t in chaos_tasks:
                t.cancel()
            await asyncio.gather(*chaos_tasks, return_exceptions=True)
        finally:
            pool.stop()
            api.terminate()
            try:
                api.wait(10)
            except subprocess.TimeoutExpired:
                api.kill()
            workers_log.close()
            api_log.close()

    # --- verdict --------------------------------------------------------------
    with db.wait_for_db(args.db) as conn:
        checks = run_checks(conn, obs, keys)
        st = stats(conn)
    events = collections.Counter()
    for line in (run_dir / "workers.log").read_text().splitlines():
        try:
            events[json.loads(line)["event"]] += 1
        except (ValueError, KeyError):
            events["unparsed_line"] += 1

    ok = all(c.ok for c in checks)
    out("=" * 72)
    for c in checks:
        out(f"{'PASS' if c.ok else 'FAIL'}  {c.name:<40} violations={c.count}"
            + (f"  sample={c.sample}" if not c.ok else ""))
    out("-" * 72)
    out(f"stats   {json.dumps(st, default=str)}")
    out(f"http    {json.dumps(dict(sorted(http.items())))}")
    out(f"events  {json.dumps(dict(sorted(events.items())))}")
    out(f"chaos   kills={pool.kills} freezes={pool.freezes} db_crashes={db_crashes[0]} "
        f"spawns={pool.spawns} "
        f"elapsed={time.monotonic() - t_start:.1f}s")
    out(f"RESULT  {'PASS' if ok else 'FAIL'}  mutation={args.mutation} seed={seed}")

    report = {
        "ok": ok, "seed": seed, "args": vars(args), "freeze_for": freeze_for,
        "checks": [c.__dict__ for c in checks], "stats": st, "http": dict(http), "events": dict(events),
        "kills": pool.kills, "freezes": pool.freezes, "db_crashes": db_crashes[0], "elapsed_s": round(time.monotonic() - t_start, 2),
    }
    (run_dir / "report.json").write_text(json.dumps(report, indent=2, default=str))
    return ok


def main(argv=None) -> int:
    args = parse_args(argv)
    out_root = Path(args.out)
    out_root.mkdir(parents=True, exist_ok=True)
    index = out_root / "index.tsv"
    if not index.exists():
        index.write_text("started_utc\trun_dir\tmutation\tseed\tjobs\tworkers\tkill_every\tfreeze_every\tresult\tfailed_checks\n")
    results = []
    for n in range(args.runs):
        seed = args.seed + n if args.seed is not None else random.SystemRandom().randrange(1_000_000)
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        tag = args.mutation.replace(",", "+") if args.mutation != "none" else "baseline"
        run_dir = out_root / f"{ts}_{tag}_s{seed}"
        run_dir.mkdir(parents=True)
        try:
            ok = asyncio.run(chaos_run(args, seed, run_dir))
            report = json.loads((run_dir / "report.json").read_text())
            failed = ",".join(c["name"] for c in report["checks"] if not c["ok"]) or "-"
        except Exception as e:  # noqa: BLE001 — a crashed harness run is still a recorded run
            ok, failed = False, f"HARNESS_ERROR:{e!r}"
            (run_dir / "harness_error.txt").write_text(repr(e))
            print(f"harness error: {e!r}", file=sys.stderr)
        with index.open("a") as f:
            f.write(f"{ts}\t{run_dir.name}\t{args.mutation}\t{seed}\t{args.jobs}\t{args.workers}\t"
                    f"{args.kill_every}\t{args.freeze_every}\t{'PASS' if ok else 'FAIL'}\t{failed}\n")
        results.append(ok)
    print(f"\n{sum(results)}/{len(results)} runs passed")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
