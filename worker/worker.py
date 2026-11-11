"""One worker process: reap expired leases, claim, 'execute', finish.

Run with:  python -m worker.worker
Single-threaded on purpose: the heartbeat happens between slices of emulated
work, so a frozen process (SIGSTOP, GC pause, blocked syscall) stops
heartbeating too — which is exactly the zombie case we need to be fenced.
"""

import os
import random
import signal
import socket
import time

import psycopg

from app import config, db, queue
from app.logs import log

_stop = False


def _on_term(_sig, _frame):
    global _stop
    _stop = True


def process(conn: psycopg.Connection, job: queue.Claimed, worker_id: str, rng: random.Random) -> None:
    payload = job.payload or {}
    work_ms = payload.get("work_ms") or rng.uniform(config.WORK_MS_MIN, config.WORK_MS_MAX)
    end = time.monotonic() + work_ms / 1000.0

    while True:
        left = end - time.monotonic()
        if left <= 0:
            break
        time.sleep(min(left, config.HEARTBEAT_SECONDS))
        if time.monotonic() >= end:
            break
        state = queue.heartbeat(conn, job)
        if state == "lost":
            log("lease_lost", worker=worker_id, job_id=job.id, token=job.fencing_token)
            return
        if state == "cancel":
            final = queue.finish(conn, job, worker_id, "cancelled")
            log("finished", worker=worker_id, job_id=job.id, token=job.fencing_token, status=final, why="cancel_seen")
            return

    if payload.get("fail"):
        final = queue.finish(conn, job, worker_id, "failed", error="emulated failure")
    else:
        final = queue.finish(conn, job, worker_id, "succeeded", result={"worker": worker_id, "attempt": job.attempts})
    if final is None:
        log("stale_result_dropped", worker=worker_id, job_id=job.id, token=job.fencing_token)
    else:
        log("finished", worker=worker_id, job_id=job.id, token=job.fencing_token, status=final)


def main() -> None:
    signal.signal(signal.SIGTERM, _on_term)
    signal.signal(signal.SIGINT, _on_term)
    worker_id = os.environ.get("SOUP_WORKER_ID") or f"{socket.gethostname()}-{os.getpid()}"
    rng = random.Random()
    conn = db.wait_for_db()
    log("worker_started", worker=worker_id, pid=os.getpid(), mutations=sorted(config.MUTATIONS),
        skew=config.CLOCK_SKEW_S)
    last_reap = 0.0

    while not _stop:
        try:
            if time.monotonic() - last_reap >= config.REAP_SECONDS:
                last_reap = time.monotonic()
                for r in queue.reap(conn):
                    log("reaped", worker=worker_id, job_id=r["id"], new_status=r["status"], token=r["fencing_token"])
            job = queue.claim(conn, worker_id)
            if job is None:
                time.sleep(config.POLL_SECONDS)
                continue
            log("claimed", worker=worker_id, job_id=job.id, token=job.fencing_token, attempt=job.attempts)
            process(conn, job, worker_id, rng)
        except psycopg.OperationalError as e:
            log("db_error", worker=worker_id, error=repr(e))
            try:
                conn.close()
            except Exception:  # noqa: BLE001, S110 — connection already broken
                pass
            conn = db.wait_for_db()
        except Exception as e:  # noqa: BLE001 — keep the worker alive; the error is visible in the log
            log("error", worker=worker_id, error=repr(e))
            time.sleep(0.2)

    log("worker_stopped", worker=worker_id)
    conn.close()


if __name__ == "__main__":
    main()
