"""Claim-path throughput ceiling: preload N queued jobs with SQL, start W workers
with ~zero work, time the drain. Isolates claim+finish cost from the API.

    python -m chaos.claim_ceiling --jobs 30000 --workers 8 16 32
"""

import argparse
import os
import time

from app import db
from worker.pool import WorkerPool

URL = os.environ.get("CHAOS_DATABASE_URL", "postgresql://soup:soup@localhost:55432/soup_chaos")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jobs", type=int, default=30000)
    ap.add_argument("--workers", type=int, nargs="+", default=[8, 16, 32])
    args = ap.parse_args()
    db.ensure_database(URL)
    print("workers\tjobs\tseconds\tjobs_per_s")
    for w in args.workers:
        with db.connect(URL) as c:
            db.reset_schema(c)
            c.execute("""INSERT INTO jobs (idempotency_key, request_hash, payload)
                         SELECT 'c' || g, 'h', '{"work_ms": 0.001}'::jsonb FROM generate_series(1, %s) g""",
                      (args.jobs,))
            c.execute("ANALYZE jobs")
        log = open(os.devnull, "w")
        pool = WorkerPool(w, env={"DATABASE_URL": URL, "SOUP_POLL_SECONDS": "0.01"}, log_file=log)
        t0 = time.monotonic()
        pool.start()
        with db.connect(URL) as c:
            while c.execute("SELECT count(*) AS n FROM jobs WHERE status IN ('queued','running')").fetchone()["n"]:
                time.sleep(0.2)
        dt = time.monotonic() - t0
        pool.stop()
        print(f"{w}\t{args.jobs}\t{dt:.1f}\t{args.jobs / dt:.0f}", flush=True)


if __name__ == "__main__":
    main()
