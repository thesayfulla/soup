"""Production-mode entry point: run N workers, respawn any that die.

    python -m worker.supervisor --workers 4
"""

import argparse
import signal
import time

from app import db
from app.logs import log
from worker.pool import WorkerPool

_stop = False


def _on_term(_sig, _frame):
    global _stop
    _stop = True


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    args = ap.parse_args()
    signal.signal(signal.SIGTERM, _on_term)
    signal.signal(signal.SIGINT, _on_term)

    with db.wait_for_db() as conn:
        db.apply_migrations(conn)

    pool = WorkerPool(args.workers)
    pool.start()
    log("supervisor_started", workers=args.workers)
    while not _stop:
        for slot in pool.respawn_dead():
            log("worker_respawned", slot=slot)
        time.sleep(1.0)
    pool.stop()
    log("supervisor_stopped")


if __name__ == "__main__":
    main()
