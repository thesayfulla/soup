"""Deterministic tests for each silent-failure case. Each test forces the
interleaving explicitly instead of hoping a race happens."""

import threading

import pytest

from app import queue


def expire_lease(conn, job_id):
    conn.execute("UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = %s", (job_id,))


def effects(conn, job_id):
    return conn.execute("SELECT * FROM side_effects WHERE job_id = %s", (job_id,)).fetchall()


def status(conn, job_id):
    return queue.get_job(conn, job_id)["status"]


# ---------------------------------------------------------------- idempotency


def test_concurrent_creates_same_key_yield_one_job(conn, connect):
    n = 16
    barrier = threading.Barrier(n)
    results = []

    def go():
        c = connect()
        barrier.wait()
        results.append(queue.create_job(c, "same-key", {"x": 1}))

    threads = [threading.Thread(target=go) for _ in range(n)]
    [t.start() for t in threads]
    [t.join() for t in threads]

    assert len({str(r[0]["id"]) for r in results}) == 1
    assert sum(created for _, created in results) == 1
    assert conn.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"] == 1


def test_same_key_different_body_is_rejected(conn):
    queue.create_job(conn, "k", {"x": 1})
    with pytest.raises(queue.IdempotencyConflict):
        queue.create_job(conn, "k", {"x": 2})


# ---------------------------------------------------------------- claiming


def test_one_job_many_concurrent_claimers_exactly_one_wins(conn, connect):
    queue.create_job(conn, "k", {})
    n = 16
    barrier = threading.Barrier(n)
    got = []

    def go(i):
        c = connect()
        barrier.wait()
        got.append(queue.claim(c, f"w{i}"))

    threads = [threading.Thread(target=go, args=(i,)) for i in range(n)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert sum(1 for g in got if g is not None) == 1


# ---------------------------------------------------------------- zombie / fencing


def test_zombie_result_is_rejected_after_lease_expired(conn):
    job, _ = queue.create_job(conn, "k", {})
    a = queue.claim(conn, "A")
    expire_lease(conn, job["id"])          # A froze past its lease
    reaped = queue.reap(conn)
    assert [r["status"] for r in reaped] == ["queued"]
    b = queue.claim(conn, "B")
    assert b.fencing_token == a.fencing_token + 1

    assert queue.heartbeat(conn, a) == "lost"                 # A wakes up
    assert queue.finish(conn, a, "A", "succeeded") is None     # stale write dropped
    assert queue.finish(conn, b, "B", "succeeded") == "succeeded"

    eff = effects(conn, job["id"])
    assert len(eff) == 1 and eff[0]["worker_id"] == "B" and eff[0]["fencing_token"] == b.fencing_token


def test_zombie_cannot_write_while_job_is_requeued(conn):
    job, _ = queue.create_job(conn, "k", {})
    a = queue.claim(conn, "A")
    expire_lease(conn, job["id"])
    queue.reap(conn)                       # back to queued, nobody owns it yet
    assert queue.finish(conn, a, "A", "succeeded") is None
    assert status(conn, job["id"]) == "queued"
    assert effects(conn, job["id"]) == []


# ---------------------------------------------------------------- cancel


def test_cancel_queued_is_immediate_and_never_runs(conn):
    job, _ = queue.create_job(conn, "k", {})
    row, outcome = queue.cancel_job(conn, job["id"])
    assert outcome == "cancelled" and row["status"] == "cancelled"
    assert queue.claim(conn, "A") is None


def test_cancel_requested_beats_completion(conn):
    job, _ = queue.create_job(conn, "k", {})
    a = queue.claim(conn, "A")
    _, outcome = queue.cancel_job(conn, job["id"])
    assert outcome == "cancel_requested"
    assert queue.finish(conn, a, "A", "succeeded") == "cancelled"
    assert effects(conn, job["id"]) == []


def test_cancel_after_completion_is_rejected(conn):
    job, _ = queue.create_job(conn, "k", {})
    a = queue.claim(conn, "A")
    queue.finish(conn, a, "A", "succeeded")
    _, outcome = queue.cancel_job(conn, job["id"])
    assert outcome == "already_terminal"
    assert status(conn, job["id"]) == "succeeded"


def _race_finish_vs_cancel(c1, c2, claimed, job_id) -> dict:
    barrier = threading.Barrier(2)
    out = {}

    def do_finish():
        barrier.wait()
        out["finish"] = queue.finish(c1, claimed, "A", "succeeded")

    def do_cancel():
        barrier.wait()
        out["cancel"] = queue.cancel_job(c2, job_id)[1]

    ts = [threading.Thread(target=do_finish), threading.Thread(target=do_cancel)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    return out


def test_cancel_and_complete_race_is_consistent(conn, connect):
    """Run the race many times for real; whichever wins, state and effects agree."""
    c1, c2 = connect(), connect()
    winners = set()
    for i in range(50):
        job, _ = queue.create_job(conn, f"k{i}", {})
        a = queue.claim(conn, "A")
        out = _race_finish_vs_cancel(c1, c2, a, job["id"])
        final = status(conn, job["id"])
        n_eff = len(effects(conn, job["id"]))
        winners.add(out["cancel"])
        if out["cancel"] == "cancel_requested":
            assert final == "cancelled" and n_eff == 0
        else:
            assert out["cancel"] == "already_terminal" and final == "succeeded" and n_eff == 1


def test_reaper_honours_pending_cancel(conn):
    job, _ = queue.create_job(conn, "k", {})
    queue.claim(conn, "A")
    queue.cancel_job(conn, job["id"])
    expire_lease(conn, job["id"])          # A died before seeing the cancel
    queue.reap(conn)
    assert status(conn, job["id"]) == "cancelled"


def test_reaper_fails_job_after_max_attempts(conn):
    job, _ = queue.create_job(conn, "k", {}, max_attempts=1)
    queue.claim(conn, "A")
    expire_lease(conn, job["id"])
    queue.reap(conn)
    assert status(conn, job["id"]) == "failed"


# ---------------------------------------------------------------- clock skew


def test_lease_uses_db_clock_not_worker_clock(conn, monkeypatch):
    """With the correct code, a worker whose clock is an hour off still writes
    a lease relative to DB time."""
    from app import config

    monkeypatch.setattr(config, "CLOCK_SKEW_S", -3600.0)
    job, _ = queue.create_job(conn, "k", {})
    queue.claim(conn, "skewed")
    row = conn.execute(
        "SELECT extract(epoch FROM lease_expires_at - now()) AS left FROM jobs WHERE id = %s", (job["id"],)
    ).fetchone()
    assert 0 < float(row["left"]) <= config.LEASE_SECONDS + 1
    assert queue.reap(conn) == []


# ---------------------------------------------------------------- pagination


def test_keyset_pagination_returns_every_job_once(conn):
    for i in range(25):
        queue.create_job(conn, f"k{i}", {})
    seen, cursor = [], None
    while True:
        rows, cursor = queue.list_jobs(conn, limit=10, cursor=cursor)
        seen += [r["idempotency_key"] for r in rows]
        if cursor is None:
            break
    assert sorted(seen) == sorted(f"k{i}" for i in range(25))
    assert len(seen) == len(set(seen))


# ---------------------------------------------------------------- frozen inside a transaction


def test_worker_frozen_inside_transaction_releases_lock(conn):
    """A worker that froze between UPDATE and COMMIT holds the row lock. Postgres'
    idle_in_transaction_session_timeout kills that session and rolls it back."""
    import time

    from app import db
    from tests.conftest import TEST_URL

    job, _ = queue.create_job(conn, "k", {})
    frozen = db.connect(TEST_URL, idle_tx_timeout_ms=500)
    frozen.execute("BEGIN")
    frozen.execute("SELECT id FROM jobs WHERE id = %s FOR UPDATE", (job["id"],))
    # ... and now the process is SIGSTOPped: no COMMIT, no ROLLBACK.

    assert queue.claim(conn, "B") is None          # row is locked, SKIP LOCKED skips it
    time.sleep(1.5)                                # server kills the idle-in-transaction session
    got = queue.claim(conn, "B")
    assert got is not None and got.id == job["id"]
    frozen.close()


def test_without_idle_timeout_frozen_transaction_blocks_forever(conn):
    """The control: with the guard disabled (0 = off) the job stays unclaimable."""
    import time

    from app import db
    from tests.conftest import TEST_URL

    queue.create_job(conn, "k", {})
    frozen = db.connect(TEST_URL, idle_tx_timeout_ms=0)
    frozen.execute("BEGIN")
    frozen.execute("SELECT id FROM jobs FOR UPDATE")
    time.sleep(1.5)
    assert queue.claim(conn, "B") is None
    frozen.close()
