"""Queue state machine. Every SQL statement that matters for correctness lives here.

    queued ──claim──▶ running ──finish──▶ succeeded | failed | cancelled
       ▲                 │
       └────reap─────────┘   (lease expired; or → cancelled / failed, see reap())
    queued ──cancel──▶ cancelled

Rules:
  * Time comparisons use the database clock (now()), never the worker's clock.
  * Every worker write (heartbeat, finish) is fenced: WHERE fencing_token = <mine>
    AND status = 'running'. Zero rows updated means "you no longer own this job".
  * The side effect is inserted in the same transaction as the fenced transition
    to 'succeeded', and side_effects.job_id is UNIQUE.
"""

from __future__ import annotations

import base64
import hashlib
import json
import time
import uuid
from dataclasses import dataclass

import psycopg
from psycopg.types.json import Jsonb

from app import config

TERMINAL = ("succeeded", "failed", "cancelled")


class IdempotencyConflict(Exception):
    """Same Idempotency-Key, different request body."""

    def __init__(self, existing: dict):
        super().__init__("idempotency key reused with a different request")
        self.existing = existing


@dataclass
class Claimed:
    id: uuid.UUID
    fencing_token: int
    attempts: int
    payload: dict


def _m(name: str) -> bool:
    return name in config.MUTATIONS


def request_hash(payload: dict, max_attempts: int) -> str:
    canon = json.dumps({"payload": payload, "max_attempts": max_attempts}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canon.encode()).hexdigest()


# ---------------------------------------------------------------- API side


def create_job(conn: psycopg.Connection, key: str, payload: dict, max_attempts: int = 5) -> tuple[dict, bool]:
    """Returns (job, created). Safe under concurrent requests with the same key:
    ON CONFLICT DO NOTHING waits for the competing insert to commit, then the
    follow-up SELECT (new snapshot under READ COMMITTED) sees it."""
    h = request_hash(payload, max_attempts)
    row = conn.execute(
        """
        INSERT INTO jobs (idempotency_key, request_hash, payload, max_attempts)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING *
        """,
        (key, h, Jsonb(payload), max_attempts),
    ).fetchone()
    if row is not None:
        return row, True
    row = conn.execute("SELECT * FROM jobs WHERE idempotency_key = %s", (key,)).fetchone()
    if row is None:  # would mean the conflicting row vanished; we never delete jobs
        raise RuntimeError(f"idempotency key {key!r} conflicted but row not found")
    if row["request_hash"] != h:
        raise IdempotencyConflict(row)
    return row, False


def get_job(conn: psycopg.Connection, job_id: uuid.UUID) -> dict | None:
    return conn.execute("SELECT * FROM jobs WHERE id = %s", (job_id,)).fetchone()


def encode_cursor(row: dict) -> str:
    raw = json.dumps([row["created_at"].isoformat(), str(row["id"])])
    return base64.urlsafe_b64encode(raw.encode()).decode()


def decode_cursor(cursor: str) -> tuple[str, str]:
    created_at, job_id = json.loads(base64.urlsafe_b64decode(cursor.encode()))
    return created_at, job_id


def list_jobs(
    conn: psycopg.Connection, limit: int = 50, cursor: str | None = None, status: str | None = None
) -> tuple[list[dict], str | None]:
    """Keyset pagination on (created_at, id). Stable under concurrent inserts
    *as long as rows become visible in created_at order* — see DESIGN.md for
    the case where they don't."""
    where, params = [], []
    if cursor:
        c_at, c_id = decode_cursor(cursor)
        where.append("(created_at, id) > (%s::timestamptz, %s::uuid)")
        params += [c_at, c_id]
    if status:
        where.append("status = %s")
        params.append(status)
    q = "SELECT * FROM jobs"
    if where:
        q += " WHERE " + " AND ".join(where)
    q += " ORDER BY created_at, id LIMIT %s"
    params.append(limit + 1)
    rows = conn.execute(q, params).fetchall()
    next_cursor = encode_cursor(rows[limit - 1]) if len(rows) > limit else None
    return rows[:limit], next_cursor


def cancel_job(conn: psycopg.Connection, job_id: uuid.UUID) -> tuple[dict | None, str]:
    """Returns (job, outcome) with outcome in:
    cancelled | cancel_requested | already_terminal | not_found.

    queued  -> cancelled immediately.
    running -> cancel_requested=true; the owning worker (heartbeat/finish) or the
               reaper turns it into 'cancelled'. Because finish() checks the flag
               under the row lock, an accepted cancel always ends 'cancelled'.
    SET expressions see the OLD row, and under READ COMMITTED a concurrent claim
    makes this UPDATE re-evaluate against the new version (EvalPlanQual)."""
    row = conn.execute(
        """
        UPDATE jobs SET
            status           = CASE WHEN status = 'queued' THEN 'cancelled' ELSE status END,
            finished_at      = CASE WHEN status = 'queued' THEN now() ELSE finished_at END,
            cancel_requested = true,
            updated_at       = now()
        WHERE id = %s AND status IN ('queued', 'running')
        RETURNING *
        """,
        (job_id,),
    ).fetchone()
    if row is not None:
        return row, ("cancelled" if row["status"] == "cancelled" else "cancel_requested")
    row = get_job(conn, job_id)
    if row is None:
        return None, "not_found"
    return row, "already_terminal"


# ---------------------------------------------------------------- worker side


def _local_now() -> float:
    return time.time() + config.CLOCK_SKEW_S


def _lease_expr() -> tuple[str, dict]:
    """SQL for the new lease expiry. Correct version uses DB time."""
    if _m("local_clock"):
        return "to_timestamp(%(local_now)s + %(lease)s)", {"local_now": _local_now(), "lease": config.LEASE_SECONDS}
    return "now() + make_interval(secs => %(lease)s)", {"lease": config.LEASE_SECONDS}


_CLAIM_SQL = """
WITH next AS (
    SELECT id FROM jobs
    WHERE status = 'queued'
    ORDER BY created_at, id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs j SET
    status           = 'running',
    lease_owner      = %(worker)s,
    lease_expires_at = {lease},
    fencing_token    = j.fencing_token + 1,
    attempts         = j.attempts + 1,
    updated_at       = now()
FROM next
WHERE j.id = next.id
RETURNING j.id, j.fencing_token, j.attempts, j.payload
"""


def claim(conn: psycopg.Connection, worker_id: str) -> Claimed | None:
    lease_sql, lease_params = _lease_expr()
    with conn.transaction():
        if _m("racy_claim"):
            row = _racy_claim(conn, worker_id, lease_sql, lease_params)
        else:
            row = conn.execute(_CLAIM_SQL.format(lease=lease_sql), {"worker": worker_id, **lease_params}).fetchone()
        if row is None:
            return None
        conn.execute(
            "INSERT INTO executions (job_id, worker_id, fencing_token) VALUES (%s, %s, %s)",
            (row["id"], worker_id, row["fencing_token"]),
        )
    return Claimed(row["id"], row["fencing_token"], row["attempts"], row["payload"])


def _racy_claim(conn, worker_id, lease_sql, lease_params):
    """MUTATION: the classic bug. Read without a lock, compute the token in the
    client, write back by id only."""
    cand = conn.execute(
        "SELECT id, fencing_token FROM jobs WHERE status = 'queued' ORDER BY created_at, id LIMIT 1"
    ).fetchone()
    if cand is None:
        return None
    return conn.execute(
        f"""
        UPDATE jobs SET status = 'running', lease_owner = %(worker)s, lease_expires_at = {lease_sql},
               fencing_token = %(tok)s, attempts = attempts + 1, updated_at = now()
        WHERE id = %(id)s
        RETURNING id, fencing_token, attempts, payload
        """,
        {"worker": worker_id, "tok": cand["fencing_token"] + 1, "id": cand["id"], **lease_params},
    ).fetchone()


def _fence() -> str:
    if _m("no_fencing"):
        return "id = %(id)s AND status = 'running'"
    return "id = %(id)s AND fencing_token = %(tok)s AND status = 'running'"


def heartbeat(conn: psycopg.Connection, job: Claimed) -> str:
    """Extend the lease. Returns 'ok', 'cancel' (cancel requested) or 'lost'."""
    lease_sql, lease_params = _lease_expr()
    row = conn.execute(
        f"UPDATE jobs SET lease_expires_at = {lease_sql}, updated_at = now() WHERE {_fence()} RETURNING cancel_requested",
        {"id": job.id, "tok": job.fencing_token, **lease_params},
    ).fetchone()
    if row is None:
        return "lost"
    return "cancel" if row["cancel_requested"] else "ok"


def finish(
    conn: psycopg.Connection,
    job: Claimed,
    worker_id: str,
    outcome: str,
    result: dict | None = None,
    error: str | None = None,
) -> str | None:
    """Fenced transition to a terminal state + the side effect, atomically.
    Returns the final status, or None if the write was rejected as stale.
    A pending cancel request always wins over success/failure."""
    assert outcome in TERMINAL
    if _m("naive_complete"):
        return _naive_finish(conn, job, worker_id, outcome, result, error)
    with conn.transaction():
        row = conn.execute(
            f"""
            UPDATE jobs SET
                status           = CASE WHEN cancel_requested THEN 'cancelled' ELSE %(outcome)s::text END,
                result           = CASE WHEN cancel_requested THEN NULL ELSE %(result)s::jsonb END,
                error            = CASE WHEN cancel_requested THEN NULL ELSE %(error)s::text END,
                lease_owner      = NULL,
                lease_expires_at = NULL,
                finished_at      = now(),
                updated_at       = now()
            WHERE {_fence()}
            RETURNING status
            """,
            {
                "id": job.id,
                "tok": job.fencing_token,
                "outcome": outcome,
                "result": Jsonb(result) if result is not None else None,
                "error": error,
            },
        ).fetchone()
        if row is None:
            return None
        if row["status"] == "succeeded":
            _write_side_effect(conn, job, worker_id)
        return row["status"]


def _write_side_effect(conn, job: Claimed, worker_id: str) -> None:
    conn.execute(
        "INSERT INTO side_effects (job_id, fencing_token, worker_id) VALUES (%s, %s, %s)",
        (job.id, job.fencing_token, worker_id),
    )


def _naive_finish(conn, job, worker_id, outcome, result, error):
    """MUTATION: effect first, then an unguarded status write."""
    if outcome == "succeeded":
        _write_side_effect(conn, job, worker_id)
    conn.execute(
        """
        UPDATE jobs SET status = %s, result = %s, error = %s, lease_owner = NULL,
               lease_expires_at = NULL, finished_at = now(), updated_at = now()
        WHERE id = %s
        """,
        (outcome, Jsonb(result) if result is not None else None, error, job.id),
    )
    return outcome


def reap(conn: psycopg.Connection, batch: int = 200) -> list[dict]:
    """Recover jobs whose lease expired (worker dead or frozen).
    cancel requested -> cancelled; attempts exhausted -> failed; else -> queued.
    Does not bump the fencing token: the next claim does, and status != 'running'
    already fences out the old owner in the meantime."""
    if _m("local_clock"):
        expired, params = "lease_expires_at < to_timestamp(%(local_now)s)", {"local_now": _local_now()}
    else:
        expired, params = "lease_expires_at < now()", {}
    with conn.transaction():
        return conn.execute(
            f"""
            WITH dead AS (
                SELECT id FROM jobs
                WHERE status = 'running' AND {expired}
                ORDER BY lease_expires_at
                LIMIT %(batch)s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE jobs j SET
                status = CASE WHEN j.cancel_requested THEN 'cancelled'
                              WHEN j.attempts >= j.max_attempts THEN 'failed'
                              ELSE 'queued' END,
                error = CASE WHEN j.cancel_requested THEN NULL
                             WHEN j.attempts >= j.max_attempts THEN 'lease expired; max attempts reached'
                             ELSE j.error END,
                finished_at = CASE WHEN j.cancel_requested OR j.attempts >= j.max_attempts
                                   THEN now() ELSE NULL END,
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = now()
            FROM dead
            WHERE j.id = dead.id
            RETURNING j.id, j.status, j.fencing_token
            """,
            {"batch": batch, **params},
        ).fetchall()
