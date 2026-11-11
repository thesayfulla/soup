"""Post-run checks. Each check returns (name, ok, count, sample) and is computed
from the database plus what the client observed over HTTP."""

from __future__ import annotations

from dataclasses import dataclass, field

import psycopg


@dataclass
class ClientObservations:
    # key -> set of job ids the API returned for that key (all 2xx responses)
    ids_by_key: dict[str, set[str]] = field(default_factory=dict)
    # job ids for which the API accepted a cancel (200 or 202)
    cancel_accepted: set[str] = field(default_factory=set)
    # keys sent with a deliberately different body, and the status codes they got
    conflict_statuses: dict[str, int] = field(default_factory=dict)


@dataclass
class Check:
    name: str
    ok: bool
    count: int
    sample: list
    note: str = ""


def _q(conn, sql: str, params=None) -> list[dict]:
    return conn.execute(sql, params).fetchall()


def run_checks(conn: psycopg.Connection, obs: ClientObservations, submitted_keys: list[str]) -> list[Check]:
    checks: list[Check] = []

    def add(name, bad, note=""):
        bad = list(bad)
        checks.append(Check(name, not bad, len(bad), bad[:5], note))

    rows = _q(conn, "SELECT id::text, idempotency_key, status, cancel_requested, fencing_token, lease_owner FROM jobs")
    jobs = {r["idempotency_key"]: r for r in rows}

    # 1. No job is lost: every submitted key has exactly one job, and every id the
    #    API ever returned for that key is that job.
    add("no_lost_jobs", [k for k in submitted_keys if k not in jobs],
        "every submitted key exists in the jobs table")
    add("one_id_per_key", [(k, sorted(ids)) for k, ids in obs.ids_by_key.items()
                           if len(ids) != 1 or (k in jobs and jobs[k]["id"] not in ids)],
        "all replays of a key returned the same job id, matching the DB")
    add("no_unexpected_jobs", [k for k in jobs if k not in set(submitted_keys)])
    add("conflicting_body_rejected", [(k, s) for k, s in obs.conflict_statuses.items() if s != 409],
        "same key + different body must get 409")

    # 2. None has more than one side effect.
    add("at_most_one_effect", _q(conn, """
        SELECT job_id::text, count(*) AS n FROM side_effects GROUP BY job_id HAVING count(*) > 1"""))

    # 3. None stuck.
    add("none_stuck", _q(conn, "SELECT id::text, status FROM jobs WHERE status IN ('queued','running')"))

    # 4. Final states consistent with effects.
    add("succeeded_has_exactly_one_effect", _q(conn, """
        SELECT j.id::text, count(e.id) AS n FROM jobs j LEFT JOIN side_effects e ON e.job_id = j.id
        WHERE j.status = 'succeeded' GROUP BY j.id HAVING count(e.id) <> 1"""))
    add("non_succeeded_has_no_effect", _q(conn, """
        SELECT j.id::text, j.status FROM jobs j JOIN side_effects e ON e.job_id = j.id
        WHERE j.status <> 'succeeded'"""))
    add("effect_written_by_current_lease", _q(conn, """
        SELECT j.id::text, j.fencing_token AS job_token, e.fencing_token AS effect_token, e.worker_id
        FROM jobs j JOIN side_effects e ON e.job_id = j.id WHERE e.fencing_token <> j.fencing_token"""),
        "a mismatch means a stale (zombie) attempt committed the result")
    add("no_double_claim", _q(conn, """
        SELECT job_id::text, fencing_token, count(*) AS n, array_agg(worker_id) AS workers
        FROM executions GROUP BY job_id, fencing_token HAVING count(*) > 1"""),
        "two executions holding the same fencing token = two owners at once")
    add("succeeded_never_cancel_requested", _q(conn, """
        SELECT id::text FROM jobs WHERE status = 'succeeded' AND cancel_requested"""),
        "an accepted cancel must win over completion")
    add("accepted_cancel_ends_cancelled",
        [(i, j["status"]) for j in jobs.values() if (i := j["id"]) in obs.cancel_accepted and j["status"] != "cancelled"])
    add("executions_at_least_one_per_succeeded", _q(conn, """
        SELECT j.id::text FROM jobs j WHERE j.status = 'succeeded'
        AND NOT EXISTS (SELECT 1 FROM executions x WHERE x.job_id = j.id)"""))
    return checks


def stats(conn: psycopg.Connection) -> dict:
    by_status = {r["status"]: r["n"] for r in _q(conn, "SELECT status, count(*) AS n FROM jobs GROUP BY status")}
    runs = _q(conn, """
        SELECT runs, count(*) AS jobs FROM (
            SELECT j.id, count(x.id) AS runs FROM jobs j LEFT JOIN executions x ON x.job_id = j.id GROUP BY j.id
        ) t GROUP BY runs ORDER BY runs""")
    totals = _q(conn, """SELECT (SELECT count(*) FROM executions) AS executions,
                                (SELECT count(*) FROM side_effects) AS side_effects,
                                (SELECT count(*) FROM jobs) AS jobs""")[0]
    return {"by_status": by_status, "executions_per_job_histogram": {r["runs"]: r["jobs"] for r in runs}, **totals}
