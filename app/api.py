import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import FastAPI, Header, HTTPException, Query, Response
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

from app import config, db, queue

pool: ConnectionPool | None = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global pool
    with db.wait_for_db() as conn:
        db.apply_migrations(conn)
    pool = ConnectionPool(
        config.DATABASE_URL,
        min_size=2,
        max_size=config.API_POOL_MAX,
        kwargs={"autocommit": True, "row_factory": dict_row, "options": db.session_options()},
        # Validate a connection on checkout, so after a Postgres restart the pool
        # replaces dead connections instead of handing them to requests.
        check=ConnectionPool.check_connection,
        timeout=10.0,
        open=True,
    )
    try:
        yield
    finally:
        pool.close()


# Endpoints are plain `def`: FastAPI runs them in its threadpool, and the SQL in
# app/queue.py is shared verbatim with the workers and tests.
app = FastAPI(title="soup job queue", lifespan=lifespan)

Status = Literal["queued", "running", "succeeded", "failed", "cancelled"]


class CreateJob(BaseModel):
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int = Field(5, ge=1, le=20)


class JobOut(BaseModel):
    id: uuid.UUID
    idempotency_key: str
    status: Status
    attempts: int
    max_attempts: int
    cancel_requested: bool
    payload: dict[str, Any]
    result: dict[str, Any] | None
    error: str | None
    lease_owner: str | None
    lease_expires_at: datetime | None
    fencing_token: int
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None


class JobPage(BaseModel):
    items: list[JobOut]
    next_cursor: str | None


class CancelOut(BaseModel):
    outcome: Literal["cancelled", "cancel_requested"]
    job: JobOut


def _out(row: dict) -> JobOut:
    return JobOut.model_validate(row)


@app.get("/healthz")
def healthz() -> dict:
    with pool.connection() as conn:
        conn.execute("SELECT 1")
    return {"ok": True}


@app.post("/jobs", status_code=201, response_model=JobOut)
def create_job(
    body: CreateJob,
    response: Response,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
):
    """201 = created, 200 = replay of an earlier request with the same key,
    409 = same key but a different body."""
    with pool.connection() as conn:
        try:
            row, created = queue.create_job(conn, idempotency_key, body.payload, body.max_attempts)
        except queue.IdempotencyConflict as e:
            raise HTTPException(409, {"error": "idempotency_key_reused", "job_id": str(e.existing["id"])})
    if not created:
        response.status_code = 200
    return _out(row)


@app.get("/jobs/{job_id}", response_model=JobOut)
def get_job(job_id: uuid.UUID):
    with pool.connection() as conn:
        row = queue.get_job(conn, job_id)
    if row is None:
        raise HTTPException(404, "job not found")
    return _out(row)


@app.get("/jobs", response_model=JobPage)
def list_jobs(
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    cursor: str | None = None,
    status: Status | None = None,
):
    try:
        with pool.connection() as conn:
            rows, next_cursor = queue.list_jobs(conn, limit, cursor, status)
    except (ValueError, TypeError):
        raise HTTPException(400, "invalid cursor")
    return JobPage(items=[_out(r) for r in rows], next_cursor=next_cursor)


@app.post("/jobs/{job_id}/cancel", response_model=CancelOut)
def cancel_job(job_id: uuid.UUID, response: Response):
    """200 = cancelled now (was queued); 202 = cancel requested (was running,
    the worker or reaper will finish it as 'cancelled'); 409 = already terminal."""
    with pool.connection() as conn:
        row, outcome = queue.cancel_job(conn, job_id)
    if outcome == "not_found":
        raise HTTPException(404, "job not found")
    if outcome == "already_terminal":
        raise HTTPException(409, {"error": "already_terminal", "status": row["status"]})
    if outcome == "cancel_requested":
        response.status_code = 202
    return CancelOut(outcome=outcome, job=_out(row))
