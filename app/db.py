import time
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

from app import config

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"
_MIGRATION_LOCK = 4_242_424


def session_options(idle_tx_timeout_ms: int | None = None) -> str:
    """Server-side guard: a session that sits idle *inside* an open transaction
    (worker frozen between UPDATE and COMMIT) is terminated by Postgres, which
    rolls the transaction back and releases its row locks. Without this, a frozen
    worker can hold a job's row lock forever and the reaper (SKIP LOCKED) never
    sees it."""
    ms = config.IDLE_TX_TIMEOUT_MS if idle_tx_timeout_ms is None else idle_tx_timeout_ms
    return f"-c idle_in_transaction_session_timeout={int(ms)}"


def connect(url: str | None = None, idle_tx_timeout_ms: int | None = None) -> psycopg.Connection:
    # autocommit=True: every statement is its own transaction unless we open one
    # explicitly with `conn.transaction()`. No hidden, long-lived transactions.
    return psycopg.connect(
        url or config.DATABASE_URL,
        autocommit=True,
        row_factory=dict_row,
        options=session_options(idle_tx_timeout_ms),
    )


def wait_for_db(url: str | None = None, timeout: float = 60.0) -> psycopg.Connection:
    deadline = time.monotonic() + timeout
    while True:
        try:
            return connect(url)
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.5)


def apply_migrations(conn: psycopg.Connection) -> None:
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (_MIGRATION_LOCK,))
        for f in sorted(MIGRATIONS_DIR.glob("*.sql")):
            conn.execute(f.read_text())


def ensure_database(url: str) -> None:
    """CREATE DATABASE if it doesn't exist (connects to the `postgres` db)."""
    params = conninfo_to_dict(url)
    name = params["dbname"]
    admin = make_conninfo(url, dbname="postgres")
    with wait_for_db(admin) as conn:
        exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone()
        if not exists:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))


def reset_schema(conn: psycopg.Connection) -> None:
    """Drop everything and re-apply migrations. Used by tests and the chaos harness."""
    conn.execute("DROP SCHEMA public CASCADE")
    conn.execute("CREATE SCHEMA public")
    apply_migrations(conn)
