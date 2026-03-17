# src/zdi_mw/state/db.py
# ZDI Middleware — Database initialisation
# Single SQLite file: state/zdi_state.db
# Created automatically on first run. Tony can inspect at any time with
# DB Browser for SQLite.
#
# Schema changes MUST go through Alembic migrations — never alter tables
# by hand or by calling init_db() after the DB exists. The CREATE TABLE IF
# NOT EXISTS guards are a safety net only.

import logging
import os
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

# Resolve DB path relative to repo root (two levels up from src/zdi_mw/state/)
_THIS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _THIS_DIR.parent.parent.parent.parent  # …/brad-email-intelligence/
DB_PATH: Path = _REPO_ROOT / "state" / "zdi_state.db"

# DDL — matches Section 2 exactly. Do not modify without an Alembic migration.
_DDL_STATEMENTS = [
    # ------------------------------------------------------------------
    # PipelineStateLedger: one row per email thread per stage per run
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS pipeline_state (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id            TEXT    NOT NULL,
        thread_id         TEXT    NOT NULL,
        stage             TEXT    NOT NULL,
        -- Stages: FETCHED | CATEGORIZED | CRM_ENRICHED | WORKDRIVE_LOOKUP
        --         DRAFT_GENERATED | DRAFT_WRITTEN | COMPLETE | FAILED
        status            TEXT    NOT NULL DEFAULT 'IN_PROGRESS',
        failure_reason    TEXT,
        timestamp_utc     TEXT    NOT NULL,
        pipeline_version  TEXT    NOT NULL
    )
    """,
    # Index for fast lookups by run + thread
    """
    CREATE INDEX IF NOT EXISTS idx_pipeline_state_run_thread
        ON pipeline_state (run_id, thread_id)
    """,

    # ------------------------------------------------------------------
    # WriteAheadLog: intent before write, confirmed after
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS wal_log (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id                  TEXT    NOT NULL,
        thread_id               TEXT    NOT NULL,
        operation               TEXT    NOT NULL,
        intent_payload_json     TEXT    NOT NULL,
        status                  TEXT    NOT NULL DEFAULT 'INTENT',
        -- Status: INTENT | CONFIRMED | FAILED
        failure_reason          TEXT,
        timestamp_utc           TEXT    NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_wal_log_status
        ON wal_log (status, timestamp_utc)
    """,

    # ------------------------------------------------------------------
    # IdempotencyKeys: SHA-256 content-aware, prevents duplicate writes
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS idempotency_keys (
        key            TEXT    PRIMARY KEY,
        operation      TEXT    NOT NULL,
        run_id         TEXT    NOT NULL,
        outcome        TEXT    NOT NULL,  -- COMPLETED | SKIPPED | FAILED
        timestamp_utc  TEXT    NOT NULL
    )
    """,

    # ------------------------------------------------------------------
    # DeadLetterQueue: failed emails awaiting retry or manual review
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS dead_letter_queue (
        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
        thread_id            TEXT    NOT NULL,
        run_id               TEXT    NOT NULL,
        failure_stage        TEXT    NOT NULL,
        failure_reason       TEXT    NOT NULL,
        retry_count          INTEGER DEFAULT 0,
        max_retries          INTEGER DEFAULT 1,
        retry_eligible       INTEGER DEFAULT 0,   -- 0=false, 1=true
        manual_review        INTEGER DEFAULT 0,
        permanent_skip       INTEGER DEFAULT 0,
        email_metadata_json  TEXT,
        run_context_json     TEXT,
        timestamp_utc        TEXT    NOT NULL,
        resolved_at          TEXT,
        resolution           TEXT,
        retry_backoff_next_at TEXT   -- ISO UTC timestamp, when to next retry
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_dlq_retry
        ON dead_letter_queue (retry_eligible, retry_backoff_next_at)
        WHERE resolved_at IS NULL
    """,

    # ------------------------------------------------------------------
    # PipelineLocks: prevents concurrent runs
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS pipeline_locks (
        lock_name    TEXT    PRIMARY KEY,
        run_id       TEXT    NOT NULL,
        acquired_at  TEXT    NOT NULL,
        pid          INTEGER NOT NULL
    )
    """,
]


def get_db_path() -> Path:
    """Return the resolved path to zdi_state.db."""
    return DB_PATH


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """
    Return a SQLite connection with WAL mode and foreign-key enforcement.

    Args:
        db_path: Override path (used in tests). Defaults to state/zdi_state.db.

    Returns:
        sqlite3.Connection with row_factory set to sqlite3.Row.
    """
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: Path | None = None) -> None:
    """
    Create all 5 tables if they do not already exist.

    Safe to call on every startup — uses CREATE TABLE IF NOT EXISTS.
    For schema changes after initial creation, use Alembic migrations.

    Args:
        db_path: Override path (used in tests).
    """
    path = db_path or DB_PATH
    logger.info("Initialising ZDI state database at %s", path)

    with get_connection(path) as conn:
        for ddl in _DDL_STATEMENTS:
            conn.execute(ddl)
        conn.commit()

    logger.info("Database initialised — all 5 tables ready")


def verify_tables(db_path: Path | None = None) -> dict[str, bool]:
    """
    Check that all 5 required tables exist.

    Returns:
        Dict mapping table name → True/False.
    """
    required = {
        "pipeline_state",
        "wal_log",
        "idempotency_keys",
        "dead_letter_queue",
        "pipeline_locks",
    }
    path = db_path or DB_PATH
    if not path.exists():
        return {t: False for t in required}

    with get_connection(path) as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    existing = {row["name"] for row in rows}
    return {t: (t in existing) for t in required}
