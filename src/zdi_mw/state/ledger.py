# src/zdi_mw/state/ledger.py
# ZDI Middleware — PipelineStateLedger
# Tracks pipeline progress: one row per email thread per stage per run.
# 8 defined stages in fixed order. All timestamps UTC ISO 8601.

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from src.zdi_mw.state.db import get_connection

logger = logging.getLogger(__name__)

# All valid pipeline stages in execution order (spec Section 2)
PIPELINE_STAGES = [
    "FETCHED",
    "CATEGORIZED",
    "CRM_ENRICHED",
    "WORKDRIVE_LOOKUP",
    "DRAFT_GENERATED",
    "DRAFT_WRITTEN",
    "COMPLETE",
    "FAILED",
]

# Valid status values
VALID_STATUSES = {"IN_PROGRESS", "COMPLETE", "FAILED", "SKIPPED"}


class LedgerError(RuntimeError):
    """Raised when ledger operations fail validation."""
    pass


class PipelineStateLedger:
    """
    Records pipeline stage progress for each email thread in a run.

    Each call to mark_stage() writes a row. The ledger is append-only —
    stages are never updated, only added. To check current state, call
    get_stage() which returns the most recent row for a given thread+stage.

    Usage:
        ledger = PipelineStateLedger(run_id, pipeline_version, db_path)
        ledger.mark_stage("thread_abc", "FETCHED", "COMPLETE")
        ledger.mark_stage("thread_abc", "CATEGORIZED", "IN_PROGRESS")
        stage = ledger.get_stage("thread_abc", "CATEGORIZED")
        incomplete = ledger.get_incomplete_threads()
    """

    def __init__(
        self,
        run_id: str,
        pipeline_version: str,
        db_path: Optional[Path] = None,
    ) -> None:
        """
        Args:
            run_id: Current pipeline run identifier.
            pipeline_version: git commit hash string for this run.
            db_path: Override DB path (used in tests).
        """
        self._run_id = run_id
        self._pipeline_version = pipeline_version
        self._db_path = db_path

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def mark_stage(
        self,
        thread_id: str,
        stage: str,
        status: str,
        failure_reason: Optional[str] = None,
    ) -> int:
        """
        Record a stage transition for a thread.

        Args:
            thread_id: Email thread identifier.
            stage: One of PIPELINE_STAGES.
            status: One of VALID_STATUSES.
            failure_reason: Required if status is 'FAILED', ignored otherwise.

        Returns:
            Row ID of the inserted record.

        Raises:
            LedgerError: If stage or status are invalid.
        """
        if stage not in PIPELINE_STAGES:
            raise LedgerError(
                f"Invalid stage '{stage}'. Valid: {PIPELINE_STAGES}"
            )
        if status not in VALID_STATUSES:
            raise LedgerError(
                f"Invalid status '{status}'. Valid: {VALID_STATUSES}"
            )

        now_utc = datetime.now(timezone.utc).isoformat()
        conn = get_connection(self._db_path)
        try:
            with conn:
                cursor = conn.execute(
                    """
                    INSERT INTO pipeline_state
                        (run_id, thread_id, stage, status, failure_reason,
                         timestamp_utc, pipeline_version)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self._run_id,
                        thread_id,
                        stage,
                        status,
                        failure_reason,
                        now_utc,
                        self._pipeline_version,
                    ),
                )
                row_id = cursor.lastrowid
        finally:
            conn.close()

        logger.debug(
            "Ledger: thread_id=%s stage=%s status=%s run_id=%s",
            thread_id,
            stage,
            status,
            self._run_id,
        )
        return row_id

    def get_stage(self, thread_id: str, stage: str) -> Optional[dict]:
        """
        Return the most recent ledger row for a thread+stage combination.

        Args:
            thread_id: Email thread identifier.
            stage: Pipeline stage to look up.

        Returns:
            Dict with keys {id, run_id, thread_id, stage, status,
            failure_reason, timestamp_utc, pipeline_version}, or None if
            no record exists.
        """
        conn = get_connection(self._db_path)
        try:
            row = conn.execute(
                """
                SELECT id, run_id, thread_id, stage, status,
                       failure_reason, timestamp_utc, pipeline_version
                FROM pipeline_state
                WHERE run_id = ? AND thread_id = ? AND stage = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (self._run_id, thread_id, stage),
            ).fetchone()
        finally:
            conn.close()

        return dict(row) if row else None

    def get_incomplete_threads(self) -> List[str]:
        """
        Return thread_ids that have at least one IN_PROGRESS stage and no COMPLETE
        final stage. Used by the WAL RECONCILE pass on startup.

        Returns:
            List of thread_id strings that were in flight when the pipeline
            last ran (or crashed).
        """
        conn = get_connection(self._db_path)
        try:
            rows = conn.execute(
                """
                SELECT DISTINCT thread_id
                FROM pipeline_state
                WHERE run_id = ?
                  AND status = 'IN_PROGRESS'
                  AND thread_id NOT IN (
                      SELECT thread_id FROM pipeline_state
                      WHERE run_id = ? AND stage = 'COMPLETE' AND status = 'COMPLETE'
                  )
                """,
                (self._run_id, self._run_id),
            ).fetchall()
        finally:
            conn.close()

        return [row["thread_id"] for row in rows]

    def get_thread_stages(self, thread_id: str) -> List[dict]:
        """
        Return all stage records for a thread in this run, ordered by id.

        Args:
            thread_id: Email thread identifier.

        Returns:
            List of stage dicts in insertion order.
        """
        conn = get_connection(self._db_path)
        try:
            rows = conn.execute(
                """
                SELECT id, run_id, thread_id, stage, status,
                       failure_reason, timestamp_utc, pipeline_version
                FROM pipeline_state
                WHERE run_id = ? AND thread_id = ?
                ORDER BY id ASC
                """,
                (self._run_id, thread_id),
            ).fetchall()
        finally:
            conn.close()

        return [dict(row) for row in rows]

    def mark_failed(self, thread_id: str, stage: str, reason: str) -> int:
        """
        Convenience method: mark a stage FAILED with a reason.

        Args:
            thread_id: Email thread identifier.
            stage: Stage that failed.
            reason: Human-readable failure reason (no PII).

        Returns:
            Row ID of the inserted record.
        """
        return self.mark_stage(thread_id, stage, "FAILED", failure_reason=reason)
