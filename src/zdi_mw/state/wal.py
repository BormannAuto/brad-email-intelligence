# src/zdi_mw/state/wal.py
# ZDI Middleware — WriteAheadLogger (WAL)
# INTENT → CONFIRMED → (RECONCILE on next startup)
# Logs write intent before any external API call. Confirms after success.
# On startup: RECONCILE pass scans for INTENT rows older than 10 minutes
# with no CONFIRMED — dead-letters them as UNCONFIRMED_INTENT_ON_STARTUP.

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.zdi_mw.state.db import get_connection

logger = logging.getLogger(__name__)

# Orphaned INTENT rows older than this are dead-lettered during RECONCILE
_ORPHAN_THRESHOLD_MINUTES = 10


class WALError(RuntimeError):
    """Raised when WAL operations encounter an unexpected database error."""
    pass


class WriteAheadLogger:
    """
    Write-ahead log for all external write operations.

    Every write to Zoho (CRM, Mail, WorkDrive) must:
      1. log_intent() BEFORE the API call → status=INTENT
      2. confirm()    AFTER success       → status=CONFIRMED
      3. fail()       on error            → status=FAILED

    On pipeline startup, call reconcile(dlq) to find INTENTs that were
    never confirmed (crash between intent and write) and dead-letter them.

    Usage:
        wal = WriteAheadLogger(run_id, db_path)
        intent_id = wal.log_intent("thread_abc", "CRM_WRITE", {"field": "phone", ...})
        # ... make API call ...
        wal.confirm(intent_id)
        # or on failure:
        wal.fail(intent_id, "Zoho returned 503")
    """

    def __init__(self, run_id: str, db_path: Optional[Path] = None) -> None:
        """
        Args:
            run_id: Current pipeline run identifier.
            db_path: Override DB path (used in tests).
        """
        self._run_id = run_id
        self._db_path = db_path

    # ------------------------------------------------------------------
    # Write lifecycle
    # ------------------------------------------------------------------

    def log_intent(
        self,
        thread_id: str,
        operation: str,
        payload: Dict[str, Any],
    ) -> int:
        """
        Record write intent before making an external API call.

        The payload is serialised to JSON. Sensitive values (token contents,
        full email bodies) must NOT be included — log IDs and hashes only.

        Args:
            thread_id: Email thread this write belongs to.
            operation: Operation name, e.g. 'CRM_WRITE_phone', 'MAIL_DRAFT'.
            payload: Dict of write parameters (no credentials, no PII content).

        Returns:
            WAL row ID (intent_id) — pass to confirm() or fail().

        Raises:
            WALError: On database error.
        """
        now_utc = datetime.now(timezone.utc).isoformat()
        payload_json = json.dumps(payload, default=str, sort_keys=True)

        conn = get_connection(self._db_path)
        try:
            with conn:
                cursor = conn.execute(
                    """
                    INSERT INTO wal_log
                        (run_id, thread_id, operation, intent_payload_json,
                         status, timestamp_utc)
                    VALUES (?, ?, ?, ?, 'INTENT', ?)
                    """,
                    (self._run_id, thread_id, operation, payload_json, now_utc),
                )
                intent_id = cursor.lastrowid
        except Exception as exc:
            raise WALError(f"Failed to log WAL intent: {exc}") from exc
        finally:
            conn.close()

        logger.debug(
            "WAL INTENT id=%d thread_id=%s operation=%s run_id=%s",
            intent_id,
            thread_id,
            operation,
            self._run_id,
        )
        return intent_id

    def confirm(self, intent_id: int) -> None:
        """
        Mark a WAL intent as CONFIRMED after a successful API write.

        Args:
            intent_id: Row ID returned by log_intent().

        Raises:
            WALError: If the row doesn't exist or is not in INTENT status.
        """
        conn = get_connection(self._db_path)
        try:
            with conn:
                rowcount = conn.execute(
                    """
                    UPDATE wal_log
                    SET status = 'CONFIRMED'
                    WHERE id = ? AND status = 'INTENT'
                    """,
                    (intent_id,),
                ).rowcount
        except Exception as exc:
            raise WALError(f"Failed to confirm WAL intent id={intent_id}: {exc}") from exc
        finally:
            conn.close()

        if rowcount == 0:
            raise WALError(
                f"WAL confirm failed: id={intent_id} not found or not in INTENT status"
            )

        logger.debug("WAL CONFIRMED id=%d", intent_id)

    def fail(self, intent_id: int, reason: str) -> None:
        """
        Mark a WAL intent as FAILED after an unsuccessful API write.

        Args:
            intent_id: Row ID returned by log_intent().
            reason: Failure description (no credentials, no PII content).
        """
        conn = get_connection(self._db_path)
        try:
            with conn:
                conn.execute(
                    """
                    UPDATE wal_log
                    SET status = 'FAILED', failure_reason = ?
                    WHERE id = ?
                    """,
                    (reason, intent_id),
                )
        except Exception as exc:
            raise WALError(f"Failed to mark WAL intent failed id={intent_id}: {exc}") from exc
        finally:
            conn.close()

        logger.warning("WAL FAILED id=%d reason=%s", intent_id, reason)

    # ------------------------------------------------------------------
    # Startup RECONCILE pass
    # ------------------------------------------------------------------

    def reconcile(self, dlq: "DeadLetterQueue") -> List[dict]:
        """
        Startup RECONCILE: scan all INTENT rows (across all runs) that are
        older than _ORPHAN_THRESHOLD_MINUTES and have never been CONFIRMED.
        These represent write intents from a crashed pipeline run.

        Dead-letters each orphan with reason='UNCONFIRMED_INTENT_ON_STARTUP'.
        Marks each orphaned row as FAILED in the WAL.

        Args:
            dlq: DeadLetterQueue instance to receive orphaned intents.

        Returns:
            List of orphaned WAL row dicts that were dead-lettered.
        """
        threshold_utc = (
            datetime.now(timezone.utc) - timedelta(minutes=_ORPHAN_THRESHOLD_MINUTES)
        ).isoformat()

        conn = get_connection(self._db_path)
        try:
            orphans = conn.execute(
                """
                SELECT id, run_id, thread_id, operation,
                       intent_payload_json, timestamp_utc
                FROM wal_log
                WHERE status = 'INTENT'
                  AND timestamp_utc < ?
                """,
                (threshold_utc,),
            ).fetchall()
        finally:
            conn.close()

        if not orphans:
            logger.info("WAL RECONCILE: no orphaned intents found")
            return []

        logger.warning(
            "WAL RECONCILE: found %d orphaned INTENT row(s) — dead-lettering",
            len(orphans),
        )

        reconciled = []
        for row in orphans:
            row_dict = dict(row)
            try:
                dlq.insert(
                    thread_id=row_dict["thread_id"],
                    failure_stage=row_dict["operation"],
                    failure_reason="UNCONFIRMED_INTENT_ON_STARTUP",
                    run_context_json=json.dumps({
                        "run_id": row_dict["run_id"],
                        "reconciled_at": datetime.now(timezone.utc).isoformat(),
                        "orphan_wal_id": row_dict["id"],
                        "orphan_timestamp_utc": row_dict["timestamp_utc"],
                    }),
                )
                self.fail(row_dict["id"], "UNCONFIRMED_INTENT_ON_STARTUP")
                reconciled.append(row_dict)
                logger.warning(
                    "WAL RECONCILE: dead-lettered orphan wal_id=%d thread_id=%s "
                    "operation=%s original_run_id=%s",
                    row_dict["id"],
                    row_dict["thread_id"],
                    row_dict["operation"],
                    row_dict["run_id"],
                )
            except Exception as exc:
                logger.error(
                    "WAL RECONCILE: failed to process orphan wal_id=%d error=%s",
                    row_dict["id"],
                    exc,
                )

        return reconciled

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def get_by_id(self, intent_id: int) -> Optional[dict]:
        """Return a WAL row by ID, or None if not found."""
        conn = get_connection(self._db_path)
        try:
            row = conn.execute(
                "SELECT * FROM wal_log WHERE id = ?", (intent_id,)
            ).fetchone()
        finally:
            conn.close()
        return dict(row) if row else None

    def get_thread_intents(self, thread_id: str) -> List[dict]:
        """Return all WAL rows for a thread in this run, ordered by id."""
        conn = get_connection(self._db_path)
        try:
            rows = conn.execute(
                """
                SELECT * FROM wal_log
                WHERE run_id = ? AND thread_id = ?
                ORDER BY id ASC
                """,
                (self._run_id, thread_id),
            ).fetchall()
        finally:
            conn.close()
        return [dict(row) for row in rows]


# Deferred import to avoid circular dependency at module level
from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue  # noqa: E402
