# src/zdi_mw/state/dead_letter_queue.py
# ZDI Middleware — DeadLetterQueue
# Failed email threads awaiting retry or manual review.
# Retry policies loaded from src/config/retry_policies.json at init.
# Tony reviews human_review_queue.json entries weekly.

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.zdi_mw.state.db import get_connection

logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
_DEFAULT_RETRY_POLICIES_PATH = _REPO_ROOT / "src" / "config" / "retry_policies.json"


class DLQError(RuntimeError):
    """Raised when DLQ operations fail validation or database errors occur."""
    pass


class DeadLetterQueue:
    """
    Stores failed email threads with retry metadata and manual review flags.

    Retry policies are loaded from retry_policies.json once at init and
    applied to every insert based on the failure_reason string.

    Usage:
        dlq = DeadLetterQueue(db_path=db_path)
        dlq.insert("thread_abc", "DRAFT_GENERATED", "Claude timeout",
                   run_context_json='{"run_id": "..."}')
        retryable = dlq.get_retryable()
        dlq.mark_resolved(row_id, "RETRIED_SUCCESSFULLY")
    """

    def __init__(
        self,
        db_path: Optional[Path] = None,
        retry_policies_path: Optional[Path] = None,
    ) -> None:
        """
        Args:
            db_path: Override DB path (used in tests).
            retry_policies_path: Override policies path (used in tests).
        """
        self._db_path = db_path
        self._policies = self._load_retry_policies(
            retry_policies_path or _DEFAULT_RETRY_POLICIES_PATH
        )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def insert(
        self,
        thread_id: str,
        failure_stage: str,
        failure_reason: str,
        run_context_json: Optional[str] = None,
        email_metadata_json: Optional[str] = None,
    ) -> int:
        """
        Insert a failed thread into the DLQ.

        Retry policy is looked up by failure_reason and applied automatically.
        If the failure maps to alert_immediate=true, a WARNING is logged.

        Args:
            thread_id: Email thread identifier.
            failure_stage: Pipeline stage where failure occurred.
            failure_reason: Failure type key (must match retry_policies.json key,
                            or 'UNCONFIRMED_INTENT_ON_STARTUP' for WAL orphans).
            run_context_json: JSON string of RunContext.to_log_dict() (no credentials).
            email_metadata_json: JSON string of email metadata (IDs and hashes only).

        Returns:
            DLQ row ID.
        """
        policy = self._get_policy(failure_reason)

        retry_eligible = 1 if policy.get("retry_eligible", False) else 0
        manual_review = 1 if policy.get("manual_review", False) else 0
        permanent_skip = 1 if policy.get("permanent_skip", False) else 0
        max_retries = int(policy.get("max_retries", 0))
        alert_immediate = policy.get("alert_immediate", False)

        # Calculate first retry time if applicable
        backoff = policy.get("backoff_seconds")
        retry_backoff_next_at = None
        if retry_eligible and backoff:
            first_backoff = backoff[0] if isinstance(backoff, list) else int(backoff)
            retry_backoff_next_at = (
                datetime.now(timezone.utc) + timedelta(seconds=first_backoff)
            ).isoformat()

        now_utc = datetime.now(timezone.utc).isoformat()

        conn = get_connection(self._db_path)
        try:
            with conn:
                cursor = conn.execute(
                    """
                    INSERT INTO dead_letter_queue
                        (thread_id, run_id, failure_stage, failure_reason,
                         retry_count, max_retries, retry_eligible,
                         manual_review, permanent_skip,
                         email_metadata_json, run_context_json,
                         timestamp_utc, retry_backoff_next_at)
                    VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        thread_id,
                        self._run_id_from_context(run_context_json),
                        failure_stage,
                        failure_reason,
                        max_retries,
                        retry_eligible,
                        manual_review,
                        permanent_skip,
                        email_metadata_json,
                        run_context_json,
                        now_utc,
                        retry_backoff_next_at,
                    ),
                )
                row_id = cursor.lastrowid
        except Exception as exc:
            raise DLQError(f"Failed to insert DLQ entry: {exc}") from exc
        finally:
            conn.close()

        log_level = logging.WARNING if alert_immediate else logging.INFO
        logger.log(
            log_level,
            "DLQ INSERT id=%d thread_id=%s stage=%s reason=%s "
            "retry_eligible=%s manual_review=%s permanent_skip=%s alert=%s",
            row_id,
            thread_id,
            failure_stage,
            failure_reason,
            bool(retry_eligible),
            bool(manual_review),
            bool(permanent_skip),
            alert_immediate,
        )

        if alert_immediate:
            logger.warning(
                "DLQ ALERT_IMMEDIATE: thread_id=%s failure_reason=%s — "
                "immediate human attention required",
                thread_id,
                failure_reason,
            )

        return row_id

    def get_retryable(self) -> List[dict]:
        """
        Return DLQ entries that are eligible for retry and past their backoff window.

        Returns:
            List of DLQ row dicts where retry_eligible=1, resolved_at IS NULL,
            permanent_skip=0, retry_count < max_retries, and
            retry_backoff_next_at <= now (or IS NULL).
        """
        now_utc = datetime.now(timezone.utc).isoformat()
        conn = get_connection(self._db_path)
        try:
            rows = conn.execute(
                """
                SELECT *
                FROM dead_letter_queue
                WHERE retry_eligible = 1
                  AND resolved_at IS NULL
                  AND permanent_skip = 0
                  AND retry_count < max_retries
                  AND (retry_backoff_next_at IS NULL
                       OR retry_backoff_next_at <= ?)
                ORDER BY timestamp_utc ASC
                """,
                (now_utc,),
            ).fetchall()
        finally:
            conn.close()

        return [dict(row) for row in rows]

    def get_manual_review_items(self) -> List[dict]:
        """
        Return DLQ entries flagged for human review that are unresolved.

        Returns:
            List of DLQ row dicts where manual_review=1 and resolved_at IS NULL.
        """
        conn = get_connection(self._db_path)
        try:
            rows = conn.execute(
                """
                SELECT *
                FROM dead_letter_queue
                WHERE manual_review = 1
                  AND resolved_at IS NULL
                ORDER BY timestamp_utc ASC
                """,
            ).fetchall()
        finally:
            conn.close()

        return [dict(row) for row in rows]

    def mark_resolved(self, dlq_id: int, resolution: str) -> None:
        """
        Mark a DLQ entry as resolved.

        Args:
            dlq_id: Row ID of the DLQ entry.
            resolution: Resolution description, e.g. 'RETRIED_SUCCESSFULLY',
                        'MANUALLY_SKIPPED', 'PROMOTED_TO_DRAFT'.
        """
        now_utc = datetime.now(timezone.utc).isoformat()
        conn = get_connection(self._db_path)
        try:
            with conn:
                rowcount = conn.execute(
                    """
                    UPDATE dead_letter_queue
                    SET resolved_at = ?, resolution = ?
                    WHERE id = ? AND resolved_at IS NULL
                    """,
                    (now_utc, resolution, dlq_id),
                ).rowcount
        finally:
            conn.close()

        if rowcount == 0:
            logger.warning(
                "DLQ mark_resolved: id=%d not found or already resolved", dlq_id
            )
        else:
            logger.info("DLQ RESOLVED id=%d resolution=%s", dlq_id, resolution)

    def increment_retry_count(self, dlq_id: int, next_backoff_seconds: Optional[int] = None) -> None:
        """
        Increment retry_count and update next retry window.

        Args:
            dlq_id: DLQ row ID.
            next_backoff_seconds: Seconds until next retry attempt (None = now).
        """
        next_at = None
        if next_backoff_seconds is not None:
            next_at = (
                datetime.now(timezone.utc) + timedelta(seconds=next_backoff_seconds)
            ).isoformat()

        conn = get_connection(self._db_path)
        try:
            with conn:
                conn.execute(
                    """
                    UPDATE dead_letter_queue
                    SET retry_count = retry_count + 1,
                        retry_backoff_next_at = ?
                    WHERE id = ?
                    """,
                    (next_at, dlq_id),
                )
        finally:
            conn.close()

        logger.debug("DLQ retry count incremented id=%d next_at=%s", dlq_id, next_at)

    def get_by_id(self, dlq_id: int) -> Optional[dict]:
        """Return a DLQ row by ID, or None if not found."""
        conn = get_connection(self._db_path)
        try:
            row = conn.execute(
                "SELECT * FROM dead_letter_queue WHERE id = ?", (dlq_id,)
            ).fetchone()
        finally:
            conn.close()
        return dict(row) if row else None

    def get_unresolved_count(self) -> int:
        """Return the count of unresolved DLQ entries."""
        conn = get_connection(self._db_path)
        try:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM dead_letter_queue WHERE resolved_at IS NULL"
            ).fetchone()
        finally:
            conn.close()
        return row["cnt"] if row else 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_retry_policies(self, path: Path) -> Dict[str, Any]:
        """
        Load retry policies from JSON. Returns empty dict on failure
        (pipeline should not crash because of a missing config file at DLQ init).
        """
        try:
            with open(path) as f:
                data = json.load(f)
            policies = data.get("retry_policies", {})
            logger.info(
                "DLQ: loaded %d retry policies from %s", len(policies), path
            )
            return policies
        except FileNotFoundError:
            logger.warning(
                "DLQ: retry_policies.json not found at %s — using empty policies", path
            )
            return {}
        except json.JSONDecodeError as exc:
            logger.error(
                "DLQ: failed to parse retry_policies.json: %s — using empty policies", exc
            )
            return {}

    def _get_policy(self, failure_reason: str) -> Dict[str, Any]:
        """
        Look up the retry policy for a failure reason.
        Falls back to a safe default (no retry, no alert) if not found.
        """
        if failure_reason in self._policies:
            return self._policies[failure_reason]

        # Partial match: check if failure_reason starts with a known key
        for key, policy in self._policies.items():
            if failure_reason.startswith(key):
                return policy

        logger.debug(
            "DLQ: no policy found for failure_reason='%s' — using safe default",
            failure_reason,
        )
        return {"retry_eligible": False, "manual_review": True}

    @staticmethod
    def _run_id_from_context(run_context_json: Optional[str]) -> str:
        """Extract run_id from serialised context JSON, or return 'unknown'."""
        if not run_context_json:
            return "unknown"
        try:
            ctx = json.loads(run_context_json)
            return ctx.get("run_id", "unknown")
        except (json.JSONDecodeError, AttributeError):
            return "unknown"
