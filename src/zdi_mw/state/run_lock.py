# src/zdi_mw/state/run_lock.py
# ZDI Middleware — RunLock
# Prevents concurrent pipeline runs using the PipelineLocks SQLite table.
# Acquired at startup, released on clean exit via atexit handler.
# On crash: stale lock is detected by pid liveness check on next startup.

import atexit
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from src.zdi_mw.state.db import get_connection

logger = logging.getLogger(__name__)

_LOCK_NAME = "pipeline_main"
_LOCK_TIMEOUT_SECONDS = 3600  # 1 hour — if a pid is dead and lock is older, it's stale


class LockAlreadyHeldError(RuntimeError):
    """Raised when the pipeline lock is held by another live process."""
    pass


class RunLock:
    """
    Advisory process lock using the pipeline_locks SQLite table.

    Usage:
        lock = RunLock(conn)
        lock.acquire(run_id)          # raises LockAlreadyHeldError if blocked
        # ... pipeline runs ...
        lock.release()                # explicit release on clean exit
        # atexit handler calls release() automatically on crash

    The lock stores the acquiring PID. On startup, if a lock exists for a
    dead PID (process no longer running), the lock is considered stale and
    is cleared with a warning logged. This handles the crash-recovery case.
    """

    def __init__(self, db_path: Optional[Path] = None) -> None:
        """
        Args:
            db_path: Override DB path (used in tests). Defaults to state/zdi_state.db.
        """
        self._db_path = db_path
        self._run_id: Optional[str] = None
        self._acquired = False
        # Register atexit so crash releases the lock
        atexit.register(self._atexit_release)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def acquire(self, run_id: str) -> None:
        """
        Attempt to acquire the pipeline lock for run_id.

        Clears stale locks (dead pid) automatically with a warning.
        Raises LockAlreadyHeldError if a live process holds the lock.

        Args:
            run_id: The current run identifier.

        Raises:
            LockAlreadyHeldError: If lock is held by a live process.
        """
        conn = get_connection(self._db_path)
        try:
            with conn:
                existing = conn.execute(
                    "SELECT run_id, pid, acquired_at FROM pipeline_locks WHERE lock_name = ?",
                    (_LOCK_NAME,),
                ).fetchone()

                if existing is not None:
                    existing_pid = existing["pid"]
                    existing_run_id = existing["run_id"]
                    existing_acquired_at = existing["acquired_at"]

                    if self._is_pid_alive(existing_pid):
                        raise LockAlreadyHeldError(
                            f"Pipeline lock held by pid={existing_pid} "
                            f"run_id={existing_run_id} acquired_at={existing_acquired_at}. "
                            f"If that process has crashed, delete the pipeline_locks row manually."
                        )
                    else:
                        logger.warning(
                            "Clearing stale lock from dead process pid=%s run_id=%s acquired_at=%s",
                            existing_pid,
                            existing_run_id,
                            existing_acquired_at,
                        )
                        conn.execute(
                            "DELETE FROM pipeline_locks WHERE lock_name = ?",
                            (_LOCK_NAME,),
                        )

                # Insert our lock
                conn.execute(
                    """
                    INSERT INTO pipeline_locks (lock_name, run_id, acquired_at, pid)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        _LOCK_NAME,
                        run_id,
                        datetime.now(timezone.utc).isoformat(),
                        os.getpid(),
                    ),
                )

        finally:
            conn.close()

        self._run_id = run_id
        self._acquired = True
        logger.info("Pipeline lock acquired run_id=%s pid=%s", run_id, os.getpid())

    def release(self) -> None:
        """
        Release the pipeline lock. Safe to call multiple times.
        No-op if lock was never acquired.
        """
        if not self._acquired:
            return
        conn = get_connection(self._db_path)
        try:
            with conn:
                conn.execute(
                    "DELETE FROM pipeline_locks WHERE lock_name = ? AND pid = ?",
                    (_LOCK_NAME, os.getpid()),
                )
        except sqlite3.Error as exc:
            # Log but don't raise — release is called from atexit, must not crash
            logger.error("Failed to release pipeline lock: %s", exc)
        finally:
            conn.close()

        self._acquired = False
        logger.info("Pipeline lock released run_id=%s", self._run_id)

    def is_held(self) -> bool:
        """Return True if this instance currently holds the lock."""
        return self._acquired

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_pid_alive(pid: int) -> bool:
        """Check whether a process with the given pid is still running."""
        try:
            # Signal 0 checks existence without sending a signal
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            # Process exists but we don't own it — still alive
            return True

    def _atexit_release(self) -> None:
        """Called automatically on process exit (clean or crash)."""
        if self._acquired:
            logger.info("atexit: releasing pipeline lock run_id=%s", self._run_id)
            self.release()
