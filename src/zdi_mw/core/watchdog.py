# src/zdi_mw/core/watchdog.py
# ZDI Middleware — WatchdogTimer
# 45-second per-stage timeout. On fire: marks stage FAILED in Ledger,
# pushes to DLQ with reason='TIMEOUT', then continues pipeline.
# Used as a context manager — clean exit cancels the timer.

import logging
import threading
from typing import Any, Optional

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_SECONDS = 45


class WatchdogTimer:
    """
    Per-stage timeout enforcer used as a context manager.

    On clean exit (__exit__ with no exception): timer is cancelled.
    On timeout (stage takes too long): fires the timeout handler which:
      - marks the stage FAILED in the Ledger
      - pushes a DLQ entry with reason='TIMEOUT'
      - sets self.fired = True so the caller can check

    The pipeline continues with remaining emails after a timeout —
    the timer does NOT abort the whole run.

    Usage:
        with WatchdogTimer(
            stage_name="DRAFT_GENERATED",
            thread_id="thread_abc",
            run_context=ctx,
            ledger=ledger,
            dlq=dlq,
        ) as dog:
            # ... do stage work ...
            # if this takes > 45s, timeout fires automatically

        if dog.fired:
            continue  # skip to next email

    Injectable timeout for testing:
        with WatchdogTimer(..., timeout_seconds=0.05) as dog:
            time.sleep(0.2)  # trigger timeout in test
    """

    def __init__(
        self,
        stage_name: str,
        thread_id: str,
        run_context: Any,          # RunContext
        ledger: Any,               # PipelineStateLedger
        dlq: Any,                  # DeadLetterQueue
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        """
        Args:
            stage_name: Pipeline stage being monitored, e.g. 'DRAFT_GENERATED'.
            thread_id: Email thread ID being processed.
            run_context: RunContext for this pipeline run.
            ledger: PipelineStateLedger instance.
            dlq: DeadLetterQueue instance.
            timeout_seconds: Seconds before timeout fires. Default 45.
        """
        self.stage_name = stage_name
        self.thread_id = thread_id
        self.run_context = run_context
        self.ledger = ledger
        self.dlq = dlq
        self.timeout_seconds = timeout_seconds
        self.fired = False
        self._timer: Optional[threading.Timer] = None

    # ------------------------------------------------------------------
    # Context manager protocol
    # ------------------------------------------------------------------

    def __enter__(self) -> "WatchdogTimer":
        """Start the watchdog timer."""
        self._timer = threading.Timer(
            interval=self.timeout_seconds,
            function=self._on_timeout,
        )
        self._timer.daemon = True  # don't block process exit
        self._timer.start()
        logger.debug(
            "WatchdogTimer started stage=%s thread_id=%s timeout=%.1fs",
            self.stage_name,
            self.thread_id,
            self.timeout_seconds,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """
        Cancel the timer on exit.

        Returns False so any exception propagates normally.
        If the timer already fired, the stage is already marked FAILED in
        the Ledger and DLQ — no additional action needed here.
        """
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

        if self.fired:
            logger.info(
                "WatchdogTimer: stage=%s thread_id=%s timed out — "
                "already dead-lettered, continuing pipeline",
                self.stage_name,
                self.thread_id,
            )

        # Suppress the exception if the timeout fired (stage already handled)
        if self.fired and exc_type is not None:
            logger.warning(
                "WatchdogTimer: suppressing exception after timeout "
                "stage=%s exc_type=%s",
                self.stage_name,
                exc_type.__name__ if exc_type else "None",
            )
            return True  # suppress exception — pipeline continues

        return False  # propagate any other exception

    # ------------------------------------------------------------------
    # Timeout handler
    # ------------------------------------------------------------------

    def _on_timeout(self) -> None:
        """
        Called by threading.Timer when the stage exceeds timeout_seconds.

        Marks stage FAILED in Ledger, inserts DLQ entry, sets self.fired=True.
        All errors are caught — this runs in a daemon thread and must not crash.
        """
        self.fired = True
        logger.warning(
            "WatchdogTimer FIRED stage=%s thread_id=%s timeout=%.1fs",
            self.stage_name,
            self.thread_id,
            self.timeout_seconds,
        )

        # Mark stage FAILED in Ledger
        try:
            self.ledger.mark_failed(
                self.thread_id,
                self.stage_name,
                f"TIMEOUT after {self.timeout_seconds:.0f}s",
            )
        except Exception as exc:
            logger.error(
                "WatchdogTimer: failed to mark ledger stage=%s thread_id=%s: %s",
                self.stage_name,
                self.thread_id,
                exc,
            )

        # Push to DLQ
        try:
            import json
            self.dlq.insert(
                thread_id=self.thread_id,
                failure_stage=self.stage_name,
                failure_reason="Timeout",
                run_context_json=json.dumps(self.run_context.to_log_dict()),
            )
        except Exception as exc:
            logger.error(
                "WatchdogTimer: failed to insert DLQ entry stage=%s thread_id=%s: %s",
                self.stage_name,
                self.thread_id,
                exc,
            )
