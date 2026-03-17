# src/zdi_mw/core/system_health.py
# ZDI Middleware — SystemHealthCheck
# Pre-flight probes for all 5 external dependencies.
# Total timeout: 10 seconds across all probes combined (via concurrent futures).
# Returns degraded_flags dict — pipeline continues in degraded mode, never aborts.

import concurrent.futures
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

# Default probe timeout: 10s total budget split across 5 probes.
# Individual probes are given 8s so we have headroom; the executor
# enforces the hard 10s wall clock.
_TOTAL_TIMEOUT_SECONDS = 10
_PER_PROBE_TIMEOUT = 8  # individual requests.get / API calls

# Keys must match what pipeline_runner passes to RunContext.degraded_flags
DEPENDENCY_KEYS = ("mail_down", "crm_down", "workdrive_down", "claude_down", "db_down")


class SystemHealthCheck:
    """
    Probes all 5 ZDI dependencies concurrently and returns a degraded_flags dict.

    Probes run in parallel using ThreadPoolExecutor. Total wall-clock time is
    capped at _TOTAL_TIMEOUT_SECONDS (10s). Any probe that times out or raises
    marks its dependency as down — never raises to the caller.

    Each probe method accepts an optional injector for testing (allows mocking
    the underlying API clients without monkey-patching).

    Usage:
        checker = SystemHealthCheck(
            mail_token=tokens["mail"],
            crm_token=tokens["crm"],
            workdrive_token=tokens["workdrive"],
            claude_api_key=os.environ["ANTHROPIC_API_KEY"],
            db_path=db_path,
        )
        flags = checker.run()
        # flags = {'mail_down': False, 'crm_down': False, ...}
    """

    def __init__(
        self,
        mail_token: str = "",
        crm_token: str = "",
        workdrive_token: str = "",
        claude_api_key: str = "",
        db_path: Optional[Path] = None,
        # Injectable overrides for testing — pass callables that replace
        # the real API calls.  Each should return True (healthy) or raise.
        _mail_probe: Optional[Callable[[], bool]] = None,
        _crm_probe: Optional[Callable[[], bool]] = None,
        _workdrive_probe: Optional[Callable[[], bool]] = None,
        _claude_probe: Optional[Callable[[], bool]] = None,
        _db_probe: Optional[Callable[[], bool]] = None,
    ) -> None:
        self._mail_token = mail_token
        self._crm_token = crm_token
        self._workdrive_token = workdrive_token
        self._claude_api_key = claude_api_key
        self._db_path = db_path

        # Injectable probe overrides (used in tests)
        self._mail_probe_override = _mail_probe
        self._crm_probe_override = _crm_probe
        self._workdrive_probe_override = _workdrive_probe
        self._claude_probe_override = _claude_probe
        self._db_probe_override = _db_probe

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def run(self) -> Dict[str, bool]:
        """
        Run all 5 dependency probes concurrently within the 10-second budget.

        Returns:
            Dict with keys mail_down, crm_down, workdrive_down, claude_down,
            db_down — True means that dependency is unavailable.
        """
        flags: Dict[str, bool] = {k: False for k in DEPENDENCY_KEYS}

        probe_map: Dict[str, Callable[[], bool]] = {
            "mail_down": self._probe_mail,
            "crm_down": self._probe_crm,
            "workdrive_down": self._probe_workdrive,
            "claude_down": self._probe_claude,
            "db_down": self._probe_db,
        }

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_key = {
                executor.submit(probe_fn): key
                for key, probe_fn in probe_map.items()
            }
            done, not_done = concurrent.futures.wait(
                future_to_key.keys(),
                timeout=_TOTAL_TIMEOUT_SECONDS,
            )

            # Probes that didn't finish within total timeout
            for future in not_done:
                key = future_to_key[future]
                logger.warning("Health probe timed out key=%s — marking as down", key)
                flags[key] = True
                future.cancel()

            # Probes that completed — check for exceptions
            for future in done:
                key = future_to_key[future]
                try:
                    healthy = future.result()
                    if not healthy:
                        logger.warning("Health probe returned unhealthy key=%s", key)
                        flags[key] = True
                except Exception as exc:
                    logger.warning(
                        "Health probe failed key=%s error=%s — marking as down",
                        key,
                        exc,
                    )
                    flags[key] = True

        self._log_summary(flags)
        return flags

    # ------------------------------------------------------------------
    # Individual probes
    # ------------------------------------------------------------------

    def _probe_mail(self) -> bool:
        """
        Probe Zoho Mail: list top-level folders.
        Healthy = HTTP 200 with non-error body.
        """
        if self._mail_probe_override is not None:
            return self._mail_probe_override()

        import requests  # verified: pypi.org/project/requests

        if not self._mail_token:
            logger.warning("Mail token empty — probe skipped, marking down")
            return False

        try:
            resp = requests.get(
                "https://mail.zoho.com/api/accounts",
                headers={"Authorization": f"Zoho-oauthtoken {self._mail_token}"},
                timeout=_PER_PROBE_TIMEOUT,
            )
            if resp.status_code == 200:
                return True
            logger.warning("Mail probe non-200 status=%s", resp.status_code)
            return False
        except requests.RequestException as exc:
            raise RuntimeError(f"Mail probe request failed: {exc}") from exc

    def _probe_crm(self) -> bool:
        """
        Probe Zoho CRM: whoami call (/crm/v8/users?type=CurrentUser).
        Healthy = HTTP 200.
        """
        if self._crm_probe_override is not None:
            return self._crm_probe_override()

        import requests

        if not self._crm_token:
            logger.warning("CRM token empty — probe skipped, marking down")
            return False

        try:
            resp = requests.get(
                "https://www.zohoapis.com/crm/v8/users?type=CurrentUser",
                headers={"Authorization": f"Zoho-oauthtoken {self._crm_token}"},
                timeout=_PER_PROBE_TIMEOUT,
            )
            if resp.status_code == 200:
                return True
            logger.warning("CRM probe non-200 status=%s", resp.status_code)
            return False
        except requests.RequestException as exc:
            raise RuntimeError(f"CRM probe request failed: {exc}") from exc

    def _probe_workdrive(self) -> bool:
        """
        Probe Zoho WorkDrive: list root workspace.
        Healthy = HTTP 200.
        """
        if self._workdrive_probe_override is not None:
            return self._workdrive_probe_override()

        import requests

        if not self._workdrive_token:
            logger.warning("WorkDrive token empty — probe skipped, marking down")
            return False

        try:
            resp = requests.get(
                "https://workdrive.zoho.com/api/v1/privatespace",
                headers={"Authorization": f"Zoho-oauthtoken {self._workdrive_token}"},
                timeout=_PER_PROBE_TIMEOUT,
            )
            if resp.status_code == 200:
                return True
            logger.warning("WorkDrive probe non-200 status=%s", resp.status_code)
            return False
        except requests.RequestException as exc:
            raise RuntimeError(f"WorkDrive probe request failed: {exc}") from exc

    def _probe_claude(self) -> bool:
        """
        Probe Claude API: list models endpoint.
        Healthy = HTTP 200.
        """
        if self._claude_probe_override is not None:
            return self._claude_probe_override()

        import requests

        if not self._claude_api_key:
            logger.warning("Claude API key empty — probe skipped, marking down")
            return False

        try:
            resp = requests.get(
                "https://api.anthropic.com/v1/models",
                headers={
                    "x-api-key": self._claude_api_key,
                    "anthropic-version": "2023-06-01",
                },
                timeout=_PER_PROBE_TIMEOUT,
            )
            if resp.status_code == 200:
                return True
            logger.warning("Claude probe non-200 status=%s", resp.status_code)
            return False
        except requests.RequestException as exc:
            raise RuntimeError(f"Claude probe request failed: {exc}") from exc

    def _probe_db(self) -> bool:
        """
        Probe SQLite: perform a write test (INSERT + DELETE on pipeline_locks).
        Healthy = write succeeds without error.
        """
        if self._db_probe_override is not None:
            return self._db_probe_override()

        from src.zdi_mw.state.db import get_connection

        try:
            conn = get_connection(self._db_path)
            test_lock = "__health_probe__"
            with conn:
                # Clean up any leftover probe row
                conn.execute(
                    "DELETE FROM pipeline_locks WHERE lock_name = ?", (test_lock,)
                )
                conn.execute(
                    "INSERT INTO pipeline_locks (lock_name, run_id, acquired_at, pid) "
                    "VALUES (?, ?, ?, ?)",
                    (
                        test_lock,
                        "health_probe",
                        datetime.now(timezone.utc).isoformat(),
                        os.getpid(),
                    ),
                )
                conn.execute(
                    "DELETE FROM pipeline_locks WHERE lock_name = ?", (test_lock,)
                )
            conn.close()
            return True
        except sqlite3.Error as exc:
            raise RuntimeError(f"DB probe write test failed: {exc}") from exc

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _log_summary(flags: Dict[str, bool]) -> None:
        """Log a one-line health summary."""
        down = [k for k, v in flags.items() if v]
        if not down:
            logger.info("SystemHealthCheck: all dependencies healthy")
        else:
            logger.warning(
                "SystemHealthCheck: degraded dependencies: %s",
                ", ".join(down),
            )
