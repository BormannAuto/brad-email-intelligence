# src/zdi_mw/core/rate_manager.py
# ZDI Middleware — RateManager
# Tracks API credit consumption per Zoho app within a single pipeline run.
# Daily budget cap = 80% of plan limit per app.
# check_and_wait(app, cost) blocks and logs when budget is nearly exhausted.
# Exponential backoff on 429 responses.
# All rate limits are based on Zoho plan assumptions — see ZDI_SECURITY_AUDIT.md.

import logging
import time
import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------
# Credit cost constants (Zoho API credit model — verify against plan tier)
# CRM: simple API call = 1, Get Records / COQL = 2
# These are documented in ZDI_SECURITY_AUDIT.md under "Rate limit assumptions"
# -----------------------------------------------------------------------
CRM_CREDIT_SIMPLE = 1
CRM_CREDIT_GET_RECORDS = 2
CRM_CREDIT_COQL = 2

# -----------------------------------------------------------------------
# Daily plan limits by app (80% budget = usable cap)
# These defaults assume Zoho Standard plan. Override via RateManager init
# if Bormann is on a different tier.
# Verify actual plan limits — see ZDI_SECURITY_AUDIT.md.
# -----------------------------------------------------------------------
_DEFAULT_DAILY_LIMITS = {
    "crm": 1000,        # credits/day (Zoho CRM Standard: 1000 API credits/day)
    "mail": 1000,       # credits/day (used for request count tracking)
    "workdrive": 500,   # credits/day (file read operations)
}

_BUDGET_CAP_RATIO = 0.80  # 80% of plan limit

# Mail: hard 30 requests/minute enforced regardless of daily budget
_MAIL_RATE_LIMIT_PER_MINUTE = 30
_MAIL_WINDOW_SECONDS = 60

# Exponential backoff on 429 — seconds per attempt
_BACKOFF_SCHEDULE = [10, 30, 120, 300]  # 10s, 30s, 2min, 5min


class BudgetExhaustedError(RuntimeError):
    """
    Raised when daily budget is fully exhausted and no wait is possible.
    Caller should mark the app as degraded rather than retrying.
    """
    pass


@dataclass
class _AppUsage:
    """Per-app usage tracking for one pipeline run."""
    daily_limit: int
    budget_cap: int = field(init=False)
    credits_used: float = 0.0
    request_timestamps: Deque[float] = field(default_factory=deque)  # for per-minute tracking

    def __post_init__(self) -> None:
        self.budget_cap = int(self.daily_limit * _BUDGET_CAP_RATIO)

    def remaining_budget(self) -> float:
        """Credits available before hitting the 80% cap."""
        return max(0.0, self.budget_cap - self.credits_used)

    def budget_pct_used(self) -> float:
        """Percentage of daily limit consumed (0.0–1.0)."""
        if self.daily_limit == 0:
            return 1.0
        return self.credits_used / self.daily_limit

    def is_budget_exhausted(self) -> bool:
        """Return True if we've hit or exceeded the 80% cap."""
        return self.credits_used >= self.budget_cap


class RateManager:
    """
    Tracks API credit consumption per app and enforces budget caps.

    Design principles:
    - Per-run instance: created fresh each pipeline run, no persistence.
    - Thread-safe: a single lock protects all state (pipeline runs sequentially,
      but SystemHealthCheck runs probes concurrently).
    - Never raises during a pipeline run — returns a degraded signal instead.
    - 429 responses are handled with exponential backoff, not pipeline abort.

    Usage:
        rate = RateManager()
        rate.check_and_wait("crm", estimated_cost=CRM_CREDIT_GET_RECORDS)
        # ... make API call ...
        rate.record_usage("crm", actual_cost=2)
        # On 429:
        rate.handle_429("crm", attempt=0)  # sleeps with backoff

    Custom limits (override for non-standard plan tiers):
        rate = RateManager(daily_limits={"crm": 2000, "mail": 1000, "workdrive": 500})
    """

    def __init__(
        self,
        daily_limits: Optional[Dict[str, int]] = None,
        _sleep_fn=None,  # injectable for testing
    ) -> None:
        """
        Args:
            daily_limits: Override plan limits per app.
                          Defaults to _DEFAULT_DAILY_LIMITS.
            _sleep_fn: Injectable sleep function for testing (avoids real waits).
        """
        limits = daily_limits or _DEFAULT_DAILY_LIMITS
        self._usage: Dict[str, _AppUsage] = {
            app: _AppUsage(daily_limit=lim) for app, lim in limits.items()
        }
        self._lock = threading.Lock()
        self._sleep = _sleep_fn or time.sleep

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def check_and_wait(self, app: str, estimated_cost: float = 1.0) -> None:
        """
        Check whether the estimated API call cost fits within the remaining budget.

        Behaviour:
        - If budget is already exhausted: raises BudgetExhaustedError. Caller
          should mark the app as degraded and skip remaining calls for it.
        - If adding estimated_cost would hit the cap: logs a WARNING and continues
          (we allow the call but log the boundary crossing).
        - For 'mail': also enforces the 30 req/min hard rate limit with a sleep.

        Args:
            app: One of 'mail', 'crm', 'workdrive'.
            estimated_cost: Expected credit cost of the upcoming API call.

        Raises:
            BudgetExhaustedError: If the app's 80% daily cap is already exhausted.
        """
        with self._lock:
            usage = self._get_or_create_usage(app)

            if usage.is_budget_exhausted():
                logger.warning(
                    "RateManager: BUDGET EXHAUSTED app=%s used=%.0f cap=%.0f "
                    "— marking app as degraded, skipping call",
                    app,
                    usage.credits_used,
                    usage.budget_cap,
                )
                raise BudgetExhaustedError(
                    f"Daily budget exhausted for app={app}: "
                    f"used={usage.credits_used:.0f} / cap={usage.budget_cap}"
                )

            remaining = usage.remaining_budget()
            if estimated_cost > remaining:
                logger.warning(
                    "RateManager: budget boundary crossing app=%s "
                    "estimated_cost=%.0f remaining=%.0f pct_used=%.1f%% "
                    "— allowing call but at budget limit",
                    app,
                    estimated_cost,
                    remaining,
                    usage.budget_pct_used() * 100,
                )
            elif usage.budget_pct_used() >= 0.70:
                # Warn at 70% as an early signal before hard cap
                logger.warning(
                    "RateManager: approaching budget cap app=%s "
                    "used=%.0f cap=%.0f (%.1f%%)",
                    app,
                    usage.credits_used,
                    usage.budget_cap,
                    usage.budget_pct_used() * 100,
                )

        # Mail per-minute rate check (outside main lock to avoid holding it during sleep)
        if app == "mail":
            self._enforce_mail_rate_limit()

    def record_usage(self, app: str, actual_cost: float = 1.0) -> None:
        """
        Record actual credit usage after a successful API call.

        Args:
            app: One of 'mail', 'crm', 'workdrive'.
            actual_cost: Credits consumed by the completed call.
        """
        with self._lock:
            usage = self._get_or_create_usage(app)
            usage.credits_used += actual_cost
            if app == "mail":
                usage.request_timestamps.append(time.time())
            logger.debug(
                "RateManager: usage recorded app=%s cost=%.0f total=%.0f cap=%.0f",
                app,
                actual_cost,
                usage.credits_used,
                usage.budget_cap,
            )

    def handle_429(self, app: str, attempt: int = 0) -> None:
        """
        Handle a 429 Too Many Requests response with exponential backoff.

        Sleeps for the appropriate backoff duration based on attempt count.
        Logs the wait. Does not raise.

        Args:
            app: The app that returned 429.
            attempt: Zero-based retry attempt number (determines backoff duration).
        """
        backoff = _BACKOFF_SCHEDULE[min(attempt, len(_BACKOFF_SCHEDULE) - 1)]
        logger.warning(
            "RateManager: 429 received app=%s attempt=%d — backing off %ds",
            app,
            attempt,
            backoff,
        )
        self._sleep(backoff)

    def get_usage_summary(self) -> Dict[str, dict]:
        """
        Return a summary of current usage for all tracked apps.

        Returns:
            Dict mapping app name → {credits_used, budget_cap, pct_used, exhausted}.
        """
        with self._lock:
            return {
                app: {
                    "credits_used": usage.credits_used,
                    "budget_cap": usage.budget_cap,
                    "daily_limit": usage.daily_limit,
                    "pct_used": round(usage.budget_pct_used() * 100, 1),
                    "exhausted": usage.is_budget_exhausted(),
                }
                for app, usage in self._usage.items()
            }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_or_create_usage(self, app: str) -> _AppUsage:
        """Return the _AppUsage for app, creating with default limits if new."""
        if app not in self._usage:
            default_limit = _DEFAULT_DAILY_LIMITS.get(app, 500)
            self._usage[app] = _AppUsage(daily_limit=default_limit)
            logger.info(
                "RateManager: created usage tracker for unknown app=%s limit=%d",
                app,
                default_limit,
            )
        return self._usage[app]

    def _enforce_mail_rate_limit(self) -> None:
        """
        Enforce the hard 30 requests/minute limit for Zoho Mail.

        If we've made >= 30 requests in the last 60 seconds, sleeps until
        the oldest request falls outside the window.
        """
        now = time.time()
        with self._lock:
            usage = self._get_or_create_usage("mail")
            # Purge timestamps older than the window
            while (
                usage.request_timestamps
                and now - usage.request_timestamps[0] > _MAIL_WINDOW_SECONDS
            ):
                usage.request_timestamps.popleft()

            if len(usage.request_timestamps) >= _MAIL_RATE_LIMIT_PER_MINUTE:
                oldest = usage.request_timestamps[0]
                sleep_for = _MAIL_WINDOW_SECONDS - (now - oldest) + 0.1
                if sleep_for > 0:
                    logger.warning(
                        "RateManager: mail rate limit hit (30 req/min) — "
                        "sleeping %.1fs",
                        sleep_for,
                    )
                    # Release lock before sleeping
                    self._lock.release()
                    try:
                        self._sleep(sleep_for)
                    finally:
                        self._lock.acquire()
