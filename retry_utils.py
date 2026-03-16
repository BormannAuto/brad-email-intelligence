"""
retry_utils.py
Bormann Marketing — Email Intelligence System v3
Exponential backoff retry decorator for all Zoho API calls.

Security rationale: Zoho API calls can fail transiently (rate limits, 5xx errors,
network blips). Without retry, a single transient failure aborts an entire pipeline
zone. With capped retry + exponential backoff, transient failures heal automatically
without runaway loops or hammering the API.

Design decisions:
  - Max 3 retries (4 total attempts) — aggressive enough to recover; bounded enough
    to not blow the 20-minute pipeline timeout.
  - Backoff: 2s, 4s, 8s with ±0.5s jitter — standard exponential, avoids thundering herd.
  - Retryable: 429, 500, 502, 503, 504 — transient. Non-retryable: 400, 401, 403, 404.
  - On final failure: re-raise the original exception so the caller can handle it.
  - Per-call timeout: 30 seconds. Zoho API calls should never hang indefinitely.
"""

import logging
import time
import random
import functools
from typing import Callable, Any

logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES        = 3        # Retries after the initial attempt (4 total attempts)
BASE_BACKOFF_S     = 2.0      # Initial backoff in seconds
BACKOFF_MULTIPLIER = 2.0      # Exponential multiplier
JITTER_S           = 0.5      # ±jitter added to each backoff to avoid thundering herd

# HTTP status codes that are safe to retry
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Exception types that indicate a transient network error (always retry)
TRANSIENT_EXCEPTION_TYPES = (
    ConnectionError,
    TimeoutError,
    OSError,
)


class ZohoRateLimitError(Exception):
    """Raised when Zoho returns 429 Too Many Requests."""
    pass


class ZohoAPIError(Exception):
    """Raised when Zoho returns a non-retryable error."""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(f"Zoho API error {status_code}: {message}")


def _backoff_seconds(attempt: int) -> float:
    """
    Return backoff duration for the given attempt number (0-indexed).
    attempt=0 → ~2s, attempt=1 → ~4s, attempt=2 → ~8s
    """
    base = BASE_BACKOFF_S * (BACKOFF_MULTIPLIER ** attempt)
    jitter = random.uniform(-JITTER_S, JITTER_S)
    return max(0.1, base + jitter)


def with_retry(func: Callable = None, *, caller: str = "unknown") -> Callable:
    """
    Decorator — wraps a function with retry + exponential backoff.

    Usage:
        @with_retry(caller="zoho_mail")
        def fetch_emails(token):
            ...

    The wrapped function must raise ZohoRateLimitError, ZohoAPIError,
    or a TRANSIENT_EXCEPTION_TYPES exception to trigger a retry.

    Non-retryable ZohoAPIError (4xx, except 429) is re-raised immediately.
    """
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            for attempt in range(MAX_RETRIES + 1):  # attempts: 0, 1, 2, 3
                try:
                    return fn(*args, **kwargs)
                except ZohoAPIError as exc:
                    # Non-retryable client errors — fail immediately
                    if exc.status_code not in RETRYABLE_STATUS_CODES:
                        logger.error(
                            f"[retry_utils] {caller}: non-retryable API error "
                            f"(HTTP {exc.status_code}) — not retrying."
                        )
                        raise
                    last_exception = exc
                    _log_retry(caller, attempt, exc)
                except ZohoRateLimitError as exc:
                    last_exception = exc
                    _log_retry(caller, attempt, exc)
                except TRANSIENT_EXCEPTION_TYPES as exc:
                    last_exception = exc
                    _log_retry(caller, attempt, exc)

                # If we've exhausted retries, stop
                if attempt >= MAX_RETRIES:
                    break

                sleep_s = _backoff_seconds(attempt)
                logger.info(f"[retry_utils] {caller}: backing off {sleep_s:.1f}s before retry {attempt + 1}/{MAX_RETRIES}.")
                time.sleep(sleep_s)

            logger.error(f"[retry_utils] {caller}: all {MAX_RETRIES + 1} attempts failed.")
            raise last_exception  # Re-raise the last exception for caller to handle

        return wrapper

    # Allow both @with_retry and @with_retry(caller="foo") usage
    if func is not None:
        # Called without arguments: @with_retry
        return decorator(func)
    return decorator


def _log_retry(caller: str, attempt: int, exc: Exception) -> None:
    """Log a retry attempt without leaking sensitive content."""
    exc_type = type(exc).__name__
    if attempt < MAX_RETRIES:
        logger.warning(
            f"[retry_utils] {caller}: attempt {attempt + 1}/{MAX_RETRIES + 1} failed "
            f"({exc_type}). Will retry."
        )
    else:
        logger.error(
            f"[retry_utils] {caller}: attempt {attempt + 1}/{MAX_RETRIES + 1} failed "
            f"({exc_type}). No more retries."
        )


def check_response_status(response, caller: str = "unknown") -> None:
    """
    Check a requests.Response object and raise appropriate exception.

    Call this immediately after every requests.get() / requests.post() call
    to ensure errors surface with the correct retry classification.

    Args:
        response: requests.Response object
        caller: module name for log context
    """
    if response.status_code == 200:
        return
    if response.status_code == 429:
        raise ZohoRateLimitError(f"{caller}: rate limited (429). Will retry.")
    if response.status_code in RETRYABLE_STATUS_CODES:
        raise ZohoAPIError(response.status_code, f"{caller}: server error — eligible for retry.")
    # 4xx non-429 — non-retryable
    raise ZohoAPIError(response.status_code, f"{caller}: client error — not retrying.")
