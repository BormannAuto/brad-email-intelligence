# tests/test_phase2.py
# ZDI Middleware — Phase 2 Verification Gate
#
# Verification criteria (spec Section 4 Phase 2):
#   1. Tokens refresh without error (mocked HTTP)
#   2. RateManager blocks a call when budget is exhausted and logs the block
#   3. All three OAuth flows operate independently
#   4. Tokens NEVER appear in any log output
#
# No real API calls are made. All network I/O is mocked.

import logging
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))


# ===========================================================================
# Helpers
# ===========================================================================

def _make_mock_post(access_token: str = "tok_abc123", expires_in: int = 3600):
    """Return a mock HTTP post function that simulates a successful Zoho OAuth response."""
    def _post(url, data, timeout):
        resp = SimpleNamespace()
        resp.status_code = 200
        resp.json = lambda: {"access_token": access_token, "expires_in": expires_in}
        return resp
    return _post


def _make_failing_post(status_code: int = 401):
    """Return a mock HTTP post function that simulates a failed Zoho OAuth response."""
    def _post(url, data, timeout):
        resp = SimpleNamespace()
        resp.status_code = status_code
        resp.json = lambda: {"error": "invalid_client"}
        return resp
    return _post


def _set_env_creds(monkeypatch):
    """Set all 9 required Zoho credential env vars to dummy values."""
    for app in ("MAIL", "CRM", "WORKDRIVE"):
        monkeypatch.setenv(f"ZOHO_{app}_CLIENT_ID", f"client_{app.lower()}")
        monkeypatch.setenv(f"ZOHO_{app}_CLIENT_SECRET", f"secret_{app.lower()}")
        monkeypatch.setenv(f"ZOHO_{app}_REFRESH_TOKEN", f"refresh_{app.lower()}")


# ===========================================================================
# Gate 1 — Tokens refresh without error
# ===========================================================================

class TestAuthManagerRefresh:
    """Verify that refresh_all() works correctly for all three apps."""

    def test_refresh_all_success(self, monkeypatch):
        """All three tokens refresh when HTTP mock returns 200."""
        from src.zdi_mw.core.auth_manager import AuthManager

        _set_env_creds(monkeypatch)
        auth = AuthManager(_http_post=_make_mock_post("tok_xyz789"))
        auth.refresh_all()

        assert auth.is_loaded("mail"), "mail token not loaded after refresh"
        assert auth.is_loaded("crm"), "crm token not loaded after refresh"
        assert auth.is_loaded("workdrive"), "workdrive token not loaded after refresh"

    def test_get_token_returns_correct_value(self, monkeypatch):
        """get_token() returns the access token from the mock response."""
        from src.zdi_mw.core.auth_manager import AuthManager

        _set_env_creds(monkeypatch)
        auth = AuthManager(_http_post=_make_mock_post("THE_TOKEN"))
        auth.refresh_all()

        assert auth.get_token("mail") == "THE_TOKEN"
        assert auth.get_token("crm") == "THE_TOKEN"
        assert auth.get_token("workdrive") == "THE_TOKEN"

    def test_refresh_fails_on_bad_status(self, monkeypatch):
        """refresh_all() raises AuthError when Zoho returns non-200."""
        from src.zdi_mw.core.auth_manager import AuthManager, AuthError

        _set_env_creds(monkeypatch)
        auth = AuthManager(_http_post=_make_failing_post(401))
        with pytest.raises(AuthError):
            auth.refresh_all()

    def test_missing_env_vars_raises_auth_error(self, monkeypatch):
        """load_credentials() raises AuthError when env vars are missing."""
        from src.zdi_mw.core.auth_manager import AuthManager, AuthError

        # Remove all Zoho env vars
        for app in ("MAIL", "CRM", "WORKDRIVE"):
            for field in ("CLIENT_ID", "CLIENT_SECRET", "REFRESH_TOKEN"):
                monkeypatch.delenv(f"ZOHO_{app}_{field}", raising=False)

        auth = AuthManager(_http_post=_make_mock_post())
        with pytest.raises(AuthError) as exc_info:
            auth.load_credentials()

        assert "Missing required environment variables" in str(exc_info.value)

    def test_get_token_invalid_app_raises(self, monkeypatch):
        """get_token() raises AuthError for an unknown app name."""
        from src.zdi_mw.core.auth_manager import AuthManager, AuthError

        _set_env_creds(monkeypatch)
        auth = AuthManager(_http_post=_make_mock_post())
        auth.refresh_all()

        with pytest.raises(AuthError):
            auth.get_token("fax_machine")


# ===========================================================================
# Gate 2 — Three OAuth flows operate independently
# ===========================================================================

class TestAuthManagerIsolation:
    """Verify that each OAuth flow is independent — different tokens, no sharing."""

    def test_three_flows_get_separate_tokens(self, monkeypatch):
        """Each app gets its own token even if mock returns different values per call."""
        from src.zdi_mw.core.auth_manager import AuthManager

        # Track which app's credentials were used in each POST call
        call_log = []

        def _post(url, data, timeout):
            # Identify app by which client_id was sent
            call_log.append(data["client_id"])
            resp = SimpleNamespace()
            resp.status_code = 200
            # Return a token that encodes the client_id so we can verify isolation
            resp.json = lambda cid=data["client_id"]: {
                "access_token": f"tok_for_{cid}",
                "expires_in": 3600,
            }
            return resp

        _set_env_creds(monkeypatch)
        auth = AuthManager(_http_post=_post)
        auth.refresh_all()

        # Three separate HTTP calls were made
        assert len(call_log) == 3, f"Expected 3 token refresh calls, got {len(call_log)}"

        # Each app got a token tied to its own client_id
        mail_token = auth.get_token("mail")
        crm_token = auth.get_token("crm")
        wd_token = auth.get_token("workdrive")

        assert mail_token != crm_token, "mail and crm tokens must be independent"
        assert crm_token != wd_token, "crm and workdrive tokens must be independent"
        assert "client_mail" in mail_token
        assert "client_crm" in crm_token
        assert "client_workdrive" in wd_token

    def test_one_app_failure_does_not_hide_others(self, monkeypatch):
        """If one app's refresh fails, AuthError is raised and reports the failure."""
        from src.zdi_mw.core.auth_manager import AuthManager, AuthError

        def _selective_post(url, data, timeout):
            # Only CRM fails
            if data["client_id"] == "client_crm":
                resp = SimpleNamespace()
                resp.status_code = 400
                resp.json = lambda: {"error": "invalid_grant"}
                return resp
            resp = SimpleNamespace()
            resp.status_code = 200
            resp.json = lambda: {"access_token": "tok_ok", "expires_in": 3600}
            return resp

        _set_env_creds(monkeypatch)
        auth = AuthManager(_http_post=_selective_post)

        with pytest.raises(AuthError) as exc_info:
            auth.refresh_all()

        # Error message must identify the failing app
        assert "crm" in str(exc_info.value).lower() or "1 app" in str(exc_info.value).lower()


# ===========================================================================
# Gate 3 — RateManager blocks when budget is exhausted
# ===========================================================================

class TestRateManagerBudget:
    """Verify credit tracking, 80% cap, and BudgetExhaustedError."""

    def test_usage_accumulates(self):
        """record_usage() accumulates credits correctly."""
        from src.zdi_mw.core.rate_manager import RateManager

        rate = RateManager(daily_limits={"crm": 100})
        rate.record_usage("crm", actual_cost=10)
        rate.record_usage("crm", actual_cost=25)

        summary = rate.get_usage_summary()
        assert summary["crm"]["credits_used"] == 35
        assert summary["crm"]["budget_cap"] == 80  # 80% of 100

    def test_budget_not_exhausted_under_cap(self):
        """check_and_wait() does not raise when under the 80% cap."""
        from src.zdi_mw.core.rate_manager import RateManager

        rate = RateManager(daily_limits={"crm": 100}, _sleep_fn=lambda s: None)
        rate.record_usage("crm", actual_cost=50)  # 50% used, cap is 80
        rate.check_and_wait("crm", estimated_cost=1)  # should not raise

    def test_budget_exhausted_raises(self):
        """check_and_wait() raises BudgetExhaustedError when cap is hit."""
        from src.zdi_mw.core.rate_manager import RateManager, BudgetExhaustedError

        rate = RateManager(daily_limits={"crm": 100}, _sleep_fn=lambda s: None)
        rate.record_usage("crm", actual_cost=80)  # exactly at cap

        with pytest.raises(BudgetExhaustedError):
            rate.check_and_wait("crm", estimated_cost=1)

    def test_budget_exhausted_logs_warning(self, caplog):
        """check_and_wait() logs a WARNING when budget is exhausted."""
        from src.zdi_mw.core.rate_manager import RateManager, BudgetExhaustedError

        rate = RateManager(daily_limits={"crm": 100}, _sleep_fn=lambda s: None)
        rate.record_usage("crm", actual_cost=80)

        with caplog.at_level(logging.WARNING):
            with pytest.raises(BudgetExhaustedError):
                rate.check_and_wait("crm", estimated_cost=1)

        assert any("BUDGET EXHAUSTED" in r.message for r in caplog.records), (
            "Expected 'BUDGET EXHAUSTED' in log output when budget is exhausted"
        )

    def test_degraded_mode_not_crash_on_exhaustion(self):
        """
        Pipeline should catch BudgetExhaustedError and mark app degraded,
        not crash. This test verifies the error is catchable and distinct.
        """
        from src.zdi_mw.core.rate_manager import RateManager, BudgetExhaustedError

        rate = RateManager(daily_limits={"mail": 50}, _sleep_fn=lambda s: None)
        rate.record_usage("mail", actual_cost=40)  # at cap (80% of 50)

        degraded = False
        try:
            rate.check_and_wait("mail", estimated_cost=1)
        except BudgetExhaustedError:
            degraded = True  # pipeline would set degraded_flags["mail_down"] = True

        assert degraded, "BudgetExhaustedError must be catchable for degraded mode"

    def test_80_percent_cap_calculation(self):
        """Budget cap is exactly 80% of the daily limit."""
        from src.zdi_mw.core.rate_manager import RateManager

        rate = RateManager(daily_limits={"crm": 1000})
        summary = rate.get_usage_summary()
        assert summary["crm"]["budget_cap"] == 800, (
            f"Expected budget_cap=800 (80% of 1000), got {summary['crm']['budget_cap']}"
        )

    def test_exponential_backoff_on_429(self):
        """handle_429() calls sleep with backoff schedule values."""
        from src.zdi_mw.core.rate_manager import RateManager, _BACKOFF_SCHEDULE

        sleep_calls = []
        rate = RateManager(_sleep_fn=lambda s: sleep_calls.append(s))

        rate.handle_429("crm", attempt=0)
        rate.handle_429("crm", attempt=1)
        rate.handle_429("crm", attempt=2)

        assert sleep_calls[0] == _BACKOFF_SCHEDULE[0], "Attempt 0 backoff mismatch"
        assert sleep_calls[1] == _BACKOFF_SCHEDULE[1], "Attempt 1 backoff mismatch"
        assert sleep_calls[2] == _BACKOFF_SCHEDULE[2], "Attempt 2 backoff mismatch"

    def test_three_apps_tracked_independently(self):
        """CRM, mail, and workdrive usage counters are independent."""
        from src.zdi_mw.core.rate_manager import RateManager

        rate = RateManager(
            daily_limits={"crm": 100, "mail": 200, "workdrive": 150},
            _sleep_fn=lambda s: None,
        )
        rate.record_usage("crm", actual_cost=50)
        rate.record_usage("mail", actual_cost=10)
        rate.record_usage("workdrive", actual_cost=30)

        summary = rate.get_usage_summary()
        assert summary["crm"]["credits_used"] == 50
        assert summary["mail"]["credits_used"] == 10
        assert summary["workdrive"]["credits_used"] == 30

    def test_simulate_full_budget_exhaustion_workflow(self, caplog):
        """
        Simulate exhausting CRM budget across multiple calls.
        Verify: pipeline degrades for CRM while mail still works.
        """
        from src.zdi_mw.core.rate_manager import RateManager, BudgetExhaustedError

        rate = RateManager(
            daily_limits={"crm": 100, "mail": 1000},
            _sleep_fn=lambda s: None,
        )

        # Simulate many CRM calls exhausting the budget
        for _ in range(8):
            rate.check_and_wait("crm", estimated_cost=10)
            rate.record_usage("crm", actual_cost=10)

        # CRM is now exhausted (80 credits used, cap = 80)
        with pytest.raises(BudgetExhaustedError):
            rate.check_and_wait("crm", estimated_cost=1)

        # Mail is unaffected
        rate.check_and_wait("mail", estimated_cost=1)  # must not raise
        summary = rate.get_usage_summary()
        assert summary["crm"]["exhausted"] is True
        assert summary["mail"]["exhausted"] is False


# ===========================================================================
# Gate 4 — Token values NEVER appear in log output
# ===========================================================================

class TestTokenSecurityInLogs:
    """
    Verify that no token value, client_secret, or refresh_token ever
    appears in any log record produced by auth_manager.py.
    """

    def test_token_value_not_in_logs(self, monkeypatch, caplog):
        """The actual access token value must not appear in any log message."""
        from src.zdi_mw.core.auth_manager import AuthManager

        _set_env_creds(monkeypatch)
        secret_token = "SUPER_SECRET_TOKEN_VALUE_12345"
        auth = AuthManager(_http_post=_make_mock_post(secret_token))

        with caplog.at_level(logging.DEBUG, logger="src.zdi_mw.core.auth_manager"):
            auth.refresh_all()
            _ = auth.get_token("mail")
            _ = auth.get_token("crm")
            _ = auth.get_token("workdrive")

        all_log_text = " ".join(r.message for r in caplog.records)
        assert secret_token not in all_log_text, (
            f"SECURITY VIOLATION: token value '{secret_token}' found in log output"
        )

    def test_client_secret_not_in_logs(self, monkeypatch, caplog):
        """Client secrets must not appear in any log message."""
        from src.zdi_mw.core.auth_manager import AuthManager

        _set_env_creds(monkeypatch)
        auth = AuthManager(_http_post=_make_mock_post())

        with caplog.at_level(logging.DEBUG, logger="src.zdi_mw.core.auth_manager"):
            auth.refresh_all()

        all_log_text = " ".join(r.message for r in caplog.records)
        for app in ("mail", "crm", "workdrive"):
            secret = f"secret_{app}"
            assert secret not in all_log_text, (
                f"SECURITY VIOLATION: client_secret for {app} found in logs"
            )

    def test_refresh_token_not_in_logs(self, monkeypatch, caplog):
        """Refresh tokens must not appear in any log message."""
        from src.zdi_mw.core.auth_manager import AuthManager

        _set_env_creds(monkeypatch)
        auth = AuthManager(_http_post=_make_mock_post())

        with caplog.at_level(logging.DEBUG, logger="src.zdi_mw.core.auth_manager"):
            auth.refresh_all()

        all_log_text = " ".join(r.message for r in caplog.records)
        for app in ("mail", "crm", "workdrive"):
            rt = f"refresh_{app}"
            assert rt not in all_log_text, (
                f"SECURITY VIOLATION: refresh_token for {app} found in logs"
            )

    def test_token_not_in_auth_error_message(self, monkeypatch):
        """AuthError messages must not include credential values."""
        from src.zdi_mw.core.auth_manager import AuthManager, AuthError

        _set_env_creds(monkeypatch)

        def _failing_post(url, data, timeout):
            resp = SimpleNamespace()
            resp.status_code = 401
            resp.json = lambda: {"error": "invalid_client"}
            return resp

        auth = AuthManager(_http_post=_failing_post)
        with pytest.raises(AuthError) as exc_info:
            auth.refresh_all()

        error_text = str(exc_info.value)
        for app in ("mail", "crm", "workdrive"):
            assert f"secret_{app}" not in error_text
            assert f"refresh_{app}" not in error_text
