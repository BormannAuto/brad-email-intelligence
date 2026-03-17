# src/zdi_mw/core/auth_manager.py
# ZDI Middleware — AuthManager
# Three isolated OAuth 2.0 refresh flows: Mail, CRM, WorkDrive.
# Credentials live in .env only — never in any file committed to the repo.
# Tokens are refreshed at RunContext creation (startup), not on demand.
# SECURITY: tokens are NEVER logged, printed, or serialised to any file.

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional

import requests  # verified: pypi.org/project/requests

logger = logging.getLogger(__name__)

# Zoho OAuth token endpoint — same for all three apps
_ZOHO_TOKEN_URL = "https://accounts.zoho.com/oauth/v2/token"

# Refresh a token this many seconds before it actually expires (safety buffer)
_EXPIRY_BUFFER_SECONDS = 300  # 5 minutes

# --- Environment variable names per OAuth flow ---
# Each flow has its own isolated credential set. Never share secrets between apps.
_ENV_KEYS = {
    "mail": {
        "client_id": "ZOHO_MAIL_CLIENT_ID",
        "client_secret": "ZOHO_MAIL_CLIENT_SECRET",
        "refresh_token": "ZOHO_MAIL_REFRESH_TOKEN",
    },
    "crm": {
        "client_id": "ZOHO_CRM_CLIENT_ID",
        "client_secret": "ZOHO_CRM_CLIENT_SECRET",
        "refresh_token": "ZOHO_CRM_REFRESH_TOKEN",
    },
    "workdrive": {
        "client_id": "ZOHO_WORKDRIVE_CLIENT_ID",
        "client_secret": "ZOHO_WORKDRIVE_CLIENT_SECRET",
        "refresh_token": "ZOHO_WORKDRIVE_REFRESH_TOKEN",
    },
}

# Minimum required OAuth scopes per flow (documented for ZDI_SECURITY_AUDIT.md)
REQUIRED_SCOPES = {
    "mail": ["ZohoMail.messages.ALL"],
    "crm": ["ZohoCRM.modules.READ", "ZohoCRM.modules.CREATE"],
    "workdrive": ["WorkDrive.files.READ"],
}


class AuthError(RuntimeError):
    """Raised when OAuth token refresh fails. Never includes token values."""
    pass


@dataclass
class _TokenState:
    """Internal state for one OAuth flow. Never exposed outside AuthManager."""
    access_token: str = field(repr=False)  # repr=False prevents accidental __repr__ logging
    expires_at: float = 0.0  # unix timestamp when token expires

    def is_expired(self) -> bool:
        """Return True if token is within the expiry buffer window."""
        return time.time() >= (self.expires_at - _EXPIRY_BUFFER_SECONDS)


class AuthManager:
    """
    Manages OAuth 2.0 refresh tokens for all three Zoho apps.

    Each app (mail, crm, workdrive) is completely isolated — separate
    client_id, client_secret, and refresh_token, loaded from environment
    variables. No credential is ever shared between apps or written to disk.

    Usage:
        auth = AuthManager()
        auth.refresh_all()              # call once at pipeline startup
        token = auth.get_token("crm")   # returns current access token

    Injectable override for testing:
        auth = AuthManager(_http_post=mock_post_fn)
        # mock_post_fn(url, data, timeout) must return a mock Response object
    """

    def __init__(self, _http_post=None) -> None:
        """
        Args:
            _http_post: Injectable callable replacing requests.post (for testing).
                        Signature: (url, data, timeout) -> Response-like object
        """
        self._tokens: dict[str, _TokenState] = {}
        self._http_post = _http_post or requests.post
        self._credentials: dict[str, dict[str, str]] = {}

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def load_credentials(self) -> None:
        """
        Load all credential sets from environment variables.

        Raises:
            AuthError: If any required environment variable is missing or empty.
        """
        missing = []
        for app, keys in _ENV_KEYS.items():
            creds = {}
            for field_name, env_var in keys.items():
                val = os.environ.get(env_var, "")
                if not val:
                    missing.append(env_var)
                creds[field_name] = val
            self._credentials[app] = creds

        if missing:
            raise AuthError(
                f"Missing required environment variables: {', '.join(missing)}. "
                f"See .env.example for the full list."
            )

        # Log app names only — never log credential values
        logger.info(
            "AuthManager: credentials loaded for apps=%s",
            list(self._credentials.keys()),
        )

    def refresh_all(self) -> None:
        """
        Refresh access tokens for all three apps.

        Called once at pipeline startup. Subsequent calls are safe —
        only refreshes tokens that are expired or near expiry.

        Raises:
            AuthError: If any refresh fails (collects all failures, raises once).
        """
        if not self._credentials:
            self.load_credentials()

        errors = []
        for app in ("mail", "crm", "workdrive"):
            try:
                self._refresh_app(app)
            except AuthError as exc:
                errors.append(str(exc))

        if errors:
            raise AuthError(
                f"Token refresh failed for {len(errors)} app(s). "
                f"Details (no credentials included): {'; '.join(errors)}"
            )

    def get_token(self, app: str) -> str:
        """
        Return a valid access token for the given app.

        Tokens are refreshed at startup. If somehow expired mid-run
        (e.g. very long pipeline), refreshes on this call as a safety net.

        Args:
            app: One of 'mail', 'crm', 'workdrive'.

        Returns:
            Access token string.

        Raises:
            AuthError: If app name is invalid or token is unavailable.
        """
        if app not in _ENV_KEYS:
            raise AuthError(
                f"Unknown app '{app}'. Valid apps: {list(_ENV_KEYS.keys())}"
            )

        if app not in self._tokens or self._tokens[app].is_expired():
            logger.info(
                "AuthManager: token for app=%s expired or missing — refreshing", app
            )
            self._refresh_app(app)

        return self._tokens[app].access_token

    def is_loaded(self, app: str) -> bool:
        """Return True if a non-expired token is available for the given app."""
        return app in self._tokens and not self._tokens[app].is_expired()

    # ------------------------------------------------------------------
    # Internal helpers — token values NEVER appear below this line in logs
    # ------------------------------------------------------------------

    def _refresh_app(self, app: str) -> None:
        """
        Perform the Zoho OAuth refresh_token grant for a single app.

        SECURITY CONTRACT:
          - The access_token value from the response is NEVER logged at any level.
          - client_secret and refresh_token are NEVER logged.
          - Only app name, HTTP status, expires_in, and error codes are logged.

        Args:
            app: One of 'mail', 'crm', 'workdrive'.

        Raises:
            AuthError: On network error, HTTP error, or Zoho error body.
        """
        creds = self._credentials.get(app)
        if not creds:
            raise AuthError(
                f"Credentials not loaded for app={app}. Call load_credentials() first."
            )

        logger.info("AuthManager: initiating token refresh for app=%s", app)

        try:
            response = self._http_post(
                _ZOHO_TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "client_id": creds["client_id"],
                    "client_secret": creds["client_secret"],
                    "refresh_token": creds["refresh_token"],
                },
                timeout=15,
            )
        except Exception as exc:
            # Catch all — never include credentials in the exception message
            raise AuthError(
                f"Network error during token refresh for app={app}: {type(exc).__name__}"
            ) from exc

        if response.status_code != 200:
            raise AuthError(
                f"Token refresh failed for app={app}: HTTP {response.status_code}"
            )

        body = response.json()

        if "error" in body:
            raise AuthError(
                f"Token refresh error for app={app}: error_code={body['error']}"
            )

        if "access_token" not in body:
            raise AuthError(
                f"Token refresh response missing 'access_token' field for app={app}"
            )

        expires_in = int(body.get("expires_in", 3600))
        self._tokens[app] = _TokenState(
            access_token=body["access_token"],
            expires_at=time.time() + expires_in,
        )

        # Log success — expires_in is safe, token value is NOT logged
        logger.info(
            "AuthManager: token refreshed successfully for app=%s expires_in=%ds",
            app,
            expires_in,
        )
