# src/zdi_mw/clients/safe_mail.py
# ZDI Middleware — SafeMailClient
#
# safe_create_draft() — gates before Zoho Mail draft creation:
#   Gate 1: HOLD double-check — category != "HOLD" AND recipient not in hold_senders
#           (both checks must pass — either one blocks the write)
#   Gate 2: thread_id freshness — live API verify, NOT from cache
#   Gate 3: recipient == source fromAddress (anti-misdirection)
#   Gate 4: inReplyTo header must be set (non-empty string)
#   Gate 5: body word count < 500 (flag only — does not block per spec)
#   Gate 6: SHA-256 idempotency key via IdempotencyKeys table
#   → WAL INTENT → draft create → WAL CONFIRMED + accuracy_log
#
# LOGGING RULE: never log recipient addresses, body content, or subject lines.
# Log only: thread_id, word_count, gate that blocked, outcome.

import hashlib
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from src.zdi_mw.state.db import get_connection

logger = logging.getLogger(__name__)

_HOLD_CATEGORY = "HOLD"
_MAX_WORD_COUNT = 500


@dataclass
class DraftResult:
    """Result of a safe_create_draft() call."""
    success: bool
    draft_id: Optional[str] = None
    skipped: bool = False
    skip_reason: str = ""
    word_count: int = 0
    word_count_flagged: bool = False
    idempotency_hit: bool = False
    wal_intent_id: Optional[int] = None
    error: str = ""


class SafeMailClient:
    """
    Gate-enforced Mail draft client. THE authorised path for Zoho Mail drafts.

    Injectable dependencies:
        _mail_create_draft_fn: (thread_id, recipient, body, in_reply_to) -> Optional[str]
            Returns draft_id on success, None on failure.
        _mail_thread_verify_fn: (thread_id) -> bool
            Live API call to confirm thread still exists and is current.
        _email_config: dict loaded from email_config.json (injectable for tests).
        _wal, _db_path, _rate_manager, _auth_manager, _accuracy_logger as usual.
    """

    def __init__(
        self,
        run_context: Any,
        accuracy_logger: Any,
        _mail_create_draft_fn: Optional[Callable[[str, str, str, str], Optional[str]]] = None,
        _mail_thread_verify_fn: Optional[Callable[[str], bool]] = None,
        _email_config: Optional[Dict] = None,
        _rate_manager: Optional[Any] = None,
        _auth_manager: Optional[Any] = None,
        _wal: Optional[Any] = None,
        _db_path: Optional[Path] = None,
    ) -> None:
        self._run_context = run_context
        self._accuracy_logger = accuracy_logger
        self._mail_create_draft_fn = _mail_create_draft_fn
        self._mail_thread_verify_fn = _mail_thread_verify_fn
        self._email_config = _email_config or {}
        self._rate_manager = _rate_manager
        self._auth_manager = _auth_manager
        self._wal = _wal
        self._db_path = _db_path

    def safe_create_draft(
        self,
        thread_id: str,
        recipient: str,
        body: str,
        category: str,
        source_from_address: str,
        in_reply_to: str = "",
    ) -> DraftResult:
        """
        Create a Mail draft after passing all safety gates.

        Args:
            thread_id:           Email thread ID.
            recipient:           Draft recipient address.
            body:                Draft body text.
            category:            Email category from CategorizerOutput.
            source_from_address: fromAddress of the source email.
            in_reply_to:         inReplyTo header value (required — must be non-empty).

        Returns:
            DraftResult with success/skipped/error details.
        """
        # Gate 1: HOLD double-check
        hold_senders: List[str] = self._email_config.get("hold_senders", [])
        is_hold_category = (category == _HOLD_CATEGORY)
        is_hold_sender = any(
            s.lower() == recipient.lower() for s in hold_senders
        )
        if is_hold_category or is_hold_sender:
            reason = "HOLD_CATEGORY" if is_hold_category else "HOLD_SENDER"
            logger.info(
                "SafeMail: BLOCKED gate=1_hold thread_id=%s reason=%s",
                thread_id, reason,
            )
            return DraftResult(success=False, skipped=True, skip_reason=f"HOLD:{reason}")

        # Gate 2: Thread freshness (live API check)
        if self._mail_thread_verify_fn is not None:
            try:
                thread_valid = self._mail_thread_verify_fn(thread_id)
            except Exception as exc:
                logger.error(
                    "SafeMail: thread_verify failed thread_id=%s exc=%s",
                    thread_id, type(exc).__name__,
                )
                return DraftResult(success=False, error=f"THREAD_VERIFY_ERROR:{type(exc).__name__}")
            if not thread_valid:
                logger.info("SafeMail: BLOCKED gate=2_thread_stale thread_id=%s", thread_id)
                return DraftResult(success=False, skipped=True, skip_reason="THREAD_NOT_FOUND")

        # Gate 3: Recipient == source fromAddress
        if recipient.strip().lower() != source_from_address.strip().lower():
            logger.info(
                "SafeMail: BLOCKED gate=3_recipient_mismatch thread_id=%s",
                thread_id,
            )
            return DraftResult(success=False, skipped=True, skip_reason="RECIPIENT_MISMATCH")

        # Gate 4: inReplyTo must be set
        if not in_reply_to or not in_reply_to.strip():
            logger.info("SafeMail: BLOCKED gate=4_no_in_reply_to thread_id=%s", thread_id)
            return DraftResult(success=False, skipped=True, skip_reason="MISSING_IN_REPLY_TO")

        # Gate 5: Word count (flag only — does not block)
        word_count = len(body.split())
        word_count_flagged = word_count > _MAX_WORD_COUNT
        if word_count_flagged:
            logger.warning(
                "SafeMail: word_count_exceeded thread_id=%s count=%d limit=%d",
                thread_id, word_count, _MAX_WORD_COUNT,
            )

        # Gate 6: Idempotency key
        idem_key = _compute_draft_idempotency_key(
            self._run_context.run_id, thread_id, recipient, body
        )
        if self._db_path is not None and _idempotency_key_exists(idem_key, self._db_path):
            logger.info("SafeMail: idempotency HIT thread_id=%s", thread_id)
            return DraftResult(
                success=True, skipped=True, skip_reason="IDEMPOTENCY_HIT",
                idempotency_hit=True, word_count=word_count,
            )

        # Rate check
        if self._rate_manager is not None:
            try:
                self._rate_manager.check_and_wait("mail", estimated_cost=0.5)
            except Exception as exc:
                logger.warning("SafeMail: rate blocked thread_id=%s exc=%s", thread_id, exc)
                return DraftResult(success=False, error=f"RATE_BLOCKED:{exc}")

        # WAL INTENT
        intent_id: Optional[int] = None
        if self._wal is not None:
            try:
                intent_id = self._wal.log_intent(
                    thread_id=thread_id,
                    operation="MAIL_DRAFT",
                    payload={
                        "thread_id": thread_id,
                        "body_hash": _sha256(body),
                        "recipient_hash": _sha256(recipient),
                        "word_count": word_count,
                        "idempotency_key": idem_key,
                    },
                )
            except Exception as exc:
                logger.error("SafeMail: WAL intent failed thread_id=%s exc=%s", thread_id, type(exc).__name__)
                return DraftResult(success=False, error=f"WAL_INTENT_ERROR:{type(exc).__name__}")

        # Actual draft creation
        draft_id: Optional[str] = None
        if self._mail_create_draft_fn is not None:
            try:
                draft_id = self._mail_create_draft_fn(thread_id, recipient, body, in_reply_to)
            except Exception as exc:
                logger.error("SafeMail: draft create failed thread_id=%s exc=%s", thread_id, type(exc).__name__)
                if self._wal is not None and intent_id is not None:
                    try:
                        self._wal.fail(intent_id, f"MAIL_DRAFT_ERROR:{type(exc).__name__}")
                    except Exception:
                        pass
                return DraftResult(success=False, error=f"DRAFT_ERROR:{type(exc).__name__}")

            if draft_id is None:
                if self._wal is not None and intent_id is not None:
                    try:
                        self._wal.fail(intent_id, "MAIL_DRAFT_RETURNED_NONE")
                    except Exception:
                        pass
                return DraftResult(success=False, error="MAIL_DRAFT_RETURNED_NONE")

        # WAL CONFIRM + idempotency record
        if self._wal is not None and intent_id is not None:
            try:
                self._wal.confirm(intent_id)
            except Exception as exc:
                logger.error("SafeMail: WAL confirm failed thread_id=%s exc=%s", thread_id, type(exc).__name__)

        if self._db_path is not None:
            _record_idempotency_key(idem_key, "MAIL_DRAFT", self._run_context.run_id, self._db_path)

        # Accuracy log
        try:
            self._accuracy_logger.log_action(
                run_id=self._run_context.run_id,
                pipeline_version=self._run_context.pipeline_version,
                brad_voice_version=self._run_context.brad_voice_version,
                prompt_name="safe_mail_draft",
                prompt_version="1.0",
                prompt_fallback_level=0,
                model_version="n/a",
                source_input_hash=_sha256(body),
                thread_id=thread_id,
                outcome="SUCCESS",
                extra={
                    "word_count": word_count,
                    "word_count_flagged": word_count_flagged,
                    "category": category,
                },
            )
        except Exception as exc:
            logger.error("SafeMail: accuracy_logger failed exc=%s", type(exc).__name__)

        logger.info("SafeMail: DRAFT CREATED thread_id=%s draft_id=%s wc=%d", thread_id, draft_id, word_count)
        return DraftResult(
            success=True,
            draft_id=draft_id,
            word_count=word_count,
            word_count_flagged=word_count_flagged,
            wal_intent_id=intent_id,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compute_draft_idempotency_key(run_id: str, thread_id: str, recipient: str, body: str) -> str:
    raw = f"{run_id}{thread_id}{_sha256(recipient)}{_sha256(body)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _idempotency_key_exists(key: str, db_path: Path) -> bool:
    try:
        with get_connection(db_path) as conn:
            row = conn.execute("SELECT 1 FROM idempotency_keys WHERE key = ?", (key,)).fetchone()
            return row is not None
    except sqlite3.Error as exc:
        logger.error("SafeMail: idempotency check DB error exc=%s", exc)
        return False


def _record_idempotency_key(key: str, operation: str, run_id: str, db_path: Path) -> None:
    try:
        now = datetime.now(timezone.utc).isoformat()
        with get_connection(db_path) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO idempotency_keys (key, operation, run_id, outcome, timestamp_utc) VALUES (?, ?, ?, 'COMPLETED', ?)",
                (key, operation, run_id, now),
            )
    except sqlite3.Error as exc:
        logger.error("SafeMail: record idempotency DB error exc=%s", exc)
