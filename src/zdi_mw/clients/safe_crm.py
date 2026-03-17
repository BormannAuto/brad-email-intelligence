# src/zdi_mw/clients/safe_crm.py
# ZDI Middleware — SafeCRMClient
#
# safe_write() — 4 gates in sequence before any Zoho CRM write:
#   Gate 1: confidence != LOW
#   Gate 2: value_present_in_source() — value appears verbatim in source_text
#   Gate 3: read_before_write() — fill-blanks only (existing non-empty value blocks)
#   Gate 4: SHA-256 idempotency key — deduplicate via IdempotencyKeys table
#   → Atomic write + WAL INTENT/CONFIRMED + accuracy_log entry
#
# LOGGING RULE: never log contact names, field values, or source text content.
# Log only: contact_id hash prefix (8 chars), field name, gate that blocked, outcome.

import hashlib
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from src.zdi_mw.state.db import get_connection

logger = logging.getLogger(__name__)

_CONFIDENCE_LOW = "LOW"


@dataclass
class WriteResult:
    """Result of a safe_write() call."""
    success: bool
    skipped: bool = False
    skip_reason: str = ""
    idempotency_hit: bool = False
    wal_intent_id: Optional[int] = None
    error: str = ""


class SafeCRMClient:
    """Gate-enforced CRM write client. THE authorised path for Zoho CRM writes."""

    def __init__(
        self,
        run_context: Any,
        accuracy_logger: Any,
        _crm_read_fn: Optional[Callable[[str, str], Optional[str]]] = None,
        _crm_write_fn: Optional[Callable[[str, str, str], bool]] = None,
        _rate_manager: Optional[Any] = None,
        _auth_manager: Optional[Any] = None,
        _wal: Optional[Any] = None,
        _db_path: Optional[Path] = None,
    ) -> None:
        self._run_context = run_context
        self._accuracy_logger = accuracy_logger
        self._crm_read_fn = _crm_read_fn
        self._crm_write_fn = _crm_write_fn
        self._rate_manager = _rate_manager
        self._auth_manager = _auth_manager
        self._wal = _wal
        self._db_path = _db_path

    def safe_write(
        self,
        contact_id: str,
        field: str,
        value: str,
        source_text: str,
        confidence: str,
    ) -> WriteResult:
        """
        Write a single CRM field after passing all 4 safety gates.

        Returns:
            WriteResult with success/skipped/error details.
        """
        cid_prefix = _id_prefix(contact_id)

        # Gate 1: Confidence
        if confidence == _CONFIDENCE_LOW:
            logger.info("SafeCRM: BLOCKED gate=1_confidence prefix=%s field=%s", cid_prefix, field)
            return WriteResult(success=False, skipped=True, skip_reason="CONFIDENCE_LOW")

        # Gate 2: Value present in source
        if not _value_present_in_source(value, source_text):
            logger.info("SafeCRM: BLOCKED gate=2_source prefix=%s field=%s", cid_prefix, field)
            return WriteResult(success=False, skipped=True, skip_reason="VALUE_NOT_IN_SOURCE")

        # Gate 3: Read-before-write (fill blanks only)
        if self._crm_read_fn is not None:
            try:
                existing = self._crm_read_fn(contact_id, field)
            except Exception as exc:
                logger.error("SafeCRM: read failed prefix=%s field=%s exc=%s", cid_prefix, field, type(exc).__name__)
                return WriteResult(success=False, error=f"CRM_READ_ERROR:{type(exc).__name__}")
            if existing and existing.strip():
                logger.info("SafeCRM: BLOCKED gate=3_read_before_write prefix=%s field=%s", cid_prefix, field)
                return WriteResult(success=False, skipped=True, skip_reason="FIELD_ALREADY_SET")

        # Gate 4: Idempotency key
        idem_key = _compute_idempotency_key(
            self._run_context.run_id, contact_id, field, source_text, value
        )
        if self._db_path is not None and _idempotency_key_exists(idem_key, self._db_path):
            logger.info("SafeCRM: idempotency HIT prefix=%s field=%s", cid_prefix, field)
            return WriteResult(success=True, skipped=True, skip_reason="IDEMPOTENCY_HIT", idempotency_hit=True)

        # Rate check
        if self._rate_manager is not None:
            try:
                self._rate_manager.check_and_wait("crm", estimated_cost=1.0)
            except Exception as exc:
                logger.warning("SafeCRM: rate blocked prefix=%s exc=%s", cid_prefix, exc)
                return WriteResult(success=False, error=f"RATE_BLOCKED:{exc}")

        # WAL INTENT
        intent_id: Optional[int] = None
        if self._wal is not None:
            try:
                intent_id = self._wal.log_intent(
                    thread_id=contact_id,
                    operation=f"CRM_WRITE_{field}",
                    payload={
                        "contact_id_prefix": cid_prefix,
                        "field": field,
                        "source_hash": _sha256(source_text),
                        "value_hash": _sha256(value),
                        "idempotency_key": idem_key,
                    },
                )
            except Exception as exc:
                logger.error("SafeCRM: WAL intent failed prefix=%s exc=%s", cid_prefix, type(exc).__name__)
                return WriteResult(success=False, error=f"WAL_INTENT_ERROR:{type(exc).__name__}")

        # Actual write
        if self._crm_write_fn is not None:
            try:
                ok = self._crm_write_fn(contact_id, field, value)
            except Exception as exc:
                logger.error("SafeCRM: write failed prefix=%s field=%s exc=%s", cid_prefix, field, type(exc).__name__)
                if self._wal is not None and intent_id is not None:
                    try:
                        self._wal.fail(intent_id, f"CRM_WRITE_ERROR:{type(exc).__name__}")
                    except Exception:
                        pass
                return WriteResult(success=False, error=f"WRITE_ERROR:{type(exc).__name__}")
            if not ok:
                if self._wal is not None and intent_id is not None:
                    try:
                        self._wal.fail(intent_id, "CRM_WRITE_RETURNED_FALSE")
                    except Exception:
                        pass
                return WriteResult(success=False, error="CRM_WRITE_RETURNED_FALSE")

        # WAL CONFIRM + record idempotency key
        if self._wal is not None and intent_id is not None:
            try:
                self._wal.confirm(intent_id)
            except Exception as exc:
                logger.error("SafeCRM: WAL confirm failed prefix=%s exc=%s", cid_prefix, type(exc).__name__)

        if self._db_path is not None:
            _record_idempotency_key(idem_key, f"CRM_WRITE_{field}", self._run_context.run_id, self._db_path)

        # Accuracy log
        try:
            self._accuracy_logger.log_action(
                run_id=self._run_context.run_id,
                pipeline_version=self._run_context.pipeline_version,
                brad_voice_version=self._run_context.brad_voice_version,
                prompt_name="safe_crm_write",
                prompt_version="1.0",
                prompt_fallback_level=0,
                model_version="n/a",
                source_input_hash=_sha256(source_text),
                thread_id=contact_id,
                outcome="SUCCESS",
                extra={"field": field, "confidence": confidence},
            )
        except Exception as exc:
            logger.error("SafeCRM: accuracy_logger failed exc=%s", type(exc).__name__)

        logger.info("SafeCRM: WRITE SUCCESS prefix=%s field=%s", cid_prefix, field)
        return WriteResult(success=True, wal_intent_id=intent_id)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _value_present_in_source(value: str, source_text: str) -> bool:
    """Return True if value appears verbatim (case-insensitive) in source_text."""
    if not value or not source_text:
        return False
    return value.strip().lower() in source_text.lower()


def _compute_idempotency_key(run_id: str, contact_id: str, field: str, source_text: str, value: str) -> str:
    """sha256(run_id + contact_id + field + sha256(source_text) + sha256(value))."""
    raw = f"{run_id}{contact_id}{field}{_sha256(source_text)}{_sha256(value)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _id_prefix(contact_id: str) -> str:
    return hashlib.sha256(contact_id.encode("utf-8")).hexdigest()[:8]


def _idempotency_key_exists(key: str, db_path: Path) -> bool:
    try:
        with get_connection(db_path) as conn:
            row = conn.execute("SELECT 1 FROM idempotency_keys WHERE key = ?", (key,)).fetchone()
            return row is not None
    except sqlite3.Error as exc:
        logger.error("SafeCRM: idempotency check DB error exc=%s", exc)
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
        logger.error("SafeCRM: record idempotency DB error exc=%s", exc)
