# tests/test_phase3.py
# ZDI Middleware — Phase 3 Verification Gate
#
# Verification criteria (spec Section 4 Phase 3):
#   1. Ledger tracks all 8 stages correctly
#   2. WAL RECONCILE pass finds and flags orphaned INTENTs on startup
#   3. DLQ inserts and retry logic operate per retry_policies.json
#   4. Crash simulation: INTENT logged, no CONFIRMED → RECONCILE dead-letters it

import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

_TEST_POLICIES_PATH = _REPO_ROOT / "src" / "config" / "retry_policies.json"


# ===========================================================================
# Helpers
# ===========================================================================

def _setup_db(tmp_path: Path) -> Path:
    from src.zdi_mw.state.db import init_db
    db = tmp_path / "zdi.db"
    init_db(db_path=db)
    return db


def _make_run_ctx_json(run_id: str = "2026-03-16-071500-UTC") -> str:
    return json.dumps({"run_id": run_id, "pipeline_mode": "sandbox"})


# ===========================================================================
# Gate 1 — Ledger tracks all 8 stages correctly
# ===========================================================================

class TestLedger:
    """Verify PipelineStateLedger tracks all 8 stages with correct metadata."""

    def test_mark_all_eight_stages(self, tmp_path):
        """All 8 pipeline stages can be recorded without error."""
        from src.zdi_mw.state.ledger import PipelineStateLedger, PIPELINE_STAGES

        db = _setup_db(tmp_path)
        ledger = PipelineStateLedger("run-001", "git:abc1234", db_path=db)

        for stage in PIPELINE_STAGES:
            status = "FAILED" if stage == "FAILED" else "COMPLETE"
            row_id = ledger.mark_stage("thread_001", stage, status)
            assert row_id > 0, f"mark_stage returned invalid id for stage={stage}"

    def test_get_stage_returns_latest(self, tmp_path):
        """get_stage() returns the most recent row for a thread+stage."""
        from src.zdi_mw.state.ledger import PipelineStateLedger

        db = _setup_db(tmp_path)
        ledger = PipelineStateLedger("run-001", "git:abc1234", db_path=db)

        ledger.mark_stage("thread_002", "FETCHED", "IN_PROGRESS")
        ledger.mark_stage("thread_002", "FETCHED", "COMPLETE")

        result = ledger.get_stage("thread_002", "FETCHED")
        assert result is not None
        assert result["status"] == "COMPLETE", (
            "get_stage should return the most recent row"
        )

    def test_get_stage_returns_none_for_missing(self, tmp_path):
        """get_stage() returns None when no record exists."""
        from src.zdi_mw.state.ledger import PipelineStateLedger

        db = _setup_db(tmp_path)
        ledger = PipelineStateLedger("run-001", "git:abc1234", db_path=db)
        result = ledger.get_stage("nonexistent_thread", "FETCHED")
        assert result is None

    def test_mark_failed_stores_reason(self, tmp_path):
        """mark_failed() stores failure_reason in the row."""
        from src.zdi_mw.state.ledger import PipelineStateLedger

        db = _setup_db(tmp_path)
        ledger = PipelineStateLedger("run-001", "git:abc1234", db_path=db)
        ledger.mark_failed("thread_003", "DRAFT_GENERATED", "Claude API timeout")

        result = ledger.get_stage("thread_003", "DRAFT_GENERATED")
        assert result["status"] == "FAILED"
        assert result["failure_reason"] == "Claude API timeout"

    def test_invalid_stage_raises(self, tmp_path):
        """mark_stage() raises LedgerError for an invalid stage name."""
        from src.zdi_mw.state.ledger import PipelineStateLedger, LedgerError

        db = _setup_db(tmp_path)
        ledger = PipelineStateLedger("run-001", "git:abc1234", db_path=db)

        with pytest.raises(LedgerError):
            ledger.mark_stage("thread_004", "INVALID_STAGE", "COMPLETE")

    def test_invalid_status_raises(self, tmp_path):
        """mark_stage() raises LedgerError for an invalid status."""
        from src.zdi_mw.state.ledger import PipelineStateLedger, LedgerError

        db = _setup_db(tmp_path)
        ledger = PipelineStateLedger("run-001", "git:abc1234", db_path=db)

        with pytest.raises(LedgerError):
            ledger.mark_stage("thread_005", "FETCHED", "INVALID_STATUS")

    def test_timestamps_are_utc_iso(self, tmp_path):
        """All recorded timestamps are UTC ISO 8601 strings."""
        from src.zdi_mw.state.ledger import PipelineStateLedger

        db = _setup_db(tmp_path)
        ledger = PipelineStateLedger("run-001", "git:abc1234", db_path=db)
        ledger.mark_stage("thread_006", "FETCHED", "COMPLETE")

        result = ledger.get_stage("thread_006", "FETCHED")
        ts = result["timestamp_utc"]
        # Must parse as ISO 8601 UTC without error
        parsed = datetime.fromisoformat(ts)
        assert parsed.tzinfo is not None or ts.endswith("+00:00") or "Z" in ts or "+" in ts

    def test_get_incomplete_threads(self, tmp_path):
        """get_incomplete_threads() returns only threads without COMPLETE stage."""
        from src.zdi_mw.state.ledger import PipelineStateLedger

        db = _setup_db(tmp_path)
        ledger = PipelineStateLedger("run-001", "git:abc1234", db_path=db)

        # thread_A: in progress
        ledger.mark_stage("thread_A", "FETCHED", "IN_PROGRESS")
        # thread_B: complete
        ledger.mark_stage("thread_B", "FETCHED", "COMPLETE")
        ledger.mark_stage("thread_B", "COMPLETE", "COMPLETE")
        # thread_C: in progress
        ledger.mark_stage("thread_C", "FETCHED", "IN_PROGRESS")

        incomplete = ledger.get_incomplete_threads()
        assert "thread_A" in incomplete
        assert "thread_C" in incomplete
        assert "thread_B" not in incomplete, "Completed thread must not appear in incomplete list"

    def test_pipeline_version_stored(self, tmp_path):
        """pipeline_version is stored in every ledger row."""
        from src.zdi_mw.state.ledger import PipelineStateLedger

        db = _setup_db(tmp_path)
        version = "git:deadbeef"
        ledger = PipelineStateLedger("run-001", version, db_path=db)
        ledger.mark_stage("thread_007", "FETCHED", "COMPLETE")

        result = ledger.get_stage("thread_007", "FETCHED")
        assert result["pipeline_version"] == version


# ===========================================================================
# Gate 2 — WAL INTENT / CONFIRM / FAIL cycle
# ===========================================================================

class TestWAL:
    """Verify WriteAheadLogger intent→confirm→fail lifecycle."""

    def test_log_intent_returns_id(self, tmp_path):
        """log_intent() returns a positive integer row ID."""
        from src.zdi_mw.state.wal import WriteAheadLogger

        db = _setup_db(tmp_path)
        wal = WriteAheadLogger("run-001", db_path=db)
        intent_id = wal.log_intent("thread_A", "CRM_WRITE_phone", {"field": "phone"})
        assert intent_id > 0

    def test_confirm_sets_status(self, tmp_path):
        """confirm() changes status from INTENT to CONFIRMED."""
        from src.zdi_mw.state.wal import WriteAheadLogger

        db = _setup_db(tmp_path)
        wal = WriteAheadLogger("run-001", db_path=db)
        intent_id = wal.log_intent("thread_B", "MAIL_DRAFT", {"thread_id": "thread_B"})
        wal.confirm(intent_id)

        row = wal.get_by_id(intent_id)
        assert row["status"] == "CONFIRMED"

    def test_fail_sets_status_and_reason(self, tmp_path):
        """fail() sets status=FAILED and stores the failure reason."""
        from src.zdi_mw.state.wal import WriteAheadLogger

        db = _setup_db(tmp_path)
        wal = WriteAheadLogger("run-001", db_path=db)
        intent_id = wal.log_intent("thread_C", "CRM_WRITE_email", {})
        wal.fail(intent_id, "Zoho returned 503")

        row = wal.get_by_id(intent_id)
        assert row["status"] == "FAILED"
        assert row["failure_reason"] == "Zoho returned 503"

    def test_confirm_nonexistent_raises(self, tmp_path):
        """confirm() raises WALError if row doesn't exist."""
        from src.zdi_mw.state.wal import WriteAheadLogger, WALError

        db = _setup_db(tmp_path)
        wal = WriteAheadLogger("run-001", db_path=db)
        with pytest.raises(WALError):
            wal.confirm(99999)

    def test_payload_serialised_to_json(self, tmp_path):
        """Intent payload is stored as valid JSON."""
        from src.zdi_mw.state.wal import WriteAheadLogger

        db = _setup_db(tmp_path)
        wal = WriteAheadLogger("run-001", db_path=db)
        payload = {"field": "phone", "source_hash": "sha256:abc123", "value_hash": "sha256:def"}
        intent_id = wal.log_intent("thread_D", "CRM_WRITE_phone", payload)

        row = wal.get_by_id(intent_id)
        parsed_payload = json.loads(row["intent_payload_json"])
        assert parsed_payload["field"] == "phone"
        assert parsed_payload["source_hash"] == "sha256:abc123"


# ===========================================================================
# Gate 3 — WAL RECONCILE: crash simulation
# ===========================================================================

class TestWALReconcile:
    """
    Simulate a crash mid-write (INTENT logged, no CONFIRMED).
    Verify RECONCILE on next startup catches it and dead-letters it.
    """

    def test_reconcile_catches_orphaned_intent(self, tmp_path):
        """
        Core crash-recovery test from spec:
        1. Log INTENT
        2. Simulate crash (no confirm/fail)
        3. Call reconcile() on next startup
        4. Verify orphan is dead-lettered and WAL row is FAILED
        """
        from src.zdi_mw.state.wal import WriteAheadLogger
        from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue
        from src.zdi_mw.state.db import get_connection

        db = _setup_db(tmp_path)
        dlq = DeadLetterQueue(db_path=db, retry_policies_path=_TEST_POLICIES_PATH)

        # --- Simulate "run 1" that crashes mid-write ---
        wal_run1 = WriteAheadLogger("crashed-run-001", db_path=db)
        intent_id = wal_run1.log_intent(
            "thread_crash_victim",
            "CRM_WRITE_phone",
            {"field": "phone", "source_hash": "sha256:abc"},
        )

        # Backdating: manually age the INTENT row so it's past the 10-minute threshold
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()
        conn = get_connection(db)
        with conn:
            conn.execute(
                "UPDATE wal_log SET timestamp_utc = ? WHERE id = ?",
                (cutoff, intent_id),
            )
        conn.close()

        # --- Simulate "run 2" startup — RECONCILE pass ---
        wal_run2 = WriteAheadLogger("recovery-run-002", db_path=db)
        reconciled = wal_run2.reconcile(dlq)

        # Orphaned INTENT must be caught
        assert len(reconciled) >= 1, "RECONCILE must find the orphaned INTENT"
        orphan_ids = [r["id"] for r in reconciled]
        assert intent_id in orphan_ids, f"Expected intent_id={intent_id} in reconciled"

        # WAL row must now be FAILED
        wal_row = wal_run2.get_by_id(intent_id)
        assert wal_row["status"] == "FAILED", (
            f"Orphaned WAL row status should be FAILED, got {wal_row['status']}"
        )
        assert wal_row["failure_reason"] == "UNCONFIRMED_INTENT_ON_STARTUP"

        # DLQ must have an entry for the orphan
        dlq_count = dlq.get_unresolved_count()
        assert dlq_count >= 1, "DLQ must have at least one entry after RECONCILE"

    def test_reconcile_ignores_recent_intents(self, tmp_path):
        """RECONCILE must NOT dead-letter INTENTs younger than 10 minutes."""
        from src.zdi_mw.state.wal import WriteAheadLogger
        from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue

        db = _setup_db(tmp_path)
        dlq = DeadLetterQueue(db_path=db, retry_policies_path=_TEST_POLICIES_PATH)

        # Log a fresh INTENT (timestamp = now)
        wal = WriteAheadLogger("run-fresh", db_path=db)
        wal.log_intent("thread_fresh", "CRM_WRITE_email", {})

        reconciled = wal.reconcile(dlq)
        assert len(reconciled) == 0, (
            "RECONCILE must not touch INTENTs younger than 10 minutes"
        )

    def test_reconcile_ignores_confirmed_intents(self, tmp_path):
        """RECONCILE must NOT dead-letter already-CONFIRMED intents."""
        from src.zdi_mw.state.wal import WriteAheadLogger
        from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue
        from src.zdi_mw.state.db import get_connection

        db = _setup_db(tmp_path)
        dlq = DeadLetterQueue(db_path=db, retry_policies_path=_TEST_POLICIES_PATH)

        wal = WriteAheadLogger("run-confirmed", db_path=db)
        intent_id = wal.log_intent("thread_ok", "MAIL_DRAFT", {})
        wal.confirm(intent_id)  # properly confirmed

        # Age the row to simulate it being old
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
        conn = get_connection(db)
        with conn:
            conn.execute(
                "UPDATE wal_log SET timestamp_utc = ? WHERE id = ?",
                (cutoff, intent_id),
            )
        conn.close()

        reconciled = wal.reconcile(dlq)
        confirmed_ids = [r["id"] for r in reconciled]
        assert intent_id not in confirmed_ids, (
            "RECONCILE must not dead-letter CONFIRMED intents"
        )


# ===========================================================================
# Gate 4 — DLQ insert and retry logic per retry_policies.json
# ===========================================================================

class TestDeadLetterQueue:
    """Verify DLQ insert, retry eligibility, resolution, and policy loading."""

    def test_insert_creates_row(self, tmp_path):
        """insert() returns a positive row ID."""
        from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue

        db = _setup_db(tmp_path)
        dlq = DeadLetterQueue(db_path=db, retry_policies_path=_TEST_POLICIES_PATH)
        row_id = dlq.insert(
            "thread_dlq_1", "DRAFT_GENERATED", "Claude timeout",
            run_context_json=_make_run_ctx_json(),
        )
        assert row_id > 0

    @pytest.mark.parametrize("failure_reason,expected_retry,expected_manual", [
        ("Claude timeout", True, False),
        ("Rate limit hit", True, False),
        ("Validation failure", False, True),
        ("HOLD category", False, False),
        ("Auth failure", False, False),
        ("Schema mismatch", False, True),
        ("Timeout", True, False),
        ("Unconfirmed intent", False, True),
    ])
    def test_retry_policies_applied_correctly(
        self, tmp_path, failure_reason, expected_retry, expected_manual
    ):
        """Each failure type gets correct retry_eligible and manual_review flags."""
        from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue

        db = _setup_db(tmp_path)
        dlq = DeadLetterQueue(db_path=db, retry_policies_path=_TEST_POLICIES_PATH)
        row_id = dlq.insert(
            f"thread_{failure_reason[:10]}",
            "DRAFT_GENERATED",
            failure_reason,
        )

        row = dlq.get_by_id(row_id)
        assert bool(row["retry_eligible"]) == expected_retry, (
            f"failure_reason='{failure_reason}': "
            f"expected retry_eligible={expected_retry}, got {row['retry_eligible']}"
        )
        assert bool(row["manual_review"]) == expected_manual, (
            f"failure_reason='{failure_reason}': "
            f"expected manual_review={expected_manual}, got {row['manual_review']}"
        )

    def test_hold_category_is_permanent_skip(self, tmp_path):
        """HOLD category entries are flagged permanent_skip=1."""
        from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue

        db = _setup_db(tmp_path)
        dlq = DeadLetterQueue(db_path=db, retry_policies_path=_TEST_POLICIES_PATH)
        row_id = dlq.insert("thread_hold", "DRAFT_GENERATED", "HOLD category")
        row = dlq.get_by_id(row_id)
        assert row["permanent_skip"] == 1, "HOLD category must set permanent_skip=1"

    def test_get_retryable_returns_eligible_entries(self, tmp_path):
        """get_retryable() returns entries that are eligible and past backoff."""
        from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue
        from src.zdi_mw.state.db import get_connection

        db = _setup_db(tmp_path)
        dlq = DeadLetterQueue(db_path=db, retry_policies_path=_TEST_POLICIES_PATH)

        # Insert a retryable entry with a past backoff time
        row_id = dlq.insert("thread_retry", "DRAFT_GENERATED", "Claude timeout")

        # Force backoff_next_at to the past so it's immediately retryable
        past = (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat()
        conn = get_connection(db)
        with conn:
            conn.execute(
                "UPDATE dead_letter_queue SET retry_backoff_next_at = ? WHERE id = ?",
                (past, row_id),
            )
        conn.close()

        retryable = dlq.get_retryable()
        ids = [r["id"] for r in retryable]
        assert row_id in ids, "Retryable entry past backoff must appear in get_retryable()"

    def test_get_retryable_excludes_non_eligible(self, tmp_path):
        """get_retryable() must not return non-retryable entries."""
        from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue

        db = _setup_db(tmp_path)
        dlq = DeadLetterQueue(db_path=db, retry_policies_path=_TEST_POLICIES_PATH)

        # Validation failure is not retryable
        row_id = dlq.insert("thread_no_retry", "DRAFT_GENERATED", "Validation failure")
        retryable = dlq.get_retryable()
        ids = [r["id"] for r in retryable]
        assert row_id not in ids, "Non-retryable entry must not appear in get_retryable()"

    def test_mark_resolved_updates_row(self, tmp_path):
        """mark_resolved() sets resolved_at and resolution fields."""
        from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue

        db = _setup_db(tmp_path)
        dlq = DeadLetterQueue(db_path=db, retry_policies_path=_TEST_POLICIES_PATH)
        row_id = dlq.insert("thread_res", "DRAFT_GENERATED", "Claude timeout")

        dlq.mark_resolved(row_id, "RETRIED_SUCCESSFULLY")
        row = dlq.get_by_id(row_id)

        assert row["resolved_at"] is not None
        assert row["resolution"] == "RETRIED_SUCCESSFULLY"

    def test_resolved_entries_excluded_from_retryable(self, tmp_path):
        """Resolved entries must not appear in get_retryable()."""
        from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue
        from src.zdi_mw.state.db import get_connection

        db = _setup_db(tmp_path)
        dlq = DeadLetterQueue(db_path=db, retry_policies_path=_TEST_POLICIES_PATH)

        row_id = dlq.insert("thread_done", "DRAFT_GENERATED", "Claude timeout")
        past = (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat()
        conn = get_connection(db)
        with conn:
            conn.execute(
                "UPDATE dead_letter_queue SET retry_backoff_next_at = ? WHERE id = ?",
                (past, row_id),
            )
        conn.close()

        dlq.mark_resolved(row_id, "MANUALLY_SKIPPED")
        retryable = dlq.get_retryable()
        assert row_id not in [r["id"] for r in retryable]

    def test_auth_failure_triggers_alert_log(self, tmp_path, caplog):
        """Auth failure entries should log a WARNING alert."""
        import logging
        from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue

        db = _setup_db(tmp_path)
        dlq = DeadLetterQueue(db_path=db, retry_policies_path=_TEST_POLICIES_PATH)

        with caplog.at_level(logging.WARNING):
            dlq.insert("thread_auth_fail", "AUTH", "Auth failure")

        assert any("ALERT_IMMEDIATE" in r.message for r in caplog.records), (
            "Auth failure must log ALERT_IMMEDIATE warning"
        )

    def test_retry_policies_loaded_from_json(self, tmp_path):
        """DLQ correctly loads all 8 policy types from retry_policies.json."""
        from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue

        db = _setup_db(tmp_path)
        dlq = DeadLetterQueue(db_path=db, retry_policies_path=_TEST_POLICIES_PATH)

        assert len(dlq._policies) >= 8, (
            f"Expected at least 8 retry policies, got {len(dlq._policies)}"
        )

        expected_keys = {
            "Claude timeout", "Rate limit hit", "Validation failure",
            "HOLD category", "Auth failure", "Schema mismatch",
            "Timeout", "Unconfirmed intent",
        }
        for key in expected_keys:
            assert key in dlq._policies, f"Missing policy key: '{key}'"

    def test_missing_policies_file_does_not_crash(self, tmp_path):
        """DLQ init with missing policies file logs warning but doesn't raise."""
        from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue

        db = _setup_db(tmp_path)
        # Non-existent path
        dlq = DeadLetterQueue(
            db_path=db,
            retry_policies_path=tmp_path / "nonexistent.json",
        )
        # Should not raise — defaults to empty policies with manual_review=True fallback
        assert dlq._policies == {}
