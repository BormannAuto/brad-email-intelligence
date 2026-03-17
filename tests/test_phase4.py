# tests/test_phase4.py
# ZDI Middleware — Phase 4 Verification Gate
#
# Verification criteria (spec Section 4 Phase 4):
#   1. pre_validate() rejects a schema-invalid input
#   2. post_validate() catches an unsourced dollar amount
#   3. WatchdogTimer fires after configured timeout and pushes to DLQ
#   4. Pipeline continues with remaining emails after a timeout
#   5. Section 10.3: Anti-math rule flags calculated totals

import json
import sys
import time
from dataclasses import dataclass, field as dc_field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional
from unittest.mock import MagicMock

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

_SCHEMA_DIR = _REPO_ROOT / "src" / "config"


# ===========================================================================
# Helpers / fixtures
# ===========================================================================

def _setup_db(tmp_path: Path) -> Path:
    from src.zdi_mw.state.db import init_db
    db = tmp_path / "zdi.db"
    init_db(db_path=db)
    return db


def _make_run_context(mode: str = "sandbox"):
    from src.zdi_mw.core.run_context import RunContext
    return RunContext(
        run_id="2026-03-16-071500-UTC",
        pipeline_version="git:abc1234",
        utc_start=datetime(2026, 3, 16, 7, 15, 0, tzinfo=timezone.utc),
        crm_snapshot_hash="sha256:abc",
        email_fetch_window=(
            datetime(2026, 3, 15, 7, 15, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 16, 7, 15, 0, tzinfo=timezone.utc),
        ),
        brad_voice_version=3,
        prompt_registry_version="prompts-2026-03",
        pipeline_mode=mode,
        degraded_flags={k: False for k in [
            "mail_down", "crm_down", "workdrive_down", "claude_down", "db_down"
        ]},
    )


def _valid_email_metadata() -> dict:
    return {
        "messageId": "msg_001",
        "threadId": "thread_001",
        "fromAddress": "John Smith <john@acme.com>",
        "toAddress": "brad@bormann.com",
        "subject": "Quote request for PTZ camera",
        "receivedTime": 1710576000000,
        "content": "<p>Hi Brad, I need a quote for the Sony PTZ camera.</p>",
    }


def _valid_record_snapshot() -> dict:
    return {
        "email": "john@acme.com",
        "run_id": "2026-03-16-071500-UTC",
        "snapshot_at": "2026-03-16T06:00:00+00:00",  # before utc_start
        "confidence": "HIGH",
        "is_stub": False,
        "contact": {"id": "crm_001", "Full_Name": "John Smith"},
        "notes": [],
        "deals": [],
    }


# Simple Pydantic-like mock for DraftOutput
@dataclass
class MockDraftOutput:
    draft_body: str
    confidence: str = "HIGH"
    flags: List[str] = dc_field(default_factory=list)
    word_count: int = 0
    source_references: List[str] = dc_field(default_factory=list)
    hold: bool = False
    category: str = ""

    def model_dump(self):
        return {
            "draft_body": self.draft_body,
            "confidence": self.confidence,
            "flags": self.flags,
        }


@dataclass
class MockCategorizerOutput:
    category: str
    confidence: str = "HIGH"
    urgency: int = 2
    draft_eligible: bool = True
    hold: bool = False
    sentiment_signals: List[str] = dc_field(default_factory=list)
    source_email_id: str = "msg_001"

    def model_dump(self):
        return {"category": self.category, "hold": self.hold}


# ===========================================================================
# Gate 1 — pre_validate() rejects schema-invalid input
# ===========================================================================

class TestPreValidateSchema:
    """Verify pre_validate() correctly rejects malformed email_metadata."""

    def test_valid_input_passes(self):
        """A fully valid email_metadata and record_snapshot passes all gates."""
        from src.zdi_mw.core.integrity_gate import IntegrityGate

        gate = IntegrityGate(_make_run_context(), _SCHEMA_DIR / "zoho_mail_schema.json")
        result = gate.pre_validate(_valid_record_snapshot(), _valid_email_metadata())
        assert result.passed, f"Expected PASSED, got reason={result.reason}"

    def test_missing_required_field_rejected(self):
        """email_metadata missing 'messageId' must fail schema validation."""
        from src.zdi_mw.core.integrity_gate import IntegrityGate

        gate = IntegrityGate(_make_run_context(), _SCHEMA_DIR / "zoho_mail_schema.json")
        bad_metadata = _valid_email_metadata()
        del bad_metadata["messageId"]

        result = gate.pre_validate(_valid_record_snapshot(), bad_metadata)
        assert not result.passed, "Schema-invalid input must fail pre_validate()"
        assert "messageId" in result.reason.lower() or "schema" in result.reason.lower()

    def test_missing_thread_id_rejected(self):
        """Missing threadId must fail schema validation."""
        from src.zdi_mw.core.integrity_gate import IntegrityGate

        gate = IntegrityGate(_make_run_context(), _SCHEMA_DIR / "zoho_mail_schema.json")
        bad = _valid_email_metadata()
        del bad["threadId"]

        result = gate.pre_validate(_valid_record_snapshot(), bad)
        assert not result.passed

    def test_missing_from_address_rejected(self):
        """Missing fromAddress must fail schema validation."""
        from src.zdi_mw.core.integrity_gate import IntegrityGate

        gate = IntegrityGate(_make_run_context(), _SCHEMA_DIR / "zoho_mail_schema.json")
        bad = _valid_email_metadata()
        del bad["fromAddress"]

        result = gate.pre_validate(_valid_record_snapshot(), bad)
        assert not result.passed


# ===========================================================================
# Gate 2 — pre_validate() confidence check
# ===========================================================================

class TestPreValidateConfidence:
    """Verify confidence=LOW blocks pipeline."""

    def test_low_confidence_rejected(self):
        """Contact match with confidence=LOW must fail pre_validate()."""
        from src.zdi_mw.core.integrity_gate import IntegrityGate

        gate = IntegrityGate(_make_run_context(), _SCHEMA_DIR / "zoho_mail_schema.json")
        snapshot = _valid_record_snapshot()
        snapshot["confidence"] = "LOW"

        result = gate.pre_validate(snapshot, _valid_email_metadata())
        assert not result.passed
        assert "LOW" in result.reason or "confidence" in result.reason.lower()

    def test_medium_confidence_passes(self):
        """confidence=MEDIUM must pass (only LOW is blocked)."""
        from src.zdi_mw.core.integrity_gate import IntegrityGate

        gate = IntegrityGate(_make_run_context(), _SCHEMA_DIR / "zoho_mail_schema.json")
        snapshot = _valid_record_snapshot()
        snapshot["confidence"] = "MEDIUM"

        result = gate.pre_validate(snapshot, _valid_email_metadata())
        assert result.passed, f"MEDIUM confidence must pass, got: {result.reason}"


# ===========================================================================
# Gate 3 — pre_validate() empty source body
# ===========================================================================

class TestPreValidateSourceText:
    """Verify empty email body is rejected."""

    def test_empty_body_rejected(self):
        """Email with empty content after HTML strip must fail."""
        from src.zdi_mw.core.integrity_gate import IntegrityGate

        gate = IntegrityGate(_make_run_context(), _SCHEMA_DIR / "zoho_mail_schema.json")
        meta = _valid_email_metadata()
        meta["content"] = "<p></p>  <br/>"  # empty after strip

        result = gate.pre_validate(_valid_record_snapshot(), meta)
        assert not result.passed
        assert "empty" in result.reason.lower()

    def test_whitespace_only_body_rejected(self):
        """Email body that is only whitespace after strip must fail."""
        from src.zdi_mw.core.integrity_gate import IntegrityGate

        gate = IntegrityGate(_make_run_context(), _SCHEMA_DIR / "zoho_mail_schema.json")
        meta = _valid_email_metadata()
        meta["content"] = "   \n   "

        result = gate.pre_validate(_valid_record_snapshot(), meta)
        assert not result.passed


# ===========================================================================
# Gate 4 — pre_validate() temporal check (cross-run contamination)
# ===========================================================================

class TestPreValidateTemporal:
    """Verify data created AFTER RunContext.utc_start is rejected."""

    def test_future_snapshot_rejected(self):
        """record_snapshot.snapshot_at after utc_start must fail temporal gate."""
        from src.zdi_mw.core.integrity_gate import IntegrityGate

        gate = IntegrityGate(_make_run_context(), _SCHEMA_DIR / "zoho_mail_schema.json")
        snapshot = _valid_record_snapshot()
        # snapshot_at is in 2027 — clearly after utc_start (2026-03-16)
        snapshot["snapshot_at"] = "2027-01-01T00:00:00+00:00"

        result = gate.pre_validate(snapshot, _valid_email_metadata())
        assert not result.passed
        assert "contamination" in result.reason.lower() or "after" in result.reason.lower()

    def test_past_snapshot_passes(self):
        """record_snapshot.snapshot_at before utc_start must pass."""
        from src.zdi_mw.core.integrity_gate import IntegrityGate

        gate = IntegrityGate(_make_run_context(), _SCHEMA_DIR / "zoho_mail_schema.json")
        snapshot = _valid_record_snapshot()
        snapshot["snapshot_at"] = "2026-03-15T06:00:00+00:00"  # day before

        result = gate.pre_validate(snapshot, _valid_email_metadata())
        assert result.passed, f"Past snapshot must pass, got: {result.reason}"


# ===========================================================================
# Gate 5 — post_validate() catches unsourced dollar amount
# ===========================================================================

class TestPostValidateDollarTraceability:
    """Core safety gate: dollar amounts must be verbatim in source."""

    def test_sourced_dollar_passes(self):
        """Dollar amount that appears in source email passes."""
        from src.zdi_mw.core.integrity_gate import IntegrityGate

        gate = IntegrityGate(_make_run_context())
        source = "The Sony PTZ camera is priced at $2,450. Let me know."
        draft = MockDraftOutput(draft_body="The camera is priced at $2,450.")

        result = gate.post_validate(draft, source)
        assert result.passed, f"Sourced dollar must pass, got: {result.reason}"

    def test_unsourced_dollar_blocked(self):
        """Dollar amount NOT in source email must block post_validate()."""
        from src.zdi_mw.core.integrity_gate import IntegrityGate, FLAG_UNSOURCED_PRICE

        gate = IntegrityGate(_make_run_context())
        source = "Hi Brad, please send me info on the Sony PTZ."
        draft = MockDraftOutput(draft_body="The camera is priced at $3,200.")  # invented

        result = gate.post_validate(draft, source)
        assert not result.passed, "Unsourced dollar must fail post_validate()"
        assert FLAG_UNSOURCED_PRICE in result.flags or "unsourced" in result.reason.lower()

    def test_unsourced_dollar_in_workdrive_passes(self):
        """Dollar in workdrive_context (not source email) must still pass."""
        from src.zdi_mw.core.integrity_gate import IntegrityGate

        gate = IntegrityGate(_make_run_context())
        source = "Hi Brad, please send me info on the Sony PTZ."
        workdrive = "Sony SRG-300H PTZ Camera — MSRP $2,995"
        draft = MockDraftOutput(draft_body="The camera is priced at $2,995.")

        result = gate.post_validate(draft, source, workdrive_context=workdrive)
        assert result.passed, f"Dollar sourced from WorkDrive must pass, got: {result.reason}"


# ===========================================================================
# Gate 6 — post_validate() HOLD rejection
# ===========================================================================

class TestPostValidateHold:
    """HOLD category emails must be rejected unless bypass_hold=True."""

    def test_hold_category_blocked(self):
        """output with hold=True must fail post_validate()."""
        from src.zdi_mw.core.integrity_gate import IntegrityGate

        gate = IntegrityGate(_make_run_context())
        output = MockCategorizerOutput(category="CUSTOMER_ISSUE", hold=True)
        result = gate.post_validate(output, "source text here")

        assert not result.passed
        assert "HOLD" in result.reason or "hold" in result.reason.lower()

    def test_hold_bypassed_by_flag(self):
        """With bypass_hold=True, HOLD check is skipped."""
        from src.zdi_mw.core.integrity_gate import IntegrityGate

        gate = IntegrityGate(_make_run_context(), bypass_hold=True)
        output = MockCategorizerOutput(category="CUSTOMER_ISSUE", hold=True)
        # Body has no dollar amounts so no other gates trip
        output_with_body = MockDraftOutput(draft_body="Thank you for reaching out.", hold=True)
        result = gate.post_validate(output_with_body, "Thank you for reaching out.")

        assert result.passed, f"bypass_hold=True must skip HOLD gate, got: {result.reason}"


# ===========================================================================
# Gate 7 — post_validate() word count flag
# ===========================================================================

class TestPostValidateWordCount:
    """Word count > 500 adds flag but does not block."""

    def test_long_draft_flagged_not_blocked(self):
        """Draft > 500 words adds FLAG_WORD_COUNT_EXCEEDED but passes."""
        from src.zdi_mw.core.integrity_gate import IntegrityGate, FLAG_WORD_COUNT_EXCEEDED

        gate = IntegrityGate(_make_run_context())
        # 510 words, no dollar amounts
        long_body = " ".join(["word"] * 510)
        draft = MockDraftOutput(draft_body=long_body)

        result = gate.post_validate(draft, long_body)
        assert result.passed, "Long draft must not be blocked, only flagged"
        assert FLAG_WORD_COUNT_EXCEEDED in result.flags


# ===========================================================================
# Gate 8 — Section 10.3 Anti-math rule
# ===========================================================================

class TestAntiMathRule:
    """Calculated dollar totals not verbatim in source must be flagged."""

    def test_calculated_total_flagged(self):
        """
        $450 + $1200 = $1650 in draft: not verbatim in source.
        Per Section 10.3: when WorkDrive context is present, calculated totals must be
        FLAGGED as UNSOURCED_CALCULATED_VALUE but must NOT block the draft.
        Brad may have intended a summary total — he must see the flag, but the draft proceeds.
        """
        from src.zdi_mw.core.integrity_gate import IntegrityGate, FLAG_UNSOURCED_CALCULATED_VALUE

        gate = IntegrityGate(_make_run_context())
        source = "Hi Brad"
        workdrive = "Item A: $450. Item B: $1,200."
        # AI computed the total — $1,650 is not verbatim in either source
        draft = MockDraftOutput(draft_body="The total for both items is $1,650.")

        result = gate.post_validate(draft, source, workdrive_context=workdrive)
        assert FLAG_UNSOURCED_CALCULATED_VALUE in result.flags, (
            "Calculated total $1,650 not verbatim in source must trigger anti-math flag"
        )
        # Section 10.3 explicitly says: "Do not block the draft"
        assert result.passed is True, (
            "Section 10.3: calculated value with WorkDrive present must flag but NOT block"
        )

    def test_verbatim_price_not_flagged(self):
        """A price that appears verbatim in workdrive must NOT trigger anti-math."""
        from src.zdi_mw.core.integrity_gate import IntegrityGate, FLAG_UNSOURCED_CALCULATED_VALUE

        gate = IntegrityGate(_make_run_context())
        source = "Hi Brad"
        workdrive = "Sony SRG-300H MSRP: $2,995"
        draft = MockDraftOutput(draft_body="The camera retails for $2,995.")

        result = gate.post_validate(draft, source, workdrive_context=workdrive)
        assert FLAG_UNSOURCED_CALCULATED_VALUE not in result.flags, (
            "Verbatim price from WorkDrive must not trigger anti-math flag"
        )

    def test_anti_math_only_fires_with_workdrive(self):
        """Anti-math rule only activates when workdrive_context is provided."""
        from src.zdi_mw.core.integrity_gate import IntegrityGate, FLAG_UNSOURCED_CALCULATED_VALUE

        gate = IntegrityGate(_make_run_context())
        source = "Hi Brad"
        # No workdrive — anti-math rule should not fire even for unsourced totals
        draft = MockDraftOutput(draft_body="The total is $1,650.")

        # This will fail dollar_traceability gate, not anti-math
        result = gate.post_validate(draft, source, workdrive_context=None)
        assert FLAG_UNSOURCED_CALCULATED_VALUE not in result.flags


# ===========================================================================
# Gate 9 — WatchdogTimer fires and pushes to DLQ
# ===========================================================================

class TestWatchdogTimer:
    """Verify WatchdogTimer fires on timeout, DLQs the stage, pipeline continues."""

    def test_watchdog_fires_on_timeout(self, tmp_path):
        """Stage that exceeds timeout should set dog.fired=True."""
        from src.zdi_mw.state.db import init_db
        from src.zdi_mw.state.ledger import PipelineStateLedger
        from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue
        from src.zdi_mw.core.watchdog import WatchdogTimer

        db = _setup_db(tmp_path)
        ledger = PipelineStateLedger("run-001", "git:abc", db_path=db)
        dlq = DeadLetterQueue(db_path=db, retry_policies_path=_REPO_ROOT / "src/config/retry_policies.json")
        ctx = _make_run_context()

        with WatchdogTimer(
            stage_name="DRAFT_GENERATED",
            thread_id="thread_timeout_test",
            run_context=ctx,
            ledger=ledger,
            dlq=dlq,
            timeout_seconds=0.1,  # 100ms for test speed
        ) as dog:
            time.sleep(0.3)  # exceed timeout

        assert dog.fired is True, "WatchdogTimer must have fired"

    def test_watchdog_creates_dlq_entry(self, tmp_path):
        """On timeout, a DLQ entry must be created."""
        from src.zdi_mw.state.db import init_db
        from src.zdi_mw.state.ledger import PipelineStateLedger
        from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue
        from src.zdi_mw.core.watchdog import WatchdogTimer

        db = _setup_db(tmp_path)
        ledger = PipelineStateLedger("run-001", "git:abc", db_path=db)
        dlq = DeadLetterQueue(db_path=db, retry_policies_path=_REPO_ROOT / "src/config/retry_policies.json")
        ctx = _make_run_context()

        with WatchdogTimer(
            stage_name="CRM_ENRICHED",
            thread_id="thread_dlq_check",
            run_context=ctx,
            ledger=ledger,
            dlq=dlq,
            timeout_seconds=0.1,
        ) as dog:
            time.sleep(0.3)

        # Allow timer thread to complete
        time.sleep(0.1)

        count = dlq.get_unresolved_count()
        assert count >= 1, "DLQ must have at least one entry after watchdog timeout"

    def test_watchdog_marks_ledger_failed(self, tmp_path):
        """On timeout, the stage must be marked FAILED in the Ledger."""
        from src.zdi_mw.state.db import init_db
        from src.zdi_mw.state.ledger import PipelineStateLedger
        from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue
        from src.zdi_mw.core.watchdog import WatchdogTimer

        db = _setup_db(tmp_path)
        ledger = PipelineStateLedger("run-001", "git:abc", db_path=db)
        dlq = DeadLetterQueue(db_path=db, retry_policies_path=_REPO_ROOT / "src/config/retry_policies.json")
        ctx = _make_run_context()

        with WatchdogTimer(
            stage_name="WORKDRIVE_LOOKUP",
            thread_id="thread_ledger_check",
            run_context=ctx,
            ledger=ledger,
            dlq=dlq,
            timeout_seconds=0.1,
        ) as dog:
            time.sleep(0.3)

        time.sleep(0.1)  # allow timer thread

        stage = ledger.get_stage("thread_ledger_check", "WORKDRIVE_LOOKUP")
        assert stage is not None, "Ledger must have an entry for the timed-out stage"
        assert stage["status"] == "FAILED", (
            f"Stage must be FAILED after timeout, got {stage['status']}"
        )

    def test_watchdog_clean_exit_does_not_fire(self, tmp_path):
        """Stage completing within timeout must NOT fire the watchdog."""
        from src.zdi_mw.state.db import init_db
        from src.zdi_mw.state.ledger import PipelineStateLedger
        from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue
        from src.zdi_mw.core.watchdog import WatchdogTimer

        db = _setup_db(tmp_path)
        ledger = PipelineStateLedger("run-001", "git:abc", db_path=db)
        dlq = DeadLetterQueue(db_path=db, retry_policies_path=_REPO_ROOT / "src/config/retry_policies.json")
        ctx = _make_run_context()

        with WatchdogTimer(
            stage_name="FETCHED",
            thread_id="thread_clean",
            run_context=ctx,
            ledger=ledger,
            dlq=dlq,
            timeout_seconds=5.0,  # generous timeout
        ) as dog:
            pass  # completes instantly

        assert dog.fired is False, "Clean stage must not fire watchdog"
        assert dlq.get_unresolved_count() == 0, "Clean stage must not create DLQ entry"

    def test_pipeline_continues_after_timeout(self, tmp_path):
        """
        Spec gate: pipeline processes remaining emails after a timeout.
        Simulate two threads — thread_1 times out, thread_2 completes normally.
        """
        from src.zdi_mw.state.db import init_db
        from src.zdi_mw.state.ledger import PipelineStateLedger
        from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue
        from src.zdi_mw.core.watchdog import WatchdogTimer

        db = _setup_db(tmp_path)
        ledger = PipelineStateLedger("run-001", "git:abc", db_path=db)
        dlq = DeadLetterQueue(db_path=db, retry_policies_path=_REPO_ROOT / "src/config/retry_policies.json")
        ctx = _make_run_context()

        threads_processed = []

        # Thread 1 — times out
        with WatchdogTimer(
            stage_name="DRAFT_GENERATED",
            thread_id="thread_slow",
            run_context=ctx, ledger=ledger, dlq=dlq,
            timeout_seconds=0.1,
        ) as dog1:
            time.sleep(0.3)
        # dog1.fired=True, but we continue

        # Thread 2 — completes fine (pipeline continues)
        with WatchdogTimer(
            stage_name="DRAFT_GENERATED",
            thread_id="thread_fast",
            run_context=ctx, ledger=ledger, dlq=dlq,
            timeout_seconds=5.0,
        ) as dog2:
            threads_processed.append("thread_fast")

        assert dog1.fired is True, "thread_slow must have timed out"
        assert dog2.fired is False, "thread_fast must have completed cleanly"
        assert "thread_fast" in threads_processed, "Pipeline must continue after timeout"
