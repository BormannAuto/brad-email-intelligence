# tests/test_phase7.py
# ZDI Middleware — Phase 7 Verification Gate
#
# Verification criteria (spec Section 4 Phase 7):
#   1. safe_write() blocks a LOW confidence write
#   2. safe_write() blocks an unsourced value (not in source_text)
#   3. safe_write() blocks an overwrite of existing CRM data
#   4. safe_write() succeeds when all gates pass
#   5. safe_write() idempotency key prevents duplicate writes
#   6. safe_write() WAL INTENT logged, then CONFIRMED on success
#   7. safe_create_draft() blocks a HOLD category email
#   8. safe_create_draft() blocks a HOLD sender from email_config
#   9. safe_create_draft() blocks mismatched recipient
#  10. safe_create_draft() blocks missing inReplyTo header
#  11. safe_create_draft() flags (does not block) body > 500 words
#  12. safe_create_draft() blocks a stale thread
#  13. safe_create_draft() succeeds when all gates pass
#  14. WorkDrive injection gate rejects a stale chunk (age > 7 days)
#  15. WorkDrive gate rejects a cross-brand chunk
#  16. WorkDrive gate rejects LOW confidence chunk
#  17. WorkDrive gate rejects chunk with untraceable numeric value
#  18. WorkDrive gate rejects truncated chunk
#  19. WorkDrive gate passes a clean chunk
#  20. safe_write() idempotency hit logged in accuracy_log

import sys
from dataclasses import dataclass, field as dc_field
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, call

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))


# ===========================================================================
# Helpers / fixtures
# ===========================================================================

@dataclass(frozen=True)
class _RunContext:
    run_id: str = "2026-03-16-140000-UTC"
    pipeline_version: str = "git:test1234"
    brad_voice_version: int = 1
    utc_start: datetime = dc_field(
        default_factory=lambda: datetime(2026, 3, 16, 14, 0, 0, tzinfo=timezone.utc)
    )


def _ctx(**kw):
    return _RunContext(**kw)


def _accuracy_logger(tmp_path):
    from src.zdi_mw.loggers.accuracy_logger import AccuracyLogger
    return AccuracyLogger(log_path=tmp_path / "acc.json")


def _init_db(tmp_path):
    from src.zdi_mw.state.db import init_db
    db = tmp_path / "zdi_state.db"
    init_db(db_path=db)
    return db


# ===========================================================================
# Gate tests — SafeCRMClient
# ===========================================================================

class TestSafeCRMGates:
    """Verify each gate blocks correctly."""

    def test_blocks_low_confidence(self, tmp_path):
        """Gate 1: LOW confidence write is blocked."""
        from src.zdi_mw.clients.safe_crm import SafeCRMClient

        client = SafeCRMClient(
            run_context=_ctx(), accuracy_logger=_accuracy_logger(tmp_path)
        )
        result = client.safe_write(
            contact_id="c001", field="Phone",
            value="555-1234", source_text="Phone: 555-1234",
            confidence="LOW",
        )
        assert result.success is False
        assert result.skip_reason == "CONFIDENCE_LOW"

    def test_blocks_unsourced_value(self, tmp_path):
        """Gate 2: Value not present in source_text is blocked."""
        from src.zdi_mw.clients.safe_crm import SafeCRMClient

        client = SafeCRMClient(
            run_context=_ctx(), accuracy_logger=_accuracy_logger(tmp_path)
        )
        result = client.safe_write(
            contact_id="c001", field="Phone",
            value="999-0000",
            source_text="The customer's phone is 555-1234.",
            confidence="HIGH",
        )
        assert result.success is False
        assert result.skip_reason == "VALUE_NOT_IN_SOURCE"

    def test_blocks_overwrite_existing_field(self, tmp_path):
        """Gate 3: If field already has a value in CRM, write is blocked."""
        from src.zdi_mw.clients.safe_crm import SafeCRMClient

        client = SafeCRMClient(
            run_context=_ctx(), accuracy_logger=_accuracy_logger(tmp_path),
            _crm_read_fn=lambda cid, f: "555-EXISTING",
        )
        result = client.safe_write(
            contact_id="c001", field="Phone",
            value="555-1234", source_text="Phone: 555-1234",
            confidence="HIGH",
        )
        assert result.success is False
        assert result.skip_reason == "FIELD_ALREADY_SET"

    def test_fills_blank_field(self, tmp_path):
        """Gate 3 passes when the CRM field is currently blank."""
        from src.zdi_mw.clients.safe_crm import SafeCRMClient

        write_calls = []
        def fake_write(cid, field, val):
            write_calls.append((cid, field, val))
            return True

        client = SafeCRMClient(
            run_context=_ctx(), accuracy_logger=_accuracy_logger(tmp_path),
            _crm_read_fn=lambda cid, f: None,
            _crm_write_fn=fake_write,
        )
        result = client.safe_write(
            contact_id="c001", field="Phone",
            value="555-1234", source_text="Phone: 555-1234",
            confidence="HIGH",
        )
        assert result.success is True
        assert write_calls == [("c001", "Phone", "555-1234")]

    def test_idempotency_prevents_duplicate_write(self, tmp_path):
        """Gate 4: Second identical write returns idempotency_hit=True without calling CRM."""
        from src.zdi_mw.clients.safe_crm import SafeCRMClient

        db = _init_db(tmp_path)
        write_calls = []

        def fake_write(cid, field, val):
            write_calls.append(1)
            return True

        ctx = _ctx()
        client = SafeCRMClient(
            run_context=ctx, accuracy_logger=_accuracy_logger(tmp_path),
            _crm_read_fn=lambda cid, f: None,
            _crm_write_fn=fake_write,
            _db_path=db,
        )

        # First write
        r1 = client.safe_write(
            contact_id="c001", field="Phone",
            value="555-1234", source_text="Phone: 555-1234",
            confidence="HIGH",
        )
        assert r1.success is True

        # Second identical write — should hit idempotency
        r2 = client.safe_write(
            contact_id="c001", field="Phone",
            value="555-1234", source_text="Phone: 555-1234",
            confidence="HIGH",
        )
        assert r2.idempotency_hit is True
        assert len(write_calls) == 1, "CRM write should only be called once"

    def test_wal_intent_and_confirm(self, tmp_path):
        """WAL logs INTENT before write and CONFIRMED after success."""
        from src.zdi_mw.clients.safe_crm import SafeCRMClient

        mock_wal = MagicMock()
        mock_wal.log_intent.return_value = 42

        client = SafeCRMClient(
            run_context=_ctx(), accuracy_logger=_accuracy_logger(tmp_path),
            _crm_read_fn=lambda cid, f: None,
            _crm_write_fn=lambda cid, f, v: True,
            _wal=mock_wal,
        )
        result = client.safe_write(
            contact_id="c001", field="Phone",
            value="555-1234", source_text="Phone: 555-1234",
            confidence="HIGH",
        )
        assert result.success is True
        mock_wal.log_intent.assert_called_once()
        mock_wal.confirm.assert_called_once_with(42)

    def test_wal_fail_called_on_write_error(self, tmp_path):
        """WAL.fail() is called when the CRM write raises."""
        from src.zdi_mw.clients.safe_crm import SafeCRMClient

        mock_wal = MagicMock()
        mock_wal.log_intent.return_value = 99

        def bad_write(cid, f, v):
            raise ConnectionError("Simulated Zoho failure")

        client = SafeCRMClient(
            run_context=_ctx(), accuracy_logger=_accuracy_logger(tmp_path),
            _crm_read_fn=lambda cid, f: None,
            _crm_write_fn=bad_write,
            _wal=mock_wal,
        )
        result = client.safe_write(
            contact_id="c001", field="Phone",
            value="555-1234", source_text="Phone: 555-1234",
            confidence="HIGH",
        )
        assert result.success is False
        mock_wal.fail.assert_called_once()


# ===========================================================================
# Gate tests — SafeMailClient
# ===========================================================================

class TestSafeMailGates:
    """Verify SafeMailClient gate logic."""

    def test_blocks_hold_category(self, tmp_path):
        """Gate 1: HOLD category email is blocked."""
        from src.zdi_mw.clients.safe_mail import SafeMailClient

        client = SafeMailClient(
            run_context=_ctx(), accuracy_logger=_accuracy_logger(tmp_path),
        )
        result = client.safe_create_draft(
            thread_id="t001", recipient="customer@example.com",
            body="Thank you for your inquiry.",
            category="HOLD",
            source_from_address="customer@example.com",
            in_reply_to="<thread-001@mail.zoho.com>",
        )
        assert result.success is False
        assert "HOLD" in result.skip_reason

    def test_blocks_hold_sender_from_config(self, tmp_path):
        """Gate 1: Recipient in hold_senders list is blocked (even if category != HOLD)."""
        from src.zdi_mw.clients.safe_mail import SafeMailClient

        email_config = {"hold_senders": ["flagged@vendor.com"]}
        client = SafeMailClient(
            run_context=_ctx(), accuracy_logger=_accuracy_logger(tmp_path),
            _email_config=email_config,
        )
        result = client.safe_create_draft(
            thread_id="t002", recipient="flagged@vendor.com",
            body="Thank you.", category="QUOTE_REQUEST",
            source_from_address="flagged@vendor.com",
            in_reply_to="<thread-002@mail.zoho.com>",
        )
        assert result.success is False
        assert "HOLD_SENDER" in result.skip_reason

    def test_blocks_stale_thread(self, tmp_path):
        """Gate 2: Thread not found in live API blocks draft creation."""
        from src.zdi_mw.clients.safe_mail import SafeMailClient

        client = SafeMailClient(
            run_context=_ctx(), accuracy_logger=_accuracy_logger(tmp_path),
            _mail_thread_verify_fn=lambda tid: False,
        )
        result = client.safe_create_draft(
            thread_id="t003", recipient="customer@example.com",
            body="Reply content.", category="QUOTE_REQUEST",
            source_from_address="customer@example.com",
            in_reply_to="<ref>",
        )
        assert result.success is False
        assert result.skip_reason == "THREAD_NOT_FOUND"

    def test_blocks_recipient_mismatch(self, tmp_path):
        """Gate 3: Recipient != source fromAddress is blocked."""
        from src.zdi_mw.clients.safe_mail import SafeMailClient

        client = SafeMailClient(
            run_context=_ctx(), accuracy_logger=_accuracy_logger(tmp_path),
            _mail_thread_verify_fn=lambda tid: True,
        )
        result = client.safe_create_draft(
            thread_id="t004", recipient="wrong@example.com",
            body="Reply.", category="QUOTE_REQUEST",
            source_from_address="correct@example.com",
            in_reply_to="<ref>",
        )
        assert result.success is False
        assert result.skip_reason == "RECIPIENT_MISMATCH"

    def test_blocks_missing_in_reply_to(self, tmp_path):
        """Gate 4: Empty inReplyTo header is blocked."""
        from src.zdi_mw.clients.safe_mail import SafeMailClient

        client = SafeMailClient(
            run_context=_ctx(), accuracy_logger=_accuracy_logger(tmp_path),
            _mail_thread_verify_fn=lambda tid: True,
        )
        result = client.safe_create_draft(
            thread_id="t005", recipient="customer@example.com",
            body="Reply.", category="QUOTE_REQUEST",
            source_from_address="customer@example.com",
            in_reply_to="",  # missing
        )
        assert result.success is False
        assert result.skip_reason == "MISSING_IN_REPLY_TO"

    def test_flags_but_does_not_block_long_body(self, tmp_path):
        """Gate 5: Body > 500 words sets word_count_flagged=True but does NOT block."""
        from src.zdi_mw.clients.safe_mail import SafeMailClient

        long_body = " ".join(["word"] * 501)
        client = SafeMailClient(
            run_context=_ctx(), accuracy_logger=_accuracy_logger(tmp_path),
            _mail_thread_verify_fn=lambda tid: True,
            _mail_create_draft_fn=lambda tid, rec, body, irt: "draft_001",
        )
        result = client.safe_create_draft(
            thread_id="t006", recipient="customer@example.com",
            body=long_body, category="QUOTE_REQUEST",
            source_from_address="customer@example.com",
            in_reply_to="<ref>",
        )
        assert result.success is True
        assert result.word_count_flagged is True
        assert result.word_count == 501

    def test_success_all_gates_pass(self, tmp_path):
        """All gates pass: draft is created and draft_id returned."""
        from src.zdi_mw.clients.safe_mail import SafeMailClient

        client = SafeMailClient(
            run_context=_ctx(), accuracy_logger=_accuracy_logger(tmp_path),
            _mail_thread_verify_fn=lambda tid: True,
            _mail_create_draft_fn=lambda tid, rec, body, irt: "draft_abc123",
        )
        result = client.safe_create_draft(
            thread_id="t007", recipient="customer@example.com",
            body="Thank you for your inquiry about the Shure MX412.",
            category="QUOTE_REQUEST",
            source_from_address="customer@example.com",
            in_reply_to="<thread-007@mail.zoho.com>",
        )
        assert result.success is True
        assert result.draft_id == "draft_abc123"
        assert result.word_count_flagged is False

    def test_wal_intent_confirmed_on_success(self, tmp_path):
        """WAL logs INTENT then CONFIRMED on successful draft creation."""
        from src.zdi_mw.clients.safe_mail import SafeMailClient

        mock_wal = MagicMock()
        mock_wal.log_intent.return_value = 77

        client = SafeMailClient(
            run_context=_ctx(), accuracy_logger=_accuracy_logger(tmp_path),
            _mail_thread_verify_fn=lambda tid: True,
            _mail_create_draft_fn=lambda tid, rec, body, irt: "draft_xyz",
            _wal=mock_wal,
        )
        result = client.safe_create_draft(
            thread_id="t008", recipient="c@example.com",
            body="Short reply.", category="QUOTE_REQUEST",
            source_from_address="c@example.com",
            in_reply_to="<ref008>",
        )
        assert result.success is True
        mock_wal.log_intent.assert_called_once()
        mock_wal.confirm.assert_called_once_with(77)


# ===========================================================================
# Gate tests — SafeWorkDriveClient
# ===========================================================================

class TestSafeWorkDriveGates:
    """Verify SafeWorkDriveClient 5-gate logic."""

    def _make_chunk(self, **overrides):
        """Minimal valid chunk dict."""
        base = {
            "chunk_id": "chunk_001",
            "brand_tag": "Shure",
            "confidence": "HIGH",
            "index_age_days": 3,
            "content": "MX412 price $450",
            "raw_source_values": ["$450"],
        }
        base.update(overrides)
        return base

    def test_blocks_stale_index(self, tmp_path):
        """Gate 1: index_age_days > 7 is blocked."""
        from src.zdi_mw.clients.safe_workdrive import SafeWorkDriveClient

        chunk = self._make_chunk(index_age_days=8)
        client = SafeWorkDriveClient(
            run_context=_ctx(),
            _workdrive_lookup_fn=lambda b, m, q: chunk,
        )
        result = client.safe_lookup_chunk("Shure", "MX412", "price inquiry")
        assert result.blocked is True
        assert "STALE_INDEX" in result.block_reason

    def test_blocks_cross_brand(self, tmp_path):
        """Gate 2: Chunk brand_tag != requested brand is blocked."""
        from src.zdi_mw.clients.safe_workdrive import SafeWorkDriveClient

        chunk = self._make_chunk(brand_tag="Biamp")  # wrong brand
        client = SafeWorkDriveClient(
            run_context=_ctx(),
            _workdrive_lookup_fn=lambda b, m, q: chunk,
        )
        result = client.safe_lookup_chunk("Shure", "MX412", "price inquiry")
        assert result.blocked is True
        assert result.block_reason == "BRAND_MISMATCH"

    def test_blocks_low_confidence(self, tmp_path):
        """Gate 3: LOW confidence chunk is blocked."""
        from src.zdi_mw.clients.safe_workdrive import SafeWorkDriveClient

        chunk = self._make_chunk(confidence="LOW")
        client = SafeWorkDriveClient(
            run_context=_ctx(),
            _workdrive_lookup_fn=lambda b, m, q: chunk,
        )
        result = client.safe_lookup_chunk("Shure", "MX412", "price inquiry")
        assert result.blocked is True
        assert result.block_reason == "CONFIDENCE_LOW"

    def test_blocks_untraceable_numeric(self, tmp_path):
        """Gate 4: Numeric value in content not present in raw_source_values is blocked."""
        from src.zdi_mw.clients.safe_workdrive import SafeWorkDriveClient

        # $1650 is not in raw_source_values — would be a calculated total
        chunk = self._make_chunk(
            content="MX412 $450, amp $1200, total $1650",
            raw_source_values=["$450", "$1200"],  # $1650 missing
        )
        client = SafeWorkDriveClient(
            run_context=_ctx(),
            _workdrive_lookup_fn=lambda b, m, q: chunk,
        )
        result = client.safe_lookup_chunk("Shure", "MX412", "price inquiry")
        assert result.blocked is True
        assert "UNTRACEABLE_NUMERICS" in result.block_reason

    def test_blocks_truncated_chunk(self, tmp_path):
        """Gate 5: Chunk ending with truncation indicator is blocked."""
        from src.zdi_mw.clients.safe_workdrive import SafeWorkDriveClient

        chunk = self._make_chunk(
            content="MX412 is available in multiple configurations...",
            raw_source_values=[],
        )
        client = SafeWorkDriveClient(
            run_context=_ctx(),
            _workdrive_lookup_fn=lambda b, m, q: chunk,
        )
        result = client.safe_lookup_chunk("Shure", "MX412", "availability")
        assert result.blocked is True
        assert result.block_reason == "CHUNK_TRUNCATED"

    def test_passes_clean_chunk(self, tmp_path):
        """All 5 gates pass: clean chunk is returned."""
        from src.zdi_mw.clients.safe_workdrive import SafeWorkDriveClient

        chunk = self._make_chunk(
            content="MX412 standard price is $450 per unit.",
            raw_source_values=["$450"],
        )
        client = SafeWorkDriveClient(
            run_context=_ctx(),
            _workdrive_lookup_fn=lambda b, m, q: chunk,
        )
        result = client.safe_lookup_chunk("Shure", "MX412", "price inquiry")
        assert result.blocked is False
        assert result.chunk is not None
        assert result.chunk["chunk_id"] == "chunk_001"

    def test_returns_none_when_no_chunk_found(self, tmp_path):
        """Lookup returning None gives chunk=None, blocked=False."""
        from src.zdi_mw.clients.safe_workdrive import SafeWorkDriveClient

        client = SafeWorkDriveClient(
            run_context=_ctx(),
            _workdrive_lookup_fn=lambda b, m, q: None,
        )
        result = client.safe_lookup_chunk("Shure", "MX412", "price")
        assert result.chunk is None
        assert result.blocked is False
        assert result.block_reason == "NO_CHUNK_FOUND"

    def test_no_lookup_fn_returns_blocked(self, tmp_path):
        """No lookup function returns blocked=True with NO_LOOKUP_FN reason."""
        from src.zdi_mw.clients.safe_workdrive import SafeWorkDriveClient

        client = SafeWorkDriveClient(run_context=_ctx())
        result = client.safe_lookup_chunk("Shure", "MX412", "price")
        assert result.blocked is True
        assert result.block_reason == "NO_LOOKUP_FN"


# ===========================================================================
# Helper unit tests
# ===========================================================================

class TestHelpers:
    """Verify module-level helper functions."""

    def test_value_present_in_source_case_insensitive(self):
        from src.zdi_mw.clients.safe_crm import _value_present_in_source
        assert _value_present_in_source("555-1234", "Call us at 555-1234 today") is True
        assert _value_present_in_source("555-1234", "No match here") is False

    def test_idempotency_key_deterministic(self):
        from src.zdi_mw.clients.safe_crm import _compute_idempotency_key
        k1 = _compute_idempotency_key("run1", "c001", "Phone", "source", "555-1234")
        k2 = _compute_idempotency_key("run1", "c001", "Phone", "source", "555-1234")
        assert k1 == k2

    def test_idempotency_key_changes_with_run_id(self):
        from src.zdi_mw.clients.safe_crm import _compute_idempotency_key
        k1 = _compute_idempotency_key("run1", "c001", "Phone", "source", "555-1234")
        k2 = _compute_idempotency_key("run2", "c001", "Phone", "source", "555-1234")
        assert k1 != k2

    def test_truncation_detection(self):
        from src.zdi_mw.clients.safe_workdrive import _is_truncated
        assert _is_truncated("content ends with comma,") is True
        assert _is_truncated("content ends with pipe|") is True
        assert _is_truncated("content ends with dash-") is True
        assert _is_truncated("content ends with ellipsis...") is True
        assert _is_truncated("content ends normally.") is False
        assert _is_truncated("full sentence here") is False

    def test_untraceable_numerics_finds_calculated_total(self):
        from src.zdi_mw.clients.safe_workdrive import _find_untraceable_numerics
        content = "Item A $450, Item B $1200, Total $1650"
        sources = ["$450", "$1200"]  # $1650 is calculated
        untraceable = _find_untraceable_numerics(content, sources)
        assert "$1650" in untraceable

    def test_untraceable_numerics_passes_when_all_sourced(self):
        from src.zdi_mw.clients.safe_workdrive import _find_untraceable_numerics
        content = "Price is $450 per unit"
        sources = ["$450"]
        untraceable = _find_untraceable_numerics(content, sources)
        assert untraceable == []


# ===========================================================================
# VERIFICATION GATE
# ===========================================================================

class TestVerificationGate:
    """
    Phase 7 Verification Gate (spec Section 4 Phase 7):
    safe_write() blocks LOW confidence write ✓ (test_blocks_low_confidence)
    safe_write() blocks unsourced value ✓ (test_blocks_unsourced_value)
    safe_write() blocks overwrite of existing data ✓ (test_blocks_overwrite_existing_field)
    safe_create_draft() blocks HOLD category ✓ (test_blocks_hold_category)
    WorkDrive injection gate rejects stale chunk ✓ (test_blocks_stale_index)

    This class runs the full gate sequence explicitly as named in the spec.
    """

    def test_verification_gate_crm_low_confidence(self, tmp_path):
        """GATE: safe_write() blocks a LOW confidence write."""
        from src.zdi_mw.clients.safe_crm import SafeCRMClient
        client = SafeCRMClient(run_context=_ctx(), accuracy_logger=_accuracy_logger(tmp_path))
        result = client.safe_write("c999", "Mobile", "555-9999", "Mobile: 555-9999", "LOW")
        assert result.success is False and result.skip_reason == "CONFIDENCE_LOW"

    def test_verification_gate_crm_unsourced_value(self, tmp_path):
        """GATE: safe_write() blocks an unsourced value."""
        from src.zdi_mw.clients.safe_crm import SafeCRMClient
        client = SafeCRMClient(run_context=_ctx(), accuracy_logger=_accuracy_logger(tmp_path))
        result = client.safe_write("c999", "Mobile", "555-XXXX", "Nothing useful here", "HIGH")
        assert result.success is False and result.skip_reason == "VALUE_NOT_IN_SOURCE"

    def test_verification_gate_crm_overwrite_blocked(self, tmp_path):
        """GATE: safe_write() blocks an overwrite of existing CRM data."""
        from src.zdi_mw.clients.safe_crm import SafeCRMClient
        client = SafeCRMClient(
            run_context=_ctx(), accuracy_logger=_accuracy_logger(tmp_path),
            _crm_read_fn=lambda cid, f: "EXISTING_VALUE",
        )
        result = client.safe_write("c999", "Mobile", "555-1234", "Mobile: 555-1234", "HIGH")
        assert result.success is False and result.skip_reason == "FIELD_ALREADY_SET"

    def test_verification_gate_mail_hold_blocked(self, tmp_path):
        """GATE: safe_create_draft() blocks a HOLD category email."""
        from src.zdi_mw.clients.safe_mail import SafeMailClient
        client = SafeMailClient(run_context=_ctx(), accuracy_logger=_accuracy_logger(tmp_path))
        result = client.safe_create_draft(
            thread_id="t_gate", recipient="c@example.com",
            body="Hello.", category="HOLD",
            source_from_address="c@example.com",
            in_reply_to="<ref>",
        )
        assert result.success is False and "HOLD" in result.skip_reason

    def test_verification_gate_workdrive_stale_blocked(self, tmp_path):
        """GATE: WorkDrive injection gate rejects a stale chunk."""
        from src.zdi_mw.clients.safe_workdrive import SafeWorkDriveClient
        stale = {
            "chunk_id": "c1", "brand_tag": "Shure", "confidence": "HIGH",
            "index_age_days": 10, "content": "Price $450", "raw_source_values": ["$450"],
        }
        client = SafeWorkDriveClient(run_context=_ctx(), _workdrive_lookup_fn=lambda b, m, q: stale)
        result = client.safe_lookup_chunk("Shure", "MX412", "price")
        assert result.blocked is True and "STALE_INDEX" in result.block_reason
