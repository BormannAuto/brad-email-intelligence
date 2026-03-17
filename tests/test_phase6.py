# tests/test_phase6.py
# ZDI Middleware — Phase 6 Verification Gate
#
# Verification criteria (spec Section 4 Phase 6):
#   1. hydrate_contact() returns identical result for same contact called twice
#      (from cache on second call — CRM client called only once)
#   2. Stub contacts contain email only; all other fields null/empty
#   3. HydratedContact is frozen — mutation raises FrozenInstanceError
#   4. Real contact assembles correctly from CRM raw data
#   5. Notes filtered to last 30 days, max 10
#   6. Deals filtered to Open/Negotiation/Quote Sent only
#   7. Attachments metadata only, max 5; content never included
#   8. CRM lookup exception → graceful stub (pipeline continues)
#   9. No CRM function configured → stub returned
#  10. cache_size reflects unique objects, not aliases
#  11. clear_cache() resets state
#  12. Normalised email (case folded) produces cache hits regardless of original case
#  13. Multiple emails in same run each get separate cache entries
#  14. contact field is MappingProxyType (immutable mapping)
#  15. first_seen for stub is utc_now() (not None)
#  16. snapshot_at and run_id present on all records
#  17. Notes beyond 10 are discarded
#  18. Attachments beyond 5 are discarded
#  19. Deals with wrong stage (e.g. Closed Won) excluded
#  20. Notes older than 30 days excluded
#  21. VERIFICATION GATE: same email called twice → mock CRM call count == 1

import sys
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, call

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))


# ===========================================================================
# Helpers / fixtures
# ===========================================================================

from dataclasses import dataclass, field as dc_field


@dataclass(frozen=True)
class _RunContext:
    run_id: str = "2026-03-16-130000-UTC"
    pipeline_version: str = "git:test1234"
    brad_voice_version: int = 1
    utc_start: datetime = dc_field(
        default_factory=lambda: datetime(2026, 3, 16, 13, 0, 0, tzinfo=timezone.utc)
    )


def _make_run_context(**kwargs) -> _RunContext:
    return _RunContext(**kwargs)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _days_ago(n: int) -> str:
    """ISO string for a datetime n days ago."""
    return (_now_utc() - timedelta(days=n)).isoformat()


def _make_crm_raw(
    contact_id: str = "crm_001",
    notes: Optional[list] = None,
    deals: Optional[list] = None,
    attachments: Optional[list] = None,
) -> Dict[str, Any]:
    """Build a minimal valid CRM raw response dict."""
    return {
        "id": contact_id,
        "created_at": _days_ago(60),
        "contact_fields": {
            "first_name": "Alice",
            "last_name": "Smith",
            "phone": "555-1234",
        },
        "notes": notes if notes is not None else [],
        "deals": deals if deals is not None else [],
        "attachments": attachments if attachments is not None else [],
    }


# ===========================================================================
# Gate 1 — Stub contact behavior
# ===========================================================================

class TestStubContact:
    """Verify stub contact structure when no CRM match is found."""

    def test_stub_returned_when_no_crm_match(self):
        """hydrate_contact() returns a stub when CRM lookup returns None."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: None)
        record = assembler.hydrate_contact("nobody@example.com", "thread_001")

        assert record.is_stub is True
        assert record.id is None

    def test_stub_email_normalised(self):
        """Stub contact email is lowercased and stripped."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: None)
        record = assembler.hydrate_contact("  UPPER@Example.COM  ", "thread_001")

        assert record.email == "upper@example.com"

    def test_stub_contact_field_is_none(self):
        """Stub contact has contact=None (no CRM data inferred)."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: None)
        record = assembler.hydrate_contact("stub@example.com", "thread_001")

        assert record.contact is None

    def test_stub_notes_empty_tuple(self):
        """Stub contact has notes=() — not None, not a list."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: None)
        record = assembler.hydrate_contact("stub@example.com", "thread_001")

        assert record.notes == ()
        assert record.deals == ()
        assert record.attachments_metadata == ()

    def test_stub_first_seen_is_utc_now(self):
        """Stub first_seen is a UTC datetime (time of assembly)."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        before = _now_utc()
        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: None)
        record = assembler.hydrate_contact("stub@example.com", "thread_001")
        after = _now_utc()

        assert record.first_seen is not None
        assert before <= record.first_seen <= after

    def test_stub_has_run_id(self):
        """Stub contact's run_id matches run_context.run_id."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        ctx = _make_run_context(run_id="test-run-xyz")
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: None)
        record = assembler.hydrate_contact("stub@example.com", "thread_001")

        assert record.run_id == "test-run-xyz"

    def test_stub_returned_when_no_lookup_fn(self):
        """When _crm_lookup_fn is None, every call returns a stub."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=None)
        record = assembler.hydrate_contact("any@example.com", "thread_001")

        assert record.is_stub is True

    def test_stub_when_crm_raises(self):
        """CRM lookup exception produces a stub (pipeline continues)."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        def bad_crm(email: str):
            raise ConnectionError("Simulated CRM down")

        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=bad_crm)
        record = assembler.hydrate_contact("any@example.com", "thread_001")

        assert record.is_stub is True
        assert record.id is None


# ===========================================================================
# Gate 2 — Real contact assembly
# ===========================================================================

class TestRealContactAssembly:
    """Verify real contact is assembled correctly from CRM raw data."""

    def test_real_contact_is_not_stub(self):
        """When CRM returns data, is_stub=False."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        raw = _make_crm_raw()
        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: raw)
        record = assembler.hydrate_contact("real@example.com", "thread_001")

        assert record.is_stub is False
        assert record.id == "crm_001"

    def test_real_contact_fields_accessible(self):
        """contact field contains CRM contact_fields as MappingProxyType."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler
        import types

        raw = _make_crm_raw()
        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: raw)
        record = assembler.hydrate_contact("real@example.com", "thread_001")

        assert isinstance(record.contact, types.MappingProxyType)
        assert record.contact["first_name"] == "Alice"

    def test_contact_proxy_is_immutable(self):
        """MappingProxyType contact field cannot be mutated."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        raw = _make_crm_raw()
        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: raw)
        record = assembler.hydrate_contact("real@example.com", "thread_001")

        with pytest.raises(TypeError):
            record.contact["first_name"] = "MUTATED"  # type: ignore[index]

    def test_real_contact_snapshot_and_run_id(self):
        """snapshot_at is a recent UTC datetime; run_id matches context."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        raw = _make_crm_raw()
        before = _now_utc()
        ctx = _make_run_context(run_id="run-real-001")
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: raw)
        record = assembler.hydrate_contact("real@example.com", "thread_001")
        after = _now_utc()

        assert before <= record.snapshot_at <= after
        assert record.run_id == "run-real-001"


# ===========================================================================
# Gate 3 — Frozen contract
# ===========================================================================

class TestFrozenContract:
    """Verify HydratedContact is truly immutable."""

    def test_frozen_dataclass_raises_on_set(self):
        """Attempting to set a field on HydratedContact raises FrozenInstanceError."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: None)
        record = assembler.hydrate_contact("frozen@example.com", "t1")

        with pytest.raises(FrozenInstanceError):
            record.email = "mutated@example.com"  # type: ignore[misc]

    def test_frozen_dataclass_raises_on_delete(self):
        """Attempting to delete a field on HydratedContact raises FrozenInstanceError."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: None)
        record = assembler.hydrate_contact("frozen@example.com", "t1")

        with pytest.raises(FrozenInstanceError):
            del record.is_stub  # type: ignore[misc]

    def test_notes_is_tuple_not_list(self):
        """notes, deals, attachments_metadata are tuples — not mutable lists."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        notes = [{"id": "n1", "created_at": _days_ago(1), "note_type": "INBOUND"}]
        raw = _make_crm_raw(notes=notes)
        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: raw)
        record = assembler.hydrate_contact("real@example.com", "t1")

        assert isinstance(record.notes, tuple)
        assert isinstance(record.deals, tuple)
        assert isinstance(record.attachments_metadata, tuple)


# ===========================================================================
# Gate 4 — Notes filtering
# ===========================================================================

class TestNotesFiltering:
    """Verify notes are filtered to last 30 days, max 10."""

    def test_notes_within_30_days_included(self):
        """Notes created within 30 days are included."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        notes = [
            {"id": "n1", "created_at": _days_ago(5), "note_type": "INBOUND"},
            {"id": "n2", "created_at": _days_ago(29), "note_type": "OUTBOUND"},
        ]
        raw = _make_crm_raw(notes=notes)
        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: raw)
        record = assembler.hydrate_contact("real@example.com", "t1")

        assert len(record.notes) == 2

    def test_notes_older_than_30_days_excluded(self):
        """Notes older than 30 days are excluded."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        notes = [
            {"id": "n1", "created_at": _days_ago(31), "note_type": "INBOUND"},
            {"id": "n2", "created_at": _days_ago(100), "note_type": "OUTBOUND"},
        ]
        raw = _make_crm_raw(notes=notes)
        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: raw)
        record = assembler.hydrate_contact("real@example.com", "t1")

        assert len(record.notes) == 0

    def test_notes_capped_at_10(self):
        """More than 10 recent notes are trimmed to exactly 10."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        notes = [
            {"id": f"n{i}", "created_at": _days_ago(1), "note_type": "INBOUND"}
            for i in range(15)
        ]
        raw = _make_crm_raw(notes=notes)
        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: raw)
        record = assembler.hydrate_contact("real@example.com", "t1")

        assert len(record.notes) == 10

    def test_note_content_not_in_metadata(self):
        """Note 'content' field is not included in the returned metadata."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        notes = [
            {
                "id": "n1",
                "created_at": _days_ago(1),
                "note_type": "INBOUND",
                "content": "SECRET BUSINESS CONTENT — MUST NOT APPEAR",
            }
        ]
        raw = _make_crm_raw(notes=notes)
        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: raw)
        record = assembler.hydrate_contact("real@example.com", "t1")

        assert len(record.notes) == 1
        assert "content" not in record.notes[0]
        assert "SECRET BUSINESS CONTENT" not in str(record.notes[0])


# ===========================================================================
# Gate 5 — Deals filtering
# ===========================================================================

class TestDealsFiltering:
    """Verify deals are filtered to allowed stages only."""

    def test_open_deals_included(self):
        """Deals with stage='Open' are included."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        deals = [{"id": "d1", "stage": "Open", "amount": 5000, "closing_date": None, "created_at": _days_ago(10)}]
        raw = _make_crm_raw(deals=deals)
        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: raw)
        record = assembler.hydrate_contact("real@example.com", "t1")

        assert len(record.deals) == 1
        assert record.deals[0]["stage"] == "Open"

    def test_negotiation_and_quote_sent_included(self):
        """Deals with Negotiation and Quote Sent stages are included."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        deals = [
            {"id": "d2", "stage": "Negotiation", "amount": 3000, "closing_date": None, "created_at": _days_ago(5)},
            {"id": "d3", "stage": "Quote Sent", "amount": 1500, "closing_date": None, "created_at": _days_ago(3)},
        ]
        raw = _make_crm_raw(deals=deals)
        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: raw)
        record = assembler.hydrate_contact("real@example.com", "t1")

        assert len(record.deals) == 2

    def test_closed_won_excluded(self):
        """Deals with stage 'Closed Won' are excluded."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        deals = [
            {"id": "d4", "stage": "Closed Won", "amount": 9000, "closing_date": None, "created_at": _days_ago(2)},
            {"id": "d5", "stage": "Closed Lost", "amount": 500, "closing_date": None, "created_at": _days_ago(2)},
        ]
        raw = _make_crm_raw(deals=deals)
        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: raw)
        record = assembler.hydrate_contact("real@example.com", "t1")

        assert len(record.deals) == 0


# ===========================================================================
# Gate 6 — Attachments filtering
# ===========================================================================

class TestAttachmentsFiltering:
    """Verify attachments are metadata-only and capped at 5."""

    def test_attachments_capped_at_5(self):
        """More than 5 attachments are trimmed to exactly 5."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        attachments = [
            {"id": f"a{i}", "filename": f"file{i}.pdf", "file_size": 1024, "mime_type": "application/pdf", "created_at": _days_ago(1), "content": "BINARY DATA"}
            for i in range(8)
        ]
        raw = _make_crm_raw(attachments=attachments)
        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: raw)
        record = assembler.hydrate_contact("real@example.com", "t1")

        assert len(record.attachments_metadata) == 5

    def test_attachment_content_excluded(self):
        """Attachment 'content' and 'data' fields are never included."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        attachments = [
            {
                "id": "a1",
                "filename": "quote.pdf",
                "file_size": 2048,
                "mime_type": "application/pdf",
                "created_at": _days_ago(2),
                "content": "RAW FILE CONTENT HERE",
                "data": "BASE64_ENCODED_DATA",
                "download_url": "https://secret.url/file",
            }
        ]
        raw = _make_crm_raw(attachments=attachments)
        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: raw)
        record = assembler.hydrate_contact("real@example.com", "t1")

        att = record.attachments_metadata[0]
        assert "content" not in att
        assert "data" not in att
        assert "download_url" not in att
        assert att["filename"] == "quote.pdf"


# ===========================================================================
# Gate 7 — RunCache behavior
# ===========================================================================

class TestRunCache:
    """Verify RunCache hit/miss behavior and per-run isolation."""

    def test_second_call_returns_cached_record(self):
        """Second call with same email returns the exact same HydratedContact object."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        raw = _make_crm_raw()
        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: raw)

        record1 = assembler.hydrate_contact("alice@example.com", "thread_001")
        record2 = assembler.hydrate_contact("alice@example.com", "thread_002")

        assert record1 is record2, "Cache hit must return the identical object"

    def test_different_emails_get_separate_records(self):
        """Two different email addresses produce two separate cache entries."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        call_count = 0

        def crm(email: str):
            nonlocal call_count
            call_count += 1
            return _make_crm_raw(contact_id=f"crm_{call_count}")

        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=crm)

        r1 = assembler.hydrate_contact("alice@example.com", "t1")
        r2 = assembler.hydrate_contact("bob@example.com", "t2")

        assert r1 is not r2
        assert assembler.cache_size == 2

    def test_clear_cache_resets_state(self):
        """clear_cache() forces a fresh CRM lookup on next call."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        call_count = 0

        def crm(email: str):
            nonlocal call_count
            call_count += 1
            return _make_crm_raw()

        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=crm)

        assembler.hydrate_contact("alice@example.com", "t1")
        assert call_count == 1

        assembler.clear_cache()
        assembler.hydrate_contact("alice@example.com", "t2")
        assert call_count == 2, "After clear_cache(), CRM must be called again"

    def test_case_insensitive_cache_hit(self):
        """Upper and lower case variants of same email hit the same cache entry."""
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        call_count = 0

        def crm(email: str):
            nonlocal call_count
            call_count += 1
            return _make_crm_raw()

        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=crm)

        assembler.hydrate_contact("Alice@Example.COM", "t1")
        assembler.hydrate_contact("alice@example.com", "t2")
        assembler.hydrate_contact("ALICE@EXAMPLE.COM", "t3")

        assert call_count == 1, "All case variants should hit the cache after first call"


# ===========================================================================
# VERIFICATION GATE
# ===========================================================================

class TestVerificationGate:
    """
    Phase 6 Verification Gate (spec Section 4 Phase 6):
    'hydrate_contact() returns identical result for same contact called twice
    (from cache second time), stub contacts contain email only, all other fields null.'
    """

    def test_cache_hit_mock_called_once(self):
        """
        VERIFICATION GATE Part 1:
        Call hydrate_contact() twice with same email.
        Mock CRM call count must be exactly 1 (cache serves second call).
        """
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        mock_crm = MagicMock(return_value=_make_crm_raw(contact_id="contact_gate"))
        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=mock_crm)

        result1 = assembler.hydrate_contact("gate_test@example.com", "thread_gate_1")
        result2 = assembler.hydrate_contact("gate_test@example.com", "thread_gate_2")

        assert result1 is result2, "Second call must return the exact same object"
        mock_crm.assert_called_once(), "CRM lookup must be called exactly once — second call served from cache"

    def test_stub_contains_only_required_fields(self):
        """
        VERIFICATION GATE Part 2:
        Stub contacts contain email only; id=None, is_stub=True,
        contact=None, notes=(), deals=(), attachments_metadata=().
        """
        from src.zdi_mw.assemblers.record_assembler import RecordAssembler

        ctx = _make_run_context()
        assembler = RecordAssembler(ctx, _crm_lookup_fn=lambda e: None)
        record = assembler.hydrate_contact("stubcheck@example.com", "thread_check")

        assert record.is_stub is True, "Stub must have is_stub=True"
        assert record.id is None, "Stub must have id=None"
        assert record.contact is None, "Stub must have contact=None"
        assert record.notes == (), "Stub must have empty notes tuple"
        assert record.deals == (), "Stub must have empty deals tuple"
        assert record.attachments_metadata == (), "Stub must have empty attachments_metadata tuple"
        assert record.email == "stubcheck@example.com", "Stub must preserve email"
        assert record.first_seen is not None, "Stub must have first_seen set to utc_now()"
        assert record.snapshot_at is not None, "Stub must have snapshot_at"
        assert record.run_id == ctx.run_id, "Stub must have correct run_id"
