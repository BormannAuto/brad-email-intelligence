# src/zdi_mw/assemblers/record_assembler.py
# ZDI Middleware — RecordAssembler + RunCache
#
# hydrate_contact(email, thread_id, run_context) returns a frozen HydratedContact.
# RunCache: {contact_id: HydratedContact} dict within the RecordAssembler instance.
#   - Cache hit skips ALL CRM API calls.
#   - Cache is per-run only — never persists between runs.
#   - Keyed by contact_id (real contacts) or normalised email hash (stubs).
#
# Stub logic: if no CRM match found:
#   HydratedContact(email=email, id=None, is_stub=True, first_seen=utc_now(),
#                   contact=None, notes=(), deals=(), attachments_metadata=(),
#                   snapshot_at=utc_now(), run_id=run_id)
#   Nothing inferred. No field values beyond what is listed above. Ever.
#
# Frozen contract: HydratedContact is dataclass(frozen=True). All collection fields
# are tuples (immutable). The nested contact dict is wrapped in MappingProxyType.
# Pipeline may read, never mutate.
#
# Injectable dependency: _crm_lookup_fn for unit testing without real API.
#
# LOGGING RULE: never log email addresses, contact names, or any business data.
# Log only: cache key prefixes (first 8 hex chars), thread_id, hit/miss booleans.

import hashlib
import logging
import types
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# Allowed deal stages for inclusion in hydrated record (spec §Phase 6)
_ALLOWED_DEAL_STAGES = {"Open", "Negotiation", "Quote Sent"}

# Limits per spec
_MAX_NOTES = 10
_NOTES_WINDOW_DAYS = 30
_MAX_ATTACHMENTS = 5


# ---------------------------------------------------------------------------
# Frozen return type
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class HydratedContact:
    """
    Immutable snapshot of a CRM contact assembled for a single pipeline run.

    Fields:
        email:               Normalised input email address used for lookup.
        id:                  CRM contact ID, or None if stub.
        is_stub:             True when no CRM match found.
        first_seen:          UTC datetime (stub: time of assembly; real: CRM created_at).
        contact:             MappingProxyType of schema-validated CRM contact fields,
                             or None if stub.
        notes:               Tuple of note metadata dicts (last 30 days, max 10).
        deals:               Tuple of deal dicts (Open/Negotiation/Quote Sent only).
        attachments_metadata: Tuple of attachment metadata dicts (max 5, no content).
        snapshot_at:         UTC timestamp when this record was assembled.
        run_id:              run_id from RunContext.
    """

    email: str
    id: Optional[str]
    is_stub: bool
    first_seen: Optional[datetime]

    # CRM data — None for stub contacts; populated from CRM for real contacts
    contact: Optional[types.MappingProxyType]  # type: ignore[type-arg]
    notes: Tuple[Any, ...]
    deals: Tuple[Any, ...]
    attachments_metadata: Tuple[Any, ...]

    # Run metadata
    snapshot_at: datetime
    run_id: str


# ---------------------------------------------------------------------------
# RecordAssembler
# ---------------------------------------------------------------------------

class RecordAssembler:
    """
    Assembles a frozen HydratedContact for each email address encountered in a run.

    RunCache: {cache_key: HydratedContact}
        - Real contacts: keyed by contact_id
        - Stub contacts: keyed by sha256(normalised_email)
        - Populated on first hydrate_contact() call per email; subsequent calls
          return the cached result without any CRM API calls.
        - Never persists between runs (instance-level dict, reset on new instance).

    Injectable dependency:
        _crm_lookup_fn: Callable[[str], Optional[dict]]
            Takes normalised email address, returns raw CRM response dict or None.
            Expected dict keys: 'id', 'created_at', 'contact_fields',
            'notes', 'deals', 'attachments'.
            If None, every lookup produces a stub (used when SafeClients not yet wired).

    Usage:
        assembler = RecordAssembler(run_context, _crm_lookup_fn=mock_crm_fn)
        record = assembler.hydrate_contact("customer@example.com", "thread_123")
        # record is frozen — pipeline may read, never mutate
    """

    def __init__(
        self,
        run_context: Any,
        _crm_lookup_fn: Optional[Callable[[str], Optional[Dict[str, Any]]]] = None,
    ) -> None:
        """
        Args:
            run_context:       RunContext for this pipeline run.
            _crm_lookup_fn:    Injectable CRM lookup callable for testing.
                               Signature: (email: str) -> Optional[dict]
        """
        self._run_context = run_context
        self._crm_lookup_fn = _crm_lookup_fn
        # RunCache — cleared on new instance, never persisted to disk
        self._cache: Dict[str, HydratedContact] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def hydrate_contact(
        self,
        email: str,
        thread_id: str,
    ) -> HydratedContact:
        """
        Return a frozen HydratedContact for the given email address.

        Cache hit:  return cached record immediately — zero CRM API calls.
        Cache miss: perform CRM lookup, assemble HydratedContact, store in cache.

        Args:
            email:      Sender/recipient email address for CRM lookup.
            thread_id:  Email thread ID (for debug logging only — not stored).

        Returns:
            HydratedContact (frozen dataclass, immutable).
        """
        normalised_email = email.strip().lower()
        email_key = _email_hash(normalised_email)

        # Check by email hash first (covers both stubs and real contacts)
        if email_key in self._cache:
            logger.debug(
                "RecordAssembler: cache HIT key_prefix=%s thread_id=%s",
                email_key[:8],
                thread_id,
            )
            return self._cache[email_key]

        # Cache miss — perform CRM lookup
        logger.debug(
            "RecordAssembler: cache MISS key_prefix=%s thread_id=%s",
            email_key[:8],
            thread_id,
        )
        record = self._build_record(normalised_email, thread_id)

        # Store by email hash (always) and by contact_id (real contacts)
        self._cache[email_key] = record
        if record.id:
            self._cache[record.id] = record

        return record

    @property
    def cache_size(self) -> int:
        """Number of unique HydratedContact objects in the RunCache."""
        return len({id(v) for v in self._cache.values()})

    def clear_cache(self) -> None:
        """Clear the RunCache. Only call between runs — never mid-run."""
        self._cache.clear()

    # ------------------------------------------------------------------
    # Internal: record construction
    # ------------------------------------------------------------------

    def _build_record(self, normalised_email: str, thread_id: str) -> HydratedContact:
        """Perform CRM lookup and build the frozen HydratedContact."""
        now_utc = datetime.now(timezone.utc)
        run_id = self._run_context.run_id

        if self._crm_lookup_fn is None:
            logger.warning(
                "RecordAssembler: no CRM lookup function configured — "
                "returning stub for thread_id=%s",
                thread_id,
            )
            return _make_stub(normalised_email, now_utc, run_id)

        try:
            raw = self._crm_lookup_fn(normalised_email)
        except Exception as exc:
            logger.error(
                "RecordAssembler: CRM lookup raised thread_id=%s exc_type=%s",
                thread_id,
                type(exc).__name__,
            )
            return _make_stub(normalised_email, now_utc, run_id)

        if raw is None:
            logger.info(
                "RecordAssembler: no CRM match — stub for thread_id=%s",
                thread_id,
            )
            return _make_stub(normalised_email, now_utc, run_id)

        return _make_real(normalised_email, raw, now_utc, run_id)


# ---------------------------------------------------------------------------
# Module-level builder functions (keep class lean, easier to unit-test)
# ---------------------------------------------------------------------------

def _make_stub(normalised_email: str, now_utc: datetime, run_id: str) -> HydratedContact:
    """
    Build a stub HydratedContact when no CRM match is found.

    Per spec §Phase 6 stub logic:
    Return ONLY: email, id=None, is_stub=True, first_seen=utc_now().
    Nothing else. No inferred fields. Ever.
    """
    return HydratedContact(
        email=normalised_email,
        id=None,
        is_stub=True,
        first_seen=now_utc,
        contact=None,
        notes=(),
        deals=(),
        attachments_metadata=(),
        snapshot_at=now_utc,
        run_id=run_id,
    )


def _make_real(
    normalised_email: str,
    raw: Dict[str, Any],
    now_utc: datetime,
    run_id: str,
) -> HydratedContact:
    """
    Build a real HydratedContact from raw CRM API data.

    Applies all spec filters:
    - notes: last _NOTES_WINDOW_DAYS days only, max _MAX_NOTES entries
    - deals: _ALLOWED_DEAL_STAGES only
    - attachments_metadata: max _MAX_ATTACHMENTS, metadata fields only (never content)
    """
    contact_id = str(raw.get("id", "")).strip() or None

    first_seen = _parse_datetime(raw.get("created_at"), now_utc)

    raw_contact_fields = raw.get("contact_fields", {})
    contact_proxy = types.MappingProxyType(
        dict(raw_contact_fields) if isinstance(raw_contact_fields, dict) else {}
    )

    notes = _filter_notes(raw.get("notes", []), now_utc)
    deals = _filter_deals(raw.get("deals", []))
    attachments_metadata = _filter_attachments(raw.get("attachments", []))

    return HydratedContact(
        email=normalised_email,
        id=contact_id,
        is_stub=False,
        first_seen=first_seen,
        contact=contact_proxy,
        notes=notes,
        deals=deals,
        attachments_metadata=attachments_metadata,
        snapshot_at=now_utc,
        run_id=run_id,
    )


def _filter_notes(raw_notes: list, now_utc: datetime) -> Tuple[Any, ...]:
    """
    Filter notes: last _NOTES_WINDOW_DAYS days, max _MAX_NOTES entries.
    Metadata only — note content deliberately excluded.
    """
    cutoff = now_utc - timedelta(days=_NOTES_WINDOW_DAYS)
    filtered = []
    for note in raw_notes:
        if not isinstance(note, dict):
            continue
        note_dt = _parse_datetime(note.get("created_at"), None)
        if note_dt is None or note_dt < cutoff:
            continue
        filtered.append({
            "id": note.get("id"),
            "created_at": note.get("created_at"),
            "note_type": note.get("note_type"),
            # 'content' deliberately excluded — pipeline stages fetch content
            # via SafeClients at call time if needed
        })
        if len(filtered) >= _MAX_NOTES:
            break
    return tuple(filtered)


def _filter_deals(raw_deals: list) -> Tuple[Any, ...]:
    """Filter deals to allowed stages: Open, Negotiation, Quote Sent."""
    filtered = []
    for deal in raw_deals:
        if not isinstance(deal, dict):
            continue
        if deal.get("stage") not in _ALLOWED_DEAL_STAGES:
            continue
        filtered.append({
            "id": deal.get("id"),
            "stage": deal.get("stage"),
            "amount": deal.get("amount"),
            "closing_date": deal.get("closing_date"),
            "created_at": deal.get("created_at"),
        })
    return tuple(filtered)


def _filter_attachments(raw_attachments: list) -> Tuple[Any, ...]:
    """
    Filter attachments to metadata only, max _MAX_ATTACHMENTS.
    NEVER includes attachment content, data, or download URLs.
    """
    filtered = []
    for att in raw_attachments:
        if not isinstance(att, dict):
            continue
        filtered.append({
            "id": att.get("id"),
            "filename": att.get("filename"),
            "file_size": att.get("file_size"),
            "mime_type": att.get("mime_type"),
            "created_at": att.get("created_at"),
            # 'content', 'data', 'download_url' deliberately excluded
        })
        if len(filtered) >= _MAX_ATTACHMENTS:
            break
    return tuple(filtered)


def _email_hash(normalised_email: str) -> str:
    """sha256 of the normalised email — never store raw email as cache key."""
    return hashlib.sha256(normalised_email.encode("utf-8")).hexdigest()


def _parse_datetime(value: Any, fallback: Optional[datetime]) -> Optional[datetime]:
    """
    Parse an ISO datetime string or datetime object.
    Returns fallback if value is None, unparseable, or wrong type.
    """
    if value is None:
        return fallback
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return fallback
    return fallback
