# src/zdi_mw/core/run_context.py
# ZDI Middleware — RunContext
# Instantiated ONCE at pipeline startup. Passed as first argument to every
# method that makes an external call. Immutable after creation.
# NO data created during a run may be used as context input for that same run.

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Tuple


@dataclass(frozen=True)
class RunContext:
    """
    Immutable context object for a single pipeline run.

    Instantiated once at startup by pipeline_runner.py and passed to every
    component that makes an external call. Prevents cross-run contamination
    and feedback loops by establishing a hard temporal boundary (utc_start).

    Fields added per Section 10.1: metadata_snapshot_hash captures the Zoho
    CRM field metadata state at run start — drift between runs triggers a
    SCHEMA_DRIFT_DETECTED warning.
    """

    run_id: str
    """Format: 'YYYY-MM-DD-HHMMSS-UTC' — generated at pipeline startup."""

    pipeline_version: str
    """'git:abc1234' — from subprocess git rev-parse HEAD."""

    utc_start: datetime
    """UTC timestamp when this run was initialised. Used as the temporal fence:
    all input data must predate this timestamp."""

    crm_snapshot_hash: str
    """sha256 of all hydrated contact records assembled during this run."""

    email_fetch_window: Tuple[datetime, datetime]
    """(start, end) UTC window for email fetching."""

    brad_voice_version: int
    """Version integer from brad_voice_profile.json 'version' field."""

    prompt_registry_version: str
    """e.g. 'prompts-2026-03' — slug identifying the active prompt set."""

    pipeline_mode: str
    """'live' | 'dry_run' | 'sandbox' | 'human_promoted'"""

    degraded_flags: Dict[str, bool]
    """{'crm_down': False, 'mail_down': False, 'workdrive_down': False,
    'claude_down': False, 'db_down': False} — set by SystemHealthCheck."""

    metadata_snapshot_hash: str = ""
    """sha256 of Zoho CRM field metadata fetched at startup (Section 10.1).
    Populated by SafeCRMClient after the Metadata API call. Empty string
    if CRM is degraded. Compared between runs to detect SCHEMA_DRIFT."""

    def is_degraded(self) -> bool:
        """Return True if any dependency is marked down."""
        return any(self.degraded_flags.values())

    def to_log_dict(self) -> dict:
        """Return a JSON-serialisable dict suitable for accuracy_log entries.

        Deliberately excludes email_fetch_window datetimes (not needed in
        per-action log entries) and metadata_snapshot_hash (logged separately
        at run start only).
        """
        return {
            "run_id": self.run_id,
            "pipeline_version": self.pipeline_version,
            "utc_start": self.utc_start.isoformat(),
            "pipeline_mode": self.pipeline_mode,
            "degraded_flags": self.degraded_flags,
            "brad_voice_version": self.brad_voice_version,
            "prompt_registry_version": self.prompt_registry_version,
        }
