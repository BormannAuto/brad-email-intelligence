# src/zdi_mw/loggers/accuracy_logger.py
# ZDI Middleware — AccuracyLogger
# Append-only structured log for every AI action.
# Format: one JSON object per line (NDJSON) in zoho_accuracy_log.json.
# Archive to CSV after 90 days. Never delete.
# Logging rule: IDs, counts, hashes, status codes, durations ONLY —
#   never email content, subjects, sender addresses, draft bodies, or names.

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_DEFAULT_LOG_PATH = Path("logs/zoho_accuracy_log.json")
_LOG_LOCK = threading.Lock()


class AccuracyLogger:
    """
    Append-only structured logger for AI pipeline actions.

    Each call to log_action() appends one JSON line to zoho_accuracy_log.json.

    Thread-safe via a module-level lock (all instances share the same file).
    Injectable log_path for testing.
    """

    def __init__(self, log_path: Optional[Path] = None) -> None:
        """
        Args:
            log_path: Path to the NDJSON log file. Defaults to logs/zoho_accuracy_log.json.
        """
        self.log_path = Path(log_path) if log_path else _DEFAULT_LOG_PATH
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def log_action(
        self,
        run_id: str,
        pipeline_version: str,
        brad_voice_version: int,
        prompt_name: str,
        prompt_version: str,
        prompt_fallback_level: int,
        model_version: str,
        source_input_hash: str,
        thread_id: str,
        outcome: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Append one accuracy log entry.

        Args:
            run_id: Run identifier, e.g. '2026-03-16-071500-UTC'.
            pipeline_version: Git ref, e.g. 'git:abc1234'.
            brad_voice_version: Integer voice profile version.
            prompt_name: Template name, e.g. 'draft_v1'.
            prompt_version: Template version string, e.g. '1.0'.
            prompt_fallback_level: 0=primary, 1=minor fallback, 2=hardcoded fallback.
            model_version: Claude model string, e.g. 'claude-sonnet-4-6'.
            source_input_hash: sha256 hex digest of sorted input dict.
            thread_id: Email thread ID being processed.
            outcome: Result string, e.g. 'SUCCESS', 'SKIPPED_CAP', 'VALIDATION_FAILED'.
            extra: Optional additional metadata (must follow logging rule — no PII).
        """
        entry: Dict[str, Any] = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "run_id": run_id,
            "pipeline_version": pipeline_version,
            "brad_voice_version": brad_voice_version,
            "prompt_name": prompt_name,
            "prompt_version": prompt_version,
            "prompt_fallback_level": prompt_fallback_level,
            "model_version": model_version,
            "source_input_hash": source_input_hash,
            "thread_id": thread_id,
            "outcome": outcome,
        }
        if extra:
            entry.update(extra)

        self._append(entry)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _append(self, entry: Dict[str, Any]) -> None:
        """Append entry as a single JSON line. Thread-safe."""
        line = json.dumps(entry, default=str) + "\n"
        with _LOG_LOCK:
            try:
                with open(self.log_path, "a", encoding="utf-8") as fh:
                    fh.write(line)
            except OSError as exc:
                logger.error(
                    "AccuracyLogger: failed to write entry run_id=%s exc=%s",
                    entry.get("run_id", "?"),
                    exc,
                )

    def read_all(self) -> list:
        """
        Read all entries from the log file. Returns list of dicts.
        Used for testing and audit tooling only.
        """
        if not self.log_path.exists():
            return []
        entries = []
        with open(self.log_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        entries.append(json.loads(line))
                    except json.JSONDecodeError:
                        logger.warning("AccuracyLogger: malformed log line skipped")
        return entries
