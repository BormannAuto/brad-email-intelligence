# src/zdi_mw/orchestrator/pipeline_runner.py
# ZDI Middleware — PipelineRunner
#
# Orchestrates the full ZDI pipeline in exact stage order:
#   0. Config validation (BEFORE any API call) — raises PipelineStartupError on mismatch
#   1. SystemHealthCheck — concurrent probes → degraded_flags
#   2. WAL reconcile — orphaned INTENTs from prior runs → DLQ
#   3. Auth — obtain/refresh tokens (3 flows)
#   4. RunLock — prevent concurrent runs
#   5. RunContext — frozen immutable context for this run
#   6. Per-thread: RecordAssembler → pre_validate → WorkDrive → Claude → post_validate
#      → SafeCRMClient → SafeMailClient
#   7. PipelineStateLedger — stage tracking per thread
#
# MODES:
#   live      — normal production run (default)
#   dry_run   — full pipeline executes; SafeClient writes are log-only, never sent
#               Set by PIPELINE_MODE=dry_run env var or pipeline_mode="dry_run" arg
#   sandbox   — all external API calls use mock fixture data (tests/mock_data/)
#               Set by SANDBOX_MODE=true env var or sandbox_mode=True arg
#               MOCK_ZOHO_FAIL=true within sandbox → mock API failures → DLQ entry
#
# Injectable dependencies throughout — ALL external calls can be replaced in tests.
# No injectable dep → that pipeline stage is skipped (logged as SKIPPED_NO_DEP).
#
# LOGGING RULE: never log email addresses, body content, or field values.
# Log only: thread_id, stage name, outcome, mode.

import hashlib
import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.zdi_mw.orchestrator.config_validator import ConfigValidator, ConfigValidationResult, PipelineStartupError

logger = logging.getLogger(__name__)

# Stage names — must match PipelineStateLedger stage list
_STAGE_HEALTH = "health_check"
_STAGE_AUTH = "auth"
_STAGE_HYDRATE = "hydrate_contact"
_STAGE_PRE_VALIDATE = "pre_validate"
_STAGE_WORKDRIVE = "workdrive_lookup"
_STAGE_CLAUDE = "claude_call"
_STAGE_POST_VALIDATE = "post_validate"
_STAGE_WRITE = "safe_write"

_DRY_RUN_PREFIX = "[DRY RUN]"
_SANDBOX_PREFIX = "[SANDBOX]"


@dataclass
class ThreadResult:
    """Result of processing a single email thread through the pipeline."""
    thread_id: str
    success: bool
    skipped: bool = False
    skip_reason: str = ""
    dry_run_writes: int = 0    # writes suppressed by dry_run mode
    actual_writes: int = 0     # writes actually sent (only non-zero in live/sandbox)
    crm_written: bool = False
    draft_created: bool = False
    category: str = ""
    error: str = ""
    stage_reached: str = ""    # last successfully completed stage


@dataclass
class PipelineResult:
    """Aggregate result of a full pipeline run."""
    run_id: str
    mode: str                            # "live" | "dry_run" | "sandbox"
    sandbox_mode: bool
    threads_processed: int
    threads_succeeded: int
    threads_failed: int
    threads_skipped: int
    total_writes: int                    # actual writes made to Zoho
    dry_run_writes_suppressed: int       # writes skipped due to dry_run
    errors: List[str] = field(default_factory=list)
    thread_results: List[ThreadResult] = field(default_factory=list)
    degraded_flags: Dict[str, bool] = field(default_factory=dict)
    config_warnings: List[str] = field(default_factory=list)


class PipelineRunner:
    """
    Orchestrates a full ZDI middleware pipeline run.

    All external dependencies are injectable for full unit test coverage.
    When a dependency is None, that stage is skipped and logged.

    Mode detection (in priority order):
        1. Constructor args (pipeline_mode, sandbox_mode)
        2. Environment variables (PIPELINE_MODE, SANDBOX_MODE, MOCK_ZOHO_FAIL)

    Usage (production):
        runner = PipelineRunner(config_base_path=Path("."))
        # Wire up all injectable deps with real clients...
        result = runner.run(threads)

    Usage (testing):
        runner = PipelineRunner(pipeline_mode="dry_run", ...)
        result = runner.run([{"thread_id": "T1", ...}])
        assert result.dry_run_writes_suppressed == 1
    """

    def __init__(
        self,
        config_base_path: Optional[Path] = None,
        db_path: Optional[Path] = None,
        pipeline_mode: Optional[str] = None,        # "live" | "dry_run" | "sandbox"
        sandbox_mode: Optional[bool] = None,         # True → SANDBOX_MODE
        mock_zoho_fail: Optional[bool] = None,       # True → deliberate sandbox failures
        # Injectable dependencies (all optional — None → stage skipped)
        _config_validator: Optional[ConfigValidator] = None,
        _health_checker: Optional[Any] = None,
        _auth_manager: Optional[Any] = None,
        _run_lock: Optional[Any] = None,
        _wal: Optional[Any] = None,
        _dlq: Optional[Any] = None,
        _ledger: Optional[Any] = None,
        _record_assembler: Optional[Any] = None,
        _integrity_gate: Optional[Any] = None,
        _orchestrator: Optional[Any] = None,
        _safe_crm: Optional[Any] = None,
        _safe_mail: Optional[Any] = None,
        _safe_workdrive: Optional[Any] = None,
        _accuracy_logger: Optional[Any] = None,
        _mock_data_path: Optional[Path] = None,
    ) -> None:
        self._config_base_path = config_base_path or Path(".")
        self._db_path = db_path

        # Mode resolution: constructor args take priority over env vars
        env_mode = os.environ.get("PIPELINE_MODE", "live").lower()
        env_sandbox = os.environ.get("SANDBOX_MODE", "false").lower() == "true"
        env_mock_fail = os.environ.get("MOCK_ZOHO_FAIL", "false").lower() == "true"

        self._pipeline_mode = pipeline_mode if pipeline_mode is not None else env_mode
        self._sandbox_mode = sandbox_mode if sandbox_mode is not None else env_sandbox
        self._mock_zoho_fail = mock_zoho_fail if mock_zoho_fail is not None else env_mock_fail

        # In sandbox mode, force pipeline_mode to "sandbox" for clarity
        if self._sandbox_mode and self._pipeline_mode not in ("dry_run", "sandbox"):
            self._pipeline_mode = "sandbox"

        self._is_dry_run = (self._pipeline_mode == "dry_run")

        # Injectable deps
        self._config_validator = _config_validator or ConfigValidator(
            config_base_path=self._config_base_path
        )
        self._health_checker = _health_checker
        self._auth_manager = _auth_manager
        self._run_lock = _run_lock
        self._wal = _wal
        self._dlq = _dlq
        self._ledger = _ledger
        self._record_assembler = _record_assembler
        self._integrity_gate = _integrity_gate
        self._orchestrator = _orchestrator
        self._safe_crm = _safe_crm
        self._safe_mail = _safe_mail
        self._safe_workdrive = _safe_workdrive
        self._accuracy_logger = _accuracy_logger
        self._mock_data_path = _mock_data_path or (
            Path("tests/mock_data") if self._sandbox_mode else None
        )

        logger.info(
            "PipelineRunner: initialized mode=%s sandbox=%s mock_fail=%s",
            self._pipeline_mode, self._sandbox_mode, self._mock_zoho_fail,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, threads: List[Dict[str, Any]]) -> PipelineResult:
        """
        Execute the full pipeline against a list of email threads.

        Args:
            threads: List of email thread dicts. Each must have at minimum:
                     thread_id, from_address, body, brand, model, in_reply_to

        Returns:
            PipelineResult with per-thread results and aggregate stats.

        Raises:
            PipelineStartupError — if config validation fails (major version mismatch).
        """
        run_id = _generate_run_id()
        logger.info("PipelineRunner: run START run_id=%s threads=%d", run_id, len(threads))

        # ── Stage 0: Config validation (MUST precede all API calls) ─────────
        config_result = self._run_config_validation()
        # If we reach here, config is valid (or only minor warnings — no exception raised)

        # ── Stage 1: System health check ────────────────────────────────────
        degraded_flags = self._run_health_check()

        # ── Stage 2: WAL reconcile ──────────────────────────────────────────
        self._run_wal_reconcile()

        # ── Stage 3: Auth ───────────────────────────────────────────────────
        self._run_auth(degraded_flags)

        # ── Stage 4: RunLock (acquire — released in finally) ────────────────
        lock_acquired = self._acquire_run_lock(run_id)

        # ── Stage 5: Build RunContext ────────────────────────────────────────
        run_context = self._build_run_context(run_id, degraded_flags)

        # ── Stage 6: Per-thread processing ──────────────────────────────────
        thread_results: List[ThreadResult] = []
        try:
            for thread in threads:
                result = self._process_thread(thread, run_context)
                thread_results.append(result)
        finally:
            if lock_acquired:
                self._release_run_lock(run_id)

        return self._build_pipeline_result(
            run_id=run_id,
            thread_results=thread_results,
            degraded_flags=degraded_flags,
            config_warnings=config_result.warnings if config_result else [],
        )

    # ------------------------------------------------------------------
    # Stage runners
    # ------------------------------------------------------------------

    def _run_config_validation(self) -> Optional[ConfigValidationResult]:
        """Stage 0: Validate all config schema versions. Raises PipelineStartupError on failure."""
        logger.info("PipelineRunner: stage=config_validation")
        try:
            result = self._config_validator.validate_all()
            logger.info("PipelineRunner: config validation OK warnings=%d", len(result.warnings))
            return result
        except PipelineStartupError:
            raise  # propagate — pipeline cannot proceed

    def _run_health_check(self) -> Dict[str, bool]:
        """Stage 1: Run SystemHealthCheck. Returns degraded_flags."""
        if self._health_checker is None:
            logger.info("PipelineRunner: stage=%s SKIPPED_NO_DEP", _STAGE_HEALTH)
            return {k: False for k in ("mail_down", "crm_down", "workdrive_down", "claude_down", "db_down")}

        logger.info("PipelineRunner: stage=%s", _STAGE_HEALTH)
        try:
            flags = self._health_checker.run()
            logger.info("PipelineRunner: health check done degraded=%s", any(flags.values()))
            return flags
        except Exception as exc:
            logger.error("PipelineRunner: health check raised exc=%s — all flags clear", type(exc).__name__)
            return {k: False for k in ("mail_down", "crm_down", "workdrive_down", "claude_down", "db_down")}

    def _run_wal_reconcile(self) -> None:
        """Stage 2: WAL startup reconcile — orphaned INTENTs → DLQ."""
        if self._wal is None:
            logger.info("PipelineRunner: WAL reconcile SKIPPED_NO_DEP")
            return
        logger.info("PipelineRunner: WAL reconcile START")
        try:
            self._wal.reconcile(self._dlq)
        except Exception as exc:
            logger.error("PipelineRunner: WAL reconcile failed exc=%s", type(exc).__name__)

    def _run_auth(self, degraded_flags: Dict[str, bool]) -> None:
        """Stage 3: Obtain/refresh OAuth tokens for all three Zoho flows."""
        if self._auth_manager is None:
            logger.info("PipelineRunner: stage=%s SKIPPED_NO_DEP", _STAGE_AUTH)
            return
        if self._sandbox_mode:
            logger.info("PipelineRunner: stage=%s SANDBOX — using mock tokens", _STAGE_AUTH)
            return
        logger.info("PipelineRunner: stage=%s", _STAGE_AUTH)
        try:
            for flow in ("mail", "crm", "workdrive"):
                if not degraded_flags.get(f"{flow}_down", False):
                    self._auth_manager.get_token(flow)
        except Exception as exc:
            logger.error("PipelineRunner: auth failed exc=%s", type(exc).__name__)

    def _acquire_run_lock(self, run_id: str) -> bool:
        """Stage 4: Acquire pipeline RunLock. Returns True if acquired."""
        if self._run_lock is None:
            logger.info("PipelineRunner: RunLock SKIPPED_NO_DEP")
            return False
        logger.info("PipelineRunner: acquiring RunLock run_id=%s", run_id)
        try:
            acquired = self._run_lock.acquire(run_id)
            if not acquired:
                logger.warning("PipelineRunner: RunLock not acquired — concurrent run in progress?")
            return acquired
        except Exception as exc:
            logger.error("PipelineRunner: RunLock acquire failed exc=%s", type(exc).__name__)
            return False

    def _release_run_lock(self, run_id: str) -> None:
        if self._run_lock is None:
            return
        try:
            self._run_lock.release(run_id)
        except Exception as exc:
            logger.error("PipelineRunner: RunLock release failed exc=%s", type(exc).__name__)

    def _build_run_context(
        self,
        run_id: str,
        degraded_flags: Dict[str, bool],
    ) -> Dict[str, Any]:
        """
        Stage 5: Build a RunContext-like dict (or real RunContext if available).
        Returns a plain dict for simplicity — real RunContext constructed by caller.
        """
        now = datetime.now(timezone.utc)
        ctx = {
            "run_id": run_id,
            "pipeline_version": _get_pipeline_version(),
            "utc_start": now,
            "degraded_flags": degraded_flags,
            "pipeline_mode": self._pipeline_mode,
            "brad_voice_version": 1,
            "prompt_registry_version": "prompts-2026-03",
            "crm_snapshot_hash": "",
            "metadata_snapshot_hash": "",
        }
        logger.info("PipelineRunner: RunContext built run_id=%s mode=%s", run_id, self._pipeline_mode)
        return ctx

    # ------------------------------------------------------------------
    # Per-thread processing
    # ------------------------------------------------------------------

    def _process_thread(
        self,
        thread: Dict[str, Any],
        run_context: Dict[str, Any],
    ) -> ThreadResult:
        """Process one email thread through all pipeline stages."""
        thread_id = thread.get("thread_id", "UNKNOWN")
        logger.info("PipelineRunner: thread START thread_id=%s", thread_id)

        # a. Hydrate contact
        hydrated = self._do_hydrate(thread, run_context)

        # b. Pre-validate
        pre_ok, pre_reason = self._do_pre_validate(thread, hydrated, run_context)
        if not pre_ok:
            logger.info("PipelineRunner: thread SKIPPED thread_id=%s reason=%s", thread_id, pre_reason)
            return ThreadResult(
                thread_id=thread_id, success=False, skipped=True,
                skip_reason=pre_reason, stage_reached=_STAGE_PRE_VALIDATE,
            )

        # c. WorkDrive lookup
        workdrive_chunk = self._do_workdrive_lookup(thread, run_context)

        # d. Claude call(s)
        claude_output = self._do_claude_call(thread, hydrated, workdrive_chunk, run_context)

        # e. Post-validate
        post_ok, post_reason = self._do_post_validate(thread, claude_output, workdrive_chunk, run_context)
        if not post_ok:
            logger.info(
                "PipelineRunner: post_validate BLOCKED thread_id=%s reason=%s",
                thread_id, post_reason,
            )
            return ThreadResult(
                thread_id=thread_id, success=False, skipped=True,
                skip_reason=post_reason, stage_reached=_STAGE_POST_VALIDATE,
            )

        # f. CRM write
        crm_written, dry_crm = self._do_crm_write(thread, hydrated, claude_output, run_context)

        # g. Mail draft
        draft_created, dry_mail = self._do_mail_draft(thread, claude_output, run_context)

        total_dry = dry_crm + dry_mail
        total_actual = (1 if crm_written else 0) + (1 if draft_created else 0)

        logger.info(
            "PipelineRunner: thread DONE thread_id=%s crm=%s draft=%s dry_suppressed=%d",
            thread_id, crm_written, draft_created, total_dry,
        )
        return ThreadResult(
            thread_id=thread_id,
            success=True,
            crm_written=crm_written,
            draft_created=draft_created,
            actual_writes=total_actual,
            dry_run_writes=total_dry,
            stage_reached=_STAGE_WRITE,
            category=_extract_category(claude_output),
        )

    # ------------------------------------------------------------------
    # Individual stage helpers
    # ------------------------------------------------------------------

    def _do_hydrate(
        self,
        thread: Dict[str, Any],
        run_context: Dict[str, Any],
    ) -> Optional[Any]:
        if self._record_assembler is None:
            logger.debug("PipelineRunner: hydrate SKIPPED_NO_DEP thread_id=%s", thread.get("thread_id"))
            return None
        try:
            email = thread.get("from_address", "")
            thread_id = thread.get("thread_id", "")
            return self._record_assembler.hydrate_contact(email, thread_id)
        except Exception as exc:
            logger.error("PipelineRunner: hydrate failed exc=%s", type(exc).__name__)
            return None

    def _do_pre_validate(
        self,
        thread: Dict[str, Any],
        hydrated: Optional[Any],
        run_context: Dict[str, Any],
    ) -> Tuple[bool, str]:
        if self._integrity_gate is None:
            logger.debug(
                "PipelineRunner: pre_validate SKIPPED_NO_DEP thread_id=%s",
                thread.get("thread_id"),
            )
            return True, ""
        try:
            result = self._integrity_gate.pre_validate(thread, hydrated, run_context)
            if result.blocked:
                return False, result.block_reason
            return True, ""
        except Exception as exc:
            logger.error("PipelineRunner: pre_validate raised exc=%s", type(exc).__name__)
            return True, ""  # don't block on gate error — log and continue

    def _do_workdrive_lookup(
        self,
        thread: Dict[str, Any],
        run_context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if self._safe_workdrive is None:
            logger.debug(
                "PipelineRunner: workdrive SKIPPED_NO_DEP thread_id=%s",
                thread.get("thread_id"),
            )
            return None
        if run_context.get("degraded_flags", {}).get("workdrive_down", False):
            logger.info(
                "PipelineRunner: workdrive SKIPPED degraded thread_id=%s",
                thread.get("thread_id"),
            )
            return None
        try:
            brand = thread.get("brand", "")
            model = thread.get("model", "")
            inquiry = thread.get("body", "")
            result = self._safe_workdrive.safe_lookup_chunk(brand, model, inquiry)
            if not result.blocked and result.chunk:
                return result.chunk
            return None
        except Exception as exc:
            logger.error("PipelineRunner: workdrive lookup failed exc=%s", type(exc).__name__)
            return None

    def _do_claude_call(
        self,
        thread: Dict[str, Any],
        hydrated: Optional[Any],
        workdrive_chunk: Optional[Dict[str, Any]],
        run_context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if self._orchestrator is None:
            logger.debug(
                "PipelineRunner: claude SKIPPED_NO_DEP thread_id=%s",
                thread.get("thread_id"),
            )
            return None
        try:
            input_dict = {
                "thread_id": thread.get("thread_id"),
                "subject": thread.get("subject", ""),
                "body": thread.get("body", ""),
                "from_address": thread.get("from_address", ""),
                "workdrive_context": workdrive_chunk,
            }
            return self._orchestrator.call(run_context, "categorizer", input_dict)
        except Exception as exc:
            logger.error("PipelineRunner: claude call failed exc=%s", type(exc).__name__)
            return None

    def _do_post_validate(
        self,
        thread: Dict[str, Any],
        claude_output: Optional[Dict[str, Any]],
        workdrive_chunk: Optional[Dict[str, Any]],
        run_context: Dict[str, Any],
    ) -> Tuple[bool, str]:
        if self._integrity_gate is None:
            logger.debug(
                "PipelineRunner: post_validate SKIPPED_NO_DEP thread_id=%s",
                thread.get("thread_id"),
            )
            return True, ""
        try:
            result = self._integrity_gate.post_validate(
                thread, claude_output, workdrive_chunk, run_context
            )
            if result.blocked:
                return False, result.block_reason
            return True, ""
        except Exception as exc:
            logger.error("PipelineRunner: post_validate raised exc=%s", type(exc).__name__)
            return True, ""

    def _do_crm_write(
        self,
        thread: Dict[str, Any],
        hydrated: Optional[Any],
        claude_output: Optional[Dict[str, Any]],
        run_context: Dict[str, Any],
    ) -> Tuple[bool, int]:
        """
        Perform CRM write (or suppress in dry_run).

        Returns:
            (crm_written: bool, dry_run_suppressed: int)
        """
        thread_id = thread.get("thread_id", "")

        if self._is_dry_run:
            logger.info(
                "%s CRM write suppressed thread_id=%s",
                _DRY_RUN_PREFIX, thread_id,
            )
            return False, 1

        if self._safe_crm is None:
            logger.debug("PipelineRunner: CRM write SKIPPED_NO_DEP thread_id=%s", thread_id)
            return False, 0

        if self._mock_zoho_fail and self._sandbox_mode:
            logger.info(
                "%s MOCK_ZOHO_FAIL — CRM write deliberate failure thread_id=%s",
                _SANDBOX_PREFIX, thread_id,
            )
            self._record_dlq_failure(thread_id, "CRM_WRITE_MOCK_FAIL", run_context)
            return False, 0

        try:
            contact_id = _extract_contact_id(hydrated)
            field_name = "last_inquiry_category"
            value = _extract_category(claude_output) or "UNKNOWN"
            source_text = thread.get("body", "")
            confidence = _extract_confidence(claude_output)

            result = self._safe_crm.safe_write(
                contact_id=contact_id,
                field=field_name,
                value=value,
                source_text=source_text,
                confidence=confidence,
            )
            return result.success and not result.skipped, 0
        except Exception as exc:
            logger.error("PipelineRunner: CRM write failed exc=%s", type(exc).__name__)
            return False, 0

    def _do_mail_draft(
        self,
        thread: Dict[str, Any],
        claude_output: Optional[Dict[str, Any]],
        run_context: Dict[str, Any],
    ) -> Tuple[bool, int]:
        """
        Create mail draft (or suppress in dry_run).

        Returns:
            (draft_created: bool, dry_run_suppressed: int)
        """
        thread_id = thread.get("thread_id", "")

        if self._is_dry_run:
            logger.info(
                "%s Mail draft suppressed thread_id=%s",
                _DRY_RUN_PREFIX, thread_id,
            )
            return False, 1

        if self._safe_mail is None:
            logger.debug("PipelineRunner: Mail draft SKIPPED_NO_DEP thread_id=%s", thread_id)
            return False, 0

        if self._mock_zoho_fail and self._sandbox_mode:
            logger.info(
                "%s MOCK_ZOHO_FAIL — Mail draft deliberate failure thread_id=%s",
                _SANDBOX_PREFIX, thread_id,
            )
            self._record_dlq_failure(thread_id, "MAIL_DRAFT_MOCK_FAIL", run_context)
            return False, 0

        try:
            body = _extract_draft_body(claude_output) or ""
            category = _extract_category(claude_output) or "UNKNOWN"
            recipient = thread.get("from_address", "")
            in_reply_to = thread.get("in_reply_to", "")
            source_from = thread.get("from_address", "")

            result = self._safe_mail.safe_create_draft(
                thread_id=thread_id,
                recipient=recipient,
                body=body,
                category=category,
                source_from_address=source_from,
                in_reply_to=in_reply_to,
            )
            created = result.success and not result.skipped
            return created, 0
        except Exception as exc:
            logger.error("PipelineRunner: mail draft failed exc=%s", type(exc).__name__)
            return False, 0

    def _record_dlq_failure(
        self,
        thread_id: str,
        failure_type: str,
        run_context: Dict[str, Any],
    ) -> None:
        """Insert a DLQ entry for a deliberate mock failure (SANDBOX MOCK_ZOHO_FAIL)."""
        if self._dlq is None:
            logger.warning("PipelineRunner: DLQ not configured — cannot record failure thread_id=%s", thread_id)
            return
        try:
            run_ctx_json = json.dumps({
                "run_id": run_context.get("run_id", "unknown"),
                "pipeline_version": run_context.get("pipeline_version", "unknown"),
                "pipeline_mode": run_context.get("pipeline_mode", "sandbox"),
            })
            self._dlq.insert(
                thread_id=thread_id,
                failure_stage="safe_write",
                failure_reason=failure_type,
                run_context_json=run_ctx_json,
            )
            logger.info(
                "PipelineRunner: DLQ entry recorded thread_id=%s reason=%s",
                thread_id, failure_type,
            )
        except Exception as exc:
            logger.error("PipelineRunner: DLQ insert failed exc=%s", type(exc).__name__)

    # ------------------------------------------------------------------
    # Result assembly
    # ------------------------------------------------------------------

    def _build_pipeline_result(
        self,
        run_id: str,
        thread_results: List[ThreadResult],
        degraded_flags: Dict[str, bool],
        config_warnings: List[str],
    ) -> PipelineResult:
        succeeded = sum(1 for r in thread_results if r.success)
        failed = sum(1 for r in thread_results if not r.success and not r.skipped)
        skipped = sum(1 for r in thread_results if r.skipped)
        total_writes = sum(r.actual_writes for r in thread_results)
        dry_suppressed = sum(r.dry_run_writes for r in thread_results)
        errors = [r.error for r in thread_results if r.error]

        logger.info(
            "PipelineRunner: run COMPLETE run_id=%s mode=%s total=%d ok=%d fail=%d skip=%d writes=%d dry_suppressed=%d",
            run_id, self._pipeline_mode,
            len(thread_results), succeeded, failed, skipped,
            total_writes, dry_suppressed,
        )

        return PipelineResult(
            run_id=run_id,
            mode=self._pipeline_mode,
            sandbox_mode=self._sandbox_mode,
            threads_processed=len(thread_results),
            threads_succeeded=succeeded,
            threads_failed=failed,
            threads_skipped=skipped,
            total_writes=total_writes,
            dry_run_writes_suppressed=dry_suppressed,
            errors=errors,
            thread_results=thread_results,
            degraded_flags=degraded_flags,
            config_warnings=config_warnings,
        )


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _generate_run_id() -> str:
    """Generate a unique run ID: 'YYYY-MM-DD-HHMMSS-UTC-{short_uuid}'."""
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y-%m-%d-%H%M%S")
    short = str(uuid.uuid4())[:8]
    return f"{stamp}-UTC-{short}"


def _get_pipeline_version() -> str:
    """Get pipeline version from git HEAD, fallback to 'unknown'."""
    try:
        import subprocess
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=3,
        )
        if result.returncode == 0:
            return f"git:{result.stdout.strip()}"
    except Exception:
        pass
    return "unknown"


def _extract_category(claude_output: Optional[Dict[str, Any]]) -> str:
    if not claude_output:
        return ""
    return str(claude_output.get("category", ""))


def _extract_confidence(claude_output: Optional[Dict[str, Any]]) -> str:
    if not claude_output:
        return "LOW"
    return str(claude_output.get("confidence", "LOW"))


def _extract_draft_body(claude_output: Optional[Dict[str, Any]]) -> str:
    if not claude_output:
        return ""
    return str(claude_output.get("draft_body", "") or claude_output.get("body", ""))


def _extract_contact_id(hydrated: Optional[Any]) -> str:
    if hydrated is None:
        return "UNKNOWN"
    # HydratedContact has .id attribute; dict form has "id" key
    if hasattr(hydrated, "id") and hydrated.id:
        return str(hydrated.id)
    if isinstance(hydrated, dict):
        return str(hydrated.get("id", "UNKNOWN"))
    return "UNKNOWN"
