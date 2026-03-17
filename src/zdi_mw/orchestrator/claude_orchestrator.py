# src/zdi_mw/orchestrator/claude_orchestrator.py
# ZDI Middleware — ClaudeOrchestrator
# Structured Outputs enforced via tool_use (constrained decoding).
# Every call: max_tokens=1000, source_input_hash logged, compressed input stored.
# Global 50-call cap per run — log warning and skip AI on overflow (do not abort).
#
# Section 10.5 compliance: uses tool_choice={'type':'tool','name':'output'} pattern
# from current Anthropic SDK (GA). Pydantic models → model_json_schema() → tools param.
#
# LOGGING RULE: Never log email content, names, subjects, or draft bodies.
# Log only: thread_id, run_id, hashes, counts, status codes, durations.

import gzip
import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, TypeVar

from pydantic import BaseModel

logger = logging.getLogger(__name__)

_DEFAULT_LOGS_INPUTS_DIR = Path("logs/inputs")
_MAX_CALLS_PER_RUN = 50
_DEFAULT_MODEL = "claude-sonnet-4-6"
_MAX_TOKENS = 1000

T = TypeVar("T", bound=BaseModel)


# ---------------------------------------------------------------------------
# Pydantic output models — one per Claude call type
# ---------------------------------------------------------------------------

class CategorizerOutput(BaseModel):
    """Structured output for email categorizer calls."""

    category: str                   # one of 8 defined categories
    confidence: str                 # HIGH | MEDIUM | LOW
    urgency: int                    # 1=urgent, 2=radar, 3=summary
    draft_eligible: bool
    hold: bool
    sentiment_signals: List[str]
    source_email_id: str            # must match input email_id


class DraftOutput(BaseModel):
    """Structured output for draft generation calls."""

    draft_body: str
    confidence: str                 # HIGH | MEDIUM | LOW
    flags: List[str]                # UNSOURCED_PRICE | UNSOURCED_MODEL | NEEDS_BRAD_INPUT
    word_count: int
    source_references: List[str]    # thread IDs or WorkDrive chunk IDs used


class SignatureExtractOutput(BaseModel):
    """Structured output for email signature extraction calls."""

    name: Optional[str]             # None if not explicitly stated
    title: Optional[str]
    phone: Optional[str]
    mobile: Optional[str]
    company: Optional[str]
    website: Optional[str]
    confidence_per_field: Dict[str, str]   # field → HIGH | MEDIUM | LOW
    extraction_notes: str


class SentLogOutput(BaseModel):
    """Structured output for sent-log CRM note generation calls."""

    summary_text: str
    topic_tags: List[str]
    next_action: str               # NONE | FOLLOW_UP_7D | FOLLOW_UP_14D | AWAIT_RESPONSE | ESCALATE
    crm_note_text: str


class NoteOutput(BaseModel):
    """Structured output for CRM note generation calls."""

    note_text: str
    note_type: str                 # INBOUND_INQUIRY | QUOTE_DISCUSSION | SUPPORT_INTERACTION | GENERAL_CONTACT | ORDER_UPDATE
    requires_follow_up: bool
    follow_up_hint: Optional[str]  # null if requires_follow_up is false


# ---------------------------------------------------------------------------
# Call result container
# ---------------------------------------------------------------------------

class OrchestratorCallResult:
    """
    Result of a single Claude Orchestrator call.

    Attributes:
        output: Parsed Pydantic model, or None if capped/failed.
        source_input_hash: sha256 hex digest of the sorted input dict.
        prompt_name: Template name used, e.g. 'draft_v1'.
        prompt_version: Template version, e.g. '1.0'.
        fallback_level: 0/1/2 from PromptRegistry.
        model_version: Claude model string used.
        call_index: 1-based index of this call within the run.
        skipped_cap: True if this call was skipped due to 50-call cap.
        duration_ms: Wall-clock milliseconds for the Claude API call.
    """

    def __init__(
        self,
        output: Optional[BaseModel],
        source_input_hash: str,
        prompt_name: str,
        prompt_version: str,
        fallback_level: int,
        model_version: str,
        call_index: int,
        skipped_cap: bool = False,
        duration_ms: int = 0,
    ) -> None:
        self.output = output
        self.source_input_hash = source_input_hash
        self.prompt_name = prompt_name
        self.prompt_version = prompt_version
        self.fallback_level = fallback_level
        self.model_version = model_version
        self.call_index = call_index
        self.skipped_cap = skipped_cap
        self.duration_ms = duration_ms


# ---------------------------------------------------------------------------
# ClaudeOrchestrator
# ---------------------------------------------------------------------------

class ClaudeOrchestrator:
    """
    THE authorised path for all Claude API calls in ZDI Middleware.

    Enforces per Section 5:
    - Structured Outputs via tool_choice (constrained decoding)
    - max_tokens=1000 on every call
    - 50-call global cap per run (log warning + skip, do not abort)
    - source_input_hash = sha256(json.dumps(sorted_input_dict).encode())
    - Compressed raw input stored in logs/inputs/ keyed by source_input_hash

    Injectable dependencies for testing:
    - _anthropic_client: override with a mock client
    - _messages_create_fn: override the actual API call function
    - _inputs_dir: override compressed input storage path

    Usage:
        orchestrator = ClaudeOrchestrator(run_context, accuracy_logger)
        result = orchestrator.call(
            prompt_name="draft",
            rendered_prompt="...",
            input_dict={...},
            output_model=DraftOutput,
            prompt_version="1.0",
            fallback_level=0,
            thread_id="thread_abc",
        )
        if result.skipped_cap:
            # handle cap
        elif result.output:
            draft = result.output  # typed DraftOutput
    """

    def __init__(
        self,
        run_context: Any,           # RunContext
        accuracy_logger: Any,       # AccuracyLogger
        model: str = _DEFAULT_MODEL,
        inputs_dir: Optional[Path] = None,
        _anthropic_client: Optional[Any] = None,
        _messages_create_fn: Optional[Callable] = None,
    ) -> None:
        """
        Args:
            run_context: RunContext for this pipeline run (provides run_id etc.).
            accuracy_logger: AccuracyLogger instance for structured logging.
            model: Claude model string. Defaults to 'claude-sonnet-4-6'.
            inputs_dir: Directory for compressed input storage. Defaults to logs/inputs/.
            _anthropic_client: Injectable Anthropic client (testing / dependency injection).
            _messages_create_fn: Injectable callable(model, max_tokens, tools, tool_choice,
                messages) → response object. Overrides real API call when set.
        """
        self.run_context = run_context
        self.accuracy_logger = accuracy_logger
        self.model = model
        self.inputs_dir = Path(inputs_dir) if inputs_dir else _DEFAULT_LOGS_INPUTS_DIR
        self.inputs_dir.mkdir(parents=True, exist_ok=True)

        self._call_count: int = 0
        self._anthropic_client = _anthropic_client
        self._messages_create_fn = _messages_create_fn

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def call_count(self) -> int:
        """Number of Claude API calls made so far in this run."""
        return self._call_count

    def call(
        self,
        prompt_name: str,
        rendered_prompt: str,
        input_dict: Dict[str, Any],
        output_model: Type[T],
        prompt_version: str,
        fallback_level: int,
        thread_id: str,
    ) -> OrchestratorCallResult:
        """
        Make a single Claude API call with Structured Outputs enforcement.

        Args:
            prompt_name: Template name, e.g. 'draft_v1'.
            rendered_prompt: Fully rendered Jinja2 prompt string.
            input_dict: Raw input data (pre-render) for hash + audit storage.
            output_model: Pydantic model class for structured output.
            prompt_version: Template version string from PromptLoadResult.
            fallback_level: 0/1/2 from PromptRegistry.
            thread_id: Email thread ID being processed.

        Returns:
            OrchestratorCallResult with output, hash, fallback_level, etc.
        """
        source_input_hash = self._compute_hash(input_dict)
        call_index = self._call_count + 1

        # Enforce 50-call cap
        if self._call_count >= _MAX_CALLS_PER_RUN:
            logger.warning(
                "ClaudeOrchestrator: 50-call cap reached — skipping AI call "
                "call_index=%d thread_id=%s prompt=%s",
                call_index,
                thread_id,
                prompt_name,
            )
            self._log_accuracy(
                prompt_name=prompt_name,
                prompt_version=prompt_version,
                fallback_level=fallback_level,
                source_input_hash=source_input_hash,
                thread_id=thread_id,
                outcome="SKIPPED_CAP",
            )
            return OrchestratorCallResult(
                output=None,
                source_input_hash=source_input_hash,
                prompt_name=prompt_name,
                prompt_version=prompt_version,
                fallback_level=fallback_level,
                model_version=self.model,
                call_index=call_index,
                skipped_cap=True,
            )

        # Store compressed input for audit trail
        self._store_input(source_input_hash, rendered_prompt, input_dict)

        # Execute the Claude API call
        self._call_count += 1
        start_ms = int(time.monotonic() * 1000)
        output, outcome = self._execute_call(
            rendered_prompt=rendered_prompt,
            output_model=output_model,
            source_input_hash=source_input_hash,
            thread_id=thread_id,
            prompt_name=prompt_name,
        )
        duration_ms = int(time.monotonic() * 1000) - start_ms

        self._log_accuracy(
            prompt_name=prompt_name,
            prompt_version=prompt_version,
            fallback_level=fallback_level,
            source_input_hash=source_input_hash,
            thread_id=thread_id,
            outcome=outcome,
            extra={"duration_ms": duration_ms, "call_index": call_index},
        )

        return OrchestratorCallResult(
            output=output,
            source_input_hash=source_input_hash,
            prompt_name=prompt_name,
            prompt_version=prompt_version,
            fallback_level=fallback_level,
            model_version=self.model,
            call_index=call_index,
            skipped_cap=False,
            duration_ms=duration_ms,
        )

    # ------------------------------------------------------------------
    # Internal: Claude API call
    # ------------------------------------------------------------------

    def _execute_call(
        self,
        rendered_prompt: str,
        output_model: Type[T],
        source_input_hash: str,
        thread_id: str,
        prompt_name: str,
    ) -> Tuple[Optional[BaseModel], str]:
        """
        Execute the actual Claude API call via tool_choice structured output pattern.

        Returns: (parsed_output_or_None, outcome_string)
        """
        tool_schema = output_model.model_json_schema()
        tools = [{"name": "output", "input_schema": tool_schema}]
        tool_choice = {"type": "tool", "name": "output"}
        messages = [{"role": "user", "content": rendered_prompt}]

        try:
            if self._messages_create_fn is not None:
                # Injected test callable
                response = self._messages_create_fn(
                    model=self.model,
                    max_tokens=_MAX_TOKENS,
                    tools=tools,
                    tool_choice=tool_choice,
                    messages=messages,
                )
            else:
                # Real Anthropic client
                client = self._get_client()
                response = client.messages.create(
                    model=self.model,
                    max_tokens=_MAX_TOKENS,
                    tools=tools,
                    tool_choice=tool_choice,
                    messages=messages,
                )

            # Parse structured output from tool_use response block
            tool_use_block = next(
                (b for b in response.content if hasattr(b, "type") and b.type == "tool_use"),
                None,
            )
            if tool_use_block is None:
                logger.error(
                    "ClaudeOrchestrator: no tool_use block in response "
                    "hash=%s thread_id=%s prompt=%s",
                    source_input_hash[:16],
                    thread_id,
                    prompt_name,
                )
                return None, "NO_TOOL_USE_BLOCK"

            parsed = output_model(**tool_use_block.input)
            return parsed, "SUCCESS"

        except Exception as exc:
            logger.error(
                "ClaudeOrchestrator: API call failed hash=%s thread_id=%s "
                "prompt=%s exc_type=%s",
                source_input_hash[:16],
                thread_id,
                prompt_name,
                type(exc).__name__,
            )
            return None, f"ERROR:{type(exc).__name__}"

    # ------------------------------------------------------------------
    # Internal: hashing + storage
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_hash(input_dict: Dict[str, Any]) -> str:
        """
        Compute sha256 of the sorted JSON-serialised input dict.

        Returns: hex digest string.
        """
        serialised = json.dumps(input_dict, sort_keys=True, default=str).encode("utf-8")
        return hashlib.sha256(serialised).hexdigest()

    def _store_input(
        self,
        source_input_hash: str,
        rendered_prompt: str,
        input_dict: Dict[str, Any],
    ) -> None:
        """
        Store gzip-compressed raw input in logs/inputs/{source_input_hash}.json.gz.
        Used for 6-month audit capability. Skips if file already exists (idempotent).
        """
        out_path = self.inputs_dir / f"{source_input_hash}.json.gz"
        if out_path.exists():
            return  # already stored — idempotent

        payload = {
            "stored_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_input_hash": source_input_hash,
            "rendered_prompt": rendered_prompt,
            "input_dict": input_dict,
        }
        try:
            compressed = gzip.compress(
                json.dumps(payload, default=str).encode("utf-8"),
                compresslevel=9,
            )
            out_path.write_bytes(compressed)
            logger.debug(
                "ClaudeOrchestrator: stored compressed input hash=%s bytes=%d",
                source_input_hash[:16],
                len(compressed),
            )
        except OSError as exc:
            logger.error(
                "ClaudeOrchestrator: failed to store input hash=%s exc=%s",
                source_input_hash[:16],
                exc,
            )

    def _log_accuracy(
        self,
        prompt_name: str,
        prompt_version: str,
        fallback_level: int,
        source_input_hash: str,
        thread_id: str,
        outcome: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log to AccuracyLogger. Swallows exceptions — logging failure must not crash pipeline."""
        try:
            self.accuracy_logger.log_action(
                run_id=self.run_context.run_id,
                pipeline_version=self.run_context.pipeline_version,
                brad_voice_version=self.run_context.brad_voice_version,
                prompt_name=prompt_name,
                prompt_version=prompt_version,
                prompt_fallback_level=fallback_level,
                model_version=self.model,
                source_input_hash=source_input_hash,
                thread_id=thread_id,
                outcome=outcome,
                extra=extra,
            )
        except Exception as exc:
            logger.error(
                "ClaudeOrchestrator: accuracy_logger.log_action failed "
                "prompt=%s thread_id=%s exc=%s",
                prompt_name,
                thread_id,
                exc,
            )

    def _get_client(self) -> Any:
        """Return (or lazily create) the Anthropic client."""
        if self._anthropic_client is not None:
            return self._anthropic_client
        # Lazy import — avoids requiring anthropic at module load time in tests
        try:
            from anthropic import Anthropic
            self._anthropic_client = Anthropic()
        except ImportError as exc:
            raise RuntimeError(
                "ClaudeOrchestrator: anthropic package not installed. "
                "Run: pip install anthropic"
            ) from exc
        return self._anthropic_client
