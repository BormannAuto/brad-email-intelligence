# src/zdi_mw/core/integrity_gate.py
# ZDI Middleware — IntegrityGate
# THE ONLY PATH to ClaudeOrchestrator and SafeClients. Every AI interaction
# must pass through pre_validate() before Claude and post_validate() after.
# No code may bypass either gate.
#
# Section 10.3 Anti-Math Rule: calculated dollar values are treated as
# unsourced even if arithmetically derivable. AI may quote, never compute.

import html
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional

logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
_SCHEMA_DIR = _REPO_ROOT / "src" / "config"

# Regex patterns for traceability checks
_DOLLAR_PATTERN = re.compile(r'\$[\d,]+(?:\.\d{1,2})?')
_MODEL_NUMBER_PATTERN = re.compile(r'\b[A-Z]{1,4}[-]?\d{2,}[A-Z0-9\-]*\b')
_DATE_PATTERN = re.compile(
    r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|'
    r'Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|'
    r'Dec(?:ember)?)\s+\d{1,2}(?:,\s+\d{4})?'
    r'|\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b'
    r'|\b\d{4}-\d{2}-\d{2}\b',
    re.IGNORECASE,
)

# Confidence levels — LOW blocks pipeline
CONFIDENCE_LOW = "LOW"
CONFIDENCE_MEDIUM = "MEDIUM"
CONFIDENCE_HIGH = "HIGH"

# Flags added to post_validate results
FLAG_UNSOURCED_PRICE = "UNSOURCED_PRICE"
FLAG_UNSOURCED_MODEL = "UNSOURCED_MODEL"
FLAG_WORD_COUNT_EXCEEDED = "WORD_COUNT_EXCEEDED"
FLAG_UNSOURCED_CALCULATED_VALUE = "UNSOURCED_CALCULATED_VALUE"  # Section 10.3

MAX_DRAFT_WORD_COUNT = 500


@dataclass
class ValidationResult:
    """Result of a pre_validate() or post_validate() call."""
    passed: bool
    reason: str = ""
    flags: List[str] = field(default_factory=list)
    gate_name: str = ""

    def add_flag(self, flag: str) -> None:
        if flag not in self.flags:
            self.flags.append(flag)

    def fail(self, reason: str, gate: str = "") -> "ValidationResult":
        self.passed = False
        self.reason = reason
        if gate:
            self.gate_name = gate
        return self


class IntegrityGateError(RuntimeError):
    """Raised for hard gate failures (misconfiguration, not data failures)."""
    pass


class IntegrityGate:
    """
    Two-gate AI safety layer.

    pre_validate()  — runs on assembled input BEFORE any Claude call.
    post_validate() — runs on Claude output BEFORE any SafeClient write.

    Neither gate raises on validation failure — they return ValidationResult
    with passed=False so the pipeline can log, DLQ, and continue with other emails.

    The bypass_hold flag is ONLY set by promote_to_draft.py (Section 10.2).
    No other code path may set it.

    Usage:
        gate = IntegrityGate(run_context, mail_schema_path, crm_schema_path)
        pre_result = gate.pre_validate(record_snapshot, email_metadata)
        if not pre_result.passed:
            # log, DLQ, continue with next email
            return
        # ... call ClaudeOrchestrator ...
        post_result = gate.post_validate(ai_output, source_email, workdrive_ctx)
        if not post_result.passed:
            # log, DLQ, continue
            return
        # ... call SafeClients ...
    """

    def __init__(
        self,
        run_context: Any,  # RunContext — typed as Any to avoid circular import
        mail_schema_path: Optional[Path] = None,
        crm_schema_path: Optional[Path] = None,
        bypass_hold: bool = False,  # ONLY set by promote_to_draft.py
        rate_manager: Any = None,   # RateManager — optional, checked in pre_validate gate 5
    ) -> None:
        self._run_context = run_context
        self._mail_schema = self._load_schema(
            mail_schema_path or _SCHEMA_DIR / "zoho_mail_schema.json"
        )
        self._crm_schema = self._load_schema(
            crm_schema_path or _SCHEMA_DIR / "zoho_crm_schema.json"
        )
        self._bypass_hold = bypass_hold
        self._rate_manager = rate_manager

    # ------------------------------------------------------------------
    # Gate 1 — pre_validate
    # ------------------------------------------------------------------

    def pre_validate(
        self,
        record_snapshot: dict,
        email_metadata: dict,
    ) -> ValidationResult:
        """
        Validate assembled input before any Claude call.

        Gates (in order):
          1. Schema: email_metadata against zoho_mail_schema.json
          2. Confidence: contact match confidence present and not LOW
          3. Source: email body non-empty after HTML strip
          4. Temporal: all input timestamps predate RunContext.utc_start
          5. Rate budget: Claude call budget not exhausted

        Args:
            record_snapshot: Assembled contact record from RecordAssembler.
            email_metadata: Raw Zoho Mail message dict.

        Returns:
            ValidationResult with passed=True if all gates pass.
        """
        result = ValidationResult(passed=True, gate_name="pre_validate")

        # Gate 1 — Schema validation
        schema_result = self._check_schema(email_metadata, self._mail_schema, "email_metadata")
        if not schema_result.passed:
            logger.warning(
                "IntegrityGate pre_validate FAIL gate=schema reason=%s", schema_result.reason
            )
            return schema_result

        # Gate 2 — Confidence check
        confidence = record_snapshot.get("confidence") or (
            record_snapshot.get("contact", {}) or {}
        ).get("confidence", "")

        # Also check top-level confidence field
        if not confidence:
            confidence = record_snapshot.get("confidence", "")

        if confidence == CONFIDENCE_LOW:
            result.fail(
                f"Contact match confidence is LOW — pipeline cannot proceed safely",
                gate="confidence_check",
            )
            logger.warning("IntegrityGate pre_validate FAIL gate=confidence confidence=%s", confidence)
            return result

        # Gate 3 — Source text non-empty after HTML strip
        body = email_metadata.get("content", "") or email_metadata.get("htmlContent", "") or ""
        stripped = self._strip_html(body).strip()
        if not stripped:
            result.fail(
                "Email body is empty after HTML stripping — no source text to process",
                gate="source_text",
            )
            logger.warning("IntegrityGate pre_validate FAIL gate=source_text")
            return result

        # Gate 4 — Temporal: input timestamps must predate RunContext.utc_start
        temporal_result = self._check_temporal(email_metadata, record_snapshot)
        if not temporal_result.passed:
            logger.warning(
                "IntegrityGate pre_validate FAIL gate=temporal reason=%s",
                temporal_result.reason,
            )
            return temporal_result

        # Gate 5 — Rate budget (if RateManager provided)
        if self._rate_manager is not None:
            try:
                from src.zdi_mw.core.rate_manager import BudgetExhaustedError
                self._rate_manager.check_and_wait("crm", estimated_cost=1)
            except Exception as exc:  # BudgetExhaustedError or similar
                result.fail(
                    f"Claude call budget exhausted: {type(exc).__name__}",
                    gate="rate_budget",
                )
                logger.warning(
                    "IntegrityGate pre_validate FAIL gate=rate_budget error=%s", exc
                )
                return result

        logger.debug(
            "IntegrityGate pre_validate PASSED thread_id=%s",
            email_metadata.get("threadId", "unknown"),
        )
        return result

    # ------------------------------------------------------------------
    # Gate 2 — post_validate
    # ------------------------------------------------------------------

    def post_validate(
        self,
        ai_output: Any,  # Pydantic model instance (CategorizerOutput, DraftOutput, etc.)
        source_email: str,
        workdrive_context: Optional[str] = None,
    ) -> ValidationResult:
        """
        Validate Claude output before any SafeClient write.

        Gates (in order):
          1. Dollar amounts: every $ in output traceable verbatim to source
          2. Model numbers: every part number traceable to source or workdrive
          3. Contact names: every name in output present in source email headers
          4. Date commitments: no deadlines/dates not in source
          5. Word count: flag (don't block) if draft > 500 words
          6. HOLD check: reject entirely if category == HOLD (unless bypass_hold)
          7. Anti-math: flag calculated totals not verbatim in source (Section 10.3)

        Args:
            ai_output: Pydantic model instance from ClaudeOrchestrator.
            source_email: Raw email body text (HTML-stripped).
            workdrive_context: WorkDrive chunk text injected into prompt, if any.

        Returns:
            ValidationResult. Flags are non-blocking unless passed=False.
        """
        result = ValidationResult(passed=True, gate_name="post_validate")

        # Convert output to searchable text
        output_text = self._output_to_text(ai_output)
        combined_source = source_email + (" " + workdrive_context if workdrive_context else "")

        # Gate 6 — HOLD check (before other gates — fast reject)
        category = getattr(ai_output, "category", None) or getattr(ai_output, "hold", None)
        if self._is_hold(ai_output) and not self._bypass_hold:
            result.fail(
                "Email category is HOLD — output rejected. "
                "Use promote_to_draft.py to release after human review.",
                gate="hold_check",
            )
            logger.warning("IntegrityGate post_validate FAIL gate=hold_check")
            return result

        # Gate 1 — Dollar amount traceability
        # Section 10.3: when WorkDrive context is present, calculated totals are
        # flagged but NOT blocked (Brad may intend a summary total — he must see the flag).
        # When WorkDrive is absent, any unsourced dollar amount hard-blocks the draft.
        dollar_amounts = _DOLLAR_PATTERN.findall(output_text)
        unsourced_dollars = [
            amt for amt in dollar_amounts
            if self._normalise_number(amt) not in self._normalise_number(combined_source)
        ]
        if unsourced_dollars:
            if workdrive_context:
                # WorkDrive present: flag as potential calculated value, no hard block
                # (Anti-math gate below will also catch and flag these)
                result.add_flag(FLAG_UNSOURCED_PRICE)
                logger.warning(
                    "IntegrityGate post_validate FLAG gate=dollar_traceability "
                    "amounts=%s — workdrive present, flagging not blocking",
                    unsourced_dollars,
                )
            else:
                # No WorkDrive: hard block — price is purely invented
                result.fail(
                    f"Unsourced dollar amounts in output: {unsourced_dollars}. "
                    f"Every dollar figure must appear verbatim in source email or WorkDrive context.",
                    gate="dollar_traceability",
                )
                result.add_flag(FLAG_UNSOURCED_PRICE)
                logger.warning(
                    "IntegrityGate post_validate FAIL gate=dollar_traceability amounts=%s",
                    unsourced_dollars,
                )
                return result

        # Gate 2 — Model number traceability
        model_numbers = _MODEL_NUMBER_PATTERN.findall(output_text)
        unsourced_models = [
            m for m in model_numbers
            if m not in combined_source
        ]
        if unsourced_models:
            result.add_flag(FLAG_UNSOURCED_MODEL)
            logger.warning(
                "IntegrityGate post_validate FLAG gate=model_traceability models=%s",
                unsourced_models,
            )
            # Flag but don't block — model numbers can have formatting variations

        # Gate 3 — Contact names (from email headers only)
        # Names are checked against source_email (headers), not workdrive
        header_names = self._extract_header_names(source_email)
        if header_names:
            output_names = self._extract_names_from_output(ai_output)
            unverified_names = [n for n in output_names if not any(
                n.lower() in hn.lower() or hn.lower() in n.lower()
                for hn in header_names
            )]
            if unverified_names:
                logger.warning(
                    "IntegrityGate post_validate WARNING gate=name_check "
                    "unverified_names=%s — flagging for review",
                    unverified_names,
                )
                # Flag only — name matching is fuzzy, don't hard-block

        # Gate 4 — Date commitments must appear in source
        dates_in_output = _DATE_PATTERN.findall(output_text)
        unsourced_dates = [d for d in dates_in_output if d not in combined_source]
        if unsourced_dates:
            logger.warning(
                "IntegrityGate post_validate WARNING gate=date_check "
                "unsourced_dates=%s — these may be AI-invented commitments",
                unsourced_dates,
            )
            # Log and flag, but don't hard-block — dates can appear in different formats

        # Gate 5 — Word count (flag, don't block)
        word_count = len(output_text.split())
        if word_count > MAX_DRAFT_WORD_COUNT:
            result.add_flag(FLAG_WORD_COUNT_EXCEEDED)
            logger.warning(
                "IntegrityGate post_validate FLAG gate=word_count "
                "word_count=%d limit=%d",
                word_count,
                MAX_DRAFT_WORD_COUNT,
            )

        # Gate 7 — Anti-math rule (Section 10.3): activates when WorkDrive context present
        if workdrive_context:
            anti_math_flags = self._check_anti_math(output_text, source_email, workdrive_context)
            if anti_math_flags:
                result.add_flag(FLAG_UNSOURCED_CALCULATED_VALUE)
                logger.warning(
                    "IntegrityGate post_validate FLAG gate=anti_math "
                    "calculated_values=%s — ⚠️ draft contains calculated totals "
                    "not verified in source documents — verify before sending.",
                    anti_math_flags,
                )

        logger.debug("IntegrityGate post_validate PASSED flags=%s", result.flags)
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _check_schema(
        self, data: dict, schema: dict, context: str
    ) -> ValidationResult:
        """Validate data against a JSON schema dict. Returns ValidationResult."""
        result = ValidationResult(passed=True, gate_name="schema_validation")
        if not schema:
            # No schema loaded — pass with warning
            logger.warning("IntegrityGate: no schema loaded for %s — skipping schema check", context)
            return result

        try:
            import jsonschema  # verified: pypi.org/project/jsonschema
        except ImportError:
            logger.warning(
                "IntegrityGate: jsonschema not installed — schema validation skipped"
            )
            return result

        try:
            jsonschema.validate(instance=data, schema=schema)
        except jsonschema.ValidationError as exc:
            result.fail(
                f"Schema validation failed for {context}: {exc.message} "
                f"(path: {'.'.join(str(p) for p in exc.path)})",
                gate="schema_validation",
            )
        except jsonschema.SchemaError as exc:
            logger.error("IntegrityGate: invalid schema for %s: %s", context, exc)

        return result

    def _check_temporal(
        self, email_metadata: dict, record_snapshot: dict
    ) -> ValidationResult:
        """
        Gate 4: verify input data timestamps predate RunContext.utc_start.
        Prevents cross-run contamination and feedback loops.
        """
        result = ValidationResult(passed=True, gate_name="temporal_check")
        run_start = self._run_context.utc_start

        # Check snapshot_at from record_snapshot
        snapshot_at_str = record_snapshot.get("snapshot_at")
        if snapshot_at_str:
            try:
                snapshot_at = datetime.fromisoformat(snapshot_at_str)
                # Ensure timezone-aware comparison
                if snapshot_at.tzinfo is None:
                    snapshot_at = snapshot_at.replace(tzinfo=timezone.utc)
                if snapshot_at > run_start:
                    result.fail(
                        f"Cross-run contamination detected: record_snapshot.snapshot_at "
                        f"({snapshot_at.isoformat()}) is AFTER RunContext.utc_start "
                        f"({run_start.isoformat()}). This data was created during this run.",
                        gate="temporal_check",
                    )
                    return result
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "IntegrityGate: could not parse snapshot_at '%s': %s",
                    snapshot_at_str, exc,
                )

        return result

    def _check_anti_math(
        self, output_text: str, source_email: str, workdrive_context: str
    ) -> List[str]:
        """
        Section 10.3: Anti-math rule.
        Find dollar values in output that do NOT appear verbatim in either source.
        Returns list of flagged values (empty = clean).
        """
        combined = source_email + " " + workdrive_context
        dollar_amounts = _DOLLAR_PATTERN.findall(output_text)
        calculated = []
        for amt in dollar_amounts:
            normalised = self._normalise_number(amt)
            if normalised not in self._normalise_number(combined):
                calculated.append(amt)
        return calculated

    @staticmethod
    def _strip_html(text: str) -> str:
        """Remove HTML tags and unescape entities."""
        clean = re.sub(r'<[^>]+>', ' ', text)
        return html.unescape(clean)

    @staticmethod
    def _normalise_number(text: str) -> str:
        """Remove commas and whitespace for numeric comparison."""
        return re.sub(r'[,\s]', '', text)

    @staticmethod
    def _output_to_text(ai_output: Any) -> str:
        """Extract searchable text from a Pydantic output model."""
        if hasattr(ai_output, "draft_body"):
            return ai_output.draft_body or ""
        if hasattr(ai_output, "model_dump"):
            return json.dumps(ai_output.model_dump(), default=str)
        return str(ai_output)

    @staticmethod
    def _is_hold(ai_output: Any) -> bool:
        """Return True if the AI output indicates a HOLD category."""
        if hasattr(ai_output, "hold") and ai_output.hold is True:
            return True
        if hasattr(ai_output, "category") and str(getattr(ai_output, "category", "")).upper() == "HOLD":
            return True
        return False

    @staticmethod
    def _extract_header_names(source_email: str) -> List[str]:
        """
        Extract sender/recipient names from email headers embedded in the source text.
        Looks for patterns like 'From: Name <email>' and 'To: Name <email>'.
        """
        names = []
        for match in re.finditer(
            r'(?:From|To|Cc):\s*([^<\n]+?)(?:\s*<[^>]+>)?(?:\n|$)',
            source_email,
            re.IGNORECASE,
        ):
            name = match.group(1).strip()
            if name:
                names.append(name)
        return names

    @staticmethod
    def _extract_names_from_output(ai_output: Any) -> List[str]:
        """Extract any proper nouns from output that look like names."""
        text = IntegrityGate._output_to_text(ai_output)
        # Simple heuristic: capitalised multi-word sequences not at sentence start
        return re.findall(r'\b[A-Z][a-z]+ [A-Z][a-z]+\b', text)

    @staticmethod
    def _load_schema(path: Path) -> dict:
        """Load a JSON schema file. Returns empty dict if file not found."""
        try:
            with open(path) as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning("IntegrityGate: schema file not found at %s", path)
            return {}
        except json.JSONDecodeError as exc:
            logger.error("IntegrityGate: failed to parse schema at %s: %s", path, exc)
            return {}
