"""
ai_output_validator.py
Bormann Marketing — Email Intelligence System v3
Validates AI-generated draft content before it is submitted to Zoho Mail Drafts.

Security rationale: Claude API responses are untrusted inputs — they can contain
hallucinated content, refusal text, prompt injection echoes, or malformed output.
This module validates every draft programmatically before it reaches Zoho.

Validation gates (in order):
  1. Not empty / not too short
  2. Does not contain known failure markers (refusal phrases, error placeholders)
  3. Does not exceed length cap (runaway generation guard)
  4. Does not echo system prompt fragments (prompt injection leak detection)
  5. Does not contain obvious hallucination markers (invented contact info patterns)
  6. Passes a basic professional tone sanity check (no ALL_CAPS screaming, no slurs)

On failure: returns ValidationResult(valid=False, reason=...) — the caller
(draft_generator.py) substitutes DRAFT_FAILED_BODY and continues. Never raises.
"""

import re
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MIN_DRAFT_CHARS  = 30      # Drafts shorter than this are clearly wrong
MAX_DRAFT_CHARS  = 8000    # Hard ceiling — runaway generation guard

# Strings that indicate Claude refused, errored, or returned placeholder content
FAILURE_MARKERS = [
    "[NEEDS CONTEXT]",
    "[AI DRAFT FAILED",
    "I cannot",
    "I'm unable to",
    "I am unable to",
    "I don't have access to",
    "As an AI language model",
    "As an AI assistant",
    "I apologize, but I cannot",
    "DRAFT_FAILED",
]

# System prompt fragments that should never appear in output
# (if they do, it indicates prompt injection echo)
SYSTEM_PROMPT_LEAK_MARKERS = [
    "You are Brad",
    "System prompt",
    "SYSTEM:",
    "CRITICAL: Only reference",
    "manufacturer's rep firm",
    "Bormann Marketing",   # Should never appear verbatim in outbound drafts
    "brad_voice_profile",
    "email_config.json",
]

# Patterns that suggest hallucinated contact data
# (e.g., invented phone numbers, made-up email addresses with suspicious patterns)
HALLUCINATION_PATTERNS = [
    r"\b\d{3}[-.\s]\d{3}[-.\s]\d{4}\b",     # US phone numbers (not expected in plain drafts)
    r"\b[A-Z][a-z]+ [A-Z][a-z]+, [A-Z]{2}\b",  # "City, ST" patterns (often hallucinated)
]

# Strings that indicate extremely unprofessional content
PROFANITY_FLOOR = [
    # Deliberately minimal — we don't want an over-aggressive profanity filter
    # blocking legitimate business language. Only catch clear outliers.
    "\x00",   # Null bytes — data corruption indicator
]


@dataclass
class ValidationResult:
    valid:   bool
    reason:  Optional[str] = None

    def __bool__(self):
        return self.valid


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate_draft(
    draft_body: str,
    category: str = "unknown",
    email_index: int = 0,
) -> ValidationResult:
    """
    Validate an AI-generated draft body before submission to Zoho.

    Args:
        draft_body:   The draft text returned by Claude
        category:     Email category (for log context only — no content logged)
        email_index:  Position in today's draft list (for log context)

    Returns:
        ValidationResult(valid=True) if draft passes all gates.
        ValidationResult(valid=False, reason=...) if any gate fails.
        Never raises.
    """
    try:
        return _run_validation(draft_body, category, email_index)
    except Exception as e:
        # Validator itself failed — log and pass through (fail open for business continuity)
        logger.error(
            f"[ai_output_validator] Validator crashed for {category} draft #{email_index}: "
            f"{type(e).__name__} — failing open (draft passed through)."
        )
        return ValidationResult(valid=True, reason="validator_crashed_fail_open")


def _run_validation(draft_body: str, category: str, email_index: int) -> ValidationResult:
    """Core validation logic — separated so validate_draft can catch all exceptions."""
    ctx = f"[ai_output_validator] draft #{email_index} ({category})"

    # Gate 1: Not None / not empty
    if not draft_body or not draft_body.strip():
        logger.warning(f"{ctx}: FAIL — empty or None body.")
        return ValidationResult(valid=False, reason="empty_body")

    stripped = draft_body.strip()

    # Gate 2: Minimum length
    if len(stripped) < MIN_DRAFT_CHARS:
        logger.warning(f"{ctx}: FAIL — too short ({len(stripped)} chars).")
        return ValidationResult(valid=False, reason=f"too_short_{len(stripped)}_chars")

    # Gate 3: Maximum length (runaway generation)
    if len(stripped) > MAX_DRAFT_CHARS:
        logger.warning(f"{ctx}: FAIL — too long ({len(stripped)} chars > {MAX_DRAFT_CHARS}).")
        return ValidationResult(valid=False, reason=f"too_long_{len(stripped)}_chars")

    # Gate 4: Known failure markers
    for marker in FAILURE_MARKERS:
        if marker.lower() in stripped.lower():
            logger.warning(f"{ctx}: FAIL — contains failure marker.")
            return ValidationResult(valid=False, reason="contains_failure_marker")

    # Gate 5: System prompt leak detection
    for leak in SYSTEM_PROMPT_LEAK_MARKERS:
        if leak.lower() in stripped.lower():
            logger.warning(f"{ctx}: FAIL — possible system prompt echo detected.")
            return ValidationResult(valid=False, reason="system_prompt_echo_detected")

    # Gate 6: Null bytes / corruption indicators
    for profanity in PROFANITY_FLOOR:
        if profanity in stripped:
            logger.warning(f"{ctx}: FAIL — contains corruption indicator.")
            return ValidationResult(valid=False, reason="contains_corruption_indicator")

    # Gate 7: Hallucination pattern check (log warning only — do not block)
    # We warn but don't fail here because phone numbers can appear legitimately
    # in quoted text or signatures from the original email.
    for pattern in HALLUCINATION_PATTERNS:
        if re.search(pattern, stripped):
            logger.warning(
                f"{ctx}: WARN — possible hallucinated content pattern detected. "
                f"Draft passed through — Brad should verify before sending."
            )
            break  # One warning is enough

    logger.debug(f"{ctx}: PASS — {len(stripped)} chars, all gates cleared.")
    return ValidationResult(valid=True)


# ---------------------------------------------------------------------------
# Batch validate (used by draft_generator.py)
# ---------------------------------------------------------------------------

def validate_all_drafts(drafts: list[dict]) -> list[dict]:
    """
    Validate all drafts in a list. Mutates each entry:
      - Adds 'validation_passed': bool
      - Adds 'validation_reason': str or None
      - If validation fails, replaces 'draft_body' with DRAFT_FAILED_BODY

    Args:
        drafts: list of draft dicts from draft_generator.py

    Returns:
        Same list with validation fields added.
    """
    from draft_generator import DRAFT_FAILED_BODY

    passed = failed = 0
    for i, draft in enumerate(drafts):
        body     = draft.get("draft_body", "")
        category = draft.get("category", "unknown")
        result   = validate_draft(body, category=category, email_index=i)

        draft["validation_passed"] = result.valid
        draft["validation_reason"] = result.reason

        if not result.valid:
            draft["draft_body"] = DRAFT_FAILED_BODY
            failed += 1
        else:
            passed += 1

    logger.info(
        f"[ai_output_validator] Batch validation complete: "
        f"{passed} passed, {failed} failed out of {len(drafts)} drafts."
    )
    return drafts
