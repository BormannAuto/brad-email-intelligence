"""
api_guard.py
Bormann Marketing — Email Intelligence System v3
Enforces the 50-call-per-run hard cap on Claude API usage.
Every module that calls the Claude API must use make_claude_call() from this module.

Security rationale: uncapped AI calls can exhaust budget and generate unexpected costs.
The 50-call cap ensures a runaway categorizer or draft loop never exceeds a defined ceiling.
"""

import logging
from anthropic import Anthropic

logger = logging.getLogger(__name__)

# Hard cap per pipeline run (per CLAUDE.md locked decision)
CLAUDE_CALL_CAP = 50

# Module-level counter — reset only by a fresh Python process (i.e., each GitHub Actions run)
_call_count: int = 0
_cap_reached: bool = False


def reset_counter() -> None:
    """Reset counter — called at pipeline start for explicit clarity."""
    global _call_count, _cap_reached
    _call_count = 0
    _cap_reached = False


def get_call_count() -> int:
    return _call_count


def make_claude_call(client: Anthropic, *, model: str, max_tokens: int,
                     system: str, messages: list, caller: str = "unknown") -> str:
    """
    Wrapper for all Claude API calls. Enforces the 50-call cap.
    Returns response text on success.
    Raises RuntimeError if cap is reached.
    Raises ValueError if max_tokens is not set (guard against accidental omission).

    Args:
        client:     Anthropic client instance
        model:      Model string
        max_tokens: REQUIRED — no default. Every call must be explicit.
        system:     System prompt
        messages:   Message list
        caller:     Name of the calling module for logging

    Returns: response content text
    """
    global _call_count, _cap_reached

    if max_tokens is None or max_tokens <= 0:
        raise ValueError(
            f"make_claude_call from '{caller}': max_tokens must be set explicitly. "
            f"Uncapped responses can exhaust memory and generate unexpected API costs."
        )

    if _cap_reached or _call_count >= CLAUDE_CALL_CAP:
        _cap_reached = True
        logger.warning(
            f"[api_guard] Claude API cap reached ({CLAUDE_CALL_CAP} calls). "
            f"Skipping call from '{caller}'. Investigate if this fires regularly."
        )
        raise RuntimeError(
            f"Claude API call cap ({CLAUDE_CALL_CAP}) reached. "
            f"Call from '{caller}' was skipped. Pipeline continues without this AI content."
        )

    _call_count += 1
    logger.debug(f"[api_guard] Claude call {_call_count}/{CLAUDE_CALL_CAP} — caller: {caller}")

    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=messages,
    )
    return response.content[0].text


def claude_cap_status() -> dict:
    """Return current cap status for logging/debrief."""
    return {
        "calls_made":  _call_count,
        "cap":         CLAUDE_CALL_CAP,
        "cap_reached": _cap_reached,
    }
