"""
voice_profile.py
Bormann Marketing — Email Intelligence System v3
Loads Brad's voice profile and generates system prompts for draft generation.
"""

import json
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

VOICE_PROFILE_FILE = "voice/brad_voice_profile.json"

_DEFAULT_PROFILE = {
    "summary": (
        "Brad is the owner of Bormann Marketing. He writes in a direct, "
        "warm-but-professional tone. Short sentences. No corporate buzzwords. "
        "Gets to the point fast. Closes with clear next steps."
    ),
    "manufacturer_principal": (
        "With manufacturers, Brad is collegial and confident — peer to peer. "
        "He's direct about issues and asks for what he needs without hedging. "
        "Tone: collaborative, efficient, occasionally informal."
    ),
    "dealer_customer": (
        "With dealers and customers, Brad is helpful and solution-oriented. "
        "He's the trusted rep who knows the products and the territory. "
        "Tone: approachable, knowledgeable, never pushy."
    ),
    "internal_team": (
        "With his own team, Brad is brief and direct. Action items are clear. "
        "No hand-holding. Tone: efficient, supportive, occasionally dry."
    ),
    "unknown": (
        "Brad writes professionally and directly. He's concise and closes "
        "every email with a clear action or next step."
    ),
}


def load_profile() -> dict:
    """
    Load Brad's voice profile from JSON file.
    Falls back to default profile if file doesn't exist yet.
    """
    try:
        with open(VOICE_PROFILE_FILE, encoding="utf-8") as f:
            profile = json.load(f)
        logger.debug("Voice profile loaded from file.")
        return profile
    except FileNotFoundError:
        logger.info(
            "brad_voice_profile.json not found — using default profile. "
            "Run build_profile.py after adding voice samples."
        )
        return _DEFAULT_PROFILE.copy()
    except Exception as e:
        logger.warning(f"Could not load voice profile: {e} — using default.")
        return _DEFAULT_PROFILE.copy()


# Recipient type → profile key mapping
_CATEGORY_TO_VOICE = {
    "Manufacturer":        "manufacturer_principal",
    "Dealer-Customer":     "dealer_customer",
    "Internal-Team":       "internal_team",
    "Business-Dev":        "unknown",
    "Other":               "unknown",
}


def get_draft_system_prompt(recipient_type: str, profile: Optional[dict] = None) -> str:
    """
    Returns a Claude system prompt for draft generation tuned to the recipient type.

    Args:
        recipient_type: email category (Manufacturer, Dealer-Customer, etc.)
        profile: loaded voice profile dict (optional — loads from file if None)
    """
    if profile is None:
        profile = load_profile()

    voice_key   = _CATEGORY_TO_VOICE.get(recipient_type, "unknown")
    voice_notes = profile.get(voice_key) or profile.get("unknown", _DEFAULT_PROFILE["unknown"])
    summary     = profile.get("summary", _DEFAULT_PROFILE["summary"])

    system_prompt = f"""You are drafting an email reply on behalf of Brad, the owner of Bormann Marketing, \
a manufacturer's rep firm in Minnesota covering pro AV and low-voltage products (MN/ND/SD/WI territory).

Brad's overall voice:
{summary}

Voice guidance for this recipient type ({recipient_type}):
{voice_notes}

HARD RULES:
- Reply body only — no subject line, no metadata, no greeting/salutation (Brad will add those himself)
- Do not start the reply with "Dear" or "Hi [Name]"
- Match Brad's natural writing length — if the incoming email is brief, reply brief
- No corporate jargon, no filler phrases ("Hope this finds you well", "Per my last email", "Going forward")
- Close with a clear next step or action item when appropriate
- If you referenced product information from a spec sheet, add ONE line at the end: Source: [file_name]"""

    return system_prompt
