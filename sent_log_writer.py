"""
sent_log_writer.py
Bormann Marketing — Email Intelligence System v3
Writes a first-person log entry for each draft, stores in sent_log.json,
and writes a note to the CRM contact record.
"""

import json
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional
from anthropic import Anthropic

logger = logging.getLogger(__name__)

SENT_LOG_FILE    = "data/sent_log.json"
RETENTION_DAYS   = 30

SYSTEM_PROMPT = """You write concise, first-person log entries for Brad, the owner of Bormann Marketing. \
Each entry should sound like a personal note Brad would write himself — not a system log. \
Write in past tense. Be specific and direct. Max 30 words.

Format: [I told / I confirmed / I asked / I pushed back on] [who] [what the core message was] [why or what happens next].

Examples:
"I told Sarah at AVI Systems the Shure MXA910 is back in stock and ships next week."
"I confirmed with Mike at Biamp that the demo unit is available for the Henderson account."
"I asked Jason to resend the revised quote — I needed the updated line item breakdown."
"""


def _load_voice_summary() -> str:
    """Load brief voice summary from brad_voice_profile.json for log style."""
    try:
        with open("voice/brad_voice_profile.json") as f:
            profile = json.load(f)
        return profile.get("summary", "")
    except Exception:
        return ""


def _load_sent_log() -> list[dict]:
    """Load existing sent log, pruning entries older than 30 days."""
    try:
        with open(SENT_LOG_FILE) as f:
            entries = json.load(f)
    except FileNotFoundError:
        return []
    except Exception as e:
        logger.warning(f"Could not load sent_log.json: {e}")
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    pruned = []
    for entry in entries:
        try:
            entry_dt = datetime.fromisoformat(entry.get("date", ""))
            if entry_dt >= cutoff:
                pruned.append(entry)
        except Exception:
            pruned.append(entry)  # keep if date unparseable
    return pruned


def _save_sent_log(entries: list[dict]) -> None:
    os.makedirs("data", exist_ok=True)
    try:
        with open(SENT_LOG_FILE, "w", encoding="utf-8") as f:
            json.dump(entries, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Could not save sent_log.json: {e}")


def write_sent_log(
    drafts: list[dict],
    crm_session: Optional[dict],
) -> list[dict]:
    """
    For each draft, generate a log entry and write to sent_log.json + CRM.

    Args:
        drafts: list of draft records from draft_generator.py
        crm_session: active CRM session (or None if CRM unavailable)

    Returns:
        list of new log entries appended this run
    """
    if not drafts:
        logger.info("No drafts to log.")
        return []

    client       = Anthropic()
    voice_summary = _load_voice_summary()
    existing_log  = _load_sent_log()
    new_entries   = []

    for draft in drafts:
        draft_body     = draft.get("draft_body", "")
        recipient_name = draft.get("recipient_name", "")
        recipient_co   = draft.get("category", "")  # company not always in draft record
        category       = draft.get("category", "")
        draft_id       = draft.get("draft_id", "")

        if not draft_body:
            continue

        # Build prompt
        voice_note   = f"Voice guidance: {voice_summary}" if voice_summary else ""
        user_message = (
            f"Write a one-sentence log entry for Brad.\n\n"
            f"{voice_note}\n\n"
            f"Draft sent to: {recipient_name} ({category})\n"
            f"Draft body:\n{draft_body[:600]}"
        )

        try:
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",  # fast + cheap for log entries
                max_tokens=80,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_message}],
            )
            log_entry_text = response.content[0].text.strip().strip('"')
        except Exception as e:
            logger.warning(f"Log entry generation failed for draft {draft_id}: {e}")
            log_entry_text = f"Draft sent to {recipient_name}."

        now = datetime.now(timezone.utc)
        entry = {
            "date":               now.isoformat(),
            "draft_id":           draft_id,
            "recipient_name":     recipient_name,
            "recipient_company":  draft.get("recipient_email", "").split("@")[-1],
            "category":           category,
            "log_entry":          log_entry_text,
        }
        new_entries.append(entry)
        logger.info(f"Log entry: {log_entry_text[:80]}")

        # Write to CRM
        if crm_session and draft.get("recipient_email"):
            _write_crm_note(crm_session, draft, log_entry_text, now)

    # Append new entries to rolling log
    existing_log.extend(new_entries)
    _save_sent_log(existing_log)
    logger.info(f"sent_log_writer: wrote {len(new_entries)} entries.")
    return new_entries


def _write_crm_note(
    crm_session: dict,
    draft: dict,
    log_entry_text: str,
    timestamp: datetime,
) -> None:
    """Write log entry as a CRM note. Never blocks pipeline on failure."""
    try:
        from zoho_crm_connector import lookup_contact, add_note

        contact = lookup_contact(crm_session, draft.get("recipient_email", ""))
        if not contact.get("found") or not contact.get("contact_id"):
            logger.debug(
                f"CRM note skipped — no contact found for {draft.get('recipient_email')}"
            )
            return

        note_text = (
            f"{timestamp.strftime('%Y-%m-%d')} AI-Draft-Log: {log_entry_text}"
        )
        success = add_note(crm_session, contact["contact_id"], note_text)
        if success:
            logger.debug(
                f"CRM note written for {draft.get('recipient_name')} "
                f"({contact['contact_id']})"
            )
    except Exception as e:
        logger.warning(f"CRM note write failed for {draft.get('recipient_email')}: {e}")
