"""
sent_log_writer.py
Bormann Marketing — Email Intelligence System v3

Security:
- Uses api_guard for 50-call cap.
- Prompts include required "only reference provided content" instruction.
- run_id deduplication: skips entries if today's run_id already in sent_log.json.
- Log statements contain no email content, contact names, or email addresses.
"""

import json
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional
from anthropic import Anthropic
from api_guard import make_claude_call

logger = logging.getLogger(__name__)

SENT_LOG_FILE  = "data/sent_log.json"
RETENTION_DAYS = 30

SYSTEM_PROMPT = """You write concise, first-person log entries for Brad, the owner of Bormann Marketing.
Sound like a personal note Brad would write — not a system log. Write in past tense. Max 30 words.

Format: [I told / I confirmed / I asked / I pushed back on] [who] [what the core message was] [why or what happens next].

IMPORTANT: Only reference information explicitly stated in the draft email provided.
Do not infer, extrapolate, or add context not present in the source email.
Write in first-person past tense as Brad. Do not invent details."""


def _load_voice_summary() -> str:
    try:
        with open("voice/brad_voice_profile.json") as f:
            return json.load(f).get("summary", "")
    except Exception:
        return ""


def _load_sent_log() -> list[dict]:
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
            pruned.append(entry)
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
    run_id: Optional[str] = None,
) -> list[dict]:
    """
    For each draft, generate a log entry and write to sent_log.json + CRM.
    run_id deduplication: skips if this run_id already has entries in sent_log.json.
    """
    if not drafts:
        logger.info("No drafts to log.")
        return []

    if run_id is None:
        run_id = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    client        = Anthropic()
    voice_summary = _load_voice_summary()
    existing_log  = _load_sent_log()

    # Idempotency: skip if run_id already present in log
    existing_run_ids = {e.get("run_id") for e in existing_log}
    if run_id in existing_run_ids:
        logger.warning(
            f"sent_log_writer: run_id {run_id} already in sent_log.json — "
            f"skipping to prevent duplicate entries."
        )
        return []

    new_entries = []

    for draft in drafts:
        draft_body     = draft.get("draft_body", "")
        recipient_name = draft.get("recipient_name", "")
        category       = draft.get("category", "")
        draft_id       = draft.get("draft_id", "")
        is_placeholder = draft.get("is_placeholder", False)

        if not draft_body or is_placeholder:
            continue

        voice_note   = f"Voice guidance: {voice_summary}" if voice_summary else ""
        user_message = (
            f"Write a one-sentence log entry for Brad.\n\n"
            f"{voice_note}\n\n"
            f"Draft sent to: {recipient_name} ({category})\n"
            f"Draft body:\n{draft_body[:600]}"
        )

        try:
            log_entry_text = make_claude_call(
                client,
                model="claude-haiku-4-5-20251001",
                max_tokens=80,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_message}],
                caller="sent_log_writer",
            ).strip().strip('"')
        except RuntimeError as e:
            logger.warning(f"Sent log entry skipped — API cap: {e}")
            log_entry_text = f"Draft sent ({category})."
        except Exception as e:
            logger.warning(f"Log entry generation failed for draft {draft_id}: {e}")
            log_entry_text = f"Draft sent ({category})."

        now = datetime.now(timezone.utc)
        entry = {
            "date":              now.isoformat(),
            "run_id":            run_id,
            "draft_id":          draft_id,
            "recipient_name":    recipient_name,
            "recipient_company": draft.get("recipient_email", "").split("@")[-1],
            "category":          category,
            "log_entry":         log_entry_text,
        }
        new_entries.append(entry)
        # Log count and category only — no contact names or log entry content
        logger.debug(f"Log entry generated — category: {category}")

        # Write to CRM
        if crm_session and draft.get("recipient_email"):
            _write_crm_note(crm_session, draft, log_entry_text, now)

    existing_log.extend(new_entries)
    _save_sent_log(existing_log)
    logger.info(f"sent_log_writer: wrote {len(new_entries)} entries (run_id: {run_id}).")
    return new_entries


def _write_crm_note(
    crm_session: dict,
    draft: dict,
    log_entry_text: str,
    timestamp: datetime,
) -> None:
    """Write log entry as CRM note. Never blocks pipeline on failure."""
    try:
        from zoho_crm_connector import lookup_contact, add_note

        contact = lookup_contact(crm_session, draft.get("recipient_email", ""))
        if not contact.get("found") or not contact.get("contact_id"):
            logger.debug("CRM note skipped — contact not found.")
            return

        note_text = f"{timestamp.strftime('%Y-%m-%d')} AI-Draft-Log: {log_entry_text}"
        success   = add_note(crm_session, contact["contact_id"], note_text)
        if success:
            logger.debug("CRM note written successfully.")
    except Exception as e:
        # Never log recipient email address — category only
        logger.warning(f"CRM note write failed (category: {draft.get('category', '?')}): {e}")
