"""
draft_generator.py
Bormann Marketing — Email Intelligence System v3
Generates reply drafts for emails where draft_needed=True and hold_flag=False.
Saves drafts to today_drafts.json and creates them in Zoho Mail Drafts.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional
from anthropic import Anthropic
from voice_profile import load_profile, get_draft_system_prompt
from workdrive_retriever import retrieve_product_context

logger = logging.getLogger(__name__)

TODAY_DRAFTS_FILE = "today_drafts.json"


def _build_user_prompt(
    email: dict,
    thread_context: str,
    crm_context: dict,
    workdrive_chunks: list[dict],
) -> str:
    """Build the full user-turn prompt for draft generation."""
    parts = []

    # CRM context
    if crm_context.get("found"):
        parts.append(
            f"CRM Context:\n"
            f"- Contact: {crm_context.get('name', 'Unknown')}\n"
            f"- Company: {crm_context.get('company', 'Unknown')}\n"
            f"- Recent Note: {crm_context.get('recent_note', 'None')}"
        )

    # WorkDrive product context
    if workdrive_chunks:
        wd_block = "Product Reference (from spec sheets):\n"
        for chunk in workdrive_chunks:
            wd_block += (
                f"\n[Source: {chunk['file_name']}]\n"
                f"{chunk['chunk_text'][:800]}\n"
            )
        parts.append(wd_block)

    # Thread context
    if thread_context:
        parts.append(f"Thread context (most recent messages):\n{thread_context}")

    # The email to reply to
    parts.append(
        f"Email to reply to:\n"
        f"From: {email.get('sender_name', '')} <{email.get('sender_email', '')}>\n"
        f"Subject: {email.get('subject', '')}\n"
        f"Body:\n{email.get('body_plain', '')}"
    )

    parts.append(
        "Write a reply in Brad's voice. "
        "Reply body only — no subject, no metadata, no salutation. "
        "If you used product information from the provided spec sheet context, "
        "add one line at the end: Source: [file_name]."
    )

    return "\n\n".join(parts)


def generate_drafts(
    emails: list[dict],
    mail_session: dict,
    crm_session: Optional[dict],
) -> list[dict]:
    """
    Generate reply drafts for eligible emails.
    Eligible: draft_needed=True AND hold_flag=False.

    Returns list of draft records (also written to today_drafts.json).
    """
    from zoho_mail_connector import fetch_thread_context, create_draft

    eligible = [
        e for e in emails
        if e.get("draft_needed") and not e.get("hold_flag")
    ]

    if not eligible:
        logger.info("No drafts needed for this run.")
        return []

    client  = Anthropic()
    profile = load_profile()
    drafts  = []

    for email in eligible:
        email_id      = email.get("id", "")
        thread_id     = email.get("threadId", "")
        subject       = email.get("subject", "")
        sender_email  = email.get("sender_email", "")
        sender_name   = email.get("sender_name", "")
        category      = email.get("category", "Other")
        crm_context   = email.get("crm_context", {})
        product_inq   = email.get("product_inquiry", False)

        logger.info(
            f"Generating draft for: '{subject}' to {sender_name} "
            f"(type: {category}, workdrive: {product_inq})"
        )

        # Fetch thread context
        thread_context = ""
        if thread_id:
            try:
                thread_context = fetch_thread_context(mail_session, thread_id)
            except Exception as e:
                logger.warning(f"Could not fetch thread context for {subject}: {e}")

        # WorkDrive retrieval for product inquiries
        workdrive_chunks = []
        if product_inq:
            brand = (
                crm_context.get("company", "")
                or email.get("sender_email", "").split("@")[-1].split(".")[0]
            )
            try:
                workdrive_chunks = retrieve_product_context(
                    brand=brand,
                    query=email.get("body_plain", "")[:500],
                )
            except Exception as e:
                logger.warning(f"WorkDrive retrieval failed: {e}")

        # Build prompts
        system_prompt = get_draft_system_prompt(category, profile)
        user_prompt   = _build_user_prompt(
            email, thread_context, crm_context, workdrive_chunks
        )

        # Call Claude
        try:
            response = client.messages.create(
                model="claude-opus-4-5",
                max_tokens=1024,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
            )
            draft_body = response.content[0].text.strip()
        except Exception as e:
            logger.error(f"Claude draft generation failed for '{subject}': {e}")
            continue

        # Create draft in Zoho Mail
        draft_id = None
        try:
            draft_id = create_draft(
                session=mail_session,
                to=sender_email,
                subject=f"Re: {subject}" if not subject.lower().startswith("re:") else subject,
                body=draft_body,
                in_reply_to=email.get("messageId"),
                thread_id=thread_id or None,
            )
        except Exception as e:
            logger.error(f"Zoho create_draft failed for '{subject}': {e}")

        # Record draft
        draft_record = {
            "draft_id":        str(draft_id) if draft_id else None,
            "draft_body":      draft_body,
            "email_id":        email_id,
            "recipient_email": sender_email,
            "recipient_name":  sender_name,
            "subject":         subject,
            "category":        category,
            "workdrive_used":  bool(workdrive_chunks),
            "timestamp":       datetime.now(timezone.utc).isoformat(),
        }
        drafts.append(draft_record)

        wd_note = f", workdrive_context: {'yes' if workdrive_chunks else 'no'}"
        logger.info(
            f"Draft created — '{subject}' to {sender_name} "
            f"(recipient_type: {category}{wd_note})"
        )

    # Persist to today_drafts.json (replaces prior run's file)
    os.makedirs("data", exist_ok=True)
    try:
        with open(TODAY_DRAFTS_FILE, "w", encoding="utf-8") as f:
            json.dump(drafts, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(drafts)} drafts to {TODAY_DRAFTS_FILE}")
    except Exception as e:
        logger.error(f"Could not write {TODAY_DRAFTS_FILE}: {e}")

    return drafts
