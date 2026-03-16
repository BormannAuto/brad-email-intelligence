"""
draft_generator.py
Bormann Marketing — Email Intelligence System v3
Generates reply drafts for emails where draft_needed=True and hold_flag=False.

Security:
- Uses api_guard for 50-call cap enforcement.
- Prompts include explicit anti-hallucination instruction (required by security audit).
- Log statements contain only categories and counts — no subjects or sender info.
- Failed drafts produce a placeholder, not a silent skip, so debrief still renders.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional
from anthropic import Anthropic
from api_guard import make_claude_call
from voice_profile import load_profile, get_draft_system_prompt
from workdrive_retriever import retrieve_product_context

logger = logging.getLogger(__name__)

TODAY_DRAFTS_FILE = "today_drafts.json"

# Placeholder body used when Claude generation fails — ensures debrief Zone 0 still renders
DRAFT_FAILED_BODY = "[AI DRAFT FAILED — MANUAL REQUIRED]\n\nDraft generation encountered an error for this email. Please reply manually."


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

    # Anti-hallucination instruction — REQUIRED by security audit
    parts.append(
        "DRAFT INSTRUCTIONS:\n"
        "Write a reply in Brad's voice. Reply body only — no subject, no metadata, no salutation.\n"
        "CRITICAL: Only reference information explicitly present in the email thread above. "
        "Do not invent names, deals, prices, product details, or context not present in the source. "
        "If you cannot draft a meaningful reply from the provided content alone, "
        "return exactly: [NEEDS CONTEXT]\n"
        "If you used product information from a provided spec sheet, "
        "add one line at the end: Source: [file_name]."
    )

    return "\n\n".join(parts)


def generate_drafts(
    emails: list[dict],
    mail_session: dict,
    crm_session: Optional[dict],
) -> list[dict]:
    """
    Generate reply drafts for eligible emails (draft_needed=True, hold_flag=False).
    Returns list of draft records. Failed drafts produce a placeholder, not a skip.
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
        email_id    = email.get("id", "")
        thread_id   = email.get("threadId", "")
        subject     = email.get("subject", "")
        sender_email = email.get("sender_email", "")
        sender_name  = email.get("sender_name", "")
        category     = email.get("category", "Other")
        crm_context  = email.get("crm_context", {})
        product_inq  = email.get("product_inquiry", False)

        # Logging: category and workdrive flag only — no subject, no sender name/email
        logger.info(
            f"Generating draft — category: {category}, "
            f"workdrive: {product_inq}"
        )

        # Fetch thread context
        thread_context = ""
        if thread_id:
            try:
                thread_context = fetch_thread_context(mail_session, thread_id)
            except Exception as e:
                logger.warning(f"fetch_thread_context failed (category: {category}): {e}")

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
        user_prompt   = _build_user_prompt(email, thread_context, crm_context, workdrive_chunks)

        # Call Claude via api_guard
        draft_body = None
        try:
            raw = make_claude_call(
                client,
                model="claude-opus-4-5",
                max_tokens=1024,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
                caller="draft_generator",
            )
            draft_body = raw.strip()
        except RuntimeError as e:
            # API cap reached
            logger.warning(f"Draft skipped — API cap: {e}")
            draft_body = DRAFT_FAILED_BODY
        except Exception as e:
            # Any other Claude failure — use placeholder, don't abort
            logger.error(f"Claude draft generation failed (category: {category}): {e}")
            draft_body = DRAFT_FAILED_BODY

        # Create draft in Zoho Mail (skip for placeholders)
        draft_id = None
        is_placeholder = draft_body == DRAFT_FAILED_BODY
        if not is_placeholder:
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
                logger.error(f"Zoho create_draft failed (category: {category}): {e}")

        draft_record = {
            "draft_id":        str(draft_id) if draft_id else None,
            "draft_body":      draft_body,
            "email_id":        email_id,
            "recipient_email": sender_email,
            "recipient_name":  sender_name,
            "subject":         subject,
            "category":        category,
            "workdrive_used":  bool(workdrive_chunks),
            "is_placeholder":  is_placeholder,
            "timestamp":       datetime.now(timezone.utc).isoformat(),
        }
        drafts.append(draft_record)
        logger.info(
            f"Draft {'placeholder' if is_placeholder else 'created'} — "
            f"category: {category}, workdrive: {'yes' if workdrive_chunks else 'no'}"
        )

    # Write today_drafts.json (overwritten each run — edit_tracker reads this next morning)
    try:
        with open(TODAY_DRAFTS_FILE, "w", encoding="utf-8") as f:
            json.dump(drafts, f, indent=2, ensure_ascii=False)
        logger.info(
            f"Saved {len(drafts)} drafts to {TODAY_DRAFTS_FILE} "
            f"({sum(1 for d in drafts if not d.get('is_placeholder'))} real, "
            f"{sum(1 for d in drafts if d.get('is_placeholder'))} placeholder)"
        )
    except Exception as e:
        logger.error(f"Could not write {TODAY_DRAFTS_FILE}: {e}")

    return drafts
