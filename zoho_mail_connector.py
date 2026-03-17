"""
zoho_mail_connector.py
Bormann Marketing — Email Intelligence System v3
Handles all Zoho Mail API interactions: auth, fetch, drafts, sent comparison.
"""

import os
import re
import json
import logging
import requests
import html2text
from datetime import datetime, timedelta, timezone
from typing import Optional

from retry_utils import with_retry, check_response_status

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ZOHO_ACCOUNTS_URL = "https://accounts.zoho.com/oauth/v2/token"
ZOHO_MAIL_BASE    = "https://mail.zoho.com/api/accounts"

# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def _load_mail_env() -> dict:
    """Load OAuth credentials from environment variables."""
    required = [
        "ZOHO_MAIL_CLIENT_ID",
        "ZOHO_MAIL_CLIENT_SECRET",
        "ZOHO_MAIL_REFRESH_TOKEN",
        "ZOHO_MAIL_ACCOUNT_ID",   # Zoho account ID (numeric) for API path
    ]
    creds = {}
    missing = []
    for key in required:
        val = os.environ.get(key)
        if not val:
            missing.append(key)
        creds[key] = val
    if missing:
        raise EnvironmentError(
            f"Missing required Zoho Mail env vars: {', '.join(missing)}"
        )
    return creds


def authenticate_mail() -> dict:
    """
    Load OAuth credentials and refresh access token if needed.
    Returns a session dict: {access_token, account_id}.
    """
    creds = _load_mail_env()
    resp = requests.post(ZOHO_ACCOUNTS_URL, data={
        "refresh_token": creds["ZOHO_MAIL_REFRESH_TOKEN"],
        "client_id":     creds["ZOHO_MAIL_CLIENT_ID"],
        "client_secret": creds["ZOHO_MAIL_CLIENT_SECRET"],
        "grant_type":    "refresh_token",
    }, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if "access_token" not in data:
        raise RuntimeError(f"Zoho Mail token refresh failed: {data}")

    logger.info("Zoho Mail authenticated successfully.")
    return {
        "access_token": data["access_token"],
        "account_id":   creds["ZOHO_MAIL_ACCOUNT_ID"],
    }


# ---------------------------------------------------------------------------
# Internal request helper
# ---------------------------------------------------------------------------

@with_retry(caller="zoho_mail_get")
def _mail_get(session: dict, path: str, params: Optional[dict] = None) -> dict:
    """Authenticated GET against Zoho Mail API. Retries on transient errors."""
    url = f"{ZOHO_MAIL_BASE}/{session['account_id']}/{path}"
    headers = {"Authorization": f"Zoho-oauthtoken {session['access_token']}"}
    resp = requests.get(url, headers=headers, params=params or {}, timeout=30)
    check_response_status(resp, caller="zoho_mail_get")
    return resp.json()


@with_retry(caller="zoho_mail_post")
def _mail_post(session: dict, path: str, payload: dict) -> dict:
    """Authenticated POST against Zoho Mail API. Retries on transient errors."""
    url = f"{ZOHO_MAIL_BASE}/{session['account_id']}/{path}"
    headers = {
        "Authorization": f"Zoho-oauthtoken {session['access_token']}",
        "Content-Type":  "application/json",
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    check_response_status(resp, caller="zoho_mail_post")
    return resp.json()


# ---------------------------------------------------------------------------
# Text cleaning
# ---------------------------------------------------------------------------

def _clean_body(raw_html: str) -> str:
    """Convert HTML to plain text and strip quoted reply sections."""
    h = html2text.HTML2Text()
    h.ignore_links   = True
    h.ignore_images  = True
    h.body_width     = 0
    text = h.handle(raw_html or "")

    # Remove quoted reply sections (common patterns)
    patterns = [
        r"(?m)^On .+wrote:.*$",
        r"(?m)^>.*$",
        r"(?m)^From:.*$(?:\n(?:To:|Cc:|Subject:|Date:).*$)*",
        r"(?m)^_{3,}.*$",          # underline dividers
        r"(?m)^[-]{3,}.*$",        # dash dividers
        r"(?s)--\s*\n.*",          # email signature
    ]
    for pattern in patterns:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE | re.MULTILINE)

    # Strip tracking pixel artifacts and excessive whitespace
    text = re.sub(r"\[image:.*?\]", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# ---------------------------------------------------------------------------
# Email parsing
# ---------------------------------------------------------------------------

def _parse_message(msg: dict) -> dict:
    """Normalize a Zoho message object into our standard dict."""
    return {
        "id":           msg.get("messageId") or msg.get("msgId", ""),
        "threadId":     msg.get("threadId", ""),
        "messageId":    msg.get("headerMessageId", ""),
        "subject":      msg.get("subject", ""),
        "sender_name":  msg.get("fromAddress", "").split("<")[0].strip().strip('"'),
        "sender_email": (re.search(r"<(.+?)>", msg.get("fromAddress", "")) or ["", ""])[1]
                        or msg.get("fromAddress", "").strip(),
        "date":         msg.get("receivedTime") or msg.get("sentDateInGMT", ""),
        "body_plain":   _clean_body(msg.get("content", msg.get("htmlContent", ""))),
        "has_attachment": bool(msg.get("hasAttachment", False)),
        "labels":       msg.get("folderId", ""),
        "in_reply_to":  msg.get("inReplyTo", ""),
    }


# ---------------------------------------------------------------------------
# Public fetch functions
# ---------------------------------------------------------------------------

def fetch_recent_emails(session: dict, hours: int = 24) -> list[dict]:
    """
    Fetch all emails received in the last N hours.
    Returns list of standardized email dicts.
    """
    since_ms = int(
        (datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp() * 1000
    )
    params = {
        "limit":       100,
        "start":       0,
        "sortorder":   "false",  # newest first
        "receivedTime": since_ms,
    }
    results = []
    try:
        data = _mail_get(session, "messages/view", params)
        messages = data.get("data", [])
        for msg in messages:
            parsed = _parse_message(msg)
            if parsed["date"] and int(str(parsed["date"])[:13]) >= since_ms:
                results.append(parsed)
        logger.info(f"Fetched {len(results)} recent emails (last {hours}h).")
    except Exception as e:
        logger.error(f"fetch_recent_emails failed: {e}")
    return results


def fetch_thread_context(session: dict, thread_id: str, max_messages: int = 3) -> str:
    """
    Returns last N messages in a thread as plain text for draft context.
    """
    try:
        data = _mail_get(session, f"threads/{thread_id}")
        # schema verified 2026-03-17: thread response is a flat array under data[], NOT data.messages
        messages = data.get("data", [])
        if isinstance(messages, dict):
            # Defensive: if API ever wraps it, surface the issue clearly
            logger.warning("fetch_thread_context: expected data[] array, got dict — check API response.")
            messages = []
        # Most recent first
        messages = sorted(
            messages,
            key=lambda m: m.get("receivedTime", 0),
            reverse=True
        )[:max_messages]
        parts = []
        for msg in reversed(messages):
            sender = msg.get("fromAddress", "Unknown")
            body   = _clean_body(msg.get("content", msg.get("htmlContent", "")))
            parts.append(f"[{sender}]\n{body}")
        return "\n\n---\n\n".join(parts)
    except Exception as e:
        logger.warning(f"fetch_thread_context failed for thread {thread_id}: {e}")
        return ""


def fetch_sent_emails(session: dict, days: int = 14) -> list[dict]:
    """
    Fetch Brad's sent emails from the last N days.
    Returns same dict structure as fetch_recent_emails plus recipient fields.
    """
    since_ms = int(
        (datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000
    )
    results = []
    try:
        # Zoho sent folder is typically folderId=5 or named "Sent"
        data = _mail_get(session, "messages/view", {
            "limit":     200,
            "start":     0,
            "folderId":  5,  # Sent folder — adjust if needed
            "sortorder": "false",
        })
        messages = data.get("data", [])
        for msg in messages:
            if int(str(msg.get("sentDateInGMT", 0))[:13]) < since_ms:
                continue
            parsed = _parse_message(msg)
            # Add recipient fields
            to_raw  = msg.get("toAddress", "")
            parsed["recipient_name"]  = to_raw.split("<")[0].strip().strip('"')
            parsed["recipient_email"] = (
                re.search(r"<(.+?)>", to_raw) or ["", to_raw]
            )[1].strip()
            results.append(parsed)
        logger.info(f"Fetched {len(results)} sent emails (last {days}d).")
    except Exception as e:
        logger.error(f"fetch_sent_emails failed: {e}")
    return results


def create_draft(
    session: dict,
    to: str,
    subject: str,
    body: str,
    in_reply_to: Optional[str] = None,
    thread_id: Optional[str] = None,
) -> Optional[str]:
    """
    Create a draft in Zoho Mail Drafts folder.
    Always pass in_reply_to and thread_id for replies.
    Returns draft_id on success, None on failure.
    """
    payload = {
        "toAddress": to,
        "subject":   subject,
        "content":   body,
        "mailFormat": "plaintext",
    }
    if in_reply_to:
        payload["inReplyTo"] = in_reply_to
    if thread_id:
        payload["threadId"] = thread_id

    try:
        data = _mail_post(session, "messages", payload)
        draft_id = (
            data.get("data", {}).get("messageId")
            or data.get("data", {}).get("msgId")
        )
        logger.info(f"Draft created: {draft_id}")
        return str(draft_id) if draft_id else None
    except Exception as e:
        logger.error(f"create_draft failed: {e}")
        return None


def fetch_sent_draft_comparison(
    session: dict,
    draft_ids: list[str],
    lookback_hours: int = 26,
) -> list[dict]:
    """
    NEW v3: For each draft_id, find the corresponding sent email in Brad's
    Sent folder by matching subject and recipient.
    Returns [{draft_id, original_body, sent_body, was_edited}] for each match.
    Skips gracefully if no match found.
    """
    results = []
    # Load today_drafts.json to get original bodies
    drafts_by_id = {}
    try:
        with open("today_drafts.json") as f:
            for entry in json.load(f):
                drafts_by_id[str(entry.get("draft_id"))] = entry
    except FileNotFoundError:
        logger.warning("today_drafts.json not found — skipping sent comparison.")
        return results
    except Exception as e:
        logger.error(f"Could not load today_drafts.json: {e}")
        return results

    # Fetch recent sent emails
    sent_emails = fetch_sent_emails(session, days=2)

    for draft_id in draft_ids:
        draft_entry = drafts_by_id.get(str(draft_id))
        if not draft_entry:
            continue

        original_body     = draft_entry.get("draft_body", "")
        target_recipient  = draft_entry.get("recipient_email", "").lower()
        target_subject    = (draft_entry.get("subject") or "").lower().strip()

        # Find matching sent email
        match = None
        for sent in sent_emails:
            sent_subject   = sent["subject"].lower().strip()
            sent_recipient = sent.get("recipient_email", "").lower()
            # Match on subject similarity and recipient
            subject_match = (
                target_subject in sent_subject
                or sent_subject in target_subject
                or target_subject[:30] in sent_subject
            )
            recipient_match = target_recipient == sent_recipient
            if subject_match and recipient_match:
                match = sent
                break

        if not match:
            logger.debug(f"No sent match found for draft {draft_id} — skipping.")
            continue

        sent_body  = match.get("body_plain", "")
        was_edited = original_body.strip() != sent_body.strip()

        results.append({
            "draft_id":      draft_id,
            "original_body": original_body,
            "sent_body":     sent_body,
            "was_edited":    was_edited,
        })
        logger.debug(
            f"Draft {draft_id}: was_edited={was_edited}"
        )

    logger.info(
        f"fetch_sent_draft_comparison: {len(results)} matches, "
        f"{sum(1 for r in results if r['was_edited'])} edits detected."
    )
    return results
