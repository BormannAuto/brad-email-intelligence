"""
followup_scanner.py
Bormann Marketing — Email Intelligence System v3
Scans sent mail for overdue follow-ups and detects manufacturer sentiment shifts.
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

CONTACT_BASELINES_FILE = "data/contact_baselines.json"

# Overdue thresholds (hours with no reply)
THRESHOLDS = {
    "Dealer-Customer": 48,
    "Manufacturer":    72,
    "default":         96,
}

# Exclusion patterns for sent emails (do not flag as awaiting reply)
EXCLUSION_SUBJECTS = [
    "fyi", "no-reply", "no reply", "out of office", "ooo",
    "unsubscribe", "re: re: re:",
]
EXCLUSION_RECIPIENT_PATTERNS = [
    "@noreply", "@no-reply", "donotreply@", "noreply@",
]
MAX_THREAD_LENGTH = 8  # threads this long don't need flagging


# ---------------------------------------------------------------------------
# Baseline helpers
# ---------------------------------------------------------------------------

def _load_baselines() -> dict:
    try:
        with open(CONTACT_BASELINES_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        logger.warning(f"Could not load contact_baselines.json: {e}")
        return {}


def _save_baselines(baselines: dict) -> None:
    os.makedirs("data", exist_ok=True)
    try:
        with open(CONTACT_BASELINES_FILE, "w") as f:
            json.dump(baselines, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not save contact_baselines.json: {e}")


def _update_baseline(baselines: dict, sender_email: str, sender_name: str,
                     brand: str, sentiment_score: float) -> None:
    """Update rolling sentiment baseline for a contact."""
    key = sender_email.lower()
    if key not in baselines:
        baselines[key] = {
            "sender_name":  sender_name,
            "brand":        brand,
            "scores":       [],
            "data_points":  0,
        }
    entry = baselines[key]
    entry["scores"].append(round(sentiment_score, 3))
    entry["scores"] = entry["scores"][-90:]  # keep last 90 data points
    entry["data_points"] = len(entry["scores"])
    entry["mean"] = round(sum(entry["scores"]) / len(entry["scores"]), 3)
    entry["last_updated"] = datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Exclusion checks
# ---------------------------------------------------------------------------

def _should_exclude(sent_email: dict, thread_length: int) -> bool:
    """Return True if this sent email should be excluded from follow-up scanning."""
    subject = (sent_email.get("subject") or "").lower()
    recipient = (sent_email.get("recipient_email") or "").lower()

    for excl in EXCLUSION_SUBJECTS:
        if excl in subject:
            return True

    for pattern in EXCLUSION_RECIPIENT_PATTERNS:
        if pattern in recipient:
            return True

    # Brad's send was itself a reply (has in_reply_to)
    if sent_email.get("in_reply_to"):
        return True

    if thread_length >= MAX_THREAD_LENGTH:
        return True

    return False


# ---------------------------------------------------------------------------
# Main scanner
# ---------------------------------------------------------------------------

def scan_followups(
    sent_emails: list[dict],
    received_emails: list[dict],
    categorized_received: list[dict],
    mail_session,
) -> dict:
    """
    Scan sent emails for overdue follow-ups.
    Also runs sentiment deviation checks against baselines.

    Args:
        sent_emails: from fetch_sent_emails()
        received_emails: all received emails from today + context
        categorized_received: today's received emails with sentiment_score
        mail_session: active Zoho Mail session for thread fetching

    Returns:
        {overdue: [...], sentiment_alerts: [...]}
    """
    from zoho_mail_connector import fetch_thread_context

    baselines = _load_baselines()
    now       = datetime.now(timezone.utc)

    # Build a set of sender emails from recently received (to detect replies)
    recent_received_senders = set(
        e.get("sender_email", "").lower()
        for e in received_emails
    )
    recent_received_subjects = [
        e.get("subject", "").lower()
        for e in received_emails
    ]

    overdue = []

    for sent in sent_emails:
        sent_time_raw = sent.get("date", 0)
        try:
            # Zoho returns timestamp in ms
            sent_dt = datetime.fromtimestamp(int(str(sent_time_raw)[:10]), tz=timezone.utc)
        except Exception:
            continue

        age_hours    = (now - sent_dt).total_seconds() / 3600
        recipient    = sent.get("recipient_email", "").lower()
        subject      = sent.get("subject", "")
        thread_id    = sent.get("threadId", "")
        category     = sent.get("category", "default")  # may not be set for sent mail

        # Infer category from sent email context if not set
        if category == "default":
            category = "Manufacturer" if sent.get("product_inquiry") else "default"

        threshold = THRESHOLDS.get(category, THRESHOLDS["default"])

        # Not overdue yet
        if age_hours < threshold:
            continue

        # Check exclusions
        if _should_exclude(sent, thread_length=3):  # rough estimate
            continue

        # Skip if recipient has replied recently (approximate check)
        if recipient in recent_received_senders:
            continue

        # Skip if a received email matches the subject (thread activity)
        if any(subject.lower()[:30] in rs for rs in recent_received_subjects):
            continue

        # Flag as overdue
        overdue.append({
            "subject":         subject,
            "recipient_email": recipient,
            "recipient_name":  sent.get("recipient_name", ""),
            "sent_date":       sent_dt.isoformat(),
            "days_since_sent": round(age_hours / 24, 1),
            "thread_id":       thread_id,
        })

    # ----------------------------------------------------------------
    # Sentiment baseline update and alert generation (NEW v3)
    # ----------------------------------------------------------------
    sentiment_alerts = []

    for email in categorized_received:
        sender_email   = email.get("sender_email", "").lower()
        sender_name    = email.get("sender_name", "")
        brand          = _infer_brand(email)
        score          = email.get("sentiment_score", 0.5)

        # Update baseline
        _update_baseline(baselines, sender_email, sender_name, brand, score)

        # Check for alert (10+ data points required)
        baseline_entry = baselines.get(sender_email, {})
        data_points    = baseline_entry.get("data_points", 0)
        baseline_mean  = baseline_entry.get("mean", 0.5)

        if data_points < 10:
            continue

        deviation = baseline_mean - score  # positive = colder than usual

        if deviation < 0.3:
            continue  # not enough deviation

        # Require at least one additional signal
        signals = []
        if deviation > 0.3:
            signals.append(f"tone colder than usual (score {score:.2f} vs baseline {baseline_mean:.2f})")
        # Could add: response time analysis, body length, etc.

        if not signals:
            continue

        alert_text = (
            f"{sender_name or sender_email} from {brand} is communicating "
            f"colder than their baseline. "
            f"Current tone: {score:.2f}, usual: {baseline_mean:.2f}. "
            f"Worth a check-in."
        )
        sentiment_alerts.append({
            "sender_name":    sender_name,
            "sender_email":   sender_email,
            "brand":          brand,
            "baseline_score": round(baseline_mean, 3),
            "current_score":  round(score, 3),
            "signals":        signals,
            "alert_text":     alert_text,
        })

    # Save updated baselines
    _save_baselines(baselines)

    logger.info(
        f"Follow-up scan: {len(overdue)} overdue, "
        f"{len(sentiment_alerts)} sentiment alerts."
    )
    return {
        "overdue":          overdue,
        "sentiment_alerts": sentiment_alerts,
    }


def _infer_brand(email: dict) -> str:
    """Attempt to infer manufacturer brand from email context."""
    crm = email.get("crm_context", {})
    company = crm.get("company", "")
    if company:
        return company
    domain = email.get("sender_email", "").split("@")[-1].split(".")[0].title()
    return domain or "Unknown"
