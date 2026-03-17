"""
noise_filter.py
Bormann Marketing — Email Intelligence System v3
Separates real emails from noise (newsletters, promos, automated mail).
Writes noise_senders_found.json on first pass for Zone 7 review.
"""

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

NOISE_SENDERS_FILE = "data/noise_senders_found.json"


def _load_config() -> dict:
    try:
        with open("email_config.json") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Could not load email_config.json: {e}")
        return {}


def _compile_patterns(patterns: list[str]) -> list[re.Pattern]:
    compiled = []
    for p in patterns:
        try:
            compiled.append(re.compile(p, re.IGNORECASE))
        except re.error as e:
            logger.warning(f"Invalid regex pattern '{p}': {e}")
    return compiled


# Known newsletter/automated sending domains
_DEFAULT_NEWSLETTER_DOMAINS = {
    "mailchimp.com", "constantcontact.com", "sendgrid.net",
    "amazonses.com", "klaviyo.com", "mailgun.org", "hubspot.com",
    "marketo.com", "salesforce.com", "pardot.com", "exacttarget.com",
    "bounce.zoho.com", "return.zoho.com",
}

# Subject patterns that are almost always noise
_DEFAULT_NOISE_SUBJECT_PATTERNS = [
    r"\bunsubscribe\b",
    r"\bnewsletter\b",
    r"\bspecial offer\b",
    r"\blimited time\b",
    r"\bfree shipping\b",
    r"\b\d+% off\b",
    r"\bpromo code\b",
    r"\bdeals? of the (day|week)\b",
    r"\bmonthly (digest|update|recap)\b",
    r"\bweekly (digest|update|recap|roundup)\b",
    r"\bnoreply\b",
    r"\bdo.not.reply\b",
    r"\bno-reply\b",
    r"\bautomated.message\b",
    r"\byour (receipt|invoice|statement|confirmation)\b",
    r"\border.confirm\b",
    r"\bshipping.confirmation\b",
    r"\bverif(y|ication).code\b",
    r"\bpassword.reset\b",
    r"\bwebinar.reminder\b",
    r"\bregistration.confirm\b",
]


def _is_noise(email: dict, blocked_domains: list, blocked_senders: list,
              subject_patterns: list[re.Pattern],
              sender_patterns: list[re.Pattern],
              newsletter_domains: set) -> tuple[bool, str]:
    """
    Determine if an email is noise.
    Returns (is_noise: bool, reason: str).
    """
    sender_email  = (email.get("sender_email") or "").lower()
    sender_domain = sender_email.split("@")[-1] if "@" in sender_email else ""
    subject       = (email.get("subject") or "").lower()
    labels        = str(email.get("labels") or "").lower()  # folderId is int from Zoho API

    # Hard block rules from config
    if sender_domain in [d.lower() for d in blocked_domains]:
        return True, f"blocked_domain:{sender_domain}"

    if sender_email in [s.lower() for s in blocked_senders]:
        return True, f"blocked_sender:{sender_email}"

    # Known newsletter/automated domains
    if sender_domain in newsletter_domains:
        return True, f"newsletter_domain:{sender_domain}"

    # Unsubscribe signal (very reliable)
    if "unsubscribe" in (email.get("body_plain") or "").lower()[:500]:
        return True, "unsubscribe_link_in_body"

    # List-ID or List-Unsubscribe headers would be checked here
    # (Zoho Mail API may not expose raw headers — approximate via body scan)
    if re.search(r"list-id|list-unsubscribe|list.help", subject):
        return True, "list_header_detected"

    # Subject patterns
    for pattern in subject_patterns:
        if pattern.search(subject):
            return True, f"subject_pattern:{pattern.pattern[:40]}"

    # Sender patterns from config
    for pattern in sender_patterns:
        if pattern.search(sender_email) or pattern.search(
            email.get("sender_name", "").lower()
        ):
            return True, f"sender_pattern:{pattern.pattern[:40]}"

    return False, ""


def filter_noise(emails: list[dict]) -> dict:
    """
    Main entry point. Separates emails into real and noise.
    Writes noise_senders_found.json on first run (Zone 7 display).

    Returns:
        {
            real_emails: [...],
            noise_emails: [...],
            noise_report: {total_received, noise_count, top_senders: [...]}
        }
    """
    config           = _load_config()
    blocked_domains  = config.get("blocked_domains", [])
    blocked_senders  = config.get("blocked_sender_patterns", [])
    subject_pats_raw = config.get("blocked_subject_patterns", []) + _DEFAULT_NOISE_SUBJECT_PATTERNS
    sender_pats_raw  = []  # could be expanded in config

    subject_patterns  = _compile_patterns(subject_pats_raw)
    sender_patterns   = _compile_patterns(sender_pats_raw)
    newsletter_domains = _DEFAULT_NEWSLETTER_DOMAINS | set(
        d.lower() for d in config.get("newsletter_domains", [])
    )

    real_emails  = []
    noise_emails = []
    noise_senders: dict[str, int] = {}

    for email in emails:
        flagged, reason = _is_noise(
            email, blocked_domains, blocked_senders,
            subject_patterns, sender_patterns, newsletter_domains
        )
        if flagged:
            email["noise_reason"] = reason
            noise_emails.append(email)
            sender = email.get("sender_email", "unknown")
            noise_senders[sender] = noise_senders.get(sender, 0) + 1
        else:
            real_emails.append(email)

    # Build noise report
    top_senders = sorted(
        [{"sender": k, "count": v} for k, v in noise_senders.items()],
        key=lambda x: x["count"],
        reverse=True
    )[:20]

    noise_report = {
        "total_received": len(emails),
        "noise_count":    len(noise_emails),
        "real_count":     len(real_emails),
        "top_senders":    top_senders,
        "generated_at":   datetime.now(timezone.utc).isoformat(),
    }

    # Write noise senders file (Zone 7 — first run only if file doesn't exist)
    _write_noise_senders(noise_senders)

    logger.info(
        f"Noise filter: {len(emails)} total → "
        f"{len(real_emails)} real, {len(noise_emails)} noise"
    )
    return {
        "real_emails":  real_emails,
        "noise_emails": noise_emails,
        "noise_report": noise_report,
    }


def _write_noise_senders(noise_senders: dict) -> None:
    """
    Write noise_senders_found.json.
    First run: creates file.  Subsequent runs: merges counts.
    """
    os.makedirs("data", exist_ok=True)
    existing = {}
    first_run = not os.path.exists(NOISE_SENDERS_FILE)

    if not first_run:
        try:
            with open(NOISE_SENDERS_FILE) as f:
                data = json.load(f)
                existing = {
                    entry["sender"]: entry["count"]
                    for entry in data.get("senders", [])
                }
        except Exception:
            pass

    # Merge counts
    merged = dict(existing)
    for sender, count in noise_senders.items():
        merged[sender] = merged.get(sender, 0) + count

    output = {
        "first_run":    first_run,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "dismissed":    False,
        "senders": sorted(
            [{"sender": k, "count": v} for k, v in merged.items()],
            key=lambda x: x["count"],
            reverse=True
        )
    }
    try:
        with open(NOISE_SENDERS_FILE, "w") as f:
            json.dump(output, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not write noise_senders_found.json: {e}")
