"""
categorizer.py
Bormann Marketing — Email Intelligence System v3
Single batched Claude API call to categorize all real emails with:
urgency, category, action, draft flags, sentiment, product inquiry detection.
"""

import json
import logging
from anthropic import Anthropic

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Category definitions (matches debrief zones)
# ---------------------------------------------------------------------------
CATEGORIES = [
    "Manufacturer",          # manufacturer reps, principals, brand managers
    "Dealer-Customer",       # dealers, integrators, end customers
    "Internal-Team",         # Bormann staff
    "Executive-Sensitive",   # sensitive relationship, leadership, legal-adjacent
    "Business-Dev",          # new opportunities, prospecting
    "Legal-Financial",       # contracts, billing, compliance
    "Personal",              # personal email unrelated to business
    "Other",                 # doesn't fit above
]

SYSTEM_PROMPT = """You are an email categorization assistant for Brad, the owner of Bormann Marketing, \
an 8-person manufacturer's rep firm in Minnesota covering pro AV and low-voltage products for the \
MN/ND/SD/WI territory. Brad's principals include manufacturers like Shure, Biamp, Legrand, Middle Atlantic, \
and similar pro AV and low-voltage brands. His customers are AV integrators, electrical contractors, \
dealers, and end-user accounts.

Analyze each email and respond only in JSON, in the exact schema specified.

Categories:
- Manufacturer: from a brand/principal Brad represents, or a manufacturer contact
- Dealer-Customer: from an integrator, dealer, contractor, or end-customer account
- Internal-Team: from Bormann staff (bormannmarketing.com domain or known team members)
- Executive-Sensitive: sensitive relationship dynamics, legal-adjacent, executive-level contact, anything Brad should handle personally
- Business-Dev: new leads, prospecting, partnership inquiries
- Legal-Financial: contracts, billing disputes, compliance, insurance
- Personal: personal email unrelated to Bormann Marketing business
- Other: does not fit above categories

Urgency levels:
1 = Must handle today — contract, active deal, angry customer, manufacturer issue
2 = Should handle today — pending quote, customer question, follow-up needed
3 = This week — informational, low-pressure, FYI
4 = Low priority — newsletters Brad actually reads, industry updates
5 = Noise — should have been filtered but wasn't

draft_needed: true ONLY for Manufacturer, Dealer-Customer, Internal-Team AND urgency 1 or 2.
NEVER true for Executive-Sensitive, Business-Dev, Legal-Financial, Personal.

hold_flag: true if Executive-Sensitive, or if you detect sensitive relationship dynamics
(frustrated customer, manufacturer escalation, personal/private matter).

sentiment_score: float 0.0–1.0. 1.0 = very warm and positive. 0.0 = cold, formal, clipped, or adversarial.
Base this on the language, tone, and formality of the email — not just the content.

product_inquiry: true if the email asks about product specs, pricing, availability, compatibility, or configuration.
Used to trigger WorkDrive product document retrieval."""


def categorize_emails(emails: list[dict]) -> list[dict]:
    """
    Categorize a list of real emails using a single batched Claude API call.
    Returns emails with categorization fields added.
    """
    if not emails:
        return []

    client = Anthropic()

    # Build a compact batch payload (avoid sending full bodies to save tokens)
    email_batch = []
    for i, email in enumerate(emails):
        body_preview = (email.get("body_plain") or "")[:800]
        email_batch.append({
            "index":          i,
            "subject":        email.get("subject", ""),
            "sender_name":    email.get("sender_name", ""),
            "sender_email":   email.get("sender_email", ""),
            "body_preview":   body_preview,
            "crm_company":    email.get("crm_context", {}).get("company", ""),
            "crm_found":      email.get("crm_context", {}).get("found", False),
        })

    user_message = (
        "Categorize each email in the list below. "
        "Return a JSON array with one object per email, in the same order.\n\n"
        "Required fields for each object:\n"
        '{"index": int, "category": str, "urgency": int (1-5), '
        '"action_type": "reply|forward|call|review|no_action", '
        '"action_summary": str (max 15 words), '
        '"task": str (max 20 words, imperative — e.g. \'Send updated pricing to John\'), '
        '"draft_needed": bool, '
        '"hold_flag": bool, '
        '"crm_relevant": bool, '
        '"reason": str (max 15 words explaining category/urgency), '
        '"sentiment_score": float, '
        '"product_inquiry": bool}\n\n'
        f"Emails:\n{json.dumps(email_batch, indent=2)}"
    )

    try:
        response = client.messages.create(
            model="claude-opus-4-5",
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )
        raw = response.content[0].text.strip()

        # Extract JSON array (handle markdown code fences)
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        results = json.loads(raw)

    except json.JSONDecodeError as e:
        logger.error(f"Claude categorization returned invalid JSON: {e}")
        results = []
    except Exception as e:
        logger.error(f"Claude categorization API call failed: {e}")
        results = []

    # Map results back to emails
    results_by_index = {r["index"]: r for r in results if isinstance(r, dict)}

    categorized = []
    for i, email in enumerate(emails):
        cat = results_by_index.get(i, {})
        email["category"]       = cat.get("category", "Other")
        email["urgency"]        = cat.get("urgency", 3)
        email["action_type"]    = cat.get("action_type", "review")
        email["action_summary"] = cat.get("action_summary", "")
        email["task"]           = cat.get("task", "")
        email["draft_needed"]   = cat.get("draft_needed", False)
        email["hold_flag"]      = cat.get("hold_flag", False)
        email["crm_relevant"]   = cat.get("crm_relevant", False)
        email["reason"]         = cat.get("reason", "")
        email["sentiment_score"] = float(cat.get("sentiment_score", 0.5))
        email["product_inquiry"] = cat.get("product_inquiry", False)
        categorized.append(email)

    urgency_dist = {}
    for e in categorized:
        u = e.get("urgency", "?")
        urgency_dist[u] = urgency_dist.get(u, 0) + 1

    logger.info(
        f"Categorized {len(categorized)} emails. "
        f"Urgency dist: {dict(sorted(urgency_dist.items()))}. "
        f"Drafts needed: {sum(1 for e in categorized if e.get('draft_needed'))}. "
        f"Hold flags: {sum(1 for e in categorized if e.get('hold_flag'))}."
    )
    return categorized
