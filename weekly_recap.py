"""
weekly_recap.py
Bormann Marketing — Email Intelligence System v3
Friday detection + weekly recap logic.
Called from run_pipeline.py when today is Friday.
Fetches Mon-Thu emails, summarizes via Claude, routes to debrief_builder with is_friday=True.
"""

import json
import logging
from datetime import datetime, timezone
from anthropic import Anthropic

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are summarizing a week of email activity for Brad, the owner of Bormann Marketing, \
a manufacturer's rep firm in Minnesota covering pro AV and low-voltage products. \
Analyze the email list and return a JSON object with these keys:

- "resolved_items": list of strings — things that appear to be done/closed out this week (max 5)
- "open_follow_ups": list of strings — things still needing attention (max 5)
- "brand_highlights": list of {brand, note} — notable activity from specific manufacturers (max 4)
- "recurring_issues": list of strings — patterns or recurring problems spotted (max 3)
- "week_summary": string — 2-3 sentence plain-English summary of the week's email landscape

Be specific. Use names and companies where visible. Sound like a knowledgeable assistant, not a system log."""


def build_weekly_recap(
    weekly_emails: list[dict],
    noise_report: dict,
    drafts: list[dict],
    overdue: list[dict],
    sentiment_alerts: list[dict],
) -> None:
    """
    Build the Friday weekly debrief.
    Calls Claude to summarize the week, then delegates to debrief_builder.
    """
    from debrief_builder import build_debrief

    logger.info("=== Building Friday Weekly Recap ===")

    # Summarize with Claude
    recap_data = _summarize_week(weekly_emails)

    # Inject recap into pipeline state for debrief_builder to use
    try:
        state = {}
        try:
            with open("data/pipeline_state.json") as f:
                state = json.load(f)
        except FileNotFoundError:
            pass
        state["weekly_recap"] = recap_data
        import os; os.makedirs("data", exist_ok=True)
        with open("data/pipeline_state.json", "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not save weekly recap to pipeline_state.json: {e}")

    # Build debrief with Friday flag
    build_debrief(
        categorized_emails=weekly_emails,
        noise_report=noise_report,
        drafts=drafts,
        overdue=overdue,
        sentiment_alerts=sentiment_alerts,
        is_friday=True,
    )
    logger.info("Friday debrief built successfully.")


def _summarize_week(emails: list[dict]) -> dict:
    """Call Claude to summarize the week's email activity."""
    if not emails:
        return {}

    client = Anthropic()

    # Build compact summary of week's emails
    email_summaries = []
    for e in emails[:80]:  # cap at 80 to stay within token budget
        email_summaries.append({
            "subject":      e.get("subject", ""),
            "sender":       e.get("sender_name", ""),
            "company":      e.get("crm_context", {}).get("company", ""),
            "category":     e.get("category", ""),
            "urgency":      e.get("urgency", 3),
            "action":       e.get("action_summary", ""),
            "draft_sent":   e.get("draft_needed", False),
        })

    user_message = (
        f"Here are this week's emails ({len(emails)} total):\n\n"
        f"{json.dumps(email_summaries, indent=2)}\n\n"
        f"Summarize the week."
    )

    try:
        response = client.messages.create(
            model="claude-opus-4-5",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )
        raw = response.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw.strip())
    except json.JSONDecodeError as e:
        logger.error(f"Weekly recap: Claude returned invalid JSON: {e}")
        return {}
    except Exception as e:
        logger.error(f"Weekly recap: Claude API call failed: {e}")
        return {}
