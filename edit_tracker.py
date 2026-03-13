"""
edit_tracker.py
Bormann Marketing — Email Intelligence System v3  (NEW)
Runs at pipeline start, compares yesterday's drafts to what Brad actually sent.
Accumulates edit_log.json and triggers voice profile rebuild after 10 edits.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

TODAY_DRAFTS_FILE  = "today_drafts.json"
EDIT_LOG_FILE      = "data/edit_log.json"
PIPELINE_STATE     = "data/pipeline_state.json"
REBUILD_THRESHOLD  = 10


def _load_edit_log() -> list[dict]:
    try:
        with open(EDIT_LOG_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    except Exception as e:
        logger.warning(f"Could not load edit_log.json: {e}")
        return []


def _save_edit_log(entries: list[dict]) -> None:
    os.makedirs("data", exist_ok=True)
    try:
        with open(EDIT_LOG_FILE, "w", encoding="utf-8") as f:
            json.dump(entries, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Could not save edit_log.json: {e}")


def _load_pipeline_state() -> dict:
    try:
        with open(PIPELINE_STATE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        logger.warning(f"Could not load pipeline_state.json: {e}")
        return {}


def _save_pipeline_state(state: dict) -> None:
    os.makedirs("data", exist_ok=True)
    try:
        with open(PIPELINE_STATE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logger.error(f"Could not save pipeline_state.json: {e}")


def _count_edits_since_last_build(entries: list[dict], last_build_date: Optional[str]) -> int:
    """Count edit entries added since the last profile build."""
    if not last_build_date:
        return len(entries)
    try:
        cutoff = datetime.fromisoformat(last_build_date)
    except Exception:
        return len(entries)

    count = 0
    for entry in entries:
        try:
            entry_dt = datetime.fromisoformat(entry.get("date", ""))
            if entry_dt > cutoff:
                count += 1
        except Exception:
            pass
    return count


def run_edit_tracker(mail_session: dict) -> dict:
    """
    Compare yesterday's drafts to Brad's actual sent emails.
    Updates edit_log.json and sets rebuild_profile flag if threshold reached.

    Returns: {edits_captured_today, total_edits_since_last_build, rebuild_triggered}
    """
    from zoho_mail_connector import fetch_sent_draft_comparison

    # Load prior day's draft IDs
    draft_ids = []
    try:
        with open(TODAY_DRAFTS_FILE) as f:
            prior_drafts = json.load(f)
        draft_ids = [str(d["draft_id"]) for d in prior_drafts if d.get("draft_id")]
    except FileNotFoundError:
        logger.info("today_drafts.json not found (first run or no drafts yesterday) — skipping edit tracking.")
        return {"edits_captured_today": 0, "total_edits_since_last_build": 0, "rebuild_triggered": False}
    except Exception as e:
        logger.warning(f"Could not load today_drafts.json: {e}")
        return {"edits_captured_today": 0, "total_edits_since_last_build": 0, "rebuild_triggered": False}

    if not draft_ids:
        logger.info("No draft IDs in today_drafts.json — skipping edit tracking.")
        return {"edits_captured_today": 0, "total_edits_since_last_build": 0, "rebuild_triggered": False}

    # Compare drafts to sent emails
    comparisons = fetch_sent_draft_comparison(mail_session, draft_ids, lookback_hours=26)
    edits_today = [c for c in comparisons if c.get("was_edited")]

    # Load existing edit log
    edit_log = _load_edit_log()

    # Append new edits with simple diff summary
    new_entries = []
    now = datetime.now(timezone.utc).isoformat()
    for edit in edits_today:
        entry = {
            "date":          now,
            "draft_id":      edit["draft_id"],
            "original_body": edit["original_body"],
            "sent_body":     edit["sent_body"],
            "diff_summary":  _summarize_diff(edit["original_body"], edit["sent_body"]),
        }
        edit_log.append(entry)
        new_entries.append(entry)

    _save_edit_log(edit_log)

    # Check rebuild threshold
    state           = _load_pipeline_state()
    last_build_date = state.get("last_profile_build_date")
    total_since_build = _count_edits_since_last_build(edit_log, last_build_date)
    rebuild_triggered = False

    if total_since_build >= REBUILD_THRESHOLD and not state.get("rebuild_profile"):
        state["rebuild_profile"] = True
        _save_pipeline_state(state)
        rebuild_triggered = True
        logger.info(
            f"Edit threshold reached ({total_since_build} edits since last build). "
            f"Voice profile rebuild scheduled after today's debrief."
        )

    logger.info(
        f"Edit tracker: {len(new_entries)} edits captured today, "
        f"{total_since_build} since last build, "
        f"rebuild_triggered={rebuild_triggered}, "
        f"next threshold at {REBUILD_THRESHOLD}."
    )
    return {
        "edits_captured_today":       len(new_entries),
        "total_edits_since_last_build": total_since_build,
        "rebuild_triggered":           rebuild_triggered,
    }


def _summarize_diff(original: str, sent: str) -> str:
    """Generate a brief human-readable diff summary."""
    orig_words = len(original.split())
    sent_words = len(sent.split())

    if sent_words < orig_words * 0.6:
        return f"Brad shortened significantly ({orig_words}→{sent_words} words)"
    elif sent_words > orig_words * 1.4:
        return f"Brad expanded ({orig_words}→{sent_words} words)"
    else:
        return f"Brad made edits ({orig_words}→{sent_words} words)"
