"""
run_pipeline.py
Bormann Marketing — Email Intelligence System v3
Main orchestrator called by GitHub Actions cron.
Runs 6:15am CT daily (11:15 UTC summer / 12:15 UTC winter).
"""

import json
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timezone

# ── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [pipeline] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", mode="a"),
    ],
)
logger = logging.getLogger(__name__)

# ── Step timer ────────────────────────────────────────────────────────────
_step_start: float = 0.0

def _step(name: str) -> None:
    global _step_start
    elapsed = time.monotonic() - _step_start if _step_start else 0
    if _step_start:
        logger.info(f"  ✓ Done in {elapsed:.1f}s")
    logger.info(f"── {name}")
    _step_start = time.monotonic()


def _load_pipeline_state() -> dict:
    try:
        with open("data/pipeline_state.json") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def _save_pipeline_state(state: dict) -> None:
    os.makedirs("data", exist_ok=True)
    try:
        with open("data/pipeline_state.json", "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logger.error(f"Could not save pipeline_state.json: {e}")


def _build_fallback_debrief(error_msg: str) -> None:
    """Write a fallback debrief HTML if something catastrophic fails."""
    now = datetime.now(timezone.utc)
    html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Debrief Error</title></head>
<body style="background:#0a1628;color:#e2e8f0;font-family:sans-serif;padding:32px;max-width:500px;margin:auto">
<h1 style="color:#C8963E">Email Intelligence — Error</h1>
<p style="color:#94a3b8;margin-top:8px">{now.strftime("%A, %B %-d, %Y · %H:%M UTC")}</p>
<div style="background:#1e2d3d;border-left:4px solid #B91C1C;padding:14px;margin-top:20px;border-radius:4px">
<p style="color:#f87171;font-weight:700">Pipeline encountered an error today.</p>
<p style="color:#94a3b8;margin-top:8px;font-size:13px">Check GitHub Actions logs for details.</p>
<pre style="color:#6b7280;font-size:11px;margin-top:10px;white-space:pre-wrap">{error_msg[:400]}</pre>
</div>
<p style="color:#374151;font-size:11px;margin-top:40px">Bormann Marketing Email Intelligence v3</p>
</body></html>"""
    try:
        with open("brad-debrief.html", "w") as f:
            f.write(html)
        logger.info("Fallback debrief written.")
    except Exception as e:
        logger.error(f"Could not write fallback debrief: {e}")


def _commit_debrief() -> None:
    """Commit brad-debrief.html to repo root via git for GitHub Pages."""
    import subprocess
    cmds = [
        ["git", "config", "user.email", "pipeline@bormannmarketing.com"],
        ["git", "config", "user.name",  "Bormann Pipeline"],
        ["git", "add",    "brad-debrief.html"],
        ["git", "commit", "-m", f"[bot] debrief {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
         "--allow-empty"],
        ["git", "push"],
    ]
    for cmd in cmds:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0 and cmd[1] not in ("commit",):
            logger.warning(f"Git command {' '.join(cmd)}: {result.stderr.strip()}")
    logger.info("brad-debrief.html committed to GitHub Pages.")


# ── Main pipeline ─────────────────────────────────────────────────────────

def main() -> None:
    run_start = time.monotonic()
    today     = datetime.now(timezone.utc)
    is_friday = today.weekday() == 4

    logger.info("=" * 60)
    logger.info(f"Bormann Email Intelligence v3 — {today.strftime('%Y-%m-%d %H:%M UTC')}")
    logger.info(f"Is Friday: {is_friday}")
    logger.info("=" * 60)

    os.makedirs("data", exist_ok=True)

    # ── Error boundary — entire pipeline ──
    global_error = None
    categorized_emails = []
    noise_report  = {}
    drafts        = []
    overdue       = []
    sentiment_alerts = []

    try:
        # ── STEP 1: Authenticate ──────────────────────────────────────
        _step("Step 1 — Authenticate")
        from zoho_mail_connector import authenticate_mail
        from zoho_crm_connector  import authenticate_crm
        from zoho_workdrive_connector import authenticate_workdrive

        mail_session = authenticate_mail()
        crm_session  = None
        try:
            crm_session = authenticate_crm()
        except Exception as e:
            logger.warning(f"CRM auth failed — CRM features disabled: {e}")

        wd_session = authenticate_workdrive()  # Returns None if not configured

        # ── STEP 2: Edit tracker ──────────────────────────────────────
        _step("Step 2 — Edit Tracker (check yesterday's drafts)")
        from edit_tracker import run_edit_tracker
        try:
            edit_stats = run_edit_tracker(mail_session)
            # Persist total_edits_since_build so debrief_builder can display it
            state = _load_pipeline_state()
            state["total_edits_since_build"] = edit_stats.get(
                "total_edits_since_last_build", 0
            )
            _save_pipeline_state(state)
        except Exception as e:
            logger.warning(f"Edit tracker failed (non-fatal): {e}")
            edit_stats = {}

        # ── STEP 3: Fetch recent emails ───────────────────────────────
        _step("Step 3 — Fetch Recent Emails")
        from zoho_mail_connector import fetch_recent_emails, fetch_sent_emails
        hours = 96 if is_friday else 24
        raw_emails = fetch_recent_emails(mail_session, hours=hours)
        logger.info(f"Fetched {len(raw_emails)} raw emails.")

        # ── STEP 4: Noise filter ──────────────────────────────────────
        _step("Step 4 — Noise Filter")
        from noise_filter import filter_noise
        filter_result = filter_noise(raw_emails)
        real_emails   = filter_result["real_emails"]
        noise_report  = filter_result["noise_report"]
        logger.info(
            f"Real: {len(real_emails)}, Noise: {noise_report.get('noise_count',0)}"
        )

        # ── STEP 5: CRM lookup per email ──────────────────────────────
        _step("Step 5 — CRM Lookup")
        if crm_session:
            from zoho_crm_connector import lookup_contact, lookup_account
            for email in real_emails:
                crm_data = lookup_contact(crm_session, email.get("sender_email",""))
                if not crm_data["found"]:
                    domain = email.get("sender_email","").split("@")[-1]
                    crm_data = lookup_account(crm_session, domain)
                email["crm_context"] = crm_data
        else:
            for email in real_emails:
                email["crm_context"] = {"found": False}
        logger.info("CRM lookups complete.")

        # ── STEP 6: Categorize ────────────────────────────────────────
        _step("Step 6 — Categorize Emails")
        from categorizer import categorize_emails
        categorized_emails = categorize_emails(real_emails)

        # ── STEP 7: Follow-up scanner + sentiment check ───────────────
        _step("Step 7 — Follow-up Scanner + Sentiment")
        from followup_scanner import scan_followups
        try:
            sent_emails = fetch_sent_emails(mail_session, days=14)
            scan_result = scan_followups(
                sent_emails,
                raw_emails,
                categorized_emails,
                mail_session,
            )
            overdue          = scan_result["overdue"]
            sentiment_alerts = scan_result["sentiment_alerts"]
        except Exception as e:
            logger.warning(f"Follow-up scanner failed (non-fatal): {e}")
            overdue = []
            sentiment_alerts = []

        # ── STEP 8: WorkDrive retrieval (handled inside draft_generator) ─
        # No separate step needed — draft_generator calls workdrive_retriever per email

        # ── STEP 9: Generate drafts ───────────────────────────────────
        _step("Step 9 — Generate Drafts")
        from draft_generator import generate_drafts
        drafts = generate_drafts(categorized_emails, mail_session, crm_session)
        logger.info(f"Generated {len(drafts)} drafts.")

        # ── STEP 10: Sent log writer ──────────────────────────────────
        _step("Step 10 — Sent Log Writer")
        from sent_log_writer import write_sent_log
        try:
            write_sent_log(drafts, crm_session)
        except Exception as e:
            logger.warning(f"Sent log writer failed (non-fatal): {e}")

        # ── STEP 11: Heat map (log daily + build on Fridays) ──────────
        _step("Step 11 — Heat Map")
        from heat_map import log_daily_activity, build_heat_map
        try:
            log_daily_activity(categorized_emails)
            if is_friday:
                build_heat_map()
        except Exception as e:
            logger.warning(f"Heat map step failed (non-fatal): {e}")

        # ── STEP 12: Build debrief ────────────────────────────────────
        _step("Step 12 — Build Debrief")
        if is_friday:
            from weekly_recap import build_weekly_recap
            build_weekly_recap(
                weekly_emails=categorized_emails,
                noise_report=noise_report,
                drafts=drafts,
                overdue=overdue,
                sentiment_alerts=sentiment_alerts,
            )
        else:
            from debrief_builder import build_debrief
            build_debrief(
                categorized_emails=categorized_emails,
                noise_report=noise_report,
                drafts=drafts,
                overdue=overdue,
                sentiment_alerts=sentiment_alerts,
                is_friday=False,
            )

        # ── STEP 13: Commit to GitHub Pages ──────────────────────────
        _step("Step 13 — Commit to GitHub Pages")
        try:
            _commit_debrief()
        except Exception as e:
            logger.warning(f"Git commit failed (non-fatal): {e}")

        # ── STEP 14: Voice profile rebuild (if flagged) ───────────────
        _step("Step 14 — Voice Profile Rebuild Check")
        state = _load_pipeline_state()
        if state.get("rebuild_profile"):
            logger.info("Rebuild flag set — running build_profile.py")
            from build_profile import build_profile
            try:
                build_profile()
            except Exception as e:
                logger.warning(f"build_profile failed (non-fatal): {e}")
        else:
            logger.info("No voice rebuild needed.")

    except Exception as e:
        global_error = traceback.format_exc()
        logger.error(f"PIPELINE FAILED: {e}")
        logger.error(global_error)
        _build_fallback_debrief(str(e))
        # Still try to commit fallback
        try:
            _commit_debrief()
        except Exception:
            pass
        sys.exit(1)

    finally:
        elapsed = time.monotonic() - run_start
        logger.info(f"Pipeline complete in {elapsed:.1f}s.")
        logger.info(
            f"Summary: {len(categorized_emails)} emails · "
            f"{noise_report.get('noise_count',0)} noise · "
            f"{len(drafts)} drafts · "
            f"{len(overdue)} overdue · "
            f"{len(sentiment_alerts)} sentiment alerts"
        )


if __name__ == "__main__":
    main()
