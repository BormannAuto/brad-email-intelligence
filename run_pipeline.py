"""
run_pipeline.py
Bormann Marketing — Email Intelligence System v3
Main orchestrator called by GitHub Actions cron.
Runs 6:15am CT daily (11:15 UTC summer / 12:15 UTC winter).

Security hardening:
- run_id idempotency: all write ops check for duplicate run before writing
- Global 20-minute timeout via threading.Timer
- SANDBOX_MODE=true bypasses all real API calls (required before credentials added)
- Traceback scrubbing: env var values stripped from logged exceptions
- Claude API call cap enforced via api_guard (50 calls per run)
"""

import json
import logging
import os
import re
import sys
import threading
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

# ── Constants ─────────────────────────────────────────────────────────────
GLOBAL_TIMEOUT_SECONDS = 1200  # 20 minutes
SANDBOX_MODE = os.environ.get("SANDBOX_MODE", "").lower() in ("true", "1", "yes")
MOCK_FAIL    = os.environ.get("MOCK_ZOHO_FAIL", "").lower() in ("true", "1", "yes")

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


def _scrub_traceback(tb: str) -> str:
    """
    Remove potential credential fragments from traceback before logging.
    Strips values of known secret env vars from the string.
    Security: prevents token leakage if an exception captures a request URL or payload.
    """
    secret_vars = [
        "ZOHO_MAIL_CLIENT_SECRET", "ZOHO_MAIL_REFRESH_TOKEN",
        "ZOHO_CRM_CLIENT_SECRET",  "ZOHO_CRM_REFRESH_TOKEN",
        "ZOHO_WORKDRIVE_CLIENT_SECRET", "ZOHO_WORKDRIVE_REFRESH_TOKEN",
        "ANTHROPIC_API_KEY",
    ]
    scrubbed = tb
    for var in secret_vars:
        val = os.environ.get(var, "")
        if val and len(val) > 8:
            scrubbed = scrubbed.replace(val, f"[{var}_REDACTED]")
    return scrubbed


def _build_fallback_debrief(error_msg: str) -> None:
    """Write a fallback debrief HTML if something catastrophic fails."""
    now = datetime.now(timezone.utc)
    # Scrub the error message too before embedding in HTML
    scrubbed_msg = _scrub_traceback(error_msg[:400])
    html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Debrief Error</title></head>
<body style="background:#0a1628;color:#e2e8f0;font-family:sans-serif;padding:32px;max-width:500px;margin:auto">
<h1 style="color:#C8963E">Email Intelligence — Error</h1>
<p style="color:#94a3b8;margin-top:8px">{now.strftime("%A, %B %-d, %Y · %H:%M UTC")}</p>
<div style="background:#1e2d3d;border-left:4px solid #B91C1C;padding:14px;margin-top:20px;border-radius:4px">
<p style="color:#f87171;font-weight:700">Pipeline encountered an error today.</p>
<p style="color:#94a3b8;margin-top:8px;font-size:13px">Check GitHub Actions logs for details.</p>
<pre style="color:#6b7280;font-size:11px;margin-top:10px;white-space:pre-wrap">{scrubbed_msg}</pre>
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
        ["git", "commit", "-m",
         f"[bot] debrief {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
         "--allow-empty"],
        ["git", "push"],
    ]
    for cmd in cmds:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0 and cmd[1] not in ("commit",):
            # Scrub stderr before logging — git output can include URLs with tokens
            logger.warning(f"Git command '{cmd[1]}': {_scrub_traceback(result.stderr.strip())}")
    logger.info("brad-debrief.html committed to GitHub Pages.")


def _check_debrief_exists() -> bool:
    """Verify brad-debrief.html was written and is non-empty."""
    try:
        size = os.path.getsize("brad-debrief.html")
        if size > 100:
            logger.info(f"Debrief verified: brad-debrief.html exists ({size} bytes).")
            return True
        else:
            logger.error(f"Debrief verification FAILED: file is only {size} bytes.")
            return False
    except FileNotFoundError:
        logger.error("Debrief verification FAILED: brad-debrief.html not found.")
        return False


def _check_duplicate_run(run_id: str) -> bool:
    """
    Return True if this run_id already ran successfully today.
    Checks pipeline_state.json for last_run_id.
    """
    state = _load_pipeline_state()
    last_run = state.get("last_run_id", "")
    if last_run == run_id:
        logger.warning(
            f"Duplicate run detected: run_id {run_id} already completed. "
            f"Aborting to prevent duplicate drafts and CRM notes."
        )
        return True
    return False


def _mark_run_complete(run_id: str) -> None:
    """Record this run_id as completed in pipeline_state.json."""
    state = _load_pipeline_state()
    state["last_run_id"] = run_id
    state["last_run_completed_at"] = datetime.now(timezone.utc).isoformat()
    _save_pipeline_state(state)


# ── Sandbox mode ──────────────────────────────────────────────────────────

def _load_sandbox_mail() -> list[dict]:
    """
    Load mock email data for sandbox mode.
    Reads the 'parsed_emails' key — pre-parsed email dicts matching the output
    of zoho_mail_connector._parse_message(). The file also contains raw API
    response examples under other keys (auth_response, etc.) for reference.
    """
    try:
        with open("tests/mock_zoho_mail.json") as f:
            data = json.load(f)
        emails = data.get("parsed_emails", [])
        if not isinstance(emails, list):
            logger.error("[SANDBOX] mock_zoho_mail.json 'parsed_emails' is not a list — using empty list.")
            return []
        logger.info(f"[SANDBOX] Loaded {len(emails)} mock emails.")
        return emails
    except FileNotFoundError:
        logger.warning("[SANDBOX] tests/mock_zoho_mail.json not found — using empty list.")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"[SANDBOX] mock_zoho_mail.json is malformed: {e} — using empty list.")
        return []


# ── Sandbox helpers ───────────────────────────────────────────────────────

def _sandbox_categorize(emails: list[dict]) -> list[dict]:
    """
    Return mock categorized emails for sandbox mode.
    Loads mock Claude responses from tests/mock_claude.json if available,
    otherwise applies sensible defaults so the rest of the pipeline exercises
    real code paths (debrief builder, heat map, etc.).
    """
    mock_categories = {}
    try:
        with open("tests/mock_claude.json") as f:
            mock_data = json.load(f)
        mock_categories = mock_data.get("categorizer", {})
    except Exception:
        pass

    CATEGORY_DEFAULTS = {
        "category": "Manufacturer-Principal",
        "urgency": 3,
        "draft_needed": True,
        "hold_flag": False,
        "sentiment_score": 0.7,
        "product_inquiry": False,
        "follow_up_needed": False,
        "summary": "[SANDBOX] Mock categorization",
    }

    result = []
    for email in emails:
        msg_id = email.get("id", "")
        mock_str = mock_categories.get(msg_id)
        if mock_str:
            try:
                cat = json.loads(mock_str)
            except Exception:
                cat = {}
        else:
            cat = {}
        merged = {**CATEGORY_DEFAULTS, **cat}
        enriched = {**email, **merged}
        result.append(enriched)

    logger.info(f"[SANDBOX] Categorized {len(result)} emails with mock data.")
    return result


def _sandbox_generate_drafts(categorized_emails: list[dict]) -> list[dict]:
    """
    Return mock drafts for sandbox mode.
    Loads mock draft text from tests/mock_claude.json if available.
    Only generates drafts for emails where draft_needed=True and hold_flag=False.
    """
    mock_drafts = {}
    try:
        with open("tests/mock_claude.json") as f:
            mock_data = json.load(f)
        mock_drafts = mock_data.get("draft_generator", {})
    except Exception:
        pass

    drafts = []
    for email in categorized_emails:
        if not email.get("draft_needed") or email.get("hold_flag"):
            continue
        msg_id = email.get("id", "")
        draft_body = mock_drafts.get(msg_id) or (
            f"[SANDBOX DRAFT] This is a mock draft for email {msg_id}.\n\n"
            f"Category: {email.get('category', 'unknown')}\n\n"
            "Brad"
        )
        drafts.append({
            "email_id":    msg_id,
            "draft_id":    f"SANDBOX_DRAFT_{msg_id}",
            "draft_body":  draft_body,
            "category":    email.get("category", "unknown"),
            "subject":     email.get("subject", ""),
            "recipient_email": email.get("sender_email", ""),
        })

    logger.info(f"[SANDBOX] Generated {len(drafts)} mock drafts.")
    return drafts


# ── Global timeout ────────────────────────────────────────────────────────

_timeout_fired = False

def _global_timeout_handler() -> None:
    global _timeout_fired
    _timeout_fired = True
    logger.error(
        f"GLOBAL TIMEOUT: Pipeline exceeded {GLOBAL_TIMEOUT_SECONDS // 60} minutes. "
        f"Aborting to prevent indefinite hang."
    )
    _build_fallback_debrief("Pipeline exceeded 20-minute global timeout.")
    try:
        _commit_debrief()
    except Exception:
        pass
    # Force exit from any blocking operation
    os._exit(1)


# ── Main pipeline ─────────────────────────────────────────────────────────

def main() -> None:
    run_start = time.monotonic()
    today     = datetime.now(timezone.utc)
    run_id    = today.strftime("%Y-%m-%d")
    is_friday = today.weekday() == 4

    # ── Global timeout watchdog ──
    timeout_timer = threading.Timer(GLOBAL_TIMEOUT_SECONDS, _global_timeout_handler)
    timeout_timer.daemon = True
    timeout_timer.start()

    logger.info("=" * 60)
    logger.info(f"Bormann Email Intelligence v3 — {today.strftime('%Y-%m-%d %H:%M UTC')}")
    logger.info(f"run_id: {run_id} | is_friday: {is_friday} | SANDBOX_MODE: {SANDBOX_MODE}")
    logger.info("=" * 60)

    if SANDBOX_MODE:
        logger.info("[SANDBOX MODE ACTIVE] No real APIs will be called.")

    os.makedirs("data", exist_ok=True)

    # ── Idempotency check ──
    if not SANDBOX_MODE and _check_duplicate_run(run_id):
        timeout_timer.cancel()
        sys.exit(0)  # Clean exit — not an error

    # ── Reset Claude API call counter ──
    from api_guard import reset_counter
    reset_counter()

    categorized_emails = []
    noise_report  = {}
    drafts        = []
    overdue       = []
    sentiment_alerts = []

    try:
        # ── STEP 1: Authenticate ──────────────────────────────────────
        _step("Step 1 — Authenticate")
        if SANDBOX_MODE:
            if MOCK_FAIL:
                raise RuntimeError("MOCK_ZOHO_FAIL: simulated Zoho Mail authentication failure.")
            mail_session = {"access_token": "SANDBOX_TOKEN", "account_id": "SANDBOX_ID"}
            crm_session  = {"access_token": "SANDBOX_CRM_TOKEN"}
            wd_session   = None
            logger.info("[SANDBOX] Using mock sessions.")
        else:
            from zoho_mail_connector import authenticate_mail
            from zoho_crm_connector  import authenticate_crm
            from zoho_workdrive_connector import authenticate_workdrive

            from alert_writer import alert_critical
            try:
                mail_session = authenticate_mail()
            except Exception as auth_exc:
                alert_critical(
                    "auth_failure",
                    "Zoho Mail token refresh failed — pipeline cannot start. "
                    "Check ZOHO_MAIL_CLIENT_ID, ZOHO_MAIL_CLIENT_SECRET, and "
                    "ZOHO_MAIL_REFRESH_TOKEN in GitHub Secrets.",
                    run_id=run_id,
                )
                raise
            crm_session  = None
            try:
                crm_session = authenticate_crm()
            except Exception as e:
                logger.warning(f"CRM auth failed — CRM features disabled: {e}")
            wd_session = authenticate_workdrive()

        # ── STEP 2: Edit tracker ──────────────────────────────────────
        _step("Step 2 — Edit Tracker")
        if not SANDBOX_MODE:
            from edit_tracker import run_edit_tracker
            try:
                edit_stats = run_edit_tracker(mail_session)
                state = _load_pipeline_state()
                state["total_edits_since_build"] = edit_stats.get(
                    "total_edits_since_last_build", 0
                )
                _save_pipeline_state(state)
            except Exception as e:
                logger.warning(f"Edit tracker failed (non-fatal): {e}")
        else:
            logger.info("[SANDBOX] Edit tracker skipped.")

        # ── STEP 3: Fetch emails ──────────────────────────────────────
        _step("Step 3 — Fetch Recent Emails")
        if SANDBOX_MODE:
            raw_emails = _load_sandbox_mail()
        else:
            from zoho_mail_connector import fetch_recent_emails
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
            f"Real: {len(real_emails)}, Noise: {noise_report.get('noise_count', 0)}"
        )

        # ── STEP 5: CRM lookup ────────────────────────────────────────
        _step("Step 5 — CRM Lookup")
        if crm_session and not SANDBOX_MODE:
            from zoho_crm_connector import lookup_contact, lookup_account
            for email in real_emails:
                crm_data = lookup_contact(crm_session, email.get("sender_email", ""))
                if not crm_data["found"]:
                    domain = email.get("sender_email", "").split("@")[-1]
                    crm_data = lookup_account(crm_session, domain)
                email["crm_context"] = crm_data
        else:
            for email in real_emails:
                email["crm_context"] = {"found": False}
        logger.info("CRM lookups complete.")

        # ── STEP 6: Categorize ────────────────────────────────────────
        _step("Step 6 — Categorize")
        if SANDBOX_MODE:
            categorized_emails = _sandbox_categorize(real_emails)
        else:
            from categorizer import categorize_emails
            categorized_emails = categorize_emails(real_emails)

        # ── STEP 7: Follow-up scanner ─────────────────────────────────
        _step("Step 7 — Follow-up Scanner + Sentiment")
        if not SANDBOX_MODE:
            from zoho_mail_connector import fetch_sent_emails
            from followup_scanner import scan_followups
            try:
                sent_emails = fetch_sent_emails(mail_session, days=14)
                scan_result = scan_followups(
                    sent_emails, raw_emails, categorized_emails, mail_session
                )
                overdue          = scan_result["overdue"]
                sentiment_alerts = scan_result["sentiment_alerts"]
            except Exception as e:
                logger.warning(f"Follow-up scanner failed (non-fatal): {e}")
        else:
            logger.info("[SANDBOX] Follow-up scanner skipped.")

        # ── STEP 9: Generate drafts ───────────────────────────────────
        _step("Step 9 — Generate Drafts")
        if SANDBOX_MODE:
            drafts = _sandbox_generate_drafts(categorized_emails)
        else:
            from draft_generator import generate_drafts
            drafts = generate_drafts(categorized_emails, mail_session, crm_session)
        logger.info(f"Generated {len(drafts)} drafts.")

        # ── STEP 10: Sent log writer ──────────────────────────────────
        _step("Step 10 — Sent Log Writer")
        if SANDBOX_MODE:
            logger.info("[SANDBOX] Sent log writer skipped — no Claude call in sandbox.")
        else:
            from sent_log_writer import write_sent_log
            try:
                write_sent_log(drafts, crm_session, run_id=run_id)
            except Exception as e:
                logger.warning(f"Sent log writer failed (non-fatal): {e}")

        # ── STEP 11: Heat map ─────────────────────────────────────────
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

        # Verify debrief was written non-empty
        if not _check_debrief_exists():
            raise RuntimeError("Debrief write verification failed — file missing or empty.")

        # ── STEP 13: Commit to GitHub Pages ──────────────────────────
        _step("Step 13 — Commit to GitHub Pages")
        if not SANDBOX_MODE:
            try:
                _commit_debrief()
            except Exception as e:
                logger.warning(f"Git commit failed (non-fatal): {e}")
        else:
            logger.info("[SANDBOX] Git commit skipped — output in tests/sandbox_output/ (if configured).")

        # ── STEP 14: Voice profile rebuild ───────────────────────────
        _step("Step 14 — Voice Profile Rebuild Check")
        state = _load_pipeline_state()
        if state.get("rebuild_profile") and not SANDBOX_MODE:
            logger.info("Rebuild flag set — running build_profile.py")
            from build_profile import build_profile
            try:
                build_profile()
            except Exception as e:
                logger.warning(f"build_profile failed (non-fatal): {e}")
        else:
            logger.info("No voice rebuild needed.")

        # ── Mark run complete (idempotency) ───────────────────────────
        if not SANDBOX_MODE:
            _mark_run_complete(run_id)

    except Exception as e:
        global_error = traceback.format_exc()
        scrubbed     = _scrub_traceback(global_error)
        logger.error(f"PIPELINE FAILED: {e}")
        logger.error(f"Traceback (scrubbed):\n{scrubbed}")
        _build_fallback_debrief(str(e))
        if not SANDBOX_MODE:
            try:
                _commit_debrief()
            except Exception:
                pass
        timeout_timer.cancel()
        sys.exit(1)

    finally:
        timeout_timer.cancel()
        elapsed = time.monotonic() - run_start

        from api_guard import claude_cap_status
        cap = claude_cap_status()

        if cap.get("cap_reached") and not SANDBOX_MODE:
            from alert_writer import alert_high
            alert_high(
                "cap_reached",
                f"Claude API cap reached this run: {cap['calls_made']}/{cap['cap']} calls used. "
                f"Some drafts may be missing. Investigate if this fires regularly.",
                run_id=run_id,
            )

        logger.info(f"Pipeline complete in {elapsed:.1f}s.")
        logger.info(
            f"Summary: {len(categorized_emails)} emails · "
            f"{noise_report.get('noise_count', 0)} noise · "
            f"{len(drafts)} drafts · "
            f"{len(overdue)} overdue · "
            f"{len(sentiment_alerts)} sentiment alerts · "
            f"Claude calls: {cap['calls_made']}/{cap['cap']}"
        )
        if SANDBOX_MODE:
            logger.info("[SANDBOX MODE COMPLETE]")


if __name__ == "__main__":
    main()
