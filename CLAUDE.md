# CLAUDE.md — Bormann Email Intelligence System
# Read this file completely before touching any code.
# Last updated: 2026-03-16

---

## Section 1 — What This Project Is

The Bormann Email Intelligence System is a private email automation pipeline
for Brad at Bormann Marketing — an 8-person manufacturer's rep firm covering
MN, ND, SD, and Western WI in the pro AV/low-voltage industry.

The system does five things:

1. Fetches Brad's emails from Zoho Mail every morning at 6:15am CT via GitHub Actions cron
2. Categorizes them, scores urgency, and detects follow-up needs
3. Generates ready-to-send draft replies in Brad's voice for appropriate categories
4. Builds a mobile-optimized morning debrief (brad-debrief.html) committed to GitHub Pages
5. Maintains a Sent Log, voice profile, relationship heat map, and manufacturer
   sentiment alerts over time

This is NOT a SaaS product. It is a private, single-tenant system running on
GitHub Actions (free tier). It processes confidential business email. Treat all
email content as sensitive.

---

## Section 2 — Architecture Overview

HOSTING:
  - GitHub Actions (free tier) — all pipeline logic runs here
  - GitHub Pages — serves brad-debrief.html (PIN-gated, committed to repo root)
  - No VPS, no server to maintain
  - All secrets stored in GitHub repository Secrets — NEVER in any file in the repo

TRIGGER:
  - GitHub Actions cron: `15 11 * * *` = 6:15am CDT (summer)
  - Update to `15 12 * * *` Nov–Mar (CST = UTC-6)
  - DST transitions: 2nd Sunday March, 1st Sunday November — add calendar reminder
  - workflow_dispatch available for manual testing

EXTERNAL APIS (three separate OAuth flows — never mix credentials):
  - Zoho Mail API      scope: ZohoMail.messages.ALL
  - Zoho CRM API       scope: ZohoCRM.modules.READ + ZohoCRM.modules.CREATE (Notes)
  - Zoho WorkDrive     scope: WorkDrive.files.READ  [Phase 5 — defer until Phase 1 runs 2+ weeks]
  - Claude API         Anthropic — drafts, categorization, Sent Log
                       HARD CAP: 50 calls per pipeline run. Every call must have explicit max_tokens.

PIPELINE ORDER (run_pipeline.py orchestrates all of these in sequence):
  1.  authenticate Mail + CRM + WorkDrive (three separate OAuth refreshes)
  2.  edit_tracker.py          — compare sent folder to yesterday's stored drafts  [NEW v3]
  3.  fetch_recent_emails()    — 24h normal, 96h on Fridays
  4.  noise_filter.py          — remove newsletters, auto-replies, noise
  5.  CRM lookup per email     — zoho_crm_connector.py
  6.  categorizer.py           — 8 categories, urgency scoring, sentiment score, product_inquiry
  7.  followup_scanner.py      — no-reply detection, overdue flags, sentiment baseline alerts
  8.  workdrive_retriever.py   — keyword search against workdrive_index.json (per product_inquiry)
  9.  draft_generator.py       — Claude API, category-aware prompts, WorkDrive context
  10. sent_log_writer.py       — first-person past tense Sent Log entry + CRM note
  11. heat_map.py              — log daily activity; build heat map on Fridays  [NEW v3]
  12. debrief_builder.py       — assemble all zones into brad-debrief.html
  13. git push                 — commit brad-debrief.html to GitHub Pages
  14. build_profile.py         — ONLY if rebuild_profile flag is set in pipeline_state.json

SEPARATE WEEKLY WORKFLOW:
  - workdrive_index.yml        — Sundays 6am CT, runs workdrive_indexer.py
  - Rebuilds workdrive_index.json from all WorkDrive brand folders

KEY FILES:
  brad_voice_profile.json        — Brad's voice. Version-controlled. Human-reviewed.
                                   Never written during a normal pipeline run.
  email_config.json              — Brand names, blocked senders, priority contacts, noise rules.
                                   Read-only at runtime — never modified by pipeline.
  data/sent_log.json             — Rolling 30-day log of what Brad communicated
  today_drafts.json              — Today's generated drafts (overwritten each run)
  data/edit_log.json             — Accumulated edit diffs for voice profile rebuild
  data/pipeline_state.json       — Run state: rebuild_profile flag, last build date, edit count
  data/pipeline_drift_config.json — Operational health baselines [UNCALIBRATED first 14 days]
  SECURITY_AUDIT.md              — Security audit findings and architecture decisions
  SECURITY_LOCK.md               — Go/no-go gate for real credentials (do not create prematurely)

---

## Section 3 — Locked Decisions (Do Not Relitigate)

These are final. If something comes up that seems to contradict one of these,
document the reason before changing anything.

RUNNER: GitHub Actions free tier. NOT a VPS (changed from v2). NOT Zoho Flow.
  The GitHub Actions approach is the right call for v3 — no server maintenance,
  free cron, secrets management built in.

DEBRIEF SERVING: GitHub Pages (committed to repo root). PIN-gated in HTML/JS.
  Not a VPS nginx server. PIN is a convenience gate, not a security perimeter.

CRM UPDATES: Fill-blanks only. Never overwrite manually entered CRM data.
  Unknown contacts: don't auto-create stubs. Log and skip.

VOICE PROFILE: Built from Brad's sent emails (samples in voice/samples/).
  Updated monthly by build_profile.py — MANUAL trigger only.
  Requires human review of 3 sample drafts before any updated profile goes live.
  Never updated during a normal pipeline run (only when rebuild_profile flag is set
  AND even then, runs at end of pipeline AFTER debrief is already committed).

EXECUTIVE EMAILS: HOLD always. Never auto-draft.
  Configured via hold_flag logic in categorizer.py.

GO-LIVE DAY: Tuesday or Wednesday only. Never Monday.

IDEMPOTENCY: run_id = YYYY-MM-DD. All writes check for existing run_id first.
  No duplicate drafts. No duplicate CRM notes. No duplicate Sent Log entries.

TIMESTAMPS: UTC everywhere. No local time. No mixed timezone handling.
  Any timestamp not explicitly UTC is a bug.

TOKENS: Refresh at the START of every run. Never cache between runs.
  Three separate OAuth flows — Mail, CRM, WorkDrive. Never mix credentials.

LOGGING: Operational metadata only. Never log email content, subject lines,
  sender addresses, contact names, or draft content. Log counts and categories only.

CLAUDE API CAP: Hard limit of 50 Claude API calls per pipeline run.
  Every Claude call must have explicit max_tokens set.
  If cap reached: log warning, skip remaining AI content, do not abort.

WORKDRIVE PHASE: Do not enable WorkDrive RAG until Phase 1 has run 2+ weeks.
  WorkDrive is gracefully disabled when env vars are not set.

BUILD ORDER: Core pipeline first. WorkDrive RAG (Phase 5) after 2 weeks.
  Do not make shortcuts that would be difficult to undo later.

---

## Section 4 — Email Categories

The categorizer assigns every email to exactly one of these 8 categories.

DRAFT POLICY:
  YES  = generate a ready-to-send draft reply
  HOLD = surface in Zone 2 with purple badge, never auto-draft
  NO   = categorize and surface in summary only, no draft

  Manufacturer / Principal    YES   — primary revenue relationships
  Dealer / Customer           YES   — dealer and end-customer communications
  Internal Team               YES   — Bormann staff emails
  Executive / Sensitive       HOLD  — never auto-draft under any condition
  Business Dev / External     NO    — prospecting, cold outreach
  Legal / Financial           NO    — contracts, invoices, compliance
  Personal                    NO    — personal emails
  Admin / Noise               NO    — newsletters, auto-replies, spam

draft_needed=True ONLY for Manufacturer, Dealer-Customer, Internal-Team AND urgency 1 or 2.
hold_flag=True for ALL Executive-Sensitive emails.

---

## Section 5 — Debrief Structure

Morning Debrief (brad-debrief.html) — 8 zones.
Brad reads top-to-bottom on his phone. Total read time: 60 seconds.

Zone 0  — The Brief (GREEN)     Context checkpoint before drafts. Shows only
                                 when drafts are queued. What the email is about.
                                 What position Brad is committing to by sending.
                                 "Does this match what you remember?"

Zone 1  — Header                Date, total email count, brand name flags.

Zone 2  — Top 3 Today           Hard cap: 3 items. Urgency 1 only.
                                 HOLD items shown with purple badge.

Zone 2.5 — On Your Radar        Urgency 2 items (cap 7) + overdue follow-ups.
                                 NO REPLY badge for threads with no response.
                                 TONE SHIFT badge for sentiment alert detections.  [NEW v3]

Zone 3  — CRM Pulse             Record thumbnails for Zone 2/2.5 contacts.

Zone 4  — Drafts Ready          GOLD button linking to Zoho Mail Drafts folder.
                                 Draft count shown. Never renders draft content inline.

Zone 5  — Email Summary         Collapsible by category.
                                 Manufacturer and Dealer pinned first.

Zone 6  — Task List + Sent Log  Checkboxes for action items.
                                 Collapsible Sent Log: "What I communicated recently"

Zone 7  — Inbox Noise           First run only. Dismiss button.

FRIDAY ONLY: Zones 2+2.5 replaced by Week in Review.
  Includes Brand Health heat map (Up/Stable/Down Cooling per manufacturer).  [NEW v3]
  Includes weekly Sent Log summary.
  Voice model update line showing edit count and rebuild status.  [NEW v3]

---

## Section 6 — Build Status

Update this section after each session.

PHASE 0 — Repo Setup
  [COMPLETE] GitHub repo created
  [PENDING]  SSH confirmed, Python verified in Actions
  [PENDING]  GitHub Secrets populated (waiting on Zoho OAuth + Anthropic key from Brad)
  [PENDING]  .gitignore confirmed

PHASE 1 — Core Pipeline (Debrief + Smart Drafts)
  [COMPLETE]  All 17 core Python modules built (session 2026-03-16)
  [COMPLETE]  GitHub Actions workflows: morning_debrief.yml, workdrive_index.yml
  [COMPLETE]  email_config.json template created
  [COMPLETE]  Default voice profile in place (voice/brad_voice_profile.json)
  [PENDING]   Voice samples added to voice/samples/ and build_profile.py run
  [PENDING]   Security audit passed (Bormann_Security_Audit_Prompt_v1.docx)
  [PENDING]   SECURITY_LOCK.md created
  [PENDING]   Zoho OAuth credentials obtained and added to GitHub Secrets
  [PENDING]   Anthropic API key obtained from Brad and added to GitHub Secrets
  [PENDING]   WorkDrive folder IDs added to email_config.json
  [PENDING]   Manual workflow_dispatch test run (dry run, no real inbox)
  [PENDING]   brad-debrief.html confirmed rendering on mobile
  [PENDING]   Go-live (Tuesday or Wednesday only)

PHASE 2 — Pre-Send Edit Tracker  [COMPLETE — built in Phase 1 as v3 feature]
  [PENDING]   Depends on Phase 1 running 5+ days for edit data to accumulate

PHASE 3 — Relationship Heat Map  [COMPLETE — built in Phase 1 as v3 feature]
  [PENDING]   30-day warm-up for baseline data

PHASE 4 — Manufacturer Sentiment Alerts  [COMPLETE — built in Phase 1 as v3 feature]
  [PENDING]   30-day warm-up starts automatically after Phase 1 go-live

PHASE 5 — WorkDrive RAG Layer  [BUILT — disabled until Phase 1 runs 2+ weeks]
  [PENDING]   WorkDrive OAuth credentials
  [PENDING]   WorkDrive folder IDs confirmed in email_config.json
  [PENDING]   workdrive_index.yml test run
  [PENDING]   Do not activate until Phase 1 has run 2+ weeks

PHASE 6 — Quote Draft Assist  [PENDING — hard dependency on Phase 5 validated]

SECURITY:
  [IN PROGRESS] Security audit (Bormann_Security_Audit_Prompt_v1.docx)
  [PENDING]     SECURITY_LOCK.md — required before real credentials

---

## Section 7 — Known Open Items

CRITICAL — Do not bypass:
  - SECURITY_LOCK.md must exist before real Zoho credentials are added
  - Voice samples (voice/samples/*.txt) must be added and build_profile.py run
    before go-live — default profile is a placeholder
  - Zoho API schema files (zoho_mail_schema.json, zoho_crm_schema.json)
    must be verified against live Zoho API docs — AI-generated field names
    are a known hallucination risk — do not trust without verification
  - Sandbox mode (SANDBOX_MODE=true) must pass before real credentials added

DATA INTEGRITY:
  - sent_log.json rolling 30-day append: deduplication logic relies on run_id
    pattern — not yet stress-tested at volume. Validate after 30+ days of real data.
  - today_drafts.json is overwritten each run (by design) — edit_tracker.py
    loads it at the start of the NEXT run to compare against sent folder.
    If the pipeline runs twice in one day, the first run's drafts are lost.
    run_id deduplication should prevent double-runs.

CALIBRATION (expected gaps — not bugs):
  - pipeline_drift_config.json: ALL UNCALIBRATED
    Calibrate after 14 days of real runs. Tony reviews and updates.
  - Sentiment alert baselines: UNCALIBRATED for first 30 days.
    Alerts suppressed during warm-up. This is correct behavior.
  - Brand heat map: No baseline for first 30 days. Shows "baseline" signal. Correct.

TIMEZONE:
  - Cron schedule uses UTC. Must be updated manually for DST transitions.
  - 11:15 UTC = 6:15am CDT (summer, UTC-5). 12:15 UTC = 6:15am CST (winter, UTC-6).
  - Add calendar reminder: 2nd Sunday March and 1st Sunday November.

---

## Section 8 — What Cowork Should Never Do

If asked to do any of these, refuse and explain why.

NEVER:
  - Add real API keys, tokens, or credentials to any file in the repo
  - Commit .env or any file containing credentials
  - Update brad_voice_profile.json during a normal pipeline run
    (only at end of pipeline, and only when rebuild_profile flag is set)
  - Run build_profile.py automatically on a schedule — it is a manual process
  - Auto-draft emails in the Executive / Sensitive category
  - Overwrite existing CRM field values — fill-blanks only
  - Change the pipeline module execution order in run_pipeline.py without
    documenting the reason first
  - Remove the Claude API call count cap (50 calls per run)
  - Remove explicit max_tokens from any Claude API call
  - Log email content, subject lines, sender addresses, or draft content
  - Create SECURITY_LOCK.md before all security audit sections are complete
    and sandbox validation (including deliberate failure test) has passed
  - Trust AI-generated Zoho API field names without verification against
    live Zoho API documentation
  - Activate WorkDrive RAG before Phase 1 has run 2+ weeks

ALWAYS:
  - Read CLAUDE.md completely before starting any work
  - Check SECURITY_AUDIT.md for known issues before making changes
  - Use UTC for all timestamps (datetime.now(timezone.utc))
  - Refresh OAuth tokens at the START of every pipeline run
  - Check for existing run_id before any write operation
  - Fail loudly — log module name, error message, and suggested fix
  - Keep run_pipeline.py as the single entry point
  - Add explicit max_tokens to every Claude API call
  - Keep all three OAuth credential sets completely separate

---

## Section 9 — Sandbox Mode

Trigger: `SANDBOX_MODE=true python3 run_pipeline.py`

When SANDBOX_MODE=true:
  - All Zoho Mail API calls use mock data from tests/mock_zoho_mail.json
  - All Zoho CRM API calls use mock data from tests/mock_zoho_crm.json
  - All Zoho WorkDrive API calls use mock data from tests/mock_workdrive.json
  - Claude API calls use mock responses from tests/mock_claude.json
  - No real API is called under any condition
  - Output is written to tests/sandbox_output/ — not to live files
  - Pipeline logs "SANDBOX MODE ACTIVE" at start and end of every run

Deliberate failure test:
  `SANDBOX_MODE=true MOCK_ZOHO_FAIL=true python3 run_pipeline.py`
  Expected: non-zero exit code, alert fires, debrief NOT updated

Sandbox must pass completely before SECURITY_LOCK.md is created.
Sandbox must be re-run after any major architectural change.

---

## Section 10 — How to Update This File

Update this file when:
  - A phase is completed (update Section 6 build status)
  - A new architectural decision is made (add to Section 3)
  - A new open item is discovered (add to Section 7)
  - A new hard rule is added (add to Section 8)
  - A new module is added to the pipeline (update Section 2 pipeline order)

How to update:
  1. Make the change in CLAUDE.md
  2. Update the "Last updated" date in the file header
  3. Commit with message: `docs: update CLAUDE.md — [what changed]`
  4. Do NOT make CLAUDE.md changes in the same commit as code changes.
     Keep documentation commits separate for clean history.

CLAUDE.md is the source of truth for project context.
If it conflicts with a code comment, CLAUDE.md wins.
Update CLAUDE.md, then update the code comment.
