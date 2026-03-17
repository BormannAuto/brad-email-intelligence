# SECURITY_LOCK.md
# Bormann Email Intelligence System — Pre-Credentials Gate
# Created: 2026-03-17
# Created by: Tony Grant

---

## Status: UNLOCKED — Real credentials may now be added to GitHub Secrets

All pre-credentials gate items from SECURITY_AUDIT.md Section 18 are confirmed complete.

---

## Pre-Credentials Checklist

- [x] `.gitignore` verified — runtime output files excluded (confirmed 2026-03-17)
      Added: data/pipeline_state.json
      Confirmed: data/sent_log.json, data/edit_log.json, today_drafts.json, tests/sandbox_output/, all .env variants

- [x] Zoho Mail API field names verified against live documentation (confirmed 2026-03-17)
      Corrections applied to zoho_mail_schema.json:
        — messageId confirmed (not msgId)
        — body field is 'content' (not 'htmlContent')
        — list response is data[] flat array (NOT data.messages)
        — threadId does NOT exist in draft payload — use inReplyTo + refHeader
        — folder IDs are account-specific dynamic values — must query by folderType
      Source: https://www.zoho.com/mail/help/api/

- [x] Zoho CRM API field names verified against live documentation (confirmed 2026-03-17)
      Corrections applied to zoho_crm_schema.json:
        — Full_Name confirmed for both Contacts and Leads
        — Notes module field is 'se_module' (NOT '$se_module' — $ prefix is wrong)
        — Note_Title, Note_Content, Parent_Id all confirmed
        — CRM base URL confirmed: https://www.zohoapis.com/crm/v3
      Source: https://www.zoho.com/crm/developer/docs/api/v3/

- [x] SANDBOX_MODE=true test run passes completely (confirmed 2026-03-17)
      Result: Exit code 0, "SANDBOX MODE ACTIVE" logged at start and end

- [x] SANDBOX_MODE=true MOCK_ZOHO_FAIL=true produces non-zero exit, no debrief update (confirmed 2026-03-17)
      Result: Exit code 1, RuntimeError logged, debrief NOT updated, alert fired

---

## Zoho WorkDrive Schema

zoho_workdrive_schema.json remains UNVERIFIED — WorkDrive is Phase 5 and intentionally
disabled until Phase 1 has run 2+ weeks. Verify this schema before enabling WorkDrive
OAuth credentials. This does not block the pre-credentials gate for Mail and CRM.

---

## Pre-Go-Live Items Still Pending

The following are NOT required before adding credentials, but MUST be complete
before enabling the cron schedule (per SECURITY_AUDIT.md Section 18):

- [x] ai_output_validator.validate_draft() wired into draft_generator.py (completed 2026-03-17)
      Validates each Claude response before Zoho submission; fails open on validator crash.

- [x] alert_writer calls wired into run_pipeline.py (completed 2026-03-17)
      CRITICAL alert on Zoho Mail auth failure (Step 1).
      HIGH alert on Claude API cap reached (finally block).

- [x] zoho_crm_connector.py: "$se_module" → "se_module" corrected (2026-03-17)
      $ prefix caused silent field rejection on every CRM note write.

- [x] zoho_mail_connector.py: fetch_thread_context data path corrected (2026-03-17)
      Was data.messages (always empty). Corrected to flat data[] per verified schema.
      Draft thread context was broken — now fixed before any real run.

- [ ] CODE FIX REQUIRED: fetch_sent_emails uses hardcoded folderId=5 for Sent folder.
      Schema verified: folder IDs are account-specific dynamic values — 5 is wrong.
      Must replace with dynamic folder lookup by folderType before followup_scanner
      produces reliable results. Fix before first cron run.

- [ ] VERIFY DURING DISPATCH TEST: fetch_recent_emails — confirm list endpoint returns
      full body content (not just 'summary' snippet). If full body requires separate
      content endpoint call per message, add that fetch loop before enabling cron.

- [ ] GitHub Actions workflow deploy key or scoped GITHUB_TOKEN configured
- [ ] Voice samples added to voice/samples/ and build_profile.py run (human review of 3 drafts)
- [ ] Manual workflow_dispatch test run with real credentials (no cron)
- [ ] brad-debrief.html confirmed rendering on mobile
- [ ] Go-live on Tuesday or Wednesday only (never Monday)

---

## What This File Authorizes

Adding the following GitHub Secrets to the repository:
  - ZOHO_MAIL_CLIENT_ID
  - ZOHO_MAIL_CLIENT_SECRET
  - ZOHO_MAIL_REFRESH_TOKEN
  - ZOHO_MAIL_ACCOUNT_ID
  - ZOHO_CRM_CLIENT_ID
  - ZOHO_CRM_CLIENT_SECRET
  - ZOHO_CRM_REFRESH_TOKEN
  - ANTHROPIC_API_KEY
  - DEBRIEF_PIN

WorkDrive secrets (ZOHO_WORKDRIVE_*) are NOT authorized until Phase 5 and
zoho_workdrive_schema.json is verified.

---

*This file must not be deleted or modified without re-running the full pre-credentials checklist.*
