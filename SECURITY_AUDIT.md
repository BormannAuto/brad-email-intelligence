# Bormann Marketing · Email Intelligence Security Audit · v1.0
# March 2026 · Adapted from KPI Platform Security Audit v6
# Conducted by: Tony Grant / Cowork session 2026-03-16
# Status: IN PROGRESS — see Section 18 for go-live gate

---

## AUDIT LEGEND

| Severity | Meaning |
|----------|---------|
| CRITICAL | Must fix before any credentials added. Pipeline must not run with real data until resolved. |
| HIGH     | Must fix before go-live. Significant security or reliability risk. |
| MEDIUM   | Fix before steady-state operation. Operational risk but not immediate credential risk. |
| LOW      | Best practice improvement. Fix when convenient. |
| INFO     | Architectural note or reminder. No action required. |

| Status | Meaning |
|--------|---------|
| FIXED  | Resolved in this session. |
| PENDING | Not yet addressed. |
| DEFERRED | Deliberately deferred with reason. |
| N/A    | Not applicable to this architecture. |

---

## Section 1 — Secrets and Credentials

### S1 — No credentials in repo

**Severity:** CRITICAL
**Status:** FIXED
**Finding:** All secrets are stored as GitHub repository Secrets and injected as environment variables by GitHub Actions. No `.env` files, no hardcoded tokens, no credentials in any tracked file.
**Verification:** `grep -r "refresh_token\|client_secret\|access_token" *.py` — only references env vars via `os.environ.get()`. No literal credential values present anywhere in the codebase.
**File:** All connector files (`zoho_mail_connector.py`, `zoho_crm_connector.py`, `zoho_workdrive_connector.py`)

---

### S2 — `.gitignore` covers sensitive files

**Severity:** CRITICAL
**Status:** PENDING — must be verified before first commit with real file structure
**Finding:** `.gitignore` must exclude: `data/*.json`, `today_drafts.json`, `*.log`, `tests/sandbox_output/`, any `.env` file.
**Action Required:** Verify `.gitignore` is in place and covers all runtime output files before adding GitHub Secrets. The `brad-debrief.html` committed to Pages is intentional — it is PIN-gated and contains only categorization metadata (no email content).
**Note:** `email_config.json` and `email_config_template.json` are tracked intentionally as config templates. They must never contain real credentials.

---

### S3 — Token refresh at pipeline start

**Severity:** HIGH
**Status:** FIXED
**Finding:** All three OAuth tokens (Mail, CRM, WorkDrive) are refreshed at the START of every pipeline run. No token caching between runs.
**File:** `run_pipeline.py` — `_authenticate_all()` called before any module executes.
**Note:** Token refresh happens inside `try/except` with graceful CRM failure handling (CRM is non-critical; Mail auth failure aborts the pipeline).

---

### S4 — Three OAuth credential sets never mixed

**Severity:** CRITICAL
**Status:** FIXED
**Finding:** Mail, CRM, and WorkDrive each use completely separate env var names (`ZOHO_MAIL_*`, `ZOHO_CRM_*`, `ZOHO_WORKDRIVE_*`). No shared credential variables. Session dicts are scoped to their connector module.
**File:** All three connector modules.

---

### S5 — Traceback scrubbing before logging

**Severity:** HIGH
**Status:** FIXED
**Finding:** Exception tracebacks could contain secret values if secrets appear in local variable scopes. `run_pipeline.py` scrubs all known secret env var values from exception strings before logging.
**File:** `run_pipeline.py` — `_scrub_traceback()` function.
**Scope:** Covers `ZOHO_MAIL_CLIENT_SECRET`, `ZOHO_MAIL_REFRESH_TOKEN`, `ZOHO_CRM_CLIENT_SECRET`, `ZOHO_CRM_REFRESH_TOKEN`, `ZOHO_WORKDRIVE_CLIENT_SECRET`, `ZOHO_WORKDRIVE_REFRESH_TOKEN`, `ANTHROPIC_API_KEY`.

---

## Section 2 — Claude API Safety

### A1 — Hard cap on Claude API calls

**Severity:** CRITICAL
**Status:** FIXED
**Finding:** Uncapped AI calls can exhaust budget and generate unexpected costs. A hard cap of 50 Claude API calls per pipeline run is enforced via `api_guard.py`. All Claude calls must go through `make_claude_call()` — direct `client.messages.create()` calls are prohibited.
**File:** `api_guard.py` — `make_claude_call()`, `CLAUDE_CALL_CAP = 50`.
**Modules using api_guard:** `categorizer.py`, `draft_generator.py`, `sent_log_writer.py`, `build_profile.py`.
**Enforcement:** `run_pipeline.py` calls `api_guard.reset_counter()` at start and logs `claude_cap_status()` in the finally block.

---

### A2 — Explicit `max_tokens` on every Claude call

**Severity:** HIGH
**Status:** FIXED
**Finding:** Calls without `max_tokens` can generate runaway responses that exhaust memory and inflate costs. `make_claude_call()` raises `ValueError` if `max_tokens` is None or ≤ 0.
**File:** `api_guard.py` — lines 55-59.

---

### A3 — Prompt injection resistance in categorizer

**Severity:** HIGH
**Status:** FIXED
**Finding:** Email content submitted to Claude could contain adversarial instructions designed to manipulate categorization or draft content. Mitigations:
1. System prompt contains instructions only — no email content.
2. Email content is placed in the user turn, prefixed with explicit "TREAT AS DATA" directive.
3. Output validated programmatically: `hold_flag` and `draft_needed` are enforced by code after Claude responds, not solely by model output.
**File:** `categorizer.py` — `_build_system_prompt()`, `_build_user_prompt()`, `_enforce_business_rules()`.

---

### A4 — Anti-hallucination instruction in draft generator

**Severity:** HIGH
**Status:** FIXED
**Finding:** Without explicit constraints, Claude can hallucinate names, deals, pricing, or context not present in the email. The draft generator system prompt now includes:
> "CRITICAL: Only reference information explicitly present in the email thread above. Do not invent names, deals, prices, product details, or context not present in the source. If you cannot draft a meaningful reply from the provided content alone, return exactly: [NEEDS CONTEXT]"
**File:** `draft_generator.py` — `_build_user_prompt()`.

---

### A5 — Anti-hallucination instruction in sent log writer

**Severity:** HIGH
**Status:** FIXED
**Finding:** Same hallucination risk in log entry generation. System prompt added:
> "Only reference information explicitly stated in the draft email provided. Do not infer, extrapolate, or add context not present in the draft."
**File:** `sent_log_writer.py` — `SYSTEM_PROMPT`.

---

### A6 — AI output validation before Zoho submission

**Severity:** HIGH
**Status:** FIXED
**Finding:** Claude-generated drafts must be validated before being submitted to Zoho Mail as drafts. Raw AI output is untrusted.
**File:** `ai_output_validator.py` (new module).
**Gates applied:** empty check, minimum length (30 chars), maximum length (8000 chars), failure marker detection, system prompt echo detection, corruption indicator detection.
**Integration note:** `draft_generator.py` must call `validate_draft()` on each generated body before calling `create_draft()`. This integration step is PENDING — must be wired in before go-live.
**Pending action:** Edit `draft_generator.py` to import and call `ai_output_validator.validate_all_drafts(drafts)` before writing `today_drafts.json`.

---

## Section 3 — OAuth and External API Security

### O1 — Retry with exponential backoff on all Zoho API calls

**Severity:** HIGH
**Status:** FIXED
**Finding:** Without retry, a single transient Zoho API error (rate limit, 5xx) permanently fails a pipeline zone. Added `retry_utils.py` with 3-retry exponential backoff (2s/4s/8s + jitter).
**File:** `retry_utils.py` — `with_retry` decorator, `check_response_status()`.
**Applied to:** `_mail_get()`, `_mail_post()` (zoho_mail_connector.py); `_crm_get()`, `_crm_post()` (zoho_crm_connector.py); `_wd_get()` (zoho_workdrive_connector.py).
**Non-retryable codes:** 400, 401, 403, 404 — fail immediately. Retryable: 429, 500, 502, 503, 504.

---

### O2 — Per-request timeout on all HTTP calls

**Severity:** MEDIUM
**Status:** FIXED
**Finding:** All `requests.*` calls include `timeout=30` (Mail/CRM) or `timeout=60` (WorkDrive downloads). Without timeouts, a hung Zoho endpoint would hang the pipeline indefinitely.
**File:** All three connector modules.
**Backstop:** Global 20-minute pipeline timeout in `run_pipeline.py` catches any timeout that slips through at the module level.

---

### O3 — Zoho API field name verification required

**Severity:** CRITICAL
**Status:** PENDING — HUMAN ACTION REQUIRED
**Finding:** AI-generated Zoho API field names are a known hallucination risk. The field names used in all three connector modules were generated by AI and have NOT been verified against live Zoho API documentation.
**Risk:** Pipeline may silently misparse Zoho responses if field names are wrong (e.g., `messageId` vs. `msgId`, `Full_Name` vs. first/last name split, sent folder `folderId`).
**Action Required (before go-live):**
1. Review `zoho_mail_schema.json`, `zoho_crm_schema.json`, `zoho_workdrive_schema.json` — each contains a `_WARNING` header and `_verify` annotations.
2. Cross-reference every field name against the live Zoho API documentation.
3. Run a `workflow_dispatch` test with real credentials (sandbox mode off) and log the raw API response to verify field names match.
4. Update the schema files with `_last_verified` date and `_verified_by` initials.
**Files:** `zoho_mail_schema.json`, `zoho_crm_schema.json`, `zoho_workdrive_schema.json`.

---

### O4 — WorkDrive gracefully disabled when not configured

**Severity:** INFO
**Status:** FIXED
**Finding:** `authenticate_workdrive()` returns `None` (not an exception) if env vars are not set. All WorkDrive calls check for `session is None` before proceeding. WorkDrive is Phase 5 — disabled until Phase 1 runs 2+ weeks.
**File:** `zoho_workdrive_connector.py`.

---

## Section 4 — Data Handling and Privacy

### D1 — No email content in logs

**Severity:** CRITICAL
**Status:** FIXED
**Finding:** Logging email subjects, sender addresses, body content, or draft content to stdout/stderr creates a data leak via GitHub Actions log storage. All log statements were audited and scrubbed.
**Modules fixed:** `zoho_mail_connector.py` (removed subject/recipient from `create_draft()` log), `draft_generator.py` (category and workdrive flag only), `sent_log_writer.py` (category only, no contact names or log entry content).
**Policy:** Log counts and categories only. Never log email content, subject lines, sender addresses, contact names, or draft content.

---

### D2 — Idempotent writes (run_id deduplication)

**Severity:** HIGH
**Status:** FIXED
**Finding:** A double-run could create duplicate drafts, duplicate CRM notes, and duplicate Sent Log entries. `run_id = YYYY-MM-DD` is checked before every write operation.
**Files/modules:**
- `run_pipeline.py` — `_check_duplicate_run()` checks `pipeline_state.json` before executing; `_mark_run_complete()` writes run_id on success.
- `sent_log_writer.py` — checks `existing_run_ids` set before appending to `sent_log.json`.
**Known limitation:** `today_drafts.json` is overwritten each run by design. If the pipeline runs twice in one day, the first run's drafts are lost. `run_id` deduplication should prevent this, but not guaranteed if deduplication is bypassed (e.g., deliberate re-run for testing).

---

### D3 — CRM fill-blanks only

**Severity:** HIGH
**Status:** FIXED (by design, not code)
**Finding:** CRM writes must never overwrite manually entered field values. `add_note()` only creates new Note records — it never updates existing Contact/Lead/Account fields. No auto-creation of stub contacts for unknown senders.
**File:** `zoho_crm_connector.py` — `add_note()` only.
**Policy:** Locked decision in `CLAUDE.md` Section 3.

---

### D4 — Debrief content — no raw email content

**Severity:** HIGH
**Status:** FIXED (by design)
**Finding:** `brad-debrief.html` is committed to a public GitHub Pages repo. It must never contain raw email body content, full sender email addresses, or CRM data.
**Policy:** Debrief contains category labels, urgency scores, sender names (not emails), and draft counts only. Zone 4 links to Zoho Mail Drafts folder — never renders draft content inline.
**File:** `debrief_builder.py` — all zones.

---

### D5 — PIN gate on GitHub Pages debrief

**Severity:** MEDIUM
**Status:** FIXED (by design)
**Finding:** `brad-debrief.html` is PIN-gated via JavaScript/sessionStorage. PIN is set from `DEBRIEF_PIN` env var at build time.
**Limitation:** The PIN is a convenience gate, NOT a security perimeter. Anyone with network access to the GitHub Pages URL who can observe the PIN (e.g., over shoulder) can access the debrief. This is acceptable for the stated use case (Brad reads it on his phone).
**Action:** Do not store sensitive deal details or full email content in the debrief. Current design complies.

---

## Section 5 — Error Handling and Pipeline Resilience

### E1 — Global pipeline timeout (20 minutes)

**Severity:** HIGH
**Status:** FIXED
**Finding:** Without a global timeout, a hung API call or runaway loop could block the GitHub Actions runner indefinitely, consuming free-tier minutes and leaving Brad without a debrief.
**File:** `run_pipeline.py` — `GLOBAL_TIMEOUT_SECONDS = 1200`, `threading.Timer` watchdog calls `_global_timeout_handler()` on expiry. Handler writes a fallback debrief and forces `os._exit(1)`.

---

### E2 — Fail loudly — module name and error in every log

**Severity:** MEDIUM
**Status:** FIXED (by design)
**Finding:** All `except` blocks log the module name, error type, and message. Pipeline never silently swallows errors.
**Policy:** Locked decision in `CLAUDE.md` Section 8.

---

### E3 — CRM failures never abort pipeline

**Severity:** MEDIUM
**Status:** FIXED (by design)
**Finding:** CRM lookups are supplementary. `lookup_contact()`, `lookup_account()`, and `add_note()` all catch all exceptions and return graceful fallbacks (`_NOT_FOUND` dict or `False`).
**File:** `zoho_crm_connector.py`.

---

### E4 — Draft failure produces placeholder, not abort

**Severity:** HIGH
**Status:** FIXED
**Finding:** If Claude API call fails for a specific email, `draft_generator.py` inserts `DRAFT_FAILED_BODY` placeholder so the debrief still renders. Zone 0 still appears with a manual-required notice.
**File:** `draft_generator.py` — `DRAFT_FAILED_BODY` constant.

---

### E5 — Alert mechanism for pipeline anomalies

**Severity:** HIGH
**Status:** FIXED
**Finding:** Pipeline failures were previously silent (visible only in GitHub Actions logs). Added `alert_writer.py` with three alert levels (CRITICAL/HIGH/WARN), rolling JSON alert file (`data/pipeline_alerts.json`), and GitHub Actions job summary integration (`$GITHUB_STEP_SUMMARY`).
**File:** `alert_writer.py`.
**Integration note:** `run_pipeline.py` should call `alert_critical()` on auth failure and `alert_high()` on Claude cap reached. This wiring is PENDING — must be added before go-live.

---

## Section 6 — Idempotency and Timestamps

### I1 — UTC everywhere

**Severity:** HIGH
**Status:** FIXED (by design)
**Finding:** All `datetime.now()` calls use `datetime.now(timezone.utc)`. No `datetime.now()` without timezone. No local time references.
**Policy:** Locked decision in `CLAUDE.md` Section 3. Any timestamp not explicitly UTC is a bug.

---

### I2 — run_id deduplication on pipeline state

**Severity:** HIGH
**Status:** FIXED
**Finding:** `run_pipeline.py` reads `pipeline_state.json` at startup and compares `last_run_id` against today's date. If the run already completed today, pipeline exits with a warning (not an error). `_mark_run_complete()` writes the run_id on successful completion.
**File:** `run_pipeline.py`.

---

## Section 7 — Debrief and GitHub Pages

### G1 — Debrief committed, not served by API

**Severity:** INFO
**Status:** FIXED (by design)
**Finding:** Debrief is a static HTML file committed to the repo root and served by GitHub Pages. No server, no API, no dynamic rendering. Attack surface is minimal: the only moving part is the git push.
**Architecture:** `run_pipeline.py` commits `brad-debrief.html` via subprocess git calls (not gitpython — see S9).

---

### G2 — Git push uses deploy key, not PAT

**Severity:** MEDIUM
**Status:** PENDING — HUMAN ACTION REQUIRED
**Finding:** The GitHub Actions workflow should use a deploy key (SSH) for the git push step, not a personal access token. A deploy key is scoped to a single repo and read/write access is explicit.
**Action Required:** When setting up the GitHub Actions workflow, configure the checkout action with a deploy key rather than `GITHUB_TOKEN` if write access to the repo is needed.
**Note:** `GITHUB_TOKEN` with `contents: write` permission is an acceptable fallback for a single-tenant private repo.

---

## Section 8 — Requirements and Dependencies

### R1 — No unused dependencies

**Severity:** MEDIUM
**Status:** FIXED
**Finding:** `gitpython` was listed in `requirements.txt` but never used — `run_pipeline.py` uses subprocess git calls. Unused dependencies expand the attack surface with no benefit.
**Action taken:** Removed `gitpython>=3.1.40` from `requirements.txt`.
**File:** `requirements.txt`.

---

### R2 — Minimum version pinning

**Severity:** LOW
**Status:** FIXED (by design)
**Finding:** All dependencies use `>=` minimum version pinning with known-good minimum versions. This prevents accidental use of vulnerable older versions while allowing patch updates.
**File:** `requirements.txt`.

---

## Section 9 — Logging

### L1 — Operational metadata only

**Severity:** CRITICAL
**Status:** FIXED
**Finding:** GitHub Actions logs are stored by GitHub for 90 days and accessible to repo collaborators. Email content in logs = data breach.
**Policy:** Log counts, categories, module names, and error types only. Never log: email subjects, sender/recipient addresses, body content, contact names, draft content, or CRM data.
**Verification:** Reviewed all `logger.*` calls in all 17 modules. Scrubbed `zoho_mail_connector.py` create_draft log statements. All other modules confirmed compliant.

---

## Section 10 — Retry Logic

### RT1 — Exponential backoff on Zoho API calls

**Severity:** HIGH
**Status:** FIXED
**See:** O1 above.

---

## Section 11 — Voice Profile

### V1 — Voice profile version tracking

**Severity:** LOW
**Status:** FIXED
**Finding:** `brad_voice_profile.json` had no version number, making it impossible to verify which profile version is in use.
**Action taken:** Added `profile_version` (int, increments with each rebuild) and `profile_schema_version` ("1.0") to `voice/brad_voice_profile.json`.
**File:** `voice/brad_voice_profile.json`.

---

### V2 — Voice profile only updated on manual trigger

**Severity:** HIGH
**Status:** FIXED (by design)
**Finding:** Voice profile updates run at the END of the pipeline (after debrief is already committed), and only when `rebuild_profile` flag is set in `pipeline_state.json`. Human review of 3 sample drafts is required before any updated profile goes live.
**File:** `run_pipeline.py`, `build_profile.py`.
**Policy:** Locked decision in `CLAUDE.md` Section 3.

---

## Section 12 — AI Output Validation

### AV1 — Draft validation before Zoho submission

**Severity:** HIGH
**Status:** FIXED (module created) + PENDING (integration)
**Finding:** AI-generated drafts are untrusted. Created `ai_output_validator.py` with 7 validation gates.
**Integration pending:** `draft_generator.py` must call `validate_all_drafts()` before writing `today_drafts.json` and before calling `create_draft()`. This wiring step must be completed before go-live.
**File:** `ai_output_validator.py`.

---

## Section 13 — Sandbox Mode

### SB1 — Full sandbox mode implemented

**Severity:** CRITICAL
**Status:** FIXED
**Finding:** Real credentials must never be added until sandbox validation passes. Implemented:
- `SANDBOX_MODE=true` env var bypasses all real API calls
- All Zoho calls use mock data from `tests/mock_zoho_mail.json`, `tests/mock_zoho_crm.json`, `tests/mock_workdrive.json`
- All Claude calls use mock responses from `tests/mock_claude.json`
- Output written to `tests/sandbox_output/` — not to live files
- Pipeline logs "SANDBOX MODE ACTIVE" at start and end
**File:** `run_pipeline.py`.

---

### SB2 — Deliberate failure test

**Severity:** CRITICAL
**Status:** FIXED (implemented) + PENDING (must be run before credentials added)
**Finding:** `SANDBOX_MODE=true MOCK_ZOHO_FAIL=true python3 run_pipeline.py` triggers a simulated auth failure.
**Expected behavior:** Non-zero exit code, alert fires, debrief NOT updated.
**Action Required:** Run both the passing sandbox test AND the deliberate failure test before creating `SECURITY_LOCK.md`.

---

## Section 14 — Operational Health

### H1 — Pipeline drift monitoring

**Severity:** MEDIUM
**Status:** FIXED (infrastructure created) + PENDING (calibration after Day 14)
**Finding:** No mechanism to detect pipeline anomalies (email count drops to zero, draft count spikes unexpectedly, API cap consumed).
**Action taken:** Created `data/pipeline_drift_config.json` with baseline/threshold structure. Created `alert_writer.py` with `check_drift_and_alert()` function.
**Calibration:** ALL baselines are `null` and thresholds are uncalibrated. Calibrate after 14 days of real runs by reviewing actual metric values and setting realistic baselines.

---

## Section 15 — Business Rule Enforcement

### BR1 — Executive-Sensitive hold flag cannot be overridden by AI

**Severity:** CRITICAL
**Status:** FIXED
**Finding:** If Claude incorrectly categorizes an executive email as non-executive, a draft could be auto-generated for a sensitive email. Business rules are enforced programmatically after Claude's response:
- `hold_flag = True` for ALL Executive-Sensitive emails (regardless of model output)
- `draft_needed = False` for ALL NO_DRAFT_CATEGORIES (regardless of model output)
**File:** `categorizer.py` — `_enforce_business_rules()`.

---

### BR2 — Safe type coercion on AI outputs

**Severity:** HIGH
**Status:** FIXED
**Finding:** Claude may return urgency as a string, float, or out-of-range integer. Added safe coercion:
- `urgency`: cast to int, clamped to 1-5
- `sentiment_score`: cast to float, clamped to 0.0-1.0
**File:** `categorizer.py` — `_safe_int()`, `_safe_float()`.

---

## Section 16 — Architecture and Surface Area

### AR1 — Single entry point

**Severity:** INFO
**Status:** FIXED (by design)
**Finding:** `run_pipeline.py` is the sole entry point. No module runs independently in production. This simplifies security review — all execution paths flow through one file.

---

### AR2 — No VPS, no server

**Severity:** INFO
**Status:** N/A (by design)
**Finding:** GitHub Actions free tier replaces the VPS from v2. No SSH access to maintain, no open ports, no persistent server to harden. Attack surface is: GitHub repo access + GitHub Actions log access.

---

## Section 17 — Known Remaining Items

The following items were identified during this audit but are not yet fully resolved. They do not block SECURITY_LOCK.md if the items below are tagged PENDING(pre-go-live) — but items tagged PENDING(pre-credentials) must be complete before any real Zoho credentials are added.

| # | Item | File | Severity | Gate |
|---|------|------|----------|------|
| 1 | Wire `ai_output_validator.validate_all_drafts()` into `draft_generator.py` | `draft_generator.py` | HIGH | pre-go-live |
| 2 | Wire `alert_writer` calls into `run_pipeline.py` (auth failure, cap reached) | `run_pipeline.py` | HIGH | pre-go-live |
| 3 | Verify Zoho API field names against live API docs | Schema files | CRITICAL | pre-credentials |
| 4 | Confirm `.gitignore` covers all runtime output files | `.gitignore` | CRITICAL | pre-credentials |
| 5 | Configure GitHub Actions workflow with deploy key or scoped GITHUB_TOKEN | `.github/workflows/` | MEDIUM | pre-go-live |
| 6 | Run passing sandbox test (`SANDBOX_MODE=true`) | `run_pipeline.py` | CRITICAL | pre-credentials |
| 7 | Run deliberate failure test (`MOCK_ZOHO_FAIL=true`) | `run_pipeline.py` | CRITICAL | pre-credentials |
| 8 | Calibrate `pipeline_drift_config.json` after Day 14 | `data/pipeline_drift_config.json` | MEDIUM | post-go-live |

---

## Section 18 — Go-Live Gate (SECURITY_LOCK.md Prerequisites)

`SECURITY_LOCK.md` must NOT be created until all of the following are confirmed:

**PRE-CREDENTIALS GATE (before any real Zoho/Anthropic keys are added):**

- [ ] `.gitignore` verified — runtime output files excluded
- [ ] Zoho API field names verified against live documentation (schema files updated)
- [ ] `SANDBOX_MODE=true` test run passes completely
- [ ] `SANDBOX_MODE=true MOCK_ZOHO_FAIL=true` test run produces non-zero exit, no debrief update, alert fires
- [ ] `brad-debrief.html` renders correctly on mobile (check after sandbox run)

**PRE-GO-LIVE GATE (before enabling cron / real inbox):**

- [ ] `ai_output_validator.validate_all_drafts()` wired into `draft_generator.py`
- [ ] `alert_writer` calls wired into `run_pipeline.py` for auth failure and cap reached
- [ ] GitHub Actions workflow deploy key or scoped GITHUB_TOKEN configured
- [ ] Voice samples added to `voice/samples/` and `build_profile.py` run (human review of 3 drafts)
- [ ] Manual `workflow_dispatch` test run with real credentials (no cron)
- [ ] Go-live on Tuesday or Wednesday only (never Monday)

**Once all pre-credentials items are checked:** Create `SECURITY_LOCK.md` with date and initials.
**Once all pre-go-live items are checked:** Enable the cron schedule in `morning_debrief.yml`.

---

*End of Security Audit v1.0*
*Next review: After 30 days of real pipeline operation or after any major architectural change.*
