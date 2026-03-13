# Bormann Email Intelligence v3 — Setup Guide
Tony Grant · March 2026

## Pre-Flight Checklist

Before the pipeline will run, complete every item in this list.

### 1. GitHub Repo
- [ ] Repo created (you have this)
- [ ] GitHub Pages enabled → Settings → Pages → Source: `main` branch, `/` root
- [ ] Copy all files from this folder into the repo root
- [ ] Push to `main`

### 2. GitHub Secrets
Go to: Repo → Settings → Secrets and variables → Actions → New repository secret

Add these secrets (get values from Zoho API console + Brad):

| Secret Name | Where to get it |
|---|---|
| `ZOHO_MAIL_CLIENT_ID` | Zoho API Console → Mail OAuth app |
| `ZOHO_MAIL_CLIENT_SECRET` | Same |
| `ZOHO_MAIL_REFRESH_TOKEN` | OAuth flow (see Zoho docs) |
| `ZOHO_MAIL_ACCOUNT_ID` | Zoho Mail → Settings → Mail API → Account ID |
| `ZOHO_CRM_CLIENT_ID` | Zoho API Console → CRM OAuth app |
| `ZOHO_CRM_CLIENT_SECRET` | Same |
| `ZOHO_CRM_REFRESH_TOKEN` | OAuth flow |
| `ZOHO_WORKDRIVE_CLIENT_ID` | Zoho API Console → WorkDrive OAuth app (optional) |
| `ZOHO_WORKDRIVE_CLIENT_SECRET` | Same |
| `ZOHO_WORKDRIVE_REFRESH_TOKEN` | OAuth flow (optional) |
| `ANTHROPIC_API_KEY` | console.anthropic.com → API Keys (get from Brad) |
| `DEBRIEF_PIN` | 4-digit PIN for the morning debrief page |

### 3. email_config.json
- [ ] Add real Zoho WorkDrive folder IDs to `workdrive_brand_folders`
- [ ] Review `blocked_domains` and add any known newsletter sources
- [ ] Review `manufacturer_domains` — add/remove as needed

### 4. Voice Samples
- [ ] Add 10–20 example emails (bodies only) to `voice/samples/*.txt`
- [ ] Run `python build_profile.py` to build the initial voice profile
- [ ] Review `voice/brad_voice_profile.json` — tweak if anything's off

### 5. First Run (Manual)
- [ ] Push everything to `main`
- [ ] Go to Actions tab → Morning Email Debrief → Run workflow (manual)
- [ ] Watch the logs — confirm it completes without errors
- [ ] Open the GitHub Pages URL and verify the debrief loads with the PIN

### 6. Enable Cron
- [ ] Confirm time zone: `15 11 * * *` = 6:15am CDT (summer). Update to `15 12 * * *` Nov-Mar.
- [ ] Add calendar reminder for DST changes (2nd Sunday March, 1st Sunday November)
- [ ] Enable the workflow schedule by removing the `# ` comment block if needed

---

## Architecture Quick Reference

```
run_pipeline.py           ← Main orchestrator (GitHub Actions)
zoho_mail_connector.py    ← Mail API: fetch, draft, sent
zoho_crm_connector.py     ← CRM API: lookup, notes
zoho_workdrive_connector.py ← WorkDrive API: folders, files
workdrive_indexer.py      ← Weekly index builder (separate workflow)
workdrive_retriever.py    ← Keyword search against index
noise_filter.py           ← Separate real mail from noise
categorizer.py            ← Claude: category, urgency, sentiment
followup_scanner.py       ← Overdue detection + sentiment alerts
voice_profile.py          ← Load voice profile for prompts
build_profile.py          ← Build/rebuild voice profile from samples
draft_generator.py        ← Claude: write replies in Brad's voice
sent_log_writer.py        ← Log entries + CRM notes
edit_tracker.py           ← Compare drafts to actual sent (v3 NEW)
heat_map.py               ← Brand activity heat map (v3 NEW)
debrief_builder.py        ← Build brad-debrief.html
weekly_recap.py           ← Friday weekly recap
```

## Troubleshooting

**Pipeline fails with auth error**
→ Zoho OAuth tokens expire. Re-run the OAuth flow and update GitHub Secrets.

**No drafts generated**
→ Check that `draft_needed=True` is firing for Manufacturer/Dealer-Customer at urgency 1-2.
→ Check `hold_flag` isn't firing incorrectly.

**Debrief page blank after loading**
→ Check GitHub Pages is enabled and pointing to repo root.
→ Confirm brad-debrief.html was committed (check Actions logs).

**WorkDrive retrieval returns empty**
→ Normal until first Sunday's workdrive_index.yml run completes.
→ Check WorkDrive folder IDs in email_config.json.

**Timezone is wrong**
→ Update cron in `.github/workflows/morning_debrief.yml`.
→ 11:15 UTC = 6:15am CDT. 12:15 UTC = 6:15am CST.
