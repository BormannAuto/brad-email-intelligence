"""
debrief_builder.py
Bormann Marketing — Email Intelligence System v3
Builds brad-debrief.html — PIN-gated, mobile-first morning debrief page.
Committed to repo root by run_pipeline.py for GitHub Pages serving.

Colors:
  Navy   #0F2942  (header background)
  Gold   #C8963E  (accents, CTA buttons)
  Green  #1A7A4A  (Zone 0 — The Brief)
  Teal   #0E6B82  (v3 feature badges)
  Red    #B91C1C  (urgent / hold)
  Purple #7C3AED  (HOLD items)
"""

import json
import logging
import os
from datetime import datetime, timezone
from html import escape
from typing import Optional

logger = logging.getLogger(__name__)

# ── PIN ────────────────────────────────────────────────────────────────────
# Default PIN — set via env var for security.
DEFAULT_PIN = os.environ.get("DEBRIEF_PIN", "2580")


# ── Data loaders ──────────────────────────────────────────────────────────

def _load_json(path: str, default):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception as e:
        logger.warning(f"Could not load {path}: {e}")
        return default


# ── HTML helpers ──────────────────────────────────────────────────────────

def _badge(text: str, color: str = "#0E6B82") -> str:
    return (
        f'<span style="background:{color};color:#fff;font-size:11px;'
        f'padding:2px 7px;border-radius:4px;font-weight:600;'
        f'letter-spacing:.5px;white-space:nowrap">{escape(text)}</span>'
    )

def _urgency_badge(urgency: int, hold: bool = False) -> str:
    if hold:
        return _badge("HOLD", "#7C3AED")
    colors = {1: "#B91C1C", 2: "#D97706", 3: "#2563EB", 4: "#6B7280"}
    labels = {1: "URGENT", 2: "TODAY",   3: "THIS WEEK", 4: "LOW"}
    c = colors.get(urgency, "#6B7280")
    l = labels.get(urgency, f"P{urgency}")
    return _badge(l, c)

def _card(content: str, border_color: str = "#334155") -> str:
    return (
        f'<div style="background:#1e2d3d;border-left:4px solid {border_color};'
        f'border-radius:6px;padding:14px 16px;margin:10px 0;'
        f'line-height:1.5">{content}</div>'
    )

def _section_header(title: str, color: str = "#C8963E", badge_text: str = "") -> str:
    badge = f' {_badge(badge_text, "#0E6B82")}' if badge_text else ""
    return (
        f'<h2 style="color:{color};font-size:15px;font-weight:700;'
        f'text-transform:uppercase;letter-spacing:1px;margin:24px 0 8px;'
        f'border-bottom:1px solid #2d3f50;padding-bottom:6px">'
        f'{escape(title)}{badge}</h2>'
    )

def _collapsible(title: str, content: str, open_: bool = False, badge: str = "") -> str:
    badge_html = f' <span style="color:#6b7280;font-size:12px">{escape(badge)}</span>' if badge else ""
    open_attr  = "open" if open_ else ""
    return (
        f'<details {open_attr} style="margin:10px 0">'
        f'<summary style="cursor:pointer;color:#93c5fd;font-weight:600;'
        f'font-size:14px;padding:6px 0;list-style:none">'
        f'▶ {escape(title)}{badge_html}</summary>'
        f'<div style="margin-top:8px">{content}</div>'
        f'</details>'
    )


# ── Zone builders ─────────────────────────────────────────────────────────

def _zone_0_brief(drafts: list[dict]) -> str:
    """Zone 0: The Brief — show each draft commitment for review."""
    if not drafts:
        return ""

    items = []
    for draft in sorted(drafts, key=lambda d: 0):  # already ordered by urgency from caller
        zoho_link = (
            f'https://mail.zoho.com/zm/#mail/folder/drafts'  # deep link not available in API
        )
        body_preview = (draft.get("draft_body") or "")[:280]
        items.append(
            _card(
                f'<div style="font-size:12px;color:#9ca3af;margin-bottom:4px">'
                f'To: <strong style="color:#e2e8f0">{escape(draft.get("recipient_name",""))}</strong>'
                f'&nbsp;&nbsp;·&nbsp;&nbsp;Re: {escape(draft.get("subject","")[:60])}</div>'
                f'<div style="color:#e2e8f0;font-size:13px;margin-bottom:8px">{escape(body_preview)}{"…" if len(body_preview)==280 else ""}</div>'
                f'<div style="font-size:12px;color:#6ee7b7;font-style:italic">Does this match what you remember? If not, edit before sending.</div>'
                f'<div style="margin-top:8px"><a href="{zoho_link}" target="_blank" '
                f'style="background:#C8963E;color:#fff;padding:5px 14px;border-radius:4px;'
                f'font-size:12px;font-weight:600;text-decoration:none">Open Draft in Zoho ↗</a></div>',
                border_color="#1A7A4A"
            )
        )

    content = "\n".join(items)
    return (
        f'<div id="zone0" style="background:#0d2010;border:2px solid #1A7A4A;'
        f'border-radius:8px;padding:16px;margin-bottom:20px">'
        f'<h2 style="color:#6ee7b7;font-size:15px;font-weight:700;'
        f'text-transform:uppercase;letter-spacing:1px;margin:0 0 12px">'
        f'⚡ The Brief — {len(drafts)} Draft{"s" if len(drafts)!=1 else ""} Ready</h2>'
        f'{content}</div>'
    )


def _zone_1_header(stats: dict, date_str: str, priority_brand: str = "") -> str:
    brand_flag = (
        f'<div style="background:#C8963E;color:#fff;padding:4px 12px;'
        f'border-radius:4px;font-weight:700;font-size:13px;display:inline-block;margin-top:6px">'
        f'⚑ {escape(priority_brand)}</div>'
    ) if priority_brand else ""

    return (
        f'<div style="margin-bottom:16px">'
        f'<div style="color:#93c5fd;font-size:13px">{escape(date_str)}</div>'
        f'<div style="color:#6b7280;font-size:12px;margin-top:4px">'
        f'{stats.get("total_received",0)} emails processed'
        f' · {stats.get("noise_count",0)} noise filtered'
        f' · {stats.get("drafts_created",0)} drafts created</div>'
        f'{brand_flag}</div>'
    )


def _zone_2_top3(emails: list[dict]) -> str:
    """Top 3 urgency-1 items."""
    top = [e for e in emails if e.get("urgency") == 1][:3]
    if not top:
        return ""

    items = []
    for e in top:
        hold = e.get("hold_flag", False)
        items.append(
            _card(
                f'{_urgency_badge(1, hold)} '
                f'<strong style="color:#f1f5f9">{escape(e.get("sender_name",""))}</strong>'
                f'<span style="color:#6b7280"> · {escape(e.get("category",""))}</span><br>'
                f'<span style="color:#e2e8f0;font-size:13px">{escape(e.get("subject",""))}</span><br>'
                f'<span style="color:#94a3b8;font-size:12px">{escape(e.get("action_summary",""))}</span>',
                border_color="#B91C1C" if not hold else "#7C3AED"
            )
        )
    return _section_header("Top 3") + "\n".join(items)


def _zone_2_5_radar(emails: list[dict], overdue: list[dict], sentiment_alerts: list[dict]) -> str:
    """On Your Radar — urgency 2 + overdue + sentiment alerts."""
    u2 = [e for e in emails if e.get("urgency") == 2][:7]
    if not u2 and not overdue and not sentiment_alerts:
        return ""

    items = []

    for e in u2:
        items.append(
            _card(
                f'{_urgency_badge(2)} '
                f'<strong style="color:#f1f5f9">{escape(e.get("sender_name",""))}</strong>'
                f'<span style="color:#6b7280"> · {escape(e.get("category",""))}</span><br>'
                f'<span style="color:#e2e8f0;font-size:13px">{escape(e.get("subject",""))}</span><br>'
                f'<span style="color:#94a3b8;font-size:12px">{escape(e.get("action_summary",""))}</span>'
            )
        )

    for od in overdue[:5]:
        days = od.get("days_since_sent", 0)
        items.append(
            _card(
                f'{_badge("NO REPLY", "#DC2626")} '
                f'<strong style="color:#f1f5f9">{escape(od.get("recipient_name",""))}</strong>'
                f'<span style="color:#6b7280"> · {days}d ago</span><br>'
                f'<span style="color:#e2e8f0;font-size:13px">{escape(od.get("subject",""))}</span>',
                border_color="#DC2626"
            )
        )

    for alert in sentiment_alerts[:3]:
        items.append(
            _card(
                f'{_badge("⚠ TONE SHIFT", "#0E6B82")} '
                f'<strong style="color:#f1f5f9">{escape(alert.get("sender_name",""))}</strong>'
                f'<span style="color:#6b7280"> · {escape(alert.get("brand",""))}</span><br>'
                f'<span style="color:#94a3b8;font-size:12px">{escape(alert.get("alert_text",""))}</span>',
                border_color="#0E6B82"
            )
        )

    return _section_header("On Your Radar") + "\n".join(items)


def _zone_3_crm(emails: list[dict]) -> str:
    """Zone 3: CRM Pulse — contacts from Zone 2/2.5 with CRM data."""
    crm_emails = [
        e for e in emails
        if e.get("crm_relevant") and e.get("urgency") in (1, 2)
        and e.get("crm_context", {}).get("found")
    ]
    if not crm_emails:
        return ""

    items = []
    for e in crm_emails[:6]:
        crm = e.get("crm_context", {})
        items.append(
            f'<div style="padding:6px 0;border-bottom:1px solid #2d3f50">'
            f'<strong style="color:#e2e8f0;font-size:13px">{escape(crm.get("name",""))}</strong>'
            f'<span style="color:#6b7280;font-size:12px"> · {escape(crm.get("company",""))}</span><br>'
            f'<span style="color:#94a3b8;font-size:12px">'
            f'Last activity: {str(crm.get("last_activity_date",""))[:10] or "unknown"}'
            f'</span></div>'
        )

    return _collapsible(
        "CRM Pulse",
        "".join(items),
        badge=f"{len(crm_emails)} contacts"
    )


def _zone_4_drafts(drafts: list[dict]) -> str:
    """Zone 4: Drafts Ready — gold button."""
    if not drafts:
        return ""
    zoho_url = "https://mail.zoho.com/zm/#mail/folder/drafts"
    return (
        f'<div style="background:#1a2030;border:2px solid #C8963E;border-radius:8px;'
        f'padding:14px 16px;margin:16px 0;text-align:center">'
        f'<div style="color:#fbbf24;font-weight:700;font-size:15px;margin-bottom:8px">'
        f'{len(drafts)} Draft{"s" if len(drafts)!=1 else ""} Waiting in Zoho Mail</div>'
        f'<a href="{zoho_url}" target="_blank" '
        f'style="background:#C8963E;color:#fff;padding:10px 28px;border-radius:6px;'
        f'font-size:14px;font-weight:700;text-decoration:none;display:inline-block">'
        f'Open Drafts in Zoho ↗</a></div>'
    )


def _zone_5_summary(emails: list[dict]) -> str:
    """Zone 5: Full email summary, collapsible by category."""
    cat_order = ["Manufacturer", "Dealer-Customer", "Internal-Team",
                 "Executive-Sensitive", "Business-Dev", "Legal-Financial", "Personal", "Other"]
    by_cat: dict[str, list] = {}
    for e in emails:
        cat = e.get("category", "Other")
        by_cat.setdefault(cat, []).append(e)

    sections = []
    for cat in cat_order:
        items = by_cat.get(cat, [])
        if not items:
            continue
        rows = []
        for e in items:
            rows.append(
                f'<div style="padding:5px 0;border-bottom:1px solid #2d3f50;font-size:12px">'
                f'{_urgency_badge(e.get("urgency",3))} '
                f'<strong style="color:#e2e8f0">{escape(e.get("sender_name",""))}</strong>'
                f' — {escape(e.get("subject","")[:70])}'
                f'<br><span style="color:#6b7280">{escape(e.get("action_summary",""))}</span>'
                f'</div>'
            )
        sections.append(_collapsible(
            cat,
            "".join(rows),
            open_=(cat in ("Manufacturer", "Dealer-Customer")),
            badge=f"{len(items)}"
        ))

    return _section_header("All Emails") + "\n".join(sections)


def _zone_6_tasks_log(emails: list[dict], sent_log: list[dict]) -> str:
    """Zone 6: Task List + Sent Log."""
    tasks = [e for e in emails if e.get("task") and e.get("urgency") in (1, 2)]
    task_html = ""
    for t in tasks:
        task_html += (
            f'<div style="display:flex;align-items:flex-start;gap:10px;'
            f'padding:6px 0;border-bottom:1px solid #2d3f50">'
            f'<input type="checkbox" style="margin-top:3px;flex-shrink:0">'
            f'<span style="color:#e2e8f0;font-size:13px">{escape(t.get("task",""))}</span>'
            f'</div>'
        )

    # Sent log — last 7 entries
    log_html = ""
    for entry in sorted(sent_log, key=lambda e: e.get("date",""), reverse=True)[:7]:
        log_html += (
            f'<div style="padding:5px 0;border-bottom:1px solid #2d3f50;'
            f'color:#94a3b8;font-size:12px;font-style:italic">'
            f'{escape(entry.get("log_entry",""))}'
            f'<span style="color:#4b5563"> — {entry.get("date","")[:10]}</span>'
            f'</div>'
        )

    sent_log_section = (
        _collapsible("What I Communicated Recently", log_html, badge=f"{min(len(sent_log),7)} entries")
        if log_html else ""
    )

    return (
        _section_header("Tasks") +
        (task_html or '<div style="color:#6b7280;font-size:13px">No action items today.</div>') +
        sent_log_section
    )


def _zone_7_noise(noise_report: dict) -> str:
    """Zone 7: Noise Report — first run only, dismissable."""
    noise_senders = _load_json("data/noise_senders_found.json", {})
    if noise_senders.get("dismissed"):
        return ""

    senders = noise_senders.get("senders", [])[:15]
    if not senders:
        return ""

    rows = "".join(
        f'<div style="padding:3px 0;color:#94a3b8;font-size:12px">'
        f'{escape(s.get("sender",""))} <span style="color:#6b7280">({s.get("count",0)})</span>'
        f'</div>'
        for s in senders
    )

    return (
        f'<div id="zone7" style="background:#1a1a0a;border:2px solid #854d0e;'
        f'border-radius:8px;padding:14px;margin-top:24px">'
        f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">'
        f'<span style="color:#fbbf24;font-weight:700;font-size:14px">📭 Noise Report (First Run)</span>'
        f'<button onclick="document.getElementById(\'zone7\').style.display=\'none\'" '
        f'style="background:#374151;color:#9ca3af;border:none;padding:3px 10px;'
        f'border-radius:4px;cursor:pointer;font-size:12px">Dismiss</button></div>'
        f'<div style="color:#a3a3a3;font-size:12px;margin-bottom:8px">'
        f'These senders were auto-filtered as noise. Review and update email_config.json if any are wrong.</div>'
        f'{rows}</div>'
    )


# ── Main builder ──────────────────────────────────────────────────────────

def build_debrief(
    categorized_emails: list[dict],
    noise_report: dict,
    drafts: list[dict],
    overdue: list[dict],
    sentiment_alerts: list[dict],
    is_friday: bool = False,
) -> str:
    """
    Build the full brad-debrief.html string.
    """
    sent_log   = _load_json("data/sent_log.json", [])
    heat_map   = _load_json("data/brand_heat_map.json", [])
    pip_state  = _load_json("data/pipeline_state.json", {})

    now      = datetime.now(timezone.utc)
    day_name = now.strftime("%A")
    date_str = now.strftime("%A, %B %-d, %Y")

    stats = {
        "total_received": noise_report.get("total_received", len(categorized_emails)),
        "noise_count":    noise_report.get("noise_count", 0),
        "drafts_created": len(drafts),
    }

    # Find if any priority manufacturer is in zone 2
    priority_brand = ""
    for e in categorized_emails:
        if e.get("urgency") == 1 and e.get("category") == "Manufacturer":
            priority_brand = e.get("crm_context", {}).get("company", "") or e.get("sender_name", "")
            break

    # Voice model line
    edit_count = pip_state.get("total_edits_since_build", "")
    rebuild_scheduled = pip_state.get("rebuild_profile", False)
    voice_model_line  = ""
    if edit_count:
        status = "scheduled" if rebuild_scheduled else "up to date"
        voice_model_line = (
            f'<div style="color:#6b7280;font-size:11px;margin-top:4px">'
            f'Voice model: {edit_count} edits captured — profile update {status}'
            f'</div>'
        )

    # ── Body sections ──
    if is_friday:
        main_content = _build_friday_content(
            categorized_emails, overdue, sentiment_alerts, heat_map, pip_state
        )
    else:
        main_content = (
            _zone_2_top3(categorized_emails) +
            _zone_2_5_radar(categorized_emails, overdue, sentiment_alerts)
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">
<title>Brad's Debrief — {escape(date_str)}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #0a1628; color: #e2e8f0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }}
  #pin-gate {{ position: fixed; inset: 0; background: #0F2942; display: flex; flex-direction: column; align-items: center; justify-content: center; z-index: 999; }}
  #pin-gate h1 {{ color: #C8963E; font-size: 22px; margin-bottom: 8px; }}
  #pin-gate p {{ color: #6b7280; font-size: 14px; margin-bottom: 20px; }}
  #pin-input {{ font-size: 24px; letter-spacing: 8px; text-align: center; background: #1e2d3d; color: #fff; border: 2px solid #334155; border-radius: 8px; padding: 12px 20px; width: 160px; -webkit-text-security: disc; }}
  #pin-error {{ color: #f87171; font-size: 13px; margin-top: 8px; min-height: 20px; }}
  #main-content {{ display: none; max-width: 600px; margin: 0 auto; padding: 16px 14px 40px; }}
  details summary::-webkit-details-marker {{ display: none; }}
  a {{ color: #60a5fa; }}
</style>
</head>
<body>

<div id="pin-gate">
  <h1>Bormann Debrief</h1>
  <p>{escape(date_str)}</p>
  <input id="pin-input" type="password" inputmode="numeric" pattern="[0-9]*"
         maxlength="4" placeholder="••••" autofocus
         oninput="if(this.value.length===4)checkPin(this.value)">
  <div id="pin-error"></div>
</div>

<div id="main-content">
  <!-- HEADER -->
  <div style="background:#0F2942;margin:-16px -14px 20px;padding:16px 14px;
              border-bottom:3px solid #C8963E">
    <div style="display:flex;justify-content:space-between;align-items:flex-start">
      <div>
        <div style="color:#C8963E;font-weight:800;font-size:18px;letter-spacing:.5px">
          BORMANN DEBRIEF
        </div>
        <div style="color:#6b7280;font-size:12px">{escape(date_str)}</div>
        {voice_model_line}
      </div>
      <div style="text-align:right;font-size:11px;color:#4b5563">
        <div>{stats["total_received"]} emails</div>
        <div>{stats["noise_count"]} filtered</div>
        <div>{stats["drafts_created"]} drafts</div>
      </div>
    </div>
    {f'<div style="margin-top:8px"><span style="background:#C8963E;color:#fff;padding:3px 10px;border-radius:4px;font-size:12px;font-weight:700">⚑ {escape(priority_brand)}</span></div>' if priority_brand else ""}
  </div>

  {_zone_0_brief(drafts)}
  {main_content}
  {_zone_3_crm(categorized_emails)}
  {_zone_4_drafts(drafts)}
  {_zone_5_summary(categorized_emails)}
  {_zone_6_tasks_log(categorized_emails, sent_log)}
  {_zone_7_noise(noise_report)}

  <div style="color:#374151;font-size:10px;text-align:center;margin-top:32px;padding-top:16px;border-top:1px solid #1e2d3d">
    Bormann Marketing Email Intelligence v3 · Generated {now.strftime("%H:%M UTC")}
  </div>
</div>

<script>
const CORRECT_PIN = "{DEFAULT_PIN}";
function checkPin(val) {{
  if (val === CORRECT_PIN) {{
    document.getElementById('pin-gate').style.display = 'none';
    document.getElementById('main-content').style.display = 'block';
    sessionStorage.setItem('debrief_auth', '1');
  }} else if (val.length === 4) {{
    document.getElementById('pin-error').textContent = 'Wrong PIN';
    document.getElementById('pin-input').value = '';
    setTimeout(() => {{ document.getElementById('pin-error').textContent = ''; }}, 2000);
  }}
}}
// Auto-unlock if already authed this session
if (sessionStorage.getItem('debrief_auth') === '1') {{
  document.getElementById('pin-gate').style.display = 'none';
  document.getElementById('main-content').style.display = 'block';
}}
</script>
</body>
</html>"""

    # Write file
    try:
        with open("brad-debrief.html", "w", encoding="utf-8") as f:
            f.write(html)
        logger.info("brad-debrief.html written successfully.")
    except Exception as e:
        logger.error(f"Could not write brad-debrief.html: {e}")

    return html


def _build_friday_content(
    emails: list[dict],
    overdue: list[dict],
    sentiment_alerts: list[dict],
    heat_map: list[dict],
    pip_state: dict,
) -> str:
    """Friday-specific: Week in Review + Brand Health."""
    # Week in Review (replaces Zones 2 and 2.5)
    sent_log = _load_json("data/sent_log.json", [])
    week_log = sent_log[-20:]  # last 20 entries as proxy for week

    log_items = "".join(
        f'<div style="padding:5px 0;border-bottom:1px solid #2d3f50;'
        f'color:#94a3b8;font-size:12px;font-style:italic">{escape(e.get("log_entry",""))}</div>'
        for e in week_log
    )

    week_review = (
        _section_header("Week in Review", color="#93c5fd") +
        _zone_2_5_radar(emails, overdue, sentiment_alerts) +
        _collapsible("What I Communicated This Week", log_items, open_=True, badge=f"{len(week_log)}")
    )

    # Brand Health
    if heat_map:
        brand_rows = []
        for b in heat_map:
            signal = b.get("signal", "")
            color  = "#1A7A4A" if "Up" in signal else ("#B91C1C" if "Down" in signal else "#2563EB")
            if signal == "baseline":
                color = "#6b7280"
            brand_rows.append(
                f'<div style="display:flex;justify-content:space-between;align-items:flex-start;'
                f'padding:7px 0;border-bottom:1px solid #2d3f50">'
                f'<div style="flex:1">'
                f'<strong style="color:#e2e8f0;font-size:13px">{escape(b.get("brand",""))}</strong><br>'
                f'<span style="color:#94a3b8;font-size:12px">{escape(b.get("one_line_note",""))}</span>'
                f'</div>'
                f'<span style="background:{color};color:#fff;padding:2px 8px;border-radius:4px;'
                f'font-size:11px;font-weight:600;white-space:nowrap;margin-left:8px">'
                f'{escape(signal)}</span></div>'
            )
        brand_health = (
            _section_header("Brand Health", color="#C8963E", badge_text="NEW") +
            _collapsible("All Brands", "".join(brand_rows), open_=True,
                         badge=f"{len(heat_map)} tracked")
        )
    else:
        brand_health = ""

    return week_review + brand_health
