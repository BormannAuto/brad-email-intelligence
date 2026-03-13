"""
heat_map.py
Bormann Marketing — Email Intelligence System v3  (NEW)
Friday-only runner. Builds brand activity heat map from 30-day email activity log.
Appended to by run_pipeline.py on every run. Heat map computed on Fridays.
"""

import json
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

EMAIL_ACTIVITY_LOG  = "data/email_activity_log.json"
BRAND_BASELINE_FILE = "data/brand_baseline.json"
BRAND_HEAT_MAP_FILE = "data/brand_heat_map.json"
BASELINE_DAYS       = 30   # days needed to build baseline
BASELINE_WINDOW     = 90   # days for baseline comparison window
ACTIVE_THRESHOLD    = 0.20  # >20% frequency change = Up/Down signal
SENTIMENT_THRESHOLD = 0.15  # sentiment shift that matters


# ---------------------------------------------------------------------------
# Daily activity logging (called every pipeline run, not just Fridays)
# ---------------------------------------------------------------------------

def log_daily_activity(categorized_emails: list[dict]) -> None:
    """
    Append today's per-brand email activity to email_activity_log.json.
    Called from run_pipeline.py every morning.
    """
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Aggregate per brand
    brand_data: dict[str, dict] = {}
    for email in categorized_emails:
        brand = _extract_brand(email)
        if not brand:
            continue
        if brand not in brand_data:
            brand_data[brand] = {"received": 0, "sentiment_scores": []}
        brand_data[brand]["received"] += 1
        score = email.get("sentiment_score")
        if score is not None:
            brand_data[brand]["sentiment_scores"].append(float(score))

    # Build per_brand_counts
    per_brand_counts = {}
    for brand, data in brand_data.items():
        scores = data["sentiment_scores"]
        per_brand_counts[brand] = {
            "received":      data["received"],
            "avg_sentiment": round(sum(scores) / len(scores), 3) if scores else 0.5,
        }

    # Load and append
    log = _load_activity_log()
    # Remove any existing entry for today (idempotent)
    log = [e for e in log if e.get("date") != today_str]
    log.append({"date": today_str, "per_brand_counts": per_brand_counts})

    # Keep rolling 90-day window
    cutoff = (datetime.now(timezone.utc) - timedelta(days=91)).strftime("%Y-%m-%d")
    log = [e for e in log if e.get("date", "") >= cutoff]

    _save_activity_log(log)
    logger.debug(f"Logged daily brand activity for {len(per_brand_counts)} brands.")


def _load_activity_log() -> list[dict]:
    try:
        with open(EMAIL_ACTIVITY_LOG) as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    except Exception as e:
        logger.warning(f"Could not load email_activity_log.json: {e}")
        return []


def _save_activity_log(log: list[dict]) -> None:
    os.makedirs("data", exist_ok=True)
    try:
        with open(EMAIL_ACTIVITY_LOG, "w") as f:
            json.dump(log, f, indent=2)
    except Exception as e:
        logger.error(f"Could not save email_activity_log.json: {e}")


# ---------------------------------------------------------------------------
# Heat map computation (Fridays only)
# ---------------------------------------------------------------------------

def build_heat_map() -> list[dict]:
    """
    Compute brand heat map from activity log.
    Returns list of brand heat map entries and writes brand_heat_map.json.
    """
    log = _load_activity_log()
    if not log:
        logger.info("No activity log data yet — cannot build heat map.")
        return []

    today  = datetime.now(timezone.utc)
    cutoff = (today - timedelta(days=BASELINE_DAYS)).strftime("%Y-%m-%d")
    enough_data = len([e for e in log if e.get("date", "") >= cutoff]) >= BASELINE_DAYS

    if not enough_data:
        logger.info(
            f"Less than {BASELINE_DAYS} days of data — writing 'baseline' signals."
        )
        return _write_baseline_signals(log)

    # Build or load 90-day baseline
    baseline = _compute_baseline(log)
    _save_baseline(baseline)

    # Compute recent 30-day window
    recent_cutoff = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    recent_log    = [e for e in log if e.get("date", "") >= recent_cutoff]

    heat_map = []
    all_brands = set(baseline.keys())
    for entry in recent_log:
        all_brands |= set(entry.get("per_brand_counts", {}).keys())

    for brand in sorted(all_brands):
        recent_counts    = [
            e["per_brand_counts"].get(brand, {}).get("received", 0)
            for e in recent_log
        ]
        recent_sentiments = [
            e["per_brand_counts"].get(brand, {}).get("avg_sentiment")
            for e in recent_log
            if e["per_brand_counts"].get(brand, {}).get("avg_sentiment") is not None
        ]

        recent_avg_freq      = sum(recent_counts) / max(len(recent_counts), 1)
        recent_avg_sentiment = (
            sum(recent_sentiments) / len(recent_sentiments)
            if recent_sentiments else 0.5
        )

        base = baseline.get(brand, {})
        base_freq      = base.get("avg_daily_received", 0)
        base_sentiment = base.get("avg_sentiment", 0.5)

        # Score
        if base_freq == 0 and recent_avg_freq == 0:
            continue  # no activity at all

        freq_change = (
            (recent_avg_freq - base_freq) / base_freq
            if base_freq > 0 else 1.0
        )
        sentiment_shift = base_sentiment - recent_avg_sentiment  # positive = colder

        signal, note = _classify_signal(
            brand, freq_change, sentiment_shift,
            recent_avg_freq, base_freq,
            recent_avg_sentiment, base_sentiment,
        )

        heat_map.append({
            "brand":       brand,
            "signal":      signal,
            "one_line_note": note,
            "trend_data": {
                "recent_avg_daily":    round(recent_avg_freq, 2),
                "baseline_avg_daily":  round(base_freq, 2),
                "freq_change_pct":     round(freq_change * 100, 1),
                "recent_sentiment":    round(recent_avg_sentiment, 3),
                "baseline_sentiment":  round(base_sentiment, 3),
                "sentiment_shift":     round(sentiment_shift, 3),
            }
        })

    # Sort: Down first, then Stable, then Up
    sort_order = {"Down - Cooling": 0, "Stable": 1, "Up - Active": 2, "baseline": 3}
    heat_map.sort(key=lambda x: sort_order.get(x.get("signal", "Stable"), 99))

    _save_heat_map(heat_map)
    logger.info(
        f"Brand heat map built: {len(heat_map)} brands. "
        f"Up: {sum(1 for b in heat_map if 'Up' in b['signal'])}, "
        f"Stable: {sum(1 for b in heat_map if 'Stable' == b['signal'])}, "
        f"Cooling: {sum(1 for b in heat_map if 'Down' in b['signal'])}."
    )
    return heat_map


def _classify_signal(
    brand: str,
    freq_change: float,
    sentiment_shift: float,
    recent_freq: float,
    base_freq: float,
    recent_sent: float,
    base_sent: float,
) -> tuple[str, str]:
    """Classify brand as Up / Stable / Cooling and generate one-line note."""

    if freq_change > ACTIVE_THRESHOLD and recent_sent >= base_sent - SENTIMENT_THRESHOLD:
        signal = "Up - Active"
        note   = (
            f"{brand} is more active than usual "
            f"(+{freq_change*100:.0f}% emails vs. baseline)."
        )

    elif freq_change < -ACTIVE_THRESHOLD or sentiment_shift > SENTIMENT_THRESHOLD:
        signal = "Down - Cooling"
        reasons = []
        if freq_change < -ACTIVE_THRESHOLD:
            reasons.append(f"email volume down {abs(freq_change)*100:.0f}% vs. baseline")
        if sentiment_shift > SENTIMENT_THRESHOLD:
            reasons.append(f"tone shift detected (sentiment {recent_sent:.2f} vs. usual {base_sent:.2f})")
        note = f"{brand}: {' and '.join(reasons)}. Consider a check-in."

    else:
        signal = "Stable"
        note   = f"{brand} relationship is tracking normally."

    return signal, note


def _write_baseline_signals(log: list[dict]) -> list[dict]:
    """Pre-baseline: mark all brands as 'baseline' (collecting data)."""
    brands = set()
    for entry in log:
        brands |= set(entry.get("per_brand_counts", {}).keys())

    heat_map = [
        {
            "brand":         brand,
            "signal":        "baseline",
            "one_line_note": f"{brand}: collecting baseline data.",
            "trend_data":    {},
        }
        for brand in sorted(brands)
    ]
    _save_heat_map(heat_map)
    return heat_map


def _compute_baseline(log: list[dict]) -> dict:
    """Compute 90-day rolling baseline per brand."""
    baseline: dict[str, dict] = {}
    today = datetime.now(timezone.utc)
    b_cutoff = (today - timedelta(days=BASELINE_WINDOW)).strftime("%Y-%m-%d")
    b_log    = [e for e in log if e.get("date", "") >= b_cutoff]
    days     = max(len(b_log), 1)

    brand_totals: dict[str, dict] = {}
    for entry in b_log:
        for brand, data in entry.get("per_brand_counts", {}).items():
            if brand not in brand_totals:
                brand_totals[brand] = {"total_received": 0, "sentiments": []}
            brand_totals[brand]["total_received"] += data.get("received", 0)
            s = data.get("avg_sentiment")
            if s is not None:
                brand_totals[brand]["sentiments"].append(s)

    for brand, totals in brand_totals.items():
        sentiments = totals["sentiments"]
        baseline[brand] = {
            "avg_daily_received": round(totals["total_received"] / days, 3),
            "avg_sentiment": round(
                sum(sentiments) / len(sentiments) if sentiments else 0.5, 3
            ),
        }
    return baseline


def _save_baseline(baseline: dict) -> None:
    os.makedirs("data", exist_ok=True)
    try:
        with open(BRAND_BASELINE_FILE, "w") as f:
            json.dump(baseline, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not save brand_baseline.json: {e}")


def _save_heat_map(heat_map: list[dict]) -> None:
    os.makedirs("data", exist_ok=True)
    try:
        with open(BRAND_HEAT_MAP_FILE, "w") as f:
            json.dump(heat_map, f, indent=2)
    except Exception as e:
        logger.error(f"Could not save brand_heat_map.json: {e}")


def _extract_brand(email: dict) -> Optional[str]:
    """Extract manufacturer brand from email context."""
    crm = email.get("crm_context", {})
    company = crm.get("company", "")
    if company:
        return company
    category = email.get("category", "")
    if category == "Manufacturer":
        domain = email.get("sender_email", "").split("@")[-1]
        parts  = domain.split(".")
        return parts[0].title() if parts else None
    return None
