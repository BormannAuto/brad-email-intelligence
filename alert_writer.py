"""
alert_writer.py
Bormann Marketing — Email Intelligence System v3
Pipeline alert mechanism for critical failures and operational anomalies.

Security rationale: A pipeline that fails silently is worse than one that fails loudly.
This module provides a lightweight alert channel that doesn't require external
infrastructure (no email, no Slack, no webhook). Primary: write a JSON alert file
that GitHub Actions can detect and surface in the workflow summary.

Alert levels:
  CRITICAL — pipeline aborted, debrief not updated (Brad sees no debrief)
  HIGH     — partial failure, debrief generated but missing content
  WARN     — anomaly detected, pipeline completed, Brad may want to investigate

Alert file: data/pipeline_alerts.json
  - Appended to each run (not overwritten)
  - Rolling 30 entries (oldest dropped when limit exceeded)
  - GitHub Actions workflow summary: prints any CRITICAL/HIGH alerts to the job summary

Usage:
    from alert_writer import write_alert, AlertLevel
    write_alert(AlertLevel.CRITICAL, "auth_failure", "Zoho Mail token refresh failed")
    write_alert(AlertLevel.HIGH, "cap_reached", "Claude API cap reached at 50 calls")
    write_alert(AlertLevel.WARN, "crm_miss_rate", "CRM lookup miss rate 80% (expected <40%)")
"""

import json
import logging
import os
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)

ALERT_FILE        = os.path.join("data", "pipeline_alerts.json")
MAX_ALERT_ENTRIES = 30   # Rolling window — keep last 30 alerts


class AlertLevel(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH     = "HIGH"
    WARN     = "WARN"


def write_alert(
    level:   AlertLevel,
    code:    str,
    message: str,
    run_id:  Optional[str] = None,
) -> None:
    """
    Write a pipeline alert to data/pipeline_alerts.json.
    Thread-safe via file read-modify-write (single process per pipeline run).
    Never raises — alert failures must not abort the pipeline.

    Args:
        level:   AlertLevel.CRITICAL / HIGH / WARN
        code:    Short machine-readable code (e.g. "auth_failure", "cap_reached")
        message: Human-readable description of the alert
        run_id:  Optional pipeline run ID (YYYY-MM-DD) for deduplication tracking
    """
    try:
        _append_alert(level, code, message, run_id)
        _write_to_github_summary(level, code, message)
    except Exception as e:
        # Alert writing must NEVER abort the pipeline
        logger.error(f"[alert_writer] Failed to write alert ({code}): {e}")


def _append_alert(
    level: AlertLevel,
    code: str,
    message: str,
    run_id: Optional[str],
) -> None:
    """Append alert entry to pipeline_alerts.json, trimming to MAX_ALERT_ENTRIES."""
    os.makedirs("data", exist_ok=True)

    # Load existing alerts
    existing = []
    if os.path.exists(ALERT_FILE):
        try:
            with open(ALERT_FILE) as f:
                existing = json.load(f)
            if not isinstance(existing, list):
                existing = []
        except (json.JSONDecodeError, IOError):
            existing = []

    # Build new entry
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "run_id":    run_id or "unknown",
        "level":     str(level.value),
        "code":      code,
        "message":   message,
    }

    existing.append(entry)

    # Trim rolling window
    if len(existing) > MAX_ALERT_ENTRIES:
        existing = existing[-MAX_ALERT_ENTRIES:]

    # Write back
    with open(ALERT_FILE, "w") as f:
        json.dump(existing, f, indent=2)

    logger.warning(f"[alert_writer] {level.value} alert written: {code} — {message}")


def _write_to_github_summary(
    level: AlertLevel,
    code: str,
    message: str,
) -> None:
    """
    Append alert to GitHub Actions job summary ($GITHUB_STEP_SUMMARY).
    This surfaces alerts directly in the GitHub Actions workflow run UI.
    No-ops if not running in GitHub Actions.
    """
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_file:
        return

    icon = {"CRITICAL": "🔴", "HIGH": "🟠", "WARN": "🟡"}.get(str(level.value), "⚪")
    line = f"{icon} **{level.value}** `{code}`: {message}\n"

    try:
        with open(summary_file, "a") as f:
            f.write(line)
    except Exception as e:
        logger.debug(f"[alert_writer] Could not write to GITHUB_STEP_SUMMARY: {e}")


# ---------------------------------------------------------------------------
# Convenience wrappers
# ---------------------------------------------------------------------------

def alert_critical(code: str, message: str, run_id: Optional[str] = None) -> None:
    write_alert(AlertLevel.CRITICAL, code, message, run_id)


def alert_high(code: str, message: str, run_id: Optional[str] = None) -> None:
    write_alert(AlertLevel.HIGH, code, message, run_id)


def alert_warn(code: str, message: str, run_id: Optional[str] = None) -> None:
    write_alert(AlertLevel.WARN, code, message, run_id)


# ---------------------------------------------------------------------------
# Drift alert helper (used by run_pipeline.py debrief section)
# ---------------------------------------------------------------------------

def check_drift_and_alert(run_id: str, pipeline_stats: dict) -> None:
    """
    Compare pipeline stats against pipeline_drift_config.json baselines.
    Writes WARN alerts for any metric that exceeds its threshold.

    Args:
        run_id:         Current run ID (YYYY-MM-DD)
        pipeline_stats: Dict with keys like email_count, draft_count, crm_miss_rate, etc.
    """
    drift_config_path = os.path.join("data", "pipeline_drift_config.json")
    if not os.path.exists(drift_config_path):
        logger.debug("[alert_writer] No pipeline_drift_config.json — skipping drift check.")
        return

    try:
        with open(drift_config_path) as f:
            config = json.load(f)
    except Exception as e:
        logger.warning(f"[alert_writer] Could not load pipeline_drift_config.json: {e}")
        return

    baselines = config.get("baselines", {})
    thresholds = config.get("thresholds", {})

    if not baselines or not thresholds:
        # Config exists but isn't calibrated yet — this is expected for first 14 days
        return

    for metric, current_value in pipeline_stats.items():
        if metric not in baselines or metric not in thresholds:
            continue
        baseline  = baselines[metric]
        threshold = thresholds[metric]

        if baseline == 0:
            continue

        deviation = abs(current_value - baseline) / baseline
        if deviation > threshold:
            alert_warn(
                code    = f"drift_{metric}",
                message = (
                    f"Metric '{metric}' deviated {deviation:.0%} from baseline "
                    f"(current={current_value}, baseline={baseline}, "
                    f"threshold={threshold:.0%}). Investigate if persistent."
                ),
                run_id  = run_id,
            )
