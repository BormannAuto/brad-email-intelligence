# src/zdi_mw/clients/safe_workdrive.py
# ZDI Middleware — SafeWorkDriveClient
#
# safe_lookup_chunk() — 5 gates before injecting WorkDrive content into Claude:
#   Gate 1: Freshness — index_age_days <= 7
#   Gate 2: Brand tag exact match — no cross-brand injection
#   Gate 3: Confidence != LOW
#   Gate 4: All numeric values in chunk traceable to raw_source_values
#   Gate 5: Chunk not truncated (does not end with ,  |  -  or  ...)
#   → Returns verified chunk dict or None with block_reason logged
#
# Injectable dependencies:
#   _workdrive_lookup_fn: (brand, model, inquiry_text) -> Optional[dict]
#       Expected chunk dict keys:
#           brand_tag: str, confidence: str, content: str,
#           index_age_days: int, raw_source_values: list[str],
#           chunk_id: str
#   _rate_manager, _auth_manager
#
# LOGGING RULE: never log chunk content, inquiry text, or brand names in full.
# Log only: chunk_id prefix, brand_tag hash, gate that blocked, outcome.

import hashlib
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

_CONFIDENCE_LOW = "LOW"
_MAX_INDEX_AGE_DAYS = 7
_TRUNCATION_ENDINGS = re.compile(r"[,|\-]$|\.{2,}$|\u2026$")


@dataclass
class WorkDriveChunkResult:
    """Result of a safe_lookup_chunk() call."""
    chunk: Optional[Dict]
    blocked: bool = False
    block_reason: str = ""


class SafeWorkDriveClient:
    """
    Gate-enforced WorkDrive chunk injection client.

    Returns a verified chunk dict suitable for injection into Claude context,
    or None if any gate fails.

    Injectable dependencies:
        _workdrive_lookup_fn: (brand, model, inquiry_text) -> Optional[dict]
    """

    def __init__(
        self,
        run_context: Any,
        _workdrive_lookup_fn: Optional[Callable[[str, str, str], Optional[Dict]]] = None,
        _rate_manager: Optional[Any] = None,
        _auth_manager: Optional[Any] = None,
    ) -> None:
        self._run_context = run_context
        self._workdrive_lookup_fn = _workdrive_lookup_fn
        self._rate_manager = _rate_manager
        self._auth_manager = _auth_manager

    def safe_lookup_chunk(
        self,
        brand: str,
        model: str,
        inquiry_text: str,
    ) -> WorkDriveChunkResult:
        """
        Look up a WorkDrive product chunk and verify it passes all 5 safety gates.

        Args:
            brand:         Exact brand name to match against chunk brand_tag.
            model:         Product model string for lookup.
            inquiry_text:  Customer inquiry text (used for relevance lookup).

        Returns:
            WorkDriveChunkResult with chunk dict or None + block_reason.
        """
        if self._workdrive_lookup_fn is None:
            logger.warning("SafeWorkDrive: no lookup function configured — returning None")
            return WorkDriveChunkResult(chunk=None, blocked=True, block_reason="NO_LOOKUP_FN")

        # Rate check
        if self._rate_manager is not None:
            try:
                self._rate_manager.check_and_wait("workdrive", estimated_cost=0.5)
            except Exception as exc:
                logger.warning("SafeWorkDrive: rate blocked exc=%s", exc)
                return WorkDriveChunkResult(chunk=None, blocked=True, block_reason=f"RATE_BLOCKED:{exc}")

        # Fetch chunk from WorkDrive index
        try:
            raw = self._workdrive_lookup_fn(brand, model, inquiry_text)
        except Exception as exc:
            logger.error("SafeWorkDrive: lookup failed exc=%s", type(exc).__name__)
            return WorkDriveChunkResult(chunk=None, blocked=True, block_reason=f"LOOKUP_ERROR:{type(exc).__name__}")

        if raw is None:
            logger.info("SafeWorkDrive: no chunk found brand_hash=%s", _prefix(brand))
            return WorkDriveChunkResult(chunk=None, blocked=False, block_reason="NO_CHUNK_FOUND")

        chunk_id = raw.get("chunk_id", "unknown")
        cid_prefix = _prefix(chunk_id)

        # Gate 1: Freshness
        index_age_days = raw.get("index_age_days", 999)
        if index_age_days > _MAX_INDEX_AGE_DAYS:
            logger.info(
                "SafeWorkDrive: BLOCKED gate=1_freshness chunk=%s age_days=%d",
                cid_prefix, index_age_days,
            )
            return WorkDriveChunkResult(
                chunk=None, blocked=True,
                block_reason=f"STALE_INDEX:age_days={index_age_days}",
            )

        # Gate 2: Brand tag exact match (no cross-brand injection)
        chunk_brand = raw.get("brand_tag", "")
        if chunk_brand.strip().lower() != brand.strip().lower():
            logger.info(
                "SafeWorkDrive: BLOCKED gate=2_brand_mismatch chunk=%s", cid_prefix
            )
            return WorkDriveChunkResult(
                chunk=None, blocked=True, block_reason="BRAND_MISMATCH"
            )

        # Gate 3: Confidence
        confidence = raw.get("confidence", "LOW")
        if confidence == _CONFIDENCE_LOW:
            logger.info(
                "SafeWorkDrive: BLOCKED gate=3_confidence chunk=%s", cid_prefix
            )
            return WorkDriveChunkResult(
                chunk=None, blocked=True, block_reason="CONFIDENCE_LOW"
            )

        # Gate 4: Numeric values traceable to raw_source_values
        content = raw.get("content", "")
        raw_source_values: List[str] = raw.get("raw_source_values", [])
        untraceable = _find_untraceable_numerics(content, raw_source_values)
        if untraceable:
            logger.info(
                "SafeWorkDrive: BLOCKED gate=4_numeric_trace chunk=%s untraceable_count=%d",
                cid_prefix, len(untraceable),
            )
            return WorkDriveChunkResult(
                chunk=None, blocked=True,
                block_reason=f"UNTRACEABLE_NUMERICS:count={len(untraceable)}",
            )

        # Gate 5: Chunk not truncated
        if _is_truncated(content):
            logger.info(
                "SafeWorkDrive: BLOCKED gate=5_truncated chunk=%s", cid_prefix
            )
            return WorkDriveChunkResult(
                chunk=None, blocked=True, block_reason="CHUNK_TRUNCATED"
            )

        logger.info("SafeWorkDrive: chunk VERIFIED chunk=%s age_days=%d", cid_prefix, index_age_days)
        return WorkDriveChunkResult(chunk=raw, blocked=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_untraceable_numerics(content: str, raw_source_values: List[str]) -> List[str]:
    """
    Extract all numeric-looking values (including dollar amounts) from content.
    Return those that do NOT appear verbatim in any raw_source_value.

    Numeric pattern: optional $ prefix, digits with optional comma separators,
    optional decimal. E.g.: $1,200.00, 450, $99.
    """
    numeric_pattern = re.compile(r"(?<![A-Za-z0-9])\$?\d+(?:,\d{3})*(?:\.\d+)?(?![A-Za-z0-9])")
    found = numeric_pattern.findall(content)

    untraceable = []
    for num in found:
        # Strip whitespace, check verbatim presence in any source value
        if not any(num.strip() in sv for sv in raw_source_values):
            untraceable.append(num)

    return untraceable


def _is_truncated(content: str) -> bool:
    """Return True if the chunk content appears to be cut off mid-sentence."""
    stripped = content.strip()
    if not stripped:
        return False
    return bool(_TRUNCATION_ENDINGS.search(stripped))


def _prefix(value: str) -> str:
    """First 8 chars of sha256(value) — safe to log."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]
