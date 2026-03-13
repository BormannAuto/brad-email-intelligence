"""
workdrive_retriever.py
Bormann Marketing — Email Intelligence System v3  (NEW)
Keyword-based retrieval from workdrive_index.json.
No vector embeddings — Phase 1 uses simple token matching.
Returns empty list gracefully if index not yet built.
"""

import json
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

INDEX_FILE = "workdrive_index.json"

# Cache the index in memory for the duration of a pipeline run
_index_cache: Optional[list[dict]] = None


def _load_index() -> list[dict]:
    global _index_cache
    if _index_cache is not None:
        return _index_cache
    try:
        with open(INDEX_FILE, encoding="utf-8") as f:
            _index_cache = json.load(f)
        logger.info(f"WorkDrive index loaded: {len(_index_cache)} chunks.")
    except FileNotFoundError:
        logger.info(
            "workdrive_index.json not found — WorkDrive retrieval disabled until "
            "first weekly index run (Sundays 6am CT)."
        )
        _index_cache = []
    except Exception as e:
        logger.warning(f"Could not load workdrive_index.json: {e}")
        _index_cache = []
    return _index_cache


def _tokenize(text: str) -> set[str]:
    """Simple token extraction — lowercase alphanumeric words, 3+ chars."""
    return set(
        w for w in re.findall(r"[a-z0-9][a-z0-9\-\.]{2,}", text.lower())
        if len(w) >= 3
    )


def _score_chunk(chunk: dict, brand_tokens: set, query_tokens: set) -> int:
    """
    Score a chunk against brand + query tokens.
    Model/part numbers (likely alphanumeric with digits) get bonus weight.
    """
    chunk_tokens = _tokenize(chunk.get("text", ""))
    brand_hits   = len(chunk_tokens & brand_tokens)
    query_hits   = len(chunk_tokens & query_tokens)

    # Bonus for model/part number matches (tokens with digits)
    model_tokens = {t for t in query_tokens if any(c.isdigit() for c in t)}
    model_hits   = len(chunk_tokens & model_tokens) * 3  # 3x weight

    return brand_hits + query_hits + model_hits


def retrieve_product_context(
    brand: str,
    query: str,
    max_chunks: int = 3,
) -> list[dict]:
    """
    Search workdrive_index.json for chunks matching brand and query keywords.
    Returns [{brand, file_name, chunk_text}] or empty list if no match
    or if index not yet built.
    """
    index = _load_index()
    if not index:
        return []

    brand_tokens = _tokenize(brand)
    query_tokens = _tokenize(query)

    if not query_tokens:
        return []

    # Filter to brand-relevant chunks first
    brand_lower = brand.lower()
    candidates  = [
        c for c in index
        if c.get("brand", "").lower() == brand_lower
        or brand_lower in c.get("file_name", "").lower()
    ]

    if not candidates:
        # Fallback: search all chunks
        logger.debug(
            f"No exact brand match for '{brand}' in index — searching all chunks."
        )
        candidates = index

    # Score and rank
    scored = [
        (chunk, _score_chunk(chunk, brand_tokens, query_tokens))
        for chunk in candidates
    ]
    scored.sort(key=lambda x: x[1], reverse=True)

    # Return top N with score > 0
    results = []
    for chunk, score in scored[:max_chunks * 2]:  # grab extra, deduplicate by file
        if score == 0:
            break
        results.append({
            "brand":      chunk.get("brand", ""),
            "file_name":  chunk.get("file_name", ""),
            "chunk_text": chunk.get("text", ""),
        })
        if len(results) >= max_chunks:
            break

    if results:
        logger.info(
            f"WorkDrive retrieval: {len(results)} chunks for brand='{brand}', "
            f"query='{query[:60]}'"
        )
    else:
        logger.debug(f"No WorkDrive hits for brand='{brand}', query='{query[:60]}'")

    return results
