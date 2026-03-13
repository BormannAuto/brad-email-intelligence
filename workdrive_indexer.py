"""
workdrive_indexer.py
Bormann Marketing — Email Intelligence System v3  (NEW)
Weekly runner — called by .github/workflows/workdrive_index.yml on Sundays at 6am CT.
NOT called by run_pipeline.py.

Rebuilds workdrive_index.json from all brand folders in email_config.json.
"""

import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from zoho_workdrive_connector import authenticate_workdrive, list_brand_folder, fetch_file_as_text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [workdrive_indexer] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

CHUNK_SIZE    = 500    # words per chunk
CHUNK_OVERLAP = 100    # word overlap between chunks
INDEX_FILE    = "workdrive_index.json"


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------

def _chunk_text(text: str, file_size_hint: str = "") -> list[str]:
    """
    Split text into ~500-word chunks with 100-word overlap.
    Returns list of text strings.
    """
    words  = text.split()
    chunks = []
    start  = 0
    while start < len(words):
        end   = min(start + CHUNK_SIZE, len(words))
        chunk = " ".join(words[start:end])
        chunks.append(chunk)
        if end >= len(words):
            break
        start = end - CHUNK_OVERLAP  # overlap
    return chunks


# ---------------------------------------------------------------------------
# Main indexer
# ---------------------------------------------------------------------------

def build_index() -> None:
    """
    Rebuild workdrive_index.json from all brand folders.
    Replaces entire file on each run.
    """
    start_time = datetime.now(timezone.utc)
    logger.info("=== WorkDrive Index Rebuild Started ===")

    # Auth
    wd_session = authenticate_workdrive()
    if wd_session is None:
        logger.error("WorkDrive not configured — aborting index rebuild.")
        sys.exit(1)

    # Load brand folder map
    try:
        with open("email_config.json") as f:
            config = json.load(f)
        brand_folders = config.get("workdrive_brand_folders", {})
    except Exception as e:
        logger.error(f"Could not load email_config.json: {e}")
        sys.exit(1)

    if not brand_folders:
        logger.warning("No brands configured in workdrive_brand_folders — nothing to index.")
        return

    all_chunks    = []
    files_processed = 0
    files_skipped   = {}

    for brand_name in brand_folders:
        logger.info(f"Processing brand: {brand_name}")
        files = list_brand_folder(wd_session, brand_name)

        if not files:
            logger.info(f"  No processable files found for {brand_name}")
            files_skipped.setdefault(brand_name, []).append(
                {"file_name": "(no files)", "reason": "folder empty or inaccessible"}
            )
            continue

        for file_info in files:
            file_id   = file_info["file_id"]
            file_name = file_info["file_name"]
            file_type = file_info["file_type"]

            logger.info(f"  Fetching: {file_name} ({file_type})")
            text = fetch_file_as_text(wd_session, file_id, file_type)

            if not text or not text.strip():
                reason = "extraction returned empty text"
                logger.warning(f"  Skipping {file_name}: {reason}")
                files_skipped.setdefault(brand_name, []).append(
                    {"file_name": file_name, "reason": reason}
                )
                continue

            chunks = _chunk_text(text)
            for idx, chunk_text in enumerate(chunks):
                all_chunks.append({
                    "brand":       brand_name,
                    "file_name":   file_name,
                    "file_id":     file_id,
                    "chunk_index": idx,
                    "text":        chunk_text,
                })
            files_processed += 1
            logger.info(f"  Indexed {file_name}: {len(chunks)} chunks")

    # Write index
    try:
        with open(INDEX_FILE, "w", encoding="utf-8") as f:
            json.dump(all_chunks, f, indent=2, ensure_ascii=False)
        logger.info(f"Wrote {len(all_chunks)} chunks to {INDEX_FILE}")
    except Exception as e:
        logger.error(f"Failed to write {INDEX_FILE}: {e}")
        sys.exit(1)

    # Summary log
    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logger.info("=== WorkDrive Index Rebuild Complete ===")
    logger.info(f"  Files processed : {files_processed}")
    logger.info(f"  Total chunks     : {len(all_chunks)}")
    logger.info(f"  Files skipped    : {sum(len(v) for v in files_skipped.values())}")
    for brand, skips in files_skipped.items():
        for skip in skips:
            logger.info(f"    {brand} / {skip['file_name']}: {skip['reason']}")
    logger.info(f"  Elapsed          : {elapsed:.1f}s")


if __name__ == "__main__":
    build_index()
