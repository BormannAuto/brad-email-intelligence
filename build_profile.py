"""
build_profile.py
Bormann Marketing — Email Intelligence System v3
Builds (or rebuilds) Brad's voice profile from writing samples and edit log.
Called by run_pipeline.py when edit_tracker triggers a rebuild (10+ edits since last build).
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from anthropic import Anthropic

logger = logging.getLogger(__name__)

SAMPLES_DIR        = "voice/samples"
VOICE_PROFILE_FILE = "voice/brad_voice_profile.json"
EDIT_LOG_FILE      = "data/edit_log.json"
PIPELINE_STATE     = "data/pipeline_state.json"

SYSTEM_PROMPT = """You are analyzing writing samples to build a voice profile for Brad, the owner of \
Bormann Marketing, an 8-person manufacturer's rep firm in Minnesota covering pro AV and low-voltage products.

Your task: analyze ALL samples provided and produce a JSON voice profile capturing Brad's authentic \
communication style — how he actually writes, not how he should write.

Output JSON with these keys:
- "summary": 2-3 sentence overall voice description
- "manufacturer_principal": how Brad writes to manufacturers/principals he represents
- "dealer_customer": how Brad writes to dealers, integrators, end customers
- "internal_team": how Brad writes to his own staff
- "unknown": fallback style for unknown/new contacts
- "do_not_use": list of phrases Brad never uses (you'll infer these from the samples)
- "signature_moves": list of 3-5 stylistic habits (e.g., "starts action items with 'Let's'")
- "built_at": ISO timestamp
- "sample_count": number of samples used
- "edit_count": number of edits ingested

Each profile section should be 3-5 sentences of concrete, specific voice guidance."""


def _load_samples() -> list[str]:
    """Load all .txt files from voice/samples directory."""
    samples = []
    samples_path = Path(SAMPLES_DIR)
    if not samples_path.exists():
        logger.warning(f"Samples directory {SAMPLES_DIR} not found.")
        return samples

    for txt_file in sorted(samples_path.glob("*.txt")):
        try:
            content = txt_file.read_text(encoding="utf-8").strip()
            if content:
                samples.append(f"--- Sample: {txt_file.name} ---\n{content}")
        except Exception as e:
            logger.warning(f"Could not read sample {txt_file}: {e}")

    logger.info(f"Loaded {len(samples)} voice samples from {SAMPLES_DIR}")
    return samples


def _load_edit_diffs() -> list[str]:
    """Load edit diffs from edit_log.json where was_edited=True."""
    diffs = []
    try:
        with open(EDIT_LOG_FILE) as f:
            entries = json.load(f)
        for entry in entries:
            original = entry.get("original_body", "").strip()
            sent     = entry.get("sent_body", "").strip()
            if original and sent and original != sent:
                diff_note = (
                    f"--- Edit Sample (date: {entry.get('date', 'unknown')}) ---\n"
                    f"AI Draft:\n{original}\n\n"
                    f"Brad corrected this draft to say:\n{sent}"
                )
                diffs.append(diff_note)
        logger.info(f"Loaded {len(diffs)} edit samples from edit_log.json")
    except FileNotFoundError:
        logger.info("edit_log.json not found — skipping edit ingestion.")
    except Exception as e:
        logger.warning(f"Could not load edit_log.json: {e}")
    return diffs


def build_profile() -> bool:
    """
    Build Brad's voice profile from samples + edits.
    Returns True on success, False on failure.
    """
    logger.info("=== Building Brad voice profile ===")

    samples = _load_samples()
    edits   = _load_edit_diffs()

    if not samples and not edits:
        logger.warning(
            "No samples or edits found. Add .txt files to voice/samples/ "
            "or accumulate edit_log.json entries before building profile."
        )
        return False

    # Assemble context
    all_material = samples + edits
    full_context = "\n\n".join(all_material)

    # Trim to ~12k tokens worth (rough estimate: 4 chars/token)
    MAX_CHARS = 48_000
    if len(full_context) > MAX_CHARS:
        logger.warning(
            f"Voice material truncated from {len(full_context)} to {MAX_CHARS} chars."
        )
        full_context = full_context[:MAX_CHARS]

    client = Anthropic()
    user_message = (
        f"Here are Brad's writing samples and any edit corrections.\n\n"
        f"{full_context}\n\n"
        f"Build the voice profile JSON. Be specific and concrete — "
        f"extract actual patterns from how Brad writes, not generic business writing advice."
    )

    try:
        response = client.messages.create(
            model="claude-opus-4-5",
            max_tokens=2048,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )
        raw = response.content[0].text.strip()

        # Strip markdown fences
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        profile = json.loads(raw)

    except json.JSONDecodeError as e:
        logger.error(f"build_profile: Claude returned invalid JSON: {e}")
        return False
    except Exception as e:
        logger.error(f"build_profile: Claude API call failed: {e}")
        return False

    # Add metadata
    profile["built_at"]    = datetime.now(timezone.utc).isoformat()
    profile["sample_count"] = len(samples)
    profile["edit_count"]   = len(edits)

    # Write profile
    os.makedirs("voice", exist_ok=True)
    try:
        with open(VOICE_PROFILE_FILE, "w", encoding="utf-8") as f:
            json.dump(profile, f, indent=2, ensure_ascii=False)
        logger.info(
            f"Voice profile written to {VOICE_PROFILE_FILE} "
            f"({len(samples)} samples, {len(edits)} edits ingested)."
        )
    except Exception as e:
        logger.error(f"Could not write voice profile: {e}")
        return False

    # Update pipeline state
    _update_pipeline_state()
    return True


def _update_pipeline_state() -> None:
    """Update last_profile_build_date in pipeline_state.json."""
    os.makedirs("data", exist_ok=True)
    state = {}
    try:
        with open(PIPELINE_STATE) as f:
            state = json.load(f)
    except FileNotFoundError:
        pass
    except Exception:
        pass

    state["last_profile_build_date"] = datetime.now(timezone.utc).isoformat()
    state["rebuild_profile"]         = False

    try:
        with open(PIPELINE_STATE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not update pipeline_state.json: {e}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [build_profile] %(levelname)s %(message)s",
    )
    success = build_profile()
    sys.exit(0 if success else 1)
