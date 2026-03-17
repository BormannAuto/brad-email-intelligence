# src/zdi_mw/orchestrator/prompt_registry.py
# ZDI Middleware — PromptRegistry
# Tiered prompt loader: versioned file → latest minor in major → hardcoded fallback.
# fallback_level logged with every accuracy_log entry:
#   0 = primary (exact versioned file found, e.g. draft_v1.jinja2)
#   1 = minor fallback (primary file absent; older/other minor in same major used)
#   2 = hardcoded minimal safe fallback (no file at all in this major)
# Raises PromptMajorVersionError if the requested major version has no fallback.

import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from jinja2 import Environment, StrictUndefined, TemplateError

logger = logging.getLogger(__name__)

_DEFAULT_PROMPTS_DIR = Path("prompts")

# ---------------------------------------------------------------------------
# Hardcoded minimal safe fallbacks — used when no template file is found.
# These are intentionally minimal: they instruct Claude to return safe
# structured output and flag the entry for human review.
# ---------------------------------------------------------------------------
_HARDCODED_FALLBACKS: Dict[str, str] = {
    "categorizer": (
        "Classify the email. If uncertain, set category to HOLD, hold to true, "
        "draft_eligible to false, confidence to LOW, urgency to 3, "
        "source_email_id to '{{ email_id }}', sentiment_signals to []."
    ),
    "draft": (
        "Draft a brief acknowledgement reply. Set confidence to LOW, "
        "flags to ['NEEDS_BRAD_INPUT'], word_count to 0, source_references to [], "
        "draft_body to 'Thank you for your email. Brad will follow up shortly.'."
    ),
    "signature_extract": (
        "Extract contact details. If none found, set all fields to null, "
        "confidence_per_field to {}, extraction_notes to 'FALLBACK_PROMPT_USED'."
    ),
    "sent_log": (
        "Generate a minimal CRM note. Set summary_text to 'Email interaction logged.', "
        "topic_tags to [], next_action to 'NONE', crm_note_text to 'Interaction logged.'."
    ),
    "note": (
        "Generate a minimal CRM note. Set note_text to 'Interaction logged via fallback.', "
        "note_type to 'GENERAL_CONTACT', requires_follow_up to false, follow_up_hint to null."
    ),
}

# Template file patterns:
#   Primary:  {name}_v{major}.jinja2          (no explicit minor → canonical release)
#   Versioned: {name}_v{major}.{minor}.jinja2 (explicit minor version)
_PRIMARY_PATTERN = re.compile(
    r"^(?P<name>[a-z_]+)_v(?P<major>\d+)\.jinja2$"
)
_VERSIONED_PATTERN = re.compile(
    r"^(?P<name>[a-z_]+)_v(?P<major>\d+)\.(?P<minor>\d+)\.jinja2$"
)


class PromptMajorVersionError(Exception):
    """Raised when the requested major version has no matching template or hardcoded fallback."""


class PromptLoadResult:
    """
    Result of loading a prompt template.

    Attributes:
        template_source: Raw Jinja2 template string.
        prompt_name: Base name, e.g. 'draft'.
        prompt_version: Version string, e.g. '1.0' or 'fallback'.
        fallback_level: 0 (primary), 1 (minor fallback), 2 (hardcoded fallback).
    """

    def __init__(
        self,
        template_source: str,
        prompt_name: str,
        prompt_version: str,
        fallback_level: int,
    ) -> None:
        self.template_source = template_source
        self.prompt_name = prompt_name
        self.prompt_version = prompt_version
        self.fallback_level = fallback_level

    def render(self, **kwargs) -> str:
        """Render the Jinja2 template with provided variables."""
        env = Environment(undefined=StrictUndefined)
        try:
            tmpl = env.from_string(self.template_source)
            return tmpl.render(**kwargs)
        except TemplateError as exc:
            logger.error(
                "PromptRegistry.render: template error name=%s version=%s exc=%s",
                self.prompt_name,
                self.prompt_version,
                exc,
            )
            raise


class PromptRegistry:
    """
    Tiered prompt template loader.

    Resolution order for load(name, major_version):

    Tier 1 (fallback_level=0): Primary versioned file
        Looks for {name}_v{major}.jinja2 — the canonical release file.
        Also accepts the highest-minor explicit-versioned file
        {name}_v{major}.{minor}.jinja2 as primary if no canonical file exists.

    Tier 2 (fallback_level=1): Minor fallback
        Primary file is absent; a lower-minor explicit-versioned file
        (e.g. {name}_v1.0.jinja2) is used. Logs fallback_level=1.

    Tier 3 (fallback_level=2): Hardcoded minimal safe fallback
        No file of any kind found for this major. Uses _HARDCODED_FALLBACKS dict.

    Raises PromptMajorVersionError if the name is not in _HARDCODED_FALLBACKS either.

    Args:
        prompts_dir: Directory containing .jinja2 template files.
    """

    def __init__(self, prompts_dir: Optional[Path] = None) -> None:
        self.prompts_dir = Path(prompts_dir) if prompts_dir else _DEFAULT_PROMPTS_DIR

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self, name: str, major_version: int = 1) -> PromptLoadResult:
        """
        Load a prompt template by name and major version.

        Args:
            name: Template base name, e.g. 'draft', 'categorizer'.
            major_version: Required major version. Defaults to 1.

        Returns:
            PromptLoadResult with template_source, prompt_version, fallback_level.

        Raises:
            PromptMajorVersionError: If no file found and name has no hardcoded fallback.
        """
        if self.prompts_dir.exists():
            result = self._load_from_files(name, major_version)
            if result is not None:
                return result

        # Tier 3: hardcoded fallback
        if name in _HARDCODED_FALLBACKS:
            logger.warning(
                "PromptRegistry: no file found for name=%s major=%d — "
                "using hardcoded fallback (fallback_level=2)",
                name,
                major_version,
            )
            return PromptLoadResult(
                template_source=_HARDCODED_FALLBACKS[name],
                prompt_name=name,
                prompt_version="fallback",
                fallback_level=2,
            )

        raise PromptMajorVersionError(
            f"No template file or hardcoded fallback for prompt '{name}' "
            f"major_version={major_version}. "
            f"Available hardcoded fallbacks: {list(_HARDCODED_FALLBACKS.keys())}"
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_from_files(
        self, name: str, major_version: int
    ) -> Optional[PromptLoadResult]:
        """
        Scan prompts_dir for a matching template file.

        Resolution order:
          1. Canonical file: {name}_v{major}.jinja2 → fallback_level=0
          2. Highest-minor explicit file: {name}_v{major}.{minor}.jinja2 → fallback_level=0
          3. Any minor explicit file (lower minor) → fallback_level=1
          (If none found, returns None → caller escalates to hardcoded)
        """
        canonical_path = self.prompts_dir / f"{name}_v{major_version}.jinja2"

        # Tier 1: canonical primary file
        if canonical_path.exists():
            logger.info(
                "PromptRegistry: loaded primary name=%s major=%d fallback_level=0",
                name,
                major_version,
            )
            return PromptLoadResult(
                template_source=canonical_path.read_text(encoding="utf-8"),
                prompt_name=name,
                prompt_version=f"{major_version}.0",
                fallback_level=0,
            )

        # Scan for minor-versioned files in this major
        minor_candidates = self._find_minor_versions(name, major_version)

        if not minor_candidates:
            return None

        # Best candidate (highest minor) → fallback_level=0 if it's the only one,
        # fallback_level=1 if the canonical was missing (which it is, since we're here)
        minor_candidates.sort(key=lambda x: x[0], reverse=True)
        best_minor, best_path = minor_candidates[0]

        # The canonical file is absent, so even the highest minor is a fallback
        fallback_level = 1
        version_str = f"{major_version}.{best_minor}"

        logger.warning(
            "PromptRegistry: canonical file missing for name=%s major=%d — "
            "using minor fallback version=%s fallback_level=1",
            name,
            major_version,
            version_str,
        )
        return PromptLoadResult(
            template_source=best_path.read_text(encoding="utf-8"),
            prompt_name=name,
            prompt_version=version_str,
            fallback_level=fallback_level,
        )

    def _find_minor_versions(
        self, name: str, major_version: int
    ) -> List[Tuple[int, Path]]:
        """
        Return list of (minor_int, Path) for all {name}_v{major}.{minor}.jinja2 files.
        """
        results = []
        for f in self.prompts_dir.glob(f"{name}_v{major_version}.*.jinja2"):
            m = _VERSIONED_PATTERN.match(f.name)
            if not m:
                continue
            if m.group("name") != name:
                continue
            if int(m.group("major")) != major_version:
                continue
            results.append((int(m.group("minor")), f))
        return results
