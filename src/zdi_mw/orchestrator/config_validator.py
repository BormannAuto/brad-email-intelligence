# src/zdi_mw/orchestrator/config_validator.py
# ZDI Middleware — ConfigValidator
#
# validate_all() — checks schema_version for all three managed config files:
#   email_config.json       → "schema_version"
#   retry_policies.json     → "schema_version"
#   brad_voice_profile.json → "profile_schema_version"
#
# Version rules:
#   Major mismatch (e.g. got 2.x, supports 1.x) → PipelineStartupError listing all bad files
#   Minor mismatch (e.g. got 1.1, supports 1.0)  → warning logged, pipeline continues
#   Missing key or unparseable                   → treated as "0.0" → PipelineStartupError
#   Missing file                                 → PipelineStartupError
#
# MUST be called before any API call (enforced by PipelineRunner step ordering).

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Supported "MAJOR.MINOR" per config type.
SUPPORTED_CONFIG_VERSIONS: Dict[str, str] = {
    "email_config": "1.0",
    "retry_policies": "1.0",
    "brad_voice_profile": "1.0",
}

# Key inside each config file that holds the version string.
_VERSION_KEY_MAP: Dict[str, str] = {
    "email_config": "schema_version",
    "retry_policies": "schema_version",
    "brad_voice_profile": "profile_schema_version",
}


class PipelineStartupError(Exception):
    """
    Raised when one or more config files have an incompatible major version
    or are missing entirely.

    Attributes:
        incompatible_files: list of config type names that failed validation.
    """

    def __init__(self, incompatible_files: List[str], message: str = "") -> None:
        self.incompatible_files = list(incompatible_files)
        super().__init__(
            message
            or (
                "Config version incompatibility — pipeline cannot start. "
                f"Incompatible files: {self.incompatible_files}"
            )
        )


@dataclass
class ConfigValidationResult:
    """Result of validate_all()."""

    valid: bool
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    incompatible_files: List[str] = field(default_factory=list)


class ConfigValidator:
    """
    Validates schema_version fields across all managed config files before
    pipeline startup.

    Injectable file path overrides allow unit tests to point at tmp_path
    fixtures without touching the real config files.

    Usage:
        validator = ConfigValidator(config_base_path=Path("..."))
        result = validator.validate_all()
        # raises PipelineStartupError if any major version mismatch found
    """

    def __init__(
        self,
        config_base_path: Optional[Path] = None,
        # Injectable path overrides for unit tests
        _email_config_path: Optional[Path] = None,
        _retry_policies_path: Optional[Path] = None,
        _brad_voice_profile_path: Optional[Path] = None,
    ) -> None:
        self._base = config_base_path or Path(".")
        self._path_overrides: Dict[str, Optional[Path]] = {
            "email_config": _email_config_path,
            "retry_policies": _retry_policies_path,
            "brad_voice_profile": _brad_voice_profile_path,
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate_all(self) -> ConfigValidationResult:
        """
        Validate schema versions for all three managed config files.

        Returns:
            ConfigValidationResult — always returned if only minor mismatches.

        Raises:
            PipelineStartupError — if any major version mismatch or missing file.
        """
        result = ConfigValidationResult(valid=True)

        for config_type, supported_version in SUPPORTED_CONFIG_VERSIONS.items():
            path = self._resolve_path(config_type)
            warning, error = self._validate_one(config_type, path, supported_version)
            if warning:
                result.warnings.append(warning)
                logger.warning("ConfigValidator: %s", warning)
            if error:
                result.errors.append(error)
                result.incompatible_files.append(config_type)
                result.valid = False
                logger.error("ConfigValidator: %s", error)

        if result.incompatible_files:
            raise PipelineStartupError(
                incompatible_files=result.incompatible_files,
                message=(
                    "Pipeline cannot start — incompatible config schema versions: "
                    + str(result.incompatible_files)
                ),
            )

        if result.warnings:
            logger.warning(
                "ConfigValidator: %d minor version warning(s) — pipeline continues",
                len(result.warnings),
            )
        else:
            logger.info("ConfigValidator: all config versions validated OK")

        return result

    def validate_one(self, config_type: str, path: Optional[Path] = None) -> Tuple[Optional[str], Optional[str]]:
        """
        Validate a single config file. Returns (warning, error) tuple.
        Does NOT raise — caller decides what to do with the result.
        """
        if config_type not in SUPPORTED_CONFIG_VERSIONS:
            return None, f"Unknown config_type '{config_type}'"
        resolved = path or self._resolve_path(config_type)
        supported = SUPPORTED_CONFIG_VERSIONS[config_type]
        return self._validate_one(config_type, resolved, supported)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _resolve_path(self, config_type: str) -> Path:
        """Return injected override path or derive from base path."""
        override = self._path_overrides.get(config_type)
        if override is not None:
            return override

        _DEFAULT_PATHS: Dict[str, str] = {
            "email_config": "email_config.json",
            "retry_policies": "src/config/retry_policies.json",
            "brad_voice_profile": "voice/brad_voice_profile.json",
        }
        return self._base / _DEFAULT_PATHS.get(config_type, f"{config_type}.json")

    def _validate_one(
        self,
        config_type: str,
        path: Path,
        supported_version: str,
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Validate one config file.

        Returns:
            (warning_str_or_None, error_str_or_None)
            warning → minor mismatch
            error   → major mismatch / missing file / bad format
        """
        # File existence check
        if not path.exists():
            return None, f"{config_type}: file not found at {path}"

        # Load JSON
        try:
            with path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
        except (json.JSONDecodeError, OSError) as exc:
            return None, f"{config_type}: could not load JSON — {type(exc).__name__}: {exc}"

        version_key = _VERSION_KEY_MAP.get(config_type, "schema_version")
        found_version = data.get(version_key)

        if not found_version or not isinstance(found_version, str):
            return None, (
                f"{config_type}: missing or invalid '{version_key}' key "
                f"(got {found_version!r})"
            )

        # Parse major.minor
        supported_major, supported_minor = _parse_version(supported_version)
        found_major, found_minor = _parse_version(found_version)

        if found_major is None or found_minor is None:
            return None, (
                f"{config_type}: unparseable version string '{found_version}'"
            )

        if found_major != supported_major:
            return None, (
                f"{config_type}: major version mismatch — "
                f"found {found_version}, supported {supported_version}"
            )

        if found_minor != supported_minor:
            return (
                f"{config_type}: minor version mismatch — "
                f"found {found_version}, supported {supported_version} (OK to continue)",
                None,
            )

        return None, None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_version(version_str: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Parse "MAJOR.MINOR" string.
    Returns (major, minor) ints, or (None, None) if unparseable.
    """
    try:
        parts = version_str.strip().split(".")
        if len(parts) != 2:
            return None, None
        return int(parts[0]), int(parts[1])
    except (ValueError, AttributeError):
        return None, None
