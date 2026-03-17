# tests/test_phase5.py
# ZDI Middleware — Phase 5 Verification Gate
#
# Verification criteria (spec Section 4 Phase 5):
#   1. All 5 prompt template files exist with correct header format
#   2. PromptRegistry loads primary file (fallback_level=0)
#   3. PromptRegistry falls back to hardcoded (fallback_level=2) when primary deleted
#   4. PromptRegistry minor-version fallback (fallback_level=1)
#   5. PromptRegistry raises on unknown prompt name
#   6. AccuracyLogger appends NDJSON entries correctly
#   7. AccuracyLogger is thread-safe (concurrent writes)
#   8. ClaudeOrchestrator computes correct source_input_hash
#   9. ClaudeOrchestrator stores compressed input file
#  10. ClaudeOrchestrator logs source_input_hash in accuracy_log
#  11. ClaudeOrchestrator enforces 50-call cap (skips, does not abort)
#  12. ClaudeOrchestrator calls API with tool_choice structured output pattern
#  13. ClaudeOrchestrator parses CategorizerOutput from tool_use response
#  14. ClaudeOrchestrator parses DraftOutput from tool_use response
#  15. ClaudeOrchestrator logs fallback_level in accuracy_log
#  16. ClaudeOrchestrator handles API error without crashing
#  17. Pydantic model schemas valid for all 5 output models
#  18. PromptLoadResult.render() renders Jinja2 variables correctly
#  19. AccuracyLogger read_all() returns all entries
#  20. VERIFICATION GATE: delete draft_v1.jinja2 → fallback_level=2 in accuracy_log

import gzip
import hashlib
import json
import shutil
import sys
import threading
from dataclasses import dataclass, field as dc_field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from unittest.mock import MagicMock

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

_PROMPTS_DIR = _REPO_ROOT / "prompts"


# ===========================================================================
# Helpers / fixtures
# ===========================================================================

@dataclass(frozen=True)
class _RunContext:
    """Minimal RunContext stub for Phase 5 tests."""
    run_id: str = "2026-03-16-120000-UTC"
    pipeline_version: str = "git:test1234"
    brad_voice_version: int = 1
    utc_start: datetime = dc_field(
        default_factory=lambda: datetime(2026, 3, 16, 12, 0, 0, tzinfo=timezone.utc)
    )


def _make_run_context(**kwargs) -> _RunContext:
    return _RunContext(**kwargs)


def _make_tool_use_response(model_class, data: dict):
    """Build a mock Anthropic response object containing a tool_use block."""
    block = MagicMock()
    block.type = "tool_use"
    block.input = data
    response = MagicMock()
    response.content = [block]
    return response


# ===========================================================================
# Gate 1 — Prompt template files exist with correct header format
# ===========================================================================

class TestPromptTemplateFiles:
    """Verify all 5 Jinja2 template files exist and have required headers."""

    REQUIRED_TEMPLATES = [
        ("categorizer_v1.jinja2", "categorizer", "CategorizerOutput"),
        ("draft_v1.jinja2", "draft", "DraftOutput"),
        ("signature_extract_v1.jinja2", "signature_extract", "SignatureExtractOutput"),
        ("sent_log_v1.jinja2", "sent_log", "SentLogOutput"),
        ("note_v1.jinja2", "note", "NoteOutput"),
    ]

    def test_all_five_templates_exist(self):
        """All 5 .jinja2 files must exist in prompts/."""
        for filename, _, _ in self.REQUIRED_TEMPLATES:
            path = _PROMPTS_DIR / filename
            assert path.exists(), f"Missing template: {filename}"

    def test_templates_have_prompt_name_header(self):
        """Each template must contain a {# prompt_name: ... #} header."""
        for filename, expected_name, _ in self.REQUIRED_TEMPLATES:
            path = _PROMPTS_DIR / filename
            content = path.read_text(encoding="utf-8")
            assert f"prompt_name: {expected_name}" in content, (
                f"{filename}: missing '{{# prompt_name: {expected_name} #}}'"
            )

    def test_templates_have_version_header(self):
        """Each template must contain a {# version: ... #} header."""
        for filename, _, _ in self.REQUIRED_TEMPLATES:
            path = _PROMPTS_DIR / filename
            content = path.read_text(encoding="utf-8")
            assert "{# version:" in content, (
                f"{filename}: missing version header"
            )

    def test_templates_have_output_schema_class_header(self):
        """Each template must contain a {# output_schema_class: ... #} header."""
        for filename, _, expected_class in self.REQUIRED_TEMPLATES:
            path = _PROMPTS_DIR / filename
            content = path.read_text(encoding="utf-8")
            assert f"output_schema_class: {expected_class}" in content, (
                f"{filename}: missing '{{# output_schema_class: {expected_class} #}}'"
            )

    def test_templates_have_reviewed_by_header(self):
        """Each template must contain {# reviewed_by: Tony #}."""
        for filename, _, _ in self.REQUIRED_TEMPLATES:
            path = _PROMPTS_DIR / filename
            content = path.read_text(encoding="utf-8")
            assert "reviewed_by: Tony" in content, (
                f"{filename}: missing '{{# reviewed_by: Tony #}}'"
            )

    def test_draft_template_has_anti_math_rule(self):
        """draft_v1.jinja2 must instruct Claude not to perform arithmetic."""
        content = (_PROMPTS_DIR / "draft_v1.jinja2").read_text(encoding="utf-8")
        assert "arithmetic" in content.lower() or "compute" in content.lower(), (
            "draft_v1.jinja2 must contain anti-math instruction"
        )


# ===========================================================================
# Gate 2 — PromptRegistry loading behavior
# ===========================================================================

class TestPromptRegistry:
    """Verify PromptRegistry 3-tier fallback logic."""

    def test_loads_primary_file(self, tmp_path):
        """When primary file exists: load returns it with fallback_level=0."""
        from src.zdi_mw.orchestrator.prompt_registry import PromptRegistry

        # Create a minimal template
        (tmp_path / "draft_v1.jinja2").write_text(
            "{# prompt_name: draft #}\nHello {{ name }}", encoding="utf-8"
        )
        registry = PromptRegistry(prompts_dir=tmp_path)
        result = registry.load("draft", major_version=1)

        assert result.fallback_level == 0
        assert result.prompt_name == "draft"
        assert "Hello" in result.template_source

    def test_fallback_level_2_when_no_file(self, tmp_path):
        """When primary file is absent and no minor versions exist: fallback_level=2."""
        from src.zdi_mw.orchestrator.prompt_registry import PromptRegistry

        # Empty directory — no template files
        registry = PromptRegistry(prompts_dir=tmp_path)
        result = registry.load("draft", major_version=1)

        assert result.fallback_level == 2
        assert result.prompt_version == "fallback"
        assert "NEEDS_BRAD_INPUT" in result.template_source or "fallback" in result.template_source

    def test_fallback_level_1_with_minor_version_file(self, tmp_path):
        """When canonical file absent but minor-versioned file exists: fallback_level=1."""
        from src.zdi_mw.orchestrator.prompt_registry import PromptRegistry

        # Create minor-versioned file (v1.2) but NOT the canonical v1
        (tmp_path / "draft_v1.2.jinja2").write_text(
            "{# prompt_name: draft #}\nMinor fallback content", encoding="utf-8"
        )
        registry = PromptRegistry(prompts_dir=tmp_path)
        result = registry.load("draft", major_version=1)

        assert result.fallback_level == 1
        assert "1.2" in result.prompt_version

    def test_prefers_highest_minor_when_multiple_minors(self, tmp_path):
        """When multiple minor-versioned files exist: highest minor wins."""
        from src.zdi_mw.orchestrator.prompt_registry import PromptRegistry

        (tmp_path / "draft_v1.0.jinja2").write_text("Old content v1.0", encoding="utf-8")
        (tmp_path / "draft_v1.2.jinja2").write_text("Newest content v1.2", encoding="utf-8")
        (tmp_path / "draft_v1.1.jinja2").write_text("Middle content v1.1", encoding="utf-8")

        registry = PromptRegistry(prompts_dir=tmp_path)
        result = registry.load("draft", major_version=1)

        assert "1.2" in result.prompt_version
        assert "Newest" in result.template_source

    def test_raises_on_unknown_prompt_name(self, tmp_path):
        """Load of an unknown prompt name (not in hardcoded fallbacks): raises."""
        from src.zdi_mw.orchestrator.prompt_registry import (
            PromptRegistry,
            PromptMajorVersionError,
        )

        registry = PromptRegistry(prompts_dir=tmp_path)
        with pytest.raises(PromptMajorVersionError):
            registry.load("nonexistent_prompt", major_version=1)

    def test_all_five_names_load_from_real_prompts_dir(self):
        """All 5 prompt names load successfully from the real prompts dir."""
        from src.zdi_mw.orchestrator.prompt_registry import PromptRegistry

        registry = PromptRegistry(prompts_dir=_PROMPTS_DIR)
        for name in ["categorizer", "draft", "signature_extract", "sent_log", "note"]:
            result = registry.load(name, major_version=1)
            assert result.fallback_level == 0, (
                f"Expected fallback_level=0 for '{name}', got {result.fallback_level}"
            )

    def test_render_substitutes_variables(self, tmp_path):
        """PromptLoadResult.render() correctly substitutes Jinja2 variables."""
        from src.zdi_mw.orchestrator.prompt_registry import PromptRegistry

        (tmp_path / "draft_v1.jinja2").write_text(
            "Hello {{ recipient }}, from {{ sender }}", encoding="utf-8"
        )
        registry = PromptRegistry(prompts_dir=tmp_path)
        result = registry.load("draft", major_version=1)

        rendered = result.render(recipient="Alice", sender="Brad")
        assert rendered == "Hello Alice, from Brad"


# ===========================================================================
# Gate 3 — AccuracyLogger
# ===========================================================================

class TestAccuracyLogger:
    """Verify AccuracyLogger appends entries correctly and is thread-safe."""

    def test_log_action_appends_entry(self, tmp_path):
        """log_action() appends one JSON line to the log file."""
        from src.zdi_mw.loggers.accuracy_logger import AccuracyLogger

        log_file = tmp_path / "logs" / "accuracy.json"
        al = AccuracyLogger(log_path=log_file)
        al.log_action(
            run_id="run-001",
            pipeline_version="git:abc",
            brad_voice_version=1,
            prompt_name="draft_v1",
            prompt_version="1.0",
            prompt_fallback_level=0,
            model_version="claude-sonnet-4-6",
            source_input_hash="abc123",
            thread_id="thread_001",
            outcome="SUCCESS",
        )

        entries = al.read_all()
        assert len(entries) == 1
        entry = entries[0]
        assert entry["run_id"] == "run-001"
        assert entry["prompt_name"] == "draft_v1"
        assert entry["prompt_fallback_level"] == 0
        assert entry["source_input_hash"] == "abc123"
        assert entry["outcome"] == "SUCCESS"

    def test_multiple_entries_all_persisted(self, tmp_path):
        """Multiple log_action() calls each produce a separate entry."""
        from src.zdi_mw.loggers.accuracy_logger import AccuracyLogger

        al = AccuracyLogger(log_path=tmp_path / "acc.json")
        for i in range(5):
            al.log_action(
                run_id=f"run-{i}",
                pipeline_version="git:abc",
                brad_voice_version=1,
                prompt_name="categorizer_v1",
                prompt_version="1.0",
                prompt_fallback_level=0,
                model_version="claude-sonnet-4-6",
                source_input_hash=f"hash{i}",
                thread_id=f"thread_{i}",
                outcome="SUCCESS",
            )

        entries = al.read_all()
        assert len(entries) == 5
        run_ids = [e["run_id"] for e in entries]
        assert run_ids == ["run-0", "run-1", "run-2", "run-3", "run-4"]

    def test_extra_fields_included(self, tmp_path):
        """Extra fields are included in the log entry."""
        from src.zdi_mw.loggers.accuracy_logger import AccuracyLogger

        al = AccuracyLogger(log_path=tmp_path / "acc.json")
        al.log_action(
            run_id="run-x",
            pipeline_version="git:abc",
            brad_voice_version=1,
            prompt_name="draft_v1",
            prompt_version="1.0",
            prompt_fallback_level=2,
            model_version="claude-sonnet-4-6",
            source_input_hash="deadbeef",
            thread_id="thread_x",
            outcome="SKIPPED_CAP",
            extra={"duration_ms": 42, "call_index": 51},
        )
        entries = al.read_all()
        assert entries[0]["duration_ms"] == 42
        assert entries[0]["call_index"] == 51

    def test_thread_safe_concurrent_writes(self, tmp_path):
        """Concurrent log_action() calls from multiple threads do not corrupt the file."""
        from src.zdi_mw.loggers.accuracy_logger import AccuracyLogger

        al = AccuracyLogger(log_path=tmp_path / "concurrent.json")
        errors = []

        def write_entry(i: int):
            try:
                al.log_action(
                    run_id=f"run-{i}",
                    pipeline_version="git:abc",
                    brad_voice_version=1,
                    prompt_name="draft_v1",
                    prompt_version="1.0",
                    prompt_fallback_level=0,
                    model_version="claude-sonnet-4-6",
                    source_input_hash=f"hash{i:04d}",
                    thread_id=f"thread_{i}",
                    outcome="SUCCESS",
                )
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=write_entry, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Thread errors: {errors}"
        entries = al.read_all()
        assert len(entries) == 20


# ===========================================================================
# Gate 4 — ClaudeOrchestrator core behavior
# ===========================================================================

class TestClaudeOrchestratorHash:
    """Verify source_input_hash computation."""

    def test_hash_is_sha256_of_sorted_json(self):
        """source_input_hash must be sha256(json.dumps(sorted_input_dict).encode())."""
        from src.zdi_mw.orchestrator.claude_orchestrator import ClaudeOrchestrator

        input_dict = {"email_id": "abc", "body": "hello", "category": "QUOTE_REQUEST"}
        expected = hashlib.sha256(
            json.dumps(input_dict, sort_keys=True).encode("utf-8")
        ).hexdigest()

        actual = ClaudeOrchestrator._compute_hash(input_dict)
        assert actual == expected

    def test_hash_is_deterministic(self):
        """Same input dict always produces same hash regardless of insertion order."""
        from src.zdi_mw.orchestrator.claude_orchestrator import ClaudeOrchestrator

        d1 = {"a": 1, "b": 2, "c": 3}
        d2 = {"c": 3, "a": 1, "b": 2}
        assert ClaudeOrchestrator._compute_hash(d1) == ClaudeOrchestrator._compute_hash(d2)


class TestClaudeOrchestratorStorage:
    """Verify compressed input storage behavior."""

    def test_stores_gzip_file_keyed_by_hash(self, tmp_path):
        """A .json.gz file named by source_input_hash is created after a call."""
        from src.zdi_mw.loggers.accuracy_logger import AccuracyLogger
        from src.zdi_mw.orchestrator.claude_orchestrator import (
            ClaudeOrchestrator,
            CategorizerOutput,
        )

        log_file = tmp_path / "acc.json"
        al = AccuracyLogger(log_path=log_file)
        ctx = _make_run_context()

        input_dict = {"email_id": "abc", "body": "test body"}
        expected_hash = ClaudeOrchestrator._compute_hash(input_dict)

        mock_response = _make_tool_use_response(CategorizerOutput, {
            "category": "QUOTE_REQUEST",
            "confidence": "HIGH",
            "urgency": 2,
            "draft_eligible": True,
            "hold": False,
            "sentiment_signals": [],
            "source_email_id": "abc",
        })

        orch = ClaudeOrchestrator(
            run_context=ctx,
            accuracy_logger=al,
            inputs_dir=tmp_path / "inputs",
            _messages_create_fn=lambda **kw: mock_response,
        )

        orch.call(
            prompt_name="categorizer_v1",
            rendered_prompt="classify this email",
            input_dict=input_dict,
            output_model=CategorizerOutput,
            prompt_version="1.0",
            fallback_level=0,
            thread_id="thread_001",
        )

        stored_file = tmp_path / "inputs" / f"{expected_hash}.json.gz"
        assert stored_file.exists(), "Compressed input file not created"

        # Verify it decompresses cleanly
        with gzip.open(stored_file, "rb") as fh:
            payload = json.loads(fh.read())
        assert payload["source_input_hash"] == expected_hash
        assert payload["input_dict"] == input_dict

    def test_storage_is_idempotent(self, tmp_path):
        """Calling _store_input twice with the same hash does not create duplicate."""
        from src.zdi_mw.loggers.accuracy_logger import AccuracyLogger
        from src.zdi_mw.orchestrator.claude_orchestrator import ClaudeOrchestrator

        al = AccuracyLogger(log_path=tmp_path / "acc.json")
        ctx = _make_run_context()
        orch = ClaudeOrchestrator(
            run_context=ctx,
            accuracy_logger=al,
            inputs_dir=tmp_path / "inputs",
        )

        orch._store_input("abc123", "rendered prompt", {"key": "val"})
        orch._store_input("abc123", "rendered prompt again", {"key": "val"})

        files = list((tmp_path / "inputs").glob("abc123*.json.gz"))
        assert len(files) == 1, "Idempotent store should not create duplicate files"


class TestClaudeOrchestratorCallCap:
    """Verify 50-call global cap behavior."""

    def test_50_call_cap_skips_not_aborts(self, tmp_path):
        """After 50 calls, additional calls return skipped_cap=True — pipeline continues."""
        from src.zdi_mw.loggers.accuracy_logger import AccuracyLogger
        from src.zdi_mw.orchestrator.claude_orchestrator import (
            ClaudeOrchestrator,
            CategorizerOutput,
        )

        al = AccuracyLogger(log_path=tmp_path / "acc.json")
        ctx = _make_run_context()

        mock_response = _make_tool_use_response(CategorizerOutput, {
            "category": "GENERAL_INQUIRY",
            "confidence": "HIGH",
            "urgency": 3,
            "draft_eligible": False,
            "hold": False,
            "sentiment_signals": [],
            "source_email_id": "thread_x",
        })

        orch = ClaudeOrchestrator(
            run_context=ctx,
            accuracy_logger=al,
            inputs_dir=tmp_path / "inputs",
            _messages_create_fn=lambda **kw: mock_response,
        )

        # Exhaust 50 calls
        for i in range(50):
            result = orch.call(
                prompt_name="categorizer_v1",
                rendered_prompt="classify",
                input_dict={"email_id": f"thread_{i}"},
                output_model=CategorizerOutput,
                prompt_version="1.0",
                fallback_level=0,
                thread_id=f"thread_{i}",
            )
            assert not result.skipped_cap, f"Call {i+1} unexpectedly capped"

        # 51st call must be skipped
        result_51 = orch.call(
            prompt_name="categorizer_v1",
            rendered_prompt="classify",
            input_dict={"email_id": "thread_overflow"},
            output_model=CategorizerOutput,
            prompt_version="1.0",
            fallback_level=0,
            thread_id="thread_overflow",
        )
        assert result_51.skipped_cap is True
        assert result_51.output is None

    def test_cap_logs_skipped_cap_in_accuracy_log(self, tmp_path):
        """Capped calls must be logged with outcome='SKIPPED_CAP'."""
        from src.zdi_mw.loggers.accuracy_logger import AccuracyLogger
        from src.zdi_mw.orchestrator.claude_orchestrator import (
            ClaudeOrchestrator,
            CategorizerOutput,
        )

        al = AccuracyLogger(log_path=tmp_path / "acc.json")
        ctx = _make_run_context()

        mock_response = _make_tool_use_response(CategorizerOutput, {
            "category": "GENERAL_INQUIRY",
            "confidence": "HIGH",
            "urgency": 3,
            "draft_eligible": False,
            "hold": False,
            "sentiment_signals": [],
            "source_email_id": "t",
        })

        orch = ClaudeOrchestrator(
            run_context=ctx,
            accuracy_logger=al,
            inputs_dir=tmp_path / "inputs",
            _messages_create_fn=lambda **kw: mock_response,
        )

        # Exhaust cap
        for i in range(50):
            orch.call(
                prompt_name="categorizer_v1",
                rendered_prompt="x",
                input_dict={"i": i},
                output_model=CategorizerOutput,
                prompt_version="1.0",
                fallback_level=0,
                thread_id=f"t{i}",
            )

        # One more
        orch.call(
            prompt_name="draft_v1",
            rendered_prompt="y",
            input_dict={"over": "cap"},
            output_model=CategorizerOutput,
            prompt_version="1.0",
            fallback_level=0,
            thread_id="t_cap",
        )

        entries = al.read_all()
        capped = [e for e in entries if e["outcome"] == "SKIPPED_CAP"]
        assert len(capped) == 1
        assert capped[0]["prompt_name"] == "draft_v1"


class TestClaudeOrchestratorStructuredOutput:
    """Verify Structured Outputs enforcement and parsing."""

    def test_categorizer_output_parsed_correctly(self, tmp_path):
        """CategorizerOutput is parsed from tool_use block."""
        from src.zdi_mw.loggers.accuracy_logger import AccuracyLogger
        from src.zdi_mw.orchestrator.claude_orchestrator import (
            ClaudeOrchestrator,
            CategorizerOutput,
        )

        al = AccuracyLogger(log_path=tmp_path / "acc.json")
        ctx = _make_run_context()

        mock_data = {
            "category": "QUOTE_REQUEST",
            "confidence": "HIGH",
            "urgency": 1,
            "draft_eligible": True,
            "hold": False,
            "sentiment_signals": ["purchase_intent"],
            "source_email_id": "email_abc",
        }
        mock_response = _make_tool_use_response(CategorizerOutput, mock_data)

        orch = ClaudeOrchestrator(
            run_context=ctx,
            accuracy_logger=al,
            inputs_dir=tmp_path / "inputs",
            _messages_create_fn=lambda **kw: mock_response,
        )

        result = orch.call(
            prompt_name="categorizer_v1",
            rendered_prompt="classify",
            input_dict={"email_id": "email_abc"},
            output_model=CategorizerOutput,
            prompt_version="1.0",
            fallback_level=0,
            thread_id="thread_abc",
        )

        assert isinstance(result.output, CategorizerOutput)
        assert result.output.category == "QUOTE_REQUEST"
        assert result.output.confidence == "HIGH"
        assert result.output.urgency == 1
        assert result.output.hold is False

    def test_draft_output_parsed_correctly(self, tmp_path):
        """DraftOutput is parsed from tool_use block."""
        from src.zdi_mw.loggers.accuracy_logger import AccuracyLogger
        from src.zdi_mw.orchestrator.claude_orchestrator import (
            ClaudeOrchestrator,
            DraftOutput,
        )

        al = AccuracyLogger(log_path=tmp_path / "acc.json")
        ctx = _make_run_context()

        mock_data = {
            "draft_body": "Thank you for your inquiry.",
            "confidence": "HIGH",
            "flags": [],
            "word_count": 5,
            "source_references": ["thread_001"],
        }
        mock_response = _make_tool_use_response(DraftOutput, mock_data)

        orch = ClaudeOrchestrator(
            run_context=ctx,
            accuracy_logger=al,
            inputs_dir=tmp_path / "inputs",
            _messages_create_fn=lambda **kw: mock_response,
        )

        result = orch.call(
            prompt_name="draft_v1",
            rendered_prompt="write draft",
            input_dict={"thread_id": "thread_001"},
            output_model=DraftOutput,
            prompt_version="1.0",
            fallback_level=0,
            thread_id="thread_001",
        )

        assert isinstance(result.output, DraftOutput)
        assert result.output.draft_body == "Thank you for your inquiry."
        assert result.output.word_count == 5

    def test_api_call_passes_tool_choice_param(self, tmp_path):
        """_messages_create_fn is called with tool_choice={'type':'tool','name':'output'}."""
        from src.zdi_mw.loggers.accuracy_logger import AccuracyLogger
        from src.zdi_mw.orchestrator.claude_orchestrator import (
            ClaudeOrchestrator,
            CategorizerOutput,
        )

        al = AccuracyLogger(log_path=tmp_path / "acc.json")
        ctx = _make_run_context()

        captured_kwargs: dict = {}

        def mock_create(**kwargs):
            captured_kwargs.update(kwargs)
            return _make_tool_use_response(CategorizerOutput, {
                "category": "GENERAL_INQUIRY",
                "confidence": "HIGH",
                "urgency": 3,
                "draft_eligible": False,
                "hold": False,
                "sentiment_signals": [],
                "source_email_id": "e1",
            })

        orch = ClaudeOrchestrator(
            run_context=ctx,
            accuracy_logger=al,
            inputs_dir=tmp_path / "inputs",
            _messages_create_fn=mock_create,
        )

        orch.call(
            prompt_name="categorizer_v1",
            rendered_prompt="classify",
            input_dict={"email_id": "e1"},
            output_model=CategorizerOutput,
            prompt_version="1.0",
            fallback_level=0,
            thread_id="e1",
        )

        assert captured_kwargs["tool_choice"] == {"type": "tool", "name": "output"}
        assert captured_kwargs["max_tokens"] == 1000
        assert any(t["name"] == "output" for t in captured_kwargs["tools"])

    def test_handles_api_error_without_crash(self, tmp_path):
        """When _messages_create_fn raises, result.output is None and pipeline continues."""
        from src.zdi_mw.loggers.accuracy_logger import AccuracyLogger
        from src.zdi_mw.orchestrator.claude_orchestrator import (
            ClaudeOrchestrator,
            DraftOutput,
        )

        al = AccuracyLogger(log_path=tmp_path / "acc.json")
        ctx = _make_run_context()

        def failing_create(**kwargs):
            raise RuntimeError("Simulated API failure")

        orch = ClaudeOrchestrator(
            run_context=ctx,
            accuracy_logger=al,
            inputs_dir=tmp_path / "inputs",
            _messages_create_fn=failing_create,
        )

        result = orch.call(
            prompt_name="draft_v1",
            rendered_prompt="write draft",
            input_dict={"thread_id": "t1"},
            output_model=DraftOutput,
            prompt_version="1.0",
            fallback_level=0,
            thread_id="t1",
        )

        assert result.output is None
        assert not result.skipped_cap


class TestClaudeOrchestratorAccuracyLog:
    """Verify source_input_hash and fallback_level appear in every accuracy_log entry."""

    def test_source_hash_in_accuracy_log(self, tmp_path):
        """Every call logs source_input_hash in the accuracy_log."""
        from src.zdi_mw.loggers.accuracy_logger import AccuracyLogger
        from src.zdi_mw.orchestrator.claude_orchestrator import (
            ClaudeOrchestrator,
            CategorizerOutput,
        )

        al = AccuracyLogger(log_path=tmp_path / "acc.json")
        ctx = _make_run_context()

        input_dict = {"email_id": "e_hash_test", "body": "test"}
        expected_hash = ClaudeOrchestrator._compute_hash(input_dict)

        mock_response = _make_tool_use_response(CategorizerOutput, {
            "category": "GENERAL_INQUIRY", "confidence": "HIGH",
            "urgency": 3, "draft_eligible": False, "hold": False,
            "sentiment_signals": [], "source_email_id": "e_hash_test",
        })

        orch = ClaudeOrchestrator(
            run_context=ctx,
            accuracy_logger=al,
            inputs_dir=tmp_path / "inputs",
            _messages_create_fn=lambda **kw: mock_response,
        )

        orch.call(
            prompt_name="categorizer_v1",
            rendered_prompt="classify",
            input_dict=input_dict,
            output_model=CategorizerOutput,
            prompt_version="1.0",
            fallback_level=0,
            thread_id="e_hash_test",
        )

        entries = al.read_all()
        assert len(entries) == 1
        assert entries[0]["source_input_hash"] == expected_hash

    def test_fallback_level_logged_correctly(self, tmp_path):
        """fallback_level from PromptLoadResult is logged in accuracy_log entry."""
        from src.zdi_mw.loggers.accuracy_logger import AccuracyLogger
        from src.zdi_mw.orchestrator.claude_orchestrator import (
            ClaudeOrchestrator,
            DraftOutput,
        )

        al = AccuracyLogger(log_path=tmp_path / "acc.json")
        ctx = _make_run_context()

        mock_response = _make_tool_use_response(DraftOutput, {
            "draft_body": "Test draft.", "confidence": "HIGH",
            "flags": [], "word_count": 2, "source_references": [],
        })

        orch = ClaudeOrchestrator(
            run_context=ctx,
            accuracy_logger=al,
            inputs_dir=tmp_path / "inputs",
            _messages_create_fn=lambda **kw: mock_response,
        )

        # Call with fallback_level=2 (simulating hardcoded fallback was used)
        orch.call(
            prompt_name="draft_v1",
            rendered_prompt="minimal draft",
            input_dict={"thread_id": "t_fallback"},
            output_model=DraftOutput,
            prompt_version="fallback",
            fallback_level=2,
            thread_id="t_fallback",
        )

        entries = al.read_all()
        assert entries[0]["prompt_fallback_level"] == 2


# ===========================================================================
# Gate 5 — Pydantic model schema validity
# ===========================================================================

class TestPydanticModelSchemas:
    """Verify all 5 Pydantic output models produce valid JSON schemas."""

    def test_all_models_have_valid_json_schema(self):
        """Each output model's model_json_schema() must return a non-empty dict."""
        from src.zdi_mw.orchestrator.claude_orchestrator import (
            CategorizerOutput,
            DraftOutput,
            NoteOutput,
            SentLogOutput,
            SignatureExtractOutput,
        )

        for model_class in [
            CategorizerOutput,
            DraftOutput,
            SignatureExtractOutput,
            SentLogOutput,
            NoteOutput,
        ]:
            schema = model_class.model_json_schema()
            assert isinstance(schema, dict), f"{model_class.__name__}: schema is not a dict"
            assert "properties" in schema, (
                f"{model_class.__name__}: schema missing 'properties' key"
            )

    def test_categorizer_output_required_fields(self):
        """CategorizerOutput must have all 7 required fields."""
        from src.zdi_mw.orchestrator.claude_orchestrator import CategorizerOutput

        schema = CategorizerOutput.model_json_schema()
        required = set(schema.get("required", []))
        expected = {
            "category", "confidence", "urgency",
            "draft_eligible", "hold", "sentiment_signals", "source_email_id",
        }
        missing = expected - required
        assert not missing, f"CategorizerOutput missing required fields: {missing}"

    def test_draft_output_required_fields(self):
        """DraftOutput must have all 5 required fields."""
        from src.zdi_mw.orchestrator.claude_orchestrator import DraftOutput

        schema = DraftOutput.model_json_schema()
        required = set(schema.get("required", []))
        expected = {"draft_body", "confidence", "flags", "word_count", "source_references"}
        missing = expected - required
        assert not missing, f"DraftOutput missing required fields: {missing}"


# ===========================================================================
# VERIFICATION GATE — Delete draft_v1.jinja2 → fallback_level=2 in accuracy_log
# ===========================================================================

class TestVerificationGate:
    """
    Phase 5 Verification Gate (spec Section 4 Phase 5):
    'Delete draft_v1.jinja2, confirm fallback activates and accuracy_log
    shows fallback_level=2.'
    """

    def test_delete_primary_activates_hardcoded_fallback(self, tmp_path):
        """
        VERIFICATION GATE:
        1. Copy prompts dir to tmp
        2. Delete draft_v1.jinja2 from tmp copy
        3. Load via PromptRegistry → must return fallback_level=2
        4. Use result in ClaudeOrchestrator call
        5. Confirm accuracy_log entry has prompt_fallback_level=2
        """
        from src.zdi_mw.loggers.accuracy_logger import AccuracyLogger
        from src.zdi_mw.orchestrator.claude_orchestrator import (
            ClaudeOrchestrator,
            DraftOutput,
        )
        from src.zdi_mw.orchestrator.prompt_registry import PromptRegistry

        # Copy prompts dir to tmp so we can delete files safely
        tmp_prompts = tmp_path / "prompts"
        shutil.copytree(_PROMPTS_DIR, tmp_prompts)

        # Delete the primary draft template
        (tmp_prompts / "draft_v1.jinja2").unlink()
        assert not (tmp_prompts / "draft_v1.jinja2").exists(), "File not deleted"

        # Load via PromptRegistry — should fall back to level 2 (hardcoded)
        registry = PromptRegistry(prompts_dir=tmp_prompts)
        load_result = registry.load("draft", major_version=1)

        assert load_result.fallback_level == 2, (
            f"Expected fallback_level=2 after deleting draft_v1.jinja2, "
            f"got fallback_level={load_result.fallback_level}"
        )
        assert load_result.prompt_version == "fallback"

        # Now run an orchestrator call using the fallback prompt
        al = AccuracyLogger(log_path=tmp_path / "acc.json")
        ctx = _make_run_context()

        mock_response = _make_tool_use_response(DraftOutput, {
            "draft_body": "Thank you. Brad will follow up shortly.",
            "confidence": "LOW",
            "flags": ["NEEDS_BRAD_INPUT"],
            "word_count": 8,
            "source_references": [],
        })

        orch = ClaudeOrchestrator(
            run_context=ctx,
            accuracy_logger=al,
            inputs_dir=tmp_path / "inputs",
            _messages_create_fn=lambda **kw: mock_response,
        )

        orch.call(
            prompt_name=f"draft_v{load_result.prompt_version}",
            rendered_prompt=load_result.template_source,
            input_dict={"thread_id": "t_fallback_gate"},
            output_model=DraftOutput,
            prompt_version=load_result.prompt_version,
            fallback_level=load_result.fallback_level,
            thread_id="t_fallback_gate",
        )

        # CRITICAL: accuracy_log must show fallback_level=2
        entries = al.read_all()
        assert len(entries) == 1
        assert entries[0]["prompt_fallback_level"] == 2, (
            f"VERIFICATION GATE FAILED: accuracy_log shows "
            f"prompt_fallback_level={entries[0]['prompt_fallback_level']}, expected 2"
        )
        assert entries[0]["outcome"] == "SUCCESS"
