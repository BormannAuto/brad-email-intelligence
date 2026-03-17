# tests/test_phase8.py
# ZDI Middleware Phase 8 — Pipeline Runner + Config Validation
#
# Test classes:
#   TestConfigValidatorVersionParsing   (7 tests)  — version string parsing, edge cases
#   TestConfigValidatorValidation       (9 tests)  — file validation, major/minor mismatch
#   TestPipelineRunnerInit              (5 tests)  — mode detection, env vars
#   TestPipelineRunnerDryRun            (8 tests)  — dry_run suppresses writes, counts correct
#   TestPipelineRunnerSandbox           (6 tests)  — sandbox mode, mock_zoho_fail, DLQ
#   TestPipelineRunnerExecution         (6 tests)  — thread processing, stage sequencing
#   TestVerificationGate                (5 tests)  — spec requirements explicitly verified
#
# Total: 46 tests

import json
import os
import pytest
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

from src.zdi_mw.orchestrator.config_validator import (
    ConfigValidator,
    ConfigValidationResult,
    PipelineStartupError,
    SUPPORTED_CONFIG_VERSIONS,
    _parse_version,
)
from src.zdi_mw.orchestrator.pipeline_runner import (
    PipelineRunner,
    PipelineResult,
    ThreadResult,
    _generate_run_id,
    _extract_category,
    _extract_confidence,
    _extract_draft_body,
    _extract_contact_id,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def valid_email_config(tmp_path) -> Path:
    p = tmp_path / "email_config.json"
    p.write_text(json.dumps({"schema_version": "1.0", "blocked_domains": []}))
    return p


@pytest.fixture
def valid_retry_policies(tmp_path) -> Path:
    p = tmp_path / "retry_policies.json"
    p.write_text(json.dumps({"schema_version": "1.0", "retry_policies": {}}))
    return p


@pytest.fixture
def valid_brad_voice(tmp_path) -> Path:
    p = tmp_path / "brad_voice_profile.json"
    p.write_text(json.dumps({"profile_schema_version": "1.0", "summary": "Test"}))
    return p


@pytest.fixture
def valid_validator(valid_email_config, valid_retry_policies, valid_brad_voice):
    return ConfigValidator(
        _email_config_path=valid_email_config,
        _retry_policies_path=valid_retry_policies,
        _brad_voice_profile_path=valid_brad_voice,
    )


def _make_thread(
    thread_id="T001",
    from_address="rep@test.test",
    subject="Test Subject",
    body="Please find Q2 pricing. Thank you.",
    brand="TestBrand",
    model="TB-100",
    in_reply_to="<msg001@test.test>",
) -> Dict:
    return {
        "thread_id": thread_id,
        "from_address": from_address,
        "subject": subject,
        "body": body,
        "brand": brand,
        "model": model,
        "in_reply_to": in_reply_to,
    }


def _make_runner_no_api(valid_validator, pipeline_mode="live", sandbox_mode=False, mock_zoho_fail=False):
    """Runner wired with valid config validator but no external API deps."""
    return PipelineRunner(
        pipeline_mode=pipeline_mode,
        sandbox_mode=sandbox_mode,
        mock_zoho_fail=mock_zoho_fail,
        _config_validator=valid_validator,
    )


# ---------------------------------------------------------------------------
# TestConfigValidatorVersionParsing
# ---------------------------------------------------------------------------

class TestConfigValidatorVersionParsing:

    def test_parse_valid_version(self):
        major, minor = _parse_version("1.0")
        assert major == 1
        assert minor == 0

    def test_parse_minor_version(self):
        major, minor = _parse_version("1.3")
        assert major == 1
        assert minor == 3

    def test_parse_major_version_two(self):
        major, minor = _parse_version("2.0")
        assert major == 2
        assert minor == 0

    def test_parse_invalid_no_dot(self):
        major, minor = _parse_version("10")
        assert major is None
        assert minor is None

    def test_parse_invalid_non_numeric(self):
        major, minor = _parse_version("x.y")
        assert major is None
        assert minor is None

    def test_parse_empty_string(self):
        major, minor = _parse_version("")
        assert major is None
        assert minor is None

    def test_parse_three_parts(self):
        # "1.2.3" has three parts — invalid for our two-part scheme
        major, minor = _parse_version("1.2.3")
        assert major is None
        assert minor is None


# ---------------------------------------------------------------------------
# TestConfigValidatorValidation
# ---------------------------------------------------------------------------

class TestConfigValidatorValidation:

    def test_all_valid_configs_pass(self, valid_validator):
        result = valid_validator.validate_all()
        assert result.valid is True
        assert len(result.errors) == 0
        assert len(result.incompatible_files) == 0

    def test_no_warnings_on_exact_match(self, valid_validator):
        result = valid_validator.validate_all()
        assert len(result.warnings) == 0

    def test_major_version_mismatch_raises(self, tmp_path, valid_retry_policies, valid_brad_voice):
        bad_email = tmp_path / "email_config.json"
        bad_email.write_text(json.dumps({"schema_version": "2.0"}))
        validator = ConfigValidator(
            _email_config_path=bad_email,
            _retry_policies_path=valid_retry_policies,
            _brad_voice_profile_path=valid_brad_voice,
        )
        with pytest.raises(PipelineStartupError) as exc_info:
            validator.validate_all()
        assert "email_config" in exc_info.value.incompatible_files

    def test_multiple_incompatible_all_listed(self, tmp_path):
        bad_email = tmp_path / "email_config.json"
        bad_email.write_text(json.dumps({"schema_version": "2.0"}))
        bad_retry = tmp_path / "retry_policies.json"
        bad_retry.write_text(json.dumps({"schema_version": "3.0"}))
        bad_voice = tmp_path / "brad_voice_profile.json"
        bad_voice.write_text(json.dumps({"profile_schema_version": "1.0"}))
        validator = ConfigValidator(
            _email_config_path=bad_email,
            _retry_policies_path=bad_retry,
            _brad_voice_profile_path=bad_voice,
        )
        with pytest.raises(PipelineStartupError) as exc_info:
            validator.validate_all()
        assert "email_config" in exc_info.value.incompatible_files
        assert "retry_policies" in exc_info.value.incompatible_files

    def test_minor_version_mismatch_warns_not_blocks(self, tmp_path, valid_retry_policies, valid_brad_voice):
        minor_email = tmp_path / "email_config.json"
        minor_email.write_text(json.dumps({"schema_version": "1.1"}))
        validator = ConfigValidator(
            _email_config_path=minor_email,
            _retry_policies_path=valid_retry_policies,
            _brad_voice_profile_path=valid_brad_voice,
        )
        result = validator.validate_all()
        # Should not raise — minor mismatch is warning only
        assert result.valid is True
        assert len(result.warnings) == 1
        assert "email_config" in result.warnings[0]

    def test_missing_schema_version_key_blocks(self, tmp_path, valid_retry_policies, valid_brad_voice):
        no_version = tmp_path / "email_config.json"
        no_version.write_text(json.dumps({"blocked_domains": []}))
        validator = ConfigValidator(
            _email_config_path=no_version,
            _retry_policies_path=valid_retry_policies,
            _brad_voice_profile_path=valid_brad_voice,
        )
        with pytest.raises(PipelineStartupError) as exc_info:
            validator.validate_all()
        assert "email_config" in exc_info.value.incompatible_files

    def test_missing_file_raises(self, tmp_path, valid_retry_policies, valid_brad_voice):
        missing = tmp_path / "email_config.json"
        # Deliberately do NOT create the file
        validator = ConfigValidator(
            _email_config_path=missing,
            _retry_policies_path=valid_retry_policies,
            _brad_voice_profile_path=valid_brad_voice,
        )
        with pytest.raises(PipelineStartupError) as exc_info:
            validator.validate_all()
        assert "email_config" in exc_info.value.incompatible_files

    def test_pipeline_startup_error_lists_incompatible(self, tmp_path):
        bad = tmp_path / "email_config.json"
        bad.write_text(json.dumps({"schema_version": "9.0"}))
        validator = ConfigValidator(_email_config_path=bad)
        try:
            validator.validate_one("email_config", bad)
        except Exception:
            pass
        _, error = validator.validate_one("email_config", bad)
        assert error is not None
        assert "major version mismatch" in error

    def test_validate_one_unknown_type_returns_error(self, valid_validator):
        _, error = valid_validator.validate_one("nonexistent_config")
        assert error is not None
        assert "Unknown config_type" in error


# ---------------------------------------------------------------------------
# TestPipelineRunnerInit
# ---------------------------------------------------------------------------

class TestPipelineRunnerInit:

    def test_default_mode_is_live(self, valid_validator):
        with patch.dict(os.environ, {"PIPELINE_MODE": "live"}, clear=False):
            runner = PipelineRunner(_config_validator=valid_validator)
        assert runner._pipeline_mode == "live"
        assert runner._is_dry_run is False

    def test_dry_run_mode_from_env(self, valid_validator):
        with patch.dict(os.environ, {"PIPELINE_MODE": "dry_run"}, clear=False):
            runner = PipelineRunner(_config_validator=valid_validator)
        assert runner._pipeline_mode == "dry_run"
        assert runner._is_dry_run is True

    def test_sandbox_mode_from_env(self, valid_validator):
        with patch.dict(os.environ, {"SANDBOX_MODE": "true"}, clear=False):
            runner = PipelineRunner(_config_validator=valid_validator)
        assert runner._sandbox_mode is True

    def test_constructor_arg_overrides_env(self, valid_validator):
        with patch.dict(os.environ, {"PIPELINE_MODE": "live"}, clear=False):
            runner = PipelineRunner(
                pipeline_mode="dry_run",
                _config_validator=valid_validator,
            )
        assert runner._pipeline_mode == "dry_run"
        assert runner._is_dry_run is True

    def test_mock_zoho_fail_from_constructor(self, valid_validator):
        runner = PipelineRunner(
            mock_zoho_fail=True,
            _config_validator=valid_validator,
        )
        assert runner._mock_zoho_fail is True


# ---------------------------------------------------------------------------
# TestPipelineRunnerDryRun
# ---------------------------------------------------------------------------

class TestPipelineRunnerDryRun:

    def test_dry_run_returns_correct_mode(self, valid_validator):
        runner = _make_runner_no_api(valid_validator, pipeline_mode="dry_run")
        result = runner.run([])
        assert result.mode == "dry_run"

    def test_dry_run_no_threads_zero_writes(self, valid_validator):
        runner = _make_runner_no_api(valid_validator, pipeline_mode="dry_run")
        result = runner.run([])
        assert result.total_writes == 0
        assert result.dry_run_writes_suppressed == 0

    def test_dry_run_suppresses_crm_write(self, valid_validator):
        mock_crm = MagicMock()
        runner = PipelineRunner(
            pipeline_mode="dry_run",
            _config_validator=valid_validator,
            _safe_crm=mock_crm,
        )
        runner.run([_make_thread()])
        # CRM write function should NEVER be called in dry_run
        mock_crm.safe_write.assert_not_called()

    def test_dry_run_suppresses_mail_draft(self, valid_validator):
        mock_mail = MagicMock()
        runner = PipelineRunner(
            pipeline_mode="dry_run",
            _config_validator=valid_validator,
            _safe_mail=mock_mail,
        )
        runner.run([_make_thread()])
        mock_mail.safe_create_draft.assert_not_called()

    def test_dry_run_counts_suppressed_writes(self, valid_validator):
        runner = PipelineRunner(
            pipeline_mode="dry_run",
            _config_validator=valid_validator,
            _safe_crm=MagicMock(),
            _safe_mail=MagicMock(),
        )
        # 2 threads → 2 CRM suppressed + 2 Mail suppressed = 4 total
        result = runner.run([_make_thread("T1"), _make_thread("T2")])
        assert result.dry_run_writes_suppressed == 4

    def test_dry_run_total_writes_is_zero(self, valid_validator):
        runner = PipelineRunner(
            pipeline_mode="dry_run",
            _config_validator=valid_validator,
        )
        result = runner.run([_make_thread()])
        assert result.total_writes == 0

    def test_dry_run_threads_still_processed(self, valid_validator):
        runner = _make_runner_no_api(valid_validator, pipeline_mode="dry_run")
        result = runner.run([_make_thread("T1"), _make_thread("T2")])
        assert result.threads_processed == 2

    def test_dry_run_result_not_sandbox(self, valid_validator):
        runner = _make_runner_no_api(valid_validator, pipeline_mode="dry_run")
        result = runner.run([_make_thread()])
        assert result.sandbox_mode is False


# ---------------------------------------------------------------------------
# TestPipelineRunnerSandbox
# ---------------------------------------------------------------------------

class TestPipelineRunnerSandbox:

    def test_sandbox_mode_reported_in_result(self, valid_validator):
        runner = _make_runner_no_api(valid_validator, sandbox_mode=True)
        result = runner.run([])
        assert result.sandbox_mode is True

    def test_sandbox_mode_string_in_result(self, valid_validator):
        runner = _make_runner_no_api(valid_validator, sandbox_mode=True)
        result = runner.run([])
        assert result.mode == "sandbox"

    def test_sandbox_no_real_api_calls(self, valid_validator):
        """In sandbox mode, auth is skipped — no real OAuth tokens requested."""
        mock_auth = MagicMock()
        runner = PipelineRunner(
            sandbox_mode=True,
            _config_validator=valid_validator,
            _auth_manager=mock_auth,
        )
        runner.run([])
        # In sandbox mode, get_token is not called
        mock_auth.get_token.assert_not_called()

    def test_sandbox_mock_fail_produces_dlq_entry(self, valid_validator, tmp_path):
        """MOCK_ZOHO_FAIL=true in sandbox → DLQ entry recorded."""
        from src.zdi_mw.state.db import init_db, get_connection
        from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue

        db_path = tmp_path / "test.db"
        init_db(db_path)

        retry_path = tmp_path / "retry_policies.json"
        retry_path.write_text(json.dumps({"schema_version": "1.0", "retry_policies": {}}))
        dlq = DeadLetterQueue(db_path=db_path, retry_policies_path=retry_path)

        runner = PipelineRunner(
            sandbox_mode=True,
            mock_zoho_fail=True,
            _config_validator=valid_validator,
            _dlq=dlq,
            _safe_mail=MagicMock(),  # provide so it tries the mail path
        )
        runner.run([_make_thread("FAIL_THREAD")])

        # Verify DLQ received entry
        with get_connection(db_path) as conn:
            rows = conn.execute(
                "SELECT thread_id, failure_reason FROM dead_letter_queue WHERE thread_id = 'FAIL_THREAD'"
            ).fetchall()
        assert len(rows) >= 1
        failure_reasons = [r[1] for r in rows]
        assert any("MOCK_FAIL" in fr for fr in failure_reasons)

    def test_sandbox_mock_fail_crm_not_written(self, valid_validator, tmp_path):
        """MOCK_ZOHO_FAIL=true → SafeCRM.safe_write never called."""
        mock_crm = MagicMock()
        runner = PipelineRunner(
            sandbox_mode=True,
            mock_zoho_fail=True,
            _config_validator=valid_validator,
            _safe_crm=mock_crm,
        )
        runner.run([_make_thread()])
        mock_crm.safe_write.assert_not_called()

    def test_sandbox_without_fail_processes_threads(self, valid_validator):
        runner = _make_runner_no_api(valid_validator, sandbox_mode=True)
        result = runner.run([_make_thread("S1"), _make_thread("S2")])
        assert result.threads_processed == 2


# ---------------------------------------------------------------------------
# TestPipelineRunnerExecution
# ---------------------------------------------------------------------------

class TestPipelineRunnerExecution:

    def test_config_validation_failure_raises_before_health_check(self, tmp_path):
        """PipelineStartupError raised BEFORE health check runs."""
        bad_email = tmp_path / "email_config.json"
        bad_email.write_text(json.dumps({"schema_version": "9.0"}))
        bad_retry = tmp_path / "retry_policies.json"
        bad_retry.write_text(json.dumps({"schema_version": "1.0"}))
        bad_voice = tmp_path / "brad_voice_profile.json"
        bad_voice.write_text(json.dumps({"profile_schema_version": "1.0"}))

        mock_health = MagicMock()
        validator = ConfigValidator(
            _email_config_path=bad_email,
            _retry_policies_path=bad_retry,
            _brad_voice_profile_path=bad_voice,
        )
        runner = PipelineRunner(
            _config_validator=validator,
            _health_checker=mock_health,
        )
        with pytest.raises(PipelineStartupError):
            runner.run([])
        # Health check NEVER called — config gate fired first
        mock_health.run.assert_not_called()

    def test_empty_threads_list(self, valid_validator):
        runner = _make_runner_no_api(valid_validator)
        result = runner.run([])
        assert result.threads_processed == 0
        assert result.threads_succeeded == 0

    def test_pre_validate_blocked_skips_thread(self, valid_validator):
        """When pre_validate returns blocked, thread is skipped, writes never attempted."""
        mock_gate = MagicMock()
        gate_result = MagicMock()
        gate_result.blocked = True
        gate_result.block_reason = "TEST_BLOCK"
        mock_gate.pre_validate.return_value = gate_result

        mock_crm = MagicMock()
        runner = PipelineRunner(
            pipeline_mode="live",
            _config_validator=valid_validator,
            _integrity_gate=mock_gate,
            _safe_crm=mock_crm,
        )
        result = runner.run([_make_thread()])
        assert result.threads_skipped == 1
        assert result.threads_succeeded == 0
        mock_crm.safe_write.assert_not_called()

    def test_post_validate_blocked_skips_writes(self, valid_validator):
        """When post_validate blocks, CRM/mail writes never attempted."""
        mock_gate = MagicMock()
        pre_ok = MagicMock(blocked=False)
        post_blocked = MagicMock(blocked=True, block_reason="POST_BLOCK")
        mock_gate.pre_validate.return_value = pre_ok
        mock_gate.post_validate.return_value = post_blocked

        mock_crm = MagicMock()
        runner = PipelineRunner(
            pipeline_mode="live",
            _config_validator=valid_validator,
            _integrity_gate=mock_gate,
            _safe_crm=mock_crm,
        )
        result = runner.run([_make_thread()])
        assert result.threads_skipped == 1
        mock_crm.safe_write.assert_not_called()

    def test_workdrive_skipped_when_degraded(self, valid_validator):
        """WorkDrive lookup skipped when workdrive_down flag is True."""
        mock_health = MagicMock()
        mock_health.run.return_value = {
            "mail_down": False, "crm_down": False,
            "workdrive_down": True, "claude_down": False, "db_down": False,
        }
        mock_wd = MagicMock()
        runner = PipelineRunner(
            pipeline_mode="live",
            _config_validator=valid_validator,
            _health_checker=mock_health,
            _safe_workdrive=mock_wd,
        )
        runner.run([_make_thread()])
        mock_wd.safe_lookup_chunk.assert_not_called()

    def test_pipeline_result_has_run_id(self, valid_validator):
        runner = _make_runner_no_api(valid_validator)
        result = runner.run([])
        assert result.run_id
        assert "UTC" in result.run_id

    def test_config_warnings_appear_in_result(self, tmp_path, valid_retry_policies, valid_brad_voice):
        """Minor version mismatch warning propagates into PipelineResult."""
        minor_email = tmp_path / "email_config.json"
        minor_email.write_text(json.dumps({"schema_version": "1.9"}))
        validator = ConfigValidator(
            _email_config_path=minor_email,
            _retry_policies_path=valid_retry_policies,
            _brad_voice_profile_path=valid_brad_voice,
        )
        runner = PipelineRunner(_config_validator=validator)
        result = runner.run([])
        assert len(result.config_warnings) == 1
        assert "email_config" in result.config_warnings[0]


# ---------------------------------------------------------------------------
# TestVerificationGate (spec explicit requirements)
# ---------------------------------------------------------------------------

class TestVerificationGate:

    def test_vg1_dry_run_zero_actual_writes(self, valid_validator):
        """Spec: dry_run completes without any actual writes to Zoho APIs."""
        mock_crm = MagicMock()
        mock_mail = MagicMock()
        runner = PipelineRunner(
            pipeline_mode="dry_run",
            _config_validator=valid_validator,
            _safe_crm=mock_crm,
            _safe_mail=mock_mail,
        )
        result = runner.run([_make_thread("T1"), _make_thread("T2"), _make_thread("T3")])
        # Core assertion: NO actual writes
        assert result.total_writes == 0
        # Both write functions untouched
        mock_crm.safe_write.assert_not_called()
        mock_mail.safe_create_draft.assert_not_called()
        # But suppression count is non-zero (proof the code path executed)
        assert result.dry_run_writes_suppressed > 0

    def test_vg2_sandbox_produces_correct_mock_output(self, valid_validator):
        """Spec: SANDBOX_MODE=true produces correct mock output (not real API)."""
        mock_auth = MagicMock()
        runner = PipelineRunner(
            sandbox_mode=True,
            _config_validator=valid_validator,
            _auth_manager=mock_auth,
        )
        result = runner.run([_make_thread("SB1")])
        assert result.sandbox_mode is True
        assert result.mode == "sandbox"
        assert result.threads_processed == 1
        # Auth was NOT called (sandbox skips real auth)
        mock_auth.get_token.assert_not_called()

    def test_vg3_config_schema_mismatch_raises_pipeline_startup_error(self, tmp_path):
        """Spec: config schema_version mismatch raises PipelineStartupError."""
        bad = tmp_path / "email_config.json"
        bad.write_text(json.dumps({"schema_version": "2.0"}))
        ok_retry = tmp_path / "retry_policies.json"
        ok_retry.write_text(json.dumps({"schema_version": "1.0"}))
        ok_voice = tmp_path / "brad_voice_profile.json"
        ok_voice.write_text(json.dumps({"profile_schema_version": "1.0"}))
        validator = ConfigValidator(
            _email_config_path=bad,
            _retry_policies_path=ok_retry,
            _brad_voice_profile_path=ok_voice,
        )
        with pytest.raises(PipelineStartupError) as exc_info:
            PipelineRunner(_config_validator=validator).run([])
        err = exc_info.value
        assert isinstance(err.incompatible_files, list)
        assert len(err.incompatible_files) >= 1

    def test_vg4_sandbox_mock_fail_dlq_entry_type_correct(self, valid_validator, tmp_path):
        """Spec: SANDBOX_MODE=true MOCK_ZOHO_FAIL=true → correct DLQ entry."""
        from src.zdi_mw.state.db import init_db, get_connection
        from src.zdi_mw.state.dead_letter_queue import DeadLetterQueue

        db_path = tmp_path / "vg4.db"
        init_db(db_path)
        retry_path = tmp_path / "retry_policies.json"
        retry_path.write_text(json.dumps({"schema_version": "1.0", "retry_policies": {}}))
        dlq = DeadLetterQueue(db_path=db_path, retry_policies_path=retry_path)

        runner = PipelineRunner(
            sandbox_mode=True,
            mock_zoho_fail=True,
            _config_validator=valid_validator,
            _dlq=dlq,
            _safe_crm=MagicMock(),
        )
        runner.run([_make_thread("VG4_THREAD")])

        with get_connection(db_path) as conn:
            rows = conn.execute(
                "SELECT failure_reason FROM dead_letter_queue WHERE thread_id = 'VG4_THREAD'"
            ).fetchall()
        assert len(rows) >= 1
        # DLQ entry must reference the mock failure reason explicitly
        fr = rows[0][0]
        assert "MOCK_FAIL" in fr

    def test_vg5_config_validation_before_any_api(self, tmp_path):
        """Spec: config schema_version check runs BEFORE any API call."""
        bad_email = tmp_path / "email_config.json"
        bad_email.write_text(json.dumps({"schema_version": "99.0"}))
        bad_retry = tmp_path / "retry_policies.json"
        bad_retry.write_text(json.dumps({"schema_version": "1.0"}))
        bad_voice = tmp_path / "brad_voice_profile.json"
        bad_voice.write_text(json.dumps({"profile_schema_version": "1.0"}))

        validator = ConfigValidator(
            _email_config_path=bad_email,
            _retry_policies_path=bad_retry,
            _brad_voice_profile_path=bad_voice,
        )

        # These would be called if config validation did NOT block first
        mock_health = MagicMock()
        mock_auth = MagicMock()
        mock_wal = MagicMock()

        runner = PipelineRunner(
            _config_validator=validator,
            _health_checker=mock_health,
            _auth_manager=mock_auth,
            _wal=mock_wal,
        )

        with pytest.raises(PipelineStartupError):
            runner.run([_make_thread()])

        # None of these should have been touched
        mock_health.run.assert_not_called()
        mock_auth.get_token.assert_not_called()
        mock_wal.reconcile.assert_not_called()


# ---------------------------------------------------------------------------
# Standalone helper tests
# ---------------------------------------------------------------------------

class TestHelpers:

    def test_generate_run_id_contains_utc(self):
        rid = _generate_run_id()
        assert "UTC" in rid

    def test_generate_run_id_unique(self):
        r1 = _generate_run_id()
        r2 = _generate_run_id()
        assert r1 != r2

    def test_extract_category_from_dict(self):
        assert _extract_category({"category": "Dealer-Customer"}) == "Dealer-Customer"

    def test_extract_category_from_none(self):
        assert _extract_category(None) == ""

    def test_extract_confidence_defaults_low(self):
        assert _extract_confidence(None) == "LOW"
        assert _extract_confidence({}) == "LOW"

    def test_extract_confidence_from_output(self):
        assert _extract_confidence({"confidence": "HIGH"}) == "HIGH"

    def test_extract_draft_body(self):
        assert _extract_draft_body({"draft_body": "Hello"}) == "Hello"
        assert _extract_draft_body({"body": "World"}) == "World"
        assert _extract_draft_body(None) == ""

    def test_extract_contact_id_from_hydrated(self):
        mock_contact = MagicMock()
        mock_contact.id = "CONTACT123"
        assert _extract_contact_id(mock_contact) == "CONTACT123"

    def test_extract_contact_id_none_returns_unknown(self):
        assert _extract_contact_id(None) == "UNKNOWN"
