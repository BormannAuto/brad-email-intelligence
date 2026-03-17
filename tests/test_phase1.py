# tests/test_phase1.py
# ZDI Middleware — Phase 1 Verification Gate
#
# Verification criteria (from spec Section 4 Phase 1):
#   1. All 5 SQLite tables exist
#   2. RunContext instantiates cleanly
#   3. SystemHealthCheck returns correct degraded_flags when each dep is mocked as down
#   4. RunLock prevents a second instance from starting
#
# Run with: pytest tests/test_phase1.py -v
#
# These tests use only mocked external calls — no real Zoho or Claude API
# credentials are needed or used.

import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

# Ensure the repo root is in sys.path regardless of how pytest is invoked
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))


# ===========================================================================
# Gate 1 — All 5 SQLite tables exist
# ===========================================================================

class TestDatabaseSchema:
    """Verify that init_db() creates all 5 required tables."""

    def test_all_five_tables_created(self, tmp_path):
        """init_db() should create all 5 tables in a fresh database."""
        from src.zdi_mw.state.db import init_db, verify_tables

        db_file = tmp_path / "zdi_state.db"
        init_db(db_path=db_file)
        result = verify_tables(db_path=db_file)

        assert result["pipeline_state"] is True, "pipeline_state table missing"
        assert result["wal_log"] is True, "wal_log table missing"
        assert result["idempotency_keys"] is True, "idempotency_keys table missing"
        assert result["dead_letter_queue"] is True, "dead_letter_queue table missing"
        assert result["pipeline_locks"] is True, "pipeline_locks table missing"

    def test_init_db_is_idempotent(self, tmp_path):
        """Calling init_db() twice must not raise or corrupt data."""
        from src.zdi_mw.state.db import init_db, verify_tables

        db_file = tmp_path / "zdi_state.db"
        init_db(db_path=db_file)
        init_db(db_path=db_file)  # second call — must not raise
        result = verify_tables(db_path=db_file)
        assert all(result.values()), "Tables missing after second init_db() call"

    def test_pipeline_state_schema(self, tmp_path):
        """pipeline_state must have the columns specified in Section 2."""
        from src.zdi_mw.state.db import init_db, get_connection

        db_file = tmp_path / "zdi_state.db"
        init_db(db_path=db_file)

        expected_columns = {
            "id", "run_id", "thread_id", "stage", "status",
            "failure_reason", "timestamp_utc", "pipeline_version"
        }
        with get_connection(db_file) as conn:
            info = conn.execute("PRAGMA table_info(pipeline_state)").fetchall()
        actual = {row["name"] for row in info}
        assert expected_columns.issubset(actual), (
            f"pipeline_state missing columns: {expected_columns - actual}"
        )

    def test_dead_letter_queue_schema(self, tmp_path):
        """dead_letter_queue must include retry_backoff_next_at (Section 2 addition)."""
        from src.zdi_mw.state.db import init_db, get_connection

        db_file = tmp_path / "zdi_state.db"
        init_db(db_path=db_file)

        with get_connection(db_file) as conn:
            info = conn.execute("PRAGMA table_info(dead_letter_queue)").fetchall()
        col_names = {row["name"] for row in info}
        assert "retry_backoff_next_at" in col_names, (
            "dead_letter_queue missing retry_backoff_next_at column"
        )


# ===========================================================================
# Gate 2 — RunContext instantiates cleanly
# ===========================================================================

class TestRunContext:
    """Verify RunContext dataclass construction and methods."""

    def _make_run_context(self, **overrides) -> "RunContext":
        from src.zdi_mw.core.run_context import RunContext

        defaults = dict(
            run_id="2026-03-16-071500-UTC",
            pipeline_version="git:abc1234",
            utc_start=datetime(2026, 3, 16, 7, 15, 0, tzinfo=timezone.utc),
            crm_snapshot_hash="sha256:abc",
            email_fetch_window=(
                datetime(2026, 3, 15, 7, 15, 0, tzinfo=timezone.utc),
                datetime(2026, 3, 16, 7, 15, 0, tzinfo=timezone.utc),
            ),
            brad_voice_version=3,
            prompt_registry_version="prompts-2026-03",
            pipeline_mode="sandbox",
            degraded_flags={
                "mail_down": False,
                "crm_down": False,
                "workdrive_down": False,
                "claude_down": False,
                "db_down": False,
            },
            metadata_snapshot_hash="sha256:def",
        )
        defaults.update(overrides)
        return RunContext(**defaults)

    def test_instantiation(self):
        """RunContext must instantiate without error."""
        ctx = self._make_run_context()
        assert ctx.run_id == "2026-03-16-071500-UTC"
        assert ctx.pipeline_mode == "sandbox"

    def test_is_not_degraded_when_all_healthy(self):
        """is_degraded() returns False when all flags are False."""
        ctx = self._make_run_context()
        assert ctx.is_degraded() is False

    def test_is_degraded_when_one_flag_true(self):
        """is_degraded() returns True when any single flag is True."""
        ctx = self._make_run_context(
            degraded_flags={
                "mail_down": True,
                "crm_down": False,
                "workdrive_down": False,
                "claude_down": False,
                "db_down": False,
            }
        )
        assert ctx.is_degraded() is True

    def test_to_log_dict_is_json_serialisable(self):
        """to_log_dict() must return a plain dict that json.dumps accepts."""
        import json

        ctx = self._make_run_context()
        log_dict = ctx.to_log_dict()
        serialised = json.dumps(log_dict)  # must not raise
        assert "run_id" in serialised
        assert "pipeline_mode" in serialised

    def test_immutable(self):
        """RunContext is frozen — mutations must raise FrozenInstanceError."""
        ctx = self._make_run_context()
        with pytest.raises(Exception):  # FrozenInstanceError is a subclass of AttributeError
            ctx.run_id = "MUTATED"  # type: ignore[misc]

    def test_metadata_snapshot_hash_present(self):
        """metadata_snapshot_hash field exists and defaults to empty string."""
        from src.zdi_mw.core.run_context import RunContext

        # Build with default (no explicit metadata_snapshot_hash)
        defaults = dict(
            run_id="2026-03-16-071500-UTC",
            pipeline_version="git:abc1234",
            utc_start=datetime(2026, 3, 16, 7, 15, 0, tzinfo=timezone.utc),
            crm_snapshot_hash="sha256:abc",
            email_fetch_window=(
                datetime(2026, 3, 15, 7, 15, 0, tzinfo=timezone.utc),
                datetime(2026, 3, 16, 7, 15, 0, tzinfo=timezone.utc),
            ),
            brad_voice_version=3,
            prompt_registry_version="prompts-2026-03",
            pipeline_mode="sandbox",
            degraded_flags={k: False for k in [
                "mail_down", "crm_down", "workdrive_down", "claude_down", "db_down"
            ]},
        )
        ctx = RunContext(**defaults)
        assert hasattr(ctx, "metadata_snapshot_hash")
        assert ctx.metadata_snapshot_hash == ""


# ===========================================================================
# Gate 3 — SystemHealthCheck returns correct degraded_flags
# ===========================================================================

class TestSystemHealthCheck:
    """
    Verify that SystemHealthCheck marks the correct key as True when each
    dependency is mocked as down, and False when all are mocked as healthy.
    """

    def _make_checker(self, overrides: dict, tmp_db: Path) -> "SystemHealthCheck":
        """
        Build a SystemHealthCheck with all probes mocked healthy by default,
        then override the specified ones as failing.
        """
        from src.zdi_mw.state.db import init_db
        from src.zdi_mw.core.system_health import SystemHealthCheck

        init_db(db_path=tmp_db)

        healthy = lambda: True
        failing = lambda: (_ for _ in ()).throw(RuntimeError("mocked failure"))

        probes = {
            "_mail_probe": healthy,
            "_crm_probe": healthy,
            "_workdrive_probe": healthy,
            "_claude_probe": healthy,
            "_db_probe": healthy,
        }
        probes.update(overrides)

        return SystemHealthCheck(db_path=tmp_db, **probes)

    def test_all_healthy(self, tmp_path):
        """All probes return healthy → all flags False."""
        checker = self._make_checker({}, tmp_path / "zdi.db")
        flags = checker.run()
        assert flags == {
            "mail_down": False,
            "crm_down": False,
            "workdrive_down": False,
            "claude_down": False,
            "db_down": False,
        }

    @pytest.mark.parametrize("failing_key, probe_kwarg", [
        ("mail_down", "_mail_probe"),
        ("crm_down", "_crm_probe"),
        ("workdrive_down", "_workdrive_probe"),
        ("claude_down", "_claude_probe"),
        ("db_down", "_db_probe"),
    ])
    def test_single_dependency_down(self, tmp_path, failing_key, probe_kwarg):
        """When exactly one probe fails, only its flag is True."""
        from src.zdi_mw.state.db import init_db

        db = tmp_path / "zdi.db"
        init_db(db_path=db)

        def _fail():
            raise RuntimeError(f"mocked {failing_key} failure")

        checker = self._make_checker({probe_kwarg: _fail}, db)
        flags = checker.run()

        assert flags[failing_key] is True, (
            f"Expected {failing_key}=True when that probe fails"
        )
        for k, v in flags.items():
            if k != failing_key:
                assert v is False, f"Expected {k}=False, got {v}"

    def test_all_dependencies_down(self, tmp_path):
        """When all probes fail, all flags are True."""
        db = tmp_path / "zdi.db"
        from src.zdi_mw.state.db import init_db
        init_db(db_path=db)

        def _fail():
            raise RuntimeError("all down")

        checker = self._make_checker(
            {
                "_mail_probe": _fail,
                "_crm_probe": _fail,
                "_workdrive_probe": _fail,
                "_claude_probe": _fail,
                "_db_probe": _fail,
            },
            db,
        )
        flags = checker.run()
        assert all(flags.values()), "Expected all flags True when all probes fail"


# ===========================================================================
# Gate 4 — RunLock prevents a second instance from starting
# ===========================================================================

class TestRunLock:
    """Verify RunLock acquire/release and double-acquire protection."""

    def test_acquire_and_release(self, tmp_path):
        """Basic acquire then release cycle — no error expected."""
        from src.zdi_mw.state.db import init_db
        from src.zdi_mw.state.run_lock import RunLock

        db = tmp_path / "zdi.db"
        init_db(db_path=db)

        lock = RunLock(db_path=db)
        lock.acquire("test-run-001")
        assert lock.is_held() is True

        lock.release()
        assert lock.is_held() is False

    def test_double_acquire_same_pid_raises(self, tmp_path):
        """A second RunLock instance cannot acquire while first is held (same process)."""
        from src.zdi_mw.state.db import init_db
        from src.zdi_mw.state.run_lock import RunLock, LockAlreadyHeldError

        db = tmp_path / "zdi.db"
        init_db(db_path=db)

        lock1 = RunLock(db_path=db)
        lock1.acquire("test-run-002")

        lock2 = RunLock(db_path=db)
        with pytest.raises(LockAlreadyHeldError):
            lock2.acquire("test-run-003")

        lock1.release()

    @pytest.mark.skipif(sys.platform == "win32", reason="os.kill PID check is Linux-only")
    def test_stale_lock_from_dead_pid_is_cleared(self, tmp_path):
        """A lock left by a dead PID should be cleared and the new lock acquired."""
        from src.zdi_mw.state.db import init_db, get_connection
        from src.zdi_mw.state.run_lock import RunLock
        from datetime import datetime, timezone

        db = tmp_path / "zdi.db"
        init_db(db_path=db)

        # Manually insert a stale lock with a PID that cannot exist (very large int)
        dead_pid = 99999999
        with get_connection(db) as conn:
            conn.execute(
                "INSERT INTO pipeline_locks (lock_name, run_id, acquired_at, pid) "
                "VALUES (?, ?, ?, ?)",
                (
                    "pipeline_main",
                    "old-run",
                    datetime.now(timezone.utc).isoformat(),
                    dead_pid,
                ),
            )

        lock = RunLock(db_path=db)
        # Should NOT raise — stale lock from dead PID gets cleared
        lock.acquire("test-run-004")
        assert lock.is_held() is True
        lock.release()

    def test_release_is_idempotent(self, tmp_path):
        """Calling release() multiple times must not raise."""
        from src.zdi_mw.state.db import init_db
        from src.zdi_mw.state.run_lock import RunLock

        db = tmp_path / "zdi.db"
        init_db(db_path=db)

        lock = RunLock(db_path=db)
        lock.acquire("test-run-005")
        lock.release()
        lock.release()  # second release — must not raise
