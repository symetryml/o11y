"""Tests for RunModeManager."""

from otelfl.core.run_mode import RunModeManager


class TestRunModeManager:
    def test_default_is_normal(self):
        mgr = RunModeManager()
        assert mgr.active.name == "normal"
        assert mgr.active.users == 5
        assert mgr.active.spawn_rate == 1.0
        assert mgr.fallback.name == "normal"

    def test_set_mode(self):
        mgr = RunModeManager()
        mode = mgr.set_mode("high")
        assert mode.name == "high"
        assert mgr.active.users == 20
        assert mgr.active.spawn_rate == 2.0
        assert mgr.fallback.name == "high"

    def test_set_mode_clears_timed_run(self):
        mgr = RunModeManager()
        mgr.start_timed_override(users=50, spawn_rate=5.0)
        mgr.set_mode("normal")
        # Simulate running -> stopped, should NOT trigger fallback
        mgr.check_locust_state("running")
        result = mgr.check_locust_state("stopped")
        assert result is None

    def test_no_fallback_without_timed_run(self):
        mgr = RunModeManager()
        # Simulate running -> stopped without a timed run
        mgr.check_locust_state("running")
        result = mgr.check_locust_state("stopped")
        assert result is None

    def test_fallback_after_timed_run(self):
        mgr = RunModeManager()
        mgr.set_mode("low")
        mgr.start_timed_override(users=50, spawn_rate=5.0)
        assert mgr.active.name == "override"
        assert mgr.active.users == 50
        assert mgr.fallback.name == "low"

        # Simulate Locust lifecycle
        mgr.check_locust_state("spawning")
        mgr.check_locust_state("running")
        result = mgr.check_locust_state("stopped")

        assert result is not None
        assert result.name == "low"
        assert result.users == 2
        assert mgr.active.name == "low"

    def test_fallback_only_fires_once(self):
        mgr = RunModeManager()
        mgr.start_timed_override(users=50, spawn_rate=5.0)
        mgr.check_locust_state("running")
        result1 = mgr.check_locust_state("stopped")
        assert result1 is not None

        # Second running -> stopped should not trigger
        mgr.check_locust_state("running")
        result2 = mgr.check_locust_state("stopped")
        assert result2 is None

    def test_cancel_timed_run(self):
        mgr = RunModeManager()
        mgr.start_timed_override(users=50, spawn_rate=5.0)
        mgr.cancel_timed_run()
        mgr.check_locust_state("running")
        result = mgr.check_locust_state("stopped")
        assert result is None

    def test_mode_names(self):
        mgr = RunModeManager()
        assert "normal" in mgr.mode_names
        assert "low" in mgr.mode_names
        assert "high" in mgr.mode_names

    def test_str_representation(self):
        mgr = RunModeManager()
        assert "normal" in str(mgr.active)
        assert "5u" in str(mgr.active)

    def test_no_false_trigger_on_startup(self):
        """Initial state is 'unknown', so first 'stopped' should not trigger."""
        mgr = RunModeManager()
        mgr.start_timed_override(users=50, spawn_rate=5.0)
        # First poll sees "stopped" directly - no running->stopped transition
        result = mgr.check_locust_state("stopped")
        assert result is None
