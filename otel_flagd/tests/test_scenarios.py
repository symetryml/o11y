"""Tests for scenario presets."""

from __future__ import annotations

import pytest

from otelfl.core.flagd_client import FlagdClient
from otelfl.core.experiment_logger import ExperimentLogger
from otelfl.core.scenarios import SCENARIOS, apply_scenario


class TestScenarios:
    def test_all_scenarios_have_valid_flags(self, real_flagd_client: FlagdClient) -> None:
        """All scenario flags must exist in the real config."""
        flag_names = {f.name for f in real_flagd_client.list_flags()}
        for key, scenario in SCENARIOS.items():
            for flag_name in scenario.flags:
                assert flag_name in flag_names, (
                    f"Scenario '{key}' references unknown flag '{flag_name}'"
                )

    def test_apply_scenario_resets_first(self, real_flagd_client: FlagdClient) -> None:
        real_flagd_client.set_flag("adHighCpu", "on")
        apply_scenario(SCENARIOS["mild"], real_flagd_client)
        # adHighCpu should be reset to off
        assert real_flagd_client.get_flag("adHighCpu").default_variant == "off"
        # But scenario flags should be set
        assert real_flagd_client.get_flag("paymentFailure").default_variant == "10%"
        assert real_flagd_client.get_flag("imageSlowLoad").default_variant == "5sec"

    def test_apply_scenario_logs_events(self, real_flagd_client: FlagdClient) -> None:
        logger = ExperimentLogger()
        logger.start("test")
        apply_scenario(SCENARIOS["ad-chaos"], real_flagd_client, logger)
        assert len(logger.experiment.events) == 3  # 3 flags in ad-chaos
        for event in logger.experiment.events:
            assert event.event_type == "flag_change"

    def test_apply_full_outage(self, real_flagd_client: FlagdClient) -> None:
        changes = apply_scenario(SCENARIOS["full-outage"], real_flagd_client)
        assert len(changes) == 4
        assert real_flagd_client.get_flag("adFailure").default_variant == "on"
        assert real_flagd_client.get_flag("cartFailure").default_variant == "on"
        assert real_flagd_client.get_flag("paymentUnreachable").default_variant == "on"
        assert real_flagd_client.get_flag("loadGeneratorFloodHomepage").default_variant == "on"
