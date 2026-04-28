"""Tests for FlagdClient."""

from __future__ import annotations

import httpx
import pytest

from otelfl.core.flagd_client import (
    FlagdClient,
    FlagdError,
    FlagNotFoundError,
    InvalidVariantError,
)


class TestListFlags:
    def test_lists_all_flags(self, flagd_client: FlagdClient) -> None:
        flags = flagd_client.list_flags()
        assert len(flags) == 2
        names = {f.name for f in flags}
        assert names == {"boolFlag", "multiFlag"}

    def test_flag_variant_types(self, flagd_client: FlagdClient) -> None:
        flags = {f.name: f for f in flagd_client.list_flags()}
        assert flags["boolFlag"].variant_type == "boolean"
        assert flags["boolFlag"].is_boolean is True
        assert flags["multiFlag"].variant_type == "multi"
        assert flags["multiFlag"].is_boolean is False


class TestGetFlag:
    def test_get_existing_flag(self, flagd_client: FlagdClient) -> None:
        flag = flagd_client.get_flag("boolFlag")
        assert flag.name == "boolFlag"
        assert flag.default_variant == "off"
        assert flag.current_value is False

    def test_get_nonexistent_flag_raises(self, flagd_client: FlagdClient) -> None:
        with pytest.raises(FlagNotFoundError, match="nosuch"):
            flagd_client.get_flag("nosuch")


class TestSetFlag:
    def test_set_valid_variant(self, flagd_client: FlagdClient) -> None:
        flag = flagd_client.set_flag("boolFlag", "on")
        assert flag.default_variant == "on"
        assert flag.current_value is True
        # Verify persisted
        reloaded = flagd_client.get_flag("boolFlag")
        assert reloaded.default_variant == "on"

    def test_set_multi_variant(self, flagd_client: FlagdClient) -> None:
        flag = flagd_client.set_flag("multiFlag", "high")
        assert flag.default_variant == "high"
        assert flag.current_value == 75

    def test_set_invalid_variant_raises(self, flagd_client: FlagdClient) -> None:
        with pytest.raises(InvalidVariantError, match="invalid"):
            flagd_client.set_flag("boolFlag", "invalid")

    def test_set_nonexistent_flag_raises(self, flagd_client: FlagdClient) -> None:
        with pytest.raises(FlagNotFoundError):
            flagd_client.set_flag("nosuch", "on")


class TestToggleFlag:
    def test_toggle_boolean_flag(self, flagd_client: FlagdClient) -> None:
        flag = flagd_client.toggle_flag("boolFlag")
        assert flag.default_variant == "on"
        flag = flagd_client.toggle_flag("boolFlag")
        assert flag.default_variant == "off"

    def test_toggle_multi_variant_raises(self, flagd_client: FlagdClient) -> None:
        with pytest.raises(FlagdError, match="Cannot toggle"):
            flagd_client.toggle_flag("multiFlag")


class TestResetFlag:
    def test_reset_single_flag(self, flagd_client: FlagdClient) -> None:
        flagd_client.set_flag("boolFlag", "on")
        flag = flagd_client.reset_flag("boolFlag")
        assert flag.default_variant == "off"

    def test_reset_all_flags(self, flagd_client: FlagdClient) -> None:
        flagd_client.set_flag("boolFlag", "on")
        flagd_client.set_flag("multiFlag", "high")
        flags = flagd_client.reset_all()
        for f in flags:
            assert f.default_variant == "off"


class TestHTTPErrors:
    def test_connection_error_raises(self) -> None:
        def raise_error(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("refused")

        client = FlagdClient("http://bad-host:8080/feature")
        client._http = httpx.Client(transport=httpx.MockTransport(raise_error))
        with pytest.raises(FlagdError, match="Failed to read"):
            client.list_flags()


class TestFlagState:
    def test_set_flag_state_disabled(self, flagd_client: FlagdClient) -> None:
        flag = flagd_client.set_flag_state("boolFlag", "DISABLED")
        assert flag.state == "DISABLED"
        reloaded = flagd_client.get_flag("boolFlag")
        assert reloaded.state == "DISABLED"

    def test_set_flag_state_enabled(self, flagd_client: FlagdClient) -> None:
        flagd_client.set_flag_state("boolFlag", "DISABLED")
        flag = flagd_client.set_flag_state("boolFlag", "ENABLED")
        assert flag.state == "ENABLED"

    def test_toggle_flag_state(self, flagd_client: FlagdClient) -> None:
        flag = flagd_client.toggle_flag_state("boolFlag")
        assert flag.state == "DISABLED"
        flag = flagd_client.toggle_flag_state("boolFlag")
        assert flag.state == "ENABLED"

    def test_invalid_state_raises(self, flagd_client: FlagdClient) -> None:
        with pytest.raises(FlagdError, match="Invalid state"):
            flagd_client.set_flag_state("boolFlag", "INVALID")


class TestSnapshot:
    def test_get_snapshot(self, flagd_client: FlagdClient) -> None:
        snapshot = flagd_client.get_snapshot()
        assert snapshot == {"boolFlag": "off", "multiFlag": "off"}

    def test_apply_snapshot(self, flagd_client: FlagdClient) -> None:
        changes = flagd_client.apply_snapshot({"boolFlag": "on", "multiFlag": "high"})
        assert len(changes) == 2
        assert flagd_client.get_flag("boolFlag").default_variant == "on"
        assert flagd_client.get_flag("multiFlag").default_variant == "high"

    def test_apply_snapshot_skips_unknown_flags(self, flagd_client: FlagdClient) -> None:
        changes = flagd_client.apply_snapshot({"boolFlag": "on", "unknownFlag": "on"})
        assert len(changes) == 1

    def test_apply_snapshot_skips_unchanged(self, flagd_client: FlagdClient) -> None:
        changes = flagd_client.apply_snapshot({"boolFlag": "off"})
        assert len(changes) == 0


