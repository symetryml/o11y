"""Client for reading/writing flagd feature flag configuration."""

from __future__ import annotations

from typing import Any

import httpx

from otelfl.models import FlagDefinition


class FlagdError(Exception):
    """Base error for flagd operations."""


class FlagNotFoundError(FlagdError):
    """Raised when a flag name is not found."""


class InvalidVariantError(FlagdError):
    """Raised when setting an invalid variant."""


class FlagdClient:
    """HTTP client for flagd-ui REST API.

    Talks to the flagd-ui service (e.g. http://host:8080/feature/) to read
    and write flag configuration. The flagd-ui writes the config file on disk
    and flagd's file watcher hot-reloads it.
    """

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self._http = httpx.Client(timeout=10.0)

    def _read_config(self) -> dict[str, Any]:
        try:
            resp = self._http.get(f"{self.base_url}/api/read")
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as e:
            raise FlagdError(f"Failed to read flags from {self.base_url}: {e}") from e

    def _write_config(self, config: dict[str, Any]) -> None:
        try:
            resp = self._http.post(f"{self.base_url}/api/write", json={"data": config})
            resp.raise_for_status()
        except httpx.HTTPError as e:
            raise FlagdError(f"Failed to write flags to {self.base_url}: {e}") from e

    def _parse_flag(self, name: str, data: dict[str, Any]) -> FlagDefinition:
        return FlagDefinition(
            name=name,
            description=data.get("description", ""),
            state=data.get("state", "ENABLED"),
            variants=data.get("variants", {}),
            default_variant=data.get("defaultVariant", "off"),
        )

    def list_flags(self) -> list[FlagDefinition]:
        config = self._read_config()
        return [self._parse_flag(name, data) for name, data in config.get("flags", {}).items()]

    def get_flag(self, name: str) -> FlagDefinition:
        config = self._read_config()
        flags = config.get("flags", {})
        if name not in flags:
            raise FlagNotFoundError(f"Flag not found: {name}")
        return self._parse_flag(name, flags[name])

    def set_flag(self, name: str, variant: str) -> FlagDefinition:
        """Set a flag's defaultVariant to the given variant name."""
        config = self._read_config()
        flags = config.get("flags", {})
        if name not in flags:
            raise FlagNotFoundError(f"Flag not found: {name}")
        if variant not in flags[name]["variants"]:
            available = list(flags[name]["variants"].keys())
            raise InvalidVariantError(
                f"Invalid variant '{variant}' for flag '{name}'. Available: {available}"
            )
        flags[name]["defaultVariant"] = variant
        self._write_config(config)
        return self._parse_flag(name, flags[name])

    def toggle_flag(self, name: str) -> FlagDefinition:
        """Toggle a 2-variant flag between its two variants."""
        flag = self.get_flag(name)
        if len(flag.variants) != 2:
            raise FlagdError(
                f"Cannot toggle flag '{name}' with {len(flag.variants)} variants. "
                f"Use set_flag() to choose a specific variant."
            )
        other = [v for v in flag.variant_names if v != flag.default_variant][0]
        return self.set_flag(name, other)

    def set_flag_state(self, name: str, state: str) -> FlagDefinition:
        """Set a flag's state to ENABLED or DISABLED."""
        config = self._read_config()
        flags = config.get("flags", {})
        if name not in flags:
            raise FlagNotFoundError(f"Flag not found: {name}")
        if state not in ("ENABLED", "DISABLED"):
            raise FlagdError(f"Invalid state '{state}'. Must be ENABLED or DISABLED.")
        flags[name]["state"] = state
        self._write_config(config)
        return self._parse_flag(name, flags[name])

    def toggle_flag_state(self, name: str) -> FlagDefinition:
        """Toggle a flag between ENABLED and DISABLED."""
        flag = self.get_flag(name)
        new_state = "DISABLED" if flag.state == "ENABLED" else "ENABLED"
        return self.set_flag_state(name, new_state)

    def get_snapshot(self) -> dict[str, str]:
        """Return a dict of {flag_name: default_variant} for all flags."""
        return {f.name: f.default_variant for f in self.list_flags()}

    def apply_snapshot(self, snapshot: dict[str, str]) -> list[tuple[str, str, str]]:
        """Apply a flag snapshot. Returns list of (flag, previous, new) changes."""
        changes = []
        for flag_name, variant in snapshot.items():
            try:
                flag = self.get_flag(flag_name)
                previous = flag.default_variant
                if previous != variant:
                    self.set_flag(flag_name, variant)
                    changes.append((flag_name, previous, variant))
            except (FlagNotFoundError, InvalidVariantError):
                continue
        return changes

    def reset_flag(self, name: str) -> FlagDefinition:
        """Reset a flag to 'off' variant."""
        return self.set_flag(name, "off")

    def reset_all(self) -> list[FlagDefinition]:
        """Reset all flags to 'off' variant."""
        config = self._read_config()
        flags = config.get("flags", {})
        for flag_data in flags.values():
            if "off" in flag_data["variants"]:
                flag_data["defaultVariant"] = "off"
        self._write_config(config)
        return self.list_flags()
