"""Data models for otelFL."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class FlagDefinition:
    name: str
    description: str
    state: str
    variants: dict[str, Any]
    default_variant: str

    @property
    def variant_type(self) -> str:
        """Return 'boolean' if all variants are bool, else 'multi'."""
        if all(isinstance(v, bool) for v in self.variants.values()):
            return "boolean"
        return "multi"

    @property
    def is_boolean(self) -> bool:
        return self.variant_type == "boolean"

    @property
    def current_value(self) -> Any:
        return self.variants.get(self.default_variant)

    @property
    def variant_names(self) -> list[str]:
        return list(self.variants.keys())


@dataclass
class EndpointStats:
    name: str
    method: str
    num_requests: int = 0
    num_failures: int = 0
    current_rps: float = 0.0
    avg_response_time: float = 0.0
    max_response_time: float = 0.0
    min_response_time: float = 0.0
    p50: float = 0.0
    p90: float = 0.0
    p99: float = 0.0


@dataclass
class LocustStats:
    state: str = "unknown"
    user_count: int = 0
    total_rps: float = 0.0
    fail_ratio: float = 0.0
    total_avg_response_time: float = 0.0
    total_max_response_time: float = 0.0
    total_min_response_time: float = 0.0
    errors: list[dict[str, Any]] = field(default_factory=list)
    endpoints: list[EndpointStats] = field(default_factory=list)


@dataclass
class ExperimentEvent:
    timestamp: datetime
    event_type: str  # "flag_change", "load_change", "note"
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "event_type": self.event_type,
            "details": self.details,
        }


@dataclass
class Experiment:
    name: str
    started_at: datetime
    stopped_at: datetime | None = None
    events: list[ExperimentEvent] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "started_at": self.started_at.isoformat(),
            "stopped_at": self.stopped_at.isoformat() if self.stopped_at else None,
            "events": [e.to_dict() for e in self.events],
        }


@dataclass
class RunMode:
    name: str
    users: int
    spawn_rate: float

    def __str__(self) -> str:
        return f"{self.name} ({self.users}u, {self.spawn_rate}/s)"


NORMAL_MODE = RunMode("normal", users=5, spawn_rate=1.0)
LOW_MODE = RunMode("low", users=2, spawn_rate=1.0)
HIGH_MODE = RunMode("high", users=20, spawn_rate=2.0)

RUN_MODES: dict[str, RunMode] = {
    "normal": NORMAL_MODE,
    "low": LOW_MODE,
    "high": HIGH_MODE,
}
