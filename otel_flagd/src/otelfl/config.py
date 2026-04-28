"""Configuration with env var overrides."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Settings:
    flagd_url: str = field(
        default_factory=lambda: os.environ.get("OTELFL_FLAGD_URL", "http://localhost:8080/feature")
    )
    locust_url: str = field(
        default_factory=lambda: os.environ.get("OTELFL_LOCUST_URL", "http://localhost:8080/loadgen/")
    )
    poll_interval: float = field(
        default_factory=lambda: float(os.environ.get("OTELFL_POLL_INTERVAL", "2.0"))
    )
    ts_dir: Path | None = field(
        default_factory=lambda: Path(d) if (d := os.environ.get("OTELFL_TS_DIR")) else None
    )
