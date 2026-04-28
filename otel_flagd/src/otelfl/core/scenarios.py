"""Pre-configured chaos scenario presets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from otelfl.core.flagd_client import FlagdClient
from otelfl.core.experiment_logger import ExperimentLogger


@dataclass
class Scenario:
    name: str
    description: str
    flags: dict[str, str]  # flag_name -> variant
    users: int | None = None
    spawn_rate: float | None = None
    run_time: str | None = None


SCENARIOS: dict[str, Scenario] = {
    "mild": Scenario(
        name="Mild Degradation",
        description="10% payment failures + slow images",
        flags={
            "paymentFailure": "10%",
            "imageSlowLoad": "5sec",
        },
    ),
    "payment": Scenario(
        name="Payment Degradation",
        description="50% payment failures + payment service intermittent",
        flags={
            "paymentFailure": "50%",
        },
    ),
    "ad-chaos": Scenario(
        name="Ad Service Chaos",
        description="High CPU + manual GC + ad failures",
        flags={
            "adHighCpu": "on",
            "adManualGc": "on",
            "adFailure": "on",
        },
    ),
    "resource-pressure": Scenario(
        name="Resource Pressure",
        description="High CPU + memory leak + Kafka queue problems",
        flags={
            "adHighCpu": "on",
            "adManualGc": "on",
            "emailMemoryLeak": "100x",
            "kafkaQueueProblems": "on",
        },
    ),
    "full-outage": Scenario(
        name="Full Outage",
        description="Cart + payment + ad all failing, homepage flood",
        flags={
            "adFailure": "on",
            "cartFailure": "on",
            "paymentUnreachable": "on",
            "loadGeneratorFloodHomepage": "on",
        },
    ),
    "llm-issues": Scenario(
        name="LLM Issues",
        description="Inaccurate LLM responses + rate limiting",
        flags={
            "llmInaccurateResponse": "on",
            "llmRateLimitError": "on",
        },
    ),
    "cascade": Scenario(
        name="Cascading Failure",
        description="Product catalog + recommendation + cart failures",
        flags={
            "productCatalogFailure": "on",
            "recommendationCacheFailure": "on",
            "cartFailure": "on",
        },
    ),
}


def apply_scenario(
    scenario: Scenario,
    client: FlagdClient,
    logger: ExperimentLogger | None = None,
) -> list[tuple[str, str, str]]:
    """Apply a scenario's flag settings. Returns list of (flag, previous, new) changes."""
    # First reset all flags
    client.reset_all()
    changes = []
    for flag_name, variant in scenario.flags.items():
        flag = client.get_flag(flag_name)
        previous = flag.default_variant
        client.set_flag(flag_name, variant)
        changes.append((flag_name, previous, variant))
        if logger:
            logger.log_flag_change(flag_name, variant, previous)
    return changes
