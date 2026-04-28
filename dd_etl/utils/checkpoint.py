"""High-water-mark checkpoint for gap detection and recovery."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


class Checkpoint:
    """Persists the last-seen timestamp so the receiver knows where
    to resume after a restart (and whether to trigger a backfill).
    """

    def __init__(self, path: str = ".dd_etl_checkpoint.json"):
        self.path = Path(path)

    def get_last_seen(self) -> datetime | None:
        """Load last-seen timestamp from the checkpoint file."""
        if not self.path.exists():
            return None
        try:
            data = json.loads(self.path.read_text())
            ts_str = data.get("last_seen_ts")
            if ts_str:
                return datetime.fromisoformat(ts_str)
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning(f"Failed to read checkpoint: {e}")
        return None

    def update(self, timestamp: datetime) -> None:
        """Save a new high-water-mark timestamp."""
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)

        data = {"last_seen_ts": timestamp.isoformat()}
        self.path.write_text(json.dumps(data, indent=2))

    def detect_gap(
        self,
        current_time: datetime | None = None,
        max_gap_seconds: int = 300,
    ) -> tuple[datetime, datetime] | None:
        """Check if there is a gap between last checkpoint and now.

        Args:
            current_time: Override for "now" (useful for testing).
            max_gap_seconds: Minimum gap size (seconds) to consider.

        Returns:
            (gap_start, gap_end) if a gap is detected, else None.
        """
        last_seen = self.get_last_seen()
        if last_seen is None:
            return None

        now = current_time or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        gap = (now - last_seen).total_seconds()
        if gap > max_gap_seconds:
            return (last_seen, now)
        return None
