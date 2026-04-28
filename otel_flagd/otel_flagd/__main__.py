"""Allow running as `python otel_flagd`."""

import sys
from pathlib import Path

# Ensure src/ is on the import path so otelfl can be found without pip install
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from otelfl.cli.app import main

main()
