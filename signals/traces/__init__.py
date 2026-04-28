"""Trace signal — Jaeger gRPC client."""

import os as _os
import sys as _sys

# The generated protobuf files use bare imports like `import model_pb2`
# and `from gogoproto import gogo_pb2`. Add _proto/ to sys.path so they resolve.
_proto_dir = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "_proto")
if _proto_dir not in _sys.path:
    _sys.path.insert(0, _proto_dir)

from signals.traces.jaeger import (
    list_services,
    list_operations,
    fetch_traces,
    get_trace_by_id,
    aggregate_spans_to_traces,
)
