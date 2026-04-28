#!/usr/bin/env python3
"""Generate expected test results from pandas for Go parity testing.

Run this script to produce testdata/expected.json which the Go integration
tests compare against.
"""

import json
import math
import numpy as np
import pandas as pd


def nan_safe(v):
    """Convert value for JSON serialization (NaN/Inf → None)."""
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
    if isinstance(v, (np.floating,)):
        if np.isnan(v) or np.isinf(v):
            return None
        return float(v)
    if isinstance(v, (np.int64, np.int32)):
        return int(v)
    return v


def series_to_list(s):
    return [nan_safe(v) for v in s.tolist()]


results = {}

# --- Series Aggregation ---
s = pd.Series([1.0, 2.0, 3.0, float("nan"), 5.0])
results["series_agg"] = {
    "mean": nan_safe(s.mean()),
    "sum": nan_safe(s.sum()),
    "min": nan_safe(s.min()),
    "max": nan_safe(s.max()),
    "std": nan_safe(s.std()),
    "count": int(s.count()),
}

# --- Series Shift ---
s = pd.Series([10.0, 20.0, 30.0, 40.0, 50.0])
results["shift_forward_2"] = series_to_list(s.shift(2))
results["shift_backward_1"] = series_to_list(s.shift(-1))

# --- Series Arithmetic ---
a = pd.Series([1.0, 2.0, 3.0, float("nan")])
b = pd.Series([4.0, 0.0, 6.0, 7.0])
results["sub"] = series_to_list(a - b)
results["div"] = series_to_list(a / b)
results["abs"] = series_to_list((a - b).abs())

# --- Rolling ---
s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
results["rolling_mean_3_1"] = series_to_list(s.rolling(3, min_periods=1).mean())
results["rolling_std_3_2"] = series_to_list(s.rolling(3, min_periods=2).std())

# --- GroupBy Shift ---
df = pd.DataFrame({
    "entity": ["A", "A", "A", "B", "B", "B"],
    "value": [10.0, 20.0, 30.0, 100.0, 200.0, 300.0],
})
results["groupby_shift_1"] = series_to_list(df.groupby("entity")["value"].shift(1))

# --- GroupBy Agg ---
df = pd.DataFrame({
    "group": ["X", "X", "Y", "Y", "Y"],
    "val": [1.0, 2.0, 3.0, 4.0, 5.0],
})
agg = df.groupby("group")["val"].agg(["mean", "sum", "min", "max", "std", "count"]).reset_index()
results["groupby_agg"] = {
    "groups": agg["group"].tolist(),
    "mean": series_to_list(agg["mean"]),
    "sum": series_to_list(agg["sum"]),
    "min": series_to_list(agg["min"]),
    "max": series_to_list(agg["max"]),
    "std": series_to_list(agg["std"]),
    "count": series_to_list(agg["count"]),
}

# --- Pivot Table ---
df = pd.DataFrame({
    "timestamp": ["t1", "t1", "t1", "t2", "t2", "t2"],
    "entity": ["A", "A", "A", "A", "A", "A"],
    "feature": ["cpu", "mem", "disk", "cpu", "mem", "disk"],
    "value": [0.5, 0.8, 0.3, 0.6, 0.7, 0.4],
})
pivot = df.pivot_table(
    index=["timestamp", "entity"],
    columns="feature",
    values="value",
    aggfunc="first",
)
pivot = pivot.reset_index()
pivot.columns.name = None
results["pivot"] = {
    "columns": list(pivot.columns),
    "nrows": len(pivot),
    "cpu": series_to_list(pivot["cpu"]),
    "mem": series_to_list(pivot["mem"]),
    "disk": series_to_list(pivot["disk"]),
}

# --- Melt ---
wide = pd.DataFrame({
    "timestamp": ["t1", "t2"],
    "cpu": [0.5, 0.6],
    "mem": [0.8, 0.7],
})
melted = pd.melt(wide, id_vars=["timestamp"], var_name="feature", value_name="value")
results["melt"] = {
    "nrows": len(melted),
    "timestamp": melted["timestamp"].tolist(),
    "feature": melted["feature"].tolist(),
    "value": series_to_list(melted["value"]),
}

# --- Concat axis=1 ---
df1 = pd.DataFrame({"a": [1.0, 2.0], "b": [3.0, 4.0]})
df2 = pd.DataFrame({"c": [5.0, 6.0], "d": [7.0, 8.0]})
cat = pd.concat([df1, df2], axis=1)
results["concat_cols"] = {
    "columns": list(cat.columns),
    "nrows": len(cat),
}

# --- Delta / Pct Change (grouped shift + arithmetic) ---
df = pd.DataFrame({
    "entity": ["A", "A", "A", "A", "B", "B", "B", "B"],
    "value": [10.0, 20.0, 30.0, 40.0, 100.0, 200.0, 300.0, 400.0],
})
df = df.sort_values(["entity"])
shifted = df.groupby("entity")["value"].shift(2)
delta = df["value"] - shifted
results["delta_shift2"] = series_to_list(delta)

# --- Replace Inf ---
s = pd.Series([1.0, float("inf"), -float("inf"), 3.0])
replaced = s.replace([np.inf, -np.inf], np.nan)
results["replace_inf"] = series_to_list(replaced)

# --- Drop Duplicates ---
df = pd.DataFrame({
    "a": ["x", "y", "x", "y", "z"],
    "b": [1.0, 2.0, 1.0, 2.0, 3.0],
})
deduped = df.drop_duplicates().reset_index(drop=True)
results["drop_duplicates"] = {
    "nrows": len(deduped),
    "a": deduped["a"].tolist(),
    "b": series_to_list(deduped["b"]),
}

# --- String EndsWith ---
s = pd.Series(["metric_total", "metric_bucket", "metric_count", "other_total"])
results["endswith_total"] = s.str.endswith("_total").tolist()

# --- Sort ---
df = pd.DataFrame({
    "a": ["B", "A", "C", "A"],
    "b": [2.0, 1.0, 3.0, 0.0],
})
sorted_df = df.sort_values(["a", "b"])
results["sort_by_a_b"] = {
    "a": sorted_df["a"].tolist(),
    "b": series_to_list(sorted_df["b"]),
}

with open("testdata/expected.json", "w") as f:
    json.dump(results, f, indent=2)

print(f"Generated {len(results)} test cases → testdata/expected.json")
