"""Debug: trace the sliding window behavior to find infinite loop."""
import sys
sys.path.insert(0, ".")

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from strategies.engine_commons import DetectionResult
from strategies.groups.consecutive_uptrends_v1.engine import detect_streak

def _make_ts_series(n, start=None, freq_days=1):
    base = start or datetime(2020, 1, 6)
    return [base + timedelta(days=i * freq_days) for i in range(n)]

np.random.seed(100)
base = 10.0
closes = []
opens_list = []
for i in range(120):
    if 20 <= i < 25 or 60 <= i < 65:
        base *= 1.01
        o = base / 1.01
    else:
        delta = np.random.uniform(-0.3, 0.15)
        base = max(base + delta, 5.0)
        o = base + np.random.uniform(-0.1, 0.4)
    closes.append(round(base, 4))
    opens_list.append(round(o, 4))

n = len(closes)
df = pd.DataFrame({
    "code": ["TEST"] * n,
    "ts": _make_ts_series(n),
    "open": opens_list,
    "high": [max(o, c) + 0.1 for o, c in zip(opens_list, closes)],
    "low": [min(o, c) - 0.1 for o, c in zip(opens_list, closes)],
    "close": closes,
    "volume": [1000] * n,
})

params = {
    "scan_bars": 200,
    "streak_len": 5,
    "bar_gain_max": 0.02,
    "selloff_enabled": False,
    "selloff_start_pct": 0.5,
    "selloff_return_pct": 0.5,
    "selloff_bars": 3,
}

# Manual sliding window with tracing
pos = 1
iteration = 0
while pos < n:
    iteration += 1
    if iteration > 200:
        print(f"STOPPED: too many iterations at pos={pos}")
        break
    sub_df = df.iloc[:pos+1].copy()
    result = detect_streak(sub_df, params)
    if result.matched and result.pattern_end_idx is not None:
        print(f"iter={iteration} pos={pos} MATCH end_idx={result.pattern_end_idx} start_idx={result.pattern_start_idx}")
        pos = result.pattern_end_idx + 1
    else:
        pos += 1

print(f"Done after {iteration} iterations, final pos={pos}")
