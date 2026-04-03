"""Debug: verify selloff test data and vectorized results."""
import sys
sys.path.insert(0, ".")

import math
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

def _make_ts_series(n, freq_days=1):
    base = datetime(2020, 1, 6)
    return [base + timedelta(days=i * freq_days) for i in range(n)]

# Reconstruct selloff test data
n = 60
closes = []
opens_list = []
base = 20.0
for i in range(n):
    if 30 <= i < 35:
        base *= 1.015
        o = base / 1.015
    elif 36 <= i < 39:
        base *= 0.97
        o = base / 0.97
    else:
        o = base + 0.5  # bearish
    closes.append(round(base, 4))
    opens_list.append(round(o, 4))

print(f"bar 34 close={closes[34]}, bar 35 close={closes[35]}, bar 36 close={closes[36]}, bar 37 close={closes[37]}, bar 38 close={closes[38]}")

# Check what the vectorized function returns
df = pd.DataFrame({
    "code": ["TEST"] * n,
    "ts": _make_ts_series(n),
    "open": opens_list,
    "high": [max(o, c) + 0.1 for o, c in zip(opens_list, closes)],
    "low": [min(o, c) - 0.1 for o, c in zip(opens_list, closes)],
    "close": closes,
    "volume": [1000] * n,
})

from strategies.groups.consecutive_uptrends_v1.engine import detect_streak_vectorized

params = {
    "scan_bars": 200,
    "streak_len": 5,
    "bar_gain_max": 0.02,
    "selloff_enabled": True,
    "selloff_start_pct": 0.5,
    "selloff_return_pct": 0.3,
    "selloff_bars": 3,
}

results = detect_streak_vectorized(df, params)
print(f"Results count: {len(results)}")
for r in results:
    print(f"  matched={r.matched}, start={r.pattern_start_idx}, end={r.pattern_end_idx}")


