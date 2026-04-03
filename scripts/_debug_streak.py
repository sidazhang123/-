"""Debug: check which bars are qualified in the test data."""
import numpy as np

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

is_bull = [(c > o and o > 0) for o, c in zip(opens_list, closes)]
gains = [(c - o) / o if o > 0 else 999 for o, c in zip(opens_list, closes)]
is_q = [b and g <= 0.02 for b, g in zip(is_bull, gains)]
print("Qualified bars:", [i for i, q in enumerate(is_q) if q])
run = 0
for i in range(120):
    run = (run + 1) if is_q[i] else 0
    if run >= 5:
        print(f"  Streak ending at {i}, run={run}")

if not any(run >= 5 for _ in [0]):
    print("No streak of 5 found")
