"""Simulated clock shared by both data sources.

One simulated day lasts SIM_DAY_SECONDS real seconds (default 300 = 5 minutes,
a 288x speed-up), so several "days" can be demonstrated in one session.

Both simulators must agree on the current simulated time. The first process to
start writes the real start time to SIM_CLOCK_FILE, and every process computes
simulated time from that same origin:

    simulated_now = SIM_START_DATE 00:00 + (real_now - real_origin) * 288
"""

import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path

DAY_SECONDS = float(os.getenv("SIM_DAY_SECONDS", "300"))
START_DATE = date.fromisoformat(os.getenv("SIM_START_DATE", "2026-01-01"))
SPEEDUP = 86400 / DAY_SECONDS
ORIGIN_FILE = Path(os.getenv("SIM_CLOCK_FILE", "/data/sim_clock_origin.txt"))

_origin = None


def _real_origin() -> float:
    global _origin
    if _origin is None:
        ORIGIN_FILE.parent.mkdir(parents=True, exist_ok=True)
        try:
            # "x" mode fails if the file exists, so only the first process writes it.
            with ORIGIN_FILE.open("x") as f:
                f.write(str(time.time()))
        except FileExistsError:
            pass
        # Another process may have created the file but not written it yet.
        for _ in range(50):
            text = ORIGIN_FILE.read_text().strip()
            if text:
                _origin = float(text)
                break
            time.sleep(0.1)
        else:
            raise RuntimeError(f"clock origin file {ORIGIN_FILE} is empty")
    return _origin


def now() -> datetime:
    """Current simulated time (naive datetime, UTC)."""
    origin = _real_origin()  # before time.time(): the first call may create the origin
    elapsed_real = max(0.0, time.time() - origin)
    start = datetime.combine(START_DATE, datetime.min.time())
    return start + timedelta(seconds=elapsed_real * SPEEDUP)


def day_index(day: date) -> int:
    """Number of simulated days since SIM_START_DATE (0 for the first day)."""
    return (day - START_DATE).days
