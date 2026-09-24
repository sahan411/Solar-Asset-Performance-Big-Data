"""Daily-batch source: drops one tariff/billing CSV file per simulated day.

At the start of every simulated day the "utility billing system" publishes
data/incoming/tariffs_<date>.csv with one row per household:

    household_id,tariff_rate,billing_tier,subsidy_flag
    H001,0.1623,A,true

tariff_rate is in currency units per kWh and changes slightly every day.
About every third day one row is deliberately invalid (blank or negative
rate), so the batch layer's validation can be demonstrated.
"""

import csv
import os
import random
import time
from datetime import date
from pathlib import Path

from common import sim_clock
from common.households import HOUSEHOLDS
from common.log import get_logger, log_event

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
INCOMING = DATA_DIR / "incoming"
REPORTS = DATA_DIR / "reports"
RAW = DATA_DIR / "raw"
SEED = os.getenv("SIM_SEED", "42")

# Higher tiers pay a higher price per kWh.
BASE_RATE = {"A": 0.16, "B": 0.19, "C": 0.23}

log = get_logger("tariff-simulator")


def write_tariff_file(day: date) -> Path:
    rng = random.Random(f"{SEED}:tariff:{day}")
    daily_factor = rng.uniform(0.95, 1.05)  # e.g. fuel-cost adjustment
    rows = []
    for h in HOUSEHOLDS:
        rows.append({
            "household_id": h.household_id,
            "tariff_rate": f"{BASE_RATE[h.billing_tier] * daily_factor:.4f}",
            "billing_tier": h.billing_tier,
            "subsidy_flag": str(h.subsidy).lower(),
        })

    bad_rows = 0
    if sim_clock.day_index(day) % 3 == 2:
        rows[rng.randrange(len(rows))]["tariff_rate"] = rng.choice(["", "-0.1900"])
        bad_rows = 1

    path = INCOMING / f"tariffs_{day}.csv"
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)  # atomic: readers never see a half-written file
    log_event(log, "tariff_file_published", sim_date=str(day), file=path.name,
              rows=len(rows), deliberately_bad_rows=bad_rows)
    return path


def main() -> None:
    INCOMING.mkdir(parents=True, exist_ok=True)
    # The Airflow container runs as a different user and writes to these folders.
    for folder in (REPORTS, RAW):
        folder.mkdir(parents=True, exist_ok=True)
        folder.chmod(0o777)
    log_event(log, "simulator_started", incoming_dir=str(INCOMING))

    while True:
        today = sim_clock.now().date()
        if not (INCOMING / f"tariffs_{today}.csv").exists():
            write_tariff_file(today)
        time.sleep(1)


if __name__ == "__main__":
    main()
