"""End-to-end smoke test for the running SolarIQ stack.

Run this against a live deployment — `scripts/bootstrap.sh` then
`scripts/demo_start.sh` — never against mocked dependencies. It is the last
check before an assessment demo: if this fails, the demo will fail.

    python tests/e2e/smoke_test.py
    python tests/e2e/smoke_test.py --full   # also wait for the daily batch

Requires only the standard library plus `requests` (already a transitive
dependency via processing/requirements.txt's ecosystem; install directly with
`pip install requests` if running outside that environment).

Exit code 0 means every check passed; non-zero means at least one failed, with
the failure printed above the summary.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from typing import Callable

import requests

DEFAULT_BASE_URL = "http://localhost:8000"


class SmokeCheckFailed(RuntimeError):
    """Raised when a check's condition is never satisfied within its timeout."""


def wait_until(predicate: Callable[[], bool], timeout: float, interval: float = 2.0, description: str = "") -> None:
    """Poll `predicate` until it returns True or `timeout` seconds elapse.

    Never a fixed sleep: the pipeline's timing depends on Kafka/Spark/Airflow
    catching up, which varies run to run, so a bare `sleep(N)` would either be
    flaky (too short) or waste the demo's time budget (too long).
    """
    deadline = time.time() + timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            if predicate():
                return
        except requests.RequestException as exc:
            last_error = exc
        time.sleep(interval)
    suffix = f" (last error: {last_error})" if last_error else ""
    raise SmokeCheckFailed(f"Timed out after {timeout}s waiting for: {description}{suffix}")


class Runner:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.failures: list[str] = []

    def check(self, name: str, fn: Callable[[], None]) -> None:
        print(f"  [ ] {name}", end="", flush=True)
        try:
            fn()
        except SmokeCheckFailed as exc:
            print(f"\r  [FAIL] {name}\n        {exc}")
            self.failures.append(name)
        except Exception as exc:  # noqa: BLE001 - report and continue to the next check
            print(f"\r  [FAIL] {name}\n        Unexpected error: {exc}")
            self.failures.append(name)
        else:
            print(f"\r  [ OK ] {name}")

    def get(self, path: str, **params) -> requests.Response:
        return requests.get(f"{self.base_url}{path}", params=params or None, timeout=10)


def run(base_url: str, full: bool) -> int:
    runner = Runner(base_url)
    print(f"SolarIQ smoke test against {base_url}\n")

    def check_health() -> None:
        response = runner.get("/health")
        if response.status_code != 200:
            raise SmokeCheckFailed(f"/health returned {response.status_code}")

    def check_ready() -> None:
        def ready() -> bool:
            return runner.get("/ready").status_code == 200

        wait_until(ready, timeout=60, description="/ready returning 200 (database reachable)")

    def check_portfolio_live() -> None:
        def has_live_data() -> bool:
            response = runner.get("/api/v1/portfolio/live")
            if response.status_code != 200:
                return False
            body = response.json()
            return body["data_status"] == "LIVE" and body["current_power_kw"] is not None

        wait_until(has_live_data, timeout=120, description="portfolio/live reporting LIVE data")

    def check_at_least_five_plants() -> None:
        response = runner.get("/api/v1/plants")
        if response.status_code != 200:
            raise SmokeCheckFailed(f"/api/v1/plants returned {response.status_code}")
        plants = response.json()
        if len(plants) < 5:
            raise SmokeCheckFailed(f"expected at least 5 plants, got {len(plants)}")

    def check_active_alert_appears() -> None:
        # The deterministic anomaly timeline (docs/data-contracts.md section 7)
        # guarantees an underperformance window within the first simulated day;
        # under the default demo clock that is comfortably inside 3 minutes.
        def has_active_alert() -> bool:
            response = runner.get("/api/v1/alerts", status="active")
            return response.status_code == 200 and len(response.json()) > 0

        wait_until(has_active_alert, timeout=180, description="at least one active alert after the anomaly window")

    def check_daily_summary_exists() -> None:
        # Only meaningful once a full simulated day (default: 300s) plus an
        # Airflow DAG run has elapsed — opt-in via --full so a quick pre-demo
        # check isn't stuck waiting on it.
        today = datetime.now(timezone.utc).date().isoformat()

        def has_daily_summary() -> bool:
            response = runner.get("/api/v1/reports/daily", date=today)
            return response.status_code == 200

        wait_until(has_daily_summary, timeout=480, description=f"daily reconciliation report for {today}")

    runner.check("API health returns 200", check_health)
    runner.check("API ready returns 200 (database available)", check_ready)
    runner.check("Portfolio live endpoint reports live metrics", check_portfolio_live)
    runner.check("At least 5 plants are returned", check_at_least_five_plants)
    runner.check("An active alert appears after the anomaly window", check_active_alert_appears)
    if full:
        runner.check("Daily summary exists after Airflow reconciliation", check_daily_summary_exists)

    print()
    if runner.failures:
        print(f"FAILED: {len(runner.failures)} check(s) did not pass: {', '.join(runner.failures)}")
        return 1
    print("All checks passed.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help=f"API base URL (default: {DEFAULT_BASE_URL})")
    parser.add_argument(
        "--full", action="store_true", help="also wait for the Airflow daily reconciliation to complete"
    )
    args = parser.parse_args(argv)
    return run(args.base_url, args.full)


if __name__ == "__main__":
    sys.exit(main())
