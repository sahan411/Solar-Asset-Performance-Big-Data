"""Streaming source: smart meters -> Kafka topic `meter-readings`.

Every simulated 15 minutes (about 3 real seconds), each of the 30 household
meters sends one reading:

    {"meter_id": "M001", "household_id": "H001", "grid_zone": "north",
     "power_consumption_kwh": 0.21, "solar_generation_kwh": 0.64,
     "timestamp": "2026-01-01T10:15:00Z"}

Both energy values are kWh used/generated during that 15-minute interval.

To exercise the processing layer, the simulator also:
  * sends about 2% bad records (negative values, missing fields, unknown zone,
    broken JSON), which Spark must reject;
  * re-sends about 1% of readings twice, which Spark must de-duplicate;
  * simulates heavy cloud over one zone from 10:00 to 14:00 every day (a
    different zone each day), so solar output drops and the low-renewable
    alert fires.

Every message carries two Kafka headers for tracing: `trace_id` (a stable ID
for this reading) and `produced_at` (send time, used to measure latency).

All randomness is seeded, so every run produces the same data.
"""

import json
import logging
import math
import os
import random
import time
import uuid
from datetime import datetime, timedelta

from confluent_kafka import Producer

from common import sim_clock
from common.households import HOUSEHOLDS, ZONES, Household
from common.log import get_logger, log_event

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "meter-readings")
SEED = os.getenv("SIM_SEED", "42")
BAD_RECORD_RATE = float(os.getenv("BAD_RECORD_RATE", "0.02"))
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.01"))
SLOT = timedelta(minutes=15)

log = get_logger("meter-simulator")


def household_load_kw(hour: int) -> float:
    """Typical household demand (kW) by hour: morning and evening peaks."""
    if hour < 6:
        return 0.35
    if hour < 9:
        return 0.9
    if hour < 17:
        return 0.55
    if hour < 22:
        return 1.3
    return 0.6


def weather_factor(zone: str, t: datetime) -> float:
    """How much sunlight reaches the panels (1.0 = clear sky)."""
    day_rng = random.Random(f"{SEED}:weather:{t.date()}:{zone}")
    factor = day_rng.uniform(0.75, 1.0)
    cloudy_zone = ZONES[sim_clock.day_index(t.date()) % len(ZONES)]
    if zone == cloudy_zone and 10 <= t.hour < 14:
        factor *= 0.05  # the daily storm: triggers the low-renewable alert
    return factor


def make_reading(h: Household, t: datetime, rng: random.Random) -> dict:
    hours = SLOT.total_seconds() / 3600
    consumption = household_load_kw(t.hour) * h.size_factor * rng.uniform(0.85, 1.15) * hours

    hour_of_day = t.hour + t.minute / 60
    sun = max(0.0, math.sin(math.pi * (hour_of_day - 6) / 12))  # daylight 06:00-18:00
    solar = h.solar_kw * sun * weather_factor(h.grid_zone, t) * rng.uniform(0.95, 1.05) * hours

    return {
        "meter_id": h.meter_id,
        "household_id": h.household_id,
        "grid_zone": h.grid_zone,
        "power_consumption_kwh": round(consumption, 4),
        "solar_generation_kwh": round(solar, 4),
        "timestamp": t.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def corrupt(reading: dict, rng: random.Random) -> bytes:
    """Turn a good reading into one of the bad-record types Spark must reject."""
    kind = rng.choice(["negative_value", "missing_household", "unknown_zone", "broken_json"])
    bad = dict(reading)
    if kind == "negative_value":
        bad["power_consumption_kwh"] = -abs(bad["power_consumption_kwh"])
    elif kind == "missing_household":
        bad["household_id"] = None
    elif kind == "unknown_zone":
        bad["grid_zone"] = "zone-x"
    else:
        return json.dumps(bad)[:40].encode()  # truncated, not valid JSON
    return json.dumps(bad).encode()


def trace_id_for(meter_id: str, t: datetime) -> str:
    """Stable ID for one reading, carried end to end (Kafka -> Spark -> PostgreSQL)."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"smart-grid/{meter_id}/{t.isoformat()}"))


def send_slot(producer: Producer, t: datetime) -> None:
    rng = random.Random(f"{SEED}:{t.isoformat()}")
    sent = bad = duplicates = 0
    for h in HOUSEHOLDS:
        reading = make_reading(h, t, rng)
        value = json.dumps(reading).encode()
        trace_id = trace_id_for(h.meter_id, t)
        if rng.random() < BAD_RECORD_RATE:
            value = corrupt(reading, rng)
            bad += 1
            log_event(log, "bad_record_sent", level=logging.WARNING, trace_id=trace_id,
                      meter_id=h.meter_id)
        # Tracing metadata travels in Kafka headers, so the payload keeps the
        # exact fields from the brief. produced_at lets Spark measure latency.
        headers = [("trace_id", trace_id.encode()),
                   ("produced_at", str(int(time.time() * 1000)).encode())]
        # Key by meter: all readings of one meter go to the same partition, in order.
        producer.produce(TOPIC, key=h.meter_id, value=value, headers=headers)
        sent += 1
        if rng.random() < DUPLICATE_RATE:
            producer.produce(TOPIC, key=h.meter_id, value=value, headers=headers)
            duplicates += 1
            log_event(log, "duplicate_sent", trace_id=trace_id, meter_id=h.meter_id)
    producer.poll(0)
    log_event(log, "readings_sent", sim_time=t.isoformat(), sent=sent,
              bad_records=bad, duplicates=duplicates)


def on_delivery(err, msg):
    if err is not None:
        log_event(log, "delivery_failed", level=logging.ERROR, error=str(err))


def main() -> None:
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "linger.ms": 50,
        "on_delivery": on_delivery,
    })
    log_event(log, "simulator_started", topic=TOPIC, households=len(HOUSEHOLDS),
              day_seconds=sim_clock.DAY_SECONDS, start_date=str(sim_clock.START_DATE))

    # Like real meters, send any readings missed while this process was not
    # running (it starts after Kafka is ready, or after a restart): at most one
    # simulated day back, and never before the first simulated day.
    sim_start = datetime.combine(sim_clock.START_DATE, datetime.min.time())
    last_slot = None
    while True:
        now = sim_clock.now()
        current = now.replace(minute=now.minute - now.minute % 15, second=0, microsecond=0)
        if last_slot is None:
            last_slot = max(sim_start, current - timedelta(days=1)) - SLOT
            if last_slot + SLOT < current:
                log_event(log, "backfilling_missed_readings", start=last_slot + SLOT, until=current)
        while last_slot < current:
            last_slot += SLOT
            send_slot(producer, last_slot)
        producer.poll(0)
        time.sleep(0.2)


if __name__ == "__main__":
    main()
