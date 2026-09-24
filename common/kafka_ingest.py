"""Batch-layer ingestion: read new Kafka messages and save them to the raw archive.

Called by the first Airflow task (ingest_from_kafka) every run. Airflow is the
batch layer's own Kafka subscriber (consumer group "airflow-batch"), completely
separate from Spark. Every message is saved exactly as received, including bad
ones, so the batch layer can always recompute any day from the original data.

Delivery is at-least-once: offsets are committed only after the lines are
written and flushed to disk. If a run fails in between, a few messages are
saved twice; the batch layer drops such duplicates.

See common/raw_archive.py for the file layout.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition

from common.raw_archive import (LATEST_EVENT_FILE, day_dir, event_time, latest_event_time,
                                partition_date)

CONSUMER_GROUP = "airflow-batch"
BATCH_SIZE = 500


def header(msg, name):
    for key, value in msg.headers() or []:
        if key == name:
            return value.decode()
    return None


def to_line(msg):
    """(event time or None, JSON line) for one Kafka message. The value is kept unchanged."""
    value = msg.value().decode("utf-8", errors="replace")
    line = json.dumps({
        "partition": msg.partition(),
        "offset": msg.offset(),
        "key": msg.key().decode() if msg.key() else None,
        "trace_id": header(msg, "trace_id"),
        "produced_at": header(msg, "produced_at"),
        "archived_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "value": value,
    })
    return event_time(value), line


def _append(by_file: dict[Path, list[str]]) -> None:
    for path, lines in by_file.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
            f.flush()
            os.fsync(f.fileno())


def _save_latest(raw_dir: Path, latest: datetime) -> None:
    path = raw_dir / LATEST_EVENT_FILE
    tmp = path.with_suffix(".tmp")
    tmp.write_text(latest.isoformat())
    tmp.replace(path)  # atomic: readers never see a half-written file


def ingest_new_messages(bootstrap: str, topic: str, raw_dir: Path, max_seconds: float = 50,
                        group: str = CONSUMER_GROUP) -> dict:
    """Read every message not yet ingested and append it to the raw archive.

    Works like a batch read: at the start it notes where each partition ends
    right now, reads from the last committed position up to that point, and
    stops. Messages that arrive during the run are picked up by the next run.
    Returns counts for logging and metrics.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,                   # its own group: independent of Spark
        "enable.auto.commit": False,         # commit only after writing to disk
        "auto.offset.reset": "earliest",     # if a committed offset has expired
    })
    started = time.time()
    latest = latest_event_time(raw_dir)
    messages_total, latencies = 0, []
    try:
        # 1. Where to start (last committed offset) and where to stop (end right now).
        partitions = [TopicPartition(topic, p)
                      for p in consumer.list_topics(topic, timeout=10).topics[topic].partitions]
        start_at, stop_at = {}, {}
        for tp in consumer.committed(partitions, timeout=10):
            low, high = consumer.get_watermark_offsets(tp, timeout=10)
            start_at[tp.partition] = tp.offset if tp.offset >= 0 else low
            stop_at[tp.partition] = high
        remaining = {p for p in stop_at if start_at[p] < stop_at[p]}
        consumer.assign([TopicPartition(topic, p, start_at[p]) for p in remaining])

        # 2. Read up to the stop offsets, writing and committing batch by batch.
        while remaining and time.time() - started < max_seconds:
            by_file: dict[Path, list[str]] = {}
            next_offset = {}
            for msg in consumer.consume(num_messages=BATCH_SIZE, timeout=1.0):
                p = msg.partition() if msg.error() is None else None
                if p not in remaining or msg.offset() >= stop_at[p]:
                    continue  # arrived after this run started: next run
                ts, line = to_line(msg)
                path = day_dir(raw_dir, partition_date(ts)) / f"part-{p}.jsonl"
                by_file.setdefault(path, []).append(line)
                next_offset[p] = msg.offset() + 1
                if ts:
                    latest = max(latest or ts, ts)
                produced_at = header(msg, "produced_at")
                if produced_at:
                    latencies.append(time.time() * 1000 - int(produced_at))
                messages_total += 1
            if not next_offset:
                continue

            _append(by_file)
            if latest:
                _save_latest(raw_dir, latest)
            # Only now are these messages "done".
            consumer.commit(offsets=[TopicPartition(topic, p, o) for p, o in next_offset.items()],
                            asynchronous=False)
            remaining -= {p for p, o in next_offset.items() if o >= stop_at[p]}
    finally:
        consumer.close()

    return {
        "messages": messages_total,
        "latest_event_time": latest.isoformat() if latest else None,
        "avg_latency_ms": int(sum(latencies) / len(latencies)) if latencies else None,
        "max_latency_ms": int(max(latencies)) if latencies else None,
        "duration_ms": int((time.time() - started) * 1000),
    }
