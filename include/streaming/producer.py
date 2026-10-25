"""Kafka producer: replays Criteo-format rows as ad impression events.

Each row becomes one event that follows the data contract. The impression_id
is the SHA-256 of the raw row joined with \\x01, exactly as the batch Spark job
computes it, so the same impression has the same key in both paths.

    # real Kafka (docker compose -f docker-compose.streaming.yml up -d)
    python include/streaming/producer.py --src data/sample.tsv.gz --bootstrap localhost:9092 --limit 50000

    # no broker: write Kafka-shaped records to Parquet files (local dev and tests)
    python include/streaming/producer.py --src data/sample.tsv.gz --to-files data/kafka_files --files 5

Fault injection (--fault-rate etc.) simulates a buggy upstream service so the
consumer's DLQ, dedupe and late-data handling have something to do. Normal
production use validates every event against the contract before sending and
never sends an invalid one.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import random
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from contract import load_contract  # noqa: E402

# Same order as include/spark/common.py RAW_COLUMNS (a test checks they match).
RAW_COLUMNS = [
    "timestamp", "uid", "campaign", "conversion", "conversion_timestamp",
    "conversion_id", "attribution", "click", "click_pos", "click_nb", "cost",
    "cpo", "time_since_last_click",
    "cat1", "cat2", "cat3", "cat4", "cat5", "cat6", "cat7", "cat8", "cat9",
]
FAULTS = ["invalid_json", "missing_required", "wrong_type", "negative_cost", "unknown_campaign",
          "future_event", "bad_version"]
# what the consumer must report for each injected fault
EXPECTED_REASON = {
    "invalid_json": "invalid_json",
    "missing_required": "missing_required:user_id",
    "wrong_type": "type_mismatch",
    "negative_cost": "constraint:cost:minimum",
    "unknown_campaign": "quality:campaign_known",
    "future_event": "quality:not_from_future",
    "bad_version": "unsupported_schema_version",
}


@dataclass
class Record:
    key: bytes
    value: bytes
    headers: list[tuple[str, bytes]]
    expect: str                      # valid | duplicate | late | dlq:<reason>
    impression_id: str | None = None
    event: dict = field(default_factory=dict)


def impression_id(raw: dict) -> str:
    return hashlib.sha256("\x01".join(raw.get(c) or "" for c in RAW_COLUMNS).encode()).hexdigest()


def read_rows(path: str, limit: int | None = None):
    with gzip.open(path, "rt", newline="") as f:
        for i, row in enumerate(csv.DictReader(f, delimiter="\t")):
            if limit and i >= limit:
                return
            yield row


def _int(v):
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return None


def _neg1(v):
    return None if v == -1 else v


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def to_event(raw: dict, anchor: datetime, schema_version: str, produced_delay_s: float = 2.0) -> dict | None:
    """Typed event per the contract, or None if the raw row is unusable (the
    real producer would never have emitted it)."""
    ts, uid, camp = _int(raw.get("timestamp")), _int(raw.get("uid")), _int(raw.get("campaign"))
    click, conv = _int(raw.get("click")), _int(raw.get("conversion"))
    try:
        cost = float(str(raw.get("cost")).strip())
    except (TypeError, ValueError):
        cost = None
    if None in (ts, uid, camp, click, conv, cost) or cost < 0:
        return None
    event_ts = anchor + timedelta(seconds=ts)
    conv_ts = _neg1(_int(raw.get("conversion_timestamp")))
    cpo = raw.get("cpo")
    try:
        cpo = _neg1(float(str(cpo).strip()))
    except (TypeError, ValueError):
        cpo = None
    attribution = _int(raw.get("attribution"))
    return {
        "schema_version": schema_version,
        "impression_id": impression_id(raw),
        "event_ts": _iso(event_ts),
        "produced_at": _iso(event_ts + timedelta(seconds=produced_delay_s)),
        "user_id": str(raw["uid"]).strip(),
        "campaign_id": camp,
        "is_click": click == 1,
        "is_conversion_path": conv == 1,
        "cost": cost,
        "conversion_ts": _iso(anchor + timedelta(seconds=conv_ts)) if conv_ts is not None else None,
        "conversion_id": _neg1(_int(raw.get("conversion_id"))),
        "is_attributed": None if attribution is None else attribution == 1,
        "click_pos": _neg1(_int(raw.get("click_pos"))),
        "click_nb": _neg1(_int(raw.get("click_nb"))),
        "cpo": cpo,
        "time_since_last_click_s": _neg1(_int(raw.get("time_since_last_click"))),
        **{f"cat{i}": (raw.get(f"cat{i}") or "").strip() or None for i in range(1, 10)},
    }


def _inject(event: dict, fault: str) -> bytes:
    e = dict(event)
    if fault == "invalid_json":
        return json.dumps(e)[: 40].encode()          # truncated payload
    if fault == "missing_required":
        e.pop("user_id")
    elif fault == "wrong_type":
        e["campaign_id"] = "not-a-number"
    elif fault == "negative_cost":
        e["cost"] = -0.5
    elif fault == "unknown_campaign":
        e["campaign_id"] = 1                          # never in the campaign list
    elif fault == "future_event":
        e["event_ts"] = _iso(datetime.fromisoformat(e["produced_at"].replace("Z", "+00:00")) + timedelta(hours=1))
    elif fault == "bad_version":
        e["schema_version"] = "2.0.0"
    return json.dumps(e).encode()


def build_stream(rows, anchor: datetime, fault_rate: float = 0.0, dup_rate: float = 0.0,
                 late_rate: float = 0.0, seed: int = 7, validate: bool = True) -> tuple[list[Record], dict]:
    """Turn raw rows into an ordered list of Kafka records with expected outcomes.
    - duplicates: an already-sent valid event is re-sent later (producer retry)
    - late: a valid event is held back and sent at the end of the stream, after
      the watermark has passed its event time
    - faults: one of FAULTS, cycled, replacing a valid event
    """
    contract = load_contract()
    rng = random.Random(seed)
    out, held_late, stats = [], [], {"rows": 0, "skipped_invalid_source_rows": 0, "producer_rejected": 0}
    fault_i = 0
    for raw in rows:
        stats["rows"] += 1
        ev = to_event(raw, anchor, contract.version)
        if ev is None:
            stats["skipped_invalid_source_rows"] += 1
            continue
        if validate and contract.validate(ev):
            stats["producer_rejected"] += 1          # never sent
            continue
        key = str(ev["campaign_id"]).encode()
        headers = [("schema_version", contract.version.encode()), ("contract", contract.name.encode())]
        u = rng.random()
        if u < fault_rate:
            f = FAULTS[fault_i % len(FAULTS)]
            fault_i += 1
            out.append(Record(key, _inject(ev, f), headers, f"dlq:{EXPECTED_REASON[f]}", ev["impression_id"], ev))
            continue
        rec = Record(key, json.dumps(ev).encode(), headers, "valid", ev["impression_id"], ev)
        if u < fault_rate + late_rate:
            held_late.append(Record(rec.key, rec.value, rec.headers, "late", rec.impression_id, ev))
            continue
        out.append(rec)
        if rng.random() < dup_rate:
            out.append(Record(rec.key, rec.value, rec.headers, "duplicate", rec.impression_id, ev))
    out.extend(held_late)
    stats.update({k: sum(r.expect.startswith(k) for r in out) for k in ("valid", "duplicate", "late", "dlq")})
    return out, stats


# --------------------------------------------------------------------------- #
# outputs
# --------------------------------------------------------------------------- #
def write_kafka_files(records: list[Record], out_dir: str, n_files: int, topic: str, partitions: int = 6,
                      start: datetime | None = None) -> list[str]:
    """Write records as Parquet files with Kafka's source schema (key, value,
    topic, partition, offset, timestamp, timestampType, headers). The streaming
    job reads these with exactly the same parsing code as the Kafka source,
    one file per micro-batch."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    os.makedirs(out_dir, exist_ok=True)
    offsets = [0] * partitions
    per_file = max(1, -(-len(records) // n_files))
    paths = []
    for f in range(n_files):
        chunk = records[f * per_file:(f + 1) * per_file]
        if not chunk:
            break
        file_ts = datetime.now(timezone.utc) if start is None else start + timedelta(seconds=f)
        rows = {"key": [], "value": [], "topic": [], "partition": [], "offset": [], "timestamp": [],
                "timestampType": [], "headers": []}
        for i, r in enumerate(chunk):
            p = int.from_bytes(hashlib.md5(r.key).digest()[:4], "big") % partitions
            rows["key"].append(r.key); rows["value"].append(r.value); rows["topic"].append(topic)
            rows["partition"].append(p); rows["offset"].append(offsets[p]); offsets[p] += 1
            # broker append time: the moment the file was written (+1 microsecond per record for ordering)
            rows["timestamp"].append(file_ts + timedelta(microseconds=i))
            rows["timestampType"].append(0)
            rows["headers"].append([{"key": k, "value": v} for k, v in r.headers])
        schema = pa.schema([("key", pa.binary()), ("value", pa.binary()), ("topic", pa.string()),
                            ("partition", pa.int32()), ("offset", pa.int64()),
                            ("timestamp", pa.timestamp("us", tz="UTC")), ("timestampType", pa.int32()),
                            ("headers", pa.list_(pa.struct([("key", pa.string()), ("value", pa.binary())])))])
        path = os.path.join(out_dir, f"part-{f:05d}.parquet")
        pq.write_table(pa.table(rows, schema=schema), path)
        paths.append(path)
    return paths


def send_to_kafka(records: list[Record], bootstrap: str, topic: str, rate: float = 0.0) -> int:
    from confluent_kafka import Producer

    p = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all",
                  "linger.ms": 20, "compression.type": "zstd"})
    errors = []
    t0 = time.monotonic()
    for i, r in enumerate(records):
        p.produce(topic, key=r.key, value=r.value, headers=r.headers,
                  on_delivery=lambda err, msg: errors.append(err) if err else None)
        p.poll(0)
        if rate:
            ahead = (i + 1) / rate - (time.monotonic() - t0)
            if ahead > 0:
                time.sleep(ahead)
    p.flush(30)
    if errors:
        raise RuntimeError(f"{len(errors)} delivery failures, first: {errors[0]}")
    return len(records)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, help="Criteo-format .tsv.gz")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--anchor-date", default=os.getenv("CRITEO_ANCHOR_DATE", "2025-01-01"))
    ap.add_argument("--bootstrap", help="Kafka bootstrap servers, e.g. localhost:9092")
    ap.add_argument("--topic", default=None, help="defaults to the contract's topic")
    ap.add_argument("--to-files", help="write Kafka-shaped Parquet files here instead of sending")
    ap.add_argument("--files", type=int, default=5)
    ap.add_argument("--rate", type=float, default=0.0, help="events per second (0 = as fast as possible)")
    ap.add_argument("--fault-rate", type=float, default=0.0)
    ap.add_argument("--dup-rate", type=float, default=0.0)
    ap.add_argument("--late-rate", type=float, default=0.0)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()

    anchor = datetime.strptime(a.anchor_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    topic = a.topic or load_contract().topic
    records, stats = build_stream(read_rows(a.src, a.limit), anchor, a.fault_rate, a.dup_rate, a.late_rate, a.seed)
    if a.to_files:
        paths = write_kafka_files(records, a.to_files, a.files, topic)
        print(f"wrote {len(records):,} records to {len(paths)} files in {a.to_files}")
    elif a.bootstrap:
        send_to_kafka(records, a.bootstrap, topic, a.rate)
        print(f"sent {len(records):,} records to {topic} on {a.bootstrap}")
    else:
        ap.error("pass --bootstrap or --to-files")
    print(json.dumps(stats))


if __name__ == "__main__":
    main()
