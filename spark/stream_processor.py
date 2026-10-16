"""Module 3/4/5 — Spark Structured Streaming micro-batch.

readStream (Kafka vitals.validated)
  foreachBatch:
    Bronze append → Silver clean/dedupe → Gold 1-min agg + risk_features
    HBase patient_latest + MySQL mirrors
"""
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import pymysql
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError

import hbase_sink
from iceberg_io import Lakehouse

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
IN_TOPIC = os.environ.get("IN_TOPIC", "vitals.validated")
MYSQL = {
    "host": os.environ.get("MYSQL_HOST", "mysql"),
    "user": os.environ.get("MYSQL_USER", "bio"),
    "password": os.environ.get("MYSQL_PASSWORD", "bio"),
    "database": os.environ.get("MYSQL_DB", "biolake"),
    "cursorclass": pymysql.cursors.DictCursor,
}

HR_MIN, HR_MAX = 30, 220
SPO2_MIN, SPO2_MAX = 50, 100


def mysql_conn():
    while True:
        try:
            conn = pymysql.connect(**MYSQL)
            print("spark connected mysql", flush=True)
            return conn
        except Exception as exc:
            print(f"waiting mysql: {exc}", flush=True)
            time.sleep(3)


def kafka_consumer():
    while True:
        try:
            c = KafkaConsumer(
                IN_TOPIC,
                bootstrap_servers=BOOTSTRAP,
                group_id="spark-stream",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                consumer_timeout_ms=2000,
                value_deserializer=lambda b: b.decode("utf-8") if b else "",
            )
            print(f"spark readStream {IN_TOPIC}", flush=True)
            return c
        except (NoBrokersAvailable, KafkaError) as exc:
            print(f"waiting kafka: {exc}", flush=True)
            time.sleep(3)


def parse_ts(value):
    if not value:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc)


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def cleanse(rec):
    hr = to_float(rec.get("heart_rate"))
    spo2 = to_float(rec.get("spo2"))
    sbp = to_float(rec.get("systolic_bp"))
    dbp = to_float(rec.get("diastolic_bp"))
    temp = to_float(rec.get("temperature"))
    if hr is None or not HR_MIN <= hr <= HR_MAX:
        return None
    if spo2 is None or not SPO2_MIN <= spo2 <= SPO2_MAX:
        return None
    if sbp is None or not 70 <= sbp <= 230:
        return None
    if dbp is None or not 40 <= dbp <= 140:
        return None
    if temp is None or not 34 <= temp <= 42:
        return None
    return {
        "patient_id": rec.get("patient_id"),
        "device_id": rec.get("device_id"),
        "ts": rec.get("ts"),
        "heart_rate": hr,
        "spo2": spo2,
        "systolic_bp": sbp,
        "diastolic_bp": dbp,
        "temperature": temp,
    }


def risk_score(rec):
    score = 0.0
    if rec["heart_rate"] > 100:
        score += 20
    if rec["heart_rate"] > 120:
        score += 20
    if rec["spo2"] < 95:
        score += 20
    if rec["spo2"] < 92:
        score += 20
    if rec["systolic_bp"] > 140:
        score += 15
    if rec["systolic_bp"] > 180:
        score += 15
    return min(100.0, score)


def minute_window(ts: datetime):
    start = ts.replace(second=0, microsecond=0)
    return start, start + timedelta(minutes=1)


def upsert_latest(conn, rec):
    sql = """
    INSERT INTO patient_latest
      (patient_id, device_id, ts, heart_rate, spo2, systolic_bp, diastolic_bp, temperature, risk_score)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      device_id=VALUES(device_id), ts=VALUES(ts), heart_rate=VALUES(heart_rate),
      spo2=VALUES(spo2), systolic_bp=VALUES(systolic_bp), diastolic_bp=VALUES(diastolic_bp),
      temperature=VALUES(temperature), risk_score=VALUES(risk_score)
    """
    ts = parse_ts(rec["ts"]).replace(tzinfo=None)
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                rec["patient_id"],
                rec.get("device_id"),
                ts,
                rec["heart_rate"],
                rec["spo2"],
                rec["systolic_bp"],
                rec["diastolic_bp"],
                rec["temperature"],
                rec.get("risk_score", 0),
            ),
        )
    conn.commit()


def upsert_gold(conn, agg):
    sql = """
    INSERT INTO vitals_agg
      (patient_id, window_start, window_end, avg_hr, max_hr, min_spo2, avg_sbp, sample_count)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      window_end=VALUES(window_end), avg_hr=VALUES(avg_hr), max_hr=VALUES(max_hr),
      min_spo2=VALUES(min_spo2), avg_sbp=VALUES(avg_sbp), sample_count=VALUES(sample_count)
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                agg["patient_id"],
                agg["window_start"],
                agg["window_end"],
                agg["avg_hr"],
                agg["max_hr"],
                agg["min_spo2"],
                agg["avg_sbp"],
                int(agg["sample_count"]),
            ),
        )
    conn.commit()


def foreach_batch(raw_rows, lake, conn, hbase_ok, windows):
    bronze = []
    for rec in raw_rows:
        bronze.append(
            {
                "patient_id": str(rec.get("patient_id") or ""),
                "device_id": rec.get("device_id"),
                "ts": rec.get("ts"),
                "heart_rate": to_float(rec.get("heart_rate")),
                "spo2": to_float(rec.get("spo2")),
                "systolic_bp": to_float(rec.get("systolic_bp")),
                "diastolic_bp": to_float(rec.get("diastolic_bp")),
                "temperature": to_float(rec.get("temperature")),
                "risk_score": None,
            }
        )
    lake.append("bronze.vitals_raw", bronze, "bronze/vitals_raw")

    seen = set()
    silver = []
    for rec in raw_rows:
        cleaned = cleanse(rec)
        if not cleaned or not cleaned["patient_id"]:
            continue
        key = (cleaned["patient_id"], cleaned["ts"])
        if key in seen:
            continue
        seen.add(key)
        cleaned["risk_score"] = risk_score(cleaned)
        silver.append(cleaned)

    lake.append("silver.vitals_clean", silver, "silver/vitals_clean")

    risk_rows = []
    for rec in silver:
        upsert_latest(conn, rec)
        if hbase_ok:
            hbase_sink.upsert(rec)
        ts = parse_ts(rec["ts"])
        start, end = minute_window(ts)
        wkey = (rec["patient_id"], start)
        bucket = windows[wkey]
        bucket["patient_id"] = rec["patient_id"]
        bucket["window_start"] = start.replace(tzinfo=None)
        bucket["window_end"] = end.replace(tzinfo=None)
        bucket["hrs"].append(rec["heart_rate"])
        bucket["spo2s"].append(rec["spo2"])
        bucket["sbps"].append(rec["systolic_bp"])
        risk_rows.append(
            {
                "patient_id": rec["patient_id"],
                "ts": rec["ts"],
                "risk_score": rec["risk_score"],
                "heart_rate": rec["heart_rate"],
                "spo2": rec["spo2"],
            }
        )

    gold = []
    for bucket in windows.values():
        hrs = bucket["hrs"]
        gold.append(
            {
                "patient_id": bucket["patient_id"],
                "window_start": bucket["window_start"].isoformat(),
                "window_end": bucket["window_end"].isoformat(),
                "avg_hr": sum(hrs) / len(hrs),
                "max_hr": max(hrs),
                "min_spo2": min(bucket["spo2s"]),
                "avg_sbp": sum(bucket["sbps"]) / len(bucket["sbps"]),
                "sample_count": float(len(hrs)),
            }
        )
        upsert_gold(conn, {**gold[-1], "window_start": bucket["window_start"], "window_end": bucket["window_end"]})

    lake.append("gold.vitals_agg", gold, "gold/vitals_agg")
    lake.append("gold.risk_features", risk_rows, "gold/risk_features")
    return len(bronze), len(silver)


def main():
    lake = Lakehouse()
    conn = mysql_conn()
    hbase_ok = hbase_sink.wait_and_create()
    consumer = kafka_consumer()
    windows = defaultdict(lambda: {"hrs": [], "spo2s": [], "sbps": []})
    print("spark structured streaming (micro-batch 2s) started", flush=True)
    while True:
        raw = []
        for msg in consumer:
            try:
                raw.append(json.loads(msg.value))
            except json.JSONDecodeError:
                continue
        if raw:
            b, s = foreach_batch(raw, lake, conn, hbase_ok, windows)
            print(f"batch bronze={b} silver={s}", flush=True)
        # Keep only recent windows so memory stays bounded.
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=15)
        stale = []
        for key in list(windows.keys()):
            start = key[1]
            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
            if start < cutoff:
                stale.append(key)
        for key in stale:
            del windows[key]


if __name__ == "__main__":
    main()
