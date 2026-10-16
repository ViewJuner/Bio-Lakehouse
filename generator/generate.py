"""Module 1 — wearable data generator. Publishes JSON vitals to Kafka vitals.raw."""
import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("TOPIC", "vitals.raw")
INTERVAL = float(os.environ.get("INTERVAL_SEC", "1"))

PATIENTS = [
    {"patient_id": "P001", "device_id": "W001", "hr": 74, "spo2": 97, "sbp": 138, "dbp": 86, "temp": 36.6},
    {"patient_id": "P002", "device_id": "W002", "hr": 80, "spo2": 96, "sbp": 128, "dbp": 78, "temp": 36.7},
    {"patient_id": "P003", "device_id": "W003", "hr": 68, "spo2": 99, "sbp": 118, "dbp": 76, "temp": 36.5},
    {"patient_id": "P004", "device_id": "W004", "hr": 88, "spo2": 94, "sbp": 142, "dbp": 88, "temp": 36.8},
    {"patient_id": "P005", "device_id": "W005", "hr": 72, "spo2": 98, "sbp": 122, "dbp": 80, "temp": 36.6},
]


def connect():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda v: v.encode("utf-8"),
            )
            print(f"generator connected to {BOOTSTRAP}", flush=True)
            return producer
        except NoBrokersAvailable:
            print("waiting for kafka...", flush=True)
            time.sleep(3)


def sample(profile, tick):
    # P005 spikes heart rate every ~40 seconds so the demo always shows an alert.
    hr = profile["hr"] + random.randint(-4, 4)
    if profile["patient_id"] == "P005" and (tick % 40) < 8:
        hr = random.randint(128, 148)
    rec = {
        "patient_id": profile["patient_id"],
        "device_id": profile["device_id"],
        "ts": datetime.now(timezone.utc).isoformat(),
        "heart_rate": hr,
        "spo2": max(80, min(100, profile["spo2"] + random.randint(-1, 1))),
        "systolic_bp": profile["sbp"] + random.randint(-5, 5),
        "diastolic_bp": profile["dbp"] + random.randint(-4, 4),
        "temperature": round(profile["temp"] + random.uniform(-0.2, 0.2), 1),
    }
    # ~5% dirty records so Silver cleaning is visible in MinIO.
    roll = random.random()
    if roll < 0.03:
        rec.pop("spo2")
    elif roll < 0.05:
        rec["heart_rate"] = 9
    return rec


def main():
    producer = connect()
    tick = 0
    while True:
        for profile in PATIENTS:
            rec = sample(profile, tick)
            producer.send(TOPIC, key=rec["patient_id"], value=rec)
        producer.flush()
        tick += 1
        if tick % 10 == 0:
            print(f"generator tick={tick} topic={TOPIC}", flush=True)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
