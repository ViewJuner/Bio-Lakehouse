"""Module 2 — NiFi-style ETL: consume vitals.raw, validate JSON schema, publish vitals.validated.

Apache NiFi UI runs beside this service so the đồ án can import nifi/flow.json
(see HUONG-DAN-IMPORT.md). This process is the automated path for docker compose demo.
"""
import json
import os
import time

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
IN_TOPIC = os.environ.get("IN_TOPIC", "vitals.raw")
OUT_TOPIC = os.environ.get("OUT_TOPIC", "vitals.validated")
REQUIRED = {"patient_id", "device_id", "ts", "heart_rate", "spo2", "systolic_bp", "diastolic_bp", "temperature"}


def wait_clients():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda v: v.encode("utf-8"),
            )
            consumer = KafkaConsumer(
                IN_TOPIC,
                bootstrap_servers=BOOTSTRAP,
                group_id="nifi-etl",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda b: b.decode("utf-8") if b else "",
                key_deserializer=lambda b: b.decode("utf-8") if b else None,
            )
            print(f"nifi-etl connected {IN_TOPIC} -> {OUT_TOPIC}", flush=True)
            return consumer, producer
        except (NoBrokersAvailable, KafkaError) as exc:
            print(f"waiting for kafka: {exc}", flush=True)
            time.sleep(3)


def valid(rec):
    if not REQUIRED.issubset(rec.keys()):
        return False
    try:
        float(rec["heart_rate"])
        float(rec["spo2"])
        float(rec["systolic_bp"])
        float(rec["diastolic_bp"])
        float(rec["temperature"])
    except (TypeError, ValueError):
        return False
    return bool(rec.get("patient_id")) and bool(rec.get("device_id")) and bool(rec.get("ts"))


def main():
    consumer, producer = wait_clients()
    ok = drop = 0
    while True:
        for msg in consumer:
            try:
                rec = json.loads(msg.value)
            except json.JSONDecodeError:
                drop += 1
                continue
            if not valid(rec):
                drop += 1
                continue
            rec["_ingest"] = "nifi-etl"
            producer.send(OUT_TOPIC, key=rec["patient_id"], value=rec)
            ok += 1
            if (ok + drop) % 25 == 0:
                producer.flush()
                print(f"nifi-etl ok={ok} drop={drop}", flush=True)


if __name__ == "__main__":
    main()
