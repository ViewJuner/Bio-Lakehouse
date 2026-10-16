"""Module 6 — rule engine: threshold checks on vitals.validated → Kafka alerts + MySQL."""
import json
import os
import time
from datetime import datetime, timezone

import pymysql
import yaml
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
IN_TOPIC = os.environ.get("IN_TOPIC", "vitals.validated")
ALERT_TOPIC = os.environ.get("ALERT_TOPIC", "alerts")
PATH = os.environ.get("THRESHOLDS_PATH", "/app/thresholds.yaml")
MYSQL = {
    "host": os.environ.get("MYSQL_HOST", "mysql"),
    "user": os.environ.get("MYSQL_USER", "bio"),
    "password": os.environ.get("MYSQL_PASSWORD", "bio"),
    "database": os.environ.get("MYSQL_DB", "biolake"),
}


def load_thresholds():
    with open(PATH, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return {
        "heart_rate_high": float(data.get("heart_rate_high", 120)),
        "spo2_low": float(data.get("spo2_low", 92)),
        "systolic_bp_high": float(data.get("systolic_bp_high", 180)),
        "diastolic_bp_high": float(data.get("diastolic_bp_high", 110)),
        "temperature_high": float(data.get("temperature_high", 38.5)),
        "cooldown_sec": float(data.get("cooldown_sec", 20)),
    }


def connect():
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
                group_id="rule-engine",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda b: b.decode("utf-8") if b else "",
            )
            conn = pymysql.connect(**MYSQL)
            print("rule-engine ready", flush=True)
            return consumer, producer, conn
        except (NoBrokersAvailable, KafkaError, Exception) as exc:
            print(f"waiting: {exc}", flush=True)
            time.sleep(3)


def evaluate(rec, th):
    alerts = []
    hr = float(rec.get("heart_rate") or 0)
    spo2 = float(rec.get("spo2") or 100)
    sbp = float(rec.get("systolic_bp") or 0)
    dbp = float(rec.get("diastolic_bp") or 0)
    temp = float(rec.get("temperature") or 36.5)
    if hr > th["heart_rate_high"]:
        alerts.append(("high", "heart_rate_high", f"Nhịp tim {hr:.0f} bpm vượt ngưỡng {th['heart_rate_high']:.0f}"))
    if spo2 < th["spo2_low"]:
        alerts.append(("critical", "spo2_low", f"SpO2 {spo2:.0f}% dưới ngưỡng {th['spo2_low']:.0f}%"))
    if sbp > th["systolic_bp_high"]:
        alerts.append(("high", "systolic_bp_high", f"Huyết áp tâm thu {sbp:.0f} vượt {th['systolic_bp_high']:.0f}"))
    if dbp > th["diastolic_bp_high"]:
        alerts.append(("high", "diastolic_bp_high", f"Huyết áp tâm trương {dbp:.0f} vượt {th['diastolic_bp_high']:.0f}"))
    if temp > th["temperature_high"]:
        alerts.append(("medium", "temperature_high", f"Nhiệt độ {temp:.1f}°C vượt {th['temperature_high']:.1f}"))
    return alerts


def persist(conn, rec, severity, rule_name, message):
    ts = rec.get("ts")
    try:
        ts_dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        ts_dt = datetime.utcnow()
    sql = """
    INSERT INTO alerts (patient_id, device_id, ts, severity, rule_name, message, heart_rate, spo2, systolic_bp)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                rec.get("patient_id"),
                rec.get("device_id"),
                ts_dt,
                severity,
                rule_name,
                message,
                rec.get("heart_rate"),
                rec.get("spo2"),
                rec.get("systolic_bp"),
            ),
        )
    conn.commit()


def main():
    consumer, producer, conn = connect()
    last_fire = {}
    last_mtime = 0
    th = load_thresholds()
    while True:
        for msg in consumer:
            try:
                mtime = os.path.getmtime(PATH)
                if mtime != last_mtime:
                    th = load_thresholds()
                    last_mtime = mtime
                    print(f"thresholds reloaded {th}", flush=True)
            except OSError:
                pass
            try:
                rec = json.loads(msg.value)
            except json.JSONDecodeError:
                continue
            now = time.time()
            for severity, rule_name, message in evaluate(rec, th):
                key = (rec.get("patient_id"), rule_name)
                if now - last_fire.get(key, 0) < th["cooldown_sec"]:
                    continue
                last_fire[key] = now
                alert = {
                    "patient_id": rec.get("patient_id"),
                    "device_id": rec.get("device_id"),
                    "ts": rec.get("ts") or datetime.now(timezone.utc).isoformat(),
                    "severity": severity,
                    "rule_name": rule_name,
                    "message": message,
                    "heart_rate": rec.get("heart_rate"),
                    "spo2": rec.get("spo2"),
                    "systolic_bp": rec.get("systolic_bp"),
                }
                producer.send(ALERT_TOPIC, key=str(alert["patient_id"]), value=alert)
                persist(conn, rec, severity, rule_name, message)
                print(f"ALERT {alert['patient_id']} {rule_name}: {message}", flush=True)
            producer.flush()


if __name__ == "__main__":
    main()
