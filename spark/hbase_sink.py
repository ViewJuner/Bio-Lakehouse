"""HBase REST sink for table patient_latest (column family v)."""
import base64
import os
import time

import requests

HBASE = os.environ.get("HBASE_REST", "http://hbase:8080").rstrip("/")
TABLE = "patient_latest"


def _b64(value: str) -> str:
    return base64.b64encode(str(value).encode("utf-8")).decode("ascii")


def wait_and_create(timeout=180):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            r = requests.get(f"{HBASE}/version", timeout=3)
            if r.status_code < 500:
                schema = {"name": TABLE, "ColumnSchema": [{"name": "v", "COMPRESSION": "NONE"}]}
                cr = requests.post(
                    f"{HBASE}/{TABLE}/schema",
                    json=schema,
                    headers={"Accept": "application/json", "Content-Type": "application/json"},
                    timeout=5,
                )
                print(f"hbase table ensure status={cr.status_code}", flush=True)
                return True
        except Exception as exc:
            last = exc
        time.sleep(4)
    print(f"hbase not ready: {last}", flush=True)
    return False


def upsert(rec: dict) -> bool:
    cells = []
    for col in ("device_id", "ts", "heart_rate", "spo2", "systolic_bp", "diastolic_bp", "temperature", "risk_score"):
        if rec.get(col) is None:
            continue
        cells.append({"column": _b64(f"v:{col}"), "$": _b64(rec[col])})
    body = {"Row": [{"key": _b64(rec["patient_id"]), "Cell": cells}]}
    try:
        r = requests.put(
            f"{HBASE}/{TABLE}/{rec['patient_id']}",
            json=body,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=5,
        )
        return r.status_code < 300
    except Exception as exc:
        print(f"hbase put failed: {exc}", flush=True)
        return False
