"""Module 7 — dashboard API + Vietnamese website."""
import os
from datetime import datetime
from pathlib import Path

import pymysql
import yaml
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

MYSQL = {
    "host": os.environ.get("MYSQL_HOST", "mysql"),
    "user": os.environ.get("MYSQL_USER", "bio"),
    "password": os.environ.get("MYSQL_PASSWORD", "bio"),
    "database": os.environ.get("MYSQL_DB", "biolake"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}
THRESHOLDS_PATH = os.environ.get("THRESHOLDS_PATH", "/app/thresholds.yaml")
STATIC = Path(__file__).parent / "static"

app = FastAPI(title="Bio Lakehouse")
app.mount("/static", StaticFiles(directory=STATIC), name="static")


def db():
    return pymysql.connect(**MYSQL)


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/health")
def health():
    try:
        conn = db()
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS ok")
            cur.fetchone()
        conn.close()
        mysql_ok = True
    except Exception:
        mysql_ok = False
    return {"ok": mysql_ok, "mysql": mysql_ok, "service": "dashboard"}


@app.get("/api/patients")
def patients():
    conn = db()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.*, d.device_id, d.model,
                   l.heart_rate, l.spo2, l.systolic_bp, l.diastolic_bp, l.temperature,
                   l.risk_score, l.ts AS last_ts
            FROM patients p
            LEFT JOIN devices d ON d.patient_id = p.patient_id
            LEFT JOIN patient_latest l ON l.patient_id = p.patient_id
            ORDER BY p.patient_id
            """
        )
        rows = cur.fetchall()
    conn.close()
    for row in rows:
        for key, val in list(row.items()):
            if isinstance(val, datetime):
                row[key] = val.isoformat()
    return rows


@app.get("/api/latest")
def latest():
    conn = db()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM patient_latest ORDER BY patient_id")
        rows = cur.fetchall()
    conn.close()
    for row in rows:
        for key, val in list(row.items()):
            if isinstance(val, datetime):
                row[key] = val.isoformat()
    return rows


@app.get("/api/gold")
def gold():
    conn = db()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM vitals_agg
            ORDER BY window_start DESC
            LIMIT 40
            """
        )
        rows = cur.fetchall()
    conn.close()
    for row in rows:
        for key, val in list(row.items()):
            if isinstance(val, datetime):
                row[key] = val.isoformat()
    return rows


@app.get("/api/alerts")
def alerts():
    conn = db()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM alerts ORDER BY id DESC LIMIT 50")
        rows = cur.fetchall()
    conn.close()
    for row in rows:
        for key, val in list(row.items()):
            if isinstance(val, datetime):
                row[key] = val.isoformat()
    return rows


@app.get("/api/thresholds")
def get_thresholds():
    with open(THRESHOLDS_PATH, encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


class Thresholds(BaseModel):
    heart_rate_high: float
    spo2_low: float
    systolic_bp_high: float
    diastolic_bp_high: float
    temperature_high: float
    cooldown_sec: float | None = 20


@app.put("/api/thresholds")
def put_thresholds(body: Thresholds):
    path = Path(THRESHOLDS_PATH)
    if not path.exists():
        raise HTTPException(500, "thresholds.yaml missing")
    data = body.model_dump()
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("# Updated from dashboard\n")
        yaml.safe_dump(data, fh, allow_unicode=True, sort_keys=False)
    return data
