"""Iceberg lakehouse writers (MinIO S3). Falls back to JSONL/Parquet prefixes if catalog init fails."""
from __future__ import annotations

import io
import json
import os
import uuid
from datetime import datetime, timezone

from minio import Minio

BUCKET = os.environ.get("MINIO_BUCKET", "lakehouse")
ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
ACCESS = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
SECRET = os.environ.get("MINIO_SECRET_KEY", "minioadmin")


def minio_client() -> Minio:
    return Minio(ENDPOINT, access_key=ACCESS, secret_key=SECRET, secure=False)


class Lakehouse:
    def __init__(self):
        self.mc = minio_client()
        self.iceberg = False
        self.catalog = None
        self.tables = {}
        try:
            self._init_iceberg()
            self.iceberg = True
            print("lakehouse: iceberg catalog ready", flush=True)
        except Exception as exc:
            print(f"lakehouse: iceberg unavailable, using parquet/jsonl fallback ({exc})", flush=True)

    def _init_iceberg(self):
        from pyiceberg.catalog.sql import SqlCatalog
        from pyiceberg.schema import Schema
        from pyiceberg.types import DoubleType, NestedField, StringType

        warehouse = f"s3://{BUCKET}/warehouse"
        catalog = SqlCatalog(
            "biolake",
            **{
                "uri": "sqlite:////tmp/iceberg-catalog.db",
                "warehouse": warehouse,
                "s3.endpoint": f"http://{ENDPOINT}",
                "s3.access-key-id": ACCESS,
                "s3.secret-access-key": SECRET,
                "s3.path-style-access": "true",
            },
        )
        for ns in ("bronze", "silver", "gold"):
            try:
                catalog.create_namespace(ns)
            except Exception:
                pass
        vitals = Schema(
            NestedField(1, "patient_id", StringType(), required=True),
            NestedField(2, "device_id", StringType(), required=False),
            NestedField(3, "ts", StringType(), required=False),
            NestedField(4, "heart_rate", DoubleType(), required=False),
            NestedField(5, "spo2", DoubleType(), required=False),
            NestedField(6, "systolic_bp", DoubleType(), required=False),
            NestedField(7, "diastolic_bp", DoubleType(), required=False),
            NestedField(8, "temperature", DoubleType(), required=False),
            NestedField(9, "risk_score", DoubleType(), required=False),
        )
        self.catalog = catalog
        self.tables["bronze.vitals_raw"] = self._table("bronze", "vitals_raw", vitals)
        self.tables["silver.vitals_clean"] = self._table("silver", "vitals_clean", vitals)
        agg = Schema(
            NestedField(1, "patient_id", StringType(), required=True),
            NestedField(2, "window_start", StringType(), required=False),
            NestedField(3, "window_end", StringType(), required=False),
            NestedField(4, "avg_hr", DoubleType(), required=False),
            NestedField(5, "max_hr", DoubleType(), required=False),
            NestedField(6, "min_spo2", DoubleType(), required=False),
            NestedField(7, "avg_sbp", DoubleType(), required=False),
            NestedField(8, "sample_count", DoubleType(), required=False),
        )
        risk = Schema(
            NestedField(1, "patient_id", StringType(), required=True),
            NestedField(2, "ts", StringType(), required=False),
            NestedField(3, "risk_score", DoubleType(), required=False),
            NestedField(4, "heart_rate", DoubleType(), required=False),
            NestedField(5, "spo2", DoubleType(), required=False),
        )
        self.tables["gold.vitals_agg"] = self._table("gold", "vitals_agg", agg)
        self.tables["gold.risk_features"] = self._table("gold", "risk_features", risk)

    def _table(self, ns, name, schema):
        ident = f"{ns}.{name}"
        try:
            return self.catalog.create_table(ident, schema=schema)
        except Exception:
            return self.catalog.load_table(ident)


    def append(self, table_name: str, rows: list[dict], prefix: str):
        if not rows:
            return

        if self.iceberg and table_name in self.tables:
            try:
                import pyarrow as pa

                table = self.tables[table_name]
                schema = table.schema().as_arrow()

                valid_rows = [
                    row for row in rows
                    if row.get("patient_id") is not None
                    and str(row.get("patient_id")).strip() != ""
                ]

                if not valid_rows:
                    print(
                        f"iceberg append skipped: no valid patient_id for {table_name}",
                        flush=True,
                    )
                    return

                arrow_table = pa.Table.from_pylist(
                    valid_rows,
                    schema=schema,
                )
                table.append(arrow_table)
                return

            except Exception as exc:
                print(
                    f"iceberg append failed, parquet fallback: {exc}",
                    flush=True,
                )

        self._put_jsonl(prefix, rows)

    def _put_jsonl(self, prefix: str, rows: list[dict]):
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        key = f"{prefix}/dt={day}/part-{uuid.uuid4().hex}.jsonl"
        body = "\n".join(json.dumps(r, default=str) for r in rows).encode("utf-8")
        self.mc.put_object(BUCKET, key, io.BytesIO(body), length=len(body), content_type="application/jsonl")
