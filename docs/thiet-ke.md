# Tài liệu thiết kế — Bio Lakehouse

Đề tài: **Xây dựng nền tảng Big Data Lakehouse xử lý và phân tích dữ liệu sinh học thời gian thực phục vụ giám sát và cảnh báo nguy cơ tim mạch.**

## 1. Mục tiêu

Thu thập tín hiệu từ thiết bị đeo (mô phỏng), xử lý stream, lưu medallion lakehouse, tra cứu vitals mới nhất, và cảnh báo khi vượt ngưỡng.

## 2. Kiến trúc

```
Wearable (generator)
    → Kafka (vitals.raw)
    → NiFi / nifi-etl (validate)
    → Kafka (vitals.validated)
    → Spark Structured Streaming (micro-batch)
         ├─ Iceberg Bronze / Silver / Gold  (MinIO S3)
         ├─ HBase patient_latest
         └─ MySQL mirrors (dashboard)
    → Rule engine → Kafka alerts + MySQL alerts
    → Dashboard website
```

Object storage **MinIO (S3-compatible)** được dùng thay HDFS trên máy Windows, đúng lựa chọn Object Storage (S3/ADLS/GCS) trong đề bài.

## 3. Module

| Module | Công nghệ | Vai trò |
|---|---|---|
| 1 Data Generator | Python | Giả lập wearable, 1 bản ghi/giây/bệnh nhân |
| 2 Ingestion | Kafka + NiFi | Bus sự kiện + ETL/validate |
| 3 Stream processing | Spark micro-batch (Python) | Làm sạch, cửa sổ, điểm rủi ro |
| 4 Lakehouse | Iceberg trên MinIO | Bronze → Silver → Gold |
| 5 Storage | HBase + MySQL | Snapshot realtime + metadata |
| 6 Analytics | SQL + Rule engine | Ngưỡng HR/SpO2/huyết áp |
| 7 Dashboard | FastAPI + HTML/JS | Giám sát + cảnh báo |

## 4. Mô hình dữ liệu vitals

```json
{
  "patient_id": "P001",
  "device_id": "W001",
  "ts": "2026-10-17T01:00:00+00:00",
  "heart_rate": 78,
  "spo2": 98,
  "systolic_bp": 120,
  "diastolic_bp": 80,
  "temperature": 36.7
}
```

Bản ghi bẩn (~5%) thiếu field hoặc giá trị ngoài range để Silver có việc làm sạch.

## 5. Lakehouse

- **Bronze** `bronze.vitals_raw`: append raw JSON, không sửa.
- **Silver** `silver.vitals_clean`: ép kiểu, loại HR ∉ [30,220], SpO2 ∉ [50,100], BP vô lý, dedupe theo (patient_id, ts).
- **Gold** `gold.vitals_agg`: thống kê cửa sổ 1 phút.
- **Gold** `gold.risk_features`: điểm rủi ro 0–100 phục vụ cảnh báo.

## 6. Rule engine (mặc định)

| Tín hiệu | Cảnh báo |
|---|---|
| `heart_rate` > 120 | Nhịp tim cao |
| `spo2` < 92 | Oxy máu thấp |
| `systolic_bp` > 180 | Huyết áp tâm thu cao |
| `diastolic_bp` > 110 | Huyết áp tâm trương cao |
| `temperature` > 38.5 | Sốt |

Bệnh nhân **P005** được generator làm tăng nhịp tim định kỳ để demo banner.

## 7. MySQL

- `users` — tài khoản demo
- `patients` / `devices` — danh mục
- `patient_latest` — mirror HBase cho SQL
- `vitals_agg` — mirror Gold
- `alerts` — lịch sử cảnh báo

## 8. Đánh giá hiệu năng (khung đồ án)

Đo trên máy local:

- **Độ trễ** dashboard: thời gian từ `ts` generator đến khi `/api/latest` thấy bản ghi (mục tiêu demo: vài giây).
- **Thông lượng**: 5 bệnh nhân × 1 msg/s ≈ 5 events/s (có thể tăng `PATIENT_COUNT` / giảm `INTERVAL_SEC`).
- **Mở rộng**: tăng partition Kafka, replica Spark, và scale MinIO; HBase giữ truy vấn điểm theo `patient_id`.

## 9. Phạm vi demo

Không triển khai Flume, Kudu, Cassandra, MongoDB (đề bài chỉ dùng khi thực sự cần). Không cài Hadoop native trên Windows.
