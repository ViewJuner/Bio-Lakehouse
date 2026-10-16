# Bio Lakehouse

Nền tảng demo **Big Data Lakehouse** xử lý dữ liệu sinh học thời gian thực để giám sát và cảnh báo nguy cơ tim mạch.

Simulated wearables stream vitals through Kafka → NiFi ETL → Spark micro-batch → Iceberg Bronze/Silver/Gold on MinIO, with latest snapshots in HBase and a Vietnamese dashboard.

## Yêu cầu

- Docker Desktop (Windows) — `docker` must be on PATH after install/restart
- ~8 GB RAM cho toàn bộ stack
- Git tùy chọn (máy này có thể chưa có `git` trong PATH; không bắt buộc để chạy demo)

## Chạy demo

```powershell
cd C:\Users\DELL\Desktop\bio-lakehouse
.\scripts\demo.ps1
```

Hoặc:

```powershell
cd C:\Users\DELL\Desktop\bio-lakehouse
docker compose up -d --build
```

Mở dashboard: [http://localhost:8080](http://localhost:8080)

Đợi khoảng 1–2 phút lần đầu (kéo image + tạo topic/bảng).

## Cổng dịch vụ

| Dịch vụ | URL / cổng |
|---|---|
| Dashboard | http://localhost:8080 |
| NiFi UI | http://localhost:8081/nifi |
| MinIO console | http://localhost:9001 (`minioadmin` / `minioadmin`) |
| HBase Master | http://localhost:16010 |
| MySQL | localhost:3306 (`bio` / `bio`) |
| Kafka (host) | localhost:9094 |

NiFi đăng nhập (nếu hiện form): `admin` / `adminadminadmin`

## Luồng (B1–B8)

1. `generator` gửi JSON vitals → Kafka `vitals.raw`
2. `nifi-etl` (và NiFi UI) kiểm tra schema → `vitals.validated`
3. `spark` ghi **Bronze** (raw)
4. Làm sạch → **Silver**
5. Cửa sổ 1 phút + điểm rủi ro → **Gold**
6. Snapshot mới nhất → **HBase** `patient_latest` (và MySQL)
7. `rules` áp ngưỡng → Kafka `alerts` + MySQL
8. Dashboard hiển thị biểu đồ + banner cảnh báo

## Thay đổi đầu tiên (dễ nhất)

Sửa ngưỡng trong [`rules/thresholds.yaml`](rules/thresholds.yaml) rồi restart `rules`:

```powershell
docker compose restart rules
```

Ví dụ hạ `heart_rate_high` xuống `90` để cảnh báo xuất hiện ngay trên dashboard.

Hoặc sửa trên trang **Ngưỡng cảnh báo** của website (ghi vào file YAML dùng chung).

## Kiểm thử nhanh

```powershell
.\scripts\test-pipeline.ps1
```

## Cấu trúc thư mục

- `docs/thiet-ke.md` — tài liệu thiết kế
- `mysql/init.sql` — schema MySQL
- `generator/` — sinh dữ liệu wearable
- `nifi/` — ETL + flow import cho NiFi
- `spark/` — Bronze → Silver → Gold + Iceberg/MinIO + HBase
- `rules/` — rule engine
- `dashboard/` — website FastAPI
- `hbase/init.txt` — tạo bảng HBase
- `scripts/demo.ps1` — chạy đồ án

## Tài khoản MySQL

- Database: `biolake`
- User: `bio` / `bio`
- Root: `root` / `root`
