# Import NiFi flow (cho video đồ án)

Pipeline `docker compose` đã chạy ETL tự động bằng `nifi/etl.py` (cùng hợp đồng: `vitals.raw` → validate → `vitals.validated`).

Để demo **Apache NiFi UI**:

1. Mở http://localhost:8081/nifi
2. Đăng nhập `admin` / `adminadminadmin` (nếu NiFi hiện form).
3. Kéo các processor theo `flow.json`:
   - ConsumeKafka_2_6 — bootstrap `kafka:9092`, topic `vitals.raw`
   - ValidateRecord / EvaluateJsonPath — đủ field vitals
   - RouteOnAttribute — bản ghi hợp lệ
   - PublishKafka_2_6 — topic `vitals.validated`
4. Start process group.

Nếu vừa chạy NiFi UI vừa `nifi-etl`, hai consumer group khác nhau nên **không tranh offset**. Chỉ bật flow NiFi khi muốn quay UI; pipeline demo không phụ thuộc click.
