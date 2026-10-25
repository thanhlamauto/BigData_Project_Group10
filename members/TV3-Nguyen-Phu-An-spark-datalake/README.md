# TV3 — Spark Structured Streaming & Data Lake

> Người phụ trách: **Nguyen Phu An** (TV3) · Reviewer chéo của bạn: **TV2 (Nguyen Thanh Lam)** · Bạn review: PR của **TV2 (Nguyen Thanh Lam)** và PR hạ tầng của **TV1 (Pham Chi Bang)**
> Đọc trước: [plan](../../StreamRAG_Milestone_1_Plan.md) mục 1.3, 2.2, 3.2, **3.3**, 6, 11 (rủi ro Spark S3A) và [members/README.md](../README.md).

## Vai trò

Bạn giữ **lõi Big Data**: PySpark Structured Streaming đọc Kafka, kiểm tra/chuẩn hóa dữ liệu, ghi Parquet lên MinIO (Bronze/Silver) và đẩy bản sạch sang `streamrag.cleaned.v1` cho TV4. Đây là phần giảng viên chấm kỹ nhất — **không được thay Spark bằng script Python thường**.

## File bạn sở hữu

```text
services/stream_processor/
├── README.md               # cách chạy, config, JAR/version đã pin
├── Dockerfile
├── job.py                  # entrypoint spark-submit
├── schema.py               # StructType khớp data/contracts/event.schema.json
└── transforms.py           # hàm thuần (validate/normalize) để unit test
data/sample/cleaned_fixtures.jsonl   # output Spark mẫu cho TV4 — hết D5
tests/unit/test_transforms.py
```

## Làm song song thế nào

- **Không chờ Kafka của ai**: viết `transforms.py` và test bằng local SparkSession trên DataFrame dựng từ `fixtures.jsonl` (hoặc message mẫu mục 3.2 plan nếu TV2 chưa xong).
- **Không chờ Compose của TV1**: tự `docker run` 1 Kafka + 1 MinIO để spike; tự produce fixture vào Kafka bằng `kafka-console-producer`.
- Ghi Parquet ra **thư mục local** trước, đổi sang `s3a://` khi MinIO chạy — chỉ khác path + config.
- TV4 không chờ bạn: bạn giao `cleaned_fixtures.jsonl` (hết D5) để TV4 test, cleaned topic thật dùng ở M1.3.

---

## M1.1 — Nền móng (D1–D3, 26–28/10) · task M1-08

- [ ] `schema.py`: `StructType` theo contract (dùng message mẫu mục 3.2 plan, cập nhật khi TV2 merge schema sáng D2).
- [ ] `transforms.py` (hàm nhận/trả DataFrame, không phụ thuộc Kafka):
  - parse JSON, tách bản ghi lỗi/thiếu field bắt buộc;
  - trim + gộp khoảng trắng `title`/`body`, bỏ `body` rỗng;
  - parse `event_time`/`ingested_at` về timestamp UTC, thêm `processed_at`, `ingest_date`.
- [ ] Unit test transforms với local SparkSession.
- [ ] **Spike rủi ro số 1 (bắt đầu D3)**: job tối thiểu đọc Kafka → ghi Parquet. Pin version tương thích và ghi vào README module, ví dụ Spark 3.5.x (Scala 2.12):
  - `org.apache.spark:spark-sql-kafka-0-10_2.12:<đúng bản Spark>`
  - `org.apache.hadoop:hadoop-aws:<đúng bản Hadoop đi kèm Spark>` + `aws-java-sdk-bundle` tương ứng
  - **Kiểm tra lại version thực tế** trong image bạn dùng — đừng copy mù.

**Bàn giao cuối M1.1:** transforms + unit test merge; spike Kafka → Parquet chạy (local path cũng được).

---

## M1.2 — Module chạy độc lập (D4–D7, 29/10–01/11) · task M1-08, M1-09

- [ ] `readStream.format("kafka")` từ `streamrag.raw.v1`, `startingOffsets=earliest` cho lần đầu; `trigger(processingTime="5 seconds")`.
- [ ] Bản ghi lỗi → **không crash**, đếm và log mỗi micro-batch (input / valid / invalid) qua `foreachBatch` hoặc `query.lastProgress`; tùy chọn ghi `bronze/rejected/`.
- [ ] Config S3A cho MinIO: `fs.s3a.endpoint`, access/secret key từ env, `fs.s3a.path.style.access=true`, `fs.s3a.connection.ssl.enabled=false`.
- [ ] **Bronze**: raw value + Kafka metadata (topic, partition, offset, timestamp) → `s3a://<bucket>/bronze/events/`, partition `ingest_date`.
- [ ] **Silver**: bản đã validate/normalize → `s3a://<bucket>/silver/events/`, partition `ingest_date`.
- [ ] **Cleaned topic**: JSON bản Silver → `streamrag.cleaned.v1`, **key = `document_id`**, giữ đủ field contract + `processed_at`.
- [ ] Checkpoint riêng mỗi query trên MinIO (`s3a://<bucket>/checkpoints/<query>/`). Không commit checkpoint.
- [ ] Chọn 1 cách và ghi lý do trong README:
  1. `foreachBatch` ghi cả Parquet và Kafka (1 checkpoint) — đơn giản, retry batch có thể ghi lặp.
  2. Nhiều query, mỗi query 1 sink + 1 checkpoint.
  Cả hai đều **at-least-once** (không phải distributed transaction) → TV4 upsert idempotent theo `document_id` + `version`.
- [ ] **Hết D5**: commit `data/sample/cleaned_fixtures.jsonl` (output thật từ job) → báo TV4.
- [ ] Dockerfile + lệnh chạy → giao TV1 thêm vào Compose. Giữ RAM thấp: `spark.driver.memory` 1–2g, `spark.sql.shuffle.partitions` nhỏ (ví dụ 4); `local[*]` chấp nhận được ở M1 (ghi rõ).
- [ ] Chứng minh: đọc lại Silver bằng `spark.read.parquet(...)` hoặc MinIO console → chụp evidence.

**Bàn giao cuối M1.2 (Gate B):** seed từ Kafka → Silver Parquet + cleaned topic, chạy trong Compose.

---

## M1.3 — Tích hợp live (D8–D10, 02–04/11) · task M1-09

- [ ] Khi TV2 bắn live event / `inc-atlas-001` v2: xác nhận event có trong Silver và cleaned topic **không restart job**; log thời điểm xử lý.
- [ ] Cùng TV4 kiểm tra indexer nhận đúng format cleaned message thật (không chỉ fixture).

**Bàn giao cuối M1.3 (Gate C):** live event đi qua Spark trong vài micro-batch.

---

## M1.4 — Hardening & release (D11–D14, 05–08/11) · task M1-10, M1-17

- [ ] Dedup theo `event_id` có watermark `event_time` (`withWatermark("event_time", "10 minutes")` + `dropDuplicates(["event_id"])`, hoặc `dropDuplicatesWithinWatermark` nếu Spark ≥ 3.5). Ghi rõ **giới hạn**: duplicate tới sau watermark sẽ lọt → TV4 vẫn phải upsert idempotent.
- [ ] Late event (bộ edge case TV2): mô tả hành vi thực tế quan sát được.
- [ ] **Restart test**: chạy job, seed, kill container, gửi thêm event, chạy lại → tiếp tục từ checkpoint, không xử lý lại từ đầu, không mất event. Ghi số đếm trước/sau.
- [ ] Bắn bộ invalid/duplicate → job không sập; số invalid khớp kỳ vọng.
- [ ] Cross-review + clean clone (M1-17).

## Lưu ý

- Không gọi embedding model trong Spark UDF ở M1 (việc của TV4).

## Definition of Done của bạn

- [ ] Event mới vào raw topic → có trong Silver Parquet và cleaned topic, **không restart**.
- [ ] Message lỗi không làm sập stream, có counter/log.
- [ ] Restart tiếp tục từ checkpoint; test dedup đã định không nhân đôi.
- [ ] README module (version pin, lệnh chạy, giới hạn at-least-once/watermark) + unit test.
