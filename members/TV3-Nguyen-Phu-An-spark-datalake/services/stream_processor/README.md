# Stream processor (TV3 — Nguyen Phu An)

PySpark Structured Streaming: đọc `streamrag.raw.v1` → validate/normalize → ghi **Bronze** + **Silver** Parquet lên MinIO (S3A).
Đã nối thêm cleaned Kafka sink khi có biến `CLEANED_TOPIC` (stack tích hợp đặt
`streamrag.cleaned.v1`). Trong mỗi micro-batch, cleaned output dedup theo `event_id`;
Silver vẫn giữ các replay. Global dedup/watermark chưa được triển khai. Xem
[stack tích hợp](../../../../services/indexer/README.md) và `docs/evidence/` ở repo root.

## Version đã pin (kiểm tra thực tế trong image, 25/10/2026)

| Thành phần | Version | Ghi chú |
|---|---|---|
| Base image | `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu` | Python 3.10, OpenJDK 17 |
| Spark / Scala | 3.5.9 / 2.12 | |
| Hadoop (có sẵn trong image) | 3.3.4 | `hadoop-client-api/runtime-3.3.4.jar` |
| `org.apache.spark:spark-sql-kafka-0-10_2.12` | 3.5.9 | kéo theo `kafka-clients` 3.4.1, `commons-pool2` 2.11.1 |
| `org.apache.hadoop:hadoop-aws` | 3.3.4 | **phải trùng** bản Hadoop của image |
| `com.amazonaws:aws-java-sdk-bundle` | 1.12.262 | bản mà `hadoop-aws` 3.3.4 khai báo |
| `pyspark` (chỉ để test local) | 3.5.9 | `requirements.txt` |

Dockerfile resolve các JAR trên **lúc build** và chỉ copy những JAR Spark chưa có, nên job chạy offline. Đổi version Spark thì phải đổi đồng bộ cả 3 dòng trên và `requirements.txt`.

## Chạy

Tất cả lệnh chạy trong folder TV3 và dùng Docker, không cần cài Java hay Spark trên máy.

```bash
make test          # unit test transforms trên local SparkSession
```

Spike Kafka → MinIO (cần hạ tầng của TV1):

```bash
cd ../TV1-Pham-Chi-Bang-lead-architecture-devops && cp .env.example .env && make up && cd -
make topic         # tạm thời, cho tới khi TV2 có script tạo topic
make produce-dev   # bắn tests/fixtures/raw_events_dev.jsonl, key = document_id
make run-once      # xử lý hết message mới rồi thoát (trigger availableNow)
make inspect       # đọc lại Bronze/Silver bằng spark.read.parquet
make run           # chạy liên tục, micro-batch 5 giây
```

Chạy ngoài Makefile: `spark-submit --master 'local[*]' --driver-memory 1g job.py` với `PYTHONPATH` trỏ vào folder `services/` và các JAR ở bảng trên.

## Cấu hình (biến môi trường, khớp `.env.example` của TV1)

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Trong Compose dùng `kafka:29092` |
| `RAW_TOPIC` | `streamrag.raw.v1` | |
| `CLEANED_TOPIC` | không đặt | Khi đặt, phát valid JSON UTF-8 (UTC Z), key=document_id sau khi ghi lake |
| `LAKE_ROOT` | `s3a://$LAKE_BUCKET` | Đặt path local (ví dụ `/tmp/lake`) để chạy không cần MinIO |
| `CHECKPOINT_ROOT` | `$LAKE_ROOT/checkpoints` | Không commit checkpoint |
| `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` | | Chỉ dùng khi path là `s3a://` |
| `STARTING_OFFSETS` | `earliest` | Chỉ áp dụng lần chạy đầu; sau đó Spark đọc offset từ checkpoint |
| `TRIGGER` | `5 seconds` | Hoặc `availableNow` |
| `SHUFFLE_PARTITIONS` | `4` | Giữ nhỏ cho laptop |

## Layout data lake

| Path | Nội dung | Partition |
|---|---|---|
| `bronze/events/` | Raw value + `kafka_key`, `topic`, `partition`, `offset`, `kafka_timestamp` | `ingest_date` = ngày UTC của Kafka timestamp |
| `bronze/rejected/` | Bản ghi lỗi: raw value, `reject_reason`, toạ độ Kafka, `processed_at` | như Bronze |
| `silver/events/` | Đủ field contract đã chuẩn hoá + `processed_at` | `ingest_date` = ngày UTC của `ingested_at` |
| `checkpoints/raw_to_lake/` | Offset và state của query | |

## Validate và normalize (`transforms.py`)

- `title`/`body`: gộp mọi khoảng trắng (cả tab, xuống dòng) thành một dấu cách, rồi trim. Các field chuỗi khác chỉ trim. `source_url` rỗng chuyển thành null.
- `event_time`/`ingested_at`: ISO 8601 **có** múi giờ (`Z` hoặc `+07:00`), phần lẻ giây tối đa 6 chữ số, quy về UTC. Chuỗi không có múi giờ bị loại vì không biết giờ nào.
- Bản ghi lỗi **không làm sập stream**. Mỗi bản ghi lỗi mang `reject_reason` là luật đầu tiên nó vi phạm:

| `reject_reason` | Khi nào |
|---|---|
| `empty_value` | Message rỗng |
| `malformed_json` | Không phải JSON, không phải object, hoặc không có field nào của contract |
| `missing_<field>` | Thiếu field bắt buộc hoặc giá trị là `null` |
| `invalid_<field>` | Có field nhưng sai kiểu JSON (ví dụ `"version": "two"`), `version < 1`, hoặc timestamp sai định dạng |
| `empty_<field>` | Field chuỗi chỉ có khoảng trắng (ví dụ `empty_body`) |
| `unsupported_schema_version` | `schema_version` khác 1 |
| `unsupported_operation` | `operation` khác `upsert` |

Mỗi micro-batch log một dòng, ví dụ:

```text
batch=0 input=9 valid=4 invalid=5 reasons={'missing_body': 1, 'empty_body': 1, 'unsupported_operation': 1, 'invalid_event_time': 1, 'malformed_json': 1} took_ms=2703
```

## Thiết kế sink và giới hạn

- **Một query, `foreachBatch`, một checkpoint.** Mỗi batch đọc Kafka một lần rồi ghi tất cả sink, nên đếm input/valid/invalid dễ. Đổi lại, nếu batch bị retry thì file có thể bị ghi lặp. Pipeline là **at-least-once**, không phải exactly-once. TV4 phải upsert idempotent theo `document_id` + `version`.
- **Chưa dedup** (task M1-10): `event_id` gửi lặp hiện vẫn vào Silver nhiều lần.
- Contract hiện là **v0** theo message mẫu mục 3.2 của plan. Khi TV2 merge `data/contracts/event.schema.json`, cần cập nhật `schema.py` và test.
- Spark tự ép số sang chuỗi cho field chuỗi (ví dụ `"event_id": 123` thành `"123"`), nên trường hợp này không bị loại.
- `local[*]` trong một container là chấp nhận được cho M1.

## Evidence spike (25/10/2026, khoảng 22:16 giờ Việt Nam)

Chạy với Kafka `apache/kafka:3.9.1` và MinIO trong Compose của TV1, ghi qua `s3a://streamrag-lake`:

1. Produce 9 bản ghi dev rồi `run-once`: `batch=0 input=9 valid=4 invalid=5`, đúng 5 lý do như dự kiến.
2. Produce thêm 1 event mới rồi chạy lại: `batch=1 input=1 valid=1`. Job tiếp tục từ checkpoint trên MinIO, không xử lý lại từ đầu.
3. `spark.read.parquet` đọc lại được Silver với đúng schema (timestamp UTC, partition `ingest_date`), và `bronze/rejected` giữ đúng partition/offset của từng bản ghi lỗi.
