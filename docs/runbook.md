# Runbook — vận hành stack local

> Owner: TV1 (Pham Chi Bang). Bản nháp M1.1 — chỉ phần hạ tầng (Kafka, MinIO, OpenSearch). Các owner module bổ sung mục của mình (Spark: TV3, indexer/reindex: TV4, API/LLM: TV5).

## 1. Yêu cầu

- Docker Desktop (hoặc Docker Engine) với **Compose v2**, và `make`.
- RAM cấp cho Docker: hạ tầng nền dùng khoảng **1,5 GB** (đo 25/10/2026: Kafka ~450 MiB, OpenSearch ~910 MiB, MinIO ~95 MiB). Full stack (Spark, embedding model, API) cần nhiều hơn: nên cấp **≥ 6 GB** trong Docker Desktop → Settings → Resources.
- Port trống: `9092`, `9000`, `9001`, `9200` (đổi được trong `.env`: `*_HOST_PORT`).

## 2. Start / stop

```bash
cp .env.example .env     # lần đầu
make up                  # khởi động, chờ healthy, tạo bucket streamrag-lake
make ps                  # trạng thái
make logs SERVICE=kafka  # xem log 1 service (bỏ SERVICE để xem tất cả)
make down                # dừng, GIỮ dữ liệu (volumes)
make reset               # dừng và XÓA toàn bộ dữ liệu — hỏi xác nhận
```

## 3. Kiểm tra nhanh từng service

| Service | Lệnh kiểm tra | Kết quả mong đợi |
|---|---|---|
| Kafka | `docker exec streamrag-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list` | Danh sách topic (ban đầu chỉ có `__consumer_offsets`) |
| MinIO | Mở http://localhost:9001 (user/pass trong `.env`) | Thấy bucket `streamrag-lake` |
| OpenSearch | `curl -s localhost:9200/_cluster/health` | `"status":"green"` hoặc `"yellow"` |
| OpenSearch kNN | `curl -s 'localhost:9200/_cat/plugins?h=component' \| grep knn` | `opensearch-knn` |

## 4. Kết nối từ code

| Chạy ở đâu | Kafka | MinIO (S3) | OpenSearch |
|---|---|---|---|
| Trên host (script, test, notebook) | `localhost:9092` | `http://localhost:9000` | `http://localhost:9200` |
| Trong container Compose | `kafka:29092` | `http://minio:9000` | `http://opensearch:9200` |

Kafka có 2 listener; dùng sai địa chỉ (ví dụ `localhost:9092` từ trong container) sẽ kết nối được bootstrap nhưng fail khi produce/consume.

**Topic không tự tạo** (`auto.create.topics.enable=false`): produce vào topic chưa có sẽ báo `UNKNOWN_TOPIC_OR_PARTITION`. Tạo topic bằng `make init-topics` (owner: TV2).

## 5. Lỗi thường gặp

| Triệu chứng | Nguyên nhân hay gặp | Cách xử lý |
|---|---|---|
| `Cannot connect to the Docker daemon` | Docker Desktop chưa chạy | Mở Docker Desktop, đợi biểu tượng báo running |
| `make up` báo `Missing .env` | Chưa tạo `.env` | `cp .env.example .env` |
| `port is already allocated` | Port đang bị chiếm | Đổi `*_HOST_PORT` trong `.env` hoặc tắt tiến trình đang dùng port |
| OpenSearch restart liên tục / exit 137 | Thiếu RAM | Tăng RAM cho Docker; giữ `OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m` |
| OpenSearch log `max virtual memory areas vm.max_map_count [65530] is too low` | Linux host | `sudo sysctl -w vm.max_map_count=262144` |
| Kafka unhealthy ngay sau khi start | JVM khởi động chậm | Đợi; healthcheck cho phép ~2 phút. Xem `make logs SERVICE=kafka` |
| `UNKNOWN_TOPIC_OR_PARTITION` | Topic chưa được tạo | `make init-topics` |
| Spark báo `ClassNotFoundException: S3AFileSystem` | Thiếu JAR `hadoop-aws` | Xem README `services/stream_processor/` (TV3) |

## 6. Replay / rebuild *(TV4 bổ sung chi tiết)*

Kafka giữ event 7 ngày (`KAFKA_LOG_RETENTION_HOURS=168`). Để dựng lại OpenSearch: xóa index → `make init-index` → reset offset consumer group của indexer về `earliest` → chạy lại indexer. Lệnh cụ thể sẽ do TV4 viết khi indexer có mặt.

## 7. Ghi chú bảo mật

Cấu hình này **chỉ cho máy dev**: OpenSearch tắt security plugin (HTTP, không auth), MinIO dùng credential mặc định, Kafka PLAINTEXT. Không expose các port này ra Internet.
