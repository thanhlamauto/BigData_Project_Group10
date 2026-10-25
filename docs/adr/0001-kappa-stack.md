# ADR 0001 — Kappa architecture và technology stack cho Milestone 1

- **Trạng thái:** Proposed (chờ cả nhóm chốt tại Gate A, 27/10/2026)
- **Người đề xuất:** TV1 (Pham Chi Bang)
- **Liên quan:** [architecture.md](../architecture.md), [Plan M1](../../StreamRAG_Milestone_1_Plan.md) mục 1.3, 2.3

## Context

- Đề tài: RAG trên **dữ liệu đến liên tục**; câu trả lời phải phản ánh sự kiện mới và bản sửa đổi mới nhất.
- Môn học yêu cầu Lambda/Kappa architecture, Spark, message queue, NoSQL, HDFS hoặc lưu trữ tương đương, Kubernetes/Cloud.
- Milestone 1 cần một **vertical slice chạy được** trong ~14 ngày, nhóm 5 người, máy dev phổ biến 8–16 GB RAM.
- Rủi ro lớn nhất: dự án biến thành "FastAPI + vector DB" và thiếu phần Big Data.

## Decision

1. **Kiến trúc Kappa**: Kafka là event log duy nhất; một job Spark Structured Streaming xử lý mọi sự kiện. Không có batch layer riêng ở M1. Tái tạo downstream bằng replay Kafka.
2. **Stack M1**:

| Vai trò | Lựa chọn | Version pin |
|---|---|---|
| Broker | Apache Kafka, KRaft | `apache/kafka:3.9.1` |
| Stream processing | PySpark Structured Streaming | TV3 pin (Spark 3.5.x + connector tương ứng) |
| Object storage | MinIO (S3 API), Parquet | `pgsty/minio:RELEASE.2026-08-04T00-00-00Z` |
| Bucket bootstrap | `mc` có sẵn trong cùng image MinIO | (như trên) |
| Search / NoSQL + vector | OpenSearch | `opensearchproject/opensearch:2.19.4` |
| Embedding | sentence-transformers (đề xuất all-MiniLM-L6-v2, dim 384) | TV4 pin |
| API | FastAPI | TV5 pin |
| LLM | Ollama hoặc API OpenAI-compatible; `mock` cho test | cấu hình qua env |
| Dev deployment | Docker Compose + Makefile | Compose v2 |
| CI | GitHub Actions: ruff + pytest unit | — |

3. **Delivery semantics**: at-least-once + idempotent upsert (`_id = document_id`, external version). Không tuyên bố exactly-once.

## Alternatives đã cân nhắc

| Phương án | Lý do không chọn ở M1 |
|---|---|
| **Lambda** (batch + speed layer) | Phải viết logic 2 lần và merge view; dữ liệu M1 nhỏ, không cần batch layer |
| **FastAPI + vector DB, nạp tài liệu trực tiếp** | Không có Kafka/Spark → không đáp ứng yêu cầu Big Data; không có replay/streaming thật |
| **Kafka Streams / Flink** thay Spark | Môn học tập trung Spark; nhóm quen PySpark hơn |
| **Vector DB chuyên dụng** (Qdrant, Milvus) | OpenSearch vừa là document NoSQL vừa có kNN → bớt 1 store |
| **HDFS** thay object storage | Nặng cho laptop ở M1; xem lại ở milestone sau cùng yêu cầu lưu trữ phân tán |
| **Image `minio/minio` chính thức** | Không còn pull được từ Docker Hub/quay.io (đã kiểm tra 25/10/2026) |
| **SeaweedFS / Garage** thay MinIO | Khả thi, cấu hình phức tạp hơn; giữ làm phương án dự phòng |

## Consequences

**Tích cực**
- Một đường xử lý duy nhất, dễ giải thích và demo; replay Kafka cho phép dựng lại Parquet/OpenSearch.
- Spark + Kafka là thành phần chính và chứng minh được trong demo.
- Các module tách nhau qua contract (schema, topic, index, API) → 5 người làm song song.

**Tiêu cực / rủi ro**
- At-least-once: có thể ghi lặp Parquet; serving dựa vào idempotent upsert.
- Single-node MinIO/Kafka/OpenSearch **không** chứng minh tính phân tán; Compose **không** thay thế K8s/Cloud.
- `pgsty/minio` là **fork cộng đồng**, không phải bản chính thức của MinIO Inc. Đã pin tag cụ thể; nếu giảng viên/nhóm không chấp nhận, chuyển sang SeaweedFS/Garage (S3 API) hoặc S3 cloud — chỉ đổi endpoint/credential cho Spark (S3A).
- Full stack + embedding model + LLM local nặng cho máy 8 GB → cần profile nhẹ hoặc dùng LLM API bên ngoài.

## Cần xác nhận với giảng viên

- M1 có bắt buộc deploy K8s/Cloud không?
- MinIO (single node) có được chấp nhận là "lưu trữ tương đương HDFS" ở M1 không?
- Deadline và rubric chính thức của M1.
