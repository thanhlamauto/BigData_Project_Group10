# StreamRAG — Real-Time Retrieval-Augmented Generation over Continuous Data Streams

Big Data project (Group 10). Kappa architecture:
**Producer → Kafka → PySpark Structured Streaming → MinIO (Parquet) + Kafka cleaned topic → Embedding/Indexer → OpenSearch → Retrieval → LLM → API**.

> Trạng thái: **Milestone 1 đang triển khai.** Hiện chạy được: hạ tầng nền (Kafka, MinIO, OpenSearch). Producer, Spark job, indexer, API đang được các owner phát triển.

## Chạy hạ tầng local

Yêu cầu: Docker (Compose v2), `make`, Docker được cấp ≥ 2 GB RAM cho hạ tầng nền (≥ 6 GB khi chạy full stack).

Hạ tầng, CI config và tài liệu kiến trúc nằm trong folder của TV1: [`members/TV1-Pham-Chi-Bang-lead-architecture-devops/`](members/TV1-Pham-Chi-Bang-lead-architecture-devops/).

```bash
cd members/TV1-Pham-Chi-Bang-lead-architecture-devops
cp .env.example .env
make up      # Kafka :9092 · MinIO :9000 (console :9001) · OpenSearch :9200; tạo bucket streamrag-lake
make ps
make down    # giữ dữ liệu; `make reset` mới xóa volumes
```

`make help` liệt kê mọi lệnh. Các lệnh `init-topics`, `init-index`, `seed`, `simulate`, `smoke` hiện báo *not implemented* cho đến khi owner của chúng merge code. Vận hành và lỗi thường gặp: [runbook](members/TV1-Pham-Chi-Bang-lead-architecture-devops/docs/runbook.md). Kiến trúc: [architecture](members/TV1-Pham-Chi-Bang-lead-architecture-devops/docs/architecture.md).

## Bắt đầu từ đâu

1. Đọc kế hoạch Milestone 1: [StreamRAG_Milestone_1_Plan.md](StreamRAG_Milestone_1_Plan.md)
2. Xem bảng phân công: [members/README.md](members/README.md)
3. Mở folder của bạn và làm theo chỉ dẫn:
   - [TV1 Pham Chi Bang — Lead / Architecture / DevOps](members/TV1-Pham-Chi-Bang-lead-architecture-devops/README.md)
   - [TV2 Nguyen Thanh Lam — Data & Kafka ingestion](members/TV2-Nguyen-Thanh-Lam-data-kafka-ingestion/README.md)
   - [TV3 Nguyen Phu An — Spark & Data Lake](members/TV3-Nguyen-Phu-An-spark-datalake/README.md)
   - [TV4 Kieu Son Tung — OpenSearch & Indexer](members/TV4-Kieu-Son-Tung-opensearch-indexer/README.md)
   - [TV5 Nguyen Ngoc Minh — RAG API / LLM / Tests](members/TV5-Nguyen-Ngoc-Minh-rag-api-llm-tests/README.md)

## Các mốc của Milestone 1

| Mốc | Ngày | Mục tiêu |
|---|---|---|
| M1.1 — Nền móng | 26–28/10 | Contract, fixtures, skeleton, CI, Compose tối thiểu |
| M1.2 — Module chạy độc lập | 29/10–01/11 | Mỗi module chạy với fixture/mock, có test |
| M1.3 — Tích hợp live | 02–04/11 | Event mới chảy end-to-end, update version |
| M1.4 — Hardening & release | 05–08/11 | Edge case, restart, smoke test, evidence, tag `v0.1.0-m1` |

## Quy tắc

Không push thẳng vào `main` — làm trên branch `feat/m1-xx-...`, mở PR, cần 1 reviewer. Chi tiết ở mục 10 của plan.
