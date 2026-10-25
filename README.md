# StreamRAG — Real-Time Retrieval-Augmented Generation over Continuous Data Streams

Big Data project (Group 10). Kappa architecture:
**Producer → Kafka → PySpark Structured Streaming → MinIO (Parquet) + Kafka cleaned topic → Embedding/Indexer → OpenSearch → Retrieval → LLM → API**.

> Trạng thái: **Milestone 1 đang triển khai** — chưa có code chạy được. README này sẽ được TV1 cập nhật thành hướng dẫn setup khi các module được merge.

## Bắt đầu từ đâu

1. Đọc kế hoạch Milestone 1: [StreamRAG_Milestone_1_Plan.md](StreamRAG_Milestone_1_Plan.md)
2. Xem bảng phân công: [members/README.md](members/README.md)
3. Mở folder của bạn và làm theo chỉ dẫn:
   - [TV1 — Lead / Architecture / DevOps](members/TV1-lead-architecture-devops/README.md)
   - [TV2 — Data & Kafka ingestion](members/TV2-data-kafka-ingestion/README.md)
   - [TV3 — Spark & Data Lake](members/TV3-spark-datalake/README.md)
   - [TV4 — OpenSearch & Indexer](members/TV4-opensearch-indexer/README.md)
   - [TV5 — RAG API / LLM / Tests](members/TV5-rag-api-llm-tests/README.md)

## Các mốc của Milestone 1

| Mốc | Ngày | Mục tiêu |
|---|---|---|
| M1.1 — Nền móng | 26–28/10 | Contract, fixtures, skeleton, CI, Compose tối thiểu |
| M1.2 — Module chạy độc lập | 29/10–01/11 | Mỗi module chạy với fixture/mock, có test |
| M1.3 — Tích hợp live | 02–04/11 | Event mới chảy end-to-end, update version |
| M1.4 — Hardening & release | 05–08/11 | Edge case, restart, smoke test, evidence, tag `v0.1.0-m1` |

## Quy tắc

Không push thẳng vào `main` — làm trên branch `feat/m1-xx-...`, mở PR, cần 1 reviewer. Chi tiết ở mục 10 của plan.
