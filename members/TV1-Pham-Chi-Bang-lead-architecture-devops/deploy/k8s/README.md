# Kubernetes / Cloud — Roadmap (chưa triển khai ở M1)

> Owner: TV1 (Pham Chi Bang). Thư mục này **chưa chứa manifest chạy được**. M1 chạy local bằng Docker Compose ([`infra/compose.yaml`](../../infra/compose.yaml)); Compose không thay thế yêu cầu K8s/Cloud của toàn đồ án.

## Mục tiêu milestone sau

| Thành phần | Local (M1) | Đề xuất trên K8s / Cloud |
|---|---|---|
| Kafka | 1 broker KRaft, RF 1 | Strimzi operator (≥ 3 broker, RF 3) hoặc managed Kafka |
| Spark Structured Streaming | `local[*]` trong container | `spark-submit --master k8s://...` hoặc Spark operator; driver + executor pods |
| Object storage | MinIO single node | MinIO distributed / HDFS / S3 cloud (theo rubric) |
| OpenSearch | 1 node, security tắt | OpenSearch operator hoặc Helm chart, ≥ 3 node, bật security |
| Indexer, API | Container trong Compose | Deployment + Service; HPA cho API |
| Secrets | `.env` local | Kubernetes Secret / secret manager của cloud |
| Observability | `docker compose logs` | Prometheus + Grafana, log tập trung |

## Thứ tự đề xuất

1. Đóng gói image cho producer, stream processor, indexer, API (đã có Dockerfile từ M1).
2. Chạy thử trên cluster local (kind/minikube) với Helm chart/operator cho Kafka và OpenSearch.
3. Chuyển object storage sang lựa chọn phân tán đã được giảng viên chấp nhận.
4. Spark on K8s với checkpoint trên storage phân tán.
5. Đo lại freshness/throughput trên cluster và so với số đo local ở M1.
