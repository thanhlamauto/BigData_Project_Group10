# TV1 — Lead / Architecture / DevOps / Integration

> Người phụ trách: _(tên / @github)_ · Reviewer chéo của bạn: **TV3** · Bạn review: PR hạ tầng/tích hợp của mọi người
> Đọc trước: [plan](../../StreamRAG_Milestone_1_Plan.md) mục 1, 2, 4, 7, 8, 9, 10 và [members/README.md](../README.md).

## Vai trò

Bạn chốt scope + kiến trúc, dựng "khung" repo để 4 người còn lại cắm module vào, giữ CI, và **chịu trách nhiệm tích hợp + demo cuối**. Bạn **điều phối**, không phải người viết hộ code của module khác.

## File bạn sở hữu

```text
README.md
.gitignore
.env.example
Makefile
pyproject.toml (hoặc requirements/*.txt — pin version)
infra/compose.yaml
docs/architecture.md
docs/adr/0001-kappa-stack.md
docs/runbook.md
docs/milestone_1_evidence.md
deploy/k8s/README.md       # chỉ roadmap, không hứa production
.github/workflows/ci.yaml
.github/pull_request_template.md
```

## Làm song song thế nào

- Bạn **không phụ thuộc ai** trong M1.1: docs, Compose, CI, Makefile đều làm được ngay.
- Việc của bạn là **thứ người khác cần sớm** (Compose + `.env.example` hết D2, CI hết D3) → ưu tiên những thứ đó trước docs dài.
- Makefile target nào module chưa có thì in `"not implemented yet (owner: TVx)"` và exit ≠ 0, để các owner tự điền lệnh vào sau — không ai phải chờ bạn.

---

## M1.1 — Nền móng (D1–D3, 26–28/10) · task M1-01, M1-02, M1-03

**D1 — Kickoff 45 phút**
- [ ] Điền tên + GitHub username vào [members/README.md](../README.md); mời 4 người làm collaborator.
- [ ] Hỏi từng người: máy có Docker không, bao nhiêu RAM. Máy 8 GB → cần profile `lite`.
- [ ] Chốt LLM provider chính (Ollama local hay API OpenAI-compatible) + mock cho test.
- [ ] Tạo Issues M1-01 → M1-17 (gắn label mốc `M1.1`…`M1.4`) và GitHub Project board.
- [ ] Bật branch protection cho `main`: bắt buộc PR + 1 review.
- [ ] Ghi câu hỏi cho giảng viên (mục 14 plan): deadline thật, M1 có cần K8s không, MinIO có được coi là "equivalent storage" không.

**D1–D2 — Kiến trúc (Gate A, D2)**
- [ ] `docs/architecture.md`: vì sao Kappa, component diagram (Mermaid), write path / read path, storage strategy, local (Compose) vs target (K8s/Cloud), sequence diagram "new event → updated answer".
- [ ] `docs/adr/0001-kappa-stack.md`: context, decision, alternatives (Lambda; chỉ FastAPI + vector DB), consequences, giới hạn (at-least-once, MinIO single node ≠ distributed).
- [ ] Chốt cùng TV2: topic `streamrag.raw.v1`, `streamrag.cleaned.v1`; key = `document_id`.

**D2 — Compose tối thiểu (hạn hết D2, mọi người đang chờ)**
- [ ] `infra/compose.yaml`: Kafka (KRaft), MinIO (+ init bucket), OpenSearch single-node (tắt security plugin cho local và **ghi rõ**). Có healthcheck.
- [ ] Pin image tag cụ thể (không `latest`); `OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m`.
- [ ] Kafka cần **2 listener** (trong network Compose và từ host).
- [ ] `.env.example`: `KAFKA_BOOTSTRAP_SERVERS`, `RAW_TOPIC`, `CLEANED_TOPIC`, `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `LAKE_BUCKET`, `OPENSEARCH_URL`, `OPENSEARCH_INDEX`, `EMBEDDING_MODEL`, `EMBEDDING_DIM`, `LLM_PROVIDER` (`ollama|openai|mock`), `LLM_BASE_URL`, `LLM_MODEL`, `LLM_API_KEY=` (trống). **Không có secret thật.**
- [ ] `.gitignore`: `.env`, `__pycache__/`, `.venv/`, checkpoint, `data/lake/`, model cache, Parquet runtime.

**D3 — CI + Makefile**
- [ ] `.github/workflows/ci.yaml`: Python, `ruff` (lint + format check), `pytest tests/unit`. Không chạy full stack trên CI.
- [ ] `.github/pull_request_template.md`: task ID, mốc, cách test, checklist (test, docs, không secret).
- [ ] `Makefile`: `up`, `down`, `init`, `seed`, `simulate`, `test`, `smoke`, `logs`, `ps` (stub như mô tả ở trên).

**Bàn giao cuối M1.1:** `make up` boot Kafka/MinIO/OpenSearch healthy; CI chạy trên PR; architecture + ADR đã merge.

---

## M1.2 — Module chạy độc lập (D4–D7, 29/10–01/11) · task M1-02, M1-03

- [ ] Nhận Dockerfile/lệnh chạy từ TV3 (Spark), TV4 (indexer), TV5 (API) → thêm service vào Compose (dùng `depends_on: condition: service_healthy`).
- [ ] Thêm `mem_limit` cho từng service; thử `make up` trên **máy yếu nhất nhóm**, ghi RAM thực tế.
- [ ] Nối `make init` với script tạo topic (TV2), bucket (TV3), index (TV4).
- [ ] Thử 1 PR có test fail để chứng minh CI đỏ.
- [ ] README: cập nhật phần setup, **chỉ ghi lệnh đã chạy được trên `main`**.
- [ ] **D7 — Gate B**: điều phối ghép seed Kafka → Spark → Indexer → Search. Ghi blocker + người xử lý.

**Bàn giao cuối M1.2:** mọi service có trong Compose; `make up && make init` chạy được.

---

## M1.3 — Tích hợp live (D8–D10, 02–04/11) · task M1-04

- [ ] Theo dõi bảng blocker hằng ngày; ưu tiên fix tích hợp hơn feature mới.
- [ ] Viết sơ bộ `docs/runbook.md`: start/stop, log từng service, lỗi hay gặp (OpenSearch thiếu RAM / `vm.max_map_count`, Spark thiếu JAR S3A, Kafka chưa ready), cách replay Kafka để rebuild index.
- [ ] **D10 — Gate C**: cùng TV5 chạy demo event mới → câu trả lời mới, không restart.

**Bàn giao cuối M1.3:** live demo Case 1–3 (mục 8D plan) chạy được trên 1 máy.

---

## M1.4 — Hardening & release (D11–D14, 05–08/11) · task M1-04, M1-17

- [ ] `docs/milestone_1_evidence.md`: commit SHA/tag, lệnh + exit code, CPU/RAM, output 5 demo case, freshness (từ TV5), issue còn tồn.
- [ ] `deploy/k8s/README.md`: roadmap K8s/Cloud cho milestone sau.
- [ ] **D13 — Gate D**: nhờ 1 người *không viết setup* clone sạch từ `main` và chạy theo README; sửa README đến khi họ chạy được không cần hỏi.
- [ ] D14: rehearsal kịch bản demo 4–6 phút (mục 12 plan), tag `v0.1.0-m1`.

## Definition of Done của bạn

- [ ] Clone sạch + `cp .env.example .env` + `make up` → mọi service healthy.
- [ ] `docs/architecture.md` + ADR đáp ứng mục 8C plan.
- [ ] CI chạy trên mọi PR, fail khi test fail.
- [ ] Evidence + tag `v0.1.0-m1` có trên GitHub.
