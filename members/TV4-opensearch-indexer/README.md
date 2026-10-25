# TV4 — Vector/NoSQL Store & Indexing

> Người phụ trách: _(tên / @github)_ · Reviewer chéo của bạn: **TV5** · Bạn review: PR của **TV5**
> Đọc trước: [plan](../../StreamRAG_Milestone_1_Plan.md) mục 2.2, 2.3, 3.2, 3.3, 6, 8D và [members/README.md](../README.md).

## Vai trò

Bạn biến cleaned events thành tài liệu **tìm kiếm được** trong OpenSearch (NoSQL document store + vector index). Trọng tâm: **version-aware, idempotent upsert** — version mới thay bản cũ, version cũ tới muộn không ghi đè, duplicate không tạo bản thứ hai.

## File bạn sở hữu

```text
infra/opensearch/
├── index_mapping.json        # mapping + settings knn
└── init_index.py             # tạo index nếu chưa có (dùng trong make init)
services/indexer/
├── README.md
├── Dockerfile
├── embedding.py              # Embedder dùng chung với TV5
├── source.py                 # đọc từ file JSONL HOẶC Kafka (chọn qua env/flag)
├── consumer.py               # vòng lặp: source -> embed -> upsert
└── upsert.py                 # logic version-aware upsert
tests/unit/test_upsert.py, test_embedding.py
```

## Làm song song thế nào

- **Không chờ Compose**: `docker run` 1 OpenSearch single-node (tắt security cho local) để làm mapping từ D1.
- **Không chờ Spark (TV3)**: indexer có `source.py` đọc **file JSONL** (`fixtures.jsonl` của TV2, sau đó `cleaned_fixtures.jsonl` của TV3) — giả làm cleaned topic. Đổi sang Kafka chỉ là đổi source.
- Trước khi TV2 merge schema (sáng D2), dùng message mẫu mục 3.2 plan.
- TV5 đang chờ bạn → giao mapping + `Embedder` + script load fixture **hết D3**.

---

## M1.1 — Nền móng (D1–D3, 26–28/10) · task M1-11

- [ ] Chọn embedding model nhỏ, ví dụ `sentence-transformers/all-MiniLM-L6-v2` (**dimension 384**, tiếng Anh). Ghi `EMBEDDING_MODEL`, `EMBEDDING_DIM` → báo TV1 đưa vào `.env.example`.
- [ ] `index_mapping.json`:
  - settings `"index.knn": true`;
  - `embedding`: `knn_vector`, `dimension` = `EMBEDDING_DIM`, HNSW (engine `lucene` hoặc `faiss`), space `cosinesimil` (hoặc normalize + `innerproduct`). **Kiểm tra docs OpenSearch đúng bản đang dùng.**
  - metadata: `document_id`, `event_id`, `category`, `source`, `source_url` (keyword); `version` (integer); `title`, `body` (text); `event_time`, `ingested_at`, `processed_at`, `indexed_at` (date).
- [ ] `init_index.py`: idempotent (chạy 2 lần không lỗi).
- [ ] `embedding.py`: `class Embedder: encode(texts: list[str]) -> list[list[float]]`; load model **1 lần**; text index = `title + "\n" + body`. Có `FakeEmbedder` (vector deterministic theo hash) cho unit test.
- [ ] Thống nhất với TV5 cách dùng chung `Embedder` (cùng model + cùng normalize): import chung hay package chung — chọn 1, ghi lại.
- [ ] Script load `fixtures.jsonl` vào index + 1 query kNN mẫu.

**Bàn giao cuối M1.1 (hết D3 → TV5):** mapping, `init_index.py`, `Embedder`, script load fixture đã merge.

---

## M1.2 — Module chạy độc lập (D4–D7, 29/10–01/11) · task M1-12

- [ ] **Version-aware upsert** (`upsert.py`) — gợi ý external versioning của OpenSearch:
  - `_id = document_id`, `version = event.version`, `version_type = external`;
  - version mới hơn → ghi đè; version **bằng hoặc cũ hơn** → **409 conflict** → no-op (log `skipped_stale` / `skipped_duplicate`), **không** retry vô hạn, **không** crash;
  - chọn cách khác (đọc rồi so sánh) → giải thích race condition trong README.
- [ ] Thêm `indexed_at` (UTC) vào document — TV5 cần để đo freshness.
- [ ] `source.py`: `FileSource(path)` và `KafkaSource(topic=streamrag.cleaned.v1, group.id cố định)`; Kafka **commit offset sau khi** ghi OpenSearch thành công (at-least-once).
- [ ] Bulk API nếu cần, nhưng xử lý lỗi **từng item** trong response.
- [ ] Test (unit với OpenSearch mock, hoặc integration với container):
  - v1 rồi v2 cùng `document_id` → chỉ còn v2, query không trả nội dung v1;
  - v2 rồi v1 (late) → vẫn v2;
  - cùng event 2 lần → 1 document.
- [ ] Từ D5: chạy indexer với `cleaned_fixtures.jsonl` của TV3 để chắc format khớp.
- [ ] Dockerfile + lệnh chạy → giao TV1; volume cache model (`HF_HOME`), **không commit model**.

**Bàn giao cuối M1.2 (Gate B):** indexer trong Compose index được seed từ cleaned topic; search ra kết quả.

---

## M1.3 — Tích hợp live (D8–D10, 02–04/11) · task M1-12

- [ ] Chuyển hẳn sang `KafkaSource`; event live từ TV2 → searchable **không restart**.
- [ ] `inc-atlas-001` v2 → query chỉ ra v2 (Case 3). Cùng TV5 kiểm tra qua `/ask`.

**Bàn giao cuối M1.3 (Gate C):** update version phản ánh đúng trong kết quả search.

---

## M1.4 — Hardening & release (D11–D14, 05–08/11) · task M1-13, M1-17

- [ ] Log có cấu trúc mỗi message: `event_id`, `document_id`, `version`, kết quả (`indexed` / `skipped_stale` / `error`), thời gian.
- [ ] Retry có giới hạn + backoff khi OpenSearch tạm lỗi; message không parse được → log + bỏ qua (dead-letter topic để milestone sau).
- [ ] Kill indexer giữa chừng rồi chạy lại → không duplicate (nhờ `_id = document_id`).
- [ ] Bộ edge case của TV2 → không duplicate searchable, late version không ghi đè.
- [ ] Runbook (gửi TV1 đưa vào `docs/runbook.md`): xóa + tạo lại index, **replay** từ Kafka (reset offset consumer group), lỗi thường gặp (sai dimension, thiếu RAM).
- [ ] Cross-review + clean clone (M1-17).

## Definition of Done của bạn

- [ ] `make init` tạo index đúng mapping; chạy lại không lỗi.
- [ ] Event mới trên cleaned topic → searchable, không restart.
- [ ] v2 thay v1; late/duplicate không ghi đè / không nhân bản (có test).
- [ ] README module + runbook lỗi + unit test.
