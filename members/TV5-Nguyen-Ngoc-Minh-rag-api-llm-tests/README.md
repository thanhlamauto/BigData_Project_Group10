# TV5 — RAG API / LLM / Tests

> Người phụ trách: **Nguyen Ngoc Minh** (TV5) · Reviewer chéo của bạn: **TV4 (Kieu Son Tung)** · Bạn review: PR của **TV4 (Kieu Son Tung)**
> Đọc trước: [plan](../../StreamRAG_Milestone_1_Plan.md) mục 1.2 (chú ý về LLM mock), 2.2, 6, **8D–8E**, **9**, 12 và [members/README.md](../README.md).

## Code và cách chạy

Code TV5 nằm trong folder này: `services/api/`, `scripts/`, `tests/`, `fixtures/`, `docs/`.
Xem [hướng dẫn chạy API](services/api/README.md), [API contract](docs/api_contract.md)
và [kết quả kiểm chứng / phần cần tích hợp](docs/validation.md).
Các checkbox bên dưới giữ làm tiêu chí nghiệm thu của cả nhóm; có code/test offline
không đồng nghĩa đã hoàn thành demo pipeline hoặc LLM thật.

## Vai trò

Bạn làm **read path**: FastAPI nhận câu hỏi → encode query → top-k từ OpenSearch → ghép context → gọi LLM → trả `answer` + `sources`. Bạn cũng sở hữu **smoke test end-to-end** và đo **freshness** — bằng chứng "real-time" của cả nhóm.

## File bạn sở hữu

```text
services/api/
├── README.md
├── Dockerfile
├── main.py              # FastAPI app
├── retrieval.py         # Retriever interface: FakeRetriever + OpenSearchRetriever
├── llm.py               # adapter: ollama | openai-compatible | mock
└── prompt.py            # grounded prompt template
scripts/smoke_test.py
tests/integration/
tests/unit/test_prompt.py, test_llm_mock.py, test_api.py
docs/api_contract.md
```

## Làm song song thế nào

- **Không chờ OpenSearch/indexer (TV4)**: định nghĩa `Retriever` interface; `FakeRetriever` đọc `fixtures.jsonl` (hoặc message mẫu mục 3.2 plan) và trả top-k bằng so khớp từ khóa đơn giản. Đổi sang `OpenSearchRetriever` khi TV4 giao mapping (hết D3).
- **Không chờ LLM thật**: `LLM_PROVIDER=mock` từ D1; Ollama/API thêm sau.
- **Không chờ pipeline**: smoke test viết dần từ M1.2, chạy thật ở M1.3–M1.4.

---

## M1.1 — Nền móng (D1–D3, 26–28/10) · task M1-14

- [ ] Skeleton FastAPI; `GET /health` → 200.
- [ ] `POST /ask` với Pydantic model. Request: `{"question": str, "top_k": int = 3}`. Response (mục 9 plan):
  ```json
  {
    "answer": "...",
    "sources": [{"document_id": "...", "version": 2, "title": "...", "event_time": "...", "score": 0.0}],
    "retrieval_ms": 0
  }
  ```
  Có thể thêm `llm_provider`, `llm_ms`.
- [ ] `FakeRetriever` + `mock` LLM → `/ask` chạy end-to-end **trong process**, không cần container nào.
- [ ] Unit test API bằng FastAPI `TestClient` (chạy trong CI).
- [ ] Draft `docs/api_contract.md` (request/response, mã lỗi, env).

**Bàn giao cuối M1.1:** `/health`, `/ask` (fake + mock) + test đã merge.

---

## M1.2 — Module chạy độc lập (D4–D7, 29/10–01/11) · task M1-14, M1-15

- [ ] `OpenSearchRetriever`: encode query bằng **cùng `Embedder`** của TV4 (cùng model + normalize), kNN top-k, trả metadata (không trả vector). Test trên index fixture của TV4 (tự chạy `init_index.py` + script load fixture).
- [ ] `llm.py`: `generate(prompt) -> str`, chọn qua `LLM_PROVIDER`:
  - `ollama`: `LLM_BASE_URL`, `LLM_MODEL`;
  - `openai`: API OpenAI-compatible, `LLM_API_KEY` từ env — **không commit key**;
  - `mock`: deterministic từ context (test/CI).
- [ ] Grounded prompt (`prompt.py`): chỉ trả lời dựa trên context; không đủ → nói không đủ thông tin; context gồm `title`, `body`, `event_time`, `version` của từng source.
- [ ] **Context rỗng** → không gọi LLM, trả "không đủ thông tin" + `sources: []`.
- [ ] Lỗi LLM (timeout, 5xx) → trả lỗi rõ ràng (502/503) hoặc retrieval kèm cờ lỗi — không treo request.
- [ ] **Tuyệt đối không hard-code câu trả lời** cho câu hỏi demo.
- [ ] Dockerfile + lệnh chạy → giao TV1 thêm vào Compose.
- [ ] Bắt đầu `scripts/smoke_test.py` (khung các bước, xem M1.4).

**Bàn giao cuối M1.2 (Gate B):** `/ask` trên OpenSearch thật trả đúng `document_id` cho câu hỏi seed.

---

## M1.3 — Tích hợp live (D8–D10, 02–04/11) · task M1-15

- [ ] **Ít nhất 1 lần chạy với LLM thật** (Case 5) — lưu log/screenshot. Không được → báo cáo rõ là chỉ có retrieval + mock.
- [ ] Cùng TV1 chạy **Gate C (D10)**: TV2 bắn event mới / `inc-atlas-001` v2 → `/ask` trả thông tin mới, `sources` là version 2, answer chứa `21:30`.
- [ ] Freeze API v1 (D8).

**Bàn giao cuối M1.3 (Gate C):** demo live Case 1–3 + LLM thật.

---

## M1.4 — Hardening & release (D11–D14, 05–08/11) · task M1-16, M1-17

- [ ] `scripts/smoke_test.py` (gọi qua `make smoke`), **exit 0 = PASS, ≠0 = FAIL**, in từng bước:
  1. `/health` = 200.
  2. **Seed**: câu hỏi phase `seed` trong `test_cases.jsonl` → `expected_document_id` có trong `sources`.
  3. **Stream**: bắn 1 event mới (simulator TV2) → poll `/ask` mỗi 1–2s đến khi thấy (timeout ~120s), ghi `first_visible_at`.
  4. **Revision**: bắn `inc-atlas-001` v2 → `sources` có v2, không còn v1; answer chứa `21:30`.
  5. **Fault**: bắn malformed + duplicate → vẫn trả lời, không có 2 bản cùng `document_id` trong `sources`.
- [ ] **Freshness = `first_visible_at - ingested_at`** cho bước 3/4 → in ra, gửi TV1 đưa vào evidence.
- [ ] Đánh giá đơn giản trên `test_cases.jsonl`: hit@k (`expected_document_id` trong top-k), tỉ lệ answer chứa `expected_facts`.
- [ ] Nếu kịp (P1): 1 event/s trong 5 phút → sent/processed/indexed, p50/p95 freshness. **Không ghi số khi chưa đo.**
- [ ] Hoàn thiện `docs/api_contract.md`; cùng TV1 chạy **Gate D (D13)** — bạn là người chạy demo chính.
- [ ] Cross-review + clean clone (M1-17).

## Lưu ý

- Score kNN của OpenSearch không phải xác suất; đừng diễn giải quá mức.
- Mock chỉ cho test. Demo "RAG end-to-end" phải dùng LLM thật.

## Definition of Done của bạn

- [ ] `/health` 200; `/ask` trả `answer` + `sources` (`document_id`, `version`) + `retrieval_ms`.
- [ ] Unit test chạy offline với mock trong CI.
- [ ] Có log/demo LLM thật.
- [ ] `make smoke` PASS cho Case 1–4 và in freshness; README module + API contract.
