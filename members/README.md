# Phân công Milestone 1 — StreamRAG (5 thành viên, làm song song)

> Bối cảnh đầy đủ: [StreamRAG_Milestone_1_Plan.md](../StreamRAG_Milestone_1_Plan.md). **Đọc plan trước, sau đó đọc folder của mình.**
> Thời gian: **D1 = 26/10/2026 → D14 = 08/11/2026** (giả định, xác nhận lại tại kickoff).

## Ai làm gì

| Thành viên | Folder chỉ dẫn | Vai trò | Task trong plan | Reviewer chéo |
|---|---|---|---|---|
| **TV1** — Pham Chi Bang | [Pham-Chi-Bang](Pham-Chi-Bang/README.md) | Lead, Architecture, DevOps, Integration | M1-01 → M1-04 | TV3 (Nguyen Phu An) |
| **TV2** — Nguyen Thanh Lam | [Nguyen-Thanh-Lam](Nguyen-Thanh-Lam/README.md) | Data contract, dataset, Kafka producer | M1-05 → M1-07 | TV3 (Nguyen Phu An) |
| **TV3** — Nguyen Phu An | [Nguyen-Phu-An](Nguyen-Phu-An/README.md) | PySpark Structured Streaming, MinIO/Parquet | M1-08 → M1-10 | TV2 (Nguyen Thanh Lam) |
| **TV4** — Kieu Son Tung | [Kieu-Son-Tung](Kieu-Son-Tung/README.md) | OpenSearch, embedding, indexer | M1-11 → M1-13 | TV5 (Nguyen Ngoc Minh) |
| **TV5** — Nguyen Ngoc Minh | [Nguyen-Ngoc-Minh](Nguyen-Ngoc-Minh/README.md) | FastAPI `/ask`, LLM, smoke test | M1-14 → M1-16 | TV4 (Kieu Son Tung) |

M1-17 (cross-review, chạy thử trên máy khác, rehearsal) là việc của **cả nhóm** ở M1.4.

## Milestone 1 được chia thành 4 mốc nhỏ

| Mốc | Ngày | Mục tiêu | Gate kết thúc |
|---|---|---|---|
| **M1.1 — Nền móng** | D1–D3 (26–28/10) | Contract + fixtures + skeleton từng module + CI + Compose tối thiểu | **Gate A** (D2): contract & kiến trúc freeze v1 |
| **M1.2 — Module chạy độc lập** | D4–D7 (29/10–01/11) | Mỗi module chạy được **một mình** với fixture/mock, có test | **Gate B** (D7): ghép seed Kafka → Spark → Indexer → Search |
| **M1.3 — Tích hợp live** | D8–D10 (02–04/11) | Thay mock bằng service thật, event mới chảy end-to-end | **Gate C** (D10): live demo, update version |
| **M1.4 — Hardening & release** | D11–D14 (05–08/11) | Edge case, restart, smoke, evidence, clean clone | **Gate D** (D13) + **M1 DONE** (D14) |

## 5 người làm song song thế nào

**Nguyên tắc: trong M1.1 và M1.2, không ai phụ thuộc vào *service đang chạy* của người khác — chỉ phụ thuộc vào *file trong repo* (contract, fixtures, mapping).** Mỗi người tự dựng mock/stub cho phần đầu vào của mình. Chỉ từ M1.3 mới ghép service thật.

| | TV1 | TV2 | TV3 | TV4 | TV5 (Nguyen Ngoc Minh) |
|---|---|---|---|---|---|
| **M1.1** | ADR, architecture, Compose tối thiểu, CI, repo rules | JSON Schema + 10 fixtures (**sáng D2**), generator | Spike Spark→Parquet local trên fixture file, skeleton job | Mapping, `Embedder`, index fixture vào OpenSearch container riêng | FastAPI skeleton, `/health`, `/ask` với **FakeRetriever + mock LLM** |
| **Đầu vào dùng tạm** | — | — | Message mẫu ở mục 3.2 plan → fixture của TV2 | Message mẫu mục 3.2 → fixture TV2 | Message mẫu mục 3.2 → fixture TV2 |
| **M1.2** | Thêm service vào Compose, `make` targets, README | 400 seed, test cases, producer + simulator | Đọc Kafka, validate, Bronze/Silver trên MinIO, cleaned topic | Indexer đọc **file JSONL hoặc Kafka**, version-aware upsert | Retrieval thật trên index fixture của TV4, LLM adapter |
| **Đầu vào dùng tạm** | — | Kafka container riêng | Tự produce fixture vào Kafka bằng CLI | Fixture JSONL (giả làm cleaned topic) | Index fixture của TV4 (hoặc tự chạy `init_index` + load fixture) |
| **M1.3** | Điều phối ghép, Gate C | Live update `inc-atlas-001` v2 | Cleaned topic thật → TV4 | Consume cleaned topic thật | `/ask` trên index thật, demo LLM thật |
| **M1.4** | Runbook, evidence, tag | Edge-case fixtures + tests | Dedup, restart test | Retry, reindex/replay runbook | `make smoke`, đo freshness |

### Những "điểm chạm" duy nhất giữa các người (đều là file, không phải service)

| Hạn | Ai giao | File | Ai dùng |
|---|---|---|---|
| **Sáng D2 (27/10)** | TV2 | `data/contracts/event.schema.json`, `data/sample/fixtures.jsonl` (10 records) | TV3, TV4, TV5 |
| **Hết D2** | TV1 | `infra/compose.yaml` tối thiểu (Kafka, MinIO, OpenSearch) + `.env.example` | Tất cả |
| **Hết D3** | TV4 | `infra/opensearch/index_mapping.json`, `init_index.py`, `services/indexer/embedding.py` | TV5 (Nguyen Ngoc Minh) |
| **Hết D5** | TV3 | `data/sample/cleaned_fixtures.jsonl` (output Spark mẫu) | TV4 (Kieu Son Tung) |
| **Hết D8** | Tất cả | **Freeze v1** schema/API | Tất cả |

Trước khi có file thật (D1), mọi người code theo **message mẫu ở mục 3.2 của plan** — đó đã là contract v0.

## Quy tắc chung

1. **Không push thẳng vào `main`.** Branch theo task: `feat/m1-06-kafka-producer`, `docs/m1-01-architecture`, ...; mở PR, cần **1 reviewer** (bảng trên) và CI xanh mới merge.
2. PR nhỏ, merge sớm và thường xuyên (ít nhất 1 PR/người cho mỗi mốc) để tránh conflict khi ghép.
3. Mỗi task = 1 GitHub Issue: Owner, task ID, mốc (M1.x), input/output, DoD, dependency, status (`Todo / In Progress / In Review / Blocked / Done`).
4. Commit theo Conventional Commits: `feat(ingestion): ...`, `fix(indexer): ...`, `docs(architecture): ...`.
5. Module của bạn phải có **README ngắn (cách chạy) + ít nhất 1 test**. Bạn chịu trách nhiệm end-to-end output của mình.
6. Mỗi người chỉ sửa file mình sở hữu (ghi trong folder của mình). Cần sửa file người khác → nhờ owner hoặc tag owner review.
7. Không commit `.env`, API key, model cache, checkpoint, Parquet runtime.
8. Bị block **> 24h** → báo group ngay và tiếp tục bằng fixture/mock.
9. Daily async 5 phút: **Done / Next / Blocker**. Họp 30 phút tại D2, D7, D10, D13 (cuối mỗi mốc).
10. Từ **06/11 (D12)**: feature freeze — chỉ merge fix P0, docs, tests. Cuối milestone tag `v0.1.0-m1`.
11. Không tuyên bố "exactly-once", "real-time tức thì" hay số benchmark khi chưa đo.

> **Điều chỉnh so với plan gốc để làm song song:** CI chuyển lên M1.1 (thay vì D7–D10) để PR của mọi người có lint/test từ đầu; Compose tối thiểu giao hết D2 (thay vì D4); schema giao sáng D2; mỗi module phải chạy được với input giả trước khi ghép.
