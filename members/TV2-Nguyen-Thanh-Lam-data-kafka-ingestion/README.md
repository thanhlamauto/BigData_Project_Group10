# TV2 — Data & Kafka Ingestion

> Người phụ trách: **Nguyen Thanh Lam** (TV2) · Reviewer chéo của bạn: **TV3 (Nguyen Phu An)** · Bạn review: PR của **TV3 (Nguyen Phu An)**
> Đọc trước: [plan](../../StreamRAG_Milestone_1_Plan.md) mục 2.1, **3 (Data plan)**, 6, 8B và [members/README.md](../README.md).

## Vai trò

Bạn sở hữu **dữ liệu và hợp đồng message** — thứ mà TV3, TV4, TV5 đều code theo. Contract + 10 fixtures của bạn phải ra **sớm nhất nhóm (sáng D2)**. Sau đó bạn làm producer đẩy seed + live update vào Kafka, và bộ fixture edge-case.

## File bạn sở hữu

```text
data/README.md
data/contracts/event.schema.json
data/sample/fixtures.jsonl          # 10 records — giao sáng D2
data/sample/seed_events.jsonl       # 300–500 records
data/sample/test_cases.jsonl        # ≥ 20 câu hỏi
data/sample/edge_cases.jsonl        # duplicate/late/version/invalid
docs/data_contract.md
scripts/generate_data.py
scripts/stream_simulator.py
services/producer/
infra/kafka/                        # script tạo topic
tests/unit/test_schema.py, test_generator.py, test_producer.py
```

## Làm song song thế nào

- Bạn **không phụ thuộc ai**: generator là Python thuần; producer test với Kafka container tự chạy (`docker run` image Kafka KRaft) trước khi TV1 có Compose.
- 3 người khác đang chờ schema + fixtures → **làm cái đó đầu tiên, giao sáng D2**, rồi mới làm generator lớn.
- Unit test producer dùng mock Kafka client để chạy được trong CI.

---

## M1.1 — Nền móng (D1–D3, 26–28/10) · task M1-05

**D1 → sáng D2 — Contract (ưu tiên số 1)**
- [ ] `data/contracts/event.schema.json` (JSON Schema draft 2020-12) theo message ở mục 3.2 plan:
  `schema_version`, `event_id`, `document_id`, `version`, `operation`, `source`, `source_url`, `title`, `body`, `category`, `event_time`, `ingested_at`.
  - Required: tất cả trừ `source_url` (nullable).
  - `version`: integer ≥ 1. `operation`: enum `["upsert"]`. `event_time`/`ingested_at`: `format: date-time`, UTC (`...Z`).
  - `category`: enum nhỏ, ví dụ `incident`, `maintenance`, `resolved`, `status`.
- [ ] `data/sample/fixtures.jsonl`: **10 records hợp lệ**, có `inc-atlas-001` version 1 ("maintenance at 20:30 UTC").
- [ ] Mở PR, nhờ TV3 review nhanh, merge, **báo group**.

**D2–D3 — Generator + docs**
- [ ] `docs/data_contract.md`: ý nghĩa field, quy ước ID (`event_id` duy nhất mỗi sự kiện; `document_id` giữ nguyên khi update; `version` tăng dần), 1 document = 1 chunk, trích dẫn khi `source_url = null`.
- [ ] `scripts/generate_data.py --seed 42 --n 400 --out data/sample/seed_events.jsonl`: cùng `--seed` → **file y hệt** (không dùng `datetime.now()` khi sinh seed).
- [ ] Dữ liệu: 6–10 service giả lập (Atlas Search, Payment API, Data Gateway, ...), câu tiếng Anh tự nhiên, có giờ/ngày/trạng thái cụ thể để hỏi được.
- [ ] Validator (test): mọi dòng pass schema, `event_id` không trùng.

**Bàn giao cuối M1.1:** schema + fixtures + seed 300–500 + validator đã merge.

---

## M1.2 — Module chạy độc lập (D4–D7, 29/10–01/11) · task M1-05, M1-06

- [ ] `data/sample/test_cases.jsonl`: **≥ 20 câu hỏi**; mỗi dòng: `question`, `expected_document_id`, `expected_version`, `expected_facts` (list chuỗi phải xuất hiện), `phase` (`seed` | `after_stream`). 10 câu seed + 10 câu về event live/update. → báo TV5.
- [ ] `data/README.md`: nguồn (synthetic, nhóm tự tạo), license, số lượng, cách tái tạo, cách replay.
- [ ] `infra/kafka/`: script tạo `streamrag.raw.v1`, `streamrag.cleaned.v1` (ví dụ 3 partitions, replication 1) → đưa TV1 nối vào `make init`.
- [ ] `services/producer/`: `publish(event)` dùng `confluent-kafka` hoặc `kafka-python`; **key = `document_id`**; value JSON UTF-8; gán `ingested_at` (UTC) lúc gửi; `acks=all`, `enable.idempotence=true` (ghi chú: **không** làm cả pipeline exactly-once).
- [ ] `make seed`: đẩy `seed_events.jsonl` vào raw topic, in số message đã gửi.
- [ ] `scripts/stream_simulator.py`:
  - `--rate N` (events/giây), `--count`, `--seed`.
  - `--event-file path.jsonl` hoặc `--update inc-atlas-001` để bắn đúng 1 event (cho demo).
  - Log mỗi event: `event_id`, `document_id`, `version`, `ingested_at` (TV5 cần để đo freshness).
- [ ] Chứng minh: consume bằng `kafka-console-consumer` thấy message + key → chụp làm evidence.

**Bàn giao cuối M1.2 (Gate B):** `make seed` đẩy seed vào Kafka trong Compose; TV3 đọc được.

---

## M1.3 — Tích hợp live (D8–D10, 02–04/11) · task M1-06

- [ ] Chuẩn bị event live **chưa có trong seed** (Case 2) và `inc-atlas-001` **version 2** (20:30 → 21:30 UTC, Case 3) dưới dạng file trong `data/sample/`.
- [ ] Chạy `make simulate` khi hệ thống đang chạy → xác nhận cùng TV3/TV4 event đi qua, không restart.
- [ ] Freeze schema v1 (D8): sau đó đổi = breaking change → sửa cùng lúc `event.schema.json`, `data_contract.md`, tests, báo mọi người.

**Bàn giao cuối M1.3 (Gate C):** live event + update bắn được bằng 1 lệnh.

---

## M1.4 — Hardening & release (D11–D14, 05–08/11) · task M1-07, M1-17

- [ ] `data/sample/edge_cases.jsonl` + expected behavior trong `data/README.md`:
  - **10 duplicate**: cùng `event_id` gửi lại → downstream chỉ có 1 bản.
  - **5 late**: `event_time` cũ hơn đáng kể so với event trước.
  - **5 version update**: cùng `document_id`, `version` tăng → chỉ bản mới nhất searchable; có cả trường hợp **version cũ tới sau version mới**.
  - **5 invalid**: JSON hỏng, thiếu field, `body` rỗng, timestamp sai format → Spark bỏ qua + đếm, không crash.
- [ ] `stream_simulator.py --edge-cases` (TV3/TV4/TV5 dùng cho Case 4).
- [ ] Ingestion tests cho generator/validator/producer.
- [ ] Cross-review + chạy thử clean clone (M1-17).

## Lưu ý

- Dataset nhỏ được commit; file lớn cho load test → **không commit**, chỉ commit script.
- Dữ liệu tiếng Anh. Muốn tiếng Việt → bàn với TV4 trước D2 (đổi embedding model).

## Definition of Done của bạn

- [ ] `python scripts/generate_data.py --seed 42` tái tạo đúng file seed đã commit.
- [ ] 300–500 seed + ≥ 20 test cases + edge cases, pass validator.
- [ ] `make seed` / `make simulate` đẩy event vào Kafka **khi hệ thống đang chạy**.
- [ ] `data/README.md`, `docs/data_contract.md` đầy đủ; có unit test.
