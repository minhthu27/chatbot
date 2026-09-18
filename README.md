# Hệ thống RAG + Dữ liệu có cấu trúc — NGHIÊN CỨU VÀ PHÁT TRIỂN CƠ CHẾ QUẢN TRỊ TRI THỨC NHẰM NÂNG CAO ĐỘ CHÍNH XÁC CHO HỆ THỐNG CHATBOT ĐẠI HỌC KINH TẾ QUỐC DÂN

**Phát triển:** Nhóm P.Thảo, Minh Thu

Tài liệu này mô tả kiến trúc, quy trình vận hành và các điểm cần lưu ý khi
tích hợp hệ thống vào môi trường sản xuất.

---

## 1. Tổng quan kiến trúc

Hệ thống gồm 2 luồng xử lý dữ liệu độc lập, hội tụ tại máy chủ chatbot:

```
[PDF]              --pipeline_pdf.py------>   ┐
                                              ├─> markdown/*.md ──> chunks/*.json
[DOCX/DOC/TXT/MD]  --pipeline_docx_txt.py-->  ┘                            │
                                                                           │
                                                               conflict_detection.py
                                                         (phát hiện xung đột nội dung RAG,
                                                          đối chiếu chéo với dữ liệu bảng)
                                                                           │
                                                               rebuild-index (embedding + FAISS)
                                                                           │
[CSV/XLSX] --structured_data_pipeline.py--> tables/*.csv ──┐               │
                 (dùng structured_conflict_detection.py    │               │
                  làm lõi so khớp & đối chiếu)             │               │
                                                           ▼               ▼
                                                     gọi /admin/reload-index
                                                           │
                                                           ▼
                                             ┌──────────────────────────┐
                                             │        api8923.py        │
                                             │  FAISS + BM25 + Multi-   │
                                             │  EntityMatcher + agent   │
                                             └──────────────────────────┘
                                                           │
                                                       POST /ask
```

Sơ đồ nghiệp vụ chi tiết (upload → tiền xử lý → phát hiện xung đột → cập
nhật production) được đính kèm riêng dưới dạng hình ảnh, cho từng loại dữ
liệu (phi cấu trúc và có cấu trúc).

---

## 2. Danh sách file mã nguồn cần gửi đủ

| File | Vai trò |
|---|---|
| `api8923.py` | Máy chủ Flask phục vụ chatbot — endpoint `/ask`, `/admin/reload-index`, `/metadata` |
| `admin_api.py` | Máy chủ Flask quản trị (port riêng, mặc định 8924) — giao diện web `index.html` để upload/duyệt tài liệu, quản lý bảng dữ liệu, chạy test có kiểm soát; KHÔNG phải máy chủ chatbot, chạy song song với `api8923.py` |
| `patterns.py` | Regex tổng quát (năm, mã ngành, email) dùng lọc bổ sung sau khi đã xác định bảng/thực thể |
| `multi_entity_matcher.py` | Lớp so khớp thực thể theo `registry.json`, chạy trước mọi lời gọi LLM |
| `conflict_detection.py` | Phát hiện & xử lý xung đột dữ liệu phi cấu trúc (RAG); đối chiếu chéo với dữ liệu có cấu trúc |
| `structured_conflict_detection.py` | Module lõi xử lý dữ liệu có cấu trúc: so khớp theo khóa, đối chiếu chéo 2 chiều |
| `structured_data_pipeline.py` | Quy trình nạp/cập nhật dữ liệu có cấu trúc (`.csv`/`.xlsx`) |
| `pipeline_pdf.py` | Xử lý PDF: OCR → gợi ý metadata → sinh markdown → chunk |
| `pipeline_docx_txt.py` | Xử lý `.docx`/`.doc`/`.txt`/`.md`, tái sử dụng các bước xử lý của `pipeline_pdf.py` |
| `build_vectorstore.py` | Khởi tạo FAISS index lần đầu từ toàn bộ chunk đã có |
| `registry.json` | Khai báo cấu trúc từng bảng dữ liệu (khóa chính, cột tên, cột vai trò, quy tắc lọc...) |

---

## 3. Cấu trúc thư mục dữ liệu

```
<gốc dự án>/
├── registry.json
├── .env
├── data/
│   ├── raw/
│   │   ├── pdf/
│   │   ├── docx/
│   │   ├── txt/
│   │   └── md/
│   └── processed/
│       ├── markdown/
│       ├── chunks/
│       ├── tables/
│       ├── tables_staging/
│       ├── conflict/
│       │   ├── manifest.json
│       │   ├── decision_log.jsonl
│       │   ├── structured_decision_log.jsonl
│       │   ├── embedding_cache.pkl
│       │   └── snapshots/
│       └── vectorstore/
```

Toàn bộ thư mục con trong `data/processed/` được các module tự tạo (`mkdir`)
nếu chưa tồn tại — không cần khởi tạo tay, ngoại trừ nội dung gốc trong
`data/raw/`.

### 3.1. Chuẩn bị dữ liệu (`data`)

Do thư mục `data` chứa các file dữ liệu nặng và không được push trực tiếp lên GitHub, bạn cần tải về và giải nén thủ công theo các bước sau:

1. **Tải file dữ liệu:** Download file nén `data.zip` tại link: https://drive.google.com/file/d/1znt2tZMa5YVF2vEcV2_y27XEBl3F4Tbs/view?usp=sharing.
2. **Giải nén:** Giải nén file `data.zip` vào trực tiếp thư mục gốc của dự án.
3. **Kiểm tra:** Đảm bảo thư mục `data/` sau khi giải nén nằm đúng vị trí `<gốc dự án>/data/` như cấu trúc ở Mục 3.

---

## 4. Biến môi trường (`.env`)

| Biến | File sử dụng | Giá trị mặc định | Ghi chú |
|---|---|---|---|
| `OLLAMA_SERVER` | `api8923.py`, `conflict_detection.py`, `pipeline_pdf.py` | `http://10.2.13.58:8037/ollama` | Cổng proxy có xác thực; phải giống nhau ở mọi file |
| `OLLAMA_SECKEY` | như trên | `research` | Header xác thực gọi qua proxy (`x-ollama-seckey`) |
| `EMBED_MODEL` | `api8923.py`, `conflict_detection.py`, `build_vectorstore.py` | `qwen3-embedding:8b-ctx16k` | Phải khớp model dùng lúc `build_vectorstore.py`; đổi model bắt buộc build lại toàn bộ index |
| `CHAT_MODEL` | `api8923.py` | `qwen2.5:14b-instruct-ctx16k` | Model sinh câu trả lời cuối |
| `AGENT_MODEL` | `api8923.py` | `qwen2.5:14b-instruct-ctx16k` | Model cho pandas agent + router; cố ý dùng CHUNG model với `CHAT_MODEL` để giới hạn số model tải trên GPU |
| `METADATA_MODEL` | `pipeline_pdf.py` | `qwen2.5:14b-instruct-ctx16k` | Gợi ý metadata văn bản; dùng chung model với `CHAT_MODEL`/`AGENT_MODEL` |
| `CONFLICT_LLM_BACKEND` | `conflict_detection.py` | `ollama` | `ollama` (tự host) hoặc `openai` |
| `CONFLICT_LLM_MODEL` | `conflict_detection.py` | `qwen2.5:14b-instruct-ctx16k` (hoặc `gpt-4o-mini` nếu backend là `openai`) | Model phân tích xung đột nội dung |
| `OPENAI_API_KEY` | `pipeline_pdf.py` (bắt buộc), `conflict_detection.py` (chỉ khi backend là `openai`) | — | Dùng riêng cho bước OCR (`gpt-4.1-mini`) |
| `API_AUTH_TOKEN` | `api8923.py`, `conflict_detection.py`, `structured_data_pipeline.py` | — (bắt buộc, không hardcode) | Xác thực `/ask` và `/admin/reload-index` |
| `API8923_BASE_URL` | `conflict_detection.py` | `http://127.0.0.1:8923` | Địa chỉ gọi `/admin/reload-index` sau khi cập nhật dữ liệu — đổi giá trị này nếu `api8923.py` chạy khác cổng/máy |
| `ALLOWED_ORIGIN` | `api8923.py` | `""` | Cấu hình CORS (để trống = chặn mọi domain khác) |
| `RATE_LIMIT_MAX_REQUESTS` / `RATE_LIMIT_WINDOW_SECONDS` | `api8923.py` | `60` / `60` | Giới hạn tần suất request trên `/ask` |
| `RAG_SIMILARITY_THRESHOLD` | `api8923.py` | `0.55` | Ngưỡng lọc kết quả truy xuất RAG — cần hiệu chỉnh lại nếu đổi embedding model (kiểm tra qua hàm `probe_rag`) |
| `AGENT_KEEP_ALIVE` | `api8923.py` | `30s` | Thời gian giữ model agent trong bộ nhớ Ollama giữa các request |
| `ADMIN_API_PORT` | `admin_api.py` | `8924` | Cổng máy chủ quản trị (mục 6b) — khác cổng `api8923.py` (8923) vì chạy song song |

Toàn bộ model LLM/embedding hiện dùng đều tự host qua Ollama, ngoại trừ
bước OCR văn bản PDF (`pipeline_pdf.py`) tiếp tục dùng `gpt-4.1-mini` của
OpenAI do yêu cầu xử lý ảnh chất lượng cao.

---

## 5. `registry.json`

Mỗi bảng dữ liệu khai báo 1 entry gồm: `path`, `primary_key`, `name_columns`,
`role_columns`, `group_columns`, `pii_columns`, `hidden_columns`,
`display_columns`, `column_labels`, `categorical_filters`,
`disambiguating_columns`, `has_related_text_corpus`, `extra_instructions`.

Hiện có 2 bảng: `nganh` (danh sách ngành/điểm chuẩn/chỉ tiêu theo năm) và
`giangvien` (danh sách giảng viên, chức vụ, đơn vị công tác).

Khi thêm 1 bảng mới hoàn toàn:
- **Qua web UI** (`admin_api.py`, xem mục 6b): màn hình phân tích upload có form bắt Admin khai báo `description`, `name_columns`, `role_columns`/`group_columns`/`disambiguating_columns`, `pii_columns`/`hidden_columns`, `categorical_filters`, `has_related_text_corpus` NGAY lúc tạo bảng — registry.json được ghi đầy đủ, không cần sửa tay sau đó. Bảng ĐÃ CÓ cũng sửa lại được các field này bất cứ lúc nào qua nút "⚙️ Cấu hình cho chatbot" khi mở bảng đó.
- **Qua CLI thuần** (`structured_data_pipeline.py add ... --key-cols=...`, không qua web): hệ thống vẫn chỉ tự khởi tạo 1 entry cơ bản (thiếu `role_columns`/`group_columns`/`column_labels`/`categorical_filters` chi tiết) — cần bổ sung thủ công (hoặc mở lại bảng đó qua web UI để dùng form "⚙️ Cấu hình" điền tiếp) nếu muốn bảng đó tận dụng đầy đủ khả năng so khớp nâng cao của `multi_entity_matcher.py`.

---

## 6. API

```
POST /ask
Headers: X-API-Key: <API_AUTH_TOKEN>
Body:    {"prompt": "câu hỏi", "session_id": "tuỳ chọn"}
Response: {"content_markdown": "...", "citations": [...], "meta": {...},
           "session_id": "...", "status": "success"}

POST /admin/reload-index
Headers: X-API-Key: <API_AUTH_TOKEN>
Chức năng: nạp lại FAISS/BM25 và MultiEntityMatcher/registry/bảng dữ liệu
từ đĩa vào bộ nhớ mà không cần khởi động lại tiến trình.
conflict_detection.py và structured_data_pipeline.py tự gọi endpoint này
sau khi có thay đổi dữ liệu (qua biến API8923_BASE_URL).

GET /metadata
Trả về thông tin phiên bản dịch vụ:
{"name": "Tư vấn tuyển sinh", "description": "...", "version": "1.3.0",
 "developer": "Nhóm P.Thảo, Minh Thu"}
```

---

## 6b. Giao diện quản trị web (`admin_api.py` + `index.html`)

Ngoài API chatbot (`api8923.py`, mục 6), hệ thống có 1 máy chủ **quản trị**
riêng chạy song song, phục vụ giao diện web để upload/duyệt tài liệu, quản
lý bảng dữ liệu, và chạy test có kiểm soát — KHÔNG dùng để trả lời câu hỏi
người dùng cuối.

### Khởi động

```bash
mkdir -p frontend
cp index.html frontend/index.html      # admin_api.py phục vụ index.html từ thư mục frontend/ cạnh nó
python admin_api.py                    # mặc định cổng 8924 (đổi qua biến ADMIN_API_PORT)
```

Mở trình duyệt tới `http://<host>:8924/`. Máy chủ chatbot (`api8923.py`,
mặc định cổng 8923) cần chạy SONG SONG để các nút "Verify chatbot"/hỏi thử
trong giao diện quản trị hoạt động được.

Lần đầu mở giao diện, bấm **⚙ Cấu hình** (trên cùng bên phải) và điền:

| Trường | Giá trị |
|---|---|
| Admin URL | Địa chỉ `admin_api.py`, mặc định `http://127.0.0.1:8924` |
| Chat URL | Địa chỉ `api8923.py`, mặc định `http://127.0.0.1:8923` |
| API Key | PHẢI khớp đúng `API_AUTH_TOKEN` trong `.env` (dùng chung cho cả 2 máy chủ) |

Các giá trị này lưu trong `localStorage` của trình duyệt, không gửi lên máy
chủ nào khác ngoài chính `admin_api.py`/`api8923.py` đã khai.

### Các tab chính

- **Chatbot**: khung hỏi-đáp thử trực tiếp với `api8923.py` (gọi `/ask`) —
  dùng để kiểm tra ngay câu trả lời sau khi cập nhật dữ liệu, không cần rời
  trang.
- **Tài liệu (RAG)**: upload PDF/DOCX/DOC/TXT/MD, chạy từng bước (OCR →
  chuẩn bị Markdown → rà soát → kiểm tra xung đột), xem "Chờ kiểm tra xung
  đột", và mục ** Xung đột chéo với dữ liệu bảng** — nơi duyệt các trường
  hợp văn bản mới mâu thuẫn với bảng cấu trúc (giữ đoạn / loại đoạn / sửa
  đoạn / sửa giá trị bảng theo văn bản), thay cho việc phải trả lời qua
  terminal.
- **Dữ liệu bảng**: upload `.csv`/`.xlsx`/`.xls`, kéo-thả chọn khóa
  chính, bấm **Phân tích xung đột** để xem trước dữ liệu mới/trùng
  lặp/thay đổi + cảnh báo chéo với RAG trước khi ghi thật vào production
  (bảng mới hoàn toàn sẽ hiện thêm form khai báo `description`/
  `name_columns`/...). Mỗi bảng đã có, mở lên còn 2 nút: **Backup /
  Khôi phục** (xem và khôi phục về bất kỳ bản backup tự động nào) và **Cấu hình cho chatbot** (sửa lại toàn bộ field trong `registry.json` của
  bảng đó, kể cả `categorical_filters`).
- **Test cases**: chạy các case định nghĩa sẵn trong `test_cases.json`
  (nếu có), cùng 2 nút toàn cục ở đầu trang — xem mục 6c.

### Snapshot / Reset test (2 nút ở đầu mọi trang)

- **Snapshot**: chụp lại TOÀN BỘ trạng thái có thể bị 1 lượt test làm
  thay đổi — chunks + manifest (phía văn bản) VÀ toàn bộ bảng `.csv` +
  `registry.json` (phía có cấu trúc) — vào 1 thư mục có nhãn/thời gian
  trong `data/processed/conflict/snapshots/`.
- **Reset test**: tự tìm snapshot GẦN NHẤT và khôi phục lại TOÀN BỘ từ
  đó (cả văn bản và bảng), rồi rebuild FAISS 1 lần. Nếu KHÔNG có snapshot
  nào (quên bấm Snapshot trước khi test), chỉ dọn được văn bản có tiền tố
  `test_` (tương đương lệnh `cleanup-test`) — bảng cấu trúc SẼ KHÔNG được
  hoàn tác, giao diện sẽ cảnh báo rõ điều này trong log.

**Quy tắc vận hành:** LUÔN bấm Snapshot trước khi bắt đầu 1 lượt test,
bất kể test loại dữ liệu nào (văn bản hay bảng) — đây là cách DUY NHẤT đảm
bảo Reset test đưa được cả bảng cấu trúc về đúng trạng thái ban đầu.
Snapshot cũ hơn không bị xóa tự động nên không sợ mất, xem toàn bộ bằng
`python conflict_detection.py list-snapshots`.

### 6c. Bộ dữ liệu test mẫu và quy trình test cho người dùng cuối (thầy/giảng viên)

Thư mục `test/` (tự chuẩn bị, không nằm trong mã nguồn) gồm:

```
test/
├── test_co_cau_truc/
│   ├── test_giangvien_change.csv     # test cập nhật bảng ĐÃ CÓ ("giangvien") - có dòng thay đổi + trùng lặp
│   └── test_nganh_change.csv         # tương tự cho bảng "nganh"
└── test_phi_cau_truc/
    ├── khac_gia_tri/          test_c3a_v1.md, test_c3a_v2.md          # 2 văn bản khác NHAU về giá trị cùng 1 sự việc
    ├── khac_thoi_diem_pham_vi/ test_c2_v1.md, test_c2_v2.md            # khác biệt vì áp dụng ở thời điểm/phạm vi khác nhau (valid_from/valid_to)
    ├── khong_du_thong_tin/    test_c5_v1.md, test_c5_v2.md            # không đủ dữ kiện để kết luận trùng/xung đột
    ├── mau_thuan/             test_c4_v1.md, test_c4_v2.md            # mâu thuẫn thực sự, cần Admin quyết định
    ├── tri_thuc_moi/          test_a1_moi.md                          # hoàn toàn mới, không liên quan gì đã có
    └── trung_lap/             test_trung_lap_tuong_doi_*.pdf,
                                test_trung_lap_tuyet_doi_*.md          # trùng lặp tương đối (gần giống) và tuyệt đối (giống hệt)
```

Tất cả file đều đặt tên bắt đầu bằng `test_` — đúng tiền tố mà `cleanup-test`
(bước dự phòng khi không có snapshot) nhận diện, nên dù rơi vào trường hợp
nào ở mục 6b, các file này luôn được dọn đúng.

**Các bước chạy 1 lượt test đầy đủ:**

1. **📸 Snapshot** (đầu trang) — đặt nhãn dễ nhận, vd `truoc_test_<ngay>`.
2. Vào tab **Tài liệu (RAG)**, kéo-thả từng file trong `test_phi_cau_truc/*`
   vào khung upload, theo đúng thứ tự bạn muốn kiểm tra (khuyên chạy `v1`
   trước, `v2` sau, để thấy đúng bước phát hiện trùng lặp/xung đột với
   chính `v1` vừa lên production) → "Chi tiết" → chạy từng bước → "
   Check" → xem đúng kết quả có khớp với tên thư mục không (vd `mau_thuan/`
   phải ra kết quả mâu thuẫn, không phải "trùng lặp").
3. Vào tab **Dữ liệu bảng**, upload `test_giangvien_change.csv`/
   `test_nganh_change.csv`, chọn khóa chính, **Phân tích xung đột**, xem
   đúng số dòng thay đổi/cảnh báo chéo, chọn quyết định, **Áp dụng**.
4. Vào tab **Chatbot**, hỏi thử vài câu liên quan tới dữ liệu vừa test để
   xác nhận câu trả lời đúng như kỳ vọng.
5. **Reset test** (đầu trang) — xác nhận log báo "đã khôi phục từ
   snapshot '...'" (không phải nhánh cảnh báo "KHÔNG tìm thấy snapshot").
6. Hỏi lại đúng câu ở bước 4 lần nữa — câu trả lời phải trở về ĐÚNG như
   trước khi test (nếu vẫn nhắc tới nội dung/giá trị đã test ở bước 2-3,
   Reset chưa khôi phục hết — báo lại để kiểm tra thêm).

---

## 7. Câu lệnh vận hành

```bash
# Xử lý PDF
python pipeline_pdf.py ocr                          # OCR batch toàn bộ PDF trong data/raw/pdf
python pipeline_pdf.py ocr <duong_dan.pdf>           # OCR 1 file
python pipeline_pdf.py prepare [duong_dan.txt]       # gợi ý metadata
python pipeline_pdf.py generate [duong_dan_review.json]  # sinh markdown hoàn chỉnh
python pipeline_pdf.py finalize [duong_dan_review.json]  # chunk cuối cùng

# Xử lý DOCX/DOC/TXT/MD
python pipeline_docx_txt.py docx [file]
python pipeline_docx_txt.py txt [file]
python pipeline_docx_txt.py md [file]
python pipeline_docx_txt.py list
python pipeline_docx_txt.py detail <base_name>

# Phát hiện xung đột dữ liệu phi cấu trúc
python conflict_detection.py bootstrap                    # đăng ký lần đầu toàn bộ chunk có sẵn (chỉ chạy 1 lần)
python conflict_detection.py check-file <duong_dan.md> [base_name]
python conflict_detection.py check <base_name>
python conflict_detection.py list-pending
python conflict_detection.py status
python conflict_detection.py prune
python conflict_detection.py cleanup-test [prefix]
python conflict_detection.py reset-cache
python conflict_detection.py review-auto <base_name>
python conflict_detection.py rebuild-index
python conflict_detection.py snapshot [nhãn]              # giờ chụp CẢ bảng cấu trúc + registry.json, không chỉ RAG
python conflict_detection.py list-snapshots
python conflict_detection.py latest-snapshot              # in tên snapshot gần nhất (rỗng nếu chưa có) - dùng bởi admin_api.py cho nút Reset test
python conflict_detection.py restore-snapshot <ten>       # khôi phục CẢ bảng cấu trúc + registry.json
python conflict_detection.py revert-doc <base_name>
python conflict_detection.py list-pending-cross-web [--base-name <ten>] [--out <path>]     # ít dùng tay - admin_api.py gọi để hiện "Xung đột chéo" trên web
python conflict_detection.py apply-pending-cross-web <base_name> --decisions <path> --out <path>  # ít dùng tay - dùng qua web UI

# Xử lý dữ liệu có cấu trúc
python structured_data_pipeline.py add <duong_dan> <ten_bang> [--key-cols=A,B] [--auto safe|replace]
python structured_data_pipeline.py status
python structured_data_pipeline.py discard-staging <ten_file>
python structured_data_pipeline.py list-tables
python structured_data_pipeline.py remove-table <ten_bang>
python structured_data_pipeline.py list-backups <ten_bang>
python structured_data_pipeline.py restore-backup <ten_bang> <ten_file_backup>
# Các lệnh *-web dưới đây ít dùng tay - admin_api.py gọi ngầm khi dùng qua giao diện web (mục 6b);
# hữu ích khi cần tự động hoá bằng script riêng ngoài web UI.
python structured_data_pipeline.py analyze-web <duong_dan> <ten_bang> [--key-cols=A,B] [--force-new] --out <path>
python structured_data_pipeline.py apply-web <ten_bang> --decisions <path> --out <path>
python structured_data_pipeline.py recheck-cross-web <ten_bang> <staging_token> --name-cols=A[,B] --out <path>
python structured_data_pipeline.py get-config-web <ten_bang> --out <path>
python structured_data_pipeline.py update-config-web <ten_bang> --metadata <path> --out <path>

# Khởi tạo FAISS lần đầu (chạy 1 lần trước khi dùng api8923.py)
python build_vectorstore.py

# Khởi động máy chủ chatbot
python api8923.py

# Khởi động máy chủ quản trị + giao diện web (mục 6b) - chạy song song với api8923.py
python admin_api.py
```

---

## 8. Quyết định kiến trúc cần lưu ý trước khi chỉnh sửa

1. **Lưu trữ dữ liệu có cấu trúc hiện là file `.csv`** trong
   `data/processed/tables/`. Đây là giải pháp tạm thời trong lúc chưa tích
   hợp cơ sở dữ liệu chính thức. Toàn bộ thao tác đọc/ghi được cô lập trong
   2 hàm `load_table_df()`/`save_table_df()` của `structured_conflict_detection.py`
   — khi chuyển sang cơ sở dữ liệu thật, chỉ cần thay thế 2 hàm này, phần
   logic còn lại (so khớp, đối chiếu chéo) làm việc thuần trên `DataFrame`
   nên không cần sửa thêm.
2. **Toàn bộ LLM tự host qua Ollama**, trừ bước OCR PDF tiếp tục dùng
   `gpt-4.1-mini` vì yêu cầu xử lý ảnh chất lượng cao.
3. **Cơ chế duyệt tự động có kiểm soát**: quan hệ nội dung dạng "bổ sung
   thông tin" (không xung đột) được duyệt tự động, không cần xác nhận thủ
   công cho từng trường hợp — mọi quyết định vẫn được ghi log đầy đủ và có
   thể xem lại/ghi đè qua lệnh `review-auto`. Lựa chọn này nhằm đảm bảo khả
   năng mở rộng khi số lượng văn bản/bảng dữ liệu lớn.
4. **Metadata hiệu lực theo thời gian** (`valid_from`/`valid_to`): gắn cho
   các đoạn nội dung được xác định là khác biệt về thời điểm/phạm vi áp
   dụng (không phải mâu thuẫn thực sự). `api8923.py` đọc 2 trường này khi
   tổng hợp câu trả lời để ưu tiên đúng phiên bản đang có hiệu lực.
5. **Đồng bộ dữ liệu real-time**: sau mọi thay đổi dữ liệu (nội dung hoặc
   bảng), hệ thống tự gọi `/admin/reload-index` để máy chủ đang chạy cập
   nhật ngay lập tức, không cần khởi động lại. Yêu cầu máy chủ `api8923.py`
   phải đang hoạt động tại thời điểm gọi, nếu không hệ thống sẽ cảnh báo rõ
   ràng thay vì âm thầm bỏ qua.
6. **Bảo vệ quyết định thủ công**: khi 1 nội dung mới được đối chiếu với
   nhiều văn bản hiện có trong cùng 1 lượt xử lý, quyết định loại bỏ có chủ
   đích của người quản trị ở 1 trường hợp không bị các trường hợp khác tự
   động ghi đè.
7. **So khớp thực thể (Tầng 0)** chạy trước mọi lời gọi LLM cho các câu hỏi
   liên quan tới bảng dữ liệu — chỉ khi không khớp được gì mới chuyển sang
   agent (pandas dataframe agent qua Ollama).
8. **Quy ước đặt tên file backup tự động** (`<tên>.bak_YYYYMMDD_HHMMSS...`,
   tạo trước mọi lần ghi đè bảng/chunk/registry.json): LUÔN đặt trong 1 thư
   mục riêng KHÔNG bị các hàm liệt kê "văn bản/bảng đang có" quét tới (vd
   `data/processed/chunks/_backups/` cho chunk, cùng cấp cho bảng chỉ vì
   `/admin/tables` liệt kê qua `registry.json` chứ không glob thư mục nên
   an toàn). Vi phạm quy tắc này (từng xảy ra: backup chunk từng nằm ngay
   trong `chunks/`) khiến file backup bị hiểu lầm thành văn bản MỚI trong
   danh sách "chờ kiểm tra xung đột", xóa văn bản gốc không dọn được các
   bản backup này (mỗi lần sửa lại đẻ thêm 1 bản) — `list_all_chunk_base_names()`
   trong `conflict_detection.py` có lọc cứng thêm theo mẫu `.bak_` để phòng
   sai sót tương tự ở các script khác, nhưng cách phòng CHẮC CHẮN vẫn là
   không bao giờ tạo backup vào thư mục bị quét.
9. **📸 Snapshot/🔄 Reset test (mục 6b) hoàn tác được CẢ bảng cấu trúc**,
   không chỉ dữ liệu RAG — bất kỳ cơ chế "test" mới thêm sau này (vd 1 loại
   dữ liệu thứ 3) đều nên nối vào đúng 2 hàm `snapshot_production()`/
   `restore_snapshot()` trong `conflict_detection.py` để giữ tính nhất quán
   "1 nút Reset, hoàn tác mọi loại dữ liệu", tránh tình trạng Admin bấm
   Reset xong vẫn còn sót dữ liệu test ở 1 nơi khác.

---

## 9. Tài liệu kiểm thử đính kèm

- Báo cáo tổng hợp công việc và kết quả kiểm thử: `BAO_CAO_TIEN_DO_VA_KIEM_THU`

---

## 10. Cài đặt

```bash
pip install -r requirements.txt
```

Xem `requirements.txt` đính kèm để biết danh sách đầy đủ thư viện cần cài.