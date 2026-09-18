# BÁO CÁO TIẾN ĐỘ VÀ KIỂM THỬ HOÀN CHỈNH

**Hệ thống:** Chatbot tư vấn tuyển sinh/nhân sự NEU – RAG + Dữ liệu có cấu trúc  
**Ngày cập nhật:** 16/09/2026  
**Phạm vi báo cáo:** kiểm thử phát hiện và xử lý xung đột dữ liệu; đối chiếu chéo RAG ↔ dữ liệu có cấu trúc; kiểm thử chatbot bằng 101 câu hỏi thực tế.

---

## 1. KIỂM THỬ TRƯỜNG HỢP XUNG ĐỘT DỮ LIỆU

> Đây là nội dung trọng tâm của báo cáo. Mục tiêu không chỉ kiểm tra hệ thống có “phát hiện khác nhau” hay không, mà kiểm tra toàn bộ vòng đời của một xung đột: **phát hiện → phân loại → hỏi người quản trị khi cần → áp dụng quyết định → bảo toàn trạng thái → rebuild/reload → kiểm tra lại bằng chatbot**.

### 1.1. Phạm vi và nguyên tắc kiểm thử

Hệ thống xử lý hai nguồn dữ liệu:

1. **Dữ liệu phi cấu trúc (RAG):** các văn bản/quy chế/quy định được chunk hóa và lập chỉ mục.
2. **Dữ liệu có cấu trúc:** các bảng CSV như `nganh.csv`, `giangvien.csv`.
3. **Đối chiếu chéo:** một đoạn RAG có thể mâu thuẫn với một dòng dữ liệu bảng và ngược lại.

Quy trình cập nhật được thiết kế theo hướng mọi thay đổi quan trọng phải có quyết định rõ ràng, ghi log và sau đó được kiểm tra lại trên chatbot. `README.md` cũng xác định cơ chế duyệt tự động có kiểm soát, metadata hiệu lực theo thời gian và đồng bộ real-time sau thay đổi dữ liệu. 

### 1.2. Các loại xung đột được kiểm thử

Hệ thống hiện phân biệt các quan hệ chính:

| Quan hệ | Ý nghĩa | Cách xử lý |
|---|---|---|
| `bo_sung` | Nội dung mới bổ sung, không mâu thuẫn với nội dung cũ | Có thể tự động duyệt |
| `khac_thoi_diem_pham_vi` | Khác nhau do thời điểm/phạm vi áp dụng | Hỏi Admin; nếu giữ cả hai thì gắn `valid_from`/`valid_to` |
| `khac_gia_tri` | Cùng đối tượng nhưng giá trị khác nhau | Admin quyết định giữ mới, giữ cũ, giữ cả hai hoặc xử lý ở cấp đoạn tùy luồng |
| `mau_thuan` | Hai nội dung phủ định/mâu thuẫn trực tiếp nhau | Phải đưa ra để Admin xử lý |
| `khong_du_thong_tin` | Không đủ bằng chứng để kết luận quan hệ | Có thể giữ trạng thái chưa chắc chắn để không tự động loại dữ liệu |

Việc dùng `valid_from`/`valid_to` không nhằm coi hai dữ liệu khác nhau là “mâu thuẫn”, mà để biểu diễn các phiên bản đúng ở các khoảng thời gian khác nhau. Khi chatbot trả lời câu hỏi hiện tại, `api8923.py` sử dụng thông tin hiệu lực theo thời gian để chọn đúng phiên bản. 

---

## 2. KIỂM THỬ XUNG ĐỘT Ở CẤP VĂN BẢN

### A1 — Không có ứng viên xung đột

**Mục tiêu:** kiểm tra văn bản hoàn toàn mới không bị gán nhầm vào văn bản đã có.

**Kết quả:** ✅ Đạt.

Sau khi sửa nhánh trước đây có thể bỏ qua bước đối chiếu với dữ liệu cấu trúc, hệ thống xử lý được trường hợp không có ứng viên và tiếp tục thực hiện các bước đối chiếu cần thiết.

### A2a — Trùng khớp tuyệt đối → thay thế toàn bộ

**Mục tiêu:** khi file mới thực chất là phiên bản thay thế của file cũ, kiểm tra toàn bộ văn bản cũ có được thay thế đúng hay không.

**Kết quả:** ✅ Đạt.

### A2b — Trùng khớp tuyệt đối → giữ bản cũ

**Mục tiêu:** Admin từ chối bản mới và giữ nguyên dữ liệu đang có.

**Kết quả:** ✅ Đạt.

Trong quá trình test, file mới không được đăng ký trở thành tri thức chính; trạng thái file test được dọn bằng `cleanup-test`.

### A2c — Trùng khớp tuyệt đối → giữ cả hai

**Mục tiêu:** cho phép hai văn bản cùng tồn tại độc lập khi Admin xác định chúng đều cần được giữ.

**Kết quả:** ✅ Đạt.

Đã kiểm tra lại bằng chatbot và xác nhận chatbot có thể truy xuất đúng cả hai nguồn.

### A3 — Một file mới trùng nhiều file cũ

**Mục tiêu:** kiểm tra trường hợp một văn bản mới đồng thời khớp với nhiều văn bản cũ.

**Kết quả:** ☐ Chưa có ca thực tế để xác nhận đầy đủ.

Thiết kế đã được sửa để `find_exact_duplicate()` trả về **danh sách** các văn bản trùng thay vì chỉ một văn bản, do đó hệ thống đã có cơ chế hỏi riêng từng ứng viên. Tuy nhiên cần thêm một ca test thực tế để xác nhận toàn bộ luồng.

---

## 3. KIỂM THỬ XUNG ĐỘT Ở CẤP FILE / THỰC THỂ

### B1 — Cùng thực thể, nội dung thay đổi

**Tình huống:** file mới cùng nói về một thực thể nhưng thay đổi ban chủ nhiệm khoa.

**Kết quả:** ✅ Đạt.

Điểm quan trọng được xác nhận là cơ chế `cung_thuc_the` giúp phân biệt **cùng thực thể** với trường hợp chỉ có văn phong/cấu trúc câu giống nhau. Trong test, 5 khoa khác có cách viết tương tự đã được tự động loại khỏi danh sách ứng viên vì không phải cùng đối tượng.

### B2 — Hai thực thể khác nhau nhưng cấu trúc nội dung tương tự

**Tình huống:** Lab AI và Lab Robotics.

**Kết quả:** ✅ Đạt.

Hai văn bản được giữ độc lập thay vì bị coi là xung đột chỉ vì có cấu trúc câu tương tự.

### B3 — Cùng thực thể, Admin chủ động giữ song song

**Tình huống:** thông tin hợp tác quốc tế Hàn Quốc/Nhật Bản.

**Kết quả:** ✅ Đạt.

Hệ thống cho phép Admin giữ đồng thời hai nội dung thay vì tự động coi một nội dung là sai.

---

## 4. KIỂM THỬ XUNG ĐỘT Ở CẤP ĐOẠN — PHẦN QUAN TRỌNG NHẤT CỦA LOGIC XUNG ĐỘT

### C1 — `bo_sung`

**Tình huống:** nội dung mới bổ sung thông tin, không phủ định hoặc thay thế nội dung cũ.

**Kết quả:** ✅ Đạt.

Hệ thống tự động duyệt và không yêu cầu Admin xử lý thủ công từng trường hợp. Đây là chủ trương thiết kế để tránh số lượng thao tác tăng quá lớn khi kho dữ liệu mở rộng. Các quyết định tự động vẫn được ghi log và có thể xem lại/ghi đè bằng `review-auto`. 

### C2 — `khac_thoi_diem_pham_vi`

**Tình huống:** học phí năm học 2023–2024 và 2024–2025.

**Kết quả:** ✅ Đạt.

Hệ thống:

- nhận diện đây là khác biệt về thời điểm/phạm vi;
- gắn `valid_from`/`valid_to` khi hai phiên bản cùng được giữ;
- khi hỏi không nêu năm, chatbot chọn nội dung đang có hiệu lực;
- khi hỏi rõ năm cũ, chatbot trả lại phiên bản lịch sử tương ứng.

Đây là một test quan trọng vì nếu chỉ dùng RAG thông thường, hai phiên bản có thể cùng được truy xuất và câu trả lời có nguy cơ trộn dữ liệu.

### C3 — `khac_gia_tri`

**Tình huống:** cùng đối tượng nhưng giá trị mới khác giá trị đang có.

**Các nhánh đã test:**

- thay thế;
- giữ hai phiên bản ở cấp file;
- xử lý/thay thế ở cấp đoạn;
- giữ bản cũ ở cấp đoạn;
- giữ cả hai ở cấp đoạn.

**Kết quả:** ✅ Các nhánh đã thực hiện đều đạt.

Đặc biệt, hệ thống đã được kiểm tra với trường hợp chỉ một đoạn có thông tin sai khác. Thay vì buộc Admin loại cả văn bản, luồng cấp đoạn cho phép xử lý đúng phạm vi xung đột.

### C3d/e — Giữ bản cũ / giữ cả hai ở cấp đoạn

**Kết quả:** ✅ Đạt.

Trong quá trình test phát hiện một lỗi thực tế: văn bản đã bị loại hết chunk vẫn bị coi là còn tồn tại khi kiểm tra trùng khớp tuyệt đối với văn bản khác.

Lỗi đã được sửa bằng kiểm tra `_co_active_chunk()`. Đây là một lỗi quan trọng vì nếu không sửa, trạng thái logic của kho tri thức có thể khác trạng thái thực tế của các chunk đang hoạt động.

### C4 — `mau_thuan`

**Tình huống:** quy định về IELTS bắt buộc và không bắt buộc.

**Kết quả:** ✅ Đạt.

Hệ thống nhận diện đây là mâu thuẫn thực sự thay vì coi đơn thuần là hai thông tin bổ sung.

### C5 — `khac_gia_tri` nhưng không phải khác thời điểm

**Tình huống:** hợp tác Hàn Quốc/Nhật Bản.

**Kết quả:** ✅ Đạt.

Test xác nhận LLM không lạm dụng nhãn `khac_thoi_diem_pham_vi` chỉ vì hai đoạn có giá trị khác nhau. Không có yếu tố thời gian thật thì hệ thống không tự gắn quan hệ thời gian.

### C6 — `khong_du_thong_tin`

**Mục tiêu:** kiểm tra trường hợp bằng chứng không đủ để kết luận xung đột.

**Kết quả:** ☐ Chưa quan sát được ca thực tế.

Trong các lần test hiện có, LLM đều phân loại được vào một trong các quan hệ còn lại. Vì vậy chưa đủ dữ liệu để kết luận độ tin cậy của nhánh `khong_du_thong_tin`.

---

## 5. KIỂM THỬ ĐỐI CHIẾU CHÉO RAG ↔ DỮ LIỆU CÓ CẤU TRÚC

Đây là phần mở rộng quan trọng của hệ thống. README mô tả `conflict_detection.py` thực hiện đối chiếu RAG với bảng, trong khi `structured_data_pipeline.py` thực hiện luồng cập nhật bảng và đối chiếu ngược bảng → RAG. 

### 5.1. RAG → bảng

**Tình huống thực tế:** thông tin về giảng viên Ngô Đức Nghị thay đổi từ chức vụ **Phó giám đốc → Giám đốc**.

**Kết quả:** ✅ Đạt sau khi sửa lỗi JSON.

Hệ thống phát hiện được sự khác biệt giữa nội dung văn bản và bản ghi có cấu trúc, hiển thị cho Admin và thực hiện đúng cả ba lựa chọn xử lý đã test.

### 5.2. Bảng → RAG

**Tình huống thực tế:** thông tin về giảng viên Bùi Đức Thọ thay đổi từ **Giám đốc → Phó Giám đốc**.

**Kết quả:** ✅ Đạt 

Hệ thống phát hiện được sự khác biệt giữa nội dung văn bản và bản ghi có cấu trúc, hiển thị cho Admin và thực hiện đúng bốn lựa chọn xử lý đã test.

---

## 6. KIỂM THỬ DỮ LIỆU CÓ CẤU TRÚC

| Trường hợp | Kết quả |
|---|---|
| Bảng đã tồn tại + dữ liệu mới | ✅ Đạt |
| Bảng đã tồn tại + thay đổi → giữ mới | ✅ Đạt |
| Bảng đã tồn tại + thay đổi → giữ cũ | ✅ Đạt |
| Bảng đã tồn tại + thay đổi → giữ cả hai | ✅ Đạt |
| Tạo bảng mới hoàn toàn | ✅ Đạt |
| Cảnh báo bảng mới trùng toàn bộ cột với bảng cũ | ✅ Đạt |
| Cảnh báo khóa không đủ phân biệt dòng | ✅ Đạt |
| Đối chiếu bảng → RAG, 4 lựa chọn + xác nhận cuối | ✅ Đạt |

Một lỗi dtype quan trọng cũng đã được phát hiện: mã ngành ở file mới có thể là `int64` trong khi production là `str`, khiến so khớp khóa sai và dữ liệu bị coi nhầm là dữ liệu mới. Lỗi đã được xử lý bằng chuẩn hóa kiểu dữ liệu. Ngoài ra đã sửa lỗi kiểu dữ liệu của `valid_from`/`valid_to` và lỗi `AttributeError` khi xử lý record dạng dictionary.

---

## 7. BẢO TOÀN QUYẾT ĐỊNH CỦA ADMIN VÀ AN TOÀN KHI TEST

Trong trường hợp một chunk mới được đối chiếu với nhiều ứng viên, đã từng phát hiện tình trạng quyết định của Admin ở một ứng viên có thể bị quyết định của ứng viên sau ghi đè.

Hệ thống hiện sử dụng cơ chế theo dõi `chunks_admin_da_loai` xuyên suốt một lượt `check-file` để bảo vệ quyết định thủ công. Đây là thay đổi quan trọng vì nó đảm bảo hành vi của hệ thống phù hợp với quyết định mà Admin đã đưa ra. 

Các công cụ phục vụ an toàn và khôi phục gồm:

- `snapshot`
- `restore-snapshot`
- `revert-doc`
- `cleanup-test`
- `reset-cache`
- `review-auto`

Trong test thực tế, `cleanup-test` ban đầu không hoàn tác đầy đủ side-effect khi test làm thay đổi trạng thái chunk của văn bản thật. Thiếu sót này đã dẫn tới việc bổ sung cơ chế snapshot/restore/revert và cảnh báo rõ ràng.

---

# 8. KẾT LUẬN

Qua quá trình cập nhật và kiểm thử, hệ thống đã hoàn thiện phần lõi của quy trình **phát hiện – phân loại – xử lý – khôi phục – đồng bộ xung đột dữ liệu** cho cả RAG và dữ liệu có cấu trúc.

Điểm quan trọng nhất của phiên bản hiện tại là hệ thống không còn chỉ kiểm tra “hai đoạn có giống nhau hay không”, mà đã phân biệt được:

- nội dung bổ sung;
- khác nhau do thời điểm/phạm vi;
- khác giá trị;
- mâu thuẫn thực sự;
- cùng thực thể và khác thực thể;
- xung đột giữa văn bản và bảng dữ liệu;
- các trường hợp cần Admin quyết định;
- các trường hợp có thể tự động duyệt.

Các test xung đột chính ở cấp văn bản, file và đoạn đã được thực hiện; các lỗi thực tế quan trọng trong parser JSON, dtype, trạng thái chunk, entity matching, xử lý exception và dataframe rỗng đã được phát hiện và sửa.

Đối với bộ **101 câu hỏi**, hệ thống đã xử lý thành công về mặt API **101/101 request với HTTP 200**, đồng thời các nhóm Bảo mật, Giảng viên và Ngoài phạm vi đã có kết quả rà soát định lượng trong tài liệu kiểm thử. Tuy nhiên, báo cáo không gán một tỷ lệ accuracy tổng duy nhất cho 101 câu khi chưa có đáp án chuẩn cho từng câu, nhằm tránh biến chỉ số kỹ thuật thành một kết luận không có đủ căn cứ.


---

## PHỤ LỤC — CẤU HÌNH VÀ FILE LIÊN QUAN

Các file chính trong phạm vi phiên bản:

- `api8923.py`
- `conflict_detection.py`
- `structured_conflict_detection.py`
- `structured_data_pipeline.py`
- `multi_entity_matcher.py`
- `README.md`
- `ket_qua_test.xlsx`

Kiến trúc hiện tại sử dụng FAISS + BM25 + MultiEntityMatcher + pandas agent/RAG; các model chính được self-host qua Ollama, trong khi bước OCR PDF tiếp tục sử dụng OpenAI theo thiết kế hiện tại. 

