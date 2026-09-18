"""
conflict_detection.py — Module PHÁT HIỆN & XỬ LÝ XUNG ĐỘT dữ liệu phi cấu trúc.

Đây là bước còn thiếu giữa 'pipeline_pdf.py finalize' (sinh ra 1 file chunk mới
trong data/processed/chunks/) và 'build_vectorstore.py' (embed TOÀN BỘ chunk
đang active vào FAISS production). Không có bước này, 1 văn bản mới/sửa đổi
sẽ được nạp thẳng vào production mà không ai kiểm tra xem nó có TRÙNG hay
MÂU THUẪN với dữ liệu đã có hay không.

QUY ƯỚC "PENDING" — không cần thư mục staging riêng:
    'pipeline_pdf.py finalize' vẫn ghi chunk mới thẳng vào data/processed/chunks/
    như cũ. Module này coi 1 file .json trong đó là "văn bản mới cần kiểm
    tra" nếu tên file (base_name) CHƯA có mặt trong manifest.json (nơi ghi
    nhận toàn bộ văn bản ĐÃ được duyệt/đưa vào production). Nhờ vậy không
    phải sửa pipeline_pdf.py, cũng không sinh thêm 1 thư mục chunk dễ gây lẫn.

CÁC LỆNH:
    python conflict_detection.py bootstrap
        Lần đầu chạy module này: đăng ký TOÀN BỘ file .json đang có sẵn
        trong chunks/ vào manifest với trang_thai_van_ban=active (coi như đã
        được duyệt từ trước, không hồi tố kiểm tra xung đột cho dữ liệu cũ).

    python conflict_detection.py status
        CHẨN ĐOÁN NHANH: có bao nhiêu văn bản đã đăng ký production, bao
        nhiêu chunk đang active, có đang quên chạy 'bootstrap' không. Chạy
        lệnh này TRƯỚC khi báo lỗi "không phát hiện được xung đột". Lệnh này
        LUÔN đối chiếu manifest.json với danh sách file .json hiện có trong
        chunks/ - nếu bạn vừa xóa 1 file chunk thủ công, 'status' sẽ báo văn
        bản đó là "mồ côi" (có trong manifest nhưng file đã mất) thay vì vẫn
        đếm nó như đang active.

    python conflict_detection.py prune
        Dọn khỏi manifest.json các văn bản "mồ côi" nói trên (file chunk đã
        bị xóa khỏi chunks/ nhưng manifest chưa cập nhật theo). Chạy sau khi
        'status' báo có văn bản mồ côi, rồi chạy lại 'rebuild-index'.

    python conflict_detection.py cleanup-test [prefix] [--no-rebuild]
        Dọn SẠCH mọi văn bản test (mặc định prefix 'test_') khỏi markdown/,
        chunks/, manifest.json - kể cả văn bản test CHƯA từng vào manifest
        (case Admin chọn "giữ bản cũ" ở trùng lặp tuyệt đối). Tự động chạy
        lại 'rebuild-index' để FAISS production hết dữ liệu test, TRỪ KHI
        truyền '--no-rebuild' (dùng khi nơi gọi sẽ tự chạy đúng 1 lần
        'rebuild-index' ở cuối 1 chuỗi lệnh dài hơn, vd sau 'prune'). CẢNH
        BÁO thêm danh sách văn bản THẬT có thể đã bị đổi trạng thái chunk
        trong lúc test (KHÔNG tự hoàn tác phần này - xem 'snapshot'/'revert-doc').

    python conflict_detection.py snapshot [nhãn]
        Chụp lại TOÀN BỘ chunks/ + manifest.json - chạy TRƯỚC khi bắt đầu 1
        lượt test để có đường lùi CHẮC CHẮN (cleanup-test không đủ, vì nó
        không hoàn tác được thay đổi trạng thái trên văn bản THẬT).

    python conflict_detection.py list-snapshots
        Liệt kê các snapshot đã chụp.

    python conflict_detection.py restore-snapshot <ten>
        Khôi phục ĐÚNG trạng thái chunks/ + manifest.json đã chụp - tự chụp
        1 snapshot của trạng thái hiện tại trước khi ghi đè, phòng bấm nhầm.

    python conflict_detection.py revert-doc <base_name>
        Đưa toàn bộ chunk của 1 văn bản THẬT về lại active (dùng khi KHÔNG
        có snapshot và biết rõ văn bản này bị ảnh hưởng do test).

    python conflict_detection.py review-auto <base_name>
        Soát lại các quyết định đã TỰ ĐỘNG DUYỆT (bo_sung/khac_thoi_diem_pham_vi
        - không hỏi Admin lúc check-file) của ĐÚNG 1 văn bản (giống cách
        check-file nhận đúng 1 base_name). Với mỗi quyết định, Enter để giữ
        mặc định, hoặc gõ 'cu'/'moi' để ghi đè lại thành giữ bản cũ/bản mới.
        Đây là đường soát lại cho quyết định tự động, không phải bắt xác
        nhận từng cái ngay lúc check-file (sẽ không scale khi nhiều văn bản)
        - thiết kế để khi lên web, ánh xạ thẳng thành nút "Xem thêm" trên
        từng văn bản.

    python conflict_detection.py reset-cache
        Xóa embedding_cache.pkl, buộc embed lại TOÀN BỘ chunk ở lần chạy kế
        tiếp. Cache đã tự nhận biết đổi EMBED_MODEL/OLLAMA_SERVER và tự bỏ
        qua cache cũ không khớp model hiện tại, nên BÌNH THƯỜNG không cần
        chạy lệnh này bằng tay - chỉ dùng khi muốn chắc chắn tuyệt đối hoặc
        nghi ngờ cache hỏng. KHÔNG đụng tới manifest.json/quyết định đã duyệt.

    python conflict_detection.py list-pending
        Liệt kê các file .json trong chunks/ CHƯA có trong manifest (tức là
        văn bản mới, đang chờ kiểm tra xung đột).

    python conflict_detection.py check <base_name>
        Chạy TOÀN BỘ quy trình (checksum -> vector search -> LLM phân tích ->
        Admin quyết định) cho 1 văn bản mới. <base_name> là tên file KHÔNG
        đuôi, phải tồn tại đồng thời ở:
            data/processed/chunks/<base_name>.json   (từ 'pipeline_pdf.py finalize')
            data/processed/markdown/<base_name>.md   (từ 'pipeline_pdf.py generate')

    python conflict_detection.py rebuild-index
        Sau khi duyệt xong 1 hoặc nhiều văn bản, embed lại TOÀN BỘ chunk
        đang trang_thai=active vào FAISS production (tương đương chạy lại
        build_vectorstore.py, nhưng có LỌC theo trạng thái hiệu lực). Sau
        khi ghi FAISS mới, TỰ ĐỘNG gọi /admin/reload-index của api8923.py
        (endpoint mới) để chatbot ĐANG CHẠY nạp lại ngay, không cần khởi
        động lại process - đóng kẽ hở "chatbot vẫn trả lời bằng dữ liệu cũ
        dù đã duyệt thay thế". Cần .env có API_AUTH_TOKEN khớp với
        api8923.py (mặc định gọi http://127.0.0.1:8923, đổi bằng biến môi
        trường API8923_BASE_URL nếu chạy khác cổng/máy). check-file/check
        cũng TỰ HỎI có muốn chạy bước này ngay sau khi có nội dung bị thay
        thế/vô hiệu hóa, không chỉ dựa vào bạn nhớ chạy tay.

    python conflict_detection.py check-file <duong_dan.md> [base_name]
        TIỆN ÍCH TEST: đưa thẳng 1 file .md bất kỳ vào (không cần chạy qua
        prepare/generate của pipeline_pdf.py trước). Tự chunk bằng logic sao
        chép lại từ pipeline_pdf.py, ghi tạm vào production dưới tên base_name
        (mặc định = tên file bỏ đuôi), rồi chạy 'check' luôn cho file đó.
        CHỈ dùng để thử luồng xung đột - metadata sinh ra là placeholder,
        KHÔNG dùng để nạp văn bản thật (văn bản thật vẫn phải đi đủ pipeline
        ocr -> prepare -> generate -> finalize để có metadata chính xác).

PHẠM VI: module này chỉ xử lý xung đột giữa VĂN BẢN MỚI và VĂN BẢN PHI CẤU
TRÚC đã có (các bước 1-8 trong sơ đồ nghiệp vụ: checksum -> trùng tuyệt đối
-> vector search theo chunk -> LLM phân tích -> Admin quyết định cấp
file/đoạn). Bước đối chiếu với DỮ LIỆU CÓ CẤU TRÚC (bảng điểm chuẩn, bảng
giảng viên...) là 1 module RIÊNG (vd structured_conflict.py), CHƯA triển
khai ở đây — tham số `on_chunk_approved` của process_new_document() là điểm
nối để module đó chạy tiếp mà không cần sửa file này.

Cài đặt thêm (ngoài các gói pipeline_pdf.py/build_vectorstore.py đã cần):
    pip install numpy

ĐIỂM TÍCH HỢP HỆ THỐNG SAU NÀY (đọc trước khi đổi nơi lưu trữ / cách xác định
văn bản gốc) — hiện TOÀN BỘ module này định danh 1 văn bản bằng "base_name"
(tên file không đuôi, suy ra từ quy ước đặt tên trùng stem giữa các thư mục
raw/txt/markdown/chunks của pipeline_pdf.py). pipeline_pdf.py KHÔNG lưu đường dẫn
file gốc vào metadata ở đâu cả - mọi liên kết giữa chunk <-> file gốc đều là
NGẦM ĐỊNH qua tên file. Khi hệ thống thật đổi cách lưu trữ (DB, object
storage, thêm ID ổn định...), CHỈ cần sửa các hàm sau, phần còn lại của
module gọi qua chúng nên không phải sửa rải rác:
    - list_all_chunk_base_names()  -> nguồn liệt kê "văn bản nào đang có".
    - get_chunk_path() / get_markdown_path()  -> vị trí file chunk/markdown.
    - ensure_chunk_ids_and_status(..., duong_dan_goc=...)  -> nơi gắn ID/
      đường dẫn file gốc THẬT (nếu hệ thống cung cấp) vào metadata mỗi
      chunk, thay vì chỉ có "source_file"=base_name như hiện tại.
    - "source_file" trong metadata mỗi chunk  -> hiện = base_name. Đây là
      khóa dùng xuyên suốt module để nhóm/tra cứu "chunk này thuộc file
      nào" (find_candidate_files, review_candidate_file...). Nếu hệ thống
      thật đổi khóa định danh (vd dùng UUID thay vì tên file), cần đổi giá
      trị gán ở ensure_chunk_ids_and_status() VÀ chỗ build_vectorstore.py
      gán "source_file" cho nhất quán (2 nơi phải cùng 1 quy ước).
"""

import os
import re
import sys
import json
import time
import hashlib
import pickle
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
from dotenv import load_dotenv
from openai import OpenAI  # optional - chỉ dùng nếu CONFLICT_LLM_MODEL trỏ tới model OpenAI thật
from langchain_ollama import ChatOllama, OllamaEmbeddings

load_dotenv()

# ============================================================
# CẤU HÌNH — dùng lại đúng quy ước thư mục của pipeline_pdf.py/build_vectorstore.py.
# KHÔNG import 2 file đó, để tránh kéo theo phụ thuộc không cần thiết (giống
# lý do build_vectorstore.py đã nêu với pdf2image).
# ============================================================

BASE = Path(__file__).resolve().parent
output_md_folder = BASE / "data" / "processed" / "markdown"
output_chunk_folder = BASE / "data" / "processed" / "chunks"          # = "production" theo manifest
conflict_folder = BASE / "data" / "processed" / "conflict"
conflict_folder.mkdir(parents=True, exist_ok=True)

MANIFEST_PATH = conflict_folder / "manifest.json"
DECISION_LOG_PATH = conflict_folder / "decision_log.jsonl"
EMBEDDING_CACHE_PATH = conflict_folder / "embedding_cache.pkl"

OLLAMA_SERVER = os.getenv("OLLAMA_SERVER", "http://10.2.13.58:8037/ollama")
# Server GPU yêu cầu header xác thực qua proxy 8037 - đồng bộ với
# build_vectorstore.py/api8923.py.
OLLAMA_SECKEY = os.getenv("OLLAMA_SECKEY", "research")
OLLAMA_CLIENT_KWARGS = {"headers": {"x-ollama-seckey": OLLAMA_SECKEY}}
EMBED_MODEL = os.getenv("EMBED_MODEL", "qwen3-embedding:8b-ctx16k")   # PHẢI khớp build_vectorstore.py/api8923.py
VECTOR_DB_PATH = str(BASE / "data" / "processed" / "vectorstore")

CONFLICT_LLM_BACKEND = os.getenv("CONFLICT_LLM_BACKEND", "ollama")  # "ollama" (tự host, MẶC ĐỊNH) | "openai"
CONFLICT_LLM_MODEL = os.getenv(
    "CONFLICT_LLM_MODEL",
    "qwen2.5:14b-instruct-ctx16k" if CONFLICT_LLM_BACKEND != "openai" else "gpt-4o-mini",
)
# Mặc định tự host qua Ollama, tránh gửi nội dung văn bản nội bộ ra ngoài.
# Dùng cùng model với CHAT_MODEL/AGENT_MODEL trong api8923.py để không nạp
# thêm model mới lên GPU dùng chung. Muốn quay lại OpenAI, set
# CONFLICT_LLM_BACKEND=openai (+ OPENAI_API_KEY) trong .env.
TOP_K_PER_CHUNK = 5          # top-K vector search cho MỖI chunk văn bản mới
SIMILARITY_THRESHOLD = 0.55  # cosine similarity tối thiểu để coi là "có khả năng liên quan" (0..1)
MAX_CANDIDATE_FILES = 5      # chỉ đưa tối đa N file ứng viên liên quan nhất ra cho Admin
MAX_PAIRS_PER_FILE = 6       # chỉ đưa tối đa N cặp đoạn tương đồng nhất/file vào LLM + Admin

# [MỚI] Phát hiện "khả năng trùng lặp TOÀN FILE" ở cấp file, TRƯỚC khi cần
# LLM phân tích từng cặp đoạn - xem giải thích chi tiết ngay phía trên
# find_candidate_files().
NEAR_DUP_SCORE_THRESHOLD = 0.92     # điểm cosine coi là "gần như y hệt" (không chỉ "liên quan")
NEAR_DUP_FILE_COVERAGE = 0.6        # >=60% số đoạn (không tính khung) của văn bản MỚI khớp gần y hệt 1 file -> nghi ngờ TOÀN FILE trùng lặp

# ============================================================
# LỌC BOILERPLATE — các đoạn khung hành chính (quốc hiệu, tiêu ngữ, "Nơi
# nhận", chữ ký...) LẶP LẠI GẦN NHƯ Y HỆT ở HÀNG CHỤC văn bản "Quyết định"
# khác nhau. Nếu không lọc, các đoạn này sẽ:
#   1. Cho điểm cosine cực cao (0.95-0.99) giữa NHIỀU cặp file KHÔNG liên
#      quan nội dung, làm nhiễu bước xếp hạng "file liên quan".
#   2. Vì TOP_K_PER_CHUNK chỉ lấy top-K OLD chunk TOÀN CỤC cho MỖI new
#      chunk, các slot top-K của đúng những đoạn khung này dễ bị file
#      KHÔNG liên quan chiếm hết, đẩy văn bản TRÙNG THẬT (nhưng khung có
#      sai khác OCR nhỏ) ra khỏi danh sách ứng viên.
# Chỉ loại các đoạn này khỏi việc TÍNH ĐIỂM XẾP HẠNG ứng viên - KHÔNG loại
# khỏi production/RAG, chúng vẫn cần để trả lời câu hỏi bình thường.
# ============================================================
_BOILERPLATE_PATTERNS = [
    re.compile(p, re.IGNORECASE) for p in [
        r"b[ộo] gi[áa]o d[ụu]c v[àa] đ[àa]o t[ạa]o",
        r"c[ộo]ng ho[àa] x[ãa] h[ộo]i ch[ủu] ngh[ĩi]a vi[ệe]t nam",
        r"đ[ộo]c l[ậa]p\s*-\s*t[ựu] do\s*-\s*h[ạa]nh ph[úu]c",
        r"n[ơo]i nh[ậa]n\s*:",
        r"l[ưu]u\s*:\s*vt",
        r"^hi[ệe]u tr[ưu][ởo]ng\s*$",
        r"^c[ăa]n c[ứu]\s+(lu[ậa]t|ngh[ịi] đ[ịi]nh|quy[ếê]t đ[ịi]nh|th[ôo]ng t[ưu])",
    ]
]


def _is_boilerplate_chunk(content: str) -> bool:
    """1 đoạn bị coi là 'khung hành chính' nếu NGẮN (dưới 400 ký tự - các
    đoạn khung thường 1-2 dòng, khác hẳn 1 Điều/Khoản dài) VÀ khớp ít nhất
    1 pattern khung. Ngưỡng độ dài để tránh loại nhầm 1 Điều dài mà tình
    cờ nhắc lại cụm 'Căn cứ...' trong nội dung."""
    text = content.strip()
    if len(text) > 400:
        return False
    return any(p.search(text) for p in _BOILERPLATE_PATTERNS)


# Các loại quan hệ LLM có thể trả về (khớp mục "LLM phân tích các file ứng viên" trong sơ đồ)
QUAN_HE_TRUNG_LAP = "trung_lap"                   # [MỚI] 2 đoạn GIỐNG HỆT NHAU về ý nghĩa (chỉ lệch
                                                   # chính tả/khoảng trắng/dấu câu do OCR) -> KHÔNG phải
                                                   # mâu thuẫn, tự động thay bản cũ bằng bản mới, KHÔNG hỏi Admin
QUAN_HE_BO_SUNG = "bo_sung"                       # không xung đột, có thể thêm thẳng vào tri thức
QUAN_HE_KHAC_THOI_DIEM = "khac_thoi_diem_pham_vi" # khác thời điểm/phạm vi RÕ RÀNG -> KHÔNG xung đột, tự gắn valid_from/valid_to, KHÔNG hỏi Admin
QUAN_HE_KHAC_GIA_TRI = "khac_gia_tri"             # có khả năng xung đột -> cần Admin xem
QUAN_HE_MAU_THUAN = "mau_thuan"                   # mâu thuẫn rõ ràng -> cần Admin xử lý
QUAN_HE_UNCERTAIN = "khong_du_thong_tin"          # UNCERTAIN -> cần Admin xử lý, không tự quyết định
# [FIX] Bản trước thiếu QUAN_HE_TRUNG_LAP trong tập hợp lệ này - hậu quả:
# llm_analyze_pair() coi MỌI kết quả "trung_lap" LLM trả về là KHÔNG hợp lệ
# (vì không nằm trong CAC_QUAN_HE_HOP_LE) và tự ép về "khong_du_thong_tin",
# vô hiệu hóa hoàn toàn nhãn mới vừa thêm dù prompt đã đúng.
CAC_QUAN_HE_HOP_LE = {QUAN_HE_TRUNG_LAP, QUAN_HE_BO_SUNG, QUAN_HE_KHAC_THOI_DIEM, QUAN_HE_KHAC_GIA_TRI,
                       QUAN_HE_MAU_THUAN, QUAN_HE_UNCERTAIN}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ============================================================
# >>> ĐIỂM TÍCH HỢP #1 <<< — NƠI LƯU FILE GỐC (PDF/docx)
#
# pipeline_pdf.py KHÔNG lưu path nào vào metadata chunk cả (chỉ có loai_van_ban,
# tieu_de, so_hieu...). "source_file" mà module này dùng để gom nhóm ứng
# viên (trong ensure_chunk_ids_and_status) chỉ là TÊN file (base_name), đủ
# để biết "chunk này thuộc văn bản nào" nhưng KHÔNG đủ để Admin mở lại bản
# gốc ra xem trong lúc rà soát xung đột.
#
# resolve_source_location() là hàm DUY NHẤT trong cả module tra path/URL tới
# file gốc. Hiện tra theo quy ước thư mục của pipeline_pdf.py/pipeline_docx_txt.py
# (data/raw/<ext>/<base_name>.<ext>). Khi sau này đổi nơi lưu trữ (local -> S3/MinIO/SharePoint/CSDL...), CHỈ cần sửa hàm này để trả về
# path/URL/storage-key tương ứng — mọi nơi khác trong module (manifest, log,
# hiển thị cho Admin) chỉ coi giá trị trả về là 1 CHUỖI MỜ, không parse/giả
# định gì về định dạng của nó.
# ============================================================
RAW_INPUT_FOLDERS = {
    ".pdf": BASE / "data" / "raw" / "pdf",
    ".docx": BASE / "data" / "raw" / "docx",
    ".doc": BASE / "data" / "raw" / "docx",
    ".txt": BASE / "data" / "raw" / "txt",
    ".md": BASE / "data" / "raw" / "md",
}


def resolve_source_location(base_name: str) -> Optional[str]:
    for ext, folder in RAW_INPUT_FOLDERS.items():
        candidate = folder / f"{base_name}{ext}"
        if candidate.exists():
            return str(candidate)
    return None


# ============================================================
# STORAGE LAYER — TOÀN BỘ truy cập vị trí file vật lý đi qua đây, không nơi
# nào khác trong module tự ráp đường dẫn từ base_name.
# >>> ĐIỂM TÍCH HỢP HỆ THỐNG: đổi nơi lưu trữ (DB, S3, quy ước tên khác...)
#     chỉ cần sửa 2 hàm dưới đây. <<<
# ============================================================

def get_chunk_path(base_name: str) -> Path:
    return output_chunk_folder / f"{base_name}.json"


def get_markdown_path(base_name: str) -> Path:
    return output_md_folder / f"{base_name}.md"


_BAK_SUFFIX_RE = re.compile(r"\.bak_\d{8}_\d{6}(_[a-z_]+)?$")


def list_all_chunk_base_names() -> list:
    """Nguồn liệt kê 'văn bản nào đang có trong hệ chunk' - hiện dựa vào
    glob thư mục local (đúng quy ước hiện tại của pipeline_pdf.py/build_vectorstore.py).
    >>> ĐIỂM TÍCH HỢP HỆ THỐNG: nếu chuyển sang DB/object storage, đổi hàm
        này để query danh sách từ nơi lưu trữ mới - list_pending_new_documents()
        và bootstrap() gọi qua đây nên không cần sửa gì thêm. <<<

    [FIX] Bỏ qua mọi file backup dạng '<ten>.bak_YYYYMMDD_HHMMSS[...].json'
    (vd do sửa tay 1 đoạn qua web UI) - trước đây các file này lọt vào đây
    và bị coi là 1 văn bản MỚI hoàn toàn, hiện trong 'Chờ kiểm tra xung đột'
    mãi không hết dù đã xóa văn bản gốc (mỗi lần sửa lại đẻ thêm 1 bản mới,
    xem admin_api.py update_chunks()). Cũng bỏ qua thư mục con '_backups/'
    (nơi các bản backup NÊN nằm từ giờ) để chắc chắn không lọt qua glob."""
    return [p.stem for p in sorted(output_chunk_folder.glob("*.json"))
            if not _BAK_SUFFIX_RE.search(p.stem)]


# ============================================================
# MANIFEST + AUDIT LOG — nguồn sự thật cho việc "văn bản nào đã ở production"
# ============================================================

def load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    return {}


def save_manifest(manifest: dict):
    MANIFEST_PATH.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


def log_decision(event: dict):
    """Ghi audit trail dạng JSON Lines - mỗi quyết định của Admin (hoặc tự
    động duyệt 'bo_sung') là 1 dòng, phục vụ truy vết sau này (ai/khi nào/
    quyết định gì với chunk nào)."""
    event = {"luc": _now(), **event}
    with open(DECISION_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


# ============================================================
# BƯỚC 1 — Chuẩn hóa Markdown + tính checksum
# ============================================================

def normalize_markdown(text: str) -> str:
    """Chuẩn hóa nội dung Markdown trước khi tính checksum - để việc so sánh
    KHÔNG bị ảnh hưởng bởi khác biệt định dạng thuần túy (khoảng trắng thừa,
    kiểu xuống dòng khác nhau...), đúng như bước 1 trong sơ đồ."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def compute_checksum(text: str) -> str:
    return hashlib.sha256(normalize_markdown(text).encode("utf-8")).hexdigest()


def compute_new_document_checksum(base_name: str) -> str:
    md_path = get_markdown_path(base_name)
    if not md_path.exists():
        raise FileNotFoundError(
            f"Không tìm thấy {md_path} - cần chạy 'pipeline_pdf.py generate' (và rà soát) trước."
        )
    return compute_checksum(md_path.read_text(encoding="utf-8"))


# ============================================================
# CHUNK IO — đọc/ghi lại file chunk .json trong production, có migrate tại chỗ
# ============================================================

def load_chunk_file(base_name: str) -> list:
    return json.loads(get_chunk_path(base_name).read_text(encoding="utf-8"))


def save_chunk_file(base_name: str, chunks: list):
    get_chunk_path(base_name).write_text(json.dumps(chunks, ensure_ascii=False, indent=2), encoding="utf-8")


def ensure_chunk_ids_and_status(base_name: str, duong_dan_goc: Optional[str] = None) -> list:
    """Bổ sung 'chunk_id' (duy nhất), 'trang_thai' (mặc định 'active') và
    'source_file' vào metadata của từng chunk NẾU CHƯA CÓ, rồi ghi đè lại
    file .json - migrate tại chỗ, idempotent (chạy nhiều lần vô hại).

    3 trường này là phần build_vectorstore.py sẽ tự động đọc vào metadata
    FAISS (vì nó copy nguyên metadata của mỗi chunk), nên KHÔNG cần sửa
    build_vectorstore.py để FAISS "biết" trạng thái hiệu lực của từng chunk.

    duong_dan_goc: đường dẫn/ID file gốc THẬT (vd đường dẫn PDF gốc, hoặc ID
    trong hệ quản lý tài liệu) - NẾU hệ thống gọi cung cấp được. Hiện tại
    pipeline_pdf.py KHÔNG cung cấp giá trị này (không lưu path gốc ở đâu cả),
    nên field này thường sẽ rỗng cho tới khi tích hợp.
    >>> ĐIỂM TÍCH HỢP HỆ THỐNG: khi có nguồn cấp đường dẫn/ID file gốc thật
        (thay vì suy luận qua tên file), truyền vào tham số này ở nơi gọi
        (process_new_document/bootstrap) để nó được gắn vào metadata mỗi
        chunk, tách biệt khỏi "source_file" (vốn chỉ là khóa nhóm nội bộ). <<<"""
    chunks = load_chunk_file(base_name)
    changed = False
    for i, c in enumerate(chunks):
        md = c.setdefault("metadata", {})
        if "chunk_id" not in md:
            md["chunk_id"] = f"{base_name}::{i:04d}"
            changed = True
        if "trang_thai" not in md:
            md["trang_thai"] = "active"
            changed = True
        if md.get("source_file") != base_name:
            md["source_file"] = base_name
            changed = True
        if duong_dan_goc and md.get("duong_dan_goc") != duong_dan_goc:
            md["duong_dan_goc"] = duong_dan_goc
            changed = True
    if changed:
        save_chunk_file(base_name, chunks)
    return chunks


def set_chunk_status(base_name: str, chunk_ids: set, trang_thai: str, extra_metadata: Optional[dict] = None):
    chunks = load_chunk_file(base_name)
    for c in chunks:
        md = c.get("metadata", {})
        if md.get("chunk_id") in chunk_ids:
            md["trang_thai"] = trang_thai
            if extra_metadata:
                md.update(extra_metadata)
    save_chunk_file(base_name, chunks)


def set_chunk_content(base_name: str, chunk_id: str, noi_dung_moi: str):
    """Sửa trực tiếp nội dung 1 chunk (giữ nguyên chunk_id/metadata khác) -
    dùng khi 1 đoạn có 1 câu bị lỗi thời nhưng phần còn lại vẫn có giá trị:
    sửa đúng câu đó thay vì phải loại cả đoạn."""
    chunks = load_chunk_file(base_name)
    da_sua = False
    for c in chunks:
        if c.get("metadata", {}).get("chunk_id") == chunk_id:
            c["content"] = noi_dung_moi
            c["metadata"]["da_sua_tay_luc"] = _now()
            da_sua = True
    if not da_sua:
        raise ValueError(f"Không tìm thấy chunk_id='{chunk_id}' trong '{base_name}'")
    save_chunk_file(base_name, chunks)


def set_all_chunks_status(base_name: str, trang_thai: str):
    chunks = load_chunk_file(base_name)
    for c in chunks:
        c.setdefault("metadata", {})["trang_thai"] = trang_thai
    save_chunk_file(base_name, chunks)


def load_active_production_chunks(manifest: dict, exclude_base: Optional[str] = None) -> list:
    """Toàn bộ chunk KHÔNG active bị loại khỏi so sánh - dữ liệu đã 'vô hiệu
    hóa'/'bị bỏ qua'/'thay thế' không nên tiếp tục sinh cảnh báo xung đột."""
    all_chunks = []
    for base_name in manifest:
        if base_name == exclude_base:
            continue
        try:
            chunks = load_chunk_file(base_name)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            # [FIX] Trước đây không bọc try/except - 1 entry "mồ côi" trong
            # manifest (file .json đã bị xóa/hỏng, chưa kịp 'prune') sẽ làm
            # CRASH toàn bộ vector search, khiến check/check-file luôn báo
            # lỗi hệ thống thay vì "không có ứng viên". Bỏ qua + cảnh báo.
            print(f"⚠️  Bỏ qua '{base_name}' khi so sánh xung đột: {e}. Chạy 'prune' để dọn manifest.")
            continue
        for c in chunks:
            if c.get("metadata", {}).get("trang_thai") == "active":
                all_chunks.append(c)
    return all_chunks


# ============================================================
# EMBEDDING + SO SÁNH VECTOR (dùng cosine similarity trực tiếp, không phụ
# thuộc quy ước khoảng cách của FAISS index - dễ đặt ngưỡng, dễ kiểm soát)
# ============================================================

_embeddings_client = None


def get_embeddings_client() -> OllamaEmbeddings:
    global _embeddings_client
    if _embeddings_client is None:
        _embeddings_client = OllamaEmbeddings(
            base_url=OLLAMA_SERVER, model=EMBED_MODEL, client_kwargs=OLLAMA_CLIENT_KWARGS
        )
    return _embeddings_client


_conflict_llm_client = None


def get_conflict_llm_client():
    """Lazy init client LLM dùng cho llm_analyze_pair() (Bước 4-5 sơ đồ) -
    TÁCH RIÊNG khỏi get_embeddings_client() vì phục vụ mục đích khác (sinh
    JSON phân tích quan hệ, không phải embedding)."""
    global _conflict_llm_client
    if _conflict_llm_client is not None:
        return _conflict_llm_client

    if CONFLICT_LLM_BACKEND == "openai":
        _conflict_llm_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
        return _conflict_llm_client

    try:
        _conflict_llm_client = ChatOllama(
            model=CONFLICT_LLM_MODEL, temperature=0, base_url=OLLAMA_SERVER,
            client_kwargs=OLLAMA_CLIENT_KWARGS, format="json",
        )
    except TypeError:
        # Bản langchain-ollama cũ không nhận tham số format="json" (giống
        # ghi chú router_llm trong api8923.py) - vẫn chạy được, chỉ mất phần
        # ép JSON cứng; llm_analyze_pair() đã tự dọn markdown-fence + có
        # fallback UNCERTAIN khi parse lỗi nên không phụ thuộc hoàn toàn vào
        # tham số này.
        _conflict_llm_client = ChatOllama(
            model=CONFLICT_LLM_MODEL, temperature=0, base_url=OLLAMA_SERVER,
            client_kwargs=OLLAMA_CLIENT_KWARGS,
        )
    return _conflict_llm_client


def call_llm_with_retry(fn, *args, max_retries: int = 3, delay_seconds: float = 2.0, **kwargs):
    """Retry chung cho lời gọi LLM tự host qua Ollama - GPU dùng chung có thể
    tạm thời phải hoán đổi (swap) model khi nhiều tiến trình (api8923.py +
    conflict_detection.py) tranh nhau VRAM cùng lúc, gây lỗi 'model failed to
    load' THOÁNG QUA chứ không phải lỗi vĩnh viễn - đúng lý do đã ghi trong
    embed_with_retry() của api8923.py, áp dụng tương tự ở đây."""
    last_exc = None
    for attempt in range(max_retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_exc = e
            if attempt < max_retries - 1:
                print(f"    [!] Lỗi gọi LLM phân tích xung đột (thử {attempt + 1}/{max_retries}): {e} "
                      f"- thử lại sau {delay_seconds}s")
                time.sleep(delay_seconds)
    raise last_exc


def _load_embedding_cache() -> dict:
    """Trả về {chunk_id: (content_hash, vector)}.

    Cache gắn với 1 EMBED_MODEL cụ thể - vector của model này không tương
    thích với model khác dù trùng chunk_id/content_hash. File cache lưu kèm
    tên model lúc ghi; nếu EMBED_MODEL hiện tại khác với model đã lưu, toàn
    bộ cache cũ bị bỏ qua tự động."""
    if not EMBEDDING_CACHE_PATH.exists():
        return {}
    with open(EMBEDDING_CACHE_PATH, "rb") as f:
        raw = pickle.load(f)

    # Tương thích ngược: cache format CŨ (trước khi có versioning) là thẳng
    # {chunk_id: (hash, vec)}, không có key "embed_model" - coi như KHÔNG rõ
    # model nào đã tạo ra nó -> AN TOÀN là bỏ qua toàn bộ, embed lại từ đầu.
    if not isinstance(raw, dict) or "embed_model" not in raw:
        print("⚠️  embedding_cache.pkl ở định dạng cũ (không rõ EMBED_MODEL đã dùng) "
              "- bỏ qua toàn bộ cache, embed lại từ đầu cho an toàn.")
        return {}

    if raw.get("embed_model") != EMBED_MODEL:
        print(f"⚠️  embedding_cache.pkl được tạo bằng model '{raw.get('embed_model')}', "
              f"khác với EMBED_MODEL hiện tại '{EMBED_MODEL}' - TOÀN BỘ cache cũ bị bỏ qua "
              f"(tự động), sẽ embed lại từ đầu bằng model hiện tại.")
        return {}

    return raw.get("vectors", {})


def _save_embedding_cache(cache: dict):
    payload = {"embed_model": EMBED_MODEL, "vectors": cache}
    with open(EMBEDDING_CACHE_PATH, "wb") as f:
        pickle.dump(payload, f)


def reset_embedding_cache():
    """Lệnh 'reset-cache' - xóa hẳn embedding_cache.pkl, buộc embed lại TOÀN
    BỘ chunk (production lẫn văn bản đang kiểm tra) ở lần chạy 'check'/
    'rebuild-index' kế tiếp. Dùng khi: đổi EMBED_MODEL/OLLAMA_SERVER (dù cơ
    chế versioning ở _load_embedding_cache() đã tự lo việc này), nghi ngờ
    cache hỏng, hoặc muốn chắc chắn 100% dữ liệu embedding đang dùng khớp
    với model hiện tại. KHÔNG đụng tới manifest.json hay trạng thái
    (trang_thai) của bất kỳ chunk nào - chỉ xóa vector đã cache, không xóa
    quyết định xung đột nào Admin đã duyệt trước đó."""
    if not EMBEDDING_CACHE_PATH.exists():
        print("✅ Chưa có embedding_cache.pkl - không có gì để xóa.")
        return
    EMBEDDING_CACHE_PATH.unlink()
    print(f"✅ Đã xóa {EMBEDDING_CACHE_PATH}. Lần 'check'/'rebuild-index' kế tiếp sẽ embed lại "
          f"từ đầu bằng model hiện tại '{EMBED_MODEL}'.")


def embed_chunks(chunks: list) -> dict:
    """Trả về {chunk_id: np.array(vector)}. Có cache theo (chunk_id, checksum
    nội dung) - CHỈ gọi Ollama embed lại cho chunk có nội dung mới thay đổi
    hoặc chưa từng embed, tránh embed lại toàn bộ corpus mỗi lần kiểm tra 1
    văn bản mới (embedding là bước tốn thời gian nhất khi corpus đã lớn)."""
    cache = _load_embedding_cache()
    to_embed_keys, to_embed_texts = [], []
    result = {}

    for c in chunks:
        md = c.get("metadata", {})
        cid = md["chunk_id"]
        content_hash = hashlib.sha256(c["content"].encode("utf-8")).hexdigest()
        cached = cache.get(cid)
        if cached and cached[0] == content_hash:
            result[cid] = np.array(cached[1], dtype=np.float32)
        else:
            to_embed_keys.append((cid, content_hash))
            to_embed_texts.append(c["content"])

    if to_embed_texts:
        embeddings = get_embeddings_client()
        vectors = embeddings.embed_documents(to_embed_texts)
        for (cid, content_hash), vec in zip(to_embed_keys, vectors):
            cache[cid] = (content_hash, vec)
            result[cid] = np.array(vec, dtype=np.float32)
        _save_embedding_cache(cache)

    return result


def cosine_similarity_matrix(A: np.ndarray, B: np.ndarray) -> np.ndarray:
    A_norm = A / (np.linalg.norm(A, axis=1, keepdims=True) + 1e-8)
    B_norm = B / (np.linalg.norm(B, axis=1, keepdims=True) + 1e-8)
    return A_norm @ B_norm.T


# ============================================================
# CHUNKING CHO LỆNH 'check-file' — SAO CHÉP lại logic chunk theo header + giữ
# nguyên bảng từ pipeline_pdf.py (KHÔNG import pipeline_pdf.py, để né pdf2image
# đúng như build_vectorstore.py đã né). Chỉ dùng khi test nhanh 1 file .md
# rời, chưa qua prepare/generate. NẾU pipeline_pdf.py đổi cách chunk (CHUNK_SIZE,
# quy tắc bảng, tên field header...), nhớ đồng bộ lại đoạn này.
# ============================================================

TEST_CHUNK_SIZE = 800
TEST_CHUNK_OVERLAP = 100
TEST_TABLE_MAX_ROWS_PER_CHUNK = 1

_TEST_TABLE_BLOCK_PATTERN = re.compile(
    r"(?:^\|.*\|[ \t]*\n)(?:^\|[\-: \t|]+\|[ \t]*\n)(?:^\|.*\|[ \t]*\n?)*",
    re.MULTILINE,
)
_TEST_PAGE_MARKER_PATTERN = re.compile(r"\n*=+\s*TRANG\s+(?P<pagenum>\d+)\s*=+\n*")
_TEST_SPECIAL_BLOCK_PATTERN = re.compile(
    r"(?P<table>" + _TEST_TABLE_BLOCK_PATTERN.pattern + r")"
    r"|(?P<page>" + _TEST_PAGE_MARKER_PATTERN.pattern + r")",
    re.MULTILINE,
)


def _test_split_preserving_tables(text: str, chunk_size: int, chunk_overlap: int, current_page: int = 1):
    from langchain_text_splitters import RecursiveCharacterTextSplitter
    sub_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)

    pieces = []
    last_end = 0
    page = current_page

    for m in _TEST_SPECIAL_BLOCK_PATTERN.finditer(text):
        before = text[last_end:m.start()]
        if before.strip():
            pieces.extend([{"content": p, "is_table": False, "trang": page}
                            for p in sub_splitter.split_text(before)])

        if m.group("table"):
            table_text = m.group("table")
            rows = table_text.strip("\n").split("\n")
            if len(rows) <= TEST_TABLE_MAX_ROWS_PER_CHUNK + 2:
                pieces.append({"content": table_text, "is_table": True, "trang": page})
            else:
                header_row, separator_row = rows[0], rows[1]
                data_rows = rows[2:]
                for i in range(0, len(data_rows), TEST_TABLE_MAX_ROWS_PER_CHUNK):
                    group = data_rows[i:i + TEST_TABLE_MAX_ROWS_PER_CHUNK]
                    sub_table = "\n".join([header_row, separator_row] + group)
                    pieces.append({"content": sub_table, "is_table": True, "trang": page})
        else:
            page = int(m.group("pagenum"))

        last_end = m.end()

    tail = text[last_end:]
    if tail.strip():
        pieces.extend([{"content": p, "is_table": False, "trang": page}
                        for p in sub_splitter.split_text(tail)])

    return pieces, page


def chunk_markdown_for_test(markdown_text: str, doc_metadata: dict) -> list:
    from langchain_text_splitters import MarkdownHeaderTextSplitter

    header_splitter = MarkdownHeaderTextSplitter(
        headers_to_split_on=[("#", "chuong_hoac_muc"), ("##", "dieu"), ("###", "muc_con")],
        strip_headers=False,
    )
    header_chunks = header_splitter.split_text(markdown_text)

    final_chunks = []
    current_page = 1
    for chunk in header_chunks:
        pieces, current_page = _test_split_preserving_tables(
            chunk.page_content, TEST_CHUNK_SIZE, TEST_CHUNK_OVERLAP, current_page
        )
        for piece in pieces:
            final_chunks.append({
                "content": piece["content"],
                "metadata": {
                    **doc_metadata,
                    **chunk.metadata,
                    "content_type": "table" if piece["is_table"] else "text",
                    "trang": piece["trang"],
                },
            })
    return final_chunks


def ingest_md_file_for_test(md_path: str, base_name: Optional[str] = None) -> str:
    """
    Dùng cho lệnh CLI 'check-file' — nhận thẳng 1 file .md rời (KHÔNG đi qua
    prepare/generate của pipeline_pdf.py, chưa có OCR/metadata xác nhận). Chunk
    NHANH bằng chunk_markdown_for_test() ở trên, rồi ghi vào ĐÚNG 2 thư mục
    production (markdown/ và chunks/) để process_new_document() xử lý y hệt
    1 văn bản mới bình thường (không có code path riêng nào khác).

    LƯU Ý: metadata sinh ra chỉ là placeholder cho mục đích TEST (loai_van_ban
    mặc định "Khác", không có so_hieu/ngay_ban_hanh...). Dùng để thử luồng
    xung đột, KHÔNG dùng để nạp văn bản thật vào production — văn bản thật
    vẫn phải đi đủ pipeline_pdf.py (ocr -> prepare -> generate -> finalize) để
    có metadata đầy đủ, chính xác.
    """
    md_path = Path(md_path)
    if not md_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {md_path}")

    base_name = base_name or md_path.stem
    manifest = load_manifest()
    if base_name in manifest:
        raise ValueError(
            f"'{base_name}' đã có trong manifest (đã là production). "
            f"Truyền base_name khác, vd: check-file {md_path} {base_name}_test"
        )

    markdown_text = md_path.read_text(encoding="utf-8")
    doc_metadata = {
        "loai_van_ban": "Khác",
        "tieu_de": base_name,
        "so_hieu": None,
        "ngay_ban_hanh": None,
        "co_quan_ban_hanh": None,
        "do_tin_cay": "test",
    }
    chunks = chunk_markdown_for_test(markdown_text, doc_metadata)

    output_md_folder.mkdir(parents=True, exist_ok=True)
    output_chunk_folder.mkdir(parents=True, exist_ok=True)
    (output_md_folder / f"{base_name}.md").write_text(markdown_text, encoding="utf-8")
    save_chunk_file(base_name, chunks)

    print(f"-> Đã chunk '{md_path.name}' thành {len(chunks)} chunk, ghi tạm vào production dưới tên '{base_name}'.")
    return base_name


# ============================================================
# BƯỚC 2 — Kiểm tra trùng lặp tuyệt đối
# ============================================================

def _co_active_chunk(base_name: str) -> bool:
    """Kiểm tra base_name có ít nhất 1 chunk đang trang_thai=active không.

    Cần thiết vì manifest.json gắn nhãn "trang_thai_van_ban" ở CẤP VĂN BẢN,
    nhưng 1 văn bản "active" có thể có toàn bộ chunk bên trong đã bị loại
    (bi_bo_qua) qua quyết định cấp đoạn - văn bản đó đã "rỗng" nhưng
    manifest vẫn ghi active."""
    try:
        chunks = load_chunk_file(base_name)
    except FileNotFoundError:
        return False
    return any(c.get("metadata", {}).get("trang_thai") == "active" for c in chunks)


def find_exact_duplicate(new_checksum: str, manifest: dict) -> list:
    """Trả về danh sách tất cả base_name có checksum trùng tuyệt đối - không
    chỉ 1, vì nhiều văn bản cũ có thể trùng nhau từ trước. Rỗng nếu không
    trùng ai. Chỉ tính văn bản có ít nhất 1 chunk thực sự active (xem
    _co_active_chunk)."""
    return [
        base_name for base_name, info in manifest.items()
        if info.get("checksum") == new_checksum and info.get("trang_thai_van_ban") in ("active", "doc_lap")
        and _co_active_chunk(base_name)
    ]


def _ask_yes_no(prompt: str) -> bool:
    while True:
        ans = input(f"{prompt} (y/n): ").strip().lower()
        if ans in ("y", "yes", "co", "có"):
            return True
        if ans in ("n", "no", "khong", "không"):
            return False
        print("   Vui lòng nhập y hoặc n.")


def handle_absolute_duplicate(base_name: str, old_base_names: list, manifest: dict) -> dict:
    """Hiện ĐỦ TẤT CẢ văn bản cũ trùng tuyệt đối (không chỉ 1), hỏi Admin
    quyết định RIÊNG cho TỪNG văn bản trùng - đúng theo yêu cầu: Admin có
    quyền chọn khác nhau cho từng bản (thay thế / giữ cũ / lưu độc lập),
    không bị ép xử lý gộp chung."""
    print("\n" + "=" * 60)
    print(f"⚠️  TRÙNG LẶP TUYỆT ĐỐI: '{base_name}' có nội dung giống hệt "
          f"{len(old_base_names)} văn bản đã có trong production:")
    for ob in old_base_names:
        print(f"   - {ob}")
    print("=" * 60)
    if len(old_base_names) > 1:
        print("Sẽ hỏi LẦN LƯỢT cho từng văn bản trùng ở trên - bạn có thể quyết định KHÁC NHAU cho mỗi văn bản.\n")

    ket_qua = {}
    # [FIX] Nếu base_name ĐÃ có trong manifest (đang rà soát lại 1 văn bản
    # production sau khi sửa tay .md, giờ khớp tuyệt đối 1 văn bản khác),
    # coi như "đã nạp" ngay từ đầu để không tạo đè 1 entry mới tinh (mất
    # 'lien_quan_toi'/'nguon_goc' cũ) - chỉ cần refresh checksum bên dưới.
    new_da_nap = base_name in manifest

    for old_base_name in old_base_names:
        if len(old_base_names) > 1:
            print(f"\n--- So với '{old_base_name}' ---")

        if _ask_yes_no(f"Thay thế '{old_base_name}' bằng bản mới '{base_name}'?"):
            set_all_chunks_status(old_base_name, "thay_the")
            manifest[old_base_name]["trang_thai_van_ban"] = "thay_the"
            if not new_da_nap:
                ensure_chunk_ids_and_status(base_name)
                set_all_chunks_status(base_name, "active")
                manifest[base_name] = {
                    "checksum": manifest[old_base_name]["checksum"],
                    "trang_thai_van_ban": "active",
                    "thay_the_cho": [old_base_name],
                    "cap_nhat_luc": _now(),
                }
                new_da_nap = True
            else:
                # [FIX] base_name đã có trong manifest từ trước (rà soát lại
                # sau khi sửa tay) - vẫn phải refresh checksum theo bản MỚI,
                # không chỉ nối thêm quan hệ 'thay_the_cho'.
                manifest[base_name]["checksum"] = manifest[old_base_name]["checksum"]
                manifest[base_name]["cap_nhat_luc"] = _now()
                manifest[base_name].setdefault("thay_the_cho", []).append(old_base_name)
            log_decision({"su_kien": "trung_lap_tuyet_doi_thay_the", "moi": base_name, "cu": old_base_name})
            print(f"✅ Đã vô hiệu hóa toàn bộ chunk của '{old_base_name}'.")
            ket_qua[old_base_name] = "thay_the"
            continue

        if _ask_yes_no(f"Giữ bản cũ '{old_base_name}' (bỏ qua phần trùng từ bản mới)?"):
            log_decision({"su_kien": "trung_lap_tuyet_doi_huy_ban_moi", "moi": base_name, "cu": old_base_name})
            print(f"✅ Giữ nguyên '{old_base_name}'.")
            ket_qua[old_base_name] = "giu_cu"
            continue

        print(f"Xác nhận: lưu '{base_name}' như văn bản ĐỘC LẬP song song với '{old_base_name}'.")
        if not new_da_nap:
            ensure_chunk_ids_and_status(base_name)
            set_all_chunks_status(base_name, "active")
            manifest[base_name] = {
                "checksum": manifest[old_base_name]["checksum"],
                "trang_thai_van_ban": "doc_lap",
                "trung_voi": [old_base_name],
                "cap_nhat_luc": _now(),
            }
            new_da_nap = True
        else:
            manifest[base_name]["checksum"] = manifest[old_base_name]["checksum"]
            manifest[base_name]["cap_nhat_luc"] = _now()
            manifest[base_name].setdefault("trung_voi", []).append(old_base_name)
        log_decision({"su_kien": "trung_lap_tuyet_doi_luu_doc_lap", "moi": base_name, "cu": old_base_name})
        print(f"✅ Đã lưu '{base_name}' như văn bản độc lập, song song với '{old_base_name}'.")
        ket_qua[old_base_name] = "doc_lap"

    if not new_da_nap:
        print(f"\n✅ '{base_name}' KHÔNG được nạp vào production (Admin chọn 'giữ bản cũ' cho toàn bộ "
              f"{len(old_base_names)} văn bản trùng) - file chunk/markdown tạm vẫn còn trên đĩa, "
              f"dùng 'cleanup-test' hoặc xóa tay + 'prune' nếu muốn dọn.")

    return ket_qua


# ============================================================
# BƯỚC 3 — Truy xuất, gom nhóm ứng viên theo file (vector search cấp chunk)
# ============================================================

def find_candidate_files(new_chunks: list, manifest: dict, exclude_base: str) -> dict:
    production_chunks = load_active_production_chunks(manifest, exclude_base=exclude_base)
    if not production_chunks:
        return {}

    print(f"   → Embed {len(new_chunks)} chunk mới...")
    new_vecs = embed_chunks(new_chunks)
    print(f"   → Embed {len(production_chunks)} chunk production (có cache)...")
    prod_vecs = embed_chunks(production_chunks)

    prod_ids = list(prod_vecs.keys())
    prod_matrix = np.stack([prod_vecs[i] for i in prod_ids])
    prod_by_id = {c["metadata"]["chunk_id"]: c for c in production_chunks}
    # [FIX] Đánh dấu trước chunk nào là boilerplate để loại khỏi VÒNG LẶP so
    # sánh - không tính điểm cho cả new_chunk boilerplate lẫn old_chunk
    # boilerplate, tránh nhiễu xếp hạng như đã giải thích ở trên.
    prod_is_boilerplate = {cid: _is_boilerplate_chunk(prod_by_id[cid]["content"]) for cid in prod_ids}

    print(f"   → Tính cosine similarity ({len(new_vecs)} × {len(prod_ids)})...")
    candidates = {}
    n_new_boilerplate_skipped = 0
    n_new_eligible = 0   # [MỚI] tổng số chunk MỚI không phải khung - mẫu số để tính % trùng file
    for i, new_chunk in enumerate(new_chunks, 1):
        nid = new_chunk["metadata"]["chunk_id"]
        if nid not in new_vecs: continue
        if _is_boilerplate_chunk(new_chunk["content"]):
            n_new_boilerplate_skipped += 1
            continue
        n_new_eligible += 1
        sims = cosine_similarity_matrix(new_vecs[nid].reshape(1, -1), prod_matrix)[0]
        # [FIX] Lấy dư ra 3x TOP_K rồi lọc boilerplate, thay vì lọc SAU khi đã
        # cắt top-K cố định - nếu không, 1 new_chunk có thể bị các old_chunk
        # boilerplate chiếm hết đúng TOP_K slot trước khi kịp lọc, khiến
        # old_chunk nội dung thật (dù điểm thấp hơn 1 chút) không bao giờ
        # được xét tới dù đáng lẽ nó mới là match quan trọng.
        top_idx_wide = np.argsort(-sims)[:TOP_K_PER_CHUNK * 3]
        kept = 0
        for idx in top_idx_wide:
            if kept >= TOP_K_PER_CHUNK:
                break
            old_cid = prod_ids[idx]
            if prod_is_boilerplate[old_cid]:
                continue
            score = float(sims[idx])
            if score < SIMILARITY_THRESHOLD:
                break  # đã sort giảm dần, dưới ngưỡng thì các idx sau cũng vậy
            old_chunk = prod_by_id[old_cid]
            source_file = old_chunk["metadata"]["source_file"]
            candidates.setdefault(source_file, []).append((new_chunk, old_chunk, score))
            kept += 1
        if i % 10 == 0 or i == len(new_chunks):
            print(f"     đã quét {i}/{len(new_chunks)} chunk")

    if n_new_boilerplate_skipped:
        print(f"   → Đã bỏ qua {n_new_boilerplate_skipped} chunk MỚI là khung hành chính "
              f"(không đưa vào so sánh xếp hạng ứng viên).")

    # [FIX] Xếp hạng theo (điểm trung bình) NHÂN (hệ số độ phủ) thay vì chỉ
    # điểm trung bình thuần. Độ phủ = số cặp thực sự tìm được / MAX_PAIRS_PER_FILE,
    # kẹp trong [0.5, 1.0] - 1 file chỉ có 1-2 cặp trùng ngẫu nhiên (dù điểm
    # rất cao) sẽ bị derate, trong khi file có NHIỀU đoạn khớp (bằng chứng
    # trùng lặp/liên quan mạnh hơn) giữ nguyên điểm trung bình của nó.
    def _rank_key(pairs):
        n = len(pairs)
        mean_score = sum(s for _, _, s in pairs) / n
        coverage = min(n, MAX_PAIRS_PER_FILE) / MAX_PAIRS_PER_FILE
        return mean_score * (0.5 + 0.5 * coverage)

    ranked = sorted(candidates.items(), key=lambda kv: _rank_key(kv[1]), reverse=True)[:MAX_CANDIDATE_FILES]
    result = {}
    for source_file, pairs in ranked:
        # [MỚI] Tính % chunk MỚI (không tính khung) khớp GẦN NHƯ Y HỆT (>=
        # NEAR_DUP_SCORE_THRESHOLD) với ĐÚNG file này - dùng TOÀN BỘ pairs
        # thu được (trước khi cắt xuống MAX_PAIRS_PER_FILE), vì cắt sớm sẽ
        # đánh giá thấp mức độ phủ thực sự khi 1 file trùng rất nhiều đoạn.
        distinct_new_ids_cao = {p[0]["metadata"]["chunk_id"] for p in pairs if p[2] >= NEAR_DUP_SCORE_THRESHOLD}
        ty_le_trung = (len(distinct_new_ids_cao) / n_new_eligible) if n_new_eligible else 0.0

        pairs_sorted = sorted(pairs, key=lambda p: -p[2])[:MAX_PAIRS_PER_FILE]
        diem = sum(s for _, _, s in pairs_sorted) / len(pairs_sorted)
        result[source_file] = {
            "diem": diem,
            "cap_doan": pairs_sorted,
            "ty_le_trung": round(ty_le_trung, 3),
            "so_doan_trung_cao": len(distinct_new_ids_cao),
            "tong_so_doan_moi_hop_le": n_new_eligible,
            "rat_co_kha_nang_trung_lap_toan_file": ty_le_trung >= NEAR_DUP_FILE_COVERAGE,
        }
    print(f"   → Xong: {len(result)} file ứng viên (tổng {sum(len(v['cap_doan']) for v in result.values())} cặp đoạn)")
    for sf, info in result.items():
        if info["rat_co_kha_nang_trung_lap_toan_file"]:
            print(f"   ⚠️  '{sf}' có khả năng TRÙNG LẶP TOÀN FILE cao: "
                  f"{info['so_doan_trung_cao']}/{info['tong_so_doan_moi_hop_le']} đoạn "
                  f"({info['ty_le_trung']*100:.0f}%) khớp gần như y hệt.")
    return result

# ============================================================
# BƯỚC 4 — LLM phân tích các file ứng viên
# ============================================================

CONFLICT_ANALYSIS_PROMPT = """Bạn là trợ lý phân tích xung đột tri thức cho hệ thống RAG tiếng Việt.
Bạn sẽ nhận 1 ĐOẠN VĂN BẢN MỚI và 1 ĐOẠN VĂN BẢN CŨ (đã có trong production, mức tương đồng ngữ nghĩa cao).
Nhiệm vụ: xác định quan hệ giữa 2 đoạn và trả về CHÍNH XÁC 1 JSON object:

{
  "cung_thuc_the": true | false,   // XÁC ĐỊNH TRƯỚC TIÊN: 2 đoạn có đang nói về CÙNG 1 thực thể/đối
                                    // tượng cụ thể không (cùng 1 khoa/đơn vị, cùng 1 người, cùng 1 quy
                                    // định...)? false nếu chúng nói về 2 THỰC THỂ KHÁC NHAU dù có cấu
                                    // trúc câu/chủ đề chung giống nhau (vd 2 khoa KHÁC NHAU, 2 người
                                    // KHÁC NHAU tuy cùng chức danh "Trưởng khoa") - trường hợp false thì
                                    // các field dưới KHÔNG áp dụng (không có xung đột thật giữa 2 thực
                                    // thể khác nhau), vẫn điền quan_he/tom_tat như bình thường nhưng
                                    // quan_he SẼ BỊ BỎ QUA khi cung_thuc_the=false.
  "quan_he": "trung_lap" | "bo_sung" | "khac_thoi_diem_pham_vi" | "khac_gia_tri" | "mau_thuan" | "khong_du_thong_tin",
  "tom_tat": string,     // 1-2 câu mô tả bối cảnh và mối quan hệ giữa 2 đoạn
  "ly_do": string,       // vì sao bạn phân loại như vậy
  "do_chac_chan": "cao" | "trung_binh" | "thap",
  "valid_from_doan_cu": string | null,   // CHỈ điền khi quan_he = "khac_thoi_diem_pham_vi": ngày/mốc đoạn CŨ bắt đầu có hiệu lực, định dạng YYYY-MM-DD nếu có, hoặc mô tả ngắn (vd "trước khóa 2024"); null nếu không xác định được
  "valid_to_doan_cu": string | null,     // CHỈ điền khi quan_he = "khac_thoi_diem_pham_vi": mốc đoạn CŨ hết hiệu lực (thường ngay trước khi đoạn mới có hiệu lực); null nếu không xác định được
  "valid_from_doan_moi": string | null   // CHỈ điền khi quan_he = "khac_thoi_diem_pham_vi": mốc đoạn MỚI bắt đầu có hiệu lực; null nếu không xác định được
}

QUAN TRỌNG về "cung_thuc_the": 2 đoạn có thể RẤT giống nhau về CẤU TRÚC câu chữ (vd cả 2 đều là đoạn
giới thiệu "Ban chủ nhiệm khoa X gồm...") nhưng nói về 2 ĐƠN VỊ/THỰC THỂ hoàn toàn khác nhau (vd "Khoa
Công nghệ" và "Khoa Thống kê" là 2 khoa khác nhau, dù đoạn văn có văn phong giống nhau) - đây là false,
KHÔNG PHẢI xung đột, chỉ là 2 thực thể riêng biệt được mô tả theo mẫu câu chung của toàn trường. Chỉ trả
về true khi chắc chắn 2 đoạn cùng nói về 1 đối tượng cụ thể (cùng tên khoa, cùng tên người, cùng số hiệu
văn bản...). Nếu không rõ 2 đoạn có cùng thực thể hay không, hãy trả về false (an toàn hơn là bỏ qua 1
cặp không liên quan, so với việc coi 2 thực thể khác nhau là đang xung đột với nhau).

BƯỚC KIỂM TRA BẮT BUỘC TRƯỚC KHI CHỌN "mau_thuan": tự hỏi "2 đoạn này có đang KHẲNG ĐỊNH những điều TRÁI
NGƯỢC, LOẠI TRỪ LẪN NHAU không (vd 1 đoạn nói X=5%, đoạn kia nói X=10%; 1 đoạn nói 'phải nộp trước ngày A',
đoạn kia nói 'phải nộp trước ngày B' khác hẳn)?" NẾU 2 đoạn chỉ đơn giản là ĐỌC GIỐNG NHAU hoặc GẦN NHƯ
GIỐNG HỆT NHAU (kể cả khi lệch vài từ/dấu câu/khoảng trắng do quét OCR 2 lần khác nhau, hay 1 đoạn viết
gọn hơn 1 chút nhưng KHÔNG đổi nghĩa) thì đó KHÔNG PHẢI mâu thuẫn - đây LUÔN LUÔN là "trung_lap", dù đoạn
văn có đang nói về ngày tháng, số liệu, hay bất kỳ chủ đề "nhạy cảm" nào. SAI LẦM THƯỜNG GẶP nhất của việc
phân loại này là nhầm "2 đoạn giống hệt nhau" thành "mau_thuan" chỉ vì không tìm thấy thông tin gì KHÁC để
so sánh - đây là suy luận SAI: giống hệt nhau nghĩa là KHÔNG có gì mâu thuẫn, phải chọn "trung_lap".

Định nghĩa "quan_he" (CHỈ áp dụng khi cung_thuc_the=true), xét theo đúng THỨ TỰ dưới đây (kiểm tra "trung_lap" TRƯỚC TIÊN):
- "trung_lap": nội dung 2 đoạn về CƠ BẢN LÀ MỘT - cùng ý nghĩa, cùng giá trị/số liệu/mốc thời gian nếu có,
  khác biệt (nếu có) chỉ là lỗi chính tả/OCR/dấu câu/khoảng trắng/cách ngắt dòng, KHÔNG làm thay đổi ý
  nghĩa pháp lý hay nội dung. Bao gồm cả trường hợp đoạn mới viết GỌN hơn đoạn cũ (bỏ bớt câu lặp) nhưng
  không thêm/bớt thông tin thực chất nào.
- "bo_sung": đoạn mới bổ sung thông tin THỰC SỰ MỚI (không có trong đoạn cũ), không mâu thuẫn với đoạn cũ.
- "khac_thoi_diem_pham_vi": cả 2 đoạn đều ĐÚNG nhưng áp dụng cho các mốc thời gian/phạm vi RÕ RÀNG, KHÔNG chồng lấn (vd quy định cũ áp dụng khóa trước, quy định mới áp dụng từ khóa sau; mức phí cũ áp dụng tới hết năm X, mức phí mới áp dụng từ năm Y) - không phải mâu thuẫn thực sự, cả 2 vẫn có thể cùng tồn tại nếu gắn đúng nhãn hiệu lực theo thời gian/phạm vi. CHỈ chọn quan hệ này khi ranh giới thời gian/phạm vi là RÕ RÀNG, có căn cứ trong chính 2 đoạn; nếu chỉ NGHI NGỜ có khác thời điểm nhưng không đủ căn cứ xác định ranh giới, PHẢI chọn "khong_du_thong_tin" thay vì đoán mốc thời gian.
- "khac_gia_tri": cùng 1 thuộc tính/đối tượng nhưng giá trị KHÁC NHAU RÕ RỆT (không phải sai khác chính tả), KHÔNG xác định được rõ ràng là do khác thời điểm/phạm vi (có thể do cập nhật theo thời gian nhưng không rõ mốc) - CẦN người xem lại.
- "mau_thuan": 2 đoạn khẳng định những điều LOẠI TRỪ LẪN NHAU, không thể cùng đúng - KHÔNG bao giờ dùng nhãn này chỉ vì 2 đoạn giống nhau hoặc vì thiếu thông tin để so sánh.
- "khong_du_thong_tin": không đủ ngữ cảnh để kết luận chắc chắn (KHÁC với "2 đoạn giống hệt nhau" - trường hợp đó luôn là "trung_lap", không phải "khong_du_thong_tin").

Nếu không chắc chắn VÀ 2 đoạn KHÔNG giống hệt nhau, hãy chọn "khong_du_thong_tin" và "do_chac_chan": "thap" thay vì đoán bừa.
Chỉ trả về JSON, không thêm giải thích ngoài JSON."""


def _call_conflict_llm_raw(user_content: str) -> str:
    """Gọi LLM phân tích xung đột, trả về text thô (chưa parse JSON) - tách
    riêng theo backend (Ollama tự host mặc định, hoặc OpenAI nếu
    CONFLICT_LLM_BACKEND=openai)."""
    llm_client = get_conflict_llm_client()
    if CONFLICT_LLM_BACKEND == "openai":
        response = llm_client.chat.completions.create(
            model=CONFLICT_LLM_MODEL,
            messages=[
                {"role": "system", "content": CONFLICT_ANALYSIS_PROMPT},
                {"role": "user", "content": user_content},
            ],
            temperature=0,
            response_format={"type": "json_object"},
        )
        return response.choices[0].message.content

    response = llm_client.invoke([
        {"role": "system", "content": CONFLICT_ANALYSIS_PROMPT},
        {"role": "user", "content": user_content},
    ])
    return response.content


def llm_analyze_pair(new_chunk: dict, old_chunk: dict) -> dict:
    context_new = " > ".join(
        v for k, v in new_chunk.get("metadata", {}).items()
        if k in ("chuong_hoac_muc", "dieu", "muc_con") and v
    )
    context_old = " > ".join(
        v for k, v in old_chunk.get("metadata", {}).items()
        if k in ("chuong_hoac_muc", "dieu", "muc_con") and v
    )

    user_content = (
        f"[ĐOẠN MỚI]\nNgữ cảnh: {context_new or '(không có)'}\nNội dung: {new_chunk['content']}\n\n"
        f"[ĐOẠN CŨ]\nNgữ cảnh: {context_old or '(không có)'}\nNội dung: {old_chunk['content']}"
    )

    result = {}
    try:
        raw_text = call_llm_with_retry(_call_conflict_llm_raw, user_content)
        # Model tự host qua Ollama đôi khi vẫn bọc JSON trong ```json ... ```
        # dù đã ép format="json" - dọn trước khi parse, không để 1 dòng
        # markdown thừa làm hỏng cả kết quả.
        cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw_text.strip())
        result = json.loads(cleaned)
    except Exception as e:
        print(f"    [!] Lỗi khi LLM phân tích cặp đoạn: {e}")

    if result.get("quan_he") not in CAC_QUAN_HE_HOP_LE:
        result["quan_he"] = QUAN_HE_UNCERTAIN
        result.setdefault("tom_tat", "(LLM không trả về kết quả hợp lệ)")
        result.setdefault("ly_do", "Không parse được / không rõ quan hệ - cần Admin xem trực tiếp.")
        result["do_chac_chan"] = "thap"

    return result


# ============================================================
# BƯỚC 5-6 — Admin xem danh sách file ứng viên, quyết định cấp FILE
# ============================================================

def review_candidate_file(base_name: str, source_file: str, info: dict, manifest: dict,
                           chunks_admin_da_loai: Optional[set] = None) -> str:
    """Xử lý 1 file ứng viên: LLM phân tích từng cặp, hiển thị cho Admin,
    hỏi quyết định cấp file, rồi (nếu cần) xuống cấp đoạn.
    Trả về 'da_xu_ly_toan_bo_file' hoặc 'xu_ly_tung_doan'.

    chunks_admin_da_loai: set dùng chung xuyên suốt toàn bộ các candidate
    của 1 lần check-file (khởi tạo ở process_new_document) - theo dõi những
    chunk MỚI đã bị Admin chủ động loại (giữ bản cũ) khi so với 1 candidate,
    để quyết định của Admin không bị tự động ghi đè bởi candidate khác."""

    if chunks_admin_da_loai is None:
        chunks_admin_da_loai = set()

    sample_old_md = info["cap_doan"][0][1].get("metadata", {})
    print("\n" + "-" * 60)
    if info.get("rat_co_kha_nang_trung_lap_toan_file"):
        print(f"⚠️⚠️  '{source_file}' RẤT CÓ KHẢ NĂNG TRÙNG LẶP TOÀN FILE: "
              f"{info['so_doan_trung_cao']}/{info['tong_so_doan_moi_hop_le']} đoạn "
              f"({info['ty_le_trung']*100:.0f}%) khớp gần như y hệt với văn bản mới.")
        print(f"    Khuyến nghị: chọn 'Thay thế toàn bộ file' ngay bên dưới thay vì đi từng cặp đoạn.")
    print(f"📄 File ứng viên liên quan/xung đột: '{source_file}'  (mức liên quan: {info['diem']:.2f})")
    print(f"   Tiêu đề: {sample_old_md.get('tieu_de', '?')}  |  Số hiệu: {sample_old_md.get('so_hieu', '?')}  "
          f"|  Loại: {sample_old_md.get('loai_van_ban', '?')}")
    nguon_goc = manifest.get(source_file, {}).get("nguon_goc") or resolve_source_location(source_file)
    print(f"   File gốc: {nguon_goc or '(không tìm thấy - kiểm tra lại resolve_source_location)'}")
    print("-" * 60)

    analyzed_pairs_raw = []
    total = len(info["cap_doan"])

    for i, (new_chunk, old_chunk, score) in enumerate(info["cap_doan"], 1):
        print(f"   → Phân tích cặp đoạn {i}/{total} (score={score:.2f})...")
        analysis = llm_analyze_pair(new_chunk, old_chunk)
        print(f"     ↳ quan_he={analysis['quan_he']} ({analysis.get('do_chac_chan','?')})")
        analyzed_pairs_raw.append((new_chunk, old_chunk, score, analysis))

    for i, (new_chunk, old_chunk, score, analysis) in enumerate(analyzed_pairs_raw, 1):
        khac_thuc_the = " [THỰC THỂ KHÁC - sẽ bỏ qua]" if analysis.get("cung_thuc_the") is False else ""
        print(f"  [{i}] score={score:.2f}  quan_he={analysis['quan_he']} "
              f"(độ chắc chắn: {analysis.get('do_chac_chan', '?')}){khac_thuc_the}  — {analysis.get('tom_tat', '')}")

    # Lọc theo "cung_thuc_the" trước khi hỏi Admin: các văn bản cùng mẫu câu
    # (vd "Giới thiệu Khoa X") bị vector search coi là tương đồng dù là 2
    # thực thể khác nhau - không lọc sẽ khiến Admin phải xác nhận tràn lan.
    # Chỉ giữ cặp mà LLM xác nhận CÙNG 1 thực thể (True) hoặc không trả về
    # field này (None - coi như chưa chắc, ưu tiên an toàn vẫn hỏi Admin).
    analyzed_pairs = [p for p in analyzed_pairs_raw if p[3].get("cung_thuc_the") is not False]

    if not analyzed_pairs:
        print(f"→ Bỏ qua '{source_file}' - LLM xác nhận TẤT CẢ {len(analyzed_pairs_raw)} cặp đoạn đều "
              f"nói về THỰC THỂ KHÁC (không phải cùng khoa/đơn vị/người...), không phải xung đột thật. "
              f"KHÔNG hỏi Admin.")
        log_decision({"su_kien": "tu_dong_bo_qua_khac_thuc_the", "moi": base_name, "cu": source_file,
                      "so_cap_bi_loai": len(analyzed_pairs_raw)})
        return "bo_qua_khac_thuc_the"

    if len(analyzed_pairs) < len(analyzed_pairs_raw):
        print(f"   (đã loại {len(analyzed_pairs_raw) - len(analyzed_pairs)}/{len(analyzed_pairs_raw)} cặp "
              f"vì LLM xác nhận khác thực thể, không đưa vào các bước dưới đây)")

    if _ask_yes_no(f"\nThay thế toàn bộ file '{source_file}' bằng văn bản mới?"):
        # Ghi nhận quyết định cấp file: KHÔNG xóa vật lý bản cũ, chỉ đánh dấu
        # 'thay_the' (loại khỏi truy xuất RAG khi rebuild-index) và ghi nhận
        # quan hệ phiên bản - đúng tinh thần "không đơn giản xóa vật lý bản
        # cũ mà lưu lại quan hệ giữa các phiên bản" trong sơ đồ.
        set_all_chunks_status(source_file, "thay_the")
        set_all_chunks_status(base_name, "active")
        manifest.setdefault(source_file, {})["trang_thai_van_ban"] = "thay_the"
        log_decision({"su_kien": "thay_the_toan_bo_file", "moi": base_name, "cu": source_file})
        print(f"✅ Đã vô hiệu hóa toàn bộ '{source_file}', '{base_name}' trở thành bản hiệu lực.")
        return "da_xu_ly_toan_bo_file"

    if _ask_yes_no(f"Giữ 2 phiên bản cũ - mới ('{source_file}' và '{base_name}') cùng active?"):
        set_all_chunks_status(base_name, "active")
        log_decision({"su_kien": "giu_2_phien_ban", "moi": base_name, "cu": source_file})
        print(f"✅ Cả '{source_file}' và '{base_name}' cùng active, độc lập.")
        return "da_xu_ly_toan_bo_file"

    # Xuống cấp đoạn - "trung_lap" và "bo_sung" tự động duyệt (không xung
    # đột thật, không cần Admin). "khac_thoi_diem_pham_vi" cũng hỏi Admin
    # như khac_gia_tri/mau_thuan - nếu chọn "giữ cả hai" thì gắn
    # valid_from/valid_to (xem review_chunk_level_conflict).
    print(f"\n→ Xuống cấp đoạn cho '{source_file}':")
    for new_chunk, old_chunk, score, analysis in analyzed_pairs:
        new_id = new_chunk["metadata"]["chunk_id"]
        old_id = old_chunk["metadata"]["chunk_id"]

        if analysis["quan_he"] == QUAN_HE_TRUNG_LAP:
            # [MỚI] 2 đoạn giống hệt nhau (chỉ lệch OCR) - không có gì để
            # Admin quyết định, tự động thay đoạn CŨ bằng đoạn MỚI (thường
            # được OCR từ lần quét mới, chất lượng tương đương hoặc tốt
            # hơn) để tránh tồn tại 2 bản gần như trùng nhau trong production.
            if new_id in chunks_admin_da_loai:
                print(f"   [bỏ qua tự động duyệt - trùng lặp] {new_id} - Admin đã chủ động loại đoạn này "
                      f"khi so với 1 file ứng viên khác trước đó, không tự kích hoạt lại.")
                continue
            set_chunk_status(source_file, {old_id}, "thay_the")
            set_chunk_status(base_name, {new_id}, "active")
            log_decision({
                "su_kien": "tu_dong_duyet_trung_lap",
                "moi": new_id, "cu": old_id,
                "base_name": base_name, "source_file": source_file,
                "tom_tat": analysis.get("tom_tat"), "ly_do": analysis.get("ly_do"),
                "do_chac_chan": analysis.get("do_chac_chan"),
                "da_ghi_de": False,
            })
            print(f"   [tự động duyệt - trùng lặp/giống hệt] {new_id} thay thế {old_id}")
            continue

        if analysis["quan_he"] == QUAN_HE_BO_SUNG:
            if new_id in chunks_admin_da_loai:
                # Admin đã chủ động loại chunk này khi so với candidate khác
                # trước đó trong cùng lượt check-file - không để "bo_sung"
                # tự động kích hoạt lại, ghi đè quyết định của Admin
                print(f"   [bỏ qua tự động duyệt - bổ sung] {new_id} - Admin đã chủ động loại đoạn này "
                      f"khi so với 1 file ứng viên khác trước đó, không tự kích hoạt lại.")
                continue
            set_chunk_status(base_name, {new_id}, "active")
            log_decision({
                "su_kien": "tu_dong_duyet_bo_sung",
                "moi": new_id, "cu": old_id,
                "base_name": base_name, "source_file": source_file,
                "tom_tat": analysis.get("tom_tat"), "ly_do": analysis.get("ly_do"),
                "do_chac_chan": analysis.get("do_chac_chan"),
                "da_ghi_de": False,  # cờ để 'review-auto' biết đã bị Admin ghi đè lại chưa
            })
            print(f"   [tự động duyệt - bổ sung thông tin] {new_id}")
            continue

        review_chunk_level_conflict(base_name, source_file, new_chunk, old_chunk, analysis, chunks_admin_da_loai)

    return "xu_ly_tung_doan"


# ============================================================
# BƯỚC 7-8 — Admin xem chi tiết & quyết định cấp ĐOẠN
# ============================================================

def review_chunk_level_conflict(base_name: str, source_file: str, new_chunk: dict, old_chunk: dict,
                                 analysis: dict, chunks_admin_da_loai: Optional[set] = None):
    if chunks_admin_da_loai is None:
        chunks_admin_da_loai = set()
    new_id = new_chunk["metadata"]["chunk_id"]
    old_id = old_chunk["metadata"]["chunk_id"]

    print("\n   " + "·" * 50)
    print(f"   Xung đột: {new_id}  <->  {old_id}")
    print(f"   Quan hệ (LLM): {analysis['quan_he']}  (độ chắc chắn: {analysis.get('do_chac_chan')})")
    print(f"   Lý do: {analysis.get('ly_do')}")
    if analysis["quan_he"] == QUAN_HE_UNCERTAIN:
        print("   ⚠️  UNCERTAIN – LLM không chắc chắn, cần Admin xem kỹ nội dung dưới đây.")
    if analysis["quan_he"] == QUAN_HE_KHAC_THOI_DIEM:
        print(f"   ℹ️  LLM cho rằng đây là 2 mốc thời gian/phạm vi khác nhau, KHÔNG mâu thuẫn thực sự "
              f"(đoạn cũ: {analysis.get('valid_from_doan_cu') or '?'} -> {analysis.get('valid_to_doan_cu') or '?'}, "
              f"đoạn mới từ: {analysis.get('valid_from_doan_moi') or '?'}) - vẫn hỏi Admin xác nhận theo yêu cầu.")
    print(f"   [ĐOẠN MỚI] {new_chunk['content'][:400]}")
    print(f"   [ĐOẠN CŨ]  {old_chunk['content'][:400]}")

    if _ask_yes_no("   Thay thế bằng đoạn mới?"):
        set_chunk_status(source_file, {old_id}, "vo_hieu_hoa")
        set_chunk_status(base_name, {new_id}, "active")
        chunks_admin_da_loai.discard(new_id)  # Admin vừa chủ động kích hoạt - gỡ cờ "đã loại" nếu trước đó có
        log_decision({"su_kien": "thay_the_doan", "moi": new_id, "cu": old_id})
        print("   ✅ Đoạn cũ đã vô hiệu hóa, đoạn mới được thêm vào production.")
        return

    if _ask_yes_no("   Giữ bản cũ?"):
        set_chunk_status(base_name, {new_id}, "bi_bo_qua")
        # Đánh dấu đoạn mới đã bị Admin chủ động loại - tránh bị "bo_sung"
        # tự động của candidate khác kích hoạt lại (xem review_candidate_file).
        chunks_admin_da_loai.add(new_id)
        log_decision({"su_kien": "bo_qua_doan_moi", "moi": new_id, "cu": old_id})
        print("   ✅ Đã bỏ qua đoạn mới, giữ nguyên đoạn cũ.")
        return

    # Giữ cả 2, gắn nhãn phân biệt - nếu quan hệ là khac_thoi_diem_pham_vi,
    # gắn LUÔN valid_from/valid_to theo LLM đã phân tích (để khớp đúng ý
    # nghĩa "khác thời điểm" thay vì chỉ gắn nhãn chung chung).
    extra_cu = {"nhan_phan_biet": "ban_cu"}
    extra_moi = {"nhan_phan_biet": "ban_moi"}
    if analysis["quan_he"] == QUAN_HE_KHAC_THOI_DIEM:
        if analysis.get("valid_from_doan_cu"):
            extra_cu["valid_from"] = analysis["valid_from_doan_cu"]
        if analysis.get("valid_to_doan_cu"):
            extra_cu["valid_to"] = analysis["valid_to_doan_cu"]
        if analysis.get("valid_from_doan_moi"):
            extra_moi["valid_from"] = analysis["valid_from_doan_moi"]
    set_chunk_status(source_file, {old_id}, "active", extra_metadata=extra_cu)
    set_chunk_status(base_name, {new_id}, "active", extra_metadata=extra_moi)
    chunks_admin_da_loai.discard(new_id)
    log_decision({"su_kien": "giu_ca_2_doan", "moi": new_id, "cu": old_id})
    print("   ✅ Đã giữ cả 2 đoạn, gắn nhãn phân biệt 'ban_cu' / 'ban_moi'.")


# ============================================================
# ĐIỀU PHỐI TOÀN BỘ QUY TRÌNH (BƯỚC 1-8) CHO 1 VĂN BẢN MỚI
# ============================================================

def list_pending_new_documents() -> list:
    manifest = load_manifest()
    return [base_name for base_name in list_all_chunk_base_names() if base_name not in manifest]


def list_docs_needing_recheck() -> list:
    """[FIX] Văn bản ĐÃ có trong manifest (đã qua 'check' ít nhất 1 lần) nhưng
    nội dung .md hiện tại KHÔNG còn khớp checksum đã lưu trong manifest -
    tức là Admin đã sửa tay markdown (hoặc metadata rồi sinh lại markdown)
    SAU KHI văn bản đã lên production, và chưa chạy lại 'check'/'check-file'
    để cập nhật checksum + đối chiếu trùng lặp/xung đột cho bản MỚI.

    Trước khi có hàm này: list_pending_new_documents() chỉ thấy văn bản CHƯA
    từng có trong manifest, nên 1 văn bản production bị sửa tay xong sẽ
    KHÔNG BAO GIỜ xuất hiện lại để rà soát - đây chính là lý do sửa .md sau
    khi đã lên production xong không thấy hiện tượng gì khi test trùng lặp:
    'check' đơn giản là không có cách nào được gọi lại cho văn bản đó."""
    manifest = load_manifest()
    result = []
    for base_name, info in manifest.items():
        md_path = get_markdown_path(base_name)
        if not md_path.exists():
            continue  # đã bị xóa/mồ côi - để 'status'/'prune' xử lý riêng
        try:
            current_checksum = compute_checksum(md_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if current_checksum != info.get("checksum"):
            result.append(base_name)
    return result


def print_status():
    """Lệnh 'status' - tự chẩn đoán nguyên nhân hay gặp khiến check/check-file
    báo 'không có ứng viên' dù trực giác thấy rõ ràng phải trùng/tương đồng:
    (1) manifest rỗng vì chưa chạy bootstrap, hoặc (2) manifest có nhưng 0
    chunk nào đang trang_thai=active (toàn bộ đã bị vô hiệu hóa/thay thế)."""
    manifest = load_manifest()
    all_base_names = list_all_chunk_base_names()
    all_base_names_set = set(all_base_names)

    # Văn bản có file chunk trên đĩa nhưng CHƯA có trong manifest -> chờ kiểm tra.
    pending = [b for b in all_base_names if b not in manifest]

    # Văn bản CÓ trong manifest nhưng file chunk đã bị XÓA khỏi chunks/ (vd Admin
    # xóa thủ công) -> manifest không tự dò việc xóa file nên bị "mồ côi", lệch
    # với danh sách hiện tại. Đây là phần trước đây bị bỏ sót (âm thầm continue).
    orphaned = [b for b in manifest if b not in all_base_names_set]
    orphaned_set = set(orphaned)

    total_chunks = 0
    total_active = 0
    for base_name in manifest:
        if base_name in orphaned_set:
            continue
        try:
            chunks = load_chunk_file(base_name)
        except FileNotFoundError:
            continue
        total_chunks += len(chunks)
        total_active += sum(1 for c in chunks if c.get("metadata", {}).get("trang_thai") == "active")

    con_hieu_luc = len(manifest) - len(orphaned)

    print("=== Trạng thái production (đối chiếu manifest.json với chunks/ hiện tại) ===")
    print(f"  Văn bản đã đăng ký trong manifest        : {len(manifest)}")
    print(f"  Trong đó vẫn còn file chunk trên đĩa      : {con_hieu_luc}")
    print(f"  File .json trong chunks/ (tổng)          : {len(all_base_names)}")
    print(f"  Văn bản CHƯA đăng ký (pending)            : {len(pending)}")
    print(f"  Tổng số chunk của các văn bản còn tồn tại : {total_chunks}")
    print(f"  Trong đó đang trang_thai=active           : {total_active}")

    if not manifest and all_base_names:
        print("\n⚠️  manifest RỖNG nhưng chunks/ đã có dữ liệu sẵn -> chạy 'bootstrap' trước khi check/check-file.")
    if manifest and total_active == 0:
        print("\n⚠️  manifest có văn bản nhưng KHÔNG chunk nào active -> vector search sẽ luôn rỗng.")
    if pending:
        preview = ", ".join(pending[:10]) + (" ..." if len(pending) > 10 else "")
        print(f"\n→ {len(pending)} văn bản đang chờ kiểm tra xung đột: {preview}")
    if orphaned:
        preview = ", ".join(orphaned[:10]) + (" ..." if len(orphaned) > 10 else "")
        print(f"\n⚠️  {len(orphaned)} văn bản có trong manifest nhưng file chunk đã bị XÓA khỏi "
              f"chunks/ (không còn khớp với danh sách hiện tại): {preview}")
        print("    -> Nếu việc xóa là chủ ý (gỡ văn bản khỏi production), chạy "
              "'python conflict_detection.py prune' để dọn các mục này khỏi manifest.json,\n"
              "       rồi chạy lại 'rebuild-index' để FAISS hết nạp nhầm chunk đã xóa.")


def review_auto_approved(base_name: str):
    """Lệnh 'review-auto <base_name>' - soát lại các quyết định đã tự động
    duyệt (trung_lap/bo_sung/khac_thoi_diem_pham_vi - không hỏi Admin lúc
    check-file) của 1 văn bản. Với mỗi quyết định, Enter để giữ mặc định,
    hoặc gõ 'cu'/'moi' để ghi đè lại thành giữ bản cũ/bản mới."""
    if not DECISION_LOG_PATH.exists():
        print("✅ Chưa có decision_log.jsonl - chưa có gì để soát.")
        return

    events = []
    with open(DECISION_LOG_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))

    # Quyết định đã bị ghi đè trước đó (chạy 'review-auto' rồi) - không hiện
    # lại nữa, tránh hỏi lại cái đã xử lý.
    overridden_sigs = {
        (e.get("moi"), e.get("cu")) for e in events if e.get("su_kien") == "ghi_de_tu_dong_duyet"
    }

    SU_KIEN_NHAN = {
        "tu_dong_duyet_bo_sung": "Bổ sung thông tin",
        "tu_dong_duyet_khac_thoi_diem_pham_vi": "Khác thời điểm/phạm vi",
        "tu_dong_duyet_trung_lap": "Trùng lặp/giống hệt",
    }

    candidates = [
        e for e in events
        if e.get("su_kien") in SU_KIEN_NHAN
        and e.get("base_name") == base_name
        and (e.get("moi"), e.get("cu")) not in overridden_sigs
    ]

    if not candidates:
        print(f"✅ '{base_name}' không có quyết định tự động nào cần soát lại.")
        return

    print(f"→ '{base_name}' có {len(candidates)} quyết định tự động duyệt cần soát lại:")
    so_ghi_de = 0
    for e in candidates:
        print("\n" + "-" * 60)
        loai = SU_KIEN_NHAN[e["su_kien"]]
        print(f"[{loai}] {e.get('base_name')}::{e.get('moi')}  <->  {e.get('source_file')}::{e.get('cu')}")
        print(f"   Lúc: {e.get('luc')}  |  Độ chắc chắn: {e.get('do_chac_chan', '?')}")
        if e.get("tom_tat"):
            print(f"   Tóm tắt: {e['tom_tat']}")
        if e.get("ly_do"):
            print(f"   Lý do: {e['ly_do']}")
        if e["su_kien"] == "tu_dong_duyet_khac_thoi_diem_pham_vi":
            print(f"   Hiệu lực đoạn cũ: {e.get('valid_from_doan_cu') or '?'} -> {e.get('valid_to_doan_cu') or '?'}")
            print(f"   Hiệu lực đoạn mới từ: {e.get('valid_from_doan_moi') or '?'}")

        ans = input("   Enter=giữ mặc định | 'cu'=giữ bản cũ | 'moi'=giữ bản mới | 'ca2'=giữ cả 2: ").strip().lower()
        if ans in ("", "ca2"):
            continue

        e_base_name, source_file = e.get("base_name"), e.get("source_file")
        moi_id, cu_id = e.get("moi"), e.get("cu")
        if ans == "cu":
            set_chunk_status(e_base_name, {moi_id}, "bi_bo_qua")
            print(f"   ✅ Đã ghi đè: giữ bản CŨ, bỏ qua đoạn mới {moi_id}.")
        elif ans == "moi":
            set_chunk_status(source_file, {cu_id}, "thay_the")
            print(f"   ✅ Đã ghi đè: giữ bản MỚI, vô hiệu hóa đoạn cũ {cu_id}.")
        else:
            print("   Không nhận diện được lựa chọn - giữ mặc định.")
            continue

        log_decision({
            "su_kien": "ghi_de_tu_dong_duyet", "moi": moi_id, "cu": cu_id,
            "base_name": e_base_name, "source_file": source_file, "hanh_dong": ans,
        })
        so_ghi_de += 1

    if so_ghi_de:
        print(f"\n→ Đã ghi đè {so_ghi_de} quyết định.")
        _maybe_rebuild_now()
    else:
        print("\n✅ Không ghi đè gì - giữ nguyên toàn bộ quyết định tự động.")


SNAPSHOTS_FOLDER = conflict_folder / "snapshots"
TABLES_FOLDER_FOR_SNAPSHOT = BASE / "data" / "processed" / "tables"
REGISTRY_PATH_FOR_SNAPSHOT = BASE / "registry.json"


def snapshot_production(nhan: Optional[str] = None) -> str:
    """Lệnh 'snapshot [nhãn]' - chụp lại TOÀN BỘ trạng thái production có
    thể bị lượt test làm thay đổi: chunks/ + manifest.json (phía phi cấu
    trúc) VÀ bảng cấu trúc (.csv trong data/processed/tables/) + registry.json
    (phía có cấu trúc) - để có đường lùi CHẮC CHẮN khi test. 'cleanup-test'
    chỉ xóa văn bản có prefix test_ và hoàn toàn KHÔNG đụng tới bảng cấu
    trúc - nếu 1 lượt test dùng structured_data_pipeline.py ghi thật vào 1
    bảng (vd test_giangvien_change.csv áp vào bảng 'giangvien' có sẵn),
    CHỈ snapshot/restore-snapshot mới hoàn tác được, 'reset test' trên web
    UI sẽ tự tìm snapshot gần nhất này để khôi phục luôn (xem
    admin_api.py::_run_reset_test_job). [QUAN TRỌNG] Chạy lệnh này TRƯỚC
    khi bắt đầu 1 lượt test."""
    import shutil
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    ten_thu_muc = f"{ts}_{nhan}" if nhan else ts
    dich = SNAPSHOTS_FOLDER / ten_thu_muc
    dich.mkdir(parents=True, exist_ok=True)
    shutil.copytree(output_chunk_folder, dich / "chunks", dirs_exist_ok=True,
                     ignore=shutil.ignore_patterns("_backups"))
    if MANIFEST_PATH.exists():
        shutil.copy2(MANIFEST_PATH, dich / "manifest.json")
    if TABLES_FOLDER_FOR_SNAPSHOT.exists():
        (dich / "tables").mkdir(exist_ok=True)
        for f in TABLES_FOLDER_FOR_SNAPSHOT.glob("*.csv"):
            if ".bak_" not in f.stem:  # chỉ chụp bảng THẬT, không chụp các bản backup .bak_* đã có sẵn
                shutil.copy2(f, dich / "tables" / f.name)
    if REGISTRY_PATH_FOR_SNAPSHOT.exists():
        shutil.copy2(REGISTRY_PATH_FOR_SNAPSHOT, dich / "registry.json")
    print(f"✅ Đã chụp snapshot (chunks + manifest + bảng cấu trúc + registry.json) tại: {dich}")
    print(f"   → Khôi phục bằng: python conflict_detection.py restore-snapshot {ten_thu_muc}")
    return ten_thu_muc


def list_snapshots():
    if not SNAPSHOTS_FOLDER.exists() or not any(SNAPSHOTS_FOLDER.iterdir()):
        print("✅ Chưa có snapshot nào.")
        return
    for d in sorted(SNAPSHOTS_FOLDER.iterdir()):
        print(f"   - {d.name}")


def latest_snapshot_name() -> Optional[str]:
    """Tên snapshot GẦN NHẤT (theo tên thư mục, bắt đầu bằng timestamp nên
    sort chuỗi = sort theo thời gian) - dùng bởi 'reset test' trên web UI để
    tự tìm snapshot phù hợp mà không cần Admin gõ tên tay."""
    if not SNAPSHOTS_FOLDER.exists():
        return None
    ds = sorted((d.name for d in SNAPSHOTS_FOLDER.iterdir() if d.is_dir()))
    return ds[-1] if ds else None


def restore_snapshot(ten_thu_muc: str):
    """Lệnh 'restore-snapshot <tên>' - khôi phục ĐÚNG trạng thái chunks/ +
    manifest.json + bảng cấu trúc (.csv) + registry.json đã chụp bằng
    'snapshot' (ghi đè hoàn toàn hiện tại - tự chụp 1 snapshot của trạng
    thái TRƯỚC KHI khôi phục, phòng bấm nhầm)."""
    import shutil
    nguon = SNAPSHOTS_FOLDER / ten_thu_muc
    if not nguon.exists():
        print(f"❌ Không tìm thấy snapshot '{ten_thu_muc}'. Chạy 'list-snapshots' để xem danh sách.")
        sys.exit(1)

    snapshot_production(nhan="truoc_khi_restore")

    for f in output_chunk_folder.glob("*.json"):
        f.unlink()
    for f in (nguon / "chunks").glob("*.json"):
        shutil.copy2(f, output_chunk_folder / f.name)
    if (nguon / "manifest.json").exists():
        shutil.copy2(nguon / "manifest.json", MANIFEST_PATH)

    n_tables = 0
    if (nguon / "tables").exists():
        TABLES_FOLDER_FOR_SNAPSHOT.mkdir(parents=True, exist_ok=True)
        for f in (nguon / "tables").glob("*.csv"):
            shutil.copy2(f, TABLES_FOLDER_FOR_SNAPSHOT / f.name)
            n_tables += 1
    if (nguon / "registry.json").exists():
        shutil.copy2(nguon / "registry.json", REGISTRY_PATH_FOR_SNAPSHOT)

    print(f"✅ Đã khôi phục chunks/ + manifest.json từ snapshot '{ten_thu_muc}'"
          f"{f' (+ {n_tables} bảng cấu trúc + registry.json)' if n_tables or (nguon / 'registry.json').exists() else ''}.")
    if n_tables or (nguon / "registry.json").exists():
        _notify_api8923_reload()
    print("   → Chạy 'python conflict_detection.py rebuild-index' để FAISS production khớp lại.")


def revert_doc(base_name: str):
    """Lệnh 'revert-doc <base_name>' - đưa TOÀN BỘ chunk của 1 văn bản THẬT
    về lại active (hoàn tác mọi 'thay_the'/'bi_bo_qua'/'vo_hieu_hoa' đã bị
    gán trong lúc test), và đặt lại trang_thai_van_ban='active' trong
    manifest. Dùng khi KHÔNG có snapshot để restore-snapshot (vd đã lỡ test
    trước khi kịp chạy 'snapshot') và biết rõ văn bản này đã bị ảnh hưởng do
    test - xem decision_log.jsonl để tìm base_name của các văn bản THẬT đã
    bị nhắc tới cùng 1 văn bản test_* nào đó."""
    manifest = load_manifest()
    if base_name not in manifest:
        print(f"❌ '{base_name}' không có trong manifest.json.")
        sys.exit(1)
    set_all_chunks_status(base_name, "active")
    manifest[base_name]["trang_thai_van_ban"] = "active"
    save_manifest(manifest)
    print(f"✅ Đã đưa toàn bộ chunk của '{base_name}' về lại active.")
    print("   → Chạy 'python conflict_detection.py rebuild-index' để FAISS production khớp lại.")


def cleanup_test_data(prefix: str = "test_", auto_rebuild: bool = True):
    """Lệnh 'cleanup-test [prefix]' - dọn SẠCH mọi văn bản dùng để thử
    nghiệm (mặc định mọi base_name bắt đầu bằng 'test_', đúng quy ước đặt
    tên trong bộ test case), khôi phục đúng trạng thái FAISS production như
    trước khi test:
      1. Xóa data/processed/markdown/<base_name>.md + chunks/<base_name>.json
         cho MỌI base_name khớp prefix - kể cả base_name CHƯA có trong
         manifest (trường hợp Admin từng chọn "giữ bản cũ" ở trùng lặp tuyệt
         đối, để lại file rác không đăng ký - xem handle_absolute_duplicate).
      2. Xóa khỏi manifest.json mọi entry khớp prefix.
      3. rebuild-index để FAISS production hết sạch dữ liệu test (mặc định
         BẬT - tắt bằng auto_rebuild=False nếu muốn tự chạy tay sau).

    AN TOÀN: chỉ đụng tới base_name khớp ĐÚNG prefix - không đụng gì tới
    văn bản production thật (không có prefix test_)."""
    manifest = load_manifest()
    manifest_keys_to_remove = [b for b in manifest if b.startswith(prefix)]

    # Quét luôn cả file .json trên đĩa - kể cả những base_name CHƯA từng vào
    # manifest (case "Hủy" ở trùng lặp tuyệt đối để lại file rác không đăng ký).
    chunk_base_names = {p.stem for p in output_chunk_folder.glob(f"{prefix}*.json")}
    md_base_names = {p.stem for p in output_md_folder.glob(f"{prefix}*.md")}
    all_test_base_names = set(manifest_keys_to_remove) | chunk_base_names | md_base_names

    if not all_test_base_names:
        print(f"✅ Không có văn bản test nào (prefix '{prefix}') cần dọn.")
        return

    print(f"→ Dọn {len(all_test_base_names)} văn bản test (prefix '{prefix}'): "
          f"{', '.join(sorted(all_test_base_names))}")

    for base_name in all_test_base_names:
        chunk_path = get_chunk_path(base_name)
        if chunk_path.exists():
            chunk_path.unlink()
        md_path = get_markdown_path(base_name)
        if md_path.exists():
            md_path.unlink()
        if base_name in manifest:
            del manifest[base_name]

    save_manifest(manifest)
    log_decision({"su_kien": "cleanup_test", "prefix": prefix, "base_names": sorted(all_test_base_names)})
    print(f"✅ Đã xóa {len(all_test_base_names)} văn bản test khỏi markdown/, chunks/ và manifest.json.")

    # Cảnh báo các văn bản THẬT có thể đã bị đổi trạng thái chunk trong lúc
    # test (cleanup-test không tự hoàn tác được phần này) - quét
    # decision_log.jsonl để liệt kê, không tự sửa.
    van_ban_thuc_bi_anh_huong = set()
    if DECISION_LOG_PATH.exists():
        with open(DECISION_LOG_PATH, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue
                cac_gia_tri = [e.get(k) for k in ("base_name", "source_file", "cu", "moi")]
                co_nhac_van_ban_test = any(isinstance(v, str) and v in all_test_base_names for v in cac_gia_tri)
                if not co_nhac_van_ban_test:
                    continue
                for v in cac_gia_tri:
                    if isinstance(v, str) and v not in all_test_base_names and not v.startswith(prefix) and v in manifest:
                        van_ban_thuc_bi_anh_huong.add(v)

    if van_ban_thuc_bi_anh_huong:
        print(f"\n⚠️  {len(van_ban_thuc_bi_anh_huong)} văn bản THẬT có thể đã bị đổi trạng thái chunk trong "
              f"lúc test (cleanup-test KHÔNG tự hoàn tác được phần này):")
        for v in sorted(van_ban_thuc_bi_anh_huong):
            print(f"   - {v}")
        print("   → Nếu có snapshot chụp TRƯỚC lúc test: 'python conflict_detection.py restore-snapshot <tên>'.")
        print("   → Nếu KHÔNG có snapshot: soát decision_log.jsonl cho từng văn bản trên, cân nhắc "
              "'python conflict_detection.py revert-doc <base_name>' nếu chắc chắn nó cần về lại active.")

    if auto_rebuild:
        print("→ Chạy lại rebuild-index để FAISS production hết sạch dữ liệu test...")
        rebuild_production_vectorstore()
    else:
        print("   → NHỚ chạy 'python conflict_detection.py rebuild-index' để FAISS production hết dữ liệu test.")


def prune_manifest():
    """Lệnh 'prune' - dọn khỏi manifest.json các văn bản mà file chunk tương
    ứng (data/processed/chunks/<base_name>.json) đã bị XÓA khỏi ổ đĩa (vd
    Admin xóa file thủ công). manifest.json không tự dò việc xóa file, nên
    nếu không chạy lệnh này, 'status' sẽ tiếp tục báo các văn bản đó là "đã
    đăng ký production" dù thực tế đã bị gỡ, và 'rebuild-index' vẫn đang đọc
    an toàn (bỏ qua vì load_chunk_file lỗi) nhưng số liệu/manifest bị lệch.
    Không đụng tới bất kỳ văn bản nào còn file chunk trên đĩa."""
    manifest = load_manifest()
    all_base_names = set(list_all_chunk_base_names())
    orphaned = [b for b in manifest if b not in all_base_names]

    if not orphaned:
        print("✅ manifest.json đã khớp với chunks/ hiện tại - không có mục nào cần dọn.")
        return

    for b in orphaned:
        del manifest[b]
    save_manifest(manifest)
    print(f"✅ Đã dọn {len(orphaned)} văn bản khỏi manifest.json (file chunk không còn tồn tại):")
    for b in orphaned:
        print(f"  - {b}")
    print("→ Chạy 'python conflict_detection.py rebuild-index' để FAISS production đồng bộ lại.")


def bootstrap():
    """Đăng ký toàn bộ chunk hiện có (coi như đã được duyệt từ trước khi có
    module này) vào manifest với trang_thai_van_ban=active. Chỉ chạy 1 lần
    khi mới đưa module này vào 1 corpus đã tồn tại sẵn."""
    manifest = load_manifest()
    da_them = 0
    for base_name in list_all_chunk_base_names():
        if base_name in manifest:
            continue
        ensure_chunk_ids_and_status(base_name)
        set_all_chunks_status(base_name, "active")
        checksum = None
        md_path = get_markdown_path(base_name)
        if md_path.exists():
            checksum = compute_checksum(md_path.read_text(encoding="utf-8"))
        manifest[base_name] = {
            "checksum": checksum,
            "trang_thai_van_ban": "active",
            "cap_nhat_luc": _now(),
            "nguon_goc": resolve_source_location(base_name),
            "ghi_chu": "bootstrap - chưa qua kiểm tra xung đột",
        }
        da_them += 1
    save_manifest(manifest)
    print(f"✅ Đã đăng ký {da_them} văn bản có sẵn vào manifest (trang_thai_van_ban=active).")


def _maybe_rebuild_now(reason: str = "thay_the"):
    """Chủ động HỎI (không chỉ in nhắc suông) có muốn rebuild-index + báo
    api8923.py nạp lại NGAY. Áp dụng cho CẢ 2 tình huống, không chỉ khi có
    nội dung bị vô hiệu hóa:
    - reason="thay_the": vừa có quyết định thay thế/vô hiệu hóa nội dung cũ
      -> rủi ro chatbot trả lời bằng dữ liệu SAI (đã bị thay thế).
    - reason="noi_dung_moi": vừa thêm nội dung MỚI hoàn toàn (nhánh "không
      có ứng viên") -> nội dung này chưa vào FAISS, hỏi sẽ không tìm thấy
      gì liên quan và hybrid_search có thể trả về hàng xóm không liên quan.
      Cần rebuild trước khi coi là dùng được.

    [FIX - BUG NGHIÊM TRỌNG] Trước đây hàm này LUÔN hỏi qua _ask_yes_no(),
    dùng CHUNG cơ chế auto-answer với các câu hỏi "thay thế/giữ bản cũ" ở
    admin_api.py (run_cmd_auto với 1 yn_default DUY NHẤT cho toàn bộ script).
    Web UI mặc định gọi mode="safe" -> yn_default="n" cho MỌI câu hỏi y/n
    trong cả lượt chạy 'check' - bao gồm CẢ câu hỏi "rebuild NGAY BÂY GIỜ?"
    ở đây, dù câu hỏi này KHÔNG liên quan gì đến việc chọn thay thế/giữ bản
    cũ. Hậu quả: với văn bản HOÀN TOÀN MỚI (nhánh phổ biến nhất, không có
    xung đột gì) chạy qua web, manifest.json VẪN được cập nhật (nên UI báo
    "✅ Đã ở production") nhưng FAISS KHÔNG BAO GIỜ được rebuild - chunk mới
    không thực sự vào chatbot được, đúng hiện tượng "chạy test xung đột
    nhưng không thấy hiện tượng gì" (vì thực ra CHẲNG có gì được rebuild).

    Sửa: câu hỏi "rebuild ngay?" không mang rủi ro mất dữ liệu như câu hỏi
    thay thế/giữ bản cũ (chỉ là câu hỏi về THỜI ĐIỂM chạy 1 tác vụ tốn thời
    gian) - nên khi phát hiện đang chạy KHÔNG tương tác (stdin không phải
    tty, tức đang bị gọi từ subprocess của admin_api.py qua web), tự động
    rebuild NGAY, không hỏi. Khi chạy tay thật ở terminal (stdin là tty),
    vẫn hỏi như cũ để Admin tự quyết định thời điểm."""
    print()
    non_interactive = not sys.stdin.isatty()
    if non_interactive:
        print("→ Đang chạy không tương tác (vd từ Admin API/web) - tự 'rebuild-index' ngay, không hỏi.")
        rebuild_production_vectorstore()
        return
    if reason == "noi_dung_moi":
        cau_hoi = ("Chạy 'rebuild-index' + báo api8923.py nạp lại NGAY BÂY GIỜ, để nội dung MỚI vừa "
                   "thêm có thể được chatbot tìm thấy (nếu không, hỏi ngay bây giờ sẽ KHÔNG tìm thấy gì, "
                   "có thể trả lời nhầm sang nội dung khác không liên quan)?")
    else:
        cau_hoi = ("Chạy 'rebuild-index' + báo api8923.py nạp lại NGAY BÂY GIỜ, để đảm bảo chatbot "
                   "không còn trả lời bằng dữ liệu vừa bị thay thế/vô hiệu hóa?")
    if _ask_yes_no(cau_hoi):
        rebuild_production_vectorstore()
    else:
        print("⚠️  CHƯA rebuild-index - KHÔNG được coi lần kiểm tra xung đột này là đã 'lên production' "
              "cho tới khi tự chạy 'python conflict_detection.py rebuild-index'.")


def _phat_hien_xung_dot_cheo_cau_truc(base_name: str) -> list:
    """Chỉ PHÁT HIỆN (không hỏi, không quyết định gì) - đối chiếu các đoạn
    ĐANG HIỆU LỰC (active) của base_name với bảng cấu trúc production, trả
    về list[dict] xung đột khả nghi. Tách riêng khỏi phần hỏi/quyết định
    để dùng lại được cho cả luồng CLI tương tác VÀ luồng web (ghi nhận rồi
    cho Admin duyệt sau, xem _doi_chieu_du_lieu_co_cau_truc/pending-cross)."""
    try:
        import structured_conflict_detection as scd
    except Exception as e:
        print(f"   (Bỏ qua đối chiếu dữ liệu có cấu trúc - không tải được module: {e})")
        return []

    active_chunks = [c for c in load_chunk_file(base_name) if c.get("metadata", {}).get("trang_thai") == "active"]
    if not active_chunks:
        return []

    print(f"\n→ Đối chiếu {len(active_chunks)} đoạn đang hiệu lực của '{base_name}' với dữ liệu có cấu trúc...")
    try:
        xung_dot_list = scd.doi_chieu_chunks_voi_bang(active_chunks)
    except Exception as e:
        print(f"   (Lỗi khi đối chiếu dữ liệu có cấu trúc, bỏ qua bước này: {e})")
        return []
    return xung_dot_list


_PENDING_CROSS_FOLDER = conflict_folder / "pending_cross"


def _luu_pending_cross_conflicts(base_name: str, xung_dot_list: list):
    """Ghi các xung đột chéo PHÁT HIỆN được (chưa quyết định gì) ra file
    riêng cho từng base_name - để màn hình web (/admin/pending-cross-
    conflicts) đọc lại và cho Admin duyệt SAU, không phải quyết định ngay
    lúc chạy 'check' (đúng pattern canh_bao_cheo bên bảng cấu trúc)."""
    _PENDING_CROSS_FOLDER.mkdir(parents=True, exist_ok=True)
    active_chunks = {c["metadata"]["chunk_id"]: c for c in load_chunk_file(base_name)
                     if c.get("metadata", {}).get("trang_thai") == "active"}
    items = []
    for i, xd in enumerate(xung_dot_list):
        chunk = active_chunks.get(xd.get("chunk_id"), {})
        items.append({**xd, "warning_id": i, "base_name": base_name,
                      "noi_dung_day_du": chunk.get("content", "")})
    path = _PENDING_CROSS_FOLDER / f"{base_name}.json"
    path.write_text(json.dumps({"base_name": base_name, "luc_phat_hien": datetime.now(timezone.utc).isoformat(),
                                 "items": items}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"   → Đã ghi {len(items)} cảnh báo chéo chờ duyệt vào {path.name} (xem trên web UI, "
          f"mục 'Xung đột chéo với dữ liệu có cấu trúc').")


def doc_pending_cross_conflicts_web(base_name: Optional[str] = None) -> dict:
    """Đọc lại các xung đột chéo đang CHỜ Admin duyệt - tất cả base_name
    nếu không chỉ định, hoặc 1 base_name cụ thể."""
    _PENDING_CROSS_FOLDER.mkdir(parents=True, exist_ok=True)
    files = [_PENDING_CROSS_FOLDER / f"{base_name}.json"] if base_name else sorted(_PENDING_CROSS_FOLDER.glob("*.json"))
    groups = []
    for f in files:
        if not f.exists():
            continue
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        if data.get("items"):
            groups.append(data)
    return {"ok": True, "groups": groups,
            "tong_so": sum(len(g["items"]) for g in groups)}


def ap_dung_quyet_dinh_cheo_cau_truc_web(base_name: str, decisions: list) -> dict:
    """Áp dụng quyết định Admin chọn trên web UI cho các cảnh báo chéo
    ĐANG CHỜ của 1 base_name (xem doc_pending_cross_conflicts_web) - xóa
    khỏi file pending sau khi áp dụng xong.

    decisions = [{...các field cảnh báo gốc..., "action":
                  "giu_doan|loai_doan|sua_doan|sua_bang", "noi_dung_moi": "..."}]"""
    import structured_conflict_detection as scd
    so_ap_dung = 0
    for xd in decisions:
        action = xd.get("action", "giu_doan")
        if action == "loai_doan":
            set_chunk_status(base_name, {xd["chunk_id"]}, "bi_bo_qua")
            log_decision({"su_kien": "xung_dot_cheo_loai_doan_web", "base_name": base_name,
                          **{k: v for k, v in xd.items() if k != "noi_dung_day_du"}})
        elif action == "sua_doan":
            noi_dung_moi = (xd.get("noi_dung_moi") or "").strip()
            if noi_dung_moi:
                set_chunk_content(base_name, xd["chunk_id"], noi_dung_moi)
                log_decision({"su_kien": "xung_dot_cheo_sua_noi_dung_web", "base_name": base_name,
                              "noi_dung_moi": noi_dung_moi,
                              **{k: v for k, v in xd.items() if k != "noi_dung_day_du"}})
            else:
                action = "giu_doan"
        elif action == "sua_bang":
            registry = scd.load_registry()
            table = xd.get("table")
            cfg = registry.get(table)
            if cfg:
                df = scd.load_table_df(table, registry)
                pk_col = cfg["primary_key"]
                mask = df[pk_col].astype(str) == str(xd.get("pk_value"))
                if mask.any():
                    df.loc[mask, xd["cot"]] = xd.get("gia_tri_van_ban")
                    scd.save_table_df(table, df, registry)
                    print(f"   ✅ Đã sửa '{xd['cot']}' của khóa {xd.get('pk_value')} trong bảng "
                          f"'{table}' theo văn bản.")
                else:
                    print(f"   ⚠️  Không tìm thấy khóa {xd.get('pk_value')} trong bảng '{table}' "
                          f"(có thể đã đổi) - bỏ qua sửa bảng cho mục này.")
            log_decision({"su_kien": "xung_dot_cheo_sua_bang_web", "base_name": base_name,
                          **{k: v for k, v in xd.items() if k != "noi_dung_day_du"}})
        else:
            log_decision({"su_kien": "xung_dot_cheo_giu_doan_web", "base_name": base_name,
                          **{k: v for k, v in xd.items() if k != "noi_dung_day_du"}})
        so_ap_dung += 1

    # Xóa các mục đã xử lý khỏi file pending - nếu còn mục khác chưa xử lý
    # (vd Admin chỉ duyệt 1 phần) thì GIỮ LẠI những mục chưa được gửi quyết định.
    path = _PENDING_CROSS_FOLDER / f"{base_name}.json"
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            done_ids = {xd.get("warning_id") for xd in decisions}
            remaining = [it for it in data.get("items", []) if it.get("warning_id") not in done_ids]
            if remaining:
                data["items"] = remaining
                path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            else:
                path.unlink()
        except Exception:
            pass
    _notify_api8923_reload()
    return {"ok": True, "base_name": base_name, "so_ap_dung": so_ap_dung}


def _doi_chieu_du_lieu_co_cau_truc(base_name: str, auto: Optional[str] = None):
    """Bước 9-11 sơ đồ 1 ("Phát hiện xung đột với dữ liệu có cấu trúc") -
    đối chiếu các đoạn ĐANG HIỆU LỰC (active) của base_name với bảng cấu
    trúc production, hỏi Admin nếu phát hiện khả năng xung đột chéo.

    auto=None (chạy CLI terminal): hỏi tương tác NGAY, y hệt hành vi cũ.
    auto="safe"/"replace" (chạy job web): KHÔNG hỏi qua stdin (sẽ treo vì
    admin_api.py không có mẫu tự-trả-lời cho prompt "Chọn 1/2/3:") - GHI
    LẠI các xung đột phát hiện được vào file pending để Admin duyệt SAU
    trên web UI (mục 'Xung đột chéo với dữ liệu có cấu trúc'), ĐÚNG pattern
    canh_bao_cheo bên bảng cấu trúc, thay vì tự mặc định 'giữ nguyên' âm
    thầm như bản trước (Admin không biết gì để mà duyệt lại)."""
    xung_dot_list = _phat_hien_xung_dot_cheo_cau_truc(base_name)
    if not xung_dot_list:
        print("   Không có xung đột chéo với dữ liệu có cấu trúc.")
        return

    if auto:
        print(f"⚠️  {len(xung_dot_list)} khả năng xung đột CHÉO với dữ liệu có cấu trúc - "
              f"ĐÃ GHI LẠI để Admin duyệt trên web UI (không tự quyết định gì ở đây).")
        _luu_pending_cross_conflicts(base_name, xung_dot_list)
        return

    print(f"⚠️  {len(xung_dot_list)} khả năng xung đột CHÉO với dữ liệu có cấu trúc:")
    active_chunks = [c for c in load_chunk_file(base_name) if c.get("metadata", {}).get("trang_thai") == "active"]
    chunks_theo_id = {c["metadata"]["chunk_id"]: c for c in active_chunks}
    for xd in xung_dot_list:
        print("\n" + "-" * 60)
        print(f"   Đối tượng: {xd['ten_thuc_the']}  (bảng '{xd['table']}', khóa={xd['pk_value']})")
        print(f"   Thuộc tính: {xd['cot']}")
        print(f"   Giá trị hiện hành trong bảng: {xd['gia_tri_bang']}")
        print(f"   Giá trị trong văn bản mới: {xd['gia_tri_van_ban']}")
        print(f"   Trích dẫn: \"{xd['trich_dan']}\"")
        chunk_hien_tai = chunks_theo_id.get(xd["chunk_id"])
        if chunk_hien_tai:
            print(f"   [Toàn văn đoạn hiện tại] {chunk_hien_tai.get('content', '')[:600]}")

        # 4 lựa chọn: giữ đoạn / loại đoạn / sửa đoạn / sửa bảng - cho phép
        # Admin sửa 1 câu sai mà không phải loại cả đoạn, HOẶC nếu văn bản
        # đúng hơn thì sửa NGAY giá trị trong bảng (đối xứng với 4 lựa chọn
        # bên hướng bảng->RAG, xem doi_chieu_dong_bang_voi_rag).
        print("   [1] Giữ đoạn nguyên trạng   [2] Loại đoạn khỏi production   "
              "[3] Sửa lại nội dung đoạn ngay   [4] Sửa giá trị bảng theo văn bản")
        while True:
            lua_chon = input("   Chọn 1/2/3/4: ").strip()
            if lua_chon in ("1", "2", "3", "4"):
                break
            print("   Vui lòng nhập 1, 2, 3 hoặc 4.")

        if lua_chon == "1":
            print("   ✅ Giữ đoạn trong production. LƯU Ý: dòng cảnh báo này CHỈ mang tính thông "
                  "tin - nếu bảng cần cập nhật theo văn bản này, xử lý qua "
                  "'python structured_data_pipeline.py add' - hệ thống KHÔNG tự động sửa bảng.")
            log_decision({"su_kien": "xung_dot_cheo_giu_doan", "base_name": base_name, **xd})
        elif lua_chon == "2":
            set_chunk_status(base_name, {xd["chunk_id"]}, "bi_bo_qua")
            print("   ✅ Đã loại đoạn này khỏi phần được thêm vào production.")
            log_decision({"su_kien": "xung_dot_cheo_loai_doan", "base_name": base_name, **xd})
        elif lua_chon == "3":
            noi_dung_moi = input("   Nhập nội dung ĐÃ SỬA cho đoạn này (dán toàn bộ đoạn, để trống để hủy): ").strip()
            if not noi_dung_moi:
                print("   Nội dung trống - hủy sửa, giữ nguyên đoạn cũ (chưa lưu gì).")
                continue
            set_chunk_content(base_name, xd["chunk_id"], noi_dung_moi)
            print("   ✅ Đã lưu nội dung đã sửa - đoạn vẫn active trong production với nội dung mới.")
            log_decision({"su_kien": "xung_dot_cheo_sua_noi_dung", "base_name": base_name,
                          "noi_dung_moi": noi_dung_moi, **xd})
        else:
            import structured_conflict_detection as scd
            registry = scd.load_registry()
            cfg = registry.get(xd["table"])
            if cfg:
                df = scd.load_table_df(xd["table"], registry)
                pk_col = cfg["primary_key"]
                mask = df[pk_col].astype(str) == str(xd["pk_value"])
                if mask.any():
                    df.loc[mask, xd["cot"]] = xd["gia_tri_van_ban"]
                    scd.save_table_df(xd["table"], df, registry)
                    print(f"   ✅ Đã sửa '{xd['cot']}' của khóa {xd['pk_value']} trong bảng "
                          f"'{xd['table']}' theo văn bản.")
                else:
                    print(f"   ⚠️  Không tìm thấy khóa {xd['pk_value']} trong bảng '{xd['table']}'.")
            log_decision({"su_kien": "xung_dot_cheo_sua_bang", "base_name": base_name, **xd})


def process_new_document(base_name: str, on_chunk_approved=None, auto: Optional[str] = None) -> dict:
    """
    Chạy TOÀN BỘ quy trình bước 1-8 cho 1 văn bản mới.

    on_chunk_approved: callback tùy chọn (chunk_id, source_file) -> None, gọi
    SAU KHI 1 chunk được Admin duyệt đưa vào production. Đây là điểm nối để
    module đối chiếu dữ liệu có cấu trúc (bước 9-11, module riêng) chạy tiếp
    mà không cần sửa file này - CHƯA được gọi ở bất kỳ đâu trong hàm này vì
    nằm ngoài phạm vi phi cấu trúc, để sẵn cho việc tích hợp sau.

    auto: "safe"|"replace"|None - truyền xuống bước đối chiếu chéo dữ liệu
    có cấu trúc (_doi_chieu_du_lieu_co_cau_truc) để KHÔNG chờ input() khi
    gọi từ web (xem ghi chú [FIX] ở hàm đó).
    """
    manifest = load_manifest()

    print(f"\n{'#' * 60}\n# KIỂM TRA XUNG ĐỘT PHI CẤU TRÚC: {base_name}\n{'#' * 60}")

    if not manifest:
        print("⚠️  manifest.json đang RỖNG - chưa văn bản nào được đăng ký là 'production'.")
        print("    Nếu thư mục chunks/ đã có sẵn dữ liệu từ trước, việc so sánh sẽ LUÔN")
        print("    trả về 'không có ứng viên' cho tới khi bạn chạy:")
        print("        python conflict_detection.py bootstrap")
        print("    rồi chạy lại lệnh này.\n")

    # ---------- Bước 1: chuẩn hóa + checksum ----------
    print(f"→ [B1] Chuẩn hóa + checksum...")
    new_checksum = compute_new_document_checksum(base_name)
    new_chunks = ensure_chunk_ids_and_status(base_name)

    # ---------- Bước 2: kiểm tra trùng lặp tuyệt đối ----------
    print(f"→ [B2] Kiểm tra trùng lặp tuyệt đối...")
    old_bases = find_exact_duplicate(new_checksum, manifest)
    if old_bases:
        ket_qua = handle_absolute_duplicate(base_name, old_bases, manifest)
        save_manifest(manifest)
        if base_name in manifest or "thay_the" in ket_qua.values():
            _maybe_rebuild_now()
        return {"ket_qua": ket_qua}

    print("→ Không trùng tuyệt đối với văn bản nào trong production. Tìm văn bản liên quan bằng vector search...")

    # ---------- Bước 3: tìm file ứng viên ----------
    print(f"→ [B3] Vector search tìm ứng viên...")
    candidates = find_candidate_files(new_chunks, manifest, exclude_base=base_name)

    if not candidates:
        print("→ Không có văn bản nào đủ tương đồng trong production. Toàn bộ nội dung được coi là tri thức mới.")
        set_all_chunks_status(base_name, "active")
        # [FIX] Dùng **manifest.get(base_name, {}) để UPSERT thay vì luôn tạo
        # entry mới tinh: nếu base_name ĐÃ có trong manifest (đang rà soát
        # lại 1 văn bản production vừa bị Admin sửa tay .md), phải cập nhật
        # checksum theo nội dung MỚI - trước đây nhánh này ghi đè toàn bộ
        # nên vẫn "vô tình" đúng cho case update checksum, nhưng ghi đè mất
        # các field khác (vd 'lien_quan_toi' cũ) - giữ lại cho an toàn.
        manifest[base_name] = {
            **manifest.get(base_name, {}),
            "checksum": new_checksum,
            "trang_thai_van_ban": "active",
            "cap_nhat_luc": _now(),
            "nguon_goc": resolve_source_location(base_name),
        }
        save_manifest(manifest)
        log_decision({"su_kien": "khong_co_ung_vien", "moi": base_name})
        # Văn bản hoàn toàn mới vẫn cần đối chiếu với bảng cấu trúc - không
        # trùng với RAG nào không có nghĩa là không xung đột với bảng.
        _doi_chieu_du_lieu_co_cau_truc(base_name, auto=auto)
        _maybe_rebuild_now(reason="noi_dung_moi")
        return {"ket_qua": "khong_co_ung_vien"}

    # ---------- Bước 4-8: LLM phân tích + Admin quyết định từng file ứng viên ----------
    print(f"→ Tìm thấy {len(candidates)} file ứng viên có khả năng liên quan/xung đột.")
    ket_qua_tung_file = {}
    van_ban_bi_thay_the_hoan_toan = False
    # Dùng chung cho toàn bộ candidate của lượt check-file này - xem docstring
    # review_candidate_file().
    chunks_admin_da_loai = set()
    for source_file, info in candidates.items():
        ket_qua = review_candidate_file(base_name, source_file, info, manifest, chunks_admin_da_loai)
        ket_qua_tung_file[source_file] = ket_qua
        if manifest.get(source_file, {}).get("trang_thai_van_ban") == "thay_the":
            van_ban_bi_thay_the_hoan_toan = True

    # Chunk mới không nằm trong bất kỳ cặp ứng viên nào (nội dung mới hoàn
    # toàn, không đụng tới phần nào đã có) -> tự động active.
    matched_new_ids = {nc["metadata"]["chunk_id"]
                        for info in candidates.values() for nc, _, _ in info["cap_doan"]}
    chunks_now = load_chunk_file(base_name)
    thay_doi = False
    for c in chunks_now:
        md = c["metadata"]
        if md["chunk_id"] not in matched_new_ids and md.get("trang_thai") not in ("active", "bi_bo_qua", "vo_hieu_hoa"):
            md["trang_thai"] = "active"
            thay_doi = True
    if thay_doi:
        save_chunk_file(base_name, chunks_now)

    # ---------- Cập nhật manifest cho văn bản mới HOẶC văn bản đang rà soát lại ----------
    # [FIX] Trước đây chỉ tạo entry khi 'base_name not in manifest' - nghĩa
    # là chạy lại 'check'/'check-file' cho 1 văn bản ĐÃ production (sau khi
    # sửa tay .md) không hề cập nhật checksum trong manifest. Hậu quả: lần
    # kiểm tra trùng lặp SAU đó vẫn so sánh với checksum CŨ (trước khi sửa),
    # khiến 1 bản sao trùng nội dung MỚI không bị phát hiện là trùng - đúng
    # hiện tượng "test trùng lặp không thấy gì" nếu văn bản gốc dùng để so
    # sánh từng bị sửa tay mà chưa rà soát lại. Nay LUÔN cập nhật checksum +
    # thời điểm, chỉ nâng cấp (không hạ cấp) trạng thái nếu văn bản này vừa
    # thay thế hoàn toàn 1 file khác trong lượt chạy này.
    manifest[base_name] = {
        **manifest.get(base_name, {}),
        "checksum": new_checksum,
        "trang_thai_van_ban": ("thay_the" if van_ban_bi_thay_the_hoan_toan
                               else manifest.get(base_name, {}).get("trang_thai_van_ban", "active")),
        "cap_nhat_luc": _now(),
        "nguon_goc": resolve_source_location(base_name),
        "lien_quan_toi": list(candidates.keys()),
    }
    save_manifest(manifest)

    print(f"\n✅ Hoàn tất kiểm tra xung đột phi cấu trúc cho '{base_name}'.")
    _doi_chieu_du_lieu_co_cau_truc(base_name, auto=auto)
    _maybe_rebuild_now()
    return {"ket_qua": "da_xu_ly", "ung_vien": ket_qua_tung_file}


# ============================================================
# CẬP NHẬT FAISS — embed lại chỉ những chunk đang active
# ============================================================

def _notify_api8923_reload():
    """Gọi /admin/reload-index của api8923.py NGAY SAU khi ghi FAISS mới ra
    đĩa. BẮT BUỘC PHẢI CÓ bước này: nếu không, chatbot ĐANG CHẠY (nếu có)
    tiếp tục trả lời bằng FAISS CŨ đã load lúc khởi động process - đúng kẽ
    hở có thể khiến sinh viên hỏi vẫn nhận câu trả lời theo quy chế ĐÃ BỊ
    ADMIN THAY THẾ, dù conflict_detection.py đã xử lý đúng. Best-effort: nếu
    api8923.py không chạy/endpoint lỗi, PHẢI cảnh báo THẬT RÕ (không được im
    lặng coi như đã xong) để Admin biết còn 1 bước thủ công (khởi động lại
    api8923.py) phải làm trước khi tin tưởng dữ liệu mới đã thực sự lên
    production."""
    api_base = os.getenv("API8923_BASE_URL", "http://127.0.0.1:8923")
    api_token = os.getenv("API_AUTH_TOKEN")
    if not api_token:
        print("⚠️  Chưa set API_AUTH_TOKEN trong .env - KHÔNG tự gọi được /admin/reload-index của "
              "api8923.py. Nếu chatbot đang chạy, nó VẪN DÙNG FAISS CŨ - phải tự khởi động lại "
              "api8923.py thủ công trước khi tin tưởng dữ liệu mới.")
        return
    try:
        import urllib.request
        req = urllib.request.Request(
            f"{api_base}/admin/reload-index", method="POST", headers={"X-API-Key": api_token},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode("utf-8")
        print(f"✅ Đã báo api8923.py nạp lại FAISS mới (không cần khởi động lại process): {body}")
    except Exception as e:
        print(f"⚠️  KHÔNG gọi được /admin/reload-index của api8923.py tại {api_base} ({e}). "
              f"RẤT CÓ THỂ chatbot đang chạy vẫn trả lời bằng FAISS CŨ - phải TỰ khởi động lại "
              f"api8923.py (hoặc kiểm tra api8923.py có đang chạy không / API8923_BASE_URL có đúng "
              f"không) TRƯỚC KHI coi dữ liệu mới đã thực sự lên production.")


def rebuild_production_vectorstore():
    """Embed lại TOÀN BỘ chunk đang trang_thai=active vào FAISS, lưu đúng
    VECTOR_DB_PATH mà api8923.py đang đọc - tương đương build_vectorstore.py
    gốc nhưng có LỌC theo trạng thái hiệu lực (build_vectorstore.py gốc nạp
    toàn bộ chunk trong thư mục, không phân biệt active/thay_the/vo_hieu_hoa)."""
    from langchain_core.documents import Document
    from langchain_community.vectorstores import FAISS

    manifest = load_manifest()
    docs = []
    bo_qua = []
    for base_name in manifest:
        try:
            chunks = load_chunk_file(base_name)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            # [FIX] Cùng lý do với load_active_production_chunks() - trước
            # đây KHÔNG có try/except nên 1 manifest entry mồ côi sẽ làm
            # crash toàn bộ rebuild-index (kể cả khi chỉ muốn rebuild cho
            # NHỮNG văn bản còn nguyên vẹn) - rất dễ gặp giữa lúc test
            # (xóa tay file chunk) rồi quên chạy 'prune' trước rebuild.
            bo_qua.append(base_name)
            continue
        for c in chunks:
            md = c.get("metadata", {})
            if md.get("trang_thai") == "active":
                docs.append(Document(page_content=c["content"], metadata=md))

    if bo_qua:
        print(f"⚠️  Bỏ qua {len(bo_qua)} văn bản mồ côi trong manifest (file chunk không còn tồn tại/hỏng): "
              f"{', '.join(bo_qua)}. Chạy 'python conflict_detection.py prune' để dọn manifest.json.")

    if not docs:
        print("⚠️  Không có chunk nào đang active - không tạo FAISS.")
        return

    print(f"-> Embed {len(docs)} chunk active bằng '{EMBED_MODEL}' (có thể mất vài phút)...")
    embeddings = get_embeddings_client()
    vector_store = FAISS.from_documents(docs, embeddings)
    print(f"-> Đang ghi FAISS index ra đĩa...")
    os.makedirs(VECTOR_DB_PATH, exist_ok=True)
    vector_store.save_local(VECTOR_DB_PATH)
    print(f"✅ Đã lưu FAISS index tại: {VECTOR_DB_PATH}")
    print("-> Báo api8923.py reload...")
    _notify_api8923_reload()

# ============================================================
# WEB-UI MODE — analyze & apply không tương tác (cho admin_api.py)
# ============================================================

def analyze_conflicts(base_name: str) -> dict:
    manifest = load_manifest()
    print(f"\n{'#' * 60}\n# PHÂN TÍCH XUNG ĐỘT: {base_name}\n{'#' * 60}")

    # ---- B1 ----
    print("→ [B1] Chuẩn hóa + tính checksum...")
    new_checksum = compute_new_document_checksum(base_name)
    print(f"   ✓ Checksum: {new_checksum[:16]}…")
    new_chunks = ensure_chunk_ids_and_status(base_name)
    print(f"   ✓ {len(new_chunks)} chunk đã có chunk_id + trang_thai")

    result = {
        "base_name": base_name, "checksum": new_checksum,
        "in_manifest": base_name in manifest,
        "exact_duplicates": [], "candidates": [],
        "canh_bao_trung_lap_toan_file": [],  # [MỚI]
    }

    # ---- B2 ----
    print("→ [B2] Kiểm tra trùng lặp tuyệt đối...")
    exact_dups = find_exact_duplicate(new_checksum, manifest)
    if exact_dups:
        print(f"   ⚠️  TRÙNG TUYỆT ĐỐI với {len(exact_dups)} văn bản:")
        for ob in exact_dups:
            print(f"      - {ob}")
    else:
        print("   ✓ Không trùng tuyệt đối.")

    for old_bn in exact_dups:
        info = manifest.get(old_bn, {})
        sample_meta, n_active = {}, 0
        try:
            old_chunks = load_chunk_file(old_bn)
            if old_chunks:
                m = old_chunks[0].get("metadata", {})
                sample_meta = {k: m.get(k) for k in
                               ("tieu_de", "so_hieu", "loai_van_ban", "ngay_ban_hanh")}
            n_active = sum(1 for c in old_chunks
                           if c.get("metadata", {}).get("trang_thai") == "active")
        except Exception:
            pass
        result["exact_duplicates"].append({
            "base_name": old_bn,
            "trang_thai_van_ban": info.get("trang_thai_van_ban"),
            "cap_nhat_luc": info.get("cap_nhat_luc"),
            "nguon_goc": info.get("nguon_goc") or resolve_source_location(old_bn),
            "sample_metadata": sample_meta,
            "n_active_chunks": n_active,
        })

    if result["exact_duplicates"]:
        print("\n→ Dừng tại B2: có trùng lặp tuyệt đối, KHÔNG cần vector search.")
        return result

    # ---- B3 ----
    print("→ [B3] Vector search tìm văn bản liên quan...")
    candidates = find_candidate_files(new_chunks, manifest, exclude_base=base_name)
    if not candidates:
        print("   ✓ Không có văn bản nào đủ tương đồng. Coi như tri thức mới.")
        return result

    # [MỚI] Tách riêng cảnh báo "khả năng trùng lặp toàn file" ra đầu kết
    # quả, để Admin/UI thấy ngay mà không phải đọc hết từng cặp đoạn ở dưới.
    result["canh_bao_trung_lap_toan_file"] = [
        {"source_file": sf, "ty_le_trung": info["ty_le_trung"],
         "so_doan_trung_cao": info["so_doan_trung_cao"],
         "tong_so_doan_moi_hop_le": info["tong_so_doan_moi_hop_le"]}
        for sf, info in candidates.items() if info["rat_co_kha_nang_trung_lap_toan_file"]
    ]

    # ---- B4 ----
    total_pairs_all = sum(len(info["cap_doan"]) for info in candidates.values())
    pair_idx = 0
    print(f"\n→ [B4] Gọi LLM phân tích {total_pairs_all} cặp đoạn "
          f"trên {len(candidates)} file ứng viên (mỗi cặp ~10-30s)...")

    for source_file, info in candidates.items():
        sample_old_md = info["cap_doan"][0][1].get("metadata", {})
        analyzed_pairs = []
        print(f"\n── File ứng viên: {source_file} ({len(info['cap_doan'])} cặp) ──")
        for new_chunk, old_chunk, score in info["cap_doan"]:
            pair_idx += 1
            print(f"   [{pair_idx}/{total_pairs_all}] score={score:.2f} "
                  f"new={new_chunk['metadata']['chunk_id']} <-> "
                  f"old={old_chunk['metadata']['chunk_id']}...")
            analysis = llm_analyze_pair(new_chunk, old_chunk)
            print(f"     ↳ quan_he={analysis.get('quan_he')} "
                  f"(chắc chắn: {analysis.get('do_chac_chan','?')}) "
                  f"cung_thuc_the={analysis.get('cung_thuc_the','?')}")
            if analysis.get("cung_thuc_the") is False:
                print("     → Bỏ qua: LLM xác nhận 2 đoạn thuộc THỰC THỂ KHÁC.")
                continue
            analyzed_pairs.append({
                "new_chunk_id": new_chunk["metadata"]["chunk_id"],
                "old_chunk_id": old_chunk["metadata"]["chunk_id"],
                "score": round(float(score), 4),
                "analysis": analysis,
                "new_content": new_chunk["content"],
                "old_content": old_chunk["content"],
                "new_metadata": {k: v for k, v in new_chunk.get("metadata", {}).items()
                                 if k in ("chunk_id", "chuong_hoac_muc", "dieu",
                                          "muc_con", "trang")},
                "old_metadata": {k: v for k, v in old_chunk.get("metadata", {}).items()
                                 if k in ("chunk_id", "chuong_hoac_muc", "dieu",
                                          "muc_con", "trang")},
            })
        print(f"   → Giữ {len(analyzed_pairs)}/{len(info['cap_doan'])} cặp sau khi lọc thực thể.")
        if not analyzed_pairs:
            continue
        nguon = (manifest.get(source_file, {}).get("nguon_goc")
                 or resolve_source_location(source_file))
        result["candidates"].append({
            "source_file": source_file,
            "diem": round(float(info["diem"]), 4),
            "ty_le_trung": info["ty_le_trung"],  # [MỚI]
            "rat_co_kha_nang_trung_lap_toan_file": info["rat_co_kha_nang_trung_lap_toan_file"],  # [MỚI]
            "sample_metadata": {
                "tieu_de": sample_old_md.get("tieu_de"),
                "so_hieu": sample_old_md.get("so_hieu"),
                "loai_van_ban": sample_old_md.get("loai_van_ban"),
                "ngay_ban_hanh": sample_old_md.get("ngay_ban_hanh"),
            },
            "nguon_goc": nguon,
            "cap_doan": analyzed_pairs,
        })

    print(f"\n✅ Phân tích xong {pair_idx} cặp ({len(result['candidates'])} file có xung đột thực thể).")
    return result

def quick_scan(base_name: str) -> dict:
    """B1+B2+B3 KHÔNG gọi LLM — cho web UI hiện dialog cấp file trước khi
    hỏi Admin có muốn xuống cấp đoạn (tức là có cần gọi LLM hay không)."""
    manifest = load_manifest()
    print(f"\n{'#' * 60}\n# QUICK SCAN: {base_name}\n{'#' * 60}")

    print("→ [B1] Chuẩn hóa + tính checksum...")
    new_checksum = compute_new_document_checksum(base_name)
    print(f"   ✓ Checksum: {new_checksum[:16]}…")
    new_chunks = ensure_chunk_ids_and_status(base_name)
    print(f"   ✓ {len(new_chunks)} chunk đã có chunk_id + trang_thai")

    result = {
        "base_name": base_name, "checksum": new_checksum,
        "in_manifest": base_name in manifest,
        "exact_duplicates": [], "candidate_files": [],
        "n_pairs_to_analyze": 0,
        "canh_bao_trung_lap_toan_file": [],  # [MỚI]
    }

    print("→ [B2] Kiểm tra trùng lặp tuyệt đối...")
    old_bases = find_exact_duplicate(new_checksum, manifest)
    if old_bases:
        print(f"   ⚠️  TRÙNG TUYỆT ĐỐI với {len(old_bases)} văn bản:")
        for ob in old_bases:
            print(f"      - {ob}")
    else:
        print("   ✓ Không trùng tuyệt đối.")

    for old_bn in old_bases:
        info = manifest.get(old_bn, {})
        sample_meta, n_active = {}, 0
        try:
            old_chunks = load_chunk_file(old_bn)
            if old_chunks:
                m = old_chunks[0].get("metadata", {})
                sample_meta = {k: m.get(k) for k in
                               ("tieu_de", "so_hieu", "loai_van_ban", "ngay_ban_hanh")}
            n_active = sum(1 for c in old_chunks
                           if c.get("metadata", {}).get("trang_thai") == "active")
        except Exception:
            pass
        result["exact_duplicates"].append({
            "base_name": old_bn,
            "trang_thai_van_ban": info.get("trang_thai_van_ban"),
            "cap_nhat_luc": info.get("cap_nhat_luc"),
            "nguon_goc": info.get("nguon_goc") or resolve_source_location(old_bn),
            "sample_metadata": sample_meta,
            "n_active_chunks": n_active,
        })

    if result["exact_duplicates"]:
        print("\n→ Dừng quick scan: có trùng lặp tuyệt đối, KHÔNG cần vector search.")
        return result

    print("→ [B3] Vector search tìm văn bản liên quan (CHƯA gọi LLM)...")
    candidates = find_candidate_files(new_chunks, manifest, exclude_base=base_name)
    if not candidates:
        print("   ✓ Không có văn bản nào đủ tương đồng.")
        return result

    # [MỚI] Cảnh báo trùng lặp toàn file, tách riêng để hiện ngay đầu kết quả.
    result["canh_bao_trung_lap_toan_file"] = [
        {"source_file": sf, "ty_le_trung": info["ty_le_trung"],
         "so_doan_trung_cao": info["so_doan_trung_cao"],
         "tong_so_doan_moi_hop_le": info["tong_so_doan_moi_hop_le"]}
        for sf, info in candidates.items() if info["rat_co_kha_nang_trung_lap_toan_file"]
    ]

    for source_file, info in candidates.items():
        sample_old_md = info["cap_doan"][0][1].get("metadata", {})
        nguon = (manifest.get(source_file, {}).get("nguon_goc")
                 or resolve_source_location(source_file))
        result["candidate_files"].append({
            "source_file": source_file,
            "diem": round(float(info["diem"]), 4),
            "n_cap_doan": len(info["cap_doan"]),
            "ty_le_trung": info["ty_le_trung"],  # [MỚI]
            "rat_co_kha_nang_trung_lap_toan_file": info["rat_co_kha_nang_trung_lap_toan_file"],  # [MỚI]
            "sample_metadata": {
                "tieu_de": sample_old_md.get("tieu_de"),
                "so_hieu": sample_old_md.get("so_hieu"),
                "loai_van_ban": sample_old_md.get("loai_van_ban"),
                "ngay_ban_hanh": sample_old_md.get("ngay_ban_hanh"),
            },
            "nguon_goc": nguon,
        })

    result["n_pairs_to_analyze"] = sum(c["n_cap_doan"] for c in result["candidate_files"])
    print(f"   ✓ {len(result['candidate_files'])} file ứng viên, "
          f"{result['n_pairs_to_analyze']} cặp đoạn sẽ phân tích NẾU chọn 'xuống cấp đoạn'.")
    print("→ Quick scan xong. Chờ Admin quyết định cấp file trước khi gọi LLM.")
    return result


def analyze_pair_subset(base_name: str, source_files: list) -> dict:
    """B4 CHỈ cho các file trong source_files — dùng sau khi Admin đã xác nhận
    'xuống cấp đoạn' cho 1 tập file cụ thể trong UI."""
    manifest = load_manifest()
    new_chunks = ensure_chunk_ids_and_status(base_name)

    print(f"\n{'#' * 60}\n# PHÂN TÍCH CẶP ĐOẠN (subset): {base_name}\n{'#' * 60}")
    print(f"→ Chỉ phân tích {len(source_files)} file: {source_files}")

    candidates_all = find_candidate_files(new_chunks, manifest, exclude_base=base_name)
    candidates = {k: v for k, v in candidates_all.items() if k in source_files}
    if not candidates:
        print("   ⚠️  Không có file nào khớp — manifest có thể đã đổi. Chạy lại quick scan.")
        return {"candidates": [], "n_pairs_analyzed": 0}

    total_pairs_all = sum(len(info["cap_doan"]) for info in candidates.values())
    pair_idx = 0
    result_candidates = []

    print(f"\n→ Bắt đầu gọi LLM cho {total_pairs_all} cặp "
          f"trên {len(candidates)} file (mỗi cặp ~10-30s với model local)...")

    for source_file, info in candidates.items():
        sample_old_md = info["cap_doan"][0][1].get("metadata", {})
        analyzed_pairs = []
        print(f"\n── File: {source_file} ({len(info['cap_doan'])} cặp) ──")
        for new_chunk, old_chunk, score in info["cap_doan"]:
            pair_idx += 1
            print(f"   [{pair_idx}/{total_pairs_all}] score={score:.2f}...")
            analysis = llm_analyze_pair(new_chunk, old_chunk)
            print(f"     ↳ quan_he={analysis.get('quan_he')} "
                  f"(chắc chắn: {analysis.get('do_chac_chan','?')}) "
                  f"cung_thuc_the={analysis.get('cung_thuc_the','?')}")
            if analysis.get("cung_thuc_the") is False:
                print("     → Bỏ qua: LLM xác nhận 2 đoạn thuộc THỰC THỂ KHÁC.")
                continue
            analyzed_pairs.append({
                "new_chunk_id": new_chunk["metadata"]["chunk_id"],
                "old_chunk_id": old_chunk["metadata"]["chunk_id"],
                "score": round(float(score), 4),
                "analysis": analysis,
                "new_content": new_chunk["content"],
                "old_content": old_chunk["content"],
                "new_metadata": {k: v for k, v in new_chunk.get("metadata", {}).items()
                                 if k in ("chunk_id", "chuong_hoac_muc", "dieu",
                                          "muc_con", "trang")},
                "old_metadata": {k: v for k, v in old_chunk.get("metadata", {}).items()
                                 if k in ("chunk_id", "chuong_hoac_muc", "dieu",
                                          "muc_con", "trang")},
            })
        print(f"   → Giữ {len(analyzed_pairs)}/{len(info['cap_doan'])} cặp.")
        if not analyzed_pairs:
            continue
        nguon = (manifest.get(source_file, {}).get("nguon_goc")
                 or resolve_source_location(source_file))
        result_candidates.append({
            "source_file": source_file,
            "diem": round(float(info["diem"]), 4),
            "ty_le_trung": info["ty_le_trung"],  # [MỚI]
            "rat_co_kha_nang_trung_lap_toan_file": info["rat_co_kha_nang_trung_lap_toan_file"],  # [MỚI]
            "sample_metadata": {
                "tieu_de": sample_old_md.get("tieu_de"),
                "so_hieu": sample_old_md.get("so_hieu"),
                "loai_van_ban": sample_old_md.get("loai_van_ban"),
                "ngay_ban_hanh": sample_old_md.get("ngay_ban_hanh"),
            },
            "nguon_goc": nguon,
            "cap_doan": analyzed_pairs,
        })

    print(f"\n✅ Xong {pair_idx} cặp ({len(result_candidates)} file có xung đột).")
    return {"candidates": result_candidates, "n_pairs_analyzed": pair_idx}
    
def apply_conflict_decisions(base_name: str, decisions: dict) -> dict:
    """Áp dụng quyết định của Admin từ web UI.

    decisions = {
      "exact_duplicates": [
         {"old_base_name": "...", "action": "thay_the|giu_cu|doc_lap"}],
      "chunk_decisions": [
         {"source_file": "...", "new_chunk_id": "...", "old_chunk_id": "...",
          "action": "thay_the|giu_cu|giu_ca_2",
          "analysis": {...}}]   # optional - để gắn valid_from/valid_to
    }
    """
    manifest = load_manifest()
    new_checksum = compute_new_document_checksum(base_name)
    ensure_chunk_ids_and_status(base_name)

    applied, changed = [], False

    # ---------- File-level decisions (cấp FILE, không cần phân tích cặp) ----------
    file_decs = decisions.get("file_decisions", [])
    xuong_cap_sources = set()  # các source_file giao cho chunk-level
    file_changed = False
    for d in file_decs:
        source_file, action = d["source_file"], d["action"]
        if action == "thay_the":
            set_all_chunks_status(source_file, "thay_the")
            if source_file in manifest:
                manifest[source_file]["trang_thai_van_ban"] = "thay_the"
            changed = True
            file_changed = True
            applied.append({"type": "file", "source": source_file, "action": "thay_the"})
            log_decision({"su_kien": "web_file_thay_the",
                          "moi": base_name, "cu": source_file})
        elif action == "giu_2":
            applied.append({"type": "file", "source": source_file, "action": "giu_2"})
            log_decision({"su_kien": "web_file_giu_2",
                          "moi": base_name, "cu": source_file})
            file_changed = True  # base_name cần active toàn bộ
        elif action == "xuong_cap_doan":
            xuong_cap_sources.add(source_file)
            applied.append({"type": "file", "source": source_file, "action": "xuong_cap_doan"})
            # không làm gì ở đây - chunk_decisions sẽ xử lý

    if file_changed:
        set_all_chunks_status(base_name, "active")
        manifest[base_name] = {
            **manifest.get(base_name, {}),
            "checksum": new_checksum,
            "trang_thai_van_ban": manifest.get(base_name, {}).get("trang_thai_van_ban", "active"),
            "cap_nhat_luc": _now(),
            "nguon_goc": resolve_source_location(base_name),
        }
        save_manifest(manifest)
        if not decisions.get("chunk_decisions"):
            rebuild_production_vectorstore()
            return {"ok": True, "applied": applied, "rebuild": True,
                    "message": f"Đã áp dụng {len(applied)} quyết định cấp file."}

    # ---------- Exact duplicates ----------
    exact_decs = decisions.get("exact_duplicates", [])
    if exact_decs:
        will_load = any(d["action"] in ("thay_the", "doc_lap") for d in exact_decs)
        for d in exact_decs:
            old_bn, action = d["old_base_name"], d["action"]
            if action == "thay_the":
                set_all_chunks_status(old_bn, "thay_the")
                if old_bn in manifest:
                    manifest[old_bn]["trang_thai_van_ban"] = "thay_the"
                manifest.setdefault(base_name, {}).setdefault(
                    "thay_the_cho", []).append(old_bn)
                log_decision({"su_kien": "web_exact_dup_thay_the",
                              "moi": base_name, "cu": old_bn})
                changed = True
            elif action == "giu_cu":
                log_decision({"su_kien": "web_exact_dup_giu_cu",
                              "moi": base_name, "cu": old_bn})
            elif action == "doc_lap":
                manifest.setdefault(base_name, {}).setdefault(
                    "trung_voi", []).append(old_bn)
                log_decision({"su_kien": "web_exact_dup_doc_lap",
                              "moi": base_name, "cu": old_bn})
            applied.append({"type": "exact_dup", "old": old_bn, "action": action})

        if will_load:
            set_all_chunks_status(base_name, "active")
            manifest[base_name] = {
                **manifest.get(base_name, {}),
                "checksum": new_checksum,
                "trang_thai_van_ban": manifest.get(base_name, {}).get(
                    "trang_thai_van_ban", "active"),
                "cap_nhat_luc": _now(),
                "nguon_goc": resolve_source_location(base_name),
            }
            changed = True
        save_manifest(manifest)
        if changed and not decisions.get("chunk_decisions"):
            rebuild_production_vectorstore()
            return {"ok": True, "applied": applied, "rebuild": True,
                    "message": "Đã xử lý trùng lặp tuyệt đối."}

    # ---------- Chunk-level decisions ----------
    chunk_decs = decisions.get("chunk_decisions", [])
    decided_new_ids = set()
    for d in chunk_decs:
        source_file = d["source_file"]
        new_id, old_id, action = d["new_chunk_id"], d["old_chunk_id"], d["action"]
        analysis = d.get("analysis") or {}
        decided_new_ids.add(new_id)

        if action == "thay_the":
            set_chunk_status(source_file, {old_id}, "vo_hieu_hoa")
            set_chunk_status(base_name, {new_id}, "active")
            changed = True
        elif action == "giu_cu":
            set_chunk_status(base_name, {new_id}, "bi_bo_qua")
        elif action == "giu_ca_2":
            extra_cu = {"nhan_phan_biet": "ban_cu"}
            extra_moi = {"nhan_phan_biet": "ban_moi"}
            if analysis.get("quan_he") == QUAN_HE_KHAC_THOI_DIEM:
                for k_src, k_dst in (("valid_from_doan_cu", "valid_from"),
                                     ("valid_to_doan_cu", "valid_to")):
                    if analysis.get(k_src):
                        extra_cu[k_dst] = analysis[k_src]
                if analysis.get("valid_from_doan_moi"):
                    extra_moi["valid_from"] = analysis["valid_from_doan_moi"]
            set_chunk_status(source_file, {old_id}, "active", extra_metadata=extra_cu)
            set_chunk_status(base_name, {new_id}, "active", extra_metadata=extra_moi)
            changed = True

        applied.append({"type": "chunk", "new_id": new_id, "old_id": old_id,
                        "source_file": source_file, "action": action})
        log_decision({"su_kien": f"web_chunk_{action}",
                      "moi": new_id, "cu": old_id,
                      "base_name": base_name, "source_file": source_file})

    # ---------- Activate các chunk mới chưa được quyết định ----------
    was_in_manifest = base_name in manifest
    chunks_now = load_chunk_file(base_name)
    for c in chunks_now:
        md = c.get("metadata", {})
        cid = md.get("chunk_id")
        if cid in decided_new_ids:
            continue
        if md.get("trang_thai") not in ("active", "bi_bo_qua",
                                         "vo_hieu_hoa", "thay_the"):
            md["trang_thai"] = "active"
            changed = True
    save_chunk_file(base_name, chunks_now)

    # ---------- Manifest ----------
    manifest[base_name] = {
        **manifest.get(base_name, {}),
        "checksum": new_checksum,
        "trang_thai_van_ban": manifest.get(base_name, {}).get(
            "trang_thai_van_ban", "active"),
        "cap_nhat_luc": _now(),
        "nguon_goc": resolve_source_location(base_name),
    }
    save_manifest(manifest)
    if not was_in_manifest:
        changed = True

    if changed:
        rebuild_production_vectorstore()

    return {"ok": True, "applied": applied, "rebuild": changed,
            "message": f"Đã áp dụng {len(applied)} quyết định."}
# ============================================================
# MAIN
# ============================================================

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]

    if command == "bootstrap":
        bootstrap()
    elif command == "status":
        print_status()
    elif command == "list-pending":
        pending = list_pending_new_documents()
        needs_recheck = list_docs_needing_recheck()
        if not pending and not needs_recheck:
            print("Không có văn bản mới nào đang chờ kiểm tra xung đột.")
        else:
            if pending:
                print(f"Có {len(pending)} văn bản MỚI đang chờ kiểm tra xung đột:")
                for b in pending:
                    print(f"  - {b} (mới)")
            if needs_recheck:
                print(f"Có {len(needs_recheck)} văn bản ĐÃ production nhưng vừa bị sửa tay "
                      f"(.md khác checksum đã lưu) - cần chạy lại 'check' để cập nhật:")
                for b in needs_recheck:
                    print(f"  - {b} (đã sửa - cần rà soát lại)")
    elif command == "check":
        if len(sys.argv) < 3:
            print("Cần truyền base_name, vd: python conflict_detection.py check ten_van_ban [--auto safe|replace]")
            sys.exit(1)
        auto = None
        for a in sys.argv[3:]:
            if a.startswith("--auto="):
                auto = a.split("=", 1)[1].strip().lower()
        process_new_document(sys.argv[2], auto=auto)
    elif command == "rebuild-index":
        rebuild_production_vectorstore()
    elif command == "prune":
        prune_manifest()
    elif command == "cleanup-test":
        # [FIX] Thêm cờ '--no-rebuild' để nơi gọi (vd admin_api.py /admin/reset-test)
        # có thể tự kiểm soát CHỈ rebuild-index đúng 1 LẦN ở cuối toàn bộ chuỗi
        # reset (cleanup-test -> prune -> rebuild-index), thay vì rebuild 2 lần
        # (1 lần ẨN bên trong cleanup-test vì auto_rebuild=True mặc định, 1 lần
        # tự gọi tường minh) - rebuild-index embed lại TOÀN BỘ chunk active nên
        # có thể mất vài phút, chạy dư 1 lần là lãng phí thời gian mỗi lần reset.
        args = [a for a in sys.argv[2:] if a != "--no-rebuild"]
        no_rebuild = "--no-rebuild" in sys.argv[2:]
        prefix = args[0] if args else "test_"
        cleanup_test_data(prefix=prefix, auto_rebuild=not no_rebuild)
    elif command == "review-auto":
        if len(sys.argv) < 3:
            print("Thiếu base_name. Dùng: python conflict_detection.py review-auto <base_name>")
            sys.exit(1)
        review_auto_approved(sys.argv[2])
    elif command == "snapshot":
        snapshot_production(sys.argv[2] if len(sys.argv) > 2 else None)
    elif command == "list-snapshots":
        list_snapshots()
    elif command == "latest-snapshot":
        name = latest_snapshot_name()
        print(name if name else "")
    elif command == "restore-snapshot":
        if len(sys.argv) < 3:
            print("Thiếu tên snapshot. Dùng: python conflict_detection.py restore-snapshot <ten>")
            sys.exit(1)
        restore_snapshot(sys.argv[2])
    elif command == "revert-doc":
        if len(sys.argv) < 3:
            print("Thiếu base_name. Dùng: python conflict_detection.py revert-doc <base_name>")
            sys.exit(1)
        revert_doc(sys.argv[2])
    elif command == "reset-cache":
        reset_embedding_cache()
    elif command == "check-file":
        if len(sys.argv) < 3:
            print("Cần truyền đường dẫn file .md, vd: python conflict_detection.py check-file duong_dan.md [ten_base] [--auto safe|replace]")
            sys.exit(1)
        md_path = sys.argv[2]
        rest = sys.argv[3:]
        auto = None
        positional = []
        for a in rest:
            if a.startswith("--auto="):
                auto = a.split("=", 1)[1].strip().lower()
            else:
                positional.append(a)
        base_name_arg = positional[0] if positional else None
        resolved_base = ingest_md_file_for_test(md_path, base_name_arg)
        process_new_document(resolved_base, auto=auto)
    elif command == "analyze-web":
        if len(sys.argv) < 3:
            print("Cần base_name", file=sys.stderr); sys.exit(1)
        base_name = sys.argv[2]
        out_path = None
        if len(sys.argv) > 4 and sys.argv[3] == "--out":
            out_path = sys.argv[4]
        result = analyze_conflicts(base_name)
        text = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            Path(out_path).write_text(text, encoding="utf-8")
            print(f"→ Đã ghi kết quả phân tích: {out_path}")
        else:
            print("===ANALYZE_RESULT_START===")
            print(text)
            print("===ANALYZE_RESULT_END===")
    elif command == "apply-web":
        if len(sys.argv) < 3:
            print("Cần base_name", file=sys.stderr); sys.exit(1)
        base_name = sys.argv[2]
        if len(sys.argv) < 5 or sys.argv[3] != "--decisions":
            print("Cần --decisions <path>", file=sys.stderr); sys.exit(1)
        decisions = json.loads(Path(sys.argv[4]).read_text(encoding="utf-8"))
        result = apply_conflict_decisions(base_name, decisions)
        print("===APPLY_RESULT_START===")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        print("===APPLY_RESULT_END===")
    elif command == "quickscan-web":
        if len(sys.argv) < 3:
            print("Cần base_name", file=sys.stderr); sys.exit(1)
        base_name = sys.argv[2]
        out_path = None
        if len(sys.argv) > 4 and sys.argv[3] == "--out":
            out_path = sys.argv[4]
        result = quick_scan(base_name)
        text = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            Path(out_path).write_text(text, encoding="utf-8")
            print(f"→ Đã ghi quick scan: {out_path}")
        else:
            print("===QUICKSCAN_RESULT_START===")
            print(text)
            print("===QUICKSCAN_RESULT_END===")
    elif command == "analyze-pairs-web":
        if len(sys.argv) < 3:
            print("Cần base_name", file=sys.stderr); sys.exit(1)
        base_name = sys.argv[2]
        files_json, out_path = None, None
        i = 3
        while i < len(sys.argv):
            if sys.argv[i] == "--files" and i + 1 < len(sys.argv):
                files_json = sys.argv[i + 1]; i += 2
            elif sys.argv[i] == "--out" and i + 1 < len(sys.argv):
                out_path = sys.argv[i + 1]; i += 2
            else:
                i += 1
        if not files_json:
            print("Cần --files '<json list>'", file=sys.stderr); sys.exit(1)
        source_files = json.loads(files_json)
        result = analyze_pair_subset(base_name, source_files)
        text = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            Path(out_path).write_text(text, encoding="utf-8")
            print(f"→ Đã ghi phân tích pairs: {out_path}")
        else:
            print("===PAIRS_RESULT_START===")
            print(text)
            print("===PAIRS_RESULT_END===")
    elif command == "list-pending-cross-web":
        base_name_arg = None
        out_path = None
        i = 2
        while i < len(sys.argv):
            if sys.argv[i] == "--base-name" and i + 1 < len(sys.argv):
                base_name_arg = sys.argv[i + 1]; i += 2
            elif sys.argv[i] == "--out" and i + 1 < len(sys.argv):
                out_path = sys.argv[i + 1]; i += 2
            else:
                i += 1
        result = doc_pending_cross_conflicts_web(base_name_arg)
        text = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            Path(out_path).write_text(text, encoding="utf-8")
        else:
            print(text)
    elif command == "apply-pending-cross-web":
        if len(sys.argv) < 3:
            print("Dùng: python conflict_detection.py apply-pending-cross-web <base_name> "
                  "--decisions <path> --out <path>", file=sys.stderr)
            sys.exit(1)
        base_name = sys.argv[2]
        decisions_path, out_path = None, None
        i = 3
        while i < len(sys.argv):
            if sys.argv[i] == "--decisions" and i + 1 < len(sys.argv):
                decisions_path = sys.argv[i + 1]; i += 2
            elif sys.argv[i] == "--out" and i + 1 < len(sys.argv):
                out_path = sys.argv[i + 1]; i += 2
            else:
                i += 1
        if not decisions_path:
            print("Cần --decisions <path>", file=sys.stderr); sys.exit(1)
        decisions = json.loads(Path(decisions_path).read_text(encoding="utf-8"))
        result = ap_dung_quyet_dinh_cheo_cau_truc_web(base_name, decisions)
        text = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            Path(out_path).write_text(text, encoding="utf-8")
        else:
            print(text)
        if not result.get("ok"):
            sys.exit(1)
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
