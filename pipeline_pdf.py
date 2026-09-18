"""
Pipeline OCR -> Markdown -> Chunk cho corpus văn bản hành chính.

4 lệnh con (ocr, prepare, generate, finalize), mỗi lệnh hỗ trợ cả batch
(không tham số) và 1 file (có tham số):
    python pipeline_pdf.py ocr                               # OCR batch toàn bộ thư mục PDF
    python pipeline_pdf.py ocr <duong_dan.pdf>                # OCR đúng 1 file PDF
    python pipeline_pdf.py prepare                            # gợi ý metadata batch
    python pipeline_pdf.py prepare <duong_dan.txt>            # gợi ý metadata cho 1 file -> _review.json
    python pipeline_pdf.py generate                           # sinh markdown batch (sau khi xác nhận loai_van_ban)
    python pipeline_pdf.py generate <duong_dan_review.json>   # sinh markdown cho 1 file
    python pipeline_pdf.py finalize                           # chunk batch toàn bộ file đã có markdown
    python pipeline_pdf.py finalize <duong_dan_review.json>   # chunk 1 file

File này CHỈ xử lý PDF. Xử lý .docx/.doc/.txt nằm ở file riêng
'pipeline_docx_txt.py' - dùng chung prepare/generate/finalize ở đây, vì cả
2 nguồn đều ghi kết quả vào cùng 1 thư mục .txt.

THỨ TỰ XỬ LÝ: OCR -> gợi ý loại văn bản -> USER XÁC NHẬN loại -> SINH markdown
hoàn chỉnh theo loại đã xác nhận -> USER RÀ SOÁT markdown (sửa trực tiếp nếu
cần) -> Save (finalize) -> chunk.

    1. ocr       : OCR -> .txt
    2. prepare   : .txt -> gợi ý metadata (loai_van_ban...) trong _review.json
       ---> USER MỞ FILE, XÁC NHẬN/SỬA 'metadata_xac_nhan.loai_van_ban' <---
    3. generate  : dùng loai_van_ban ĐÃ XÁC NHẬN -> sinh markdown hoàn chỉnh
       ---> USER MỞ FILE, RÀ SOÁT/SỬA TRỰC TIẾP markdown <---
    4. finalize  : dùng markdown đã rà soát -> chunk cuối cùng

QUAN TRỌNG: chạy lại 'generate' trên 1 file ĐÃ có markdown sẽ GHI ĐÈ, MẤT
mọi sửa tay trước đó - chỉ chạy 'generate' TRƯỚC KHI bắt đầu sửa tay markdown.

KIẾN TRÚC CHÍNH:
1. OCR bằng gpt-4.1-mini (vision model) - LUÔN OCR, không đọc PDF trực tiếp,
   vì đọc trực tiếp (PyPDFLoader) đọc theo thứ tự lưu trong file, không phải
   thứ tự thị giác - xáo trộn với PDF layout nhiều cột/đồ họa.
2. Model tự đánh dấu markdown header (#, ##, ###) ngay lúc OCR dựa trên độ
   quan trọng thị giác (cỡ chữ, đậm, căn giữa) - tổng quát cho mọi loại văn
   bản, không cần biết trước là Điều/Chương/La Mã/brochure.
3. Nếu OCR đã có header thật -> dùng thẳng. Regex theo loai_van_ban chỉ là
   lưới an toàn cho file OCR bằng prompt cũ.
4. Chunk theo header + xử lý bảng riêng (không cắt ngang, 1 dòng dữ liệu/chunk
   để tăng độ chính xác truy vấn).

Cài đặt:
    pip install openai pdf2image langchain-text-splitters

Set API key (Windows, mở terminal mới sau khi set):
    setx OPENAI_API_KEY "sk-...."
"""

import os
import re
import sys
import json
import time
import base64
from io import BytesIO
from pathlib import Path
from typing import Optional
from pdf2image import convert_from_path
from openai import OpenAI
from dotenv import load_dotenv
from langchain_ollama import ChatOllama
from langchain_core.messages import HumanMessage, SystemMessage

load_dotenv()
client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
# Client Ollama riêng cho bước gợi ý metadata — KHÔNG dùng chung với OpenAI
# client vì 2 API khác nhau. format="json" buộc Ollama chỉ trả JSON hợp lệ,
# tương đương response_format={"type": "json_object"} của OpenAI — cần thiết
# vì model local hay "buôn" thêm chữ giải thích quanh JSON nếu không ép.


# ============================================================
# CẤU HÌNH (chỉnh lại cho đúng máy bạn)
# ============================================================

# ============================================================
# CẤU HÌNH POPPLER — auto-detect theo OS.
#   1. POPPLER_PATH trong .env (nếu set + tồn tại)
#   2. Windows mặc định: D:\poppler\poppler-26.02.0\Library\bin
#   3. None → để pdf2image tự tìm trong PATH hệ thống (Linux: apt install poppler-utils)
# ============================================================
_env_poppler = os.getenv("POPPLER_PATH", "").strip()
_win_poppler = r"D:\poppler\poppler-26.02.0\Library\bin"
if _env_poppler and Path(_env_poppler).exists():
    POPPLER_PATH = _env_poppler
elif Path(_win_poppler).exists():
    POPPLER_PATH = _win_poppler
else:
    POPPLER_PATH = None
    
BASE = Path(__file__).resolve().parent
input_pdf_folder = str(BASE / "data" / "raw" / "pdf")   # 24 file PDF gốc
output_txt_folder = str(BASE / "data" / "processed" / "txt")
output_md_folder = str(BASE / "data" / "processed" / "markdown")
output_chunk_folder = str(BASE / "data" / "processed" / "chunks")
log_path = str(BASE / "data" / "processed" / "ocr_batch_log.json")

OCR_MODEL = "gpt-4.1-mini"
OLLAMA_SERVER = os.getenv("OLLAMA_SERVER", "http://10.2.13.58:8037/ollama")
OLLAMA_SECKEY = os.getenv("OLLAMA_SECKEY", "research")
OLLAMA_CLIENT_KWARGS = {"headers": {"x-ollama-seckey": OLLAMA_SECKEY}}
METADATA_MODEL = os.getenv("METADATA_MODEL", "qwen2.5:14b-instruct-ctx16k")
metadata_llm = ChatOllama(
    model=METADATA_MODEL,
    temperature=0,
    base_url=OLLAMA_SERVER,
    format="json",
    client_kwargs=OLLAMA_CLIENT_KWARGS,
)

# DPI mặc định 300 (đủ nét cho văn bản hành chính). Có thể override qua
# OCR_DPI trong .env nếu muốn giảm xuống 200 cho PDF scan nhẹ hơn.
# Ảnh gửi API dùng JPEG q=85 (thay vì PNG) để giảm ~10x dung lượng request.
DPI = int(os.getenv("OCR_DPI", "300"))
JPEG_QUALITY = int(os.getenv("OCR_JPEG_QUALITY", "85"))

MAX_CHARS_PER_PAGE_WARNING = 15000  # cảnh báo runaway generation (từng gặp 1 trang ra 4 triệu ký tự)
CHUNK_SIZE = 800
CHUNK_OVERLAP = 100
TABLE_MAX_ROWS_PER_CHUNK = 1  # mỗi chunk = tiêu đề cột + ĐÚNG 1 dòng dữ liệu (có căn cứ nghiên cứu)

for folder in (output_txt_folder, output_md_folder, output_chunk_folder):
    os.makedirs(folder, exist_ok=True)

LOAI_VAN_BAN_OPTIONS = [
    "Quyết định",
    "Quy chế",
    "Quy định",
    "Quy trình",
    "Nội quy",
    "Thông báo",
    "Hướng dẫn sử dụng",
    "Công văn",
    "Kế hoạch",
    "Giới thiệu",
    "Hỏi đáp",
    "Tài liệu giới thiệu / tuyển sinh",
    "Khác",
]

OCR_PROMPT = """Trích xuất toàn bộ văn bản trong ảnh này thành markdown có cấu trúc, giữ nguyên xuống dòng và định dạng gốc.

QUY TẮC ĐÁNH DẤU HEADER (áp dụng cho MỌI loại văn bản, không cần biết trước đây là loại gì):
- Đánh giá dựa trên ĐỘ QUAN TRỌNG THỊ GIÁC bạn nhìn thấy trong ảnh: cỡ chữ lớn hơn, in đậm, in hoa, căn giữa, có khoảng cách tách biệt với đoạn văn xung quanh - đó là dấu hiệu của 1 header, bất kể nó là "Điều", "Chương", tên ngành học, mục La Mã, hay tiêu đề mục trong brochure.
- '#' cho tiêu đề/mục LỚN NHẤT của trang (thường xuất hiện 1 lần đầu văn bản hoặc là ranh giới phần lớn, vd tên Chương, PHỤ LỤC, tên Trường/Khoa trong danh mục ngành).
- '##' cho mục con cấp 2 (vd Điều, mục La Mã I/II/III, tên 1 ngành/chương trình đào tạo cụ thể trong danh mục).
- '###' cho mục con cấp 3 nếu có phân cấp sâu hơn (vd mục số thập phân 1.1/2.3, tên 1 môn học cụ thể trong chương trình đào tạo).
- KHÔNG đánh header cho các dòng liệt kê/gạch đầu dòng thông thường bên trong 1 mục - chỉ đánh header cho các dòng thực sự đóng vai trò ranh giới/tiêu đề của 1 phần nội dung mới.
- Nếu 1 trang không có header rõ ràng nào (vd trang bìa, trang toàn bảng), không cần ép phải có header.

QUY TẮC BẢNG BIỂU:
- Giữ cấu trúc bảng bằng markdown (| ô | ô |) CHỈ áp dụng cho bảng dữ liệu thật (có hàng/cột rõ ràng, ô chứa dữ liệu).
- Nếu 1 bảng bị ngắt ngang do hết trang và tiếp tục ở ảnh/trang sau, hãy LẶP LẠI đúng dòng tiêu đề cột khi bắt đầu OCR ảnh đó, để bảng ở mỗi trang tự đủ nghĩa.

QUY TẮC SƠ ĐỒ/FLOWCHART (quan trọng - KHÔNG ép vào bảng):
- Nếu ảnh là sơ đồ khối/flowchart (các ô hình chữ nhật nối bằng mũi tên mô tả quy trình, không phải bảng dữ liệu), KHÔNG cố diễn đạt thành bảng markdown.
- Thay vào đó, mô tả bằng danh sách các bước tuần tự theo đúng thứ tự mũi tên chỉ dẫn, ví dụ:
  "Bước 1: [nội dung ô 1] -> Bước 2: [nội dung ô 2] -> Bước 3: [nội dung ô 3]"
  Nếu có rẽ nhánh (2 mũi tên từ 1 ô), mô tả rõ điều kiện rẽ nhánh, ví dụ:
  "Nếu [điều kiện A]: -> [bước tiếp theo A]. Nếu [điều kiện B]: -> [bước tiếp theo B]."

Chỉ trả về văn bản gốc đã đánh dấu markdown, không thêm giải thích hay bình luận."""

METADATA_SCHEMA_PROMPT = """Bạn là trợ lý trích xuất metadata cho văn bản hành chính/pháp lý tiếng Việt.
Đọc đoạn văn bản dưới đây (thường lấy từ 1-2 trang đầu, nơi chứa quốc hiệu và trích yếu) và trả về CHÍNH XÁC 1 JSON object với các trường sau:

{
  "loai_van_ban": "Quyết định" | "Quy chế" | "Quy định" | "Quy trình" | "Nội quy" | "Thông báo" | "Hướng dẫn sử dụng" | "Công văn" | "Kế hoạch" | "Giới thiệu" | "Hỏi đáp" | "Tài liệu giới thiệu / tuyển sinh" | "Khác",
  "tieu_de": string,
  "so_hieu": string | null,
  "ngay_ban_hanh": string | null,
  "co_quan_ban_hanh": string | null,
  "do_tin_cay": "cao" | "thap"
}

QUAN TRỌNG:
- loai_van_ban PHẢI là 1 trong các giá trị chuẩn liệt kê trên.
- Chỉ trả về JSON, không thêm giải thích.
- Nếu không chắc chắn 1 trường nào đó, trả về null cho trường đó thay vì bịa.
- ngay_ban_hanh phải là ngày *ban hành* của chính văn bản này, không phải ngày của văn bản trích dẫn/căn cứ bên trong.
"""


# ============================================================
# BƯỚC 1 — OCR (batch folder hoặc 1 file)
# ============================================================

def image_to_base64_url(image):
    """Encode ảnh sang JPEG thay vì PNG - giảm ~10x kích thước request.
    Ảnh scan văn bản hành chính không cần lossless; JPEG q=85 vẫn đủ nét cho OCR."""
    # Đảm bảo mode RGB (JPEG không hỗ trợ RGBA/palette)
    if image.mode not in ("RGB", "L"):
        image = image.convert("RGB")
    buf = BytesIO()
    image.save(buf, format="JPEG", quality=JPEG_QUALITY, optimize=True)
    size_mb = len(buf.getvalue()) / 1024 / 1024
    b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
    return f"data:image/jpeg;base64,{b64}", size_mb


def strip_code_fence(text: str) -> str:
    """Lột bỏ dấu ```markdown (hoặc ```) mở/đóng thừa mà model đôi khi tự ý bọc
    quanh toàn bộ nội dung trả về - phát hiện thực tế xảy ra ở ~2/3 số trang
    của 1 văn bản, gây nhiễu nội dung chunk nếu không lột bỏ."""
    text = text.strip()
    text = re.sub(r"^```(?:markdown)?\s*\n?", "", text)
    text = re.sub(r"\n?```\s*$", "", text)
    return text


def looks_like_repetition(text: str, do_dai_bat_thuong: int = 6000) -> bool:
    """Phát hiện lỗi lặp vô hạn - CHỈ coi là garbage thật khi ĐỒNG THỜI thỏa 2
    điều kiện: (1) có cụm ký tự lặp lại nhiều lần, VÀ (2) độ dài output PHÌNH
    TO BẤT THƯỜNG (vd trang 38 từng gặp phình tới 66.164 ký tự).

    Lý do bắt buộc cả 2 điều kiện: chỉ dựa vào (1) từng gây CHẶN NHẦM (false
    positive) với biểu mẫu hành chính thật có cấu trúc lặp hợp lệ (vd nhiều
    dòng 'Họ và tên: .......................' điền chỗ trống, checkbox lặp lại
    theo từng tiêu chí) - nhưng các trang đó có độ dài BÌNH THƯỜNG (1-3 nghìn
    ký tự), không hề phình to như garbage thật. Yêu cầu thêm điều kiện (2) để
    phân biệt 2 trường hợp mà không cần liệt kê hết mọi ký tự có thể gây lặp
    hợp lệ (dấu chấm, gạch dưới, checkbox...)."""
    if len(text) <= do_dai_bat_thuong:
        return False

    compact = re.sub(r"\s+", "", text)
    if re.search(r"(.{8,40}?)\1{15,}", compact):
        return True
    if re.search(r"(.{2,7}?)\1{25,}", compact):
        return True
    return False


def ocr_page(image, max_retries: int = 2):
    """
    OCR 1 trang với logging rõ ràng từng attempt để dễ debug.

    Chiến lược retry:
    - Lỗi API/network/timeout → retry NGAY (không tăng penalty).
    - Text rỗng → retry với prompt ngắn gọn hơn (không tăng penalty).
    - Phát hiện lặp / bị cắt (finish_reason=length) → retry với penalty tăng dần.
    """
    frequency_penalty = 0.0

    for attempt in range(max_retries + 1):
        try:
            img_url, size_mb = image_to_base64_url(image)
            response = client.chat.completions.create(
                model=OCR_MODEL,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "text", "text": OCR_PROMPT},
                        {"type": "image_url", "image_url": {"url": img_url}},
                    ],
                }],
                temperature=0,
                max_tokens=4000,
                frequency_penalty=frequency_penalty,
                timeout=90,
            )
            text = strip_code_fence(response.choices[0].message.content or "")
            finish_reason = response.choices[0].finish_reason
            usage = getattr(response, "usage", None)
            usage_str = f"tokens={usage.completion_tokens}/{usage.total_tokens}" if usage else ""

            print(f"    → Attempt {attempt+1}: ảnh {size_mb:.1f}MB, nhận {len(text)} ký tự, "
                  f"finish_reason={finish_reason} {usage_str}")

        except Exception as e:
            # Lỗi API (kể cả request quá lớn): log chi tiết + retry NGAY, KHÔNG tăng penalty
            print(f"    ✗ Attempt {attempt+1}: LỖI API — {type(e).__name__}: {e}")
            if attempt < max_retries:
                time.sleep(2)
                continue
            return (f"[CẢNH BÁO TỰ ĐỘNG: Lỗi API OCR sau {max_retries+1} lần thử. "
                    f"Chi tiết: {type(e).__name__}: {str(e)[:300]}. CẦN SOÁT TAY ẢNH GỐC.]")

        # Text rỗng → thử lại, KHÔNG tăng penalty (penalty cao làm model câm)
        if not text.strip():
            print(f"    ✗ Attempt {attempt+1}: model trả về RỖNG")
            if attempt < max_retries:
                time.sleep(1)
                continue
            return (f"[CẢNH BÁO TỰ ĐỘNG: Model trả về RỖNG sau {max_retries+1} lần thử. "
                    f"Trang này có thể là ảnh scan mờ/sơ đồ phức tạp. CẦN SOÁT TAY ẢNH GỐC.]")

        # Phát hiện lặp / bị cắt → đây là lỗi THẬT của model, tăng penalty để retry
        bi_nghi = looks_like_repetition(text) or finish_reason == "length"
        if not bi_nghi:
            return text

        print(f"    ⚠ Attempt {attempt+1}: NGHI LẶP/BỊ CẮT (len={len(text)}, finish_reason={finish_reason}) "
              f"— tăng penalty lên {min(frequency_penalty + 0.4, 2.0):.1f} và thử lại")
        frequency_penalty = min(frequency_penalty + 0.4, 2.0)

    # Không bao giờ tới đây (vòng lặp đã return trong từng nhánh), nhưng để an toàn:
    print(f"    ⚠ Vẫn lỗi sau {max_retries + 1} lần thử - CẦN SOÁT TAY LẠI ẢNH GỐC.")
    return (f"[CẢNH BÁO TỰ ĐỘNG: Không OCR được sau {max_retries + 1} lần thử. "
            f"CẦN KIỂM TRA TAY LẠI ẢNH GỐC.]")


def ocr_one_pdf(pdf_path: str) -> str:
    """OCR toàn bộ 1 file PDF, trả về text ghép các trang. Không bắt lỗi ở đây -
    để hàm gọi (batch loop) tự try/except riêng từng file."""
    images = convert_from_path(pdf_path, dpi=DPI, poppler_path=POPPLER_PATH)

    full_text = ""
    for i, image in enumerate(images):
        page_num = i + 1
        t_page_start = time.time()
        text = ocr_page(image)
        t_page = time.time() - t_page_start

        if len(text) > MAX_CHARS_PER_PAGE_WARNING:
            print(f"  - Trang {page_num}/{len(images)} — {t_page:.2f}s — {len(text)} ký tự "
                  f"— ⚠️ BẤT THƯỜNG DÀI, nghi lỗi runaway generation, cần soát tay!")
        else:
            print(f"  - Trang {page_num}/{len(images)} — {t_page:.2f}s — {len(text)} ký tự")

        full_text += f"\n\n===== TRANG {page_num} =====\n\n" + text

    return full_text


def ocr_single_file(pdf_input_path: str):
    txt_filename = Path(pdf_input_path).stem + ".txt"
    text_output_path = os.path.join(output_txt_folder, txt_filename)

    print(f"▶ [ocr] Đang xử lý: {pdf_input_path}")
    t_start = time.time()
    full_text = ocr_one_pdf(pdf_input_path)

    with open(text_output_path, "w", encoding="utf-8") as f:
        f.write(full_text)

    so_trang = full_text.count("===== TRANG")
    print(f"✅ Hoàn tất OCR! Đã lưu: {text_output_path} ({time.time()-t_start:.1f}s, {so_trang} trang)")


def ocr_batch_folder():
    pdf_files = [f for f in os.listdir(input_pdf_folder) if f.lower().endswith(".pdf")]
    print(f"Tìm thấy {len(pdf_files)} file PDF cần xử lý.\n" + "=" * 40)

    t_batch_start = time.time()
    ket_qua = []

    for filename in pdf_files:
        pdf_input_path = os.path.join(input_pdf_folder, filename)
        txt_filename = filename.rsplit(".", 1)[0] + ".txt"
        text_output_path = os.path.join(output_txt_folder, txt_filename)

        print(f"▶ Đang xử lý file: {filename}")
        t_file_start = time.time()
        try:
            full_text = ocr_one_pdf(pdf_input_path)
            with open(text_output_path, "w", encoding="utf-8") as f:
                f.write(full_text)
            t_file = time.time() - t_file_start
            so_trang = full_text.count("===== TRANG")
            print(f"✅ Hoàn tất! Đã lưu: {txt_filename} ({t_file:.1f}s, {so_trang} trang)\n")
            ket_qua.append({"filename": filename, "so_trang": so_trang,
                             "thoi_gian_s": round(t_file, 1), "loi": None})
        except Exception as e:
            t_file = time.time() - t_file_start
            print(f"❌ Có lỗi xảy ra khi xử lý file {filename}: {str(e)}\n")
            ket_qua.append({"filename": filename, "so_trang": 0,
                             "thoi_gian_s": round(t_file, 1), "loi": str(e)})

    t_batch_total = time.time() - t_batch_start

    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(ket_qua, f, ensure_ascii=False, indent=2)

    print("=" * 40)
    print(f"ĐÃ XỬ LÝ XONG {len(pdf_files)} FILE PDF — tổng thời gian {t_batch_total/60:.1f} phút")
    print("=" * 40)

    thanh_cong = [r for r in ket_qua if r["loi"] is None]
    that_bai = [r for r in ket_qua if r["loi"] is not None]
    print(f"\nThành công: {len(thanh_cong)}/{len(pdf_files)}")
    for r in thanh_cong:
        print(f"  ✅ {r['filename']}: {r['so_trang']} trang, {r['thoi_gian_s']}s")
    if that_bai:
        print(f"\n⚠️  Lỗi: {len(that_bai)}/{len(pdf_files)}")
        for r in that_bai:
            print(f"  ❌ {r['filename']}: {r['loi']}")
    print(f"\nLog chi tiết đã lưu: {log_path}")


# ============================================================
# BƯỚC 2 — Metadata (LLM gợi ý) + chuẩn hóa
# ============================================================

def normalize_metadata(metadata: dict) -> dict:
    """Chuẩn hóa metadata - dùng chung cho cả kết quả LLM gợi ý VÀ dữ liệu admin
    gõ/sửa tay trên form trước khi lưu, đảm bảo format nhất quán để bước phát
    hiện trùng lặp (sau này) so khớp so_hieu chính xác."""
    if metadata.get("tieu_de"):
        metadata["tieu_de"] = re.sub(r"^V/v\s*", "", metadata["tieu_de"].strip())
    else:
        metadata["tieu_de_tu_dong"] = True

    if metadata.get("so_hieu"):
        so_hieu = re.sub(r"^Số\s*:?\s*", "", metadata["so_hieu"].strip(), flags=re.IGNORECASE)
        so_hieu = re.sub(r"\s*/\s*", "/", so_hieu)
        # Dọn khoảng trắng thừa quanh dấu '-' (phát hiện thêm 2 biến thể:
        # 'QĐ- ĐHKTQD' thừa sau dấu, 'QĐ - ĐHKTQD' thừa cả 2 bên dấu).
        so_hieu = re.sub(r"\s*-\s*", "-", so_hieu)
        # Dọn khoảng trắng thừa CHEN GIỮA cụm viết tắt liền nhau (vd 'ĐHK TQD' ->
        # 'ĐHKTQD', không đi kèm dấu '-'/'/' nào ở giữa) - chỉ áp dụng cho phần
        # sau dấu '-' cuối cùng để không đụng vào phần số phía trước.
        if "-" in so_hieu:
            prefix, _, suffix = so_hieu.rpartition("-")
            suffix_clean = re.sub(r"(?<=[A-ZĐ])\s+(?=[A-ZĐ])", "", suffix)
            so_hieu = f"{prefix}-{suffix_clean}"
        metadata["so_hieu"] = so_hieu

    if metadata.get("ngay_ban_hanh"):
        raw_date = metadata["ngay_ban_hanh"].strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}$", raw_date):
            pass
        else:
            m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", raw_date)
            if m:
                day, month, year = m.groups()
                metadata["ngay_ban_hanh"] = f"{year}-{int(month):02d}-{int(day):02d}"
            else:
                metadata["dinh_dang_ngay_can_xac_nhan"] = True
    else:
        metadata["thieu_ngay_ban_hanh"] = True

    return metadata


def extract_document_metadata(first_pages_text: str) -> dict:
    """
     Gợi ý metadata từ 2000 ký tự đầu — dùng Ollama self-host
    (qwen2.5:14b-instruct-ctx16k, xem METADATA_MODEL) thay vì gọi API ngoài,
    để không gửi nội dung văn bản nội bộ ra ngoài và không tốn phí.
    Ép format="json" để model chỉ trả JSON hợp lệ - bớt được bước dọn
    ```json fence như với OpenAI.
    """
    messages = [
        SystemMessage(content=METADATA_SCHEMA_PROMPT),
        HumanMessage(content=first_pages_text),
    ]
    try:
        response = metadata_llm.invoke(messages)
        raw = response.content.strip()
    except Exception as e:
        print(f"    [!] Lỗi gọi Ollama metadata: {e}")
        raw = ""

    try:
        metadata = json.loads(raw)
    except json.JSONDecodeError:
        # Model local đôi khi vẫn bọc JSON trong ```json ... ``` dù đã ép
        # format="json" (tùy version) — thử lột fence 1 lần trước khi bỏ cuộc.
        cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
        try:
            metadata = json.loads(cleaned)
        except json.JSONDecodeError:
            metadata = {
                "loai_van_ban": None, "tieu_de": None, "so_hieu": None,
                "ngay_ban_hanh": None, "co_quan_ban_hanh": None, "do_tin_cay": "thap",
            }
            print(f"    [!] Không parse được JSON metadata, dùng giá trị rỗng. Raw: {raw[:200]}")

    if not metadata.get("do_tin_cay"):
        metadata["do_tin_cay"] = "thap"

    return normalize_metadata(metadata)


# ============================================================
# BƯỚC 3 — Header markdown: dùng thẳng nếu OCR đã đánh, regex chỉ là fallback
# ============================================================

def markup_legal_structure(text: str) -> str:
    """Quyết định/Quy chế/Quy định/Nội quy: Chương/Điều/Mục/PHỤ LỤC.
    Điều/Chương chấp nhận cả '.' và ':' sau số."""
    text = re.sub(r"(?m)^(Chương\s+[IVXLCDM]+[\.:]?\s*.*)$", r"# \1", text)
    text = re.sub(r"(?m)^([Pp]hụ\s+[Ll]ục\s+(?:\d+|[IVXLCDM]+)\b.*)$", r"# \1", text)
    text = re.sub(r"(?m)^(Điều\s+\d+[a-zđ]?\s*[\.:]\s*.*)$", r"## \1", text)
    text = re.sub(r"(?m)^(Mục\s+\d+\b.*)$", r"## \1", text)
    return text


def markup_roman_numbered_structure(text: str) -> str:
    """Hướng dẫn/Quy trình/Tài liệu giới thiệu: La Mã cấp 1, số/Bước cấp 2, thập phân cấp 3.

    QUAN TRỌNG: các dòng 'số. Chữ hoa...' CHỈ được coi là header nếu KHÔNG kết
    thúc bằng dấu câu (. , ; : …) - vì tiêu đề thật (vd '1. Mục tiêu đào tạo')
    là 1 cụm từ ngắn, không phải câu hoàn chỉnh. Nếu thiếu điều kiện này, các
    KHOẢN đánh số bên trong 1 Điều (vd '2. Đối tượng áp dụng ... liên quan.' -
    1 câu hoàn chỉnh, kết thúc bằng dấu chấm) sẽ bị nhầm thành header cùng cấp
    với chính Điều chứa nó, làm sai lệch toàn bộ phân cấp văn bản."""
    text = re.sub(r"(?m)^([IVXLCDM]+[\-\.]\s*[A-ZĐÂÊÔƯ][^\n]{0,79}[^\s\.,;:…])[ \t]*$", r"# \1", text)
    text = re.sub(r"(?m)^(\d+\.\d+\.?\s+[A-ZĐÂÊÔƯ][^\n]{0,99}[^\s\.,;:…])[ \t]*$", r"### \1", text)
    text = re.sub(r"(?m)^(\d+\.\s+[A-ZĐÂÊÔƯ][^\n]{0,99}[^\s\.,;:…])[ \t]*$", r"## \1", text)
    text = re.sub(r"(?m)^(\*?\s*Bước\s+\d+\s*:\s*.*)$", r"## \1", text)
    return text


def markup_thong_bao_structure(text: str) -> str:
    """Thông báo/Công văn: số đơn giản, tiêu đề có thể dài.
    Cùng lý do như markup_roman_numbered_structure: loại trừ dòng kết thúc
    bằng dấu câu để không vơ nhầm khoản/câu văn thường vào làm header."""
    text = re.sub(r"(?m)^(\d+\.\s+[A-ZĐÂÊÔƯ][^\n]{0,199}[^\s\.,;:…])[ \t]*$", r"# \1", text)
    return text


def markup_no_header(text: str) -> str:
    return text


def markup_roman_and_buoc_only(text: str) -> str:
    """Phiên bản an toàn của markup_roman_numbered_structure, dùng cho phần
    đính kèm của văn bản pháp lý - CHỈ đánh header cho La Mã và 'Bước N:',
    KHÔNG đánh cho mẫu số Ả Rập 'N. Tiêu đề'.

    Lý do: theo thể thức văn bản hành chính, khoản CÓ TIÊU ĐỀ được trình bày
    trên 1 dòng riêng, chữ đậm, KHÔNG có dấu câu cuối - hình thức này giống
    hệt header thật, không có cách nào phân biệt bằng regex. Số La Mã và chữ
    'Bước' thì an toàn vì khoản/điểm trong Điều không bao giờ dùng 2 quy ước
    này."""
    text = re.sub(r"(?m)^([IVXLCDM]+[\-\.]\s*[A-ZĐÂÊÔƯ][^\n]{0,79}[^\s\.,;:…])[ \t]*$", r"# \1", text)
    text = re.sub(r"(?m)^(\*?\s*Bước\s+\d+\s*:\s*.*)$", r"## \1", text)
    return text


def markup_combined_structure(text: str) -> str:
    """Quyết định ban hành X: áp dụng pháp lý (Điều/Chương/Mục) + La Mã/Bước
    cho phần đính kèm - KHÔNG áp dụng mẫu số Ả Rập 'N. Tiêu đề' (xem lý do
    trong markup_roman_and_buoc_only)."""
    text = markup_legal_structure(text)
    text = markup_roman_and_buoc_only(text)
    return text


HEADER_STRATEGY_BY_TYPE = {
    "Quyết định": markup_combined_structure,
    "Quy chế": markup_combined_structure,
    "Quy định": markup_combined_structure,
    "Nội quy": markup_combined_structure,
    "Nghị quyết": markup_combined_structure,
    "Quy trình": markup_roman_numbered_structure,
    "Hướng dẫn sử dụng": markup_roman_numbered_structure,
    "Tài liệu giới thiệu / tuyển sinh": markup_roman_numbered_structure,
    "Kế hoạch": markup_roman_numbered_structure,
    "Giới thiệu": markup_no_header,  # thường không có quy ước cố định, header (nếu có) đã do docx/OCR tự đánh
    "Hỏi đáp": markup_no_header,
    "Thông báo": markup_thong_bao_structure,
    "Công văn": markup_thong_bao_structure,
    "Khác": markup_no_header,
}


def normalize_header_levels(text: str) -> str:
    """
    Sửa lỗi model tự đánh header KHÔNG NHẤT QUÁN cấp độ giữa các trang (vì OCR
    chạy độc lập từng trang, không 'nhớ' đã đánh cấp mấy cho 'Chương' ở trang
    trước - phát hiện thực tế: 'Chương I' ra '##' nhưng 'Chương II' lại ra '#'
    trong CÙNG 1 văn bản).

    Với các mẫu ĐÃ BIẾT CHẮC ý nghĩa phân cấp (Chương/PHỤ LỤC luôn cấp 1,
    Điều/Mục luôn cấp 2), ép về đúng cấp bất kể model tự đánh '#' mấy lần.
    Header KHÔNG khớp mẫu nào (brochure, nội dung tự do) giữ nguyên theo model
    tự đánh - không có căn cứ nào tốt hơn để sửa cho trường hợp đó.
    """
    def force_level(pattern, level_prefix):
        nonlocal text
        text = re.sub(
            r"(?m)^#{1,3}\s+(" + pattern + r")",
            level_prefix + r" \1",
            text,
        )

    force_level(r"Chương\s+[IVXLCDM]+.*", "#")
    force_level(r"[Pp]hụ\s+[Ll]ục\s+(?:\d+|[IVXLCDM]+)\b.*", "#")
    force_level(r"Điều\s+\d+[a-zđ]?\s*[\.:].*", "##")
    force_level(r"Mục\s+\d+\b.*", "##")
    return text


def strip_bold_from_structural_keywords(text: str) -> str:
    """Model OCR đôi khi dùng **in đậm** thay vì header markdown thật cho các
    dòng cấu trúc (vd '**Điều 5:** ...' thay vì '## Điều 5:...') - phát hiện
    thực tế: trong CÙNG 1 văn bản có Điều được đánh '##', có Điều chỉ in đậm,
    có Điều KHÔNG đánh dấu gì cả. Lột bỏ '**' bao quanh các từ khóa cấu trúc
    đã biết chắc (Điều/Chương/Mục/PHỤ LỤC) và các dòng số thứ tự in đậm toàn
    dòng, để bước regex phía sau (vốn neo vào đầu dòng) nhận diện được."""
    text = re.sub(r"(?m)^\*\*(Điều\s+\d+[a-zđ]?\s*[.:].*?)\*\*", r"\1", text)
    text = re.sub(r"(?m)^\*\*(Chương\s+[IVXLCDM]+.*?)\*\*", r"\1", text)
    text = re.sub(r"(?m)^\*\*([Pp]hụ\s+[Ll]ục\s+(?:\d+|[IVXLCDM]+).*?)\*\*", r"\1", text)
    text = re.sub(r"(?m)^\*\*(Mục\s+\d+.*?)\*\*", r"\1", text)
    text = re.sub(r"(?m)^\*\*(\d+\.\s+.*?)\*\*\s*$", r"\1", text)
    return text


def markup_headers(text: str, loai_van_ban: str) -> str:
    """
    LUÔN chạy CẢ 2 bước, không còn chọn 1 trong 2 như trước:
    1. Chuẩn hóa cấp cho header model ĐÃ tự đánh (nếu có).
    2. BỔ SUNG header cho các dòng khớp mẫu đã biết (Điều/Chương/số thứ tự...)
       mà model CHƯA đánh dấu gì hoặc chỉ in đậm - các hàm markup_* dùng regex
       neo đầu dòng (^...) nên tự động AN TOÀN, không đụng vào dòng đã có '#'
       từ trước (dòng '## Điều 5' không khớp pattern '^Điều\\s+\\d+' vì đã có
       '##' ở đầu).

    Lý do đổi từ "chọn 1 trong 2" sang "luôn làm cả 2": phát hiện thực tế OCR
    tự đánh header KHÔNG ĐẦY ĐỦ và KHÔNG NHẤT QUÁN cách đánh dấu trong CÙNG 1
    văn bản (có Điều dùng '##', có Điều chỉ in đậm '**...**', có Điều không
    đánh dấu gì) - cách cũ (chỉ tin OCR nếu thấy dù chỉ 1 dòng '#') bỏ sót rất
    nhiều Điều/mục thật.
    """
    text = strip_bold_from_structural_keywords(text)
    text = normalize_header_levels(text)
    strategy = HEADER_STRATEGY_BY_TYPE.get(loai_van_ban, markup_legal_structure)
    text = strategy(text)
    return text


# ============================================================
# BƯỚC 4 — Chunk theo header, không cắt ngang bảng
# ============================================================

TABLE_BLOCK_PATTERN = re.compile(
    r"(?:^\|.*\|[ \t]*\n)(?:^\|[\-: \t|]+\|[ \t]*\n)(?:^\|.*\|[ \t]*\n?)*",
    re.MULTILINE,
)
PAGE_MARKER_PATTERN = re.compile(r"\n*=+\s*TRANG\s+(?P<pagenum>\d+)\s*=+\n*")

# Gộp 2 pattern thành 1 để quét 1 lượt duy nhất, giữ đúng thứ tự xen kẽ
# bảng/marker trang trong văn bản (không xử lý riêng rẽ 2 lượt sẽ làm lệch
# thứ tự khi 1 bảng bị marker trang chen ngang giữa chừng).
SPECIAL_BLOCK_PATTERN = re.compile(
    r"(?P<table>" + TABLE_BLOCK_PATTERN.pattern + r")"
    r"|(?P<page>" + PAGE_MARKER_PATTERN.pattern + r")",
    re.MULTILINE,
)


def split_preserving_tables(text: str, chunk_size: int, chunk_overlap: int, current_page: int = 1):
    """
    Cắt text theo chunk_size như bình thường, KHÔNG cắt ngang bảng markdown,
    ĐỒNG THỜI xóa marker '===== TRANG N =====' khỏi nội dung (để không lọt ra
    câu trả lời chatbot) và gắn số trang vào metadata mỗi chunk (để dùng trích
    dẫn nguồn sau này, vd 'theo trang 5 của văn bản...').

    Trả về (pieces, trang_cuoi_cung) - trang_cuoi_cung để header-chunk kế tiếp
    kế thừa đúng số trang đang ở, vì marker trang có thể nằm giữa 2 header-chunk
    khác nhau (mỗi header-chunk được xử lý qua hàm này 1 lần riêng biệt).
    """
    from langchain_text_splitters import RecursiveCharacterTextSplitter
    sub_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)

    pieces = []
    last_end = 0
    page = current_page

    for m in SPECIAL_BLOCK_PATTERN.finditer(text):
        before = text[last_end:m.start()]
        if before.strip():
            pieces.extend([{"content": p, "is_table": False, "trang": page}
                            for p in sub_splitter.split_text(before)])

        if m.group("table"):
            table_text = m.group("table")
            rows = table_text.strip("\n").split("\n")
            if len(rows) <= TABLE_MAX_ROWS_PER_CHUNK + 2:
                pieces.append({"content": table_text, "is_table": True, "trang": page})
            else:
                header_row, separator_row = rows[0], rows[1]
                data_rows = rows[2:]
                for i in range(0, len(data_rows), TABLE_MAX_ROWS_PER_CHUNK):
                    group = data_rows[i:i + TABLE_MAX_ROWS_PER_CHUNK]
                    sub_table = "\n".join([header_row, separator_row] + group)
                    pieces.append({"content": sub_table, "is_table": True, "trang": page})
        else:
            # marker trang - cập nhật trang hiện tại, KHÔNG tạo piece (bị xóa khỏi nội dung)
            page = int(m.group("pagenum"))

        last_end = m.end()

    tail = text[last_end:]
    if tail.strip():
        pieces.extend([{"content": p, "is_table": False, "trang": page}
                        for p in sub_splitter.split_text(tail)])

    return pieces, page


EMAIL_PATTERN = re.compile(r"[\w.\-]+@[\w.\-]+\.\w+")


def chunk_markdown(markdown_text: str, doc_metadata: dict) -> list:
    from langchain_text_splitters import MarkdownHeaderTextSplitter

    header_splitter = MarkdownHeaderTextSplitter(
        headers_to_split_on=[("#", "chuong_hoac_muc"), ("##", "dieu"), ("###", "muc_con")],
        strip_headers=False,
    )
    header_chunks = header_splitter.split_text(markdown_text)

    # CHỈ tìm email theo header khi:
    #   (a) loai_van_ban == "Khác"  (các file gộp nhiều thực thể, vd lecturer.txt), HOẶC
    #   (b) admin tự bật cờ "tim_email_theo_header": true trong metadata_xac_nhan.
    # Lý do: với các văn bản hành chính chuẩn (Quyết định/Quy chế/...), email trong
    # văn bản thường là email liên hệ chung của đơn vị - KHÔNG phải primary_key để
    # nối RAG <-> bảng, gán bừa sẽ gây nhiễu metadata cho mọi chunk của văn bản.
    bat_tim_email = (
        doc_metadata.get("loai_van_ban") == "Khác"
        or doc_metadata.get("tim_email_theo_header") is True
    )

    email_by_group: dict = {}
    if bat_tim_email:
        for chunk in header_chunks:
            group_key = chunk.metadata.get("chuong_hoac_muc")
            if not group_key or group_key in email_by_group:
                continue
            match = EMAIL_PATTERN.search(chunk.page_content)
            if match:
                email_by_group[group_key] = match.group(0).lower()

    final_chunks = []
    current_page = 1
    for chunk in header_chunks:
        pieces, current_page = split_preserving_tables(
            chunk.page_content, CHUNK_SIZE, CHUNK_OVERLAP, current_page
        )
        group_key = chunk.metadata.get("chuong_hoac_muc")
        entity_email = email_by_group.get(group_key) if group_key else None
        for piece in pieces:
            chunk_meta = {
                **doc_metadata,
                **chunk.metadata,
                "content_type": "table" if piece["is_table"] else "text",
                "trang": piece["trang"],
            }
            if entity_email:
                chunk_meta["entity_email"] = entity_email
            final_chunks.append({
                "content": piece["content"],
                "metadata": chunk_meta,
            })
    return final_chunks


# ============================================================
# BƯỚC 5 — prepare (metadata gợi ý + markdown) / finalize (chunk cuối)
# ============================================================

MARKDOWN_HEADER_LINE_PATTERN = re.compile(r"(?m)^#{1,6}\s+\S")


def _doan_markdown_da_chinh_xac(text: str) -> bool:
    """Đoán mặc định cho cờ 'markdown_da_chinh_xac' khi nguồn gọi không tự
    truyền vào: True nếu văn bản ĐÃ có ít nhất 1 dòng header markdown thật
    ('#'/'##'... + khoảng trắng + chữ) - thường gặp ở file .md export sẵn có
    cấu trúc, hoặc .txt đã qua pipeline_docx_txt.py (đọc trực tiếp bằng
    python-docx nên header đã chính xác tuyệt đối). Ngược lại (file .txt
    thuần không có header nào) mặc định False - vẫn cần chạy quy tắc đánh
    header theo loai_van_ban như trước (OCR-based)."""
    return bool(MARKDOWN_HEADER_LINE_PATTERN.search(text))


def prepare_for_review(txt_input_path: str, review_output_path: str,
                        markdown_da_chinh_xac: Optional[bool] = None) -> dict:
    ocr_text = open(txt_input_path, encoding="utf-8").read()

    suggested_metadata = extract_document_metadata(ocr_text[:2000])
    suggested_type = suggested_metadata.get("loai_van_ban")
    if suggested_type not in LOAI_VAN_BAN_OPTIONS:
        suggested_type = "Khác"

    # Mặc định: bật tìm email theo header nếu loại là "Khác"; còn lại tắt.
    # Admin có thể sửa trực tiếp trong _review.json nếu muốn bật/tắt thủ công.
    tim_email_mac_dinh = (suggested_type == "Khác")

    # 'markdown_da_chinh_xac': True => bước 'generate' sẽ DÙNG THẲNG văn bản
    # này làm markdown cuối, KHÔNG chạy lại quy tắc đánh header theo
    # loai_van_ban (đúng nhánh "Không" trong sơ đồ TXT/MD: "Dùng thẳng
    # markdown gốc - KHÔNG chạy lại quy tắc đánh header"). Nguồn PDF/OCR
    # không truyền tham số này nên luôn None -> tự đoán False (giữ đúng hành
    # vi cũ, không đổi gì cho luồng PDF).
    if markdown_da_chinh_xac is None:
        markdown_da_chinh_xac = _doan_markdown_da_chinh_xac(ocr_text)

    review_package = {
        "ocr_text": ocr_text,
        "metadata_goi_y": suggested_metadata,
        "metadata_xac_nhan": {
            **suggested_metadata,
            "loai_van_ban": suggested_type,
            "tim_email_theo_header": tim_email_mac_dinh,
            "markdown_da_chinh_xac": markdown_da_chinh_xac,
        },
        "loai_van_ban_options": LOAI_VAN_BAN_OPTIONS,
    }
    with open(review_output_path, "w", encoding="utf-8") as f:
        json.dump(review_package, f, ensure_ascii=False, indent=2)

    print(f"    Gợi ý: loai_van_ban={suggested_type!r}, so_hieu={suggested_metadata.get('so_hieu')!r}")
    print(f"    tim_email_theo_header mặc định = {tim_email_mac_dinh}")
    print(f"    markdown_da_chinh_xac mặc định = {markdown_da_chinh_xac} "
          f"({'sẽ dùng thẳng markdown gốc khi generate' if markdown_da_chinh_xac else 'sẽ chạy quy tắc đánh header khi generate'})")
    print(f"    -> Lưu review package: {review_output_path}")
    print(f"    -> Mở file này, sửa 'metadata_xac_nhan' (đặc biệt 'loai_van_ban' và "
          f"'markdown_da_chinh_xac' nếu cần đổi ý), rồi chạy 'generate' để sinh markdown hoàn chỉnh.")
    return review_package


def generate_markdown_for_review(review_output_path: str) -> str:
    """
    BƯỚC 2: SAU KHI đã xác nhận loai_van_ban, sinh markdown hoàn chỉnh dựa
    trên loại ĐÃ XÁC NHẬN (không phải loại gợi ý ban đầu).

    QUAN TRỌNG - đã đổi cách lưu: markdown được ghi ra 1 FILE .md RIÊNG (cùng
    tên với file review nhưng bỏ hậu tố '_review', đuôi '.md') thay vì nhét
    vào trong JSON. Lý do: JSON lưu xuống dòng dưới dạng ký tự thoát '\\n',
    mở bằng text editor thường sẽ thấy cả đoạn markdown dồn thành 1 dòng dài
    đầy '\\n', GẦN NHƯ KHÔNG ĐỌC ĐƯỢC để rà soát. File .md riêng có xuống dòng
    thật, mở bằng bất kỳ text editor/markdown previewer nào cũng đọc bình
    thường - đây chính là file bạn cần SỬA TRỰC TIẾP khi rà soát.

    Chạy lại được nhiều lần (vd sau khi đổi ý loai_van_ban) - mỗi lần chạy sẽ
    SINH LẠI và GHI ĐÈ file .md. Vì vậy: chỉ chạy lệnh này TRƯỚC KHI bắt đầu
    sửa tay file .md - nếu đã sửa tay rồi mà chạy lại 'generate', sẽ MẤT các
    sửa tay đó.
    """
    review_package = json.load(open(review_output_path, encoding="utf-8"))

    ocr_text = review_package["ocr_text"]
    confirmed_metadata = review_package["metadata_xac_nhan"]
    confirmed_type = confirmed_metadata.get("loai_van_ban")

    if confirmed_type not in LOAI_VAN_BAN_OPTIONS:
        raise ValueError(f"loai_van_ban '{confirmed_type}' không nằm trong danh sách cố định "
                          f"{LOAI_VAN_BAN_OPTIONS} - cần xác nhận lại từ dropdown trước khi generate.")

    if confirmed_metadata.get("markdown_da_chinh_xac"):
        # docx đọc trực tiếp bằng python-docx (chính xác tuyệt đối, không cần
        # đoán) hoặc txt/md đã có header thật sẵn - KHÔNG chạy markup_headers
        # (chạy vào sẽ có nguy cơ đội nhầm/đánh trùng header đã đúng).
        final_markdown = ocr_text
        print(f"    markdown_da_chinh_xac=True -> dùng thẳng markdown gốc, KHÔNG chạy quy tắc đánh header.")
    else:
        final_markdown = markup_headers(ocr_text, confirmed_type)

    base_name = Path(review_output_path).stem.replace("_review", "")
    md_path = os.path.join(output_md_folder, base_name + ".md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(final_markdown)

    print(f"    Đã sinh markdown hoàn chỉnh theo loai_van_ban='{confirmed_type}'.")
    print(f"    -> Đã ghi file markdown DỄ ĐỌC: {md_path}")
    print(f"    -> MỞ ĐÚNG FILE .md NÀY (không phải file _review.json) để rà soát/SỬA TRỰC TIẾP, "
          f"rồi chạy 'finalize'.")
    return final_markdown


def finalize_after_review(review_output_path: str, md_output_path: str, chunk_output_path: str):
    """
    BƯỚC 3: đọc markdown từ FILE .md (đã qua 'generate', admin có thể đã sửa
    trực tiếp trong file .md đó) - KHÔNG đọc từ JSON nữa (JSON không phù hợp
    để rà soát/sửa nội dung dài do escape '\\n'). File .md chính là
    'md_output_path' (cùng đường dẫn 'generate' đã ghi ra) - finalize đọc lại
    đúng file đó (giữ nguyên mọi sửa tay), rồi mới chunk.
    """
    review_package = json.load(open(review_output_path, encoding="utf-8"))

    if not os.path.exists(md_output_path):
        raise ValueError(
            f"Chưa có file markdown '{md_output_path}' - cần chạy 'generate' trước "
            f"(sau khi đã xác nhận 'loai_van_ban' đúng) rồi mới 'finalize'."
        )

    final_markdown = open(md_output_path, encoding="utf-8").read()
    confirmed_metadata = normalize_metadata(review_package["metadata_xac_nhan"])
    confirmed_type = confirmed_metadata.get("loai_van_ban")

    if confirmed_type not in LOAI_VAN_BAN_OPTIONS:
        raise ValueError(f"loai_van_ban '{confirmed_type}' không nằm trong danh sách cố định "
                          f"{LOAI_VAN_BAN_OPTIONS} - cần chọn lại từ dropdown.")

    chunks = chunk_markdown(final_markdown, confirmed_metadata)
    with open(chunk_output_path, "w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False, indent=2)

    print(f"    Đã chốt: loai_van_ban={confirmed_type!r}, so_hieu={confirmed_metadata.get('so_hieu')!r}")
    print(f"    -> Đã tạo {len(chunks)} chunks (đọc từ file markdown: {md_output_path})")
    print(f"    -> Lưu chunks: {chunk_output_path}")
    return chunks


def prepare_batch_folder():
    txt_files = [f for f in os.listdir(output_txt_folder) if f.lower().endswith(".txt")]
    print(f"Tìm thấy {len(txt_files)} file .txt cần prepare.\n" + "=" * 40)

    da_xu_ly, da_bo_qua = 0, 0
    for filename in txt_files:
        txt_path = os.path.join(output_txt_folder, filename)
        base_name = Path(filename).stem
        review_path = os.path.join(output_md_folder, base_name + "_review.json")

        if os.path.exists(review_path):
            print(f"⏭  Bỏ qua {filename} (đã có _review.json - xóa file đó nếu muốn tạo lại)")
            da_bo_qua += 1
            continue

        print(f"▶ Đang prepare: {filename}")
        try:
            prepare_for_review(txt_path, review_path)
            da_xu_ly += 1
        except Exception as e:
            print(f"❌ Lỗi khi prepare {filename}: {e}")
        print()

    print("=" * 40)
    print(f"ĐÃ PREPARE XONG: {da_xu_ly} file mới, bỏ qua {da_bo_qua} file đã có sẵn.")
    print("⚠️  QUAN TRỌNG: mở TỪNG file '..._review.json' trong thư mục markdown, "
          "kiểm tra/sửa 'metadata_xac_nhan' (đặc biệt 'loai_van_ban') TRƯỚC KHI chạy finalize batch.")


def generate_batch_folder():
    review_files = [f for f in os.listdir(output_md_folder) if f.endswith("_review.json")]
    print(f"Tìm thấy {len(review_files)} file _review.json.")
    print("⚠️  Giả định bạn ĐÃ SOÁT 'loai_van_ban' trong 'metadata_xac_nhan' của từng file trước khi chạy lệnh này.\n" + "=" * 40)

    da_xu_ly, da_bo_qua = 0, 0
    for filename in review_files:
        review_path = os.path.join(output_md_folder, filename)
        base_name = filename[:-len("_review.json")]
        md_path = os.path.join(output_md_folder, base_name + ".md")

        if os.path.exists(md_path):
            print(f"⏭  Bỏ qua {filename} (đã có file .md - xóa file đó nếu muốn sinh lại)")
            da_bo_qua += 1
            continue

        print(f"▶ Đang generate: {filename}")
        try:
            generate_markdown_for_review(review_path)
            da_xu_ly += 1
        except Exception as e:
            print(f"❌ Lỗi khi generate {filename}: {e}")
        print()

    print("=" * 40)
    print(f"ĐÃ GENERATE XONG: {da_xu_ly} file mới, bỏ qua {da_bo_qua} file đã có sẵn.")
    print("⚠️  Nhớ MỞ TỪNG FILE .md (không phải _review.json), sửa trực tiếp nếu cần, "
          "TRƯỚC KHI chạy finalize batch.")


def finalize_batch_folder():
    review_files = [f for f in os.listdir(output_md_folder) if f.endswith("_review.json")]
    print(f"Tìm thấy {len(review_files)} file _review.json cần finalize.")
    print("⚠️  Giả định bạn ĐÃ MỞ VÀ SOÁT hết các file này trước khi chạy lệnh này.\n" + "=" * 40)

    da_xu_ly, da_bo_qua = 0, 0
    for filename in review_files:
        review_path = os.path.join(output_md_folder, filename)
        base_name = filename[:-len("_review.json")]
        md_path = os.path.join(output_md_folder, base_name + ".md")
        chunk_path = os.path.join(output_chunk_folder, base_name + ".json")

        if os.path.exists(chunk_path):
            print(f"⏭  Bỏ qua {filename} (đã có chunk .json - xóa file đó nếu muốn tạo lại)")
            da_bo_qua += 1
            continue

        print(f"▶ Đang finalize: {filename}")
        try:
            finalize_after_review(review_path, md_path, chunk_path)
            da_xu_ly += 1
        except Exception as e:
            print(f"❌ Lỗi khi finalize {filename}: {e}")
        print()

    print("=" * 40)
    print(f"ĐÃ FINALIZE XONG: {da_xu_ly} file mới, bỏ qua {da_bo_qua} file đã có sẵn.")


# ============================================================
# MAIN — điều phối 3 lệnh con
# ============================================================

def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("ocr", "prepare", "generate", "finalize"):
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]
    path = sys.argv[2] if len(sys.argv) > 2 else None

    if command == "ocr":
        if path:
            ocr_single_file(path)
        else:
            ocr_batch_folder()

    elif command == "prepare":
        if path:
            base_name = Path(path).stem
            review_output_path = os.path.join(output_md_folder, base_name + "_review.json")
            print(f"▶ [prepare] Đang xử lý: {path}")
            prepare_for_review(path, review_output_path)
        else:
            prepare_batch_folder()

    elif command == "generate":
        if path:
            print(f"▶ [generate] Đang xử lý: {path}")
            generate_markdown_for_review(path)
        else:
            generate_batch_folder()

    elif command == "finalize":
        if path:
            base_name = Path(path).stem.replace("_review", "")
            md_output_path = os.path.join(output_md_folder, base_name + ".md")
            chunk_output_path = os.path.join(output_chunk_folder, base_name + ".json")
            print(f"▶ [finalize] Đang xử lý: {path}")
            finalize_after_review(path, md_output_path, chunk_output_path)
        else:
            finalize_batch_folder()


if __name__ == "__main__":
    main()