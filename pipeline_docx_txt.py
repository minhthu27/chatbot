"""
Xử lý .docx/.doc/.txt/.md -> đưa vào hàng đợi .txt chung -> dùng LẠI đúng
lệnh 'prepare'/'generate'/'finalize' của pipeline_pdf.py để đi tiếp (không lặp
code) -> dùng LẠI đúng conflict_detection.py (list-pending/check/rebuild-
index) để phát hiện xung đột & lên production - ĐÚNG những gì đã làm cho
PDF, chỉ khác bước đầu (không cần OCR). Khớp 2 sơ đồ nghiệp vụ:
  "Quy trình xử lý dữ liệu phi cấu trúc (DOCX,DOC)"
  "Quy trình xử lý dữ liệu phi cấu trúc (TXT, MD)"

CÁC LỆNH:
    python pipeline_docx_txt.py docx                       # batch toàn bộ thư mục raw/docx (.docx/.doc)
    python pipeline_docx_txt.py docx <duong_dan.docx>       # 1 file .docx/.doc
    python pipeline_docx_txt.py txt                         # batch toàn bộ thư mục raw/txt
    python pipeline_docx_txt.py txt <duong_dan.txt>         # 1 file .txt có sẵn
    python pipeline_docx_txt.py md                          # batch toàn bộ thư mục raw/md
    python pipeline_docx_txt.py md <duong_dan.md>           # 1 file .md có sẵn
    python pipeline_docx_txt.py list                        # xem trạng thái hàng đợi (mọi nguồn)
    python pipeline_docx_txt.py detail <base_name>          # "Xem chi tiết" 1 file: nội dung + metadata gợi ý

MỖI LỆNH docx/txt/md Ở TRÊN TỰ LÀM LUÔN 3 VIỆC ĐẦU TIÊN TRONG SƠ ĐỒ ("Đưa
file vào hàng đợi" -> "Đọc trực tiếp"/convert -> "Prepare - gợi ý metadata"
-> "Cập nhật trạng thái hàng đợi"), KHÔNG cần gọi tay 'pipeline_pdf.py prepare'
nữa (dù vẫn dùng lại đúng hàm prepare_for_review() của pipeline_pdf.py bên
trong - không lặp code). Với .docx/.doc, vì markdown đã chính xác tuyệt đối
nên tự làm luôn cả bước 'generate' (không có nhánh rẽ nào trong sơ đồ DOCX
cả) - Admin chỉ cần 1 bước rà soát cuối. Với .txt/.md, ĐÚNG như nhánh rẽ
"Có sinh markdown theo metadata không?" trong sơ đồ, việc "sinh hay dùng
thẳng" được quyết định bởi cờ 'markdown_da_chinh_xac' trong
'..._review.json' (tự đoán: có header '#' sẵn -> dùng thẳng; không có ->
vẫn sinh theo loai_van_ban) - ĐỂ ADMIN TỰ XÁC NHẬN rồi mới chạy 'generate'.

SAU KHI ADMIN ĐÃ XÁC NHẬN metadata + rà soát xong markdown (sửa trực tiếp
file .md nếu cần), dùng ĐÚNG các lệnh đã có, y hệt luồng PDF (đồng bộ hoàn
toàn, không phân biệt nguồn gốc file):

    # CHỈ cần cho .txt/.md có markdown_da_chinh_xac=false (xem 'list' để biết
    # file nào cần) - .docx đã tự chạy bước này rồi, KHÔNG chạy lại (sẽ ghi
    # đè mất bản đã tự sinh, dù nội dung thường giống hệt vì cùng logic):
    python pipeline_pdf.py generate <duong_dan_review.json>

    # Luôn cần cho MỌI nguồn (docx/txt/md), sau khi đã rà soát xong file .md:
    python pipeline_pdf.py finalize <duong_dan_review.json>

    # Rồi phát hiện xung đột với dữ liệu production hiện có - ĐÚNG module đã
    # dùng cho PDF, không cần sửa gì thêm (conflict_detection.py định danh 1
    # văn bản bằng base_name/chunk file, không quan tâm nguồn gốc PDF hay
    # Word/txt/md):
    python conflict_detection.py list-pending
    python conflict_detection.py check <base_name>
    # 'check' đã tự hỏi chạy 'rebuild-index' (embedding + cập nhật FAISS +
    # báo api8923.py nạp lại) ngay khi có nội dung được duyệt, khớp đúng
    # nhánh "Không xung đột -> Embedding -> Cập nhật FAISS" ở cuối sơ đồ.

KHÔNG CẦN OCR cho .docx: Word lưu sẵn cấu trúc thật (style Heading, bảng thật)
nên đọc trực tiếp bằng python-docx là CHÍNH XÁC TUYỆT ĐỐI, không cần LLM,
không tốn phí. Với file KHÔNG dùng style Heading chuẩn (rất hay gặp - người
soạn chỉ bôi đậm chữ), fallback: đoạn văn ngắn (<100 ký tự) in đậm toàn bộ
được coi là header cấp 1.

.txt/.md có sẵn: nếu đã có header markdown ('#'/'##') sẵn trong file (thường
gặp với file export sẵn có cấu trúc), pipeline_pdf.py sẽ tự nhận ra (qua cờ
'markdown_da_chinh_xac') và dùng thẳng khi 'generate', không cần xử lý gì
thêm ở đây.

QUY ƯỚC THƯ MỤC NGUỒN (PHẢI khớp RAW_INPUT_FOLDERS trong
conflict_detection.py - dùng để Admin mở lại được file gốc lúc rà soát xung
đột):
    data/raw/docx/  <- .docx VÀ .doc (dùng chung 1 thư mục)
    data/raw/txt/   <- .txt
    data/raw/md/    <- .md

Cài đặt:
    pip install python-docx
    pip install pywin32   # CHỈ cần nếu có file .doc cũ và muốn tự chuyển trên Windows
"""

import os
import re
import sys
import time
from pathlib import Path

from pipeline_pdf import (
    output_txt_folder,
    output_md_folder,
    output_chunk_folder,
    BASE,
    prepare_for_review,
    generate_markdown_for_review,
)

input_docx_folder = str(BASE / "data" / "raw" / "docx")   # dùng chung cho .docx VÀ .doc
input_txt_folder = str(BASE / "data" / "raw" / "txt")
input_md_folder = str(BASE / "data" / "raw" / "md")

for _folder in (input_docx_folder, input_txt_folder, input_md_folder):
    os.makedirs(_folder, exist_ok=True)


def docx_to_markdown_text(docx_path: str) -> str:
    """Đọc trực tiếp .docx bằng python-docx - chính xác tuyệt đối vì Word lưu
    sẵn cấu trúc thật (không phải đoán như OCR ảnh). Giữ đúng thứ tự xen kẽ
    đoạn văn/bảng như trong file gốc."""
    from docx import Document
    from docx.oxml.ns import qn

    doc = Document(docx_path)

    used_heading_styles = any(
        "heading" in (p.style.name or "").lower() or (p.style.name or "").lower() == "title"
        for p in doc.paragraphs
    )

    def get_list_ilvl(p):
        """Trả về cấp thụt lề nếu đoạn văn thực sự là mục trong danh sách
        đánh số/bullet của Word (có numPr trong XML), ngược lại trả về None.
        numPr chỉ tồn tại khi người soạn dùng nút Numbering/Bullets của Word -
        đây là tín hiệu chắc chắn, không phải suy đoán qua tên style."""
        pPr = p._p.pPr
        if pPr is None:
            return None
        numPr = pPr.find(qn("w:numPr"))
        if numPr is None:
            return None
        ilvl_el = numPr.find(qn("w:ilvl"))
        try:
            return int(ilvl_el.get(qn("w:val"))) if ilvl_el is not None else 0
        except (TypeError, ValueError):
            return 0

    lines = []
    body = doc.element.body
    for child in body.iterchildren():
        if child.tag == qn("w:p"):
            para = next((p for p in doc.paragraphs if p._p == child), None)
            if para is None or not para.text.strip():
                continue
            text = para.text.strip()
            style = (para.style.name or "").lower()
            ilvl = get_list_ilvl(para)

            if "heading 1" in style or style == "title":
                lines.append(f"# {text}")
            elif "heading 2" in style:
                lines.append(f"## {text}")
            elif "heading 3" in style or "heading 4" in style:
                lines.append(f"### {text}")
            elif ilvl is not None:
                # List/numbering THẬT theo XML (numPr) - LUÔN giữ làm mục danh
                # sách theo đúng cấp thụt lề, KHÔNG bao giờ đội thành header dù
                # dòng ngắn/không có dấu câu cuối (khác cách đoán cũ).
                lines.append(("  " * ilvl) + f"- {text}")
            elif (not used_heading_styles and style == "normal" and len(text) < 100
                  and para.runs and all(r.bold for r in para.runs if r.text.strip())):
                lines.append(f"# {text}")
            elif (not used_heading_styles and style == "normal"
                  and len(text) < 50
                  and not text.rstrip().endswith((".", ":", ";", ","))
                  and not text.rstrip().endswith("…")):
                # Lưới an toàn cấp 3: dòng NGẮN, KHÔNG kết thúc bằng dấu câu -
                # nghi là tiêu đề con (vd 'Sứ mệnh', 'Chiến lược đào tạo') dù
                # không có tín hiệu định dạng nào phân biệt với đoạn văn thường
                # (Word không lưu khác biệt - đây là suy đoán theo nội dung,
                # không hoàn hảo, chỉ dùng làm cấp 3 để không phá vỡ cấp 1/2
                # đã đúng nếu đoán sai). CHỈ áp dụng cho style 'normal' thật -
                # list đã được xử lý chắc chắn ở nhánh 'ilvl is not None' phía
                # trên nên không lọt xuống đây nữa.
                lines.append(f"### {text}")
            elif "list" in style or "bullet" in style:
                # Dự phòng: style TÊN có 'list'/'bullet' nhưng không có numPr
                # (hiếm, thường do format tay không qua nút Numbering/Bullets).
                lines.append(f"- {text}")
            else:
                lines.append(text)

        elif child.tag == qn("w:tbl"):
            table = next((t for t in doc.tables if t._tbl == child), None)
            if table is None or not table.rows:
                continue
            header_cells = [c.text.strip().replace("\n", " ") for c in table.rows[0].cells]
            lines.append("| " + " | ".join(header_cells) + " |")
            lines.append("|" + "---|" * len(header_cells))
            for row in table.rows[1:]:
                cells = [c.text.strip().replace("\n", " ") for c in row.cells]
                lines.append("| " + " | ".join(cells) + " |")

    return "\n\n".join(lines)


def convert_doc_to_docx_windows(doc_path: str) -> str:
    """Chuyển .doc (định dạng cũ) sang .docx bằng Word COM automation - CHỈ
    chạy được trên Windows, CẦN có Microsoft Word cài sẵn."""
    import win32com.client

    doc_path_abs = str(Path(doc_path).resolve())
    docx_path_abs = str(Path(doc_path).with_suffix(".docx").resolve())

    word = win32com.client.Dispatch("Word.Application")
    word.Visible = False
    try:
        wdoc = word.Documents.Open(doc_path_abs)
        wdoc.SaveAs(docx_path_abs, FileFormat=16)  # 16 = .docx
        wdoc.Close()
    finally:
        word.Quit()

    return docx_path_abs


def convert_to_markdown_text(path: str) -> str:
    if path.lower().endswith(".docx"):
        return docx_to_markdown_text(path)

    if path.lower().endswith(".doc"):
        try:
            docx_path = convert_doc_to_docx_windows(path)
        except Exception as e:
            raise RuntimeError(
                f"Không tự động chuyển được file .doc cũ sang .docx ({e}). "
                f"Cần cài pywin32 (pip install pywin32) VÀ có Microsoft Word trên máy. "
                f"Cách khắc phục nhanh: mở file bằng Word, 'Save As' -> .docx, "
                f"rồi chạy lại với file .docx vừa lưu."
            ) from e
        return docx_to_markdown_text(docx_path)

    raise ValueError(f"Không hỗ trợ định dạng file: {path} (chỉ .docx, .doc)")


# ============================================================
# HÀNG ĐỢI CHUNG — đưa file vào hàng đợi .txt + tự chạy 'prepare' (và
# 'generate' luôn cho docx) - dùng LẠI đúng hàm của pipeline_pdf.py.
# ============================================================

def _review_path_for(base_name: str) -> str:
    return os.path.join(output_md_folder, base_name + "_review.json")


def _queue_and_prepare(base_name: str, content: str, markdown_da_chinh_xac):
    """Việc DÙNG CHUNG cho docx/txt/md, khớp 4 bước đầu bên 'Hệ thống' trong
    sơ đồ: (1) đưa vào hàng đợi .txt, (2) [đọc/convert - đã làm ở nơi gọi],
    (3) prepare - gợi ý metadata (TÁI DÙNG prepare_for_review của
    pipeline_pdf.py, không lặp code), (4) 'cập nhật trạng thái hàng đợi' - ở đây
    tương đương việc _review.json đã tồn tại, xem được qua lệnh 'list'."""
    txt_queue_path = os.path.join(output_txt_folder, base_name + ".txt")
    with open(txt_queue_path, "w", encoding="utf-8") as f:
        f.write(content)

    review_path = _review_path_for(base_name)
    if os.path.exists(review_path):
        print(f"⏭  Bỏ qua prepare cho '{base_name}' (đã có {os.path.basename(review_path)} - "
              f"xóa file đó nếu muốn prepare lại từ đầu).")
        return review_path

    prepare_for_review(txt_queue_path, review_path, markdown_da_chinh_xac=markdown_da_chinh_xac)
    return review_path


def _auto_generate_if_markdown_da_chinh_xac(review_path: str, base_name: str):
    """CHỈ dùng cho .docx/.doc: markdown đã chính xác tuyệt đối (đọc trực
    tiếp bằng python-docx) nên KHÔNG có nhánh rẽ nào trong sơ đồ DOCX cả -
    tự chạy luôn 'generate' (TÁI DÙNG generate_markdown_for_review của
    pipeline_pdf.py) để Admin chỉ cần đúng 1 bước rà soát cuối, thay vì phải tự
    gõ thêm lệnh 'pipeline_pdf.py generate'. KHÔNG dùng cho .txt/.md vì sơ đồ
    TXT/MD có nhánh rẽ tường minh do Admin quyết định sau khi xác nhận
    metadata - để dành cho Admin tự chạy 'generate' khi đã sẵn sàng."""
    md_path = os.path.join(output_md_folder, base_name + ".md")
    if os.path.exists(md_path):
        print(f"⏭  Bỏ qua generate cho '{base_name}' (đã có {os.path.basename(md_path)} - "
              f"xóa file đó nếu muốn sinh lại).")
        return
    generate_markdown_for_review(review_path)


# ============================================================
# .docx / .doc
# ============================================================

def docx_single_file(path: str):
    base_name = Path(path).stem
    print(f"▶ [docx] Đang xử lý: {path}")
    t_start = time.time()
    markdown_text = convert_to_markdown_text(path)

    review_path = _queue_and_prepare(base_name, markdown_text, markdown_da_chinh_xac=True)
    _auto_generate_if_markdown_da_chinh_xac(review_path, base_name)

    print(f"✅ Hoàn tất! ({time.time()-t_start:.1f}s) -> markdown đã sẵn sàng: "
          f"{os.path.join(output_md_folder, base_name + '.md')}")
    print(f"    -> Rà soát/sửa trực tiếp file .md đó nếu cần, xác nhận metadata trong "
          f"{os.path.basename(review_path)}, rồi chạy: python pipeline_pdf.py finalize {review_path}")


def docx_batch_folder():
    files = [f for f in os.listdir(input_docx_folder) if f.lower().endswith((".docx", ".doc"))]
    print(f"Tìm thấy {len(files)} file .docx/.doc cần xử lý.\n" + "=" * 40)

    thanh_cong, that_bai = 0, 0
    for filename in files:
        path = os.path.join(input_docx_folder, filename)
        print(f"▶ Đang xử lý file: {filename}")
        try:
            docx_single_file(path)
            thanh_cong += 1
        except Exception as e:
            print(f"❌ Có lỗi xảy ra khi xử lý file {filename}: {str(e)}\n")
            that_bai += 1

    print("=" * 40)
    print(f"ĐÃ XỬ LÝ XONG {len(files)} FILE — thành công {thanh_cong}, lỗi {that_bai}")


# ============================================================
# .txt / .md có sẵn — cùng 1 logic, chỉ khác thư mục nguồn + nhãn log
# ============================================================

def _plain_text_single_file(path: str, nhan: str):
    """.txt/.md có sẵn: chỉ cần chuẩn hóa xuống dòng (\\r\\n -> \\n) rồi đưa
    vào hàng đợi + prepare - KHÔNG truyền markdown_da_chinh_xac tường minh
    (để None), pipeline_pdf.prepare_for_review() sẽ TỰ ĐOÁN dựa trên việc file
    đã có dòng header '#'/'##' sẵn hay chưa (đúng điều kiện nhánh rẽ "Có
    sinh markdown theo metadata không?" trong sơ đồ TXT/MD) - Admin xem lại
    trong '..._review.json' (trường 'markdown_da_chinh_xac'), tự đổi nếu
    đoán sai, rồi mới chạy 'pipeline_pdf.py generate'."""
    base_name = Path(path).stem
    print(f"▶ [{nhan}] Đang xử lý: {path}")
    content = open(path, encoding="utf-8").read().replace("\r\n", "\n")

    review_path = _queue_and_prepare(base_name, content, markdown_da_chinh_xac=None)

    with open(review_path, encoding="utf-8") as f:
        import json
        goi_y = json.load(f)["metadata_xac_nhan"].get("markdown_da_chinh_xac")
    print(f"✅ Đã đưa vào hàng đợi + prepare: {review_path}")
    print(f"    markdown_da_chinh_xac (tự đoán) = {goi_y} - mở file trên để xác nhận lại nếu cần.")
    if goi_y:
        print(f"    -> File đã có header sẵn: có thể chạy thẳng "
              f"'python pipeline_pdf.py generate {review_path}' (dùng thẳng, không đánh header lại).")
    else:
        print(f"    -> Chưa có header: xác nhận đúng 'loai_van_ban' trong file trên rồi chạy "
              f"'python pipeline_pdf.py generate {review_path}' để sinh header theo loại văn bản.")


def txt_single_file(path: str):
    _plain_text_single_file(path, "txt")


def md_single_file(path: str):
    _plain_text_single_file(path, "md")


def _plain_text_batch_folder(folder: str, ext: str, nhan: str, handler):
    files = [f for f in os.listdir(folder) if f.lower().endswith(ext)]
    print(f"Tìm thấy {len(files)} file {ext} cần đưa vào hàng đợi.\n" + "=" * 40)
    thanh_cong, that_bai = 0, 0
    for filename in files:
        path = os.path.join(folder, filename)
        try:
            handler(path)
            thanh_cong += 1
        except Exception as e:
            print(f"❌ Lỗi khi xử lý {filename}: {e}")
            that_bai += 1
    print("=" * 40 + f"\nĐÃ XỬ LÝ XONG {len(files)} FILE {nhan} — thành công {thanh_cong}, lỗi {that_bai}.")


def txt_batch_folder():
    _plain_text_batch_folder(input_txt_folder, ".txt", "TXT", txt_single_file)


def md_batch_folder():
    _plain_text_batch_folder(input_md_folder, ".md", "MD", md_single_file)


# ============================================================
# "list" / "detail" — soi trạng thái hàng đợi trực tiếp từ đĩa (không cần
# thêm 1 file trạng thái riêng dễ lệch dữ liệu) - khớp 2 bước "Cập nhật
# trạng thái hàng đợi" và "Chọn 1 file trong hàng đợi, bấm Xem chi tiết"
# trong sơ đồ. Dùng chung cho MỌI nguồn (docx/txt/md), vì từ bước prepare
# trở đi tất cả đều nằm chung 1 quy ước thư mục (data/processed/...).
# ============================================================

def _all_base_names_in_queue() -> list:
    names = set()
    if os.path.isdir(output_txt_folder):
        names.update(Path(f).stem for f in os.listdir(output_txt_folder) if f.lower().endswith(".txt"))
    return sorted(names)


def _trang_thai_1_file(base_name: str) -> dict:
    review_path = _review_path_for(base_name)
    md_path = os.path.join(output_md_folder, base_name + ".md")
    chunk_path = os.path.join(output_chunk_folder, base_name + ".json")

    info = {
        "base_name": base_name,
        "da_prepare": os.path.exists(review_path),
        "da_generate": os.path.exists(md_path),
        "da_finalize": os.path.exists(chunk_path),
        "loai_van_ban": None,
        "markdown_da_chinh_xac": None,
        "xung_dot": "chưa kiểm tra",
    }

    if info["da_prepare"]:
        import json
        try:
            meta = json.load(open(review_path, encoding="utf-8"))["metadata_xac_nhan"]
            info["loai_van_ban"] = meta.get("loai_van_ban")
            info["markdown_da_chinh_xac"] = meta.get("markdown_da_chinh_xac")
        except Exception:
            pass

    if info["da_finalize"]:
        # Tra cứu trạng thái production qua ĐÚNG manifest của
        # conflict_detection.py - best-effort: nếu module đó lỗi import
        # (thiếu numpy/langchain_ollama...) thì vẫn hiển thị được phần còn
        # lại, chỉ báo "không tra được" thay vì crash cả lệnh 'list'.
        try:
            import conflict_detection as cd
            manifest = cd.load_manifest()
            if base_name in manifest:
                info["xung_dot"] = f"đã ở production ({manifest[base_name].get('trang_thai_van_ban')})"
            else:
                info["xung_dot"] = "ĐANG CHỜ kiểm tra xung đột (chạy: conflict_detection.py check " + base_name + ")"
        except Exception as e:
            info["xung_dot"] = f"không tra được ({e})"

    return info


def list_queue():
    base_names = _all_base_names_in_queue()
    if not base_names:
        print("Hàng đợi đang trống - chưa có file nào được đưa vào (chạy 'docx'/'txt'/'md' trước).")
        return

    print(f"{'BASE_NAME':40} {'PREPARE':8} {'GENERATE':9} {'FINALIZE':9} {'LOAI_VAN_BAN':22} {'MD_CHINH_XAC':13} XUNG_DOT")
    print("-" * 140)
    for base_name in base_names:
        info = _trang_thai_1_file(base_name)
        print(f"{info['base_name'][:40]:40} "
              f"{'✅' if info['da_prepare'] else '⏳':8} "
              f"{'✅' if info['da_generate'] else '⏳':9} "
              f"{'✅' if info['da_finalize'] else '⏳':9} "
              f"{str(info['loai_van_ban'])[:22]:22} "
              f"{str(info['markdown_da_chinh_xac']):13} "
              f"{info['xung_dot']}")


def detail_file(base_name: str):
    """'Xem chi tiết' 1 file trong hàng đợi: hiển thị metadata gợi ý + nội
    dung đã (hoặc chưa) markdown, để Admin rà soát trước khi xác nhận."""
    review_path = _review_path_for(base_name)
    if not os.path.exists(review_path):
        print(f"❌ Chưa có '{os.path.basename(review_path)}' - file '{base_name}' chưa qua bước prepare.")
        return

    import json
    review_package = json.load(open(review_path, encoding="utf-8"))
    print(f"=== {base_name} ===")
    print("-- metadata_goi_y --")
    print(json.dumps(review_package["metadata_goi_y"], ensure_ascii=False, indent=2))
    print("-- metadata_xac_nhan (SỬA TRỰC TIẾP trong file .json này nếu cần) --")
    print(json.dumps(review_package["metadata_xac_nhan"], ensure_ascii=False, indent=2))

    md_path = os.path.join(output_md_folder, base_name + ".md")
    if os.path.exists(md_path):
        content = open(md_path, encoding="utf-8").read()
        print(f"\n-- Nội dung đã markdown ({md_path}, {len(content)} ký tự, xem 500 ký tự đầu) --")
        print(content[:500] + ("..." if len(content) > 500 else ""))
    else:
        print(f"\n-- Chưa có markdown hoàn chỉnh - chạy 'python pipeline_pdf.py generate {review_path}' trước --")
        print(review_package["ocr_text"][:500] + ("..." if len(review_package["ocr_text"]) > 500 else ""))


# ============================================================
# MAIN
# ============================================================

def main():
    if len(sys.argv) == 2 and sys.argv[1] == "list":
        list_queue()
        return

    if len(sys.argv) == 3 and sys.argv[1] == "detail":
        detail_file(sys.argv[2])
        return

    if len(sys.argv) == 2 and sys.argv[1] == "docx":
        docx_batch_folder()
        return
    if len(sys.argv) == 2 and sys.argv[1] == "txt":
        txt_batch_folder()
        return
    if len(sys.argv) == 2 and sys.argv[1] == "md":
        md_batch_folder()
        return

    if len(sys.argv) < 3 or sys.argv[1] not in ("docx", "txt", "md"):
        print(__doc__)
        sys.exit(1)

    command, path = sys.argv[1], sys.argv[2]

    if command == "docx":
        docx_single_file(path)
    elif command == "txt":
        txt_single_file(path)
    elif command == "md":
        md_single_file(path)


if __name__ == "__main__":
    main()
