"""
Module LÕI cho "Phát hiện xung đột - dữ liệu có cấu trúc" (sơ đồ 2) và phần
đối chiếu chéo dùng ở CẢ 2 hướng:

  (A) RAG (phi cấu trúc) -> bảng cấu trúc: dùng bởi conflict_detection.py ở
      bước 9-11 (sơ đồ 1) - 1 đoạn văn bản MỚI vừa được duyệt vào production,
      kiểm tra xem nó có nhắc tới 1 thực thể/thuộc tính đã có trong bảng
      cấu trúc với giá trị KHÁC không.
  (B) Bảng cấu trúc -> bảng cấu trúc: so khớp theo khóa (Bước "So khớp chính
      xác theo khóa" trong sơ đồ 2) - không cần LLM, thuần logic/pandas.
  (B') Bảng cấu trúc -> RAG: phần "cảnh báo chéo" trong sơ đồ 2 - 1 dòng MỚI/
      THAY ĐỔI trong bảng, kiểm tra xem có đoạn RAG nào đang nhắc giá trị
      KHÁC cho cùng thực thể/thuộc tính không.

QUAN TRỌNG - GHI CHÚ TÍCH HỢP HỆ THỐNG THẬT (đọc trước khi thay thế phần này):
Toàn bộ "bảng production có cấu trúc" ở đây đang là các file .csv trong
`data/processed/tables/` (đúng cách api8923.py/multi_entity_matcher.py đang
đọc qua registry.json), chưa phải database thật. Khi có database thật:
  - Thay `load_table_df()`/`save_table_df()` bằng lời gọi DB thật (SELECT/
    UPDATE/INSERT) - đây là 2 hàm DUY NHẤT đọc/viết bảng, mọi logic khác
    (phân nhóm, đối chiếu chéo) chỉ làm việc với DataFrame nên KHÔNG cần
    sửa gì thêm.
  - `registry.json` (primary_key, name_columns, column_labels...) nhiều khả
    năng vẫn cần giữ lại làm metadata mô tả bảng, dù bảng vật lý đổi.
  - Cơ chế "giữ cả 2 + valid_from/valid_to" (xem `ap_dung_quyet_dinh_thay_doi`)
    hiện làm bằng cách cho phép TRÙNG khóa chính trong file .csv (phá vỡ giả
    định 1-dòng-1-khóa mà MultiEntityMatcher.rows_by_pk() đang ngầm định) -
    CẦN đánh giá lại khi có DB thật (có thể cần bảng lịch sử riêng thay vì
    trùng khóa trong cùng bảng).
"""

import json
import re
import shutil
import unicodedata
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from multi_entity_matcher import normalize_vn

BASE = Path(__file__).resolve().parent
REGISTRY_PATH = BASE / "registry.json"
TABLES_STAGING_FOLDER = BASE / "data" / "processed" / "tables_staging"
TABLES_STAGING_FOLDER.mkdir(parents=True, exist_ok=True)
STRUCTURED_DECISION_LOG_PATH = BASE / "data" / "processed" / "conflict" / "structured_decision_log.jsonl"
STRUCTURED_DECISION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)


def _now() -> str:
    return datetime.now().isoformat()


def log_structured_decision(event: dict):
    """Audit trail riêng cho quyết định trên dữ liệu có cấu trúc - tách khỏi
    decision_log.jsonl của conflict_detection.py (2 miền dữ liệu khác nhau),
    nhưng CÙNG định dạng JSON Lines để dễ ghép lại xem chung sau này."""
    event = {"luc": _now(), **event}
    with open(STRUCTURED_DECISION_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


# ============================================================
# Đọc/ghi bảng - 2 HÀM DUY NHẤT động tới "database" (xem ghi chú đầu file)
# ============================================================

def load_registry() -> dict:
    with open(REGISTRY_PATH, encoding="utf-8") as f:
        return json.load(f)


def save_registry(registry: dict):
    with open(REGISTRY_PATH, "w", encoding="utf-8") as f:
        json.dump(registry, f, ensure_ascii=False, indent=2)


def load_table_df(table_name: str, registry: Optional[dict] = None) -> pd.DataFrame:
    registry = registry or load_registry()
    if table_name not in registry:
        return pd.DataFrame()
    path = BASE / registry[table_name]["path"]
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def save_table_df(table_name: str, df: pd.DataFrame, registry: Optional[dict] = None):
    """Ghi đè bảng production - LUÔN backup bản cũ trước (kèm timestamp) để
    có đường lùi nếu ghi sai, vì đây là dữ liệu cấu trúc THẬT đang phục vụ
    api8923.py, không phải dữ liệu test."""
    registry = registry or load_registry()
    path = BASE / registry[table_name]["path"]
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        backup_path = path.with_name(f"{path.stem}.bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}{path.suffix}")
        shutil.copy2(path, backup_path)
    df.to_csv(path, index=False)


# ============================================================
# Bước "Tiền xử lý dữ liệu" (sơ đồ 3) - dùng bởi structured_data_pipeline.py
# ============================================================

_PLACEHOLDER_RONG = {"", "n/a", "na", "-", "--", "chua co", "khong co", "null", "none", "nan"}
_SO_THAP_PHAN_VN_RE = re.compile(r"^-?\d{1,3}(\.\d{3})*,\d+$|^-?\d+,\d+$")


def tien_xu_ly_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Tiền xử lý CHUNG cho mọi bảng upload (đúng sticky note sơ đồ 3):
    - strip khoảng trắng thừa ở TÊN CỘT và giá trị chuỗi
    - chuẩn hóa Unicode NFC cho toàn bộ trường văn bản
    - chuẩn hóa số thập phân kiểu VN (dấu phẩy) -> dấu chấm, CHỈ áp dụng khi
      TOÀN BỘ giá trị không rỗng của 1 cột đều khớp mẫu số kiểu VN - tránh
      làm hỏng cột text thường có dấu phẩy (địa chỉ, ghi chú...)
    - chuẩn hóa các placeholder rỗng phổ biến ("N/A", "-", "chưa có"...)
      thành NaN thật (để pandas/so sánh xử lý đúng, không coi là 1 giá trị)
    KHÔNG tự "sửa" nội dung dữ liệu (không suy diễn)."""
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    def _clean_cell(v):
        if not isinstance(v, str):
            return v
        v2 = unicodedata.normalize("NFC", v.strip())
        if normalize_vn(v2) in _PLACEHOLDER_RONG:
            return np.nan
        return v2

    for col in df.columns:
        if df[col].dtype != object:
            continue
        df[col] = df[col].apply(_clean_cell)
        non_null = df[col].dropna().astype(str)
        if len(non_null) > 0 and non_null.str.match(_SO_THAP_PHAN_VN_RE).all():
            df[col] = df[col].apply(
                lambda v: float(str(v).replace(".", "").replace(",", ".")) if pd.notna(v) else v
            )
    return df


def tim_bang_trung_khop_cot(columns: list, registry: Optional[dict] = None, nguong: float = 0.7):
    """So khớp bộ cột của file MỚI với TỪNG bảng đã có trong registry - nếu
    tỷ lệ trùng cột (Jaccard) vượt ngưỡng, rất có thể Admin gõ NHẦM tên bảng
    (vd 'gv' thay vì 'giangvien'), tránh tạo bảng mới trùng lặp dữ liệu.
    Với vài trăm bảng, không thể trông chờ Admin nhớ hết tên chính xác -
    hệ thống phải tự phát hiện.
    Trả về (table_name, ty_le) của bảng khớp nhất nếu vượt ngưỡng, None nếu
    không có bảng nào đủ giống."""
    registry = registry or load_registry()
    new_cols = set(columns)
    best = None
    for table_name, cfg in registry.items():
        prod_df = load_table_df(table_name, registry)
        if prod_df.empty:
            continue
        old_cols = set(prod_df.columns)
        if not old_cols:
            continue
        ty_le = len(new_cols & old_cols) / len(new_cols | old_cols)
        if ty_le >= nguong and (best is None or ty_le > best[1]):
            best = (table_name, ty_le)
    return best


def kiem_tra_khoa_du_phan_biet(new_df: pd.DataFrame, prod_df: pd.DataFrame, pk_cols: list) -> list:
    """Cảnh báo nếu khóa Admin chỉ định KHÔNG ĐỦ để phân biệt dòng - trùng
    khóa NGAY TRONG file mới upload, hoặc trùng khóa trong production hiện
    có (dấu hiệu thiếu 1 cột trong khóa ghép, vd chỉ khai 'Manganh' cho bảng
    có nhiều dòng theo năm/phương thức xét tuyển). CHỈ cảnh báo, không tự
    suy đoán thêm cột nào - Admin phải tự quyết định --key-cols đúng."""
    canh_bao = []
    trung_trong_file = new_df[new_df.duplicated(subset=pk_cols, keep=False)]
    if not trung_trong_file.empty:
        so_khoa = trung_trong_file.drop_duplicates(subset=pk_cols).shape[0]
        canh_bao.append(f"Khóa {pk_cols} KHÔNG đủ phân biệt: {so_khoa} khóa bị TRÙNG NGAY TRONG file mới "
                         f"upload ({len(trung_trong_file)} dòng liên quan) - rất có thể cần thêm cột vào "
                         f"--key-cols.")
    if not prod_df.empty:
        trung_trong_prod = prod_df[prod_df.duplicated(subset=pk_cols, keep=False)]
        if not trung_trong_prod.empty:
            so_khoa = trung_trong_prod.drop_duplicates(subset=pk_cols).shape[0]
            canh_bao.append(f"Khóa {pk_cols} KHÔNG đủ phân biệt: production hiện có {so_khoa} khóa bị trùng "
                             f"({len(trung_trong_prod)} dòng) - so khớp chỉ xét ĐÚNG 1 dòng đầu tiên mỗi khóa "
                             f"trùng, CÓ THỂ bỏ sót thay đổi ở các dòng còn lại cùng khóa.")
    return canh_bao


# ============================================================
# Bước "So khớp chính xác theo khóa" (sơ đồ 2) - KHÔNG cần LLM
# ============================================================

def _gia_tri_khac_nhau(a, b) -> bool:
    a_rong, b_rong = pd.isna(a), pd.isna(b)
    if a_rong and b_rong:
        return False
    if a_rong or b_rong:
        return True
    try:
        return float(a) != float(b)
    except (TypeError, ValueError):
        return str(a).strip() != str(b).strip()


def phan_nhom_theo_khoa(new_df: pd.DataFrame, prod_df: pd.DataFrame, primary_key,
                         so_sanh_cols: Optional[list] = None) -> dict:
    """3 nhóm phân loại THEO LOGIC HỆ THỐNG (không cần LLM - so khớp CHÍNH
    XÁC theo khóa định danh, đúng sticky note sơ đồ 2):
      1. Khóa CHƯA từng có trong production -> "du_lieu_moi"
      2. Khóa TRÙNG, mọi cột giống hệt -> "trung_lap" (không cần đối chiếu)
      3. Khóa TRÙNG, có cột giá trị khác -> "thay_doi" (nêu rõ cột, giá trị cũ/mới)

    primary_key: tên 1 cột, HOẶC list nhiều cột (khóa ghép - vd bảng 'nganh'
    khóa THẬT là Manganh+Nam dù registry chỉ khai primary_key='Manganh', vì
    1 ngành có nhiều dòng theo năm - CẦN Admin xác nhận khóa ghép qua
    --composite-key ở structured_data_pipeline.py nếu bảng có đặc điểm này,
    nếu không sẽ hiểu SAI mọi năm khác là "thay đổi" của cùng 1 dòng)."""
    pk_cols = primary_key if isinstance(primary_key, list) else [primary_key]
    so_sanh_cols = so_sanh_cols or [c for c in new_df.columns if c not in pk_cols]

    # Ép dtype các cột khóa của new_df khớp với prod_df TRƯỚC khi so khớp:
    # 1 cột mã định danh (vd "Manganh") có thể được production lưu là string
    # (vì có mã bắt đầu bằng '0' ở đâu đó), nhưng file upload MỚI nếu toàn
    # giá trị số thuần sẽ bị pandas suy luận thành int64. Nếu không ép lại,
    # mọi khóa sẽ bị coi là "dữ liệu mới" dù cùng giá trị - bảng sẽ dần bị
    # nhân đôi dữ liệu mà không ai biết.
    if not prod_df.empty:
        for c in pk_cols:
            if c in prod_df.columns and c in new_df.columns and prod_df[c].dtype != new_df[c].dtype:
                new_df = new_df.copy()
                new_df[c] = new_df[c].astype(prod_df[c].dtype)

    du_lieu_moi, trung_lap, thay_doi = [], [], []

    if prod_df.empty:
        prod_indexed = None
    else:
        prod_indexed = prod_df.set_index(pk_cols) if len(pk_cols) > 1 else prod_df.set_index(pk_cols[0])

    for _, row in new_df.iterrows():
        key = tuple(row[c] for c in pk_cols) if len(pk_cols) > 1 else row[pk_cols[0]]

        if prod_indexed is None or key not in prod_indexed.index:
            du_lieu_moi.append(row)
            continue

        old_row = prod_indexed.loc[key]
        if isinstance(old_row, pd.DataFrame):
            # Khóa (theo khai báo) trùng NHIỀU dòng trong production - thường
            # là dấu hiệu thiếu cột khóa ghép (vd chỉ khai Manganh, thiếu Nam)
            # -> KHÔNG đoán, coi là "thay đổi" với dòng đầu tiên và CẢNH BÁO
            # rõ để Admin xem lại --composite-key.
            old_row = old_row.iloc[0]

        khac_cols = {}
        for c in so_sanh_cols:
            if c not in old_row.index:
                continue
            v_moi, v_cu = row.get(c), old_row.get(c)
            if _gia_tri_khac_nhau(v_moi, v_cu):
                khac_cols[c] = {"cu": v_cu, "moi": v_moi}

        if khac_cols:
            thay_doi.append({"key": key, "row_moi": row, "row_cu": old_row, "khac_cols": khac_cols})
        else:
            trung_lap.append(row)

    return {
        "du_lieu_moi": pd.DataFrame(du_lieu_moi) if du_lieu_moi else pd.DataFrame(columns=new_df.columns),
        "trung_lap": pd.DataFrame(trung_lap) if trung_lap else pd.DataFrame(columns=new_df.columns),
        "thay_doi": thay_doi,  # list[dict] (không phải DataFrame - cần giữ khac_cols kèm theo)
    }


def ap_dung_quyet_dinh_thay_doi(prod_df: pd.DataFrame, thay_doi_item: dict, primary_key,
                                 hanh_dong: str) -> pd.DataFrame:
    """Áp dụng quyết định Admin cho 1 dòng "thay đổi" (sơ đồ 2 nhánh Admin):
    hanh_dong = "giu_moi" | "giu_cu" | "giu_ca_2".
    - "giu_moi": ghi đè giá trị cũ bằng giá trị mới (UPDATE tại chỗ).
    - "giu_cu": giữ nguyên, không đổi gì (No-op, trả lại prod_df nguyên vẹn).
    - "giu_ca_2": giữ dòng cũ (gắn valid_to = hôm nay) + THÊM dòng mới (gắn
      valid_from = hôm nay) - CỐ Ý cho phép TRÙNG khóa chính (xem ghi chú
      đầu file về giới hạn của cách làm này với file .csv)."""
    pk_cols = primary_key if isinstance(primary_key, list) else [primary_key]

    if hanh_dong == "giu_cu":
        return prod_df

    if hanh_dong == "giu_moi":
        mask = pd.Series(True, index=prod_df.index)
        for c in pk_cols:
            key_val = thay_doi_item["key"] if len(pk_cols) == 1 else thay_doi_item["key"][pk_cols.index(c)]
            mask &= (prod_df[c] == key_val)
        for col, gt in thay_doi_item["khac_cols"].items():
            prod_df.loc[mask, col] = gt["moi"]
        return prod_df

    if hanh_dong == "giu_ca_2":
        today = date.today().isoformat()
        # Gán None (không phải np.nan) để cột mới có dtype=object ngay từ đầu -
        # nếu dùng np.nan, cột mang dtype float64, ghi chuỗi ngày vào sau đó
        # sẽ lỗi TypeError ở pandas 2.x.
        if "valid_from" not in prod_df.columns:
            prod_df["valid_from"] = None
        if "valid_to" not in prod_df.columns:
            prod_df["valid_to"] = None
        mask = pd.Series(True, index=prod_df.index)
        for c in pk_cols:
            key_val = thay_doi_item["key"] if len(pk_cols) == 1 else thay_doi_item["key"][pk_cols.index(c)]
            mask &= (prod_df[c] == key_val)
        prod_df.loc[mask, "valid_to"] = today
        dong_moi = thay_doi_item["row_moi"].copy()
        dong_moi["valid_from"] = today
        dong_moi["valid_to"] = np.nan
        return pd.concat([prod_df, pd.DataFrame([dong_moi])], ignore_index=True)

    raise ValueError(f"hanh_dong không hợp lệ: {hanh_dong}")


# ============================================================
# StructuredEntityIndex - tra "tên/khóa nào xuất hiện trong 1 đoạn text"
# Dùng cho CẢ 2 hướng đối chiếu chéo (A và B' ở docstring đầu file)
# ============================================================

class StructuredEntityIndex:
    """Build 1 lần cho 1 lượt xử lý (không build lại mỗi đoạn text - đúng lý
    do MultiEntityMatcher trong api8923.py cũng build 1 lần lúc khởi động)."""

    def __init__(self, registry: Optional[dict] = None):
        self.registry = registry or load_registry()
        self._name_to_matches: dict = {}  # normalize_vn(tên) -> [{"table","pk_value","ten"}]
        for table_name, cfg in self.registry.items():
            df = load_table_df(table_name, self.registry)
            if df.empty:
                continue
            pk_col = cfg["primary_key"]
            search_cols = list(dict.fromkeys(cfg.get("name_columns", []) + [pk_col]))
            for col in search_cols:
                if col not in df.columns:
                    continue
                for _, row in df.iterrows():
                    val = row.get(col)
                    if pd.isna(val):
                        continue
                    val_str = re.sub(r"\s+", " ", str(val).strip())
                    if len(val_str) < 4:  # tránh tên/mã quá ngắn gây khớp tràn lan
                        continue
                    key = normalize_vn(val_str)
                    self._name_to_matches.setdefault(key, []).append(
                        {"table": table_name, "pk_value": row.get(pk_col), "ten": val_str}
                    )

    def tim_ung_vien(self, text: str) -> list:
        """Lọc RẺ (substring, không LLM) - trả về các thực thể có tên xuất
        hiện trong text, để CHỈ những đoạn thật sự khả nghi mới cần gọi LLM
        (đúng sticky note sơ đồ 1: "Lọc rẻ bằng regex/NER... chỉ giữ lại
        những đoạn có khả năng chứa thực thể...")."""
        text_norm = normalize_vn(text)
        ket_qua, seen = [], set()
        for key, matches in self._name_to_matches.items():
            if key in text_norm:
                for m in matches:
                    sig = (m["table"], m["pk_value"])
                    if sig not in seen:
                        seen.add(sig)
                        ket_qua.append(m)
        return ket_qua


# ============================================================
# LLM trích xuất (Đối tượng, Thuộc tính, Giá trị) - dùng chung 2 hướng
# ============================================================

EXTRACT_PROMPT = """Bạn là trợ lý trích xuất thông tin có cấu trúc từ văn bản tiếng Việt.
Đoạn văn bản dưới đây có nhắc tới thực thể "{ten_thuc_the}". Hãy đọc đoạn văn bản và trích ra
TẤT CẢ các cặp (thuộc tính, giá trị) CỤ THỂ mà đoạn văn bản khẳng định về CHÍNH thực thể này
(không trích thông tin về người/đối tượng khác dù có nhắc tới).

Trả về CHÍNH XÁC 1 JSON OBJECT (không phải mảng trần) dạng:
{{"items": [{{"thuoc_tinh": string, "gia_tri": string, "trich_dan": string}}, ...]}}

- "items": mảng các cặp trích được - CÓ THỂ RỖNG: {{"items": []}}
- "thuoc_tinh": tên thuộc tính bằng tiếng Việt tự nhiên (vd "chức vụ", "điểm chuẩn", "sức chứa")
- "gia_tri": giá trị cụ thể (giữ nguyên dạng số/chữ như trong văn bản)
- "trich_dan": câu/cụm từ gốc trong văn bản chứa giá trị này (dưới 20 từ)

CHỈ trích khi có giá trị CỤ THỂ, RÕ RÀNG. Nếu đoạn văn bản không nêu giá trị cụ thể nào về
thực thể này, trả về {{"items": []}}. Chỉ trả JSON, không thêm giải thích.

Đoạn văn bản:
\"\"\"{van_ban}\"\"\""""


def _map_thuoc_tinh_to_column(thuoc_tinh_text: str, cfg: dict) -> Optional[str]:
    """Map thuộc tính tự nhiên (LLM trích, tiếng Việt tự do) về đúng tên cột
    trong DataFrame, dựa vào column_labels đã khai trong registry.json."""
    labels = cfg.get("column_labels", {})
    tt_norm = normalize_vn(thuoc_tinh_text)
    best_col, best_len = None, 0
    for col, label in labels.items():
        label_norm = normalize_vn(label)
        if label_norm == tt_norm or label_norm in tt_norm or tt_norm in label_norm:
            if len(label_norm) > best_len:
                best_col, best_len = col, len(label_norm)
    return best_col


def _extract_json_array(raw_text: str) -> list:
    """Parse kết quả LLM trích xuất thành list.

    Chấp nhận cả 2 dạng: mảng trần HOẶC object bọc {"items": [...]}. Lý do:
    client Ollama cấu hình format="json" yêu cầu top-level phải là object,
    còn một số backend khác (OpenAI) có thể trả mảng trần - hàm này xử lý
    cả 2 để không phụ thuộc vào 1 backend cụ thể."""
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw_text.strip())
    try:
        data = json.loads(cleaned)
    except (json.JSONDecodeError, TypeError):
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("items", "ket_qua", "result", "results", "data", "extracted"):
            if isinstance(data.get(key), list):
                return data[key]
    return []


def _goi_llm_trich_xuat(ten_thuc_the: str, van_ban: str) -> list:
    """Gọi LLM trích (thuộc tính, giá trị). Dùng LẠI client LLM đã cấu hình
    trong conflict_detection.py (KHÔNG tự tạo client/config riêng ở đây -
    tránh lặp lại đúng lỗi đã gặp: 2 nơi cấu hình model/server lệch nhau)."""
    import conflict_detection as cd  # import trong hàm - tránh phụ thuộc vòng lúc module load
    prompt = EXTRACT_PROMPT.format(ten_thuc_the=ten_thuc_the, van_ban=van_ban[:2000])
    try:
        raw = cd.call_llm_with_retry(_call_llm_raw_for_extract, prompt)
        return _extract_json_array(raw)
    except Exception as e:
        print(f"    [!] Lỗi khi LLM trích xuất đối chiếu cấu trúc: {e}")
        return []


def _call_llm_raw_for_extract(prompt: str) -> str:
    import conflict_detection as cd
    llm_client = cd.get_conflict_llm_client()
    if cd.CONFLICT_LLM_BACKEND == "openai":
        response = llm_client.chat.completions.create(
            model=cd.CONFLICT_LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0, response_format={"type": "json_object"},
        )
        return response.choices[0].message.content
    response = llm_client.invoke([{"role": "user", "content": prompt}])
    return response.content


# ============================================================
# Hướng (A): RAG -> bảng cấu trúc (dùng bởi conflict_detection.py)
# ============================================================

def doi_chieu_chunks_voi_bang(chunks: list, entity_index: Optional[StructuredEntityIndex] = None,
                               registry: Optional[dict] = None) -> list:
    """Với mỗi chunk (dict có 'content' + 'metadata'), lọc rẻ tìm thực thể
    khả nghi, LLM trích thuộc tính/giá trị, so với bảng cấu trúc production.
    KHÔNG hỏi Admin ở đây (chỉ TRẢ VỀ kết quả) - caller (conflict_detection.py)
    quyết định cách hỏi, để giữ UX nhất quán với các bước hỏi khác của nó.

    Trả về list[dict]: {"chunk_id","table","pk_value","ten_thuc_the","cot",
    "gia_tri_van_ban","gia_tri_bang","trich_dan"} - MỖI phần tử là 1 khả
    năng xung đột chéo cần Admin xem."""
    registry = registry or load_registry()
    entity_index = entity_index or StructuredEntityIndex(registry)
    ket_qua = []

    for chunk in chunks:
        content = chunk.get("content", "")
        chunk_id = chunk.get("metadata", {}).get("chunk_id")
        ung_vien = entity_index.tim_ung_vien(content)
        if not ung_vien:
            continue  # không có thực thể nào khả nghi -> KHÔNG gọi LLM

        for uv in ung_vien:
            cfg = registry[uv["table"]]
            if not cfg.get("has_related_text_corpus", True):
                continue  # bảng khai rõ không liên quan gì tới văn bản RAG (vd bảng điểm chuẩn)
            trich = _goi_llm_trich_xuat(uv["ten"], content)
            df = load_table_df(uv["table"], registry)
            pk_col = cfg["primary_key"]
            row_match = df[df[pk_col] == uv["pk_value"]]
            if row_match.empty:
                continue
            row = row_match.iloc[0]

            for item in trich:
                col = _map_thuoc_tinh_to_column(item.get("thuoc_tinh", ""), cfg)
                if not col or col not in row.index:
                    continue  # không map được về cột nào đã biết -> bỏ qua, không đoán
                gia_tri_bang = row.get(col)
                if pd.isna(gia_tri_bang):
                    continue
                if not _gia_tri_khac_nhau(item.get("gia_tri"), gia_tri_bang):
                    continue  # khớp nhau -> không phải xung đột
                ket_qua.append({
                    "chunk_id": chunk_id, "table": uv["table"], "pk_value": uv["pk_value"],
                    "ten_thuc_the": uv["ten"], "cot": col,
                    "gia_tri_van_ban": item.get("gia_tri"), "gia_tri_bang": gia_tri_bang,
                    "trich_dan": item.get("trich_dan", ""),
                })
    return ket_qua


# ============================================================
# Hướng (B'): bảng cấu trúc -> RAG (dùng bởi structured_data_pipeline.py)
# ============================================================

def doi_chieu_dong_bang_voi_rag(rows_can_kiem_tra: list, table_name: str, registry: Optional[dict] = None) -> list:
    """Ngược lại hướng (A): với các dòng MỚI/THAY ĐỔI trong 1 bảng cấu trúc,
    tìm chunk RAG active đang nhắc cùng thực thể, hỏi LLM xem có giá trị nào
    trong đoạn đó KHÁC với giá trị MỚI trong bảng không. CHỈ GHI NHẬN để
    hiển thị cho Admin (đúng sticky note sơ đồ 2: "chỉ ghi nhận để hiển thị,
    KHÔNG tự động xử lý gì")."""
    import conflict_detection as cd  # import trong hàm - tránh phụ thuộc vòng

    registry = registry or load_registry()
    cfg = registry.get(table_name, {})
    if not cfg.get("has_related_text_corpus", False):
        return []

    manifest = cd.load_manifest()
    active_chunks = cd.load_active_production_chunks(manifest)
    if not active_chunks:
        return []

    canh_bao = []
    for row_info in rows_can_kiem_tra:
        name_cols = cfg.get("name_columns", [])
        if isinstance(row_info, dict) and "row_moi" in row_info:
            row = row_info["row_moi"]  # entry từ nhom["thay_doi"] - đã là pd.Series
        elif isinstance(row_info, dict):
            row = pd.Series(row_info)  # entry từ nhom["du_lieu_moi"].to_dict("records")
            # là dict THUẦN, không có .index như pd.Series - nếu không bọc lại,
            # dòng "if col in row.index" dưới đây sẽ ném AttributeError.
        else:
            row = row_info
        ten_thuc_the = None
        for col in name_cols:
            if col in row.index and pd.notna(row.get(col)):
                ten_thuc_the = str(row[col]).strip()
                break
        if not ten_thuc_the or len(ten_thuc_the) < 4:
            continue

        pk_col = cfg.get("primary_key")
        khoa_dong = row.get(pk_col) if isinstance(pk_col, str) else tuple(row.get(c) for c in pk_col)

        ten_norm = normalize_vn(ten_thuc_the)
        chunk_lien_quan = [c for c in active_chunks if ten_norm in normalize_vn(c.get("content", ""))]
        if not chunk_lien_quan:
            continue

        for chunk in chunk_lien_quan[:3]:  # giới hạn số chunk đối chiếu mỗi thực thể, tránh gọi LLM tràn lan
            trich = _goi_llm_trich_xuat(ten_thuc_the, chunk.get("content", ""))
            for item in trich:
                col = _map_thuoc_tinh_to_column(item.get("thuoc_tinh", ""), cfg)
                if not col:
                    continue
                gia_tri_bang_moi = row.get(col)
                if gia_tri_bang_moi is None or pd.isna(gia_tri_bang_moi):
                    continue
                if not _gia_tri_khac_nhau(item.get("gia_tri"), gia_tri_bang_moi):
                    continue
                canh_bao.append({
                    "table": table_name, "ten_thuc_the": ten_thuc_the, "cot": col, "khoa_dong": khoa_dong,
                    "gia_tri_bang_moi": gia_tri_bang_moi, "gia_tri_van_ban": item.get("gia_tri"),
                    "nguon_van_ban": chunk.get("metadata", {}).get("source_file"),
                    "chunk_id": chunk.get("metadata", {}).get("chunk_id"),  # cần để sửa/loại đúng đoạn
                    "noi_dung_day_du": chunk.get("content", ""),
                    "trich_dan": item.get("trich_dan", ""),
                })
    return canh_bao
