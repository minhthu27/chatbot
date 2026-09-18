"""
CLI cho "Quy trình xử lý dữ liệu có cấu trúc" (sơ đồ 3) - upload file .csv/
.xlsx/.xls -> tiền xử lý -> chọn khóa chính -> lưu staging -> gọi
structured_conflict_detection.py để phát hiện xung đột với production (sơ đồ
2) -> Admin xử lý -> ghi vào "production" -> báo api8923.py nạp lại.

    python structured_data_pipeline.py add <duong_dan> <ten_bang> [--key-cols A,B] [--auto safe|replace]
        Upload 1 file .csv/.xlsx/.xls cho bảng <ten_bang>. Nếu bảng ĐÃ có
        trong registry.json, dùng đúng primary_key đã khai (bỏ --key-cols
        nếu không cần khóa GHÉP tạm thời cho lần so khớp này). Nếu bảng
        CHƯA có trong registry.json (bảng mới) - --key-cols là BẮT BUỘC
        (tương đương bước "Chọn khóa chính" trong sơ đồ), và sau khi Admin
        duyệt xong, registry.json sẽ được thêm 1 entry cơ bản cho bảng này
        (path/primary_key) - CẦN bổ sung tay display_columns/column_labels/
        role_columns/group_columns nếu muốn dùng cho matching nâng cao.
        --auto safe|replace: chạy KHÔNG tương tác (dùng khi gọi từ web) -
        quyết định thẳng cho mỗi dòng "thay đổi" (safe=giữ bản cũ,
        replace=lấy bản mới) và tự xác nhận ghi production ở bước cuối,
        thay vì chờ input() (không dùng cờ này thì hành vi y hệt trước đây,
        hỏi tương tác đầy đủ trên terminal).

    python structured_data_pipeline.py status
        Liệt kê các file còn nằm trong hàng đợi (tables_staging/) - nghĩa là
        đã upload nhưng CHƯA xử lý xong (vd bị Ctrl+C giữa đường).

    python structured_data_pipeline.py discard-staging <ten_file>
        Xóa 1 file khỏi hàng đợi (bỏ qua, không xử lý).

    python structured_data_pipeline.py list-tables
        Liệt kê các bảng đã đăng ký trong registry.json.

    python structured_data_pipeline.py remove-table <ten_bang>
        Xóa hẳn 1 bảng SAI/TEST khỏi registry.json (vd bảng tạo nhầm do gõ
        sai tên - xem cảnh báo tự động ở 'add'). File .csv được backup
        (.bak_*_before_remove) trước khi gỡ khỏi registry, không mất dữ liệu.

    python structured_data_pipeline.py list-backups <ten_bang>
        Liệt kê các bản backup .bak_* của 1 bảng (tự tạo mỗi lần 'add' ghi
        đè production) - tìm bản CŨ NHẤT để khôi phục lại trạng thái trước
        khi test.

    python structured_data_pipeline.py restore-backup <ten_bang> <ten_file_backup>
        Khôi phục bảng từ 1 bản backup cụ thể (lấy tên từ 'list-backups').

    python structured_data_pipeline.py analyze-web <duong_dan> <ten_bang> [--key-cols=A,B] --out <path>
        Dùng bởi admin_api.py: PHÂN TÍCH KHÔNG TƯƠNG TÁC, KHÔNG ghi gì vào
        production - trả JSON đầy đủ (dữ liệu mới/trùng lặp/thay đổi từng
        dòng + cảnh báo chéo với RAG) để Admin duyệt trên web UI, ĐÚNG
        pattern 'conflict_detection.py analyze-web' cho văn bản phi cấu
        trúc. Ghi tạm vào tables_staging/ (staging_token trong kết quả) để
        'apply-web' đọc lại, không phải upload file lần 2.

    python structured_data_pipeline.py apply-web <ten_bang> --decisions <path> --out <path>
        Dùng bởi admin_api.py: ÁP DỤNG quyết định Admin đã chọn (file JSON
        do web UI sinh ra từ kết quả 'analyze-web') - ĐÂY mới là lúc ghi
        thật vào production.

GHI CHÚ TÍCH HỢP HỆ THỐNG THẬT: xem docstring đầu file
structured_conflict_detection.py - "production" ở đây là file .csv, chưa
phải database thật.
"""

import sys
import json
import shutil
from pathlib import Path
from datetime import datetime, date

import pandas as pd
import numpy as np

import structured_conflict_detection as scd
import conflict_detection as cd  # dùng lại _ask_yes_no, call_llm_with_retry, _notify_api8923_reload


def _doc_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in (".xlsx", ".xls"):
        return pd.read_excel(path)
    raise ValueError(f"Không hỗ trợ định dạng '{suffix}' - chỉ nhận .csv/.xlsx/.xls")


def _in_preview(df: pd.DataFrame, n: int = 5):
    with pd.option_context("display.max_columns", None, "display.width", 160):
        print(df.head(n).to_string(index=False))


def _to_jsonable(v):
    """Ép 1 giá trị pandas/numpy về kiểu JSON serialize được - int64/float64/
    Timestamp/NaN của pandas không tự json.dumps() được."""
    if v is None:
        return None
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        return None if np.isnan(v) else float(v)
    if isinstance(v, np.bool_):
        return bool(v)
    if isinstance(v, (pd.Timestamp, datetime, date)):
        return str(v)
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return v


def _row_to_jsonable(row) -> dict:
    return {str(k): _to_jsonable(v) for k, v in dict(row).items()}


def _key_to_jsonable(key):
    if isinstance(key, tuple):
        return [_to_jsonable(k) for k in key]
    return _to_jsonable(key)


def phan_tich_upload_web(path_str: str, table_name: str, key_cols_override: list = None,
                          force_new: bool = False) -> dict:
    """Bản KHÔNG tương tác + KHÔNG ghi gì của cmd_add() - CHỈ phân tích và
    trả về TOÀN BỘ kết quả (dữ liệu mới/trùng lặp/thay đổi + cảnh báo chéo
    với RAG) dưới dạng JSON để Admin duyệt trên web UI TRƯỚC khi ghi -
    ĐÚNG pattern analyze_conflicts() bên conflict_detection.py cho văn bản
    phi cấu trúc (thay vì tự quyết định hàng loạt theo mode An toàn/Ghi đè
    như bản cũ, không cho Admin xem trước từng xung đột).

    [QUAN TRỌNG] KHÔNG phụ thuộc vào việc Admin gõ ĐÚNG tên bảng: nếu tên
    gõ vào CHƯA có trong registry nhưng cột khớp cao với 1 bảng ĐÃ CÓ, hàm
    này TỰ ĐỘNG so sánh với bảng đó luôn (thay vì chỉ cảnh báo suông rồi so
    với 0 dòng production như bản trước) - giống bên phi cấu trúc: hệ
    thống tự phát hiện, Admin chỉ cần xác nhận đúng/sai. Muốn ép tạo bảng
    MỚI riêng dù trùng cột (force_new=True) thì bỏ qua việc tự ghép này."""
    path = Path(path_str)
    if not path.exists():
        return {"error": f"Không tìm thấy file: {path}"}

    registry = scd.load_registry()
    ten_da_go = table_name
    ten_khop_tu_dong = None  # set nếu tự ghép sang 1 bảng khác tên đã gõ

    print(f"→ Đọc '{path.name}'...")
    df = _doc_file(path)
    print(f"  {len(df)} dòng, {len(df.columns)} cột: {list(df.columns)}")
    columns_goc = [str(c) for c in df.columns]

    print("→ Tiền xử lý (strip khoảng trắng, chuẩn hóa Unicode NFC, số thập phân VN, placeholder rỗng)...")
    df = scd.tien_xu_ly_dataframe(df)

    exists = table_name in registry
    if not exists and not force_new:
        tk = scd.tim_bang_trung_khop_cot(list(df.columns), registry)
        if tk:
            # Cột khớp cao với 1 bảng ĐÃ CÓ nhưng tên gõ vào lại chưa tồn tại
            # -> 99% là Admin đang cập nhật bảng đó (gõ nhầm/gõ tên hiển thị)
            # nên TỰ CHUYỂN sang so sánh với bảng đó luôn, KHÔNG chờ Admin
            # gõ lại đúng tên rồi phân tích lại lần 2.
            ten_khop_tu_dong = {"ten": tk[0], "ty_le": round(tk[1], 2), "ten_da_go": ten_da_go}
            table_name = tk[0]
            exists = True
            print(f"⚠️  Cột khớp {tk[1]:.0%} với bảng đã có '{tk[0]}' (bạn gõ '{ten_da_go}') "
                  f"- TỰ ĐỘNG so sánh với '{tk[0]}' để không bỏ sót xung đột. Nếu đây thực sự "
                  f"là 2 loại dữ liệu khác nhau, Admin có thể ép tạo bảng mới riêng trên UI.")

    bang_moi = not exists
    if bang_moi and not key_cols_override:
        return {"error": f"Bảng '{table_name}' CHƯA có trong registry.json - bảng MỚI cần "
                          f"chỉ định khóa chính (key_cols)."}

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    staging_token = f"{table_name}__{ts}.csv"
    staging_path = scd.TABLES_STAGING_FOLDER / staging_token
    df.to_csv(staging_path, index=False)
    print(f"→ Đã ghi tạm vào hàng đợi: {staging_token}")

    key_cols = key_cols_override or (
        registry[table_name]["primary_key"] if not bang_moi else None)
    if isinstance(key_cols, str):
        key_cols = [key_cols]
    missing = [c for c in key_cols if c not in df.columns]
    if missing:
        return {"error": f"Cột khóa {missing} không tồn tại trong file. Cột hiện có: {list(df.columns)}"}
    key_cols_effective = key_cols if len(key_cols) > 1 else key_cols[0]

    prod_df = pd.DataFrame() if bang_moi else scd.load_table_df(table_name, registry)
    canh_bao_khoa = scd.kiem_tra_khoa_du_phan_biet(df, prod_df, key_cols)
    for cb in canh_bao_khoa:
        print(f"⚠️  {cb}")

    print(f"→ So khớp theo khóa {key_cols} với {len(prod_df)} dòng production hiện có...")
    nhom = scd.phan_nhom_theo_khoa(df, prod_df, key_cols_effective)
    so_moi, so_trung, so_doi = len(nhom["du_lieu_moi"]), len(nhom["trung_lap"]), len(nhom["thay_doi"])
    print(f"=== Kết quả phân nhóm: {so_moi} dữ liệu mới | {so_trung} trùng lặp (bỏ qua) | "
          f"{so_doi} thay đổi ===")

    thay_doi_out = []
    for i, item in enumerate(nhom["thay_doi"]):
        thay_doi_out.append({
            "row_id": i,
            "key": _key_to_jsonable(item["key"]),
            "khac_cols": {c: {"cu": _to_jsonable(gt["cu"]), "moi": _to_jsonable(gt["moi"])}
                          for c, gt in item["khac_cols"].items()},
            "row_moi": _row_to_jsonable(item["row_moi"]),
        })

    rows_can_kiem_tra = list(nhom["du_lieu_moi"].to_dict("records")) if so_moi else []
    rows_can_kiem_tra += nhom["thay_doi"]
    canh_bao_cheo_out = []
    if rows_can_kiem_tra and not bang_moi:
        # Với bảng ĐÃ CÓ, registry đã khai has_related_text_corpus/name_columns
        # sẵn (nếu có) nên đối chiếu chéo được ngay ở đây.
        print(f"→ Đối chiếu chéo với dữ liệu RAG cho {len(rows_can_kiem_tra)} dòng mới/thay đổi "
              f"(gọi LLM - có thể mất vài phút)...")
        canh_bao_cheo_raw = scd.doi_chieu_dong_bang_voi_rag(rows_can_kiem_tra, table_name, registry)
        for i, cb in enumerate(canh_bao_cheo_raw):
            cb2 = {k: _to_jsonable(v) for k, v in cb.items() if k != "khoa_dong"}
            cb2["warning_id"] = i
            cb2["khoa_dong"] = _key_to_jsonable(cb["khoa_dong"])
            canh_bao_cheo_out.append(cb2)
        print(f"→ {len(canh_bao_cheo_out)} cảnh báo chéo với RAG." if canh_bao_cheo_out
              else "→ Không có cảnh báo chéo với RAG.")
    elif bang_moi:
        # Bảng THẬT SỰ mới - chưa có has_related_text_corpus/name_columns
        # (đó chính là điều form khai báo metadata trên UI sẽ hỏi Admin) nên
        # KHÔNG đối chiếu chéo được ở đây; UI sẽ gọi recheck-cross-web riêng
        # SAU KHI Admin khai báo xong cột "tên đối tượng" + xác nhận có liên
        # quan RAG hay không, dùng đúng staging_token này.
        print("→ Bảng MỚI hoàn toàn - chưa khai báo cột 'tên đối tượng'/liên quan văn bản nên "
              "chưa đối chiếu chéo với RAG được ở bước này (khai báo xong ở form bên dưới rồi "
              "bấm 'Kiểm tra chéo với RAG' để làm bước này).")
    else:
        print("→ Không có dòng mới/thay đổi nào cần đối chiếu chéo với RAG.")

    print(f"✅ Phân tích xong bảng '{table_name}'.")
    gia_tri_duy_nhat = {}
    for col in df.columns:
        try:
            vals = sorted(str(v) for v in df[col].dropna().unique())
        except Exception:
            vals = [str(v) for v in df[col].dropna().unique()]
        if 0 < len(vals) <= 30:
            gia_tri_duy_nhat[col] = vals
    return {
        "ok": True, "table_name": table_name, "ten_da_go": ten_da_go, "bang_moi": bang_moi,
        "ten_khop_tu_dong": ten_khop_tu_dong,
        "key_cols": key_cols, "staging_token": staging_token,
        "columns_goc": columns_goc, "columns": list(df.columns), "n_rows": len(df),
        "gia_tri_duy_nhat": gia_tri_duy_nhat,
        "canh_bao_khoa": canh_bao_khoa,
        "so_moi": so_moi,
        "mau_du_lieu_moi": [_row_to_jsonable(r) for _, r in nhom["du_lieu_moi"].head(20).iterrows()],
        "so_trung_lap": so_trung,
        "thay_doi": thay_doi_out,
        "canh_bao_cheo": canh_bao_cheo_out,
    }


def phan_tich_cheo_rag_moi_web(table_name: str, staging_token: str, name_columns: list,
                                role_columns: list = None, group_columns: list = None) -> dict:
    """Đối chiếu chéo với RAG cho 1 bảng THẬT SỰ MỚI, dùng NGAY thông tin
    Admin vừa khai ở form metadata (cột 'tên đối tượng') - KHÔNG cần chờ
    tới lần cập nhật thứ 2 mới bắt được mâu thuẫn với văn bản (registry
    thật chưa được ghi gì cả ở bước này, chỉ dùng 1 cfg TẠM để đối chiếu)."""
    staging_path = scd.TABLES_STAGING_FOLDER / staging_token
    if not staging_path.exists():
        return {"error": f"Không tìm thấy file hàng đợi '{staging_token}'."}
    if not name_columns:
        return {"error": "Cần chọn ít nhất 1 cột 'tên đối tượng' để đối chiếu chéo."}
    df = pd.read_csv(staging_path)
    cfg_tam = {
        "has_related_text_corpus": True, "name_columns": name_columns,
        "role_columns": role_columns or [], "group_columns": group_columns or [],
        "primary_key": name_columns[0],  # chỉ dùng để ghi nhận khoa_dong trong cảnh báo, không ảnh hưởng gì khác
    }
    rows = df.to_dict("records")
    print(f"→ Đối chiếu chéo với RAG cho {len(rows)} dòng (bảng mới, dùng cột tên: {name_columns})...")
    canh_bao_raw = scd.doi_chieu_dong_bang_voi_rag(rows, table_name, {table_name: cfg_tam})
    canh_bao_out = []
    for i, cb in enumerate(canh_bao_raw):
        cb2 = {k: _to_jsonable(v) for k, v in cb.items() if k != "khoa_dong"}
        cb2["warning_id"] = i
        cb2["khoa_dong"] = _key_to_jsonable(cb["khoa_dong"])
        canh_bao_out.append(cb2)
    print(f"→ {len(canh_bao_out)} cảnh báo chéo với RAG." if canh_bao_out else "→ Không có cảnh báo chéo với RAG.")
    return {"ok": True, "canh_bao_cheo": canh_bao_out}


def ap_dung_quyet_dinh_upload_web(table_name: str, decisions: dict) -> dict:
    """Áp dụng quyết định Admin chọn trên web UI cho 1 lượt phân tích upload
    bảng cấu trúc (xem phan_tich_upload_web) - ĐÂY mới là lúc THẬT SỰ ghi
    ra production, ĐÚNG pattern apply_conflict_decisions() bên
    conflict_detection.py cho văn bản phi cấu trúc.

    decisions = {
      "staging_token": "...", "key_cols": [...],
      "thay_doi_decisions": [{"row_id","key","khac_cols","row_moi",
                              "action": "giu_moi|giu_cu|giu_ca_2"}, ...],
      "canh_bao_cheo_decisions": [{...các field cảnh báo gốc từ analyze...,
                              "action": "giu_rag|loai_rag|sua_rag|sua_bang",
                              "noi_dung_moi": "..." (nếu sua_rag)}, ...],
    }"""
    staging_token = decisions.get("staging_token")
    key_cols = decisions.get("key_cols")
    if not staging_token or not key_cols:
        return {"error": "Thiếu staging_token hoặc key_cols trong decisions"}
    if isinstance(key_cols, str):
        key_cols = [key_cols]
    staging_path = scd.TABLES_STAGING_FOLDER / staging_token
    if not staging_path.exists():
        return {"error": f"Không tìm thấy file hàng đợi '{staging_token}' - có thể đã được "
                          f"áp dụng trước đó rồi, hoặc bị discard-staging."}

    registry = scd.load_registry()
    bang_moi = table_name not in registry
    df = pd.read_csv(staging_path)  # đã tiền xử lý sẵn từ lúc analyze-web
    key_cols_effective = key_cols if len(key_cols) > 1 else key_cols[0]
    prod_df = pd.DataFrame() if bang_moi else scd.load_table_df(table_name, registry)

    print(f"→ Tính lại phân nhóm theo khóa {key_cols} (đối chiếu lại với production HIỆN "
          f"TẠI, phòng có thay đổi từ lúc phân tích tới giờ)...")
    nhom = scd.phan_nhom_theo_khoa(df, prod_df, key_cols_effective)
    final_df = prod_df.copy()

    # ---------- Dữ liệu mới - tự động thêm (giữ đúng thiết kế cũ: không
    # bắt duyệt từng dòng vì không scale, chỉ log) ----------
    so_moi = len(nhom["du_lieu_moi"])
    if so_moi:
        final_df = pd.concat([final_df, nhom["du_lieu_moi"]], ignore_index=True)
        for _, row in nhom["du_lieu_moi"].iterrows():
            scd.log_structured_decision({
                "su_kien": "tu_dong_them_du_lieu_moi", "table": table_name,
                "key": row[key_cols[0]] if len(key_cols) == 1 else [row[c] for c in key_cols],
            })
        print(f"→ {so_moi} dòng dữ liệu mới sẽ được thêm.")

    # ---------- Thay đổi - theo quyết định Admin, khớp theo "key" (KHÔNG
    # theo row_id, vì thay_doi tính lại có thể khác thứ tự/số lượng lúc
    # phân tích nếu production vừa bị đổi bởi 1 admin khác) ----------
    thay_doi_decs = decisions.get("thay_doi_decisions", [])
    dec_by_key = {json.dumps(d["key"], ensure_ascii=False): d for d in thay_doi_decs}
    so_ap_dung, so_khong_khop = 0, 0
    if nhom["thay_doi"]:
        print(f"→ Áp dụng quyết định cho {len(nhom['thay_doi'])} dòng thay đổi...")
    for item in nhom["thay_doi"]:
        key_j = json.dumps(_key_to_jsonable(item["key"]), ensure_ascii=False)
        dec = dec_by_key.get(key_j)
        if dec is None:
            hanh_dong = "giu_cu"
            print(f"   ⚠️  Khóa {item['key']} không khớp quyết định nào Admin đã gửi (production "
                  f"có thể vừa đổi) - mặc định AN TOÀN giữ bản cũ. NÊN phân tích lại bảng này.")
            so_khong_khop += 1
        else:
            hanh_dong = dec.get("action") if dec.get("action") in ("giu_moi", "giu_cu", "giu_ca_2") else "giu_cu"
        final_df = scd.ap_dung_quyet_dinh_thay_doi(final_df, item, key_cols_effective, hanh_dong)
        scd.log_structured_decision({
            "su_kien": "quyet_dinh_thay_doi_web", "table": table_name, "key": item["key"],
            "khac_cols": {c: {"cu": str(v["cu"]), "moi": str(v["moi"])} for c, v in item["khac_cols"].items()},
            "hanh_dong": hanh_dong,
        })
        so_ap_dung += 1

    # ---------- Cảnh báo chéo với RAG - theo quyết định Admin (mỗi quyết
    # định đã TỰ MANG ĐỦ dữ liệu cần thiết từ lúc analyze, không cần tính
    # lại/gọi LLM lại) ----------
    cheo_decs = decisions.get("canh_bao_cheo_decisions", [])
    so_cheo_ap_dung = 0
    if cheo_decs:
        print(f"→ Áp dụng quyết định cho {len(cheo_decs)} cảnh báo chéo với RAG...")
    for cb in cheo_decs:
        action = cb.get("action", "giu_rag")
        if action == "loai_rag":
            cd.set_chunk_status(cb["nguon_van_ban"], {cb["chunk_id"]}, "bi_bo_qua")
        elif action == "sua_rag":
            noi_dung_moi = (cb.get("noi_dung_moi") or "").strip()
            if noi_dung_moi:
                cd.set_chunk_content(cb["nguon_van_ban"], cb["chunk_id"], noi_dung_moi)
            else:
                action = "giu_rag"
        elif action == "sua_bang":
            pk_cols_list = key_cols
            khoa = cb.get("khoa_dong")
            khoa_tuple = tuple(khoa) if isinstance(khoa, list) else (khoa,)
            mask = pd.Series(True, index=final_df.index)
            for c, v in zip(pk_cols_list, khoa_tuple):
                mask &= (final_df[c] == v)
            final_df.loc[mask, cb["cot"]] = cb.get("gia_tri_van_ban")
        scd.log_structured_decision({
            "su_kien": "canh_bao_cheo_bang_vs_rag_web", "table": table_name, "hanh_dong": action,
            **{k: v for k, v in cb.items() if k != "noi_dung_day_du"},
        })
        so_cheo_ap_dung += 1

    # ---------- Ghi ra production ----------
    if bang_moi:
        meta = decisions.get("metadata") or {}
        name_columns = [c for c in (meta.get("name_columns") or []) if c in final_df.columns]
        pii_columns = [c for c in (meta.get("pii_columns") or []) if c in final_df.columns]
        hidden_columns = [c for c in (meta.get("hidden_columns") or []) if c in final_df.columns]
        display_columns = [c for c in (meta.get("display_columns") or list(final_df.columns))
                            if c in final_df.columns and c not in pii_columns and c not in hidden_columns]
        disambiguating_columns = [c for c in (meta.get("disambiguating_columns") or []) if c in final_df.columns]
        categorical_filters = {col: kw_map for col, kw_map in (meta.get("categorical_filters") or {}).items()
                                if col in final_df.columns and isinstance(kw_map, dict) and kw_map}
        registry[table_name] = {
            "path": f"data/processed/tables/{table_name}.csv",
            "primary_key": key_cols[0] if len(key_cols) == 1 else key_cols,
            "name_columns": name_columns,
            "role_columns": [c for c in (meta.get("role_columns") or []) if c in final_df.columns],
            "group_columns": [c for c in (meta.get("group_columns") or []) if c in final_df.columns],
            "pii_columns": pii_columns, "hidden_columns": hidden_columns,
            "display_columns": display_columns,
            "column_labels": {k: v for k, v in (meta.get("column_labels") or {}).items()
                               if k in final_df.columns and v},
            "disambiguating_columns": disambiguating_columns,
            "categorical_filters": categorical_filters,
            "has_related_text_corpus": bool(meta.get("has_related_text_corpus")),
            "description": (meta.get("description") or "").strip()
                            or f"(TODO: Admin bổ sung mô tả cho bảng '{table_name}')",
            "extra_instructions": (meta.get("extra_instructions") or "").strip(),
        }
        scd.save_registry(registry)
        if not name_columns:
            print(f"⚠️  Bảng '{table_name}' chưa khai 'cột tên đối tượng' (name_columns) - chatbot sẽ "
                  f"KHÔNG tra được dữ liệu bảng này theo tên người/thực thể, chỉ tra được qua agent "
                  f"phân tích chung. Vào Tables → sửa registry để bổ sung nếu cần.")
        if not registry[table_name]["description"] or registry[table_name]["description"].startswith("(TODO"):
            print(f"⚠️  Bảng '{table_name}' chưa có mô tả - bộ định tuyến câu hỏi (router) của chatbot "
                  f"dùng đúng mô tả này để quyết định có tra bảng này không, để trống sẽ khiến chatbot "
                  f"khó/không tự tìm ra bảng này khi trả lời.")

    scd.save_table_df(table_name, final_df, registry)
    staging_path.unlink(missing_ok=True)
    print(f"✅ Đã ghi {len(final_df)} dòng vào production: {registry[table_name]['path']}")
    if bang_moi:
        print(f"✅ Đã thêm bảng '{table_name}' vào registry.json với metadata Admin đã khai báo.")
    print("→ Báo api8923.py nạp lại registry/bảng...")
    cd._notify_api8923_reload()

    return {
        "ok": True, "table_name": table_name, "path": registry[table_name]["path"],
        "n_rows_final": len(final_df), "bang_moi": bang_moi,
        "so_dong_moi": so_moi, "so_thay_doi_ap_dung": so_ap_dung,
        "so_thay_doi_khong_khop": so_khong_khop, "so_canh_bao_cheo_ap_dung": so_cheo_ap_dung,
    }


def cmd_add(path_str: str, table_name: str, key_cols_override: list = None, auto: str = None):
    path = Path(path_str)
    if not path.exists():
        print(f"❌ Không tìm thấy file: {path}")
        sys.exit(1)

    registry = scd.load_registry()
    bang_moi = table_name not in registry

    if bang_moi and not key_cols_override:
        print(f"❌ Bảng '{table_name}' CHƯA có trong registry.json - đây là bảng MỚI, "
              f"cần chỉ định khóa chính: --key-cols <ten_cot> (hoặc nhiều cột, phân tách bởi dấu phẩy).")
        sys.exit(1)

    print(f"→ Đọc '{path.name}'...")
    df = _doc_file(path)
    print(f"  {len(df)} dòng, {len(df.columns)} cột: {list(df.columns)}")

    # Trước khi coi đây là BẢNG MỚI, kiểm tra xem cột có trùng khớp cao với
    # 1 bảng ĐÃ CÓ không - tránh lặp lại lỗi gõ nhầm tên bảng (vd 'gv' thay
    # vì 'giangvien') tạo ra bảng trùng lặp dữ liệu. Với vài trăm bảng,
    # không thể trông chờ Admin nhớ đúng tên - hệ thống phải tự phát hiện.
    if bang_moi:
        trung_khop = scd.tim_bang_trung_khop_cot(list(df.columns), registry)
        if trung_khop:
            ten_giong, ty_le = trung_khop
            print(f"⚠️  Bảng '{table_name}' chưa có trong registry, nhưng cột của file này khớp "
                  f"{ty_le:.0%} với bảng ĐÃ CÓ '{ten_giong}'. Rất có thể đây là CẬP NHẬT cho "
                  f"'{ten_giong}', không phải bảng mới (gõ nhầm tên?).")
            if not cd._ask_yes_no(f"Vẫn tạo bảng MỚI '{table_name}' (KHÔNG cập nhật '{ten_giong}')?"):
                print(f"→ Đã hủy. Chạy lại đúng: python structured_data_pipeline.py add {path_str} {ten_giong}")
                sys.exit(0)

    print("→ Tiền xử lý (strip khoảng trắng, chuẩn hóa Unicode NFC, số thập phân VN, placeholder rỗng)...")
    df = scd.tien_xu_ly_dataframe(df)

    # Lưu staging - giữ lại để 'status' xem được hàng đợi nếu quá trình bị
    # gián đoạn giữa lúc chạy (đúng bước "Cập nhật trạng thái hàng đợi" sơ đồ 3).
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    staging_path = scd.TABLES_STAGING_FOLDER / f"{table_name}__{ts}.csv"
    df.to_csv(staging_path, index=False)
    print(f"→ Đã ghi tạm vào hàng đợi: {staging_path.name}")

    key_cols = key_cols_override or (
        registry[table_name]["primary_key"] if not bang_moi else None
    )
    if isinstance(key_cols, str):
        key_cols = [key_cols]
    missing = [c for c in key_cols if c not in df.columns]
    if missing:
        print(f"❌ Cột khóa {missing} không tồn tại trong file. Cột hiện có: {list(df.columns)}")
        sys.exit(1)
    key_cols_effective = key_cols if len(key_cols) > 1 else key_cols[0]

    prod_df = pd.DataFrame() if bang_moi else scd.load_table_df(table_name, registry)

    # Cảnh báo NGAY nếu khóa chỉ định không đủ phân biệt - trước khi so
    # khớp, để Admin có cơ hội Ctrl+C sửa lại --key-cols nếu cần, thay vì
    # phát hiện muộn sau khi đã ghi ra production.
    canh_bao_khoa = scd.kiem_tra_khoa_du_phan_biet(df, prod_df, key_cols if len(key_cols) > 1 else [key_cols[0]])
    for cb in canh_bao_khoa:
        print(f"⚠️  {cb}")

    print(f"→ So khớp theo khóa '{key_cols}' với {len(prod_df)} dòng production hiện có...")
    nhom = scd.phan_nhom_theo_khoa(df, prod_df, key_cols_effective)

    so_moi = len(nhom["du_lieu_moi"])
    so_trung = len(nhom["trung_lap"])
    so_doi = len(nhom["thay_doi"])
    print(f"\n=== Kết quả phân nhóm: {so_moi} dữ liệu mới | {so_trung} trùng lặp (bỏ qua) | "
          f"{so_doi} thay đổi ===\n")

    # ---------- Nhóm 1: dữ liệu mới ----------
    # Dữ liệu MỚI (không đè lên gì, không xung đột) được tự động thêm, KHÔNG
    # bắt Admin xác nhận từng dòng - nếu bảng có vài trăm/nghìn dòng mới
    # (vd nạp điểm chuẩn 1 năm mới), xác nhận từng dòng sẽ không scale.
    # Mỗi dòng vẫn được LOG đầy đủ vào structured_decision_log.jsonl.
    if so_moi:
        print(f"[Dữ liệu mới] {so_moi} dòng - TỰ ĐỘNG thêm vào production (không hỏi từng dòng):")
        _in_preview(nhom["du_lieu_moi"])
        for _, row in nhom["du_lieu_moi"].iterrows():
            scd.log_structured_decision({
                "su_kien": "tu_dong_them_du_lieu_moi", "table": table_name,
                "key": row[key_cols[0]] if len(key_cols) == 1 else [row[c] for c in key_cols],
            })

    # ---------- Nhóm 2: trùng lặp ----------
    if so_trung:
        print(f"\n[Trùng lặp hoàn toàn] {so_trung} dòng - không cần thao tác.")

    # ---------- Nhóm 3: thay đổi - PER-ROW, đúng sơ đồ ----------
    final_df = prod_df.copy()
    if so_doi:
        print(f"\n[Thay đổi] {so_doi} dòng có khác biệt với production - xử lý từng dòng:")
    for item in nhom["thay_doi"]:
        print("\n" + "-" * 60)
        print(f"Khóa: {item['key']}")
        for col, gt in item["khac_cols"].items():
            hien_cu = "(rỗng)" if pd.isna(gt["cu"]) else gt["cu"]
            hien_moi = "(rỗng)" if pd.isna(gt["moi"]) else gt["moi"]
            print(f"   {col}: '{hien_cu}'  ->  '{hien_moi}'")

        if auto:
            # Chạy không tương tác (vd job từ web) - KHÔNG hỏi qua stdin.
            # [FIX] Trước đây admin_api.py giả lập trả lời 2 câu (y/n) lồng
            # nhau này bằng CÙNG 1 giá trị yn_default: mode="safe" -> "n"+"n"
            # lại rơi vào nhánh else = "giu_ca_2" (giữ CẢ 2 bản, gắn
            # valid_from/valid_to), không phải "giữ bản cũ" như nhãn UI
            # "An toàn (giữ bản cũ)" hứa hẹn. Nay quyết định thẳng, đúng
            # nghĩa: auto="replace" -> lấy giá trị mới; auto="safe" -> giữ
            # nguyên giá trị cũ (giu_cu).
            hanh_dong = "giu_moi" if auto == "replace" else "giu_cu"
            print(f"   [TỰ ĐỘNG - mode={auto}] → {hanh_dong}")
        elif cd._ask_yes_no("Giữ bản mới (ghi đè)?"):
            hanh_dong = "giu_moi"
        elif cd._ask_yes_no("Giữ bản cũ (bỏ qua thay đổi)?"):
            hanh_dong = "giu_cu"
        else:
            print("   Xác nhận: giữ bản cũ + thêm bản mới, gắn valid_from/valid_to để phân biệt.")
            hanh_dong = "giu_ca_2"

        final_df = scd.ap_dung_quyet_dinh_thay_doi(final_df, item, key_cols_effective, hanh_dong)
        scd.log_structured_decision({
            "su_kien": "quyet_dinh_thay_doi", "table": table_name, "key": item["key"],
            "khac_cols": {k: {"cu": str(v["cu"]), "moi": str(v["moi"])} for k, v in item["khac_cols"].items()},
            "hanh_dong": hanh_dong, "tu_dong": bool(auto),
        })
        print(f"   ✅ Đã ghi nhận: {hanh_dong}")

    if so_moi:
        final_df = pd.concat([final_df, nhom["du_lieu_moi"]], ignore_index=True)

    # ---------- Cảnh báo chéo với dữ liệu RAG - TƯƠNG TÁC ----------
    print("\n→ Đối chiếu chéo với dữ liệu phi cấu trúc (RAG) đang production...")
    rows_can_kiem_tra = list(nhom["du_lieu_moi"].to_dict("records")) if so_moi else []
    rows_can_kiem_tra += nhom["thay_doi"]
    canh_bao_cheo = scd.doi_chieu_dong_bang_voi_rag(rows_can_kiem_tra, table_name, registry) if rows_can_kiem_tra else []

    if not canh_bao_cheo:
        print("   Không có cảnh báo chéo.")
    else:
        print(f"⚠️  {len(canh_bao_cheo)} CẢNH BÁO CHÉO - xử lý từng cái (giống conflict_detection.py: "
              f"đoạn RAG dễ sửa/loại, nhưng cũng có thể sửa NGAY giá trị sắp lưu vào bảng nếu văn bản đúng hơn):")
        for cb in canh_bao_cheo:
            print("\n" + "-" * 60)
            print(f"   Đối tượng: '{cb['ten_thuc_the']}'  /  thuộc tính: {cb['cot']}")
            print(f"   Giá trị SẮP lưu vào bảng: {cb['gia_tri_bang_moi']}")
            print(f"   Văn bản '{cb['nguon_van_ban']}' đang nói: {cb['gia_tri_van_ban']}")
            print(f"   Trích dẫn: \"{cb['trich_dan']}\"")
            print(f"   [Toàn văn đoạn RAG] {cb.get('noi_dung_day_du', '')[:600]}")
            print("   [1] Giữ đoạn RAG nguyên trạng   [2] Loại đoạn RAG khỏi production   "
                  "[3] Sửa lại nội dung đoạn RAG ngay   [4] Sửa giá trị SẮP lưu vào bảng theo văn bản")
            while True:
                lua_chon = input("   Chọn 1/2/3/4: ").strip()
                if lua_chon in ("1", "2", "3", "4"):
                    break
                print("   Vui lòng nhập 1, 2, 3 hoặc 4.")

            if lua_chon == "2":
                cd.set_chunk_status(cb["nguon_van_ban"], {cb["chunk_id"]}, "bi_bo_qua")
                print("   ✅ Đã loại đoạn RAG đó khỏi production.")
            elif lua_chon == "3":
                noi_dung_moi = input("   Nhập nội dung ĐÃ SỬA cho đoạn RAG (để trống để hủy): ").strip()
                if noi_dung_moi:
                    cd.set_chunk_content(cb["nguon_van_ban"], cb["chunk_id"], noi_dung_moi)
                    print("   ✅ Đã lưu nội dung đoạn RAG đã sửa.")
                else:
                    lua_chon = "1"
                    print("   Hủy sửa, giữ nguyên đoạn RAG (chỉ ghi nhận).")
            elif lua_chon == "4":
                # Sửa NGAY giá trị sắp lưu vào bảng theo đúng văn bản RAG - áp
                # dụng lên final_df TRƯỚC khi ghi ra production (chưa ghi gì
                # cả, chỉ sửa trong bộ nhớ, còn 1 bước xác nhận cuối ở dưới).
                pk_col = registry[table_name]["primary_key"]
                pk_cols_list = pk_col if isinstance(pk_col, list) else [pk_col]
                khoa = cb["khoa_dong"]
                khoa_tuple = khoa if isinstance(khoa, tuple) else (khoa,)
                mask = pd.Series(True, index=final_df.index)
                for c, v in zip(pk_cols_list, khoa_tuple):
                    mask &= (final_df[c] == v)
                final_df.loc[mask, cb["cot"]] = cb["gia_tri_van_ban"]
                print(f"   ✅ Sẽ ghi '{cb['gia_tri_van_ban']}' vào cột '{cb['cot']}' cho khóa {khoa} "
                      f"khi lưu bảng (chưa ghi - còn bước xác nhận cuối).")
            else:
                print("   ✅ Giữ đoạn RAG nguyên trạng, chỉ ghi nhận (thông tin).")

            scd.log_structured_decision({"su_kien": "canh_bao_cheo_bang_vs_rag", "table": table_name,
                                          "hanh_dong": lua_chon, **cb})
        print("   (Nếu văn bản RAG cần sửa nhiều/phức tạp hơn, dùng 'python conflict_detection.py check-file'.)")

    # ---------- XÁC NHẬN CUỐI CÙNG - chỉ ghi ra production sau khi Admin
    # xác nhận đã xem hết mọi thay đổi ở trên (bỏ qua nếu chạy --auto).
    # [FIX] Trước đây khi gọi tự động với "trả lời n cho mọi (y/n)" (đúng
    # ngữ nghĩa mode="safe"), câu hỏi NÀY cũng bị trả lời "n" -> HỦY TOÀN
    # BỘ, không ghi gì vào production - kể cả các dòng dữ liệu MỚI hoàn
    # toàn không hề xung đột với ai. Khi có --auto, việc "sẽ ghi vào
    # production" là hợp đồng ngầm của việc Admin đã chọn mode An
    # toàn/Ghi đè trên UI rồi, không cần hỏi lại lần nữa. ----------
    print("\n" + "=" * 60)
    print(f"SẮP GHI vào production: {len(final_df)} dòng cho bảng '{table_name}'"
          f"{' (bảng MỚI)' if bang_moi else ''}.")
    if auto:
        print(f"→ [TỰ ĐỘNG - mode={auto}] Xác nhận và tiến hành ghi vào production.")
    elif not cd._ask_yes_no("Xác nhận tiến hành cập nhật production?"):
        print("→ ĐÃ HỦY - KHÔNG ghi gì vào production. File hàng đợi vẫn còn tại "
              f"{staging_path.name} (dùng 'discard-staging' để xóa nếu không cần nữa).")
        sys.exit(0)

    # ---------- Ghi ra production ----------
    if bang_moi:
        # PHẢI tạo entry registry TRƯỚC khi gọi save_table_df() (hàm đó cần
        # registry[table_name]["path"] để biết ghi vào đâu) - nếu đảo thứ
        # tự sẽ gây KeyError.
        registry[table_name] = {
            "path": f"data/processed/tables/{table_name}.csv",
            "primary_key": key_cols[0] if len(key_cols) == 1 else key_cols,
            "name_columns": [], "pii_columns": [], "hidden_columns": [],
            "display_columns": list(final_df.columns), "column_labels": {},
            "has_related_text_corpus": False,
            "description": f"(TODO: Admin bổ sung mô tả cho bảng '{table_name}')",
        }
        scd.save_registry(registry)

    scd.save_table_df(table_name, final_df, registry)
    print(f"\n✅ Đã ghi {len(final_df)} dòng vào production: {registry[table_name]['path']}")

    if bang_moi:
        print(f"✅ Đã thêm bảng '{table_name}' vào registry.json (TODO: bổ sung tay "
              f"display_columns/column_labels/role_columns/group_columns nếu cần dùng matching nâng cao).")

    staging_path.unlink(missing_ok=True)
    print("→ Báo api8923.py nạp lại registry/bảng (không cần khởi động lại process)...")
    cd._notify_api8923_reload()


def cmd_status():
    files = sorted(scd.TABLES_STAGING_FOLDER.glob("*.csv"))
    if not files:
        print("✅ Hàng đợi (tables_staging/) trống - không có gì đang chờ xử lý.")
        return
    print(f"→ {len(files)} file đang trong hàng đợi (đã upload nhưng chưa xử lý xong):")
    for f in files:
        print(f"   - {f.name}")


def cmd_discard_staging(filename: str):
    path = scd.TABLES_STAGING_FOLDER / filename
    if not path.exists():
        print(f"❌ Không tìm thấy '{filename}' trong hàng đợi.")
        sys.exit(1)
    path.unlink()
    print(f"✅ Đã bỏ '{filename}' khỏi hàng đợi.")


def doc_metadata_bang_web(table_name: str) -> dict:
    """Đọc TOÀN BỘ metadata (registry.json) của 1 bảng ĐÃ CÓ + giá trị duy
    nhất của các cột có ít giá trị khác nhau (để Admin biết viết
    categorical_filters đúng giá trị nào đang có thật trong dữ liệu) -
    dùng cho màn hình 'Sửa thông tin bảng' trên web UI."""
    registry = scd.load_registry()
    if table_name not in registry:
        return {"error": f"Bảng '{table_name}' không tồn tại."}
    cfg = registry[table_name]
    df = scd.load_table_df(table_name, registry)
    gia_tri_duy_nhat = {}
    for col in df.columns:
        try:
            vals = sorted(str(v) for v in df[col].dropna().unique())
        except Exception:
            vals = [str(v) for v in df[col].dropna().unique()]
        if 0 < len(vals) <= 30:
            gia_tri_duy_nhat[col] = vals
    return {"ok": True, "table_name": table_name, "columns": list(df.columns),
            "metadata": cfg, "gia_tri_duy_nhat": gia_tri_duy_nhat}


def cap_nhat_metadata_bang_web(table_name: str, metadata: dict) -> dict:
    """Cập nhật CHỈ metadata (registry.json) của 1 bảng ĐÃ CÓ - KHÔNG đụng
    tới dữ liệu .csv. Tự backup registry.json trước khi ghi (phòng
    Admin điền sai) rồi báo api8923.py nạp lại."""
    registry = scd.load_registry()
    if table_name not in registry:
        return {"error": f"Bảng '{table_name}' không tồn tại."}
    cfg = registry[table_name]
    df = scd.load_table_df(table_name, registry)
    cols = set(df.columns)

    def _cols(key):
        return [c for c in (metadata.get(key) or []) if c in cols]

    pii_columns = _cols("pii_columns")
    hidden_columns = _cols("hidden_columns")
    display_columns = [c for c in (metadata.get("display_columns") or list(df.columns))
                        if c in cols and c not in pii_columns and c not in hidden_columns]
    pk = cfg.get("primary_key")
    cfg_moi = {
        **cfg,
        "primary_key": pk,  # KHÔNG cho đổi khóa chính ở màn hình này - đổi khóa cần chạy lại phân tích/ghép dữ liệu
        "name_columns": _cols("name_columns"),
        "role_columns": _cols("role_columns"),
        "group_columns": _cols("group_columns"),
        "pii_columns": pii_columns, "hidden_columns": hidden_columns,
        "display_columns": display_columns,
        "column_labels": {k: v for k, v in (metadata.get("column_labels") or {}).items() if k in cols and v},
        "disambiguating_columns": _cols("disambiguating_columns"),
        "categorical_filters": {col: kw_map for col, kw_map in (metadata.get("categorical_filters") or {}).items()
                                 if col in cols and isinstance(kw_map, dict) and kw_map},
        "has_related_text_corpus": bool(metadata.get("has_related_text_corpus")),
        "description": (metadata.get("description") or "").strip() or cfg.get("description", ""),
        "extra_instructions": (metadata.get("extra_instructions") or "").strip(),
    }
    registry_path = scd.BASE / "registry.json"
    if registry_path.exists():
        bak = registry_path.with_name(f"registry.bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        shutil.copy2(registry_path, bak)
        print(f"→ Đã backup registry.json cũ tại: {bak.name}")
    registry[table_name] = cfg_moi
    scd.save_registry(registry)
    print(f"✅ Đã cập nhật metadata cho bảng '{table_name}'.")
    print("→ Báo api8923.py nạp lại registry/bảng...")
    cd._notify_api8923_reload()
    return {"ok": True, "table_name": table_name, "metadata": cfg_moi}


def cmd_list_tables():
    registry = scd.load_registry()
    for name, cfg in registry.items():
        df = scd.load_table_df(name, registry)
        print(f"- {name}: {cfg['path']}  (primary_key={cfg['primary_key']}, {len(df)} dòng)")


def cmd_remove_table(table_name: str):
    """Lệnh 'remove-table <ten_bang>' - xóa hẳn 1 bảng SAI/TEST khỏi hệ
    thống: xóa entry trong registry.json + BACKUP rồi xóa file .csv. Dùng
    khi lỡ tạo bảng nhầm hoàn toàn (vd gõ nhầm tên bảng tạo ra bảng trùng
    lặp dữ liệu - xem cảnh báo tự động khi 'add' phát hiện trùng cột). KHÔNG
    dùng cho bảng thật đang được dùng - lệnh này XÓA VĨNH VIỄN khỏi registry
    (file .csv vẫn còn 1 bản backup .bak_* để cứu lại nếu cần)."""
    registry = scd.load_registry()
    if table_name not in registry:
        print(f"❌ Bảng '{table_name}' không có trong registry.json - không có gì để xóa.")
        sys.exit(1)
    path = scd.BASE / registry[table_name]["path"]
    if path.exists():
        backup_path = path.with_name(f"{path.stem}.bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}_before_remove{path.suffix}")
        path.rename(backup_path)
        print(f"→ Đã backup file cũ tại: {backup_path}")
    del registry[table_name]
    scd.save_registry(registry)
    print(f"✅ Đã xóa bảng '{table_name}' khỏi registry.json.")
    cd._notify_api8923_reload()


def cmd_list_backups(table_name: str):
    """Lệnh 'list-backups <ten_bang>' - liệt kê các bản backup .bak_* của 1
    bảng (tự động tạo mỗi lần save_table_df() ghi đè) - dùng để tìm bản
    backup CŨ NHẤT (thường là trạng thái TRƯỚC KHI có bất kỳ thay đổi/test
    nào) để khôi phục bằng 'restore-backup'."""
    registry = scd.load_registry()
    if table_name not in registry:
        print(f"❌ Bảng '{table_name}' không có trong registry.json.")
        sys.exit(1)
    path = scd.BASE / registry[table_name]["path"]
    backups = sorted(path.parent.glob(f"{path.stem}.bak_*"))
    if not backups:
        print(f"✅ Chưa có backup nào cho bảng '{table_name}'.")
        return
    print(f"→ {len(backups)} backup của '{table_name}' (CŨ NHẤT liệt kê trước):")
    for b in backups:
        print(f"   - {b.name}")


def cmd_restore_backup(table_name: str, backup_filename: str):
    """Lệnh 'restore-backup <ten_bang> <ten_file_backup>' - khôi phục bảng
    từ 1 bản backup cụ thể (lấy tên file từ 'list-backups'). Tự backup bản
    HIỆN TẠI trước khi ghi đè, phòng chọn nhầm."""
    registry = scd.load_registry()
    if table_name not in registry:
        print(f"❌ Bảng '{table_name}' không có trong registry.json.")
        sys.exit(1)
    path = scd.BASE / registry[table_name]["path"]
    backup_path = path.parent / backup_filename

    # Nếu backup_filename trỏ tới CHÍNH file production hiện tại (Admin gõ
    # nhầm, thường vì 'list-backups' báo "chưa có backup nào" nên đoán bừa
    # tên file bảng), chặn NGAY với thông báo rõ - tránh crash SameFileError
    # giữa đường.
    if backup_path.resolve() == path.resolve():
        print(f"❌ '{backup_filename}' chính là file PRODUCTION hiện tại, không phải file backup. "
              f"Chạy 'list-backups {table_name}' để xem tên file backup THẬT (dạng "
              f".bak_YYYYMMDD_HHMMSS.csv). Nếu danh sách đó rỗng, nghĩa là KHÔNG có backup nào để "
              f"khôi phục qua lệnh này - cần tìm lại bản gốc theo cách khác.")
        sys.exit(1)
    if not backup_path.exists():
        print(f"❌ Không tìm thấy '{backup_filename}'. Chạy 'list-backups {table_name}' để xem danh sách.")
        sys.exit(1)

    import shutil
    if path.exists():
        tu_backup = path.with_name(f"{path.stem}.bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}_before_restore{path.suffix}")
        shutil.copy2(path, tu_backup)
        print(f"→ Đã backup bản hiện tại tại: {tu_backup}")
    shutil.copy2(backup_path, path)
    print(f"✅ Đã khôi phục '{table_name}' từ '{backup_filename}'.")
    cd._notify_api8923_reload()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    command = sys.argv[1]

    if command == "add":
        if len(sys.argv) < 4:
            print("Dùng: python structured_data_pipeline.py add <duong_dan> <ten_bang> [--key-cols A,B]")
            sys.exit(1)
        path_str, table_name = sys.argv[2], sys.argv[3]
        key_cols_override = None
        auto = None
        for arg in sys.argv[4:]:
            if arg.startswith("--key-cols="):
                key_cols_override = arg.split("=", 1)[1].split(",")
            elif arg.startswith("--auto="):
                auto = arg.split("=", 1)[1].strip().lower()
        cmd_add(path_str, table_name, key_cols_override, auto=auto)
    elif command == "status":
        cmd_status()
    elif command == "discard-staging":
        if len(sys.argv) < 3:
            print("Dùng: python structured_data_pipeline.py discard-staging <ten_file>")
            sys.exit(1)
        cmd_discard_staging(sys.argv[2])
    elif command == "list-tables":
        cmd_list_tables()
    elif command == "remove-table":
        if len(sys.argv) < 3:
            print("Dùng: python structured_data_pipeline.py remove-table <ten_bang>")
            sys.exit(1)
        cmd_remove_table(sys.argv[2])
    elif command == "list-backups":
        if len(sys.argv) < 3:
            print("Dùng: python structured_data_pipeline.py list-backups <ten_bang>")
            sys.exit(1)
        cmd_list_backups(sys.argv[2])
    elif command == "restore-backup":
        if len(sys.argv) < 4:
            print("Dùng: python structured_data_pipeline.py restore-backup <ten_bang> <ten_file_backup>")
            sys.exit(1)
        cmd_restore_backup(sys.argv[2], sys.argv[3])
    elif command == "get-config-web":
        if len(sys.argv) < 3:
            print("Dùng: python structured_data_pipeline.py get-config-web <ten_bang> --out <path>", file=sys.stderr)
            sys.exit(1)
        table_name = sys.argv[2]
        out_path = None
        i = 3
        while i < len(sys.argv):
            if sys.argv[i] == "--out" and i + 1 < len(sys.argv):
                out_path = sys.argv[i + 1]; i += 2
            else:
                i += 1
        result = doc_metadata_bang_web(table_name)
        text = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            Path(out_path).write_text(text, encoding="utf-8")
        else:
            print(text)
        if not result.get("ok"):
            sys.exit(1)
    elif command == "update-config-web":
        if len(sys.argv) < 3:
            print("Dùng: python structured_data_pipeline.py update-config-web <ten_bang> --metadata <path> "
                  "--out <path>", file=sys.stderr)
            sys.exit(1)
        table_name = sys.argv[2]
        metadata_path, out_path = None, None
        i = 3
        while i < len(sys.argv):
            if sys.argv[i] == "--metadata" and i + 1 < len(sys.argv):
                metadata_path = sys.argv[i + 1]; i += 2
            elif sys.argv[i] == "--out" and i + 1 < len(sys.argv):
                out_path = sys.argv[i + 1]; i += 2
            else:
                i += 1
        if not metadata_path:
            print("Cần --metadata <path>", file=sys.stderr); sys.exit(1)
        metadata = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
        result = cap_nhat_metadata_bang_web(table_name, metadata)
        text = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            Path(out_path).write_text(text, encoding="utf-8")
        else:
            print(text)
        if not result.get("ok"):
            sys.exit(1)
    elif command == "analyze-web":
        if len(sys.argv) < 4:
            print("Dùng: python structured_data_pipeline.py analyze-web <duong_dan> <ten_bang> "
                  "[--key-cols=A,B] [--force-new] --out <path>", file=sys.stderr)
            sys.exit(1)
        path_str, table_name = sys.argv[2], sys.argv[3]
        key_cols_override, out_path, force_new = None, None, False
        i = 4
        while i < len(sys.argv):
            if sys.argv[i].startswith("--key-cols="):
                key_cols_override = sys.argv[i].split("=", 1)[1].split(","); i += 1
            elif sys.argv[i] == "--force-new":
                force_new = True; i += 1
            elif sys.argv[i] == "--out" and i + 1 < len(sys.argv):
                out_path = sys.argv[i + 1]; i += 2
            else:
                i += 1
        result = phan_tich_upload_web(path_str, table_name, key_cols_override, force_new=force_new)
        text = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            Path(out_path).write_text(text, encoding="utf-8")
            print(f"→ Đã ghi kết quả phân tích: {out_path}")
        else:
            print(text)
        if not result.get("ok"):
            sys.exit(1)
    elif command == "recheck-cross-web":
        if len(sys.argv) < 4:
            print("Dùng: python structured_data_pipeline.py recheck-cross-web <ten_bang> "
                  "<staging_token> --name-cols=A,B [--role-cols=X] [--group-cols=Y] --out <path>",
                  file=sys.stderr)
            sys.exit(1)
        table_name, staging_token = sys.argv[2], sys.argv[3]
        name_cols, role_cols, group_cols, out_path = [], [], [], None
        i = 4
        while i < len(sys.argv):
            if sys.argv[i].startswith("--name-cols="):
                name_cols = sys.argv[i].split("=", 1)[1].split(","); i += 1
            elif sys.argv[i].startswith("--role-cols="):
                role_cols = sys.argv[i].split("=", 1)[1].split(","); i += 1
            elif sys.argv[i].startswith("--group-cols="):
                group_cols = sys.argv[i].split("=", 1)[1].split(","); i += 1
            elif sys.argv[i] == "--out" and i + 1 < len(sys.argv):
                out_path = sys.argv[i + 1]; i += 2
            else:
                i += 1
        result = phan_tich_cheo_rag_moi_web(table_name, staging_token, name_cols, role_cols, group_cols)
        text = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            Path(out_path).write_text(text, encoding="utf-8")
            print(f"→ Đã ghi kết quả: {out_path}")
        else:
            print(text)
        if not result.get("ok"):
            sys.exit(1)
    elif command == "apply-web":
        if len(sys.argv) < 3:
            print("Dùng: python structured_data_pipeline.py apply-web <ten_bang> --decisions <path> "
                  "--out <path>", file=sys.stderr)
            sys.exit(1)
        table_name = sys.argv[2]
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
        result = ap_dung_quyet_dinh_upload_web(table_name, decisions)
        text = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            Path(out_path).write_text(text, encoding="utf-8")
            print(f"→ Đã ghi kết quả áp dụng: {out_path}")
        else:
            print(text)
        if not result.get("ok"):
            sys.exit(1)
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
