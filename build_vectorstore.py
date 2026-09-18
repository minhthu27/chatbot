"""
Bước còn thiếu giữa pipeline_pdf.py (finalize -> chunks/*.json) và api8923.py
(load FAISS). Chạy 1 LẦN sau khi đã finalize xong toàn bộ tài liệu, và chạy
LẠI mỗi khi có tài liệu mới / sửa tài liệu cũ.

    python build_vectorstore.py

Yêu cầu: Ollama server (OLLAMA_SERVER) đang chạy và đã pull model
'qwen3-embedding:8b-ctx16k' (PHẢI cùng model với api8923.py dùng để load lại -
embedding model đổi thì phải build lại từ đầu, không dùng lẫn được).
"""

import json
import os
from pathlib import Path

from dotenv import load_dotenv
from langchain_core.documents import Document
from langchain_community.vectorstores import FAISS
from langchain_ollama import OllamaEmbeddings

# Định nghĩa thẳng đường dẫn thay vì import từ pipeline_pdf.py - tránh kéo
# theo các phụ thuộc không cần thiết cho việc build FAISS (vd pdf2image của OCR).
BASE = Path(__file__).resolve().parent
output_chunk_folder = BASE / "data" / "processed" / "chunks"

load_dotenv()

# Cấu hình đọc từ .env - đồng bộ với api8923.py/conflict_detection.py.
# Lưu ý: default dưới đây phải khớp default trong api8923.py, nếu không
# index build ra sẽ không tương thích với model runtime mà không báo lỗi.
OLLAMA_SERVER = os.getenv("OLLAMA_SERVER", "http://10.2.13.58:8037/ollama")
# Server GPU yêu cầu header xác thực qua proxy 8037.
OLLAMA_SECKEY = os.getenv("OLLAMA_SECKEY", "research")
OLLAMA_CLIENT_KWARGS = {"headers": {"x-ollama-seckey": OLLAMA_SECKEY}}
EMBED_MODEL = os.getenv("EMBED_MODEL", "qwen3-embedding:8b-ctx16k")  # PHẢI khớp model trong api8923.py

# Nơi lưu index - api8923.py phải trỏ vector_db_path về ĐÚNG thư mục này
VECTOR_DB_PATH = str(BASE / "data" / "processed" / "vectorstore")


def load_all_chunks() -> list[Document]:
    chunk_files = sorted(Path(output_chunk_folder).glob("*.json"))
    if not chunk_files:
        raise RuntimeError(
            f"Không tìm thấy file chunk nào trong {output_chunk_folder}. "
            f"Cần chạy xong 'pipeline_pdf.py finalize' (và/hoặc pipeline_docx_txt.py) trước."
        )

    docs: list[Document] = []
    for path in chunk_files:
        chunks = json.loads(path.read_text(encoding="utf-8"))
        for c in chunks:
            content = (c.get("content") or "").strip()
            if not content:
                continue
            metadata = dict(c.get("metadata") or {})
            # Ghi tên file gốc vào metadata - dùng để nhóm chunk theo văn bản khi
            # phát hiện xung đột sau này.
            metadata["source_file"] = path.stem
            docs.append(Document(page_content=content, metadata=metadata))

    print(f"-> Đọc được {len(docs)} chunk từ {len(chunk_files)} file.")
    return docs


def main():
    docs = load_all_chunks()

    embeddings = OllamaEmbeddings(base_url=OLLAMA_SERVER, model=EMBED_MODEL, client_kwargs=OLLAMA_CLIENT_KWARGS)

    print(f"-> Đang embed {len(docs)} chunk bằng '{EMBED_MODEL}' (có thể mất vài phút)...")
    vector_store = FAISS.from_documents(docs, embeddings)

    os.makedirs(VECTOR_DB_PATH, exist_ok=True)
    vector_store.save_local(VECTOR_DB_PATH)
    print(f"✅ Đã lưu FAISS index tại: {VECTOR_DB_PATH}")
    print(f"   -> Sửa api8923.py: VECTOR_DB_PATH = r\"{VECTOR_DB_PATH}\"")


if __name__ == "__main__":
    main()
