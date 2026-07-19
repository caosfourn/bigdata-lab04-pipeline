"""
discovery.py — Task 1: Repository Cloning & File Discovery

Chức năng:
  - Duyệt toàn bộ thư mục repo đã clone.
  - Lọc bỏ các file/thư mục không cần thiết: test, setup, auto-generated.
  - Tính SHA-256 cho mỗi file để hỗ trợ idempotent replay (Task 6).
  - Trả về danh sách dict với thông tin đầy đủ cho Parser Service.
"""

import hashlib
import json
import os

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

# Tên thư mục cần bỏ qua hoàn toàn (không duyệt vào bên trong)
EXCLUDE_DIRS = {
    "tests",          # unit test / integration test
    "examples",       # ví dụ minh họa, không phải code core
    ".git",           # git metadata
    ".github",        # CI/CD config
    "build",          # build artifacts
    "dist",           # distribution packages
    "__pycache__",    # Python bytecode cache
    ".eggs",          # setuptools artifacts
    "node_modules",   # JS dependencies (nếu có)
    ".tox",           # tox test environments
    ".venv",          # virtual environment
    "venv",
    "env",
    ".mypy_cache",
    ".pytest_cache",
    "htmlcov",        # coverage reports
}

# Tên file cụ thể cần loại trừ (auto-generated, setup, config tools)
EXCLUDE_FILES = {
    "setup.py",           # packaging script
    "setup.cfg",          # packaging config
    "conftest.py",        # pytest fixtures
    "manage.py",          # Django management (nếu có)
}

# Pattern hậu tố để loại trừ file auto-generated
EXCLUDE_SUFFIXES = (
    "_pb2.py",          # Protocol Buffer generated
    "_pb2_grpc.py",     # gRPC generated
    "_generated.py",    # generic code-gen
    "_gen.py",
)


# ─────────────────────────────────────────────────────────────────────────────
# HELPER
# ─────────────────────────────────────────────────────────────────────────────

def _is_excluded_file(filename: str) -> bool:
    """Kiểm tra xem file có nằm trong danh sách loại trừ không."""
    if filename in EXCLUDE_FILES:
        return True
    for suffix in EXCLUDE_SUFFIXES:
        if filename.endswith(suffix):
            return True
    return False


def compute_file_hash(file_path: str) -> str:
    """
    Tính SHA-256 hash của nội dung file.
    Dùng để detect thay đổi khi replay (Task 6 idempotency).
    """
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def discover_python_files(repo_path: str) -> list[dict]:
    """
    Duyệt repo và thu thập tất cả file .py cần parse, kèm metadata.

    Args:
        repo_path: Đường dẫn tuyệt đối (hoặc tương đối) đến repo đã clone.

    Returns:
        Danh sách dict, mỗi phần tử là:
        {
            "relative_path": str,   # đường dẫn tương đối so với repo_path
            "absolute_path": str,   # đường dẫn tuyệt đối dùng để open() file
            "file_size_bytes": int, # kích thước file
            "file_hash": str,       # SHA-256 hash của nội dung file
        }
    """
    discovered = []
    repo_abs = os.path.abspath(repo_path)

    for root, dirs, files in os.walk(repo_abs):
        # Lọc thư mục in-place để os.walk không đi sâu vào các thư mục loại trừ
        dirs[:] = sorted([d for d in dirs if d not in EXCLUDE_DIRS])

        for filename in sorted(files):
            # Chỉ lấy file .py
            if not filename.endswith(".py"):
                continue
            # Bỏ qua các file trong danh sách loại trừ
            if _is_excluded_file(filename):
                continue

            abs_path = os.path.join(root, filename)
            rel_path = os.path.relpath(abs_path, repo_abs)

            try:
                size = os.path.getsize(abs_path)
                file_hash = compute_file_hash(abs_path)
            except OSError:
                # Bỏ qua file không đọc được (permission, broken symlink, v.v.)
                continue

            discovered.append({
                "relative_path":   rel_path,
                "absolute_path":   abs_path,
                "file_size_bytes": size,
                "file_hash":       file_hash,
            })

    return discovered


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT (chạy trực tiếp để kiểm tra)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    repo_root = sys.argv[1] if len(sys.argv) > 1 else "../lerobot"

    print(f"[Discovery] Đang quét repo: {os.path.abspath(repo_root)}")
    py_files = discover_python_files(repo_root)

    print(f"[Discovery] Tổng số file Python cốt lõi: {len(py_files)}")
    print("[Discovery] 10 file đầu tiên:")
    for f in py_files[:10]:
        print(f"  - {f['relative_path']}  ({f['file_size_bytes']} bytes)")

    # Lưu danh sách ra JSON để Parser Service & Notebook sử dụng
    output_path = os.path.join(os.path.dirname(__file__), "..", "discovered_files.json")
    with open(output_path, "w", encoding="utf-8") as out:
        json.dump(py_files, out, indent=2, ensure_ascii=False)
    print(f"\n[Discovery] Đã lưu danh sách vào: {os.path.abspath(output_path)}")