"""
schemas.py — Định nghĩa schema message cho mọi Kafka topic của Parser Service.

Được thiết kế để Thành viên 2 (Kafka Producer/Consumer) sử dụng ngay lập tức
mà không cần biết chi tiết bên trong của Parser Service.

Kafka Topic Layout (khớp với yêu cầu Task 3):
  - cpg.nodes      : AST node events
  - cpg.edges      : AST/CFG/DFG/CALL edge events
  - cpg.metadata   : Source file metadata events
  - cpg.errors     : Parser error events
"""

import datetime

# ─────────────────────────────────────────────────────────────────────────────
# KAFKA TOPIC NAMES — Thành viên 2 import từ đây để dùng chung
# ─────────────────────────────────────────────────────────────────────────────

TOPIC_NODES    = "cpg.nodes"
TOPIC_EDGES    = "cpg.edges"
TOPIC_METADATA = "cpg.metadata"
TOPIC_ERRORS   = "cpg.errors"

SCHEMA_VERSION = "1.0"


# ─────────────────────────────────────────────────────────────────────────────
# HELPER
# ─────────────────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    """Trả về timestamp UTC hiện tại theo định dạng ISO 8601."""
    return datetime.datetime.utcnow().isoformat() + "Z"


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA TEMPLATES
# ─────────────────────────────────────────────────────────────────────────────

def make_node_event(
    node_id: str,
    file_path: str,
    label: str,
    node_type: str,
    line_number: int | None,
    col_offset: int | None,
    end_lineno: int | None,
    end_col_offset: int | None,
    name: str | None = None,
    code_snippet: str | None = None,
    scope: str | None = None,
) -> dict:
    """
    Tạo một node event hoàn chỉnh để produce lên topic cpg.nodes.

    Args:
        node_id       : Stable deterministic ID (xem generate_stable_node_id trong parser_service).
        file_path     : Đường dẫn tương đối của file nguồn so với repo root.
        label         : Nhãn Neo4j, ví dụ "AST_Node", "FunctionDef", "ClassDef".
        node_type     : Tên class AST, ví dụ "FunctionDef", "If", "Call".
        line_number   : Dòng bắt đầu (1-indexed), None nếu không có.
        col_offset    : Cột bắt đầu (0-indexed), None nếu không có.
        end_lineno    : Dòng kết thúc, None nếu không có.
        end_col_offset: Cột kết thúc, None nếu không có.
        name          : Tên định danh (hàm, lớp, biến…), None nếu không có.
        code_snippet  : Đoạn code tương ứng (tùy chọn, dùng cho Neo4j full-text index).
        scope         : Tên hàm/lớp bao quanh để xác định phạm vi.
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "event_time":     _now_iso(),
        "topic":          TOPIC_NODES,
        "file_path":      file_path,
        "node_id":        node_id,
        "label":          label,
        "properties": {
            "type":            node_type,
            "line_number":     line_number,
            "col_offset":      col_offset,
            "end_lineno":      end_lineno,
            "end_col_offset":  end_col_offset,
            "name":            name,
            "code_snippet":    code_snippet,
            "scope":           scope,
        },
    }


def make_edge_event(
    edge_id: str,
    file_path: str,
    edge_type: str,
    source_id: str,
    target_id: str,
    properties: dict | None = None,
) -> dict:
    """
    Tạo một edge event hoàn chỉnh để produce lên topic cpg.edges.

    Args:
        edge_id    : Stable deterministic ID của cạnh.
        file_path  : Đường dẫn tương đối của file nguồn.
        edge_type  : Loại cạnh: "AST_CHILD" | "CFG_NEXT" | "DFG_USE" | "CALL".
        source_id  : node_id của nút nguồn.
        target_id  : node_id của nút đích.
        properties : Metadata bổ sung tuỳ loại edge (tùy chọn).
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "event_time":     _now_iso(),
        "topic":          TOPIC_EDGES,
        "file_path":      file_path,
        "edge_id":        edge_id,
        "type":           edge_type,
        "source_id":      source_id,
        "target_id":      target_id,
        "properties":     properties or {},
    }


def make_metadata_event(
    file_path: str,
    file_size_bytes: int,
    file_hash: str,
    total_nodes: int,
    total_ast_edges: int,
    total_cfg_edges: int,
    total_dfg_edges: int,
    total_call_edges: int,
    parser_version: str = "ast-stdlib",
    parse_duration_ms: float = 0.0,
) -> dict:
    """
    Tạo một metadata event hoàn chỉnh để produce lên topic cpg.metadata.
    Event này được Thành viên 3 (MongoDB) consume để lưu file-level metadata.

    Args:
        file_path        : Đường dẫn tương đối của file nguồn.
        file_size_bytes  : Kích thước file tính bằng byte.
        file_hash        : SHA-256 của file (dùng để detect thay đổi cho Task 6).
        total_nodes      : Tổng số node đã trích xuất.
        total_ast_edges  : Số cạnh AST_CHILD.
        total_cfg_edges  : Số cạnh CFG_NEXT.
        total_dfg_edges  : Số cạnh DFG_USE.
        total_call_edges : Số cạnh CALL.
        parser_version   : Thư viện parser đã dùng ("ast-stdlib" / "tree-sitter").
        parse_duration_ms: Thời gian parse (milliseconds).
    """
    return {
        "schema_version":    SCHEMA_VERSION,
        "event_time":        _now_iso(),
        "topic":             TOPIC_METADATA,
        "file_path":         file_path,
        "file_size_bytes":   file_size_bytes,
        "file_hash":         file_hash,
        "total_nodes":       total_nodes,
        "total_edges": {
            "ast":  total_ast_edges,
            "cfg":  total_cfg_edges,
            "dfg":  total_dfg_edges,
            "call": total_call_edges,
        },
        "parser_version":    parser_version,
        "parse_duration_ms": parse_duration_ms,
    }


def make_error_event(
    file_path: str,
    error_type: str,
    error_message: str,
    line_number: int | None = None,
    col_offset: int | None = None,
) -> dict:
    """
    Tạo một error event để produce lên topic cpg.errors.
    Dùng khi Parser Service gặp SyntaxError, UnicodeDecodeError, v.v.

    Args:
        file_path     : Đường dẫn tương đối của file bị lỗi.
        error_type    : Loại lỗi, ví dụ "SyntaxError", "UnicodeDecodeError".
        error_message : Nội dung thông báo lỗi đầy đủ.
        line_number   : Số dòng gây ra lỗi (nếu có).
        col_offset    : Vị trí cột gây ra lỗi (nếu có).
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "event_time":     _now_iso(),
        "topic":          TOPIC_ERRORS,
        "file_path":      file_path,
        "error_type":     error_type,
        "error_message":  error_message,
        "line_number":    line_number,
        "col_offset":     col_offset,
    }