"""
parser_service.py — Task 2: Incremental CPG Parser Service

Chức năng:
  - Nhận 1 file .py, parse thành Code Property Graph (CPG).
  - Trích xuất 4 loại phần tử theo chuẩn CPG:
      • AST nodes    — mọi nút trong cây cú pháp trừu tượng.
      • CFG edges    — luồng điều khiển (if/for/while/try/with/return).
      • DFG edges    — luồng dữ liệu (gán biến → sử dụng biến).
      • CALL edges   — lời gọi hàm (caller → callee).
  - Gán Stable ID xác định cho mọi element (đảm bảo idempotency Task 6).
  - Trả về events theo đúng schema trong schemas.py để Thành viên 2 produce
    lên Kafka mà không cần sửa đổi thêm.

Ghi chú về thư viện:
  Sử dụng module `ast` của thư viện chuẩn Python — không cần cài thêm gói.
  Thích hợp cho môi trường hạn chế, đủ để trích xuất AST/CFG/DFG/CALL.
"""

import ast
import hashlib
import os
import time
import datetime
from typing import Generator

from schemas import (
    make_node_event,
    make_edge_event,
    make_metadata_event,
    make_error_event,
)

SCHEMA_VERSION = "1.0"


# ─────────────────────────────────────────────────────────────────────────────
# STABLE ID GENERATION
# ─────────────────────────────────────────────────────────────────────────────

def _stable_node_id(relative_path: str, node: ast.AST) -> str:
    """
    Tạo Stable ID xác định (deterministic) cho một AST node.

    QUAN TRỌNG: ID phải giống nhau mọi lần parse cùng file (nếu code không đổi).
    Vì vậy KHÔNG dùng id(node) — đó là memory address, thay đổi mỗi lần chạy.

    Chiến lược: Hash SHA-256 của (file_path, node_type, lineno, col_offset).
    Dùng 16 ký tự hex đầu tiên để giữ ID ngắn gọn nhưng vẫn đủ unique.
    """
    line = getattr(node, "lineno",     0)
    col  = getattr(node, "col_offset", 0)
    node_type = node.__class__.__name__
    raw = f"{relative_path}::{node_type}@L{line}C{col}"
    return "node_" + hashlib.sha256(raw.encode()).hexdigest()[:16]


def _stable_edge_id(relative_path: str, edge_type: str, src_id: str, tgt_id: str) -> str:
    """
    Tạo Stable ID xác định cho một edge.
    Đảm bảo cùng cặp (src, tgt, type) → cùng ID → Neo4j MERGE không tạo duplicate.
    """
    raw = f"{relative_path}::{edge_type}::{src_id}::{tgt_id}"
    return "edge_" + hashlib.sha256(raw.encode()).hexdigest()[:16]


# ─────────────────────────────────────────────────────────────────────────────
# AST VISITOR — thu thập tất cả node IDs trong 1 lần duyệt O(n)
# ─────────────────────────────────────────────────────────────────────────────

class _NodeIDCollector(ast.NodeVisitor):
    """
    Bước 1: Duyệt cây AST một lần để xây dựng mapping ast_node → node_id.
    Kết quả này được tái sử dụng bởi các bước trích xuất edge (CFG/DFG/CALL)
    mà không cần parse lại hay tính lại hash.
    """

    def __init__(self, relative_path: str):
        self.relative_path = relative_path
        # Dùng id() Python làm key temporary (chỉ trong phạm vi 1 lần parse)
        self._id_map: dict[int, str] = {}

    def generic_visit(self, node: ast.AST):
        nid = _stable_node_id(self.relative_path, node)
        self._id_map[id(node)] = nid
        super().generic_visit(node)

    def get_id(self, node: ast.AST) -> str:
        """Tra cứu stable ID của node (đã được tính ở bước đầu)."""
        return self._id_map.get(id(node), _stable_node_id(self.relative_path, node))


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACTOR HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _extract_name(node: ast.AST) -> str | None:
    """Lấy tên định danh của node nếu có (hàm, lớp, biến…)."""
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return node.name                               # type: ignore[attr-defined]
    if isinstance(node, ast.Name):
        return node.id                                 # type: ignore[attr-defined]
    if isinstance(node, ast.Attribute):
        return node.attr                               # type: ignore[attr-defined]
    if isinstance(node, ast.alias):
        return node.asname or node.name               # type: ignore[attr-defined]
    if isinstance(node, ast.arg):
        return node.arg                               # type: ignore[attr-defined]
    return None


def _get_label(node: ast.AST) -> str:
    """Chọn Neo4j label phù hợp với loại node để query dễ hơn."""
    class_name = node.__class__.__name__
    # Nhóm các node quan trọng thành label riêng
    important = {
        "FunctionDef", "AsyncFunctionDef",
        "ClassDef",
        "If", "For", "AsyncFor", "While",
        "Try", "TryStar", "With", "AsyncWith",
        "Return", "Yield", "YieldFrom",
        "Import", "ImportFrom",
        "Call",
        "Assign", "AugAssign", "AnnAssign",
        "Name", "Attribute",
    }
    return class_name if class_name in important else "AST_Node"


def _get_scope(node: ast.AST, parent_map: dict[int, ast.AST]) -> str | None:
    """Leo lên cây cha để tìm hàm/lớp bao quanh gần nhất."""
    current = parent_map.get(id(node))
    while current is not None:
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            return getattr(current, "name", None)
        current = parent_map.get(id(current))
    return None


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PARSER CLASS
# ─────────────────────────────────────────────────────────────────────────────

class CPGParser:
    """
    CPGParser xử lý MỘT file Python, trích xuất CPG và trả về:
      - node_events  : list[dict]  → produce lên TOPIC_NODES
      - edge_events  : list[dict]  → produce lên TOPIC_EDGES
      - metadata     : dict        → produce lên TOPIC_METADATA
      - error_event  : dict | None → produce lên TOPIC_ERRORS (nếu có lỗi)

    Thiết kế bounded-memory: events được emit theo generator (yield), không
    tích lũy toàn bộ vào RAM. Phiên bản list-based bên dưới giữ lại để
    notebook và testing dùng dễ hơn.
    """

    def __init__(self, absolute_path: str, repo_root: str):
        """
        Args:
            absolute_path: Đường dẫn tuyệt đối đến file .py cần parse.
            repo_root    : Đường dẫn tuyệt đối đến thư mục gốc của repo đã clone.
                           Dùng để tính relative_path chuẩn hóa trong mọi event.
        """
        self.absolute_path = absolute_path
        self.repo_root     = os.path.abspath(repo_root)
        # relative_path là key chính trong mọi event — chuẩn hóa bằng forward slash
        self.relative_path = os.path.relpath(absolute_path, self.repo_root).replace("\\", "/")

    # ── PUBLIC API ────────────────────────────────────────────────────────────

    def parse(self) -> tuple[list, list, dict, dict | None]:
        """
        Parse file và trả về tuple (nodes, edges, metadata, error_event).

        Returns:
            nodes      : Danh sách node events (cho cpg.nodes topic).
            edges      : Danh sách edge events (cho cpg.edges topic).
            metadata   : Metadata event (cho cpg.metadata topic).
            error_event: Error event (cho cpg.errors topic) hoặc None nếu không có lỗi.
        """
        t_start = time.time()

        # Đọc file
        try:
            with open(self.absolute_path, "r", encoding="utf-8") as f:
                source_code = f.read()
        except (OSError, UnicodeDecodeError) as e:
            err = make_error_event(
                file_path=self.relative_path,
                error_type=type(e).__name__,
                error_message=str(e),
            )
            meta = self._empty_metadata(t_start)
            return [], [], meta, err

        # Parse AST
        try:
            tree = ast.parse(source_code, filename=self.relative_path)
        except SyntaxError as e:
            err = make_error_event(
                file_path=self.relative_path,
                error_type="SyntaxError",
                error_message=str(e.msg),
                line_number=e.lineno,
                col_offset=e.offset,
            )
            meta = self._empty_metadata(t_start)
            return [], [], meta, err

        # Xây dựng cấu trúc hỗ trợ: parent map và ID map
        parent_map   = self._build_parent_map(tree)
        id_collector = _NodeIDCollector(self.relative_path)
        id_collector.visit(tree)

        # Trích xuất 4 thành phần CPG
        node_events = list(self._extract_ast_nodes(tree, id_collector, parent_map, source_code))
        ast_edges   = list(self._extract_ast_edges(tree, id_collector))
        cfg_edges   = list(self._extract_cfg_edges(tree, id_collector))
        dfg_edges   = list(self._extract_dfg_edges(tree, id_collector))
        call_edges  = list(self._extract_call_edges(tree, id_collector))
        edge_events = ast_edges + cfg_edges + dfg_edges + call_edges

        # Tính file hash để phục vụ idempotency (Task 6)
        file_hash = self._compute_file_hash()
        duration_ms = (time.time() - t_start) * 1000

        metadata = make_metadata_event(
            file_path=self.relative_path,
            file_size_bytes=os.path.getsize(self.absolute_path),
            file_hash=file_hash,
            total_nodes=len(node_events),
            total_ast_edges=len(ast_edges),
            total_cfg_edges=len(cfg_edges),
            total_dfg_edges=len(dfg_edges),
            total_call_edges=len(call_edges),
            parser_version="ast-stdlib-3.x",
            parse_duration_ms=round(duration_ms, 2),
        )

        return node_events, edge_events, metadata, None

    # ── INTERNAL: SUPPORT STRUCTURES ─────────────────────────────────────────

    def _build_parent_map(self, tree: ast.AST) -> dict[int, ast.AST]:
        """Xây dựng mapping child id → parent node để tìm scope."""
        parent_map: dict[int, ast.AST] = {}
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                parent_map[id(child)] = node
        return parent_map

    def _compute_file_hash(self) -> str:
        """Tính SHA-256 hash của file (dùng cho Task 6 change detection)."""
        hasher = hashlib.sha256()
        with open(self.absolute_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _empty_metadata(self, t_start: float) -> dict:
        """Metadata rỗng dùng khi parse thất bại."""
        duration_ms = (time.time() - t_start) * 1000
        size = 0
        try:
            size = os.path.getsize(self.absolute_path)
        except OSError:
            pass
        return make_metadata_event(
            file_path=self.relative_path,
            file_size_bytes=size,
            file_hash="",
            total_nodes=0,
            total_ast_edges=0,
            total_cfg_edges=0,
            total_dfg_edges=0,
            total_call_edges=0,
            parse_duration_ms=round(duration_ms, 2),
        )

    # ── INTERNAL: AST NODES ──────────────────────────────────────────────────

    def _extract_ast_nodes(
        self,
        tree: ast.AST,
        id_col: _NodeIDCollector,
        parent_map: dict[int, ast.AST],
        source_code: str,
    ) -> Generator[dict, None, None]:
        """
        Duyệt toàn bộ cây AST và sinh ra node event cho mỗi node.
        Label được phân loại chi tiết để Neo4j query thuận tiện hơn.
        """
        source_lines = source_code.splitlines()

        for node in ast.walk(tree):
            node_id   = id_col.get_id(node)
            label     = _get_label(node)
            name      = _extract_name(node)
            line      = getattr(node, "lineno",         None)
            col       = getattr(node, "col_offset",     None)
            end_line  = getattr(node, "end_lineno",     None)
            end_col   = getattr(node, "end_col_offset", None)
            scope     = _get_scope(node, parent_map)

            # Lấy đoạn code tương ứng (chỉ 1 dòng, để tránh payload quá lớn)
            snippet = None
            if line is not None and 1 <= line <= len(source_lines):
                snippet = source_lines[line - 1].strip()[:120]  # tối đa 120 ký tự

            yield make_node_event(
                node_id=node_id,
                file_path=self.relative_path,
                label=label,
                node_type=node.__class__.__name__,
                line_number=line,
                col_offset=col,
                end_lineno=end_line,
                end_col_offset=end_col,
                name=name,
                code_snippet=snippet,
                scope=scope,
            )

    # ── INTERNAL: AST EDGES ──────────────────────────────────────────────────

    def _extract_ast_edges(
        self,
        tree: ast.AST,
        id_col: _NodeIDCollector,
    ) -> Generator[dict, None, None]:
        """
        Sinh AST_CHILD edges: mỗi nút cha → các nút con trực tiếp.
        Đây là bộ xương cơ bản của CPG.
        """
        for node in ast.walk(tree):
            src_id = id_col.get_id(node)
            for child in ast.iter_child_nodes(node):
                tgt_id  = id_col.get_id(child)
                edge_id = _stable_edge_id(self.relative_path, "AST_CHILD", src_id, tgt_id)
                yield make_edge_event(
                    edge_id=edge_id,
                    file_path=self.relative_path,
                    edge_type="AST_CHILD",
                    source_id=src_id,
                    target_id=tgt_id,
                    properties={"child_type": child.__class__.__name__},
                )

    # ── INTERNAL: CFG EDGES ──────────────────────────────────────────────────

    def _extract_cfg_edges(
        self,
        tree: ast.AST,
        id_col: _NodeIDCollector,
    ) -> Generator[dict, None, None]:
        """
        Sinh CFG_NEXT edges: luồng điều khiển giữa các câu lệnh.

        Mô hình hóa:
          • Trong một block (body/orelse/handlers/finalbody):
            stmt[i] → CFG_NEXT → stmt[i+1]
          • Nút điều kiện (If/For/While) → CFG_BRANCH_TRUE → body[0]
          • Nút điều kiện               → CFG_BRANCH_FALSE → orelse[0] (nếu có)
          • Try block                   → CFG_EXCEPT → handler statement[0]
        """
        for node in ast.walk(tree):
            # ── Sequential CFG trong mọi block ───────────────────────────────
            for block_attr in ("body", "orelse", "handlers", "finalbody", "finally_body"):
                block: list = getattr(node, block_attr, [])
                if not isinstance(block, list) or len(block) < 2:
                    continue
                for i in range(len(block) - 1):
                    src_stmt = block[i]
                    tgt_stmt = block[i + 1]
                    if not (isinstance(src_stmt, ast.AST) and isinstance(tgt_stmt, ast.AST)):
                        continue
                    src_id  = id_col.get_id(src_stmt)
                    tgt_id  = id_col.get_id(tgt_stmt)
                    edge_id = _stable_edge_id(self.relative_path, "CFG_NEXT", src_id, tgt_id)
                    yield make_edge_event(
                        edge_id=edge_id,
                        file_path=self.relative_path,
                        edge_type="CFG_NEXT",
                        source_id=src_id,
                        target_id=tgt_id,
                        properties={"sequence": i},
                    )

            # ── Branching: If / For / While ───────────────────────────────────
            if isinstance(node, (ast.If, ast.For, ast.AsyncFor, ast.While)):
                cond_id = id_col.get_id(node)
                body: list = getattr(node, "body", [])
                orelse: list = getattr(node, "orelse", [])

                if body:
                    tgt_id  = id_col.get_id(body[0])
                    edge_id = _stable_edge_id(self.relative_path, "CFG_BRANCH_TRUE", cond_id, tgt_id)
                    yield make_edge_event(
                        edge_id=edge_id,
                        file_path=self.relative_path,
                        edge_type="CFG_BRANCH_TRUE",
                        source_id=cond_id,
                        target_id=tgt_id,
                    )
                if orelse:
                    tgt_id  = id_col.get_id(orelse[0])
                    edge_id = _stable_edge_id(self.relative_path, "CFG_BRANCH_FALSE", cond_id, tgt_id)
                    yield make_edge_event(
                        edge_id=edge_id,
                        file_path=self.relative_path,
                        edge_type="CFG_BRANCH_FALSE",
                        source_id=cond_id,
                        target_id=tgt_id,
                    )

            # ── Try / Except ──────────────────────────────────────────────────
            if isinstance(node, (ast.Try,)):
                try_id = id_col.get_id(node)
                for handler in getattr(node, "handlers", []):
                    h_id    = id_col.get_id(handler)
                    edge_id = _stable_edge_id(self.relative_path, "CFG_EXCEPT", try_id, h_id)
                    exc_name = getattr(handler.type, "id", "Exception") if handler.type else "Exception"
                    yield make_edge_event(
                        edge_id=edge_id,
                        file_path=self.relative_path,
                        edge_type="CFG_EXCEPT",
                        source_id=try_id,
                        target_id=h_id,
                        properties={"exception_type": exc_name},
                    )

    # ── INTERNAL: DFG EDGES ──────────────────────────────────────────────────

    def _extract_dfg_edges(
        self,
        tree: ast.AST,
        id_col: _NodeIDCollector,
    ) -> Generator[dict, None, None]:
        """
        Sinh DFG_USE edges: luồng dữ liệu từ nơi định nghĩa → nơi sử dụng biến.

        Thuật toán (intraprocedural, per-file):
          1. Quét toàn bộ AST tìm mọi Name node với ctx=Store (định nghĩa).
          2. Với mỗi biến, tìm các Name node sau đó với ctx=Load (sử dụng).
          3. Nếu cùng tên biến: sinh cạnh DFG_USE (def_node → use_node).

        Giới hạn: đây là def-use đơn giản, không phân tích scope đầy đủ.
        Đủ để minh họa DFG theo yêu cầu của Lab.
        """
        # Bước 1: Thu thập tất cả defs và uses theo tên biến
        defs: dict[str, list[ast.Name]] = {}   # varname → [Store nodes]
        uses: dict[str, list[ast.Name]] = {}   # varname → [Load nodes]

        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                varname = node.id
                if isinstance(node.ctx, ast.Store):
                    defs.setdefault(varname, []).append(node)
                elif isinstance(node.ctx, ast.Load):
                    uses.setdefault(varname, []).append(node)

        # Bước 2: Ghép def → use theo thứ tự dòng (đơn giản nhất, không scope)
        for varname, def_nodes in defs.items():
            use_nodes = uses.get(varname, [])
            for def_node in def_nodes:
                def_line = getattr(def_node, "lineno", 0)
                def_id   = id_col.get_id(def_node)
                for use_node in use_nodes:
                    use_line = getattr(use_node, "lineno", 0)
                    # Chỉ nối nếu use xuất hiện SAU def (đơn giản hóa DFG)
                    if use_line >= def_line:
                        use_id  = id_col.get_id(use_node)
                        edge_id = _stable_edge_id(self.relative_path, "DFG_USE", def_id, use_id)
                        yield make_edge_event(
                            edge_id=edge_id,
                            file_path=self.relative_path,
                            edge_type="DFG_USE",
                            source_id=def_id,
                            target_id=use_id,
                            properties={"variable_name": varname},
                        )

    # ── INTERNAL: CALL EDGES ─────────────────────────────────────────────────

    def _extract_call_edges(
        self,
        tree: ast.AST,
        id_col: _NodeIDCollector,
    ) -> Generator[dict, None, None]:
        """
        Sinh CALL edges: từ nút Call → nút FunctionDef được gọi (nếu có trong file).

        Thuật toán:
          1. Xây dựng mapping tên hàm → FunctionDef node (intraprocedural).
          2. Với mỗi ast.Call node, lấy tên hàm được gọi.
          3. Nếu tên hàm tồn tại trong mapping → sinh cạnh CALL.
          4. Nếu không tìm thấy (external call) → sinh cạnh CALL_EXTERNAL.
        """
        # Bước 1: Xây dựng mapping tên → FunctionDef trong cùng file
        func_defs: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {}
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                func_defs[node.name] = node

        # Bước 2: Precompute mapping node_python_id → enclosing FunctionDef (O(n))
        # Tránh O(n²) lookup bằng cách duyệt cây 1 lần duy nhất
        _enclosing: dict[int, ast.FunctionDef | ast.AsyncFunctionDef] = {}
        def _build_enclosing(node: ast.AST, current_func=None):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                _enclosing[id(node)] = node
                current_func = node
            elif current_func is not None:
                _enclosing[id(node)] = current_func
            for child in ast.iter_child_nodes(node):
                _build_enclosing(child, current_func)
        _build_enclosing(tree)

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue

            call_node_id = id_col.get_id(node)

            # Lấy tên hàm được gọi
            callee_name: str | None = None
            if isinstance(node.func, ast.Name):
                callee_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                callee_name = node.func.attr

            if callee_name is None:
                continue

            # Tra cứu FunctionDef bao quanh từ precomputed map (O(1))
            caller_func = _enclosing.get(id(node))

            if callee_name in func_defs:
                # Internal call: caller → callee trong cùng file
                callee_node = func_defs[callee_name]
                callee_id   = id_col.get_id(callee_node)
                src_id      = id_col.get_id(caller_func) if caller_func is not None else call_node_id
                edge_id     = _stable_edge_id(self.relative_path, "CALL", src_id, callee_id)
                yield make_edge_event(
                    edge_id=edge_id,
                    file_path=self.relative_path,
                    edge_type="CALL",
                    source_id=src_id,
                    target_id=callee_id,
                    properties={
                        "callee_name": callee_name,
                        "call_site_line": getattr(node, "lineno", None),
                        "is_external": False,
                    },
                )
            else:
                # External call: chỉ lưu metadata, không có target node
                src_id  = id_col.get_id(caller_func) if caller_func is not None else call_node_id
                edge_id = _stable_edge_id(self.relative_path, "CALL_EXTERNAL", src_id, callee_name)
                yield make_edge_event(
                    edge_id=edge_id,
                    file_path=self.relative_path,
                    edge_type="CALL_EXTERNAL",
                    source_id=src_id,
                    target_id=f"external::{callee_name}",
                    properties={
                        "callee_name": callee_name,
                        "call_site_line": getattr(node, "lineno", None),
                        "is_external": True,
                    },
                )


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT (chạy trực tiếp để kiểm tra)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import json

    if len(sys.argv) < 3:
        print("Usage: python parser_service.py <absolute_file_path> <repo_root>")
        print("Example: python parser_service.py ../lerobot/src/lerobot/__init__.py ../lerobot")
        sys.exit(1)

    file_path = os.path.abspath(sys.argv[1])
    repo_root = os.path.abspath(sys.argv[2])

    print(f"[Parser] Đang parse: {file_path}")
    parser = CPGParser(absolute_path=file_path, repo_root=repo_root)
    nodes, edges, meta, err = parser.parse()

    if err:
        print(f"[Parser] ❌ Lỗi: {err['error_type']} — {err['error_message']}")
    else:
        print(f"[Parser] ✅ Hoàn tất:")
        print(f"  - Nodes     : {meta['total_nodes']}")
        print(f"  - AST edges : {meta['total_edges']['ast']}")
        print(f"  - CFG edges : {meta['total_edges']['cfg']}")
        print(f"  - DFG edges : {meta['total_edges']['dfg']}")
        print(f"  - CALL edges: {meta['total_edges']['call']}")
        print(f"  - File hash : {meta['file_hash'][:16]}…")
        print(f"  - Duration  : {meta['parse_duration_ms']} ms")
        print("\n[Parser] Sample node:")
        print(json.dumps(nodes[0], indent=2) if nodes else "  (none)")
        print("\n[Parser] Sample edge:")
        print(json.dumps(edges[0], indent=2) if edges else "  (none)")
        print("\n[Parser] Metadata:")
        print(json.dumps(meta, indent=2))