"""Incremental Code Property Graph parser.

The service accepts one Python file, extracts AST nodes plus AST, CFG, DFG, and
call edges, assigns deterministic identifiers, and returns events that follow
the shared Kafka schemas. It uses Python's standard-library ``ast`` module and
requires no parser-specific dependency.
"""

from __future__ import annotations

import ast
import hashlib
import os
import posixpath
import time
from dataclasses import dataclass
from typing import Generator

try:  # `python -m src.parser_service`
    from .schemas import (
        make_node_event,
        make_edge_event,
        make_metadata_event,
        make_error_event,
    )
except ImportError:  # `python src/parser_service.py`
    from schemas import (  # type: ignore[no-redef]
        make_node_event,
        make_edge_event,
        make_metadata_event,
        make_error_event,
    )

SCHEMA_VERSION = "1.0"


# ─────────────────────────────────────────────────────────────────────────────
# STABLE ID GENERATION
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_relative_path(relative_path: str) -> str:
    """Normalize a relative path to POSIX form for cross-platform IDs."""
    normalized = posixpath.normpath(relative_path.replace("\\", "/"))
    if normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def _stable_file_id(repo_id: str, relative_path: str) -> str:
    """Return a stable file identifier that is independent of file content."""
    normalized_path = _normalize_relative_path(relative_path)
    raw = f"repo={repo_id}\x1fpath={normalized_path}"
    return "file_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _stable_node_id(
    relative_path: str,
    node: ast.AST,
    structural_path: str | None = None,
    *,
    repo_id: str = "",
    file_id: str | None = None,
) -> str:
    """
    Return a deterministic identifier for an AST node occurrence.

    A structural path such as ``root.body[0].value`` distinguishes singleton
    AST objects that have no line or column information. The full SHA-256 hash
    is retained. ``structural_path=None`` remains as a compatibility fallback;
    the parser always supplies an actual structural path.
    """
    normalized_path = _normalize_relative_path(relative_path)
    resolved_file_id = file_id or _stable_file_id(repo_id, normalized_path)
    node_type = node.__class__.__name__
    if structural_path is None:
        line = getattr(node, "lineno", 0)
        col = getattr(node, "col_offset", 0)
        structural_path = f"legacy@L{line}C{col}"
    raw = (
        f"repo={repo_id}\x1ffile={resolved_file_id}\x1fpath={normalized_path}"
        f"\x1fast_path={structural_path}\x1ftype={node_type}"
    )
    return "node_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _stable_edge_id(
    relative_path: str,
    edge_type: str,
    src_id: str,
    tgt_id: str,
    *,
    repo_id: str = "",
    file_id: str | None = None,
    discriminator: str = "",
) -> str:
    """
    Return a deterministic edge identifier.

    The same source, target, edge type, and occurrence discriminator produce
    the same identifier, so Neo4j ``MERGE`` does not create duplicates.
    """
    normalized_path = _normalize_relative_path(relative_path)
    resolved_file_id = file_id or _stable_file_id(repo_id, normalized_path)
    raw = (
        f"repo={repo_id}\x1ffile={resolved_file_id}\x1fpath={normalized_path}"
        f"\x1ftype={edge_type}\x1fsource={src_id}\x1ftarget={tgt_id}"
        f"\x1foccurrence={discriminator}"
    )
    return "edge_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
# AST VISITOR — collect all node IDs in one O(n) traversal
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class _ASTOccurrence:
    """One AST occurrence; a singleton object may occur at multiple paths."""

    node: ast.AST
    structural_path: str
    parent_path: str | None
    scope: str | None


class _NodeIDCollector:
    """
    Traverse the AST once to build the node ID mappings. Edge extraction reuses
    these mappings without parsing the source or hashing nodes again.
    """

    def __init__(self, relative_path: str, repo_id: str = "", file_id: str | None = None):
        self.relative_path = _normalize_relative_path(relative_path)
        self.repo_id = repo_id
        self.file_id = file_id or _stable_file_id(repo_id, self.relative_path)
        self.occurrences: list[_ASTOccurrence] = []
        self._path_id_map: dict[str, str] = {}
        self._paths_by_object: dict[int, list[str]] = {}

    def visit(self, node: ast.AST):
        """Collect occurrences by field/index path instead of object identity."""
        self.occurrences.clear()
        self._path_id_map.clear()
        self._paths_by_object.clear()
        self._collect(node, "root", parent_path=None, enclosing_scope=None)
        return node

    def _collect(
        self,
        node: ast.AST,
        structural_path: str,
        parent_path: str | None,
        enclosing_scope: str | None,
    ) -> None:
        occurrence = _ASTOccurrence(
            node=node,
            structural_path=structural_path,
            parent_path=parent_path,
            scope=enclosing_scope,
        )
        self.occurrences.append(occurrence)
        self._path_id_map[structural_path] = _stable_node_id(
            self.relative_path,
            node,
            structural_path,
            repo_id=self.repo_id,
            file_id=self.file_id,
        )
        self._paths_by_object.setdefault(id(node), []).append(structural_path)

        child_scope = enclosing_scope
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            child_scope = node.name

        for field_name, value in ast.iter_fields(node):
            if isinstance(value, ast.AST):
                child_path = f"{structural_path}.{field_name}"
                self._collect(value, child_path, structural_path, child_scope)
            elif isinstance(value, list):
                for index, item in enumerate(value):
                    if isinstance(item, ast.AST):
                        child_path = f"{structural_path}.{field_name}[{index}]"
                        self._collect(item, child_path, structural_path, child_scope)

    def get_id(self, node: ast.AST, structural_path: str | None = None) -> str:
        """Return the stable ID computed during the initial traversal."""
        if structural_path is not None:
            return self._path_id_map[structural_path]
        paths = self._paths_by_object.get(id(node), [])
        if paths:
            # Semantic nodes (Name/Call/stmt/definition) have exactly one path.
            # Singleton operator/context nodes are consumed through occurrences.
            return self._path_id_map[paths[0]]
        return _stable_node_id(
            self.relative_path,
            node,
            repo_id=self.repo_id,
            file_id=self.file_id,
        )

    def get_id_by_path(self, structural_path: str) -> str:
        """Return the exact ID of an AST occurrence."""
        return self._path_id_map[structural_path]


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACTOR HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _extract_name(node: ast.AST) -> str | None:
    """Return a function, class, variable, attribute, or argument name."""
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
    """Choose a useful Neo4j label for an AST node."""
    class_name = node.__class__.__name__
    # Keep commonly queried node types as dedicated labels.
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
    """Find the nearest enclosing function or class."""
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
    Parse one Python file and return node, edge, metadata, and error events.

    Extractors yield individual events to keep each stage bounded by one file.
    ``parse`` materializes the events for tests, notebooks, and publishing.
    """

    def __init__(self, absolute_path: str, repo_root: str, repo_id: str | None = None):
        """
        Args:
            absolute_path: Absolute path of the Python file to parse.
            repo_root    : Absolute path of the cloned repository root.
            repo_id      : Stable logical repository name. Pass an explicit
                           value such as ``huggingface/lerobot`` when clones may
                           use different local directory names.
        """
        self.absolute_path = os.path.abspath(absolute_path)
        self.repo_root = os.path.abspath(repo_root)
        self.repo_id = repo_id if repo_id is not None else (
            os.path.basename(os.path.normpath(self.repo_root)) or "repository"
        )
        # Relative paths are shared event keys and always use forward slashes.
        self.relative_path = _normalize_relative_path(
            os.path.relpath(self.absolute_path, self.repo_root)
        )
        self.file_id = _stable_file_id(self.repo_id, self.relative_path)
        self.file_hash = ""

    # ── PUBLIC API ────────────────────────────────────────────────────────────

    def parse(self) -> tuple[list, list, dict, dict | None]:
        """
        Parse the file and return ``(nodes, edges, metadata, error_event)``.

        Returns:
            nodes      : Node events for ``cpg.nodes``.
            edges      : Edge events for ``cpg.edges``.
            metadata   : File event for ``cpg.metadata``.
            error_event: Event for ``cpg.errors``, or None after a successful parse.
        """
        t_start = time.time()
        # A parser instance may be reused after its file changes or is deleted.
        self.file_hash = ""

        # Read once so the content hash matches the parsed bytes exactly.
        try:
            with open(self.absolute_path, "rb") as f:
                source_bytes = f.read()
        except OSError as e:
            err = make_error_event(
                file_path=self.relative_path,
                error_type=type(e).__name__,
                error_message=str(e),
                **self._event_context(parse_status="error"),
            )
            meta = self._empty_metadata(t_start, parse_status="error")
            return [], [], meta, err

        self.file_hash = hashlib.sha256(source_bytes).hexdigest()
        try:
            source_code = source_bytes.decode("utf-8")
        except UnicodeDecodeError as e:
            err = make_error_event(
                file_path=self.relative_path,
                error_type=type(e).__name__,
                error_message=str(e),
                **self._event_context(parse_status="error"),
            )
            meta = self._empty_metadata(t_start, parse_status="error")
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
                **self._event_context(parse_status="error"),
            )
            meta = self._empty_metadata(t_start, parse_status="error")
            return [], [], meta, err

        # Build parent and ID lookup structures.
        parent_map   = self._build_parent_map(tree)
        id_collector = _NodeIDCollector(
            self.relative_path,
            repo_id=self.repo_id,
            file_id=self.file_id,
        )
        id_collector.visit(tree)

        # Extract all CPG components.
        node_events = list(self._extract_ast_nodes(tree, id_collector, parent_map, source_code))
        ast_edges   = list(self._extract_ast_edges(tree, id_collector))
        cfg_edges   = list(self._extract_cfg_edges(tree, id_collector))
        dfg_edges   = list(self._extract_dfg_edges(tree, id_collector))
        call_edges  = list(self._extract_call_edges(tree, id_collector))
        edge_events = ast_edges + cfg_edges + dfg_edges + call_edges

        duration_ms = (time.time() - t_start) * 1000

        metadata = make_metadata_event(
            file_path=self.relative_path,
            file_size_bytes=len(source_bytes),
            file_hash=self.file_hash,
            total_nodes=len(node_events),
            total_ast_edges=len(ast_edges),
            total_cfg_edges=len(cfg_edges),
            total_dfg_edges=len(dfg_edges),
            total_call_edges=len(call_edges),
            parser_version="ast-stdlib-3.x",
            parse_duration_ms=round(duration_ms, 2),
            repo_id=self.repo_id,
            file_id=self.file_id,
            parse_status="success",
        )

        return node_events, edge_events, metadata, None

    # ── INTERNAL: SUPPORT STRUCTURES ─────────────────────────────────────────

    def _build_parent_map(self, tree: ast.AST) -> dict[int, ast.AST]:
        """Build a child-to-parent map for scope lookup."""
        parent_map: dict[int, ast.AST] = {}
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                parent_map[id(child)] = node
        return parent_map

    def _compute_file_hash(self) -> str:
        """Compute the SHA-256 content hash used for revision detection."""
        hasher = hashlib.sha256()
        with open(self.absolute_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _event_context(self, parse_status: str = "success") -> dict:
        """Return fields shared by node, edge, and error events."""
        return {
            "repo_id": self.repo_id,
            "file_id": self.file_id,
            "file_hash": self.file_hash,
            "parse_status": parse_status,
        }

    def _edge_id(
        self,
        edge_type: str,
        source_id: str,
        target_id: str,
        discriminator: str = "",
    ) -> str:
        """Create an edge ID in the current repository and file context."""
        return _stable_edge_id(
            self.relative_path,
            edge_type,
            source_id,
            target_id,
            repo_id=self.repo_id,
            file_id=self.file_id,
            discriminator=discriminator,
        )

    def _empty_metadata(self, t_start: float, parse_status: str = "error") -> dict:
        """Return an empty metadata record after a parse failure."""
        duration_ms = (time.time() - t_start) * 1000
        size = 0
        try:
            size = os.path.getsize(self.absolute_path)
        except OSError:
            pass
        return make_metadata_event(
            file_path=self.relative_path,
            file_size_bytes=size,
            file_hash=self.file_hash,
            total_nodes=0,
            total_ast_edges=0,
            total_cfg_edges=0,
            total_dfg_edges=0,
            total_call_edges=0,
            parse_duration_ms=round(duration_ms, 2),
            repo_id=self.repo_id,
            file_id=self.file_id,
            parse_status=parse_status,
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
        Yield one node event for each AST occurrence.
        """
        source_lines = source_code.splitlines()

        # Give each structural occurrence its own ID, including singleton
        # objects such as Load and Add that CPython may reuse.
        for occurrence in id_col.occurrences:
            node = occurrence.node
            node_id = id_col.get_id_by_path(occurrence.structural_path)
            label     = _get_label(node)
            name      = _extract_name(node)
            line      = getattr(node, "lineno",         None)
            col       = getattr(node, "col_offset",     None)
            end_line  = getattr(node, "end_lineno",     None)
            end_col   = getattr(node, "end_col_offset", None)
            scope = occurrence.scope

            # Keep at most one short source line in each event.
            snippet = None
            if line is not None and 1 <= line <= len(source_lines):
                snippet = source_lines[line - 1].strip()[:120]

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
                **self._event_context(),
            )

    # ── INTERNAL: AST EDGES ──────────────────────────────────────────────────

    def _extract_ast_edges(
        self,
        tree: ast.AST,
        id_col: _NodeIDCollector,
    ) -> Generator[dict, None, None]:
        """
        Yield an ``AST_CHILD`` edge from each parent to every direct child.
        """
        for occurrence in id_col.occurrences:
            if occurrence.parent_path is None:
                continue
            src_id = id_col.get_id_by_path(occurrence.parent_path)
            tgt_id = id_col.get_id_by_path(occurrence.structural_path)
            edge_id = self._edge_id("AST_CHILD", src_id, tgt_id)
            yield make_edge_event(
                edge_id=edge_id,
                file_path=self.relative_path,
                edge_type="AST_CHILD",
                source_id=src_id,
                target_id=tgt_id,
                properties={"child_type": occurrence.node.__class__.__name__},
                **self._event_context(),
            )

    # ── INTERNAL: CFG EDGES ──────────────────────────────────────────────────

    def _extract_cfg_edges(
        self,
        tree: ast.AST,
        id_col: _NodeIDCollector,
    ) -> Generator[dict, None, None]:
        """
        Yield control-flow edges between statements.

        Sequential statements use ``CFG_NEXT``. Conditional and loop bodies
        use ``CFG_BRANCH_TRUE`` and ``CFG_BRANCH_FALSE``. Exception handlers
        are connected with ``CFG_EXCEPT``.
        """
        for node in ast.walk(tree):
            # Sequential control flow inside each statement block.
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
                    edge_id = self._edge_id("CFG_NEXT", src_id, tgt_id)
                    yield make_edge_event(
                        edge_id=edge_id,
                        file_path=self.relative_path,
                        edge_type="CFG_NEXT",
                        source_id=src_id,
                        target_id=tgt_id,
                        properties={"sequence": i},
                        **self._event_context(),
                    )

            # ── Branching: If / For / While ───────────────────────────────────
            if isinstance(node, (ast.If, ast.For, ast.AsyncFor, ast.While)):
                cond_id = id_col.get_id(node)
                body: list = getattr(node, "body", [])
                orelse: list = getattr(node, "orelse", [])

                if body:
                    tgt_id  = id_col.get_id(body[0])
                    edge_id = self._edge_id("CFG_BRANCH_TRUE", cond_id, tgt_id)
                    yield make_edge_event(
                        edge_id=edge_id,
                        file_path=self.relative_path,
                        edge_type="CFG_BRANCH_TRUE",
                        source_id=cond_id,
                        target_id=tgt_id,
                        **self._event_context(),
                    )
                if orelse:
                    tgt_id  = id_col.get_id(orelse[0])
                    edge_id = self._edge_id("CFG_BRANCH_FALSE", cond_id, tgt_id)
                    yield make_edge_event(
                        edge_id=edge_id,
                        file_path=self.relative_path,
                        edge_type="CFG_BRANCH_FALSE",
                        source_id=cond_id,
                        target_id=tgt_id,
                        **self._event_context(),
                    )

            # ── Try / Except ──────────────────────────────────────────────────
            if isinstance(node, (ast.Try,)):
                try_id = id_col.get_id(node)
                for handler in getattr(node, "handlers", []):
                    h_id    = id_col.get_id(handler)
                    edge_id = self._edge_id("CFG_EXCEPT", try_id, h_id)
                    exc_name = getattr(handler.type, "id", "Exception") if handler.type else "Exception"
                    yield make_edge_event(
                        edge_id=edge_id,
                        file_path=self.relative_path,
                        edge_type="CFG_EXCEPT",
                        source_id=try_id,
                        target_id=h_id,
                        properties={"exception_type": exc_name},
                        **self._event_context(),
                    )

    # ── INTERNAL: DFG EDGES ──────────────────────────────────────────────────

    def _extract_dfg_edges(
        self,
        tree: ast.AST,
        id_col: _NodeIDCollector,
    ) -> Generator[dict, None, None]:
        """
        Yield simple intrafile definition-to-use edges.

        Each ``Name`` with ``Store`` context is matched to later ``Name`` nodes
        with ``Load`` context and the same identifier. This is a lightweight
        approximation and does not perform complete scope analysis.
        """
        # Collect definitions and uses by variable name.
        defs: dict[str, list[ast.Name]] = {}   # varname → [Store nodes]
        uses: dict[str, list[ast.Name]] = {}   # varname → [Load nodes]

        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                varname = node.id
                if isinstance(node.ctx, ast.Store):
                    defs.setdefault(varname, []).append(node)
                elif isinstance(node.ctx, ast.Load):
                    uses.setdefault(varname, []).append(node)

        # Match each definition to later uses in source order.
        for varname, def_nodes in defs.items():
            use_nodes = uses.get(varname, [])
            for def_node in def_nodes:
                def_line = getattr(def_node, "lineno", 0)
                def_id   = id_col.get_id(def_node)
                for use_node in use_nodes:
                    use_line = getattr(use_node, "lineno", 0)
                    # A use must appear at or after its definition.
                    if use_line >= def_line:
                        use_id  = id_col.get_id(use_node)
                        edge_id = self._edge_id("DFG_USE", def_id, use_id)
                        yield make_edge_event(
                            edge_id=edge_id,
                            file_path=self.relative_path,
                            edge_type="DFG_USE",
                            source_id=def_id,
                            target_id=use_id,
                            properties={"variable_name": varname},
                            **self._event_context(),
                        )

    # ── INTERNAL: CALL EDGES ─────────────────────────────────────────────────

    def _extract_call_edges(
        self,
        tree: ast.AST,
        id_col: _NodeIDCollector,
    ) -> Generator[dict, None, None]:
        """
        Yield call edges for internal and external function calls.

        Local function definitions become ``CALL`` targets. Unresolved names
        use deterministic ``external::<name>`` targets on ``CALL_EXTERNAL``
        edges.
        """
        # Index local function definitions by name.
        func_defs: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {}
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                func_defs[node.name] = node

        # Build the enclosing-function lookup once in O(n).
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

            # Resolve the called name when possible.
            callee_name: str | None = None
            if isinstance(node.func, ast.Name):
                callee_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                callee_name = node.func.attr

            if callee_name is None:
                continue

            # Read the enclosing function from the precomputed map in O(1).
            caller_func = _enclosing.get(id(node))

            if callee_name in func_defs:
                # Internal call from the enclosing function to a local definition.
                callee_node = func_defs[callee_name]
                callee_id   = id_col.get_id(callee_node)
                src_id      = id_col.get_id(caller_func) if caller_func is not None else call_node_id
                edge_id = self._edge_id(
                    "CALL",
                    src_id,
                    callee_id,
                    discriminator=call_node_id,
                )
                yield make_edge_event(
                    edge_id=edge_id,
                    file_path=self.relative_path,
                    edge_type="CALL",
                    source_id=src_id,
                    target_id=callee_id,
                    properties={
                        "callee_name": callee_name,
                        "call_site_id": call_node_id,
                        "call_site_line": getattr(node, "lineno", None),
                        "is_external": False,
                    },
                    **self._event_context(),
                )
            else:
                # External calls use a deterministic placeholder target.
                src_id  = id_col.get_id(caller_func) if caller_func is not None else call_node_id
                target_id = f"external::{callee_name}"
                edge_id = self._edge_id(
                    "CALL_EXTERNAL",
                    src_id,
                    target_id,
                    discriminator=call_node_id,
                )
                yield make_edge_event(
                    edge_id=edge_id,
                    file_path=self.relative_path,
                    edge_type="CALL_EXTERNAL",
                    source_id=src_id,
                    target_id=target_id,
                    properties={
                        "callee_name": callee_name,
                        "call_site_id": call_node_id,
                        "call_site_line": getattr(node, "lineno", None),
                        "is_external": True,
                    },
                    **self._event_context(),
                )


# ─────────────────────────────────────────────────────────────────────────────
# COMMAND-LINE ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import json
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    if len(sys.argv) < 3:
        print("Usage: python parser_service.py <absolute_file_path> <repo_root>")
        print("Example: python parser_service.py ../lerobot/src/lerobot/__init__.py ../lerobot")
        sys.exit(1)

    file_path = os.path.abspath(sys.argv[1])
    repo_root = os.path.abspath(sys.argv[2])

    print(f"[Parser] Parsing: {file_path}")
    parser = CPGParser(absolute_path=file_path, repo_root=repo_root)
    nodes, edges, meta, err = parser.parse()

    if err:
        print(f"[Parser] Error: {err['error_type']} — {err['error_message']}")
    else:
        print("[Parser] Complete:")
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
