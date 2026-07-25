"""Event builders shared by the parser and Kafka publisher.

Kafka topic layout:
  - cpg.nodes      : AST node events
  - cpg.edges      : AST/CFG/DFG/CALL edge events
  - cpg.metadata   : Source file metadata events
  - cpg.errors     : Parser error events
"""

from __future__ import annotations

import datetime

# ─────────────────────────────────────────────────────────────────────────────
# KAFKA TOPIC NAMES
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
    """Return a timezone-aware ISO 8601 timestamp in UTC."""
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


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
    repo_id: str = "",
    file_id: str = "",
    file_hash: str = "",
    parse_status: str = "success",
) -> dict:
    """
    Build a complete node event for the ``cpg.nodes`` topic.

    Args:
        node_id       : Stable deterministic identifier.
        file_path     : Source path relative to the repository root.
        label         : Neo4j label, such as ``FunctionDef`` or ``ClassDef``.
        node_type     : AST class name, such as ``FunctionDef``, ``If``, or ``Call``.
        line_number   : One-based starting line, or None when unavailable.
        col_offset    : Zero-based starting column, or None when unavailable.
        end_lineno    : Ending line, or None when unavailable.
        end_col_offset: Ending column, or None when unavailable.
        name          : Function, class, or variable name when available.
        code_snippet  : Optional source excerpt for Neo4j search.
        scope         : Name of the enclosing function or class.
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "event_time":     _now_iso(),
        "topic":          TOPIC_NODES,
        "repo_id":        repo_id,
        "file_id":        file_id,
        "file_path":      file_path,
        "file_hash":      file_hash,
        "parse_status":   parse_status,
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
    repo_id: str = "",
    file_id: str = "",
    file_hash: str = "",
    parse_status: str = "success",
) -> dict:
    """
    Build a complete edge event for the ``cpg.edges`` topic.

    Args:
        edge_id    : Stable deterministic edge identifier.
        file_path  : Source path relative to the repository root.
        edge_type  : ``AST_CHILD``, ``CFG_NEXT``, ``DFG_USE``, or ``CALL``.
        source_id  : Source node identifier.
        target_id  : Target node identifier.
        properties : Optional edge-specific metadata.
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "event_time":     _now_iso(),
        "topic":          TOPIC_EDGES,
        "repo_id":        repo_id,
        "file_id":        file_id,
        "file_path":      file_path,
        "file_hash":      file_hash,
        "parse_status":   parse_status,
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
    repo_id: str = "",
    file_id: str = "",
    parse_status: str = "success",
) -> dict:
    """
    Build a complete file event for the ``cpg.metadata`` topic.

    Args:
        file_path        : Source path relative to the repository root.
        file_size_bytes  : File size in bytes.
        file_hash        : SHA-256 content hash used for revision detection.
        total_nodes      : Number of extracted nodes.
        total_ast_edges  : Number of ``AST_CHILD`` edges.
        total_cfg_edges  : Number of ``CFG_NEXT`` edges.
        total_dfg_edges  : Number of ``DFG_USE`` edges.
        total_call_edges : Number of ``CALL`` edges.
        parser_version   : Parser implementation name.
        parse_duration_ms: Parse time in milliseconds.
    """
    return {
        "schema_version":    SCHEMA_VERSION,
        "event_time":        _now_iso(),
        "topic":             TOPIC_METADATA,
        "repo_id":           repo_id,
        "file_id":           file_id,
        "file_path":         file_path,
        "file_size_bytes":   file_size_bytes,
        "file_hash":         file_hash,
        "parse_status":      parse_status,
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
    repo_id: str = "",
    file_id: str = "",
    file_hash: str = "",
    parse_status: str = "error",
) -> dict:
    """
    Build an error event for the ``cpg.errors`` topic.

    Args:
        file_path     : Relative path of the file that failed.
        error_type    : Exception class, such as ``SyntaxError``.
        error_message : Complete exception message.
        line_number   : Error line when available.
        col_offset    : Error column when available.
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "event_time":     _now_iso(),
        "topic":          TOPIC_ERRORS,
        "repo_id":        repo_id,
        "file_id":        file_id,
        "file_path":      file_path,
        "file_hash":      file_hash,
        "parse_status":   parse_status,
        "error_type":     error_type,
        "error_message":  error_message,
        "line_number":    line_number,
        "col_offset":     col_offset,
    }
