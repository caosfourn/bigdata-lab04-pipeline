"""Helpers for Person 2: Neo4j sink statements and idempotent merge Cypher."""

from __future__ import annotations


def build_constraint_statements() -> list[str]:
    """Return Neo4j constraint statements for idempotent loading."""
    return [
        "CREATE CONSTRAINT node_id_unique IF NOT EXISTS FOR (n:CodeNode) REQUIRE n.node_id IS UNIQUE;",
        "CREATE CONSTRAINT edge_id_unique IF NOT EXISTS FOR (e:CodeEdge) REQUIRE e.edge_id IS UNIQUE;",
        "CREATE CONSTRAINT metadata_file_unique IF NOT EXISTS FOR (m:CodeFile) REQUIRE m.file_path IS UNIQUE;",
    ]


def build_node_merge_cypher(event: dict) -> str:
    """Build a MERGE-based Cypher statement for a node event."""
    props = event.get("properties", {})
    label = event.get("label", "AST_Node")
    node_id = event.get("node_id", "")
    file_path = event.get("file_path", "")
    props_payload = {
        "node_id": node_id,
        "file_path": file_path,
        "type": props.get("type"),
        "line_number": props.get("line_number"),
        "col_offset": props.get("col_offset"),
        "end_lineno": props.get("end_lineno"),
        "end_col_offset": props.get("end_col_offset"),
        "name": props.get("name"),
        "scope": props.get("scope"),
        "code_snippet": props.get("code_snippet"),
        "event_time": event.get("event_time"),
    }
    payload = ", ".join(f"{k}: ${k}" for k in props_payload)
    return (
        f"MERGE (n:{label} {{node_id: $node_id}}) "
        f"SET n += {{{payload}}};"
    )


def build_edge_merge_cypher(event: dict) -> str:
    """Build a MERGE-based Cypher statement for an edge event."""
    props = event.get("properties", {})
    edge_id = event.get("edge_id", "")
    source_id = event.get("source_id", "")
    target_id = event.get("target_id", "")
    props_payload = {
        "edge_id": edge_id,
        "type": event.get("type"),
        "source_id": source_id,
        "target_id": target_id,
        "file_path": event.get("file_path"),
        "event_time": event.get("event_time"),
        **props,
    }
    payload = ", ".join(f"{k}: ${k}" for k in props_payload)
    return (
        f"MERGE (e:CodeEdge {{edge_id: $edge_id}}) "
        f"SET e += {{{payload}}};"
    )


def build_metadata_merge_cypher(event: dict) -> str:
    """Build a MERGE-based Cypher statement for metadata events."""
    props_payload = {
        "file_path": event.get("file_path"),
        "file_hash": event.get("file_hash"),
        "file_size_bytes": event.get("file_size_bytes"),
        "total_nodes": event.get("total_nodes"),
        "total_edges": event.get("total_edges"),
        "parser_version": event.get("parser_version"),
        "parse_duration_ms": event.get("parse_duration_ms"),
        "event_time": event.get("event_time"),
    }
    payload = ", ".join(f"{k}: ${k}" for k in props_payload)
    return (
        f"MERGE (m:CodeFile {{file_path: $file_path}}) "
        f"SET m += {{{payload}}};"
    )
