"""Capture Neo4j and MongoDB evidence for the Task 6 replay experiment."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone


def collect_neo4j(uri: str, user: str, password: str, file_path: str) -> dict:
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            node_count = session.run(
                "MATCH (n:CodeNode {file_path: $path}) RETURN count(n) AS count",
                path=file_path,
            ).single()["count"]
            edge_count = session.run(
                "MATCH ()-[e:CPG_EDGE {file_path: $path}]->() RETURN count(e) AS count",
                path=file_path,
            ).single()["count"]
            duplicate_nodes = session.run(
                """
                MATCH (n:CodeNode {file_path: $path})
                WITH n.node_id AS id, count(*) AS copies
                WHERE copies > 1
                RETURN count(*) AS count
                """,
                path=file_path,
            ).single()["count"]
            edge_types = {
                row["type"]: row["count"]
                for row in session.run(
                    """
                    MATCH ()-[e:CPG_EDGE {file_path: $path}]->()
                    RETURN e.type AS type, count(e) AS count
                    ORDER BY type
                    """,
                    path=file_path,
                )
            }
    finally:
        driver.close()

    return {
        "nodes": node_count,
        "edges": edge_count,
        "duplicate_node_ids": duplicate_nodes,
        "edge_types": edge_types,
    }


def collect_mongodb(uri: str, database: str, collection: str, file_path: str) -> dict:
    from pymongo import MongoClient

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        documents = list(client[database][collection].find({"file_path": file_path}))
        for document in documents:
            document["_id"] = str(document["_id"])
    finally:
        client.close()

    return {"document_count": len(documents), "documents": documents}


def main() -> int:
    parser = argparse.ArgumentParser(description="Capture Task 6 database state as JSON.")
    parser.add_argument("file_path", help="Repository-relative path stored in events")
    parser.add_argument("--neo4j-uri", default="bolt://localhost:7687")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", default="cpg-password")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    parser.add_argument("--mongo-db", default="cpg")
    parser.add_argument("--mongo-collection", default="metadata")
    parser.add_argument("--output", help="Optional JSON output path")
    args = parser.parse_args()

    evidence = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "file_path": args.file_path,
        "neo4j": collect_neo4j(
            args.neo4j_uri, args.neo4j_user, args.neo4j_password, args.file_path
        ),
        "mongodb": collect_mongodb(
            args.mongo_uri, args.mongo_db, args.mongo_collection, args.file_path
        ),
    }
    rendered = json.dumps(evidence, ensure_ascii=False, indent=2, default=str)
    print(rendered)
    if args.output:
        from pathlib import Path

        Path(args.output).write_text(rendered + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
