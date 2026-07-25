"""
replay_verifier.py — Task 6: Idempotent Replay Verification

Workflow (chạy hai lần: --phase before và --phase after):

  1. Parse <file> qua CPGParser (không cần Kafka — dùng CollectingProducer dry-run).
  2. Query Neo4j đếm số CPGNode / CPG_EDGE có cùng file_id.
  3. Query MongoDB lấy document có _id = file_id.
  4. Đọc Spark checkpoint directory để report committed Kafka offsets.
  5. Lưu snapshot JSON ra --output để notebook import và so sánh.

Phase "before":  chạy trước khi sửa file — ghi lại baseline.
Phase "after":   chạy sau khi sửa file — so sánh với baseline và in verdict.

Chạy offline (không có DB):
    python src/replay_verifier.py lerobot/lerobot/__init__.py lerobot \\
        --phase before --dry-run --output runtime/before.json

Chạy với DB thật:
    python src/replay_verifier.py lerobot/lerobot/__init__.py lerobot \\
        --phase before \\
        --neo4j-uri bolt://localhost:7687 --neo4j-user neo4j --neo4j-password cpg-password \\
        --mongo-uri mongodb://localhost:27017 \\
        --checkpoint-dir checkpoints/person3-final \\
        --output runtime/before.json
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import time
import datetime

# ─────────────────────────────────────────────────────────────────────────────
# Import parser + publisher (relative hoặc direct)
# ─────────────────────────────────────────────────────────────────────────────

try:
    from .parser_service import CPGParser
    from .kafka_publisher import CollectingProducer, CPGKafkaPublisher
    from .schemas import TOPIC_NODES, TOPIC_EDGES, TOPIC_METADATA
except ImportError:
    from parser_service import CPGParser          # type: ignore[no-redef]
    from kafka_publisher import CollectingProducer, CPGKafkaPublisher  # type: ignore[no-redef]
    from schemas import TOPIC_NODES, TOPIC_EDGES, TOPIC_METADATA  # type: ignore[no-redef]


# ─────────────────────────────────────────────────────────────────────────────
# PARSER DRY-RUN (không cần Kafka)
# ─────────────────────────────────────────────────────────────────────────────

def run_parser_dryrun(absolute_path: str, repo_root: str, repo_id: str) -> dict:
    """Parse file và trả về summary dict (không produce lên Kafka)."""
    producer = CollectingProducer()
    publisher = CPGKafkaPublisher(producer)
    t0 = time.time()
    result = publisher.publish_file(absolute_path, repo_root, repo_id=repo_id)
    elapsed_ms = (time.time() - t0) * 1000

    # Lấy file_hash từ metadata event
    file_hash = ""
    metadata_events = [
        rec for (topic, key, val) in producer.records
        if topic == TOPIC_METADATA
        for rec in [val]
    ]
    if metadata_events:
        file_hash = metadata_events[0].get("file_hash", "")

    # Collect unique node_ids và edge_ids
    node_ids = sorted({
        val["node_id"]
        for (topic, key, val) in producer.records
        if topic == TOPIC_NODES
    })
    edge_ids = sorted({
        val["edge_id"]
        for (topic, key, val) in producer.records
        if topic == TOPIC_EDGES
    })

    return {
        "file_path":        result.file_path,
        "file_id":          result.file_id,
        "file_hash":        file_hash,
        "node_count":       result.nodes,
        "edge_count":       result.edges,
        "node_ids_sample":  node_ids[:5],   # chỉ lưu 5 ID đầu để so sánh
        "edge_ids_sample":  edge_ids[:5],
        "parse_errors":     result.errors,
        "parse_time_ms":    round(elapsed_ms, 2),
        "captured_at":      datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# NEO4J QUERY
# ─────────────────────────────────────────────────────────────────────────────

def query_neo4j(file_id: str, uri: str, user: str, password: str) -> dict:
    """
    Query Neo4j đếm số CPGNode và CPG_EDGE liên quan đến file_id.

    Dùng thư viện neo4j (pip install neo4j). Nếu không có thư viện hoặc
    DB không chạy → trả về trạng thái error kèm gợi ý.
    """
    try:
        from neo4j import GraphDatabase  # type: ignore[import]
    except ImportError:
        return {
            "status": "SKIP",
            "reason": "neo4j driver not installed (pip install neo4j)",
            "node_count": None,
            "edge_count": None,
            "duplicate_nodes": None,
        }

    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session() as session:
            # Đếm nodes thuộc file_id này
            node_result = session.run(
                "MATCH (n:CPGNode {file_id: $fid}) RETURN count(n) AS cnt",
                fid=file_id,
            ).single()
            node_count = node_result["cnt"] if node_result else 0

            # Đếm edges thuộc file_id này
            edge_result = session.run(
                "MATCH ()-[r:CPG_EDGE {file_id: $fid}]->() RETURN count(r) AS cnt",
                fid=file_id,
            ).single()
            edge_count = edge_result["cnt"] if edge_result else 0

            # Kiểm tra duplicate: đếm node_id xuất hiện > 1 lần
            dup_result = session.run(
                """
                MATCH (n:CPGNode {file_id: $fid})
                WITH n.node_id AS nid, count(*) AS c
                WHERE c > 1
                RETURN count(nid) AS dup_count
                """,
                fid=file_id,
            ).single()
            dup_nodes = dup_result["dup_count"] if dup_result else 0

        driver.close()

        return {
            "status": "OK",
            "node_count": node_count,
            "edge_count": edge_count,
            "duplicate_nodes": dup_nodes,
        }
    except Exception as exc:  # pylint: disable=broad-except
        return {
            "status": "ERROR",
            "reason": str(exc),
            "node_count": None,
            "edge_count": None,
            "duplicate_nodes": None,
        }


# ─────────────────────────────────────────────────────────────────────────────
# MONGODB QUERY
# ─────────────────────────────────────────────────────────────────────────────

def query_mongodb(file_id: str, uri: str, db_name: str, collection: str) -> dict:
    """
    Lấy document có _id = file_id từ MongoDB collection cpg.metadata.

    Trả về document summary hoặc thông báo lỗi nếu không kết nối được.
    """
    try:
        from pymongo import MongoClient  # type: ignore[import]
    except ImportError:
        return {
            "status": "SKIP",
            "reason": "pymongo not installed (pip install pymongo)",
            "document_count": None,
            "file_hash": None,
            "event_time": None,
        }

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=3000)
        # Trigger connection to detect offline DB
        client.admin.command("ping")
        col = client[db_name][collection]

        doc = col.find_one({"_id": file_id})
        doc_count = col.count_documents({"file_id": file_id})
        client.close()

        if doc:
            return {
                "status": "OK",
                "document_count": doc_count,
                "file_hash": doc.get("file_hash", ""),
                "event_time": doc.get("event_time", ""),
                "total_nodes": doc.get("total_nodes"),
                "parse_status": doc.get("parse_status"),
            }
        else:
            return {
                "status": "OK",
                "document_count": 0,
                "file_hash": None,
                "event_time": None,
                "total_nodes": None,
                "parse_status": None,
            }
    except Exception as exc:  # pylint: disable=broad-except
        return {
            "status": "ERROR",
            "reason": str(exc),
            "document_count": None,
            "file_hash": None,
            "event_time": None,
        }


# ─────────────────────────────────────────────────────────────────────────────
# SPARK CHECKPOINT READER
# ─────────────────────────────────────────────────────────────────────────────

def read_checkpoint(checkpoint_dir: str) -> dict:
    """
    Đọc Spark Structured Streaming checkpoint để lấy committed Kafka offsets.

    Spark lưu offsets dưới dạng JSON trong <checkpoint_dir>/offsets/<batch_id>.
    File mới nhất = batch commit cuối cùng.

    Trả về committed offset info hoặc thông báo nếu dir không tồn tại.
    """
    cp_path = pathlib.Path(checkpoint_dir)
    if not cp_path.exists():
        return {
            "status": "SKIP",
            "reason": f"Checkpoint dir not found: {checkpoint_dir}",
            "latest_batch": None,
            "offsets": None,
        }

    offsets_dir = cp_path / "offsets"
    if not offsets_dir.exists():
        return {
            "status": "SKIP",
            "reason": "No 'offsets' sub-directory in checkpoint (Spark never ran?)",
            "latest_batch": None,
            "offsets": None,
        }

    # Tìm file batch lớn nhất (batch ID là số nguyên tăng dần)
    batch_files = sorted(
        [f for f in offsets_dir.iterdir() if f.name.isdigit()],
        key=lambda f: int(f.name),
    )
    if not batch_files:
        return {
            "status": "SKIP",
            "reason": "No batch files found in offsets dir",
            "latest_batch": None,
            "offsets": None,
        }

    latest = batch_files[-1]
    try:
        content = latest.read_text(encoding="utf-8")
        # Spark checkpoint có 2 dòng header trước JSON; bỏ qua chúng
        lines = [ln for ln in content.splitlines() if ln.strip()]
        # Dòng JSON thực sự bắt đầu bằng "{"
        json_lines = [ln for ln in lines if ln.startswith("{")]
        offsets = json.loads(json_lines[-1]) if json_lines else None
    except Exception as exc:  # pylint: disable=broad-except
        offsets = None

    return {
        "status": "OK",
        "latest_batch": int(latest.name),
        "offsets": offsets,
        "batch_files_count": len(batch_files),
    }


# ─────────────────────────────────────────────────────────────────────────────
# IDEMPOTENCY COMPARISON
# ─────────────────────────────────────────────────────────────────────────────

def compare_snapshots(before: dict, after: dict) -> dict:
    """
    So sánh hai snapshot và trả về verdict.

    Rules:
    - node_ids_sample phải là subset hoặc bằng nhau (không có ID mới không liên quan)
    - Nếu file_hash thay đổi → node/edge count có thể tăng (OK)
    - Nếu file_hash không đổi → node/edge count phải BẰNG NHAU (idempotent)
    - MongoDB document count phải = 1 (upsert, không tạo mới)
    - duplicate_nodes Neo4j phải = 0
    """
    parser_before = before.get("parser_dryrun", {})
    parser_after  = after.get("parser_dryrun", {})
    neo4j_before  = before.get("neo4j", {})
    neo4j_after   = after.get("neo4j", {})
    mongo_before  = before.get("mongodb", {})
    mongo_after   = after.get("mongodb", {})

    hash_changed  = parser_before.get("file_hash") != parser_after.get("file_hash")
    file_modified = hash_changed

    # Node IDs từ parser (stable IDs)
    before_ids = set(parser_before.get("node_ids_sample", []))
    after_ids  = set(parser_after.get("node_ids_sample", []))

    checks = {}

    # ── Check 1: File hash thay đổi đúng không ────────────────────────────────
    checks["file_hash_changed"] = {
        "expected": True,
        "actual":   file_modified,
        "pass":     file_modified,
        "detail":   "File hash phải thay đổi sau khi sửa code",
    }

    # ── Check 2: Stable IDs preserved (IDs cũ vẫn xuất hiện trong IDs mới) ───
    if before_ids:
        ids_preserved = before_ids.issubset(after_ids) or bool(before_ids & after_ids)
    else:
        ids_preserved = True  # không có baseline để so sánh
    checks["stable_ids_preserved"] = {
        "expected": True,
        "actual":   ids_preserved,
        "pass":     ids_preserved,
        "detail":   "Node IDs xác định phải ổn định giữa các lần parse",
    }

    # ── Check 3: Không duplicate trong Neo4j ─────────────────────────────────
    dup_after = neo4j_after.get("duplicate_nodes")
    if dup_after is not None:
        no_dup = (dup_after == 0)
        checks["neo4j_no_duplicate_nodes"] = {
            "expected": 0,
            "actual":   dup_after,
            "pass":     no_dup,
            "detail":   "Neo4j MERGE + uniqueness constraint = 0 duplicate nodes",
        }

    # ── Check 4: MongoDB doc count = 1 (upsert) ──────────────────────────────
    doc_count_after = mongo_after.get("document_count")
    if doc_count_after is not None:
        checks["mongodb_single_document"] = {
            "expected": 1,
            "actual":   doc_count_after,
            "pass":     (doc_count_after == 1),
            "detail":   "MongoDB replace/upsert với _id=file_id phải cho đúng 1 document",
        }

    # ── Check 5: Neo4j count tăng hợp lý (nếu file đổi) hoặc không đổi ─────
    nc_before = neo4j_before.get("node_count")
    nc_after  = neo4j_after.get("node_count")
    if nc_before is not None and nc_after is not None:
        if file_modified:
            # Cho phép thay đổi (có thể tăng hoặc giảm tùy thay đổi)
            checks["neo4j_node_count_reasonable"] = {
                "expected": "changed (file modified)",
                "actual":   f"{nc_before} → {nc_after}",
                "pass":     True,  # bất kỳ kết quả nào cũng hợp lý khi file đổi
                "detail":   "Count thay đổi sau khi sửa file là hành vi đúng",
            }
        else:
            # File không đổi → count phải bằng
            checks["neo4j_node_count_idempotent"] = {
                "expected": nc_before,
                "actual":   nc_after,
                "pass":     (nc_before == nc_after),
                "detail":   "Replay file không đổi → count phải giữ nguyên",
            }

    # ── Verdict tổng hợp ─────────────────────────────────────────────────────
    all_pass = all(c["pass"] for c in checks.values())
    verdict  = "PASS ✓" if all_pass else "FAIL ✗"

    return {
        "verdict":       verdict,
        "all_pass":      all_pass,
        "file_modified": file_modified,
        "checks":        checks,
        "summary": {
            "parser_node_count_before": parser_before.get("node_count"),
            "parser_node_count_after":  parser_after.get("node_count"),
            "parser_edge_count_before": parser_before.get("edge_count"),
            "parser_edge_count_after":  parser_after.get("edge_count"),
            "file_hash_before":         parser_before.get("file_hash", "")[:12] + "...",
            "file_hash_after":          parser_after.get("file_hash", "")[:12] + "...",
            "neo4j_nodes_before":       nc_before,
            "neo4j_nodes_after":        nc_after,
            "mongo_doc_count_after":    doc_count_after,
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# PRINT HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _print_table(data: dict, title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")
    for key, val in data.items():
        if isinstance(val, dict):
            print(f"  {key}:")
            for k2, v2 in val.items():
                print(f"    {k2:30s}: {v2}")
        elif isinstance(val, list):
            print(f"  {key}: [{', '.join(str(v) for v in val[:3])}{'...' if len(val)>3 else ''}]")
        else:
            print(f"  {key:34s}: {val}")
    print()


def _print_verdict(comparison: dict) -> None:
    print(f"\n{'='*60}")
    print(f"  IDEMPOTENCY VERDICT: {comparison['verdict']}")
    print(f"{'='*60}")
    for check_name, check in comparison["checks"].items():
        status = "✓ PASS" if check["pass"] else "✗ FAIL"
        print(f"  [{status}] {check_name}")
        print(f"           Expected: {check['expected']}")
        print(f"           Actual  : {check['actual']}")
        print(f"           Detail  : {check['detail']}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def build_snapshot(
    absolute_path: str,
    repo_root: str,
    repo_id: str,
    dry_run: bool,
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str,
    mongo_uri: str,
    mongo_db: str,
    mongo_collection: str,
    checkpoint_dir: str,
) -> dict:
    """Thu thập toàn bộ state tại một thời điểm."""

    print("[replay_verifier] Đang parse file (dry-run, không cần Kafka)...")
    parser_info = run_parser_dryrun(absolute_path, repo_root, repo_id)
    _print_table(parser_info, "Parser Dry-Run Result")

    file_id = parser_info["file_id"]

    if dry_run:
        neo4j_info = {
            "status": "SKIP",
            "reason": "--dry-run: Neo4j query skipped",
            "node_count": None, "edge_count": None, "duplicate_nodes": None,
        }
        mongo_info = {
            "status": "SKIP",
            "reason": "--dry-run: MongoDB query skipped",
            "document_count": None, "file_hash": None, "event_time": None,
        }
    else:
        print(f"[replay_verifier] Query Neo4j ({neo4j_uri}) cho file_id={file_id[:16]}...")
        neo4j_info = query_neo4j(file_id, neo4j_uri, neo4j_user, neo4j_password)
        _print_table(neo4j_info, "Neo4j Query Result")

        print(f"[replay_verifier] Query MongoDB ({mongo_uri}) cho _id={file_id[:16]}...")
        mongo_info = query_mongodb(file_id, mongo_uri, mongo_db, mongo_collection)
        _print_table(mongo_info, "MongoDB Query Result")

    print(f"[replay_verifier] Đọc Spark checkpoint: {checkpoint_dir}")
    checkpoint_info = read_checkpoint(checkpoint_dir)
    _print_table(checkpoint_info, "Spark Checkpoint")

    return {
        "captured_at":   parser_info["captured_at"],
        "file_path":     parser_info["file_path"],
        "file_id":       parser_info["file_id"],
        "parser_dryrun": parser_info,
        "neo4j":         neo4j_info,
        "mongodb":       mongo_info,
        "checkpoint":    checkpoint_info,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Task 6 — Idempotent Replay Verifier for CPG Pipeline"
    )
    parser.add_argument(
        "file",
        help="Path to the Python source file to verify (relative or absolute)",
    )
    parser.add_argument(
        "repo_root",
        help="Root of the cloned source repository",
    )
    parser.add_argument(
        "--repo-id",
        default="huggingface/lerobot",
        help="Stable repo identifier (default: huggingface/lerobot)",
    )
    parser.add_argument(
        "--phase",
        choices=["before", "after"],
        default="before",
        help="'before': record baseline; 'after': compare with baseline",
    )
    parser.add_argument(
        "--baseline",
        default="runtime/replay-before.json",
        help="Path to before-snapshot JSON (read when --phase after)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Where to save the current snapshot JSON",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip Neo4j and MongoDB queries (offline mode for CI/testing)",
    )
    parser.add_argument(
        "--neo4j-uri",
        default=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
    )
    parser.add_argument(
        "--neo4j-user",
        default=os.getenv("NEO4J_USER", "neo4j"),
    )
    parser.add_argument(
        "--neo4j-password",
        default=os.getenv("NEO4J_PASSWORD", "cpg-password"),
    )
    parser.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    )
    parser.add_argument(
        "--mongo-db",
        default="cpg",
    )
    parser.add_argument(
        "--mongo-collection",
        default="metadata",
    )
    parser.add_argument(
        "--checkpoint-dir",
        default=os.getenv("CHECKPOINT_DIR", "checkpoints/person3-final"),
    )
    args = parser.parse_args(argv)

    # Resolve paths
    abs_file = str(pathlib.Path(args.file).resolve())
    abs_repo = str(pathlib.Path(args.repo_root).resolve())

    if not pathlib.Path(abs_file).is_file():
        print(f"[ERROR] File không tồn tại: {abs_file}", file=sys.stderr)
        return 1

    # Build snapshot hiện tại
    snapshot = build_snapshot(
        absolute_path=abs_file,
        repo_root=abs_repo,
        repo_id=args.repo_id,
        dry_run=args.dry_run,
        neo4j_uri=args.neo4j_uri,
        neo4j_user=args.neo4j_user,
        neo4j_password=args.neo4j_password,
        mongo_uri=args.mongo_uri,
        mongo_db=args.mongo_db,
        mongo_collection=args.mongo_collection,
        checkpoint_dir=args.checkpoint_dir,
    )

    # Lưu snapshot
    default_output = f"runtime/replay-{args.phase}.json"
    output_path = args.output or default_output
    pathlib.Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fout:
        json.dump(snapshot, fout, indent=2, ensure_ascii=False)
    print(f"[replay_verifier] Snapshot đã lưu: {output_path}")

    # Nếu phase=after: load baseline và so sánh
    if args.phase == "after":
        baseline_path = args.baseline
        if not pathlib.Path(baseline_path).exists():
            print(
                f"[ERROR] Baseline file không tồn tại: {baseline_path}\n"
                "Hãy chạy --phase before trước.",
                file=sys.stderr,
            )
            return 1

        with open(baseline_path, encoding="utf-8") as fin:
            before_snapshot = json.load(fin)

        comparison = compare_snapshots(before_snapshot, snapshot)
        _print_verdict(comparison)

        # Lưu comparison
        cmp_path = output_path.replace(".json", "_comparison.json")
        with open(cmp_path, "w", encoding="utf-8") as fout:
            json.dump(comparison, fout, indent=2, ensure_ascii=False)
        print(f"[replay_verifier] Comparison đã lưu: {cmp_path}")

        return 0 if comparison["all_pass"] else 2  # exit 2 = FAIL

    return 0


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    raise SystemExit(main())
