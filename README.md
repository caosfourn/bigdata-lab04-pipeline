# Lab 04 - Spark Streaming: Incremental CPG Pipeline

## Tổng quan

Pipeline xây dựng **Code Property Graph (CPG)** từ repo Python [huggingface/lerobot](https://github.com/huggingface/lerobot) và streaming kết quả vào Neo4j + MongoDB qua Apache Kafka.

```
lerobot repo → Parser Service → Kafka Topics → Neo4j (graph topology)
                                             ↘ MongoDB (metadata, via Spark)
```

## Cấu trúc thư mục

```
bigdata-lab04-pipeline/
├── lerobot/                    # Repo đã clone (git shallow clone)
├── src/
│   ├── __init__.py
│   ├── schemas.py              # [Thành viên 1] Kafka message schemas & topic names
│   ├── discovery.py            # [Thành viên 1] File discovery + hash tracking
│   └── parser_service.py       # [Thành viên 1] CPG parser (AST/CFG/DFG/CALL)
├── notebooks/
│   └── task1_task2_member1.ipynb  # [Thành viên 1] Demo notebook với output thực tế
├── requirements.txt
└── [BigData] Lab04 - StreamingV0.md
```

## Nhiệm vụ Thành viên 1: Parser & Discovery

### Task 1: Khám phá file Python
```bash
cd bigdata-lab04-pipeline
python src/discovery.py lerobot
# Output: discovered_files.json
```

### Task 2: Chạy CPG Parser trên 1 file
```bash
python src/parser_service.py \
  lerobot/src/lerobot/__init__.py \
  lerobot
```

### Chạy Notebook demo đầy đủ
```bash
jupyter notebook notebooks/task1_task2_member1.ipynb
```

## Yêu cầu môi trường
- Python 3.10+
- Không cần cài gói ngoài cho Thành viên 1 (dùng stdlib `ast`, `hashlib`, `os`)

```bash
pip install -r requirements.txt   # Chỉ cần nếu chạy Kafka / Spark / Notebook
```

## Kafka Topic Layout (được Thành viên 2 sử dụng)

| Topic | Nội dung | Producer | Consumer |
|-------|----------|----------|----------|
| `cpg.nodes` | AST node events | Parser Service | Neo4j Connector |
| `cpg.edges` | AST/CFG/DFG/CALL edge events | Parser Service | Neo4j Connector |
| `cpg.metadata` | File-level metadata | Parser Service | Spark → MongoDB |
| `cpg.errors` | Parse error events | Parser Service | Monitoring |

Import topic names từ `src/schemas.py`:
```python
from schemas import TOPIC_NODES, TOPIC_EDGES, TOPIC_METADATA, TOPIC_ERRORS
```

## Idempotency (Task 6)

- Mỗi node/edge có **Stable ID** = SHA-256 của `(file_path, node_type, line, col)`.
- Mỗi file có **SHA-256 hash** để detect thay đổi.
- Neo4j dùng `MERGE ON node_id` → không tạo duplicate khi replay.
