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
│   ├── parser_service.py       # [Thành viên 1] CPG parser (AST/CFG/DFG/CALL)
│   ├── kafka_publisher.py      # [Thành viên 2] Parser → Kafka delivery adapter
│   └── topic_config.py         # [Thành viên 2] Topic contract/configuration
├── config/
│   ├── schemas/                # JSON Schema v1 cho 4 domain event types
│   └── connectors/             # Neo4j Kafka Sink connector configuration
├── infra/
│   ├── connect/                # Kafka Connect image + Neo4j connector 5.5.0
│   ├── kafka/                  # Idempotent topic bootstrap
│   └── neo4j/                  # Constraints, indexes, verification queries
├── notebooks/
│   └── task1_task2_member1.ipynb  # [Thành viên 1] Demo notebook với output thực tế
├── tests/                      # Stable-ID/schema/topic/sink contract tests
├── docker-compose.yml          # Kafka + Connect + Neo4j local stack
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

Ngoài bốn topic bắt buộc, `cpg.neo4j.dlq` chứa record lỗi ở tầng Kafka
Connect. Đây không phải parser-error topic.

Import topic names từ `src/schemas.py`:
```python
from src.schemas import TOPIC_NODES, TOPIC_EDGES, TOPIC_METADATA, TOPIC_ERRORS
```

## Nhiệm vụ Thành viên 2: Kafka + Neo4j (Task 3–4)

Khởi động broker, tạo topic, tạo Neo4j constraints, dựng Kafka Connect và đăng
ký sink connector bằng một lệnh:

```bash
cp .env.example .env
docker compose up -d --build
docker compose ps
```

Publish đúng một file Python (đơn vị incremental của đề):

```bash
python -m src.kafka_publisher lerobot \
  src/lerobot/__init__.py \
  --repo-id huggingface/lerobot
```

Kiểm tra schema/ID mà không cần Kafka:

```bash
python -m src.kafka_publisher lerobot \
  src/lerobot/__init__.py \
  --repo-id huggingface/lerobot \
  --dry-run
```

Kafka Connect REST API ở `http://localhost:8083`; Neo4j Browser ở
`http://localhost:7474`. Hướng dẫn thiết kế, truy vấn kiểm chứng và checklist
chụp evidence nằm trong [`docs/task3_task4.md`](docs/task3_task4.md). Kết quả
integration run của Thành viên 2 được lưu tại
[`docs/evidence/task3_task4_e2e.md`](docs/evidence/task3_task4_e2e.md).

Chạy test:

```bash
python -m unittest discover -s tests -v
docker compose config --quiet
```

## Idempotency (Task 6)

- `file_id` dùng SHA-256 đầy đủ của `repo_id + normalized relative path`.
- `node_id` dùng `file_id + deterministic AST structural path + node type`;
  nhờ vậy cả AST singleton không có line/column cũng không collision.
- `edge_id` dùng source/target/type và call-site discriminator khi cần.
- Neo4j có unique constraints và Cypher `MERGE` cho node, endpoint và edge.
- Metadata thành công dọn node/edge cũ cùng `file_id` nhưng khác `file_hash`;
  metadata lỗi không xoá graph hợp lệ trước đó.
