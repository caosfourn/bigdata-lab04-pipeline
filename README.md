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

## Quick start end-to-end

```powershell
Copy-Item .env.example .env
docker compose up -d --build
.\scripts\create_topics.ps1
.\scripts\register_neo4j_connector.ps1
python -m src.kafka_producer lerobot --brokers localhost:9092
```

Giao diện local:

- Kafka UI: `http://localhost:8080`
- Neo4j Browser: `http://localhost:7474`
- Kafka Connect API: `http://localhost:8083`

Trên Windows console cũ, bật UTF-8 trước khi chạy demo:

```powershell
$env:PYTHONUTF8 = "1"
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

### Thành viên 2: Neo4j sink helpers
- File mới: `src/neo4j_sink.py`
- Chứa Cypher `MERGE` cho node/edge/metadata events.
- Có constraint statements để Neo4j load idempotently.

```bash
python -m pytest -q tests/test_person2.py
```

## Idempotency (Task 6)

- Mỗi node/edge có **Stable ID** = SHA-256 của `(file_path, node_type, line, col)`.
- Mỗi file có **SHA-256 hash** để detect thay đổi.
- Neo4j dùng `MERGE ON node_id` → không tạo duplicate khi replay.

## Thành viên 3: Spark metadata → MongoDB

### Yêu cầu

- Java 11 và Apache Spark 3.5.1 (`spark-submit` phải có trong `PATH`).
- Kafka broker có topic `cpg.metadata`.
- MongoDB có thể truy cập từ Spark driver và executors.
- Thư mục checkpoint nằm trên storage bền vững và user chạy Spark có quyền ghi.

### Cấu hình và chạy

Ví dụ local với Spark 3.5, Kafka và MongoDB chạy trên máy hiện tại:

```bash
BROKERS=localhost:9092
TOPIC=cpg.metadata
MONGO_URI=mongodb://localhost:27017
CHECKPOINT_DIR=/tmp/cpg-metadata-checkpoint
SPARK_IVY_DIR=/tmp/spark-ivy

mkdir -p "$CHECKPOINT_DIR" "$SPARK_IVY_DIR"

spark-submit \
  --conf "spark.jars.ivy=$SPARK_IVY_DIR" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.7.0 \
  src/metadata_streaming_job.py \
  --brokers "$BROKERS" \
  --topic "$TOPIC" \
  --mongo-uri "$MONGO_URI" \
  --mongo-db cpg \
  --mongo-collection metadata \
  --checkpoint-location "$CHECKPOINT_DIR"
```

PowerShell tương đương:

```powershell
$env:BROKERS = "localhost:9092"
$env:TOPIC = "cpg.metadata"
$env:MONGO_URI = "mongodb://localhost:27017"
$env:CHECKPOINT_DIR = "C:\\tmp\\cpg-metadata-checkpoint"
$env:SPARK_IVY_DIR = "C:\\tmp\\spark-ivy"

New-Item -ItemType Directory -Force $env:CHECKPOINT_DIR, $env:SPARK_IVY_DIR | Out-Null

spark-submit `
  --conf "spark.jars.ivy=$env:SPARK_IVY_DIR" `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.7.0 `
  src/metadata_streaming_job.py `
  --brokers $env:BROKERS `
  --topic $env:TOPIC `
  --mongo-uri $env:MONGO_URI `
  --mongo-db cpg `
  --mongo-collection metadata `
  --checkpoint-location $env:CHECKPOINT_DIR
```

Ý nghĩa bốn biến bắt buộc:

| Biến | Ý nghĩa | Ví dụ local |
|------|---------|-------------|
| `BROKERS` | Danh sách Kafka bootstrap server Spark truy cập được | `localhost:9092` |
| `TOPIC` | Topic chứa JSON metadata | `cpg.metadata` |
| `MONGO_URI` | URI MongoDB Spark truy cập được | `mongodb://localhost:27017` |
| `CHECKPOINT_DIR` | Checkpoint riêng, bền vững cho query | `/tmp/cpg-metadata-checkpoint` |

Job tạo `_id = SHA-256(file_path)` và ghi bằng MongoDB `replace/upsert`. Khi nội
dung file thay đổi, `file_hash` và các số đếm được cập nhật trên đúng một document
hiện hành của file thay vì tạo thêm document.

Không dùng chung `CHECKPOINT_DIR` với query khác, không xóa hoặc đổi đường dẫn khi restart.
`startingOffsets` chỉ áp dụng khi checkpoint chưa tồn tại; sau đó Spark luôn tiếp tục từ
offset đã commit trong checkpoint. Nếu chạy trong Docker/Kubernetes, không dùng filesystem
tạm bên trong container làm checkpoint; mount persistent volume hoặc dùng distributed storage.

### Verify resume và không duplicate

1. Chạy job và produce ba event khác nhau vào `cpg.metadata`.
2. Chờ MongoDB nhận đủ ba document rồi dừng job (`Ctrl+C`, kill process hoặc dừng container).
3. Trong lúc job tắt, produce hai event replay có cùng `file_path + file_hash` và một event mới.
4. Chạy lại đúng command với cùng `CHECKPOINT_DIR`.
5. Trong log, kiểm tra Spark báo `Resuming at batch ... with committed offsets ...`.
6. Kiểm tra MongoDB theo khóa logic:

```javascript
use cpg
db.metadata.countDocuments({})
db.metadata.aggregate([
  {
    $group: {
      _id: { file_path: "$file_path", file_hash: "$file_hash" },
      count: { $sum: 1 }
    }
  },
  { $match: { count: { $gt: 1 } } }
])
```

Aggregation cuối phải trả về `[]`; tổng document chỉ tăng bởi event mới. Nếu Spark báo lỗi
`Permission denied` tại checkpoint, sửa quyền/mount của `CHECKPOINT_DIR` trước khi chạy lại.

Integration test đã thực hiện với Kafka offset `0..5`: job bị kill sau offset `2`, restart
từ committed offset `3`, xử lý tiếp đến offset `6`; hai event replay và một event mới cho kết
quả bốn document duy nhất, aggregation duplicate trả về `[]`.

### Unit test

```bash
python -m pytest -q tests/test_person3.py tests/test_person3_spark.py
```

Nếu host không cài PySpark, hai Spark test sẽ được skip. Chạy chúng bằng Spark thật:

```bash
spark-submit --master 'local[1]' tests/test_person3_spark.py
```
