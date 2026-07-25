# Lab 04 — Incremental CPG Streaming Pipeline

Pipeline xử lý từng file Python và lưu hai nhánh dữ liệu độc lập:

```text
Python repository → Discovery → Parser Service → Kafka
                                              ├─ cpg.nodes / cpg.edges
                                              │        ↓
                                              │  Neo4j Kafka Sink → Neo4j
                                              ├─ cpg.metadata
                                              │        ↓
                                              │  Spark Structured Streaming → MongoDB
                                              └─ cpg.errors → monitoring
```

Neo4j nhận topology trực tiếp từ Kafka, không đi qua Spark. Spark chỉ consume
`cpg.metadata` và dùng checkpoint để tiếp tục từ offset đã xử lý.

## Phân công

| Thành viên | Phạm vi | Thành phần chính |
|---|---|---|
| 1 | Task 1–2 | `discovery.py`, `parser_service.py`, schema event |
| 2 | Task 3–4 | Kafka topics/publisher, Kafka Connect, Neo4j Sink |
| 3 | Task 5 | Spark Structured Streaming, MongoDB, checkpoint |
| 4 | Task 6 + report | replay toàn pipeline, diagram, Jupyter Book/Pages |

## Cấu trúc chính

```text
src/
├── discovery.py
├── parser_service.py
├── schemas.py
├── kafka_publisher.py
├── topic_config.py
├── metadata_streaming_job.py
├── spark_metadata_consumer.py
└── spark_mongo_sink.py
config/
├── schemas/                  # JSON Schema v1
└── connectors/neo4j-sink.json
infra/
├── connect/Dockerfile
├── kafka/create-topics.sh
└── neo4j/{constraints,verify}.cypher
book/                         # Jupyter Book sources
docs/                         # Task 3–4 design/evidence
tests/
docker-compose.yml
```

## Chuẩn bị

- Python 3.14 cho Discovery/Parser/Publisher trên commit LeRobot đã pin
- Python 3.11 + Java 17 cho CI, Jupyter Book và Spark-specific unit tests
- Docker Engine/Desktop với Docker Compose
- Repo được Moodle chỉ định, shallow-clone vào `lerobot/`

```bash
git clone --depth=1 https://github.com/huggingface/lerobot.git lerobot
python3.14 -m venv .venv
.venv/bin/python -m pip install \
  kafka-python==3.0.8 jsonschema==4.25.1 \
  neo4j==5.20.0 pymongo==4.6.3
cp .env.example .env
```

LeRobot commit `0d383d09f2051444de211739196a28cc94736861` có cú pháp mà
Python 3.11 không parse được ở bốn file; full dry-run bằng Python 3.14 xử lý đủ
490 file với 0 parser error. Spark 3.5.1 chạy trong container `spark-person3`,
không cài PySpark vào virtualenv Python 3.14. Trên Windows, dùng
`.venv\Scripts\python` thay cho `.venv/bin/python`.

## Task 1–2: Discovery và Parser

```bash
.venv/bin/python src/discovery.py lerobot
.venv/bin/python src/parser_service.py \
  lerobot/src/lerobot/__init__.py \
  lerobot
```

Parser tạo ID theo contract sau:

- `file_id`: SHA-256 đầy đủ của `repo_id + normalized relative path`.
- `node_id`: `file_id + deterministic AST structural path + node type`.
- `edge_id`: source/target/type và call-site discriminator khi cần.

Cách này phân biệt cả AST singleton không có line/column và giữ ID ổn định giữa
các lần parse cùng nội dung.

## Task 3–4: Kafka và Neo4j Sink

Khởi động Kafka, topic bootstrap, Neo4j, constraints, Kafka Connect, MongoDB và
đăng ký connector:

```bash
docker compose up -d --build
docker compose ps
```

Publish đúng một file:

```bash
.venv/bin/python -m src.kafka_publisher lerobot \
  src/lerobot/__init__.py \
  --repo-id huggingface/lerobot
```

Kiểm tra contract mà không kết nối Kafka:

```bash
.venv/bin/python -m src.kafka_publisher lerobot \
  src/lerobot/__init__.py \
  --repo-id huggingface/lerobot \
  --dry-run
```

Các topic bắt buộc:

| Topic | Kafka key | Consumer |
|---|---|---|
| `cpg.nodes` | `node_id` | Neo4j Sink |
| `cpg.edges` | `edge_id` | Neo4j Sink |
| `cpg.metadata` | `file_id` | Spark; Neo4j reconciliation |
| `cpg.errors` | `file_id` | monitoring |

`cpg.neo4j.dlq` là DLQ vận hành riêng, không trộn với parser errors.

Kiểm tra connector và graph:

```bash
curl -fsS http://localhost:8083/connectors/cpg-neo4j-sink/status
docker compose exec -T neo4j cypher-shell \
  -u neo4j -p cpg-password -f /dev/stdin < infra/neo4j/verify.cypher
```

Neo4j Browser: <http://localhost:7474>. Kafka UI: <http://localhost:8080>.
Thiết kế và checklist evidence chi tiết nằm trong
[`docs/task3_task4.md`](docs/task3_task4.md).

## Task 5: Spark metadata → MongoDB

Spark job dùng schema `cpg.metadata` đầy đủ, đặt MongoDB `_id = file_id` và ghi
bằng `replace/upsert`. Một revision mới của cùng file vì vậy cập nhật đúng một
document thay vì tạo duplicate.

Chạy Spark bằng profile Docker:

```bash
docker compose --profile person3 up -d spark-person3
docker compose logs -f spark-person3
```

Checkpoint được mount tại `./checkpoints/person3-final-docker`. Không xóa hoặc
đổi path này khi test resume.

Kiểm tra MongoDB:

```bash
docker compose exec -T mongodb mongosh cpg --quiet --eval \
  'db.metadata.find({}, {_id:1,file_path:1,file_hash:1,parse_status:1}).toArray()'
```

Nghiệm thu resume:

1. Publish metadata và chờ MongoDB cập nhật.
2. Dừng `spark-person3`.
3. Khởi động lại bằng cùng checkpoint, chưa publish message mới.
4. Xác nhận Spark không xử lý lại offset cũ và MongoDB không tăng document.
5. Sửa một file, chỉ publish file đó và xác nhận cùng `_id/file_id` được update.

Checkpoint bảo vệ Kafka offsets; MongoDB `replace/upsert` bảo vệ trường hợp
micro-batch được retry.

Kết quả integration/restart gần nhất được lưu tại
[`docs/evidence/task5_spark_mongodb_e2e.md`](docs/evidence/task5_spark_mongodb_e2e.md).

## Test

```bash
.venv/bin/python -m unittest discover -s tests -v
docker compose --profile person3 config --quiet
git diff --check
```

Nếu PySpark không có trong environment, Spark-specific unit tests sẽ skip. Cần
chạy chúng trong environment Spark hoặc container trước khi nghiệm thu Task 5.

## Task 6: Replay

Với một file thật trong repo Moodle:

1. Publish baseline và lưu `file_id`, `file_hash`, parser totals, Neo4j/Mongo counts.
2. Sửa đúng một file rồi chỉ publish file đó.
3. Chờ Neo4j consumer lag về 0 và Spark hoàn thành micro-batch.
4. Neo4j phải có `count = count(DISTINCT id)` và không còn revision hash cũ.
5. MongoDB phải có đúng một document `_id = file_id` với hash mới.
6. Publish lại revision mới: mọi count vẫn giữ nguyên.
7. Restart Spark bằng checkpoint cũ: không đọc lại offset đã commit.

## Jupyter Book và GitHub Pages

`_toc.yml`, `_config.yml` và thư mục `book/` là source của báo cáo. Mỗi chapter
phải có giải thích, command/cell đã chạy, output thật, hình ảnh và reflection.

```bash
python3.11 -m venv .venv-report
.venv-report/bin/python -m pip install jupyter-book==0.15.1
.venv-report/bin/jupyter-book build . --all --warningiserror
```

HTML được tạo dưới `_build/html`. Workflow `.github/workflows/ci.yml` test mọi
pull request (hoặc feature branch khi chạy `workflow_dispatch`) nhưng chỉ deploy
Pages sau khi `main` build thành công. Trên GitHub, chủ repository cần chọn một
lần:

1. **Settings → Pages**.
2. **Build and deployment → Source → GitHub Actions**.
3. Merge nhánh hoàn chỉnh vào `main`, push và đợi cả job test lẫn deploy xanh.
4. Mở `https://caosfourn.github.io/bigdata-lab04-pipeline/` bằng cửa sổ ẩn danh.
5. Đi qua toàn bộ chapter và kiểm tra hình/output/link.

Moodle chỉ nhận đúng URL gốc Jupyter Book ở bước 4. Không nộp URL repository,
URL chapter riêng, ZIP, PDF hay Word. Trước khi publish phải kiểm tra số liệu
LeRobot cuối trong Task 3–6 khớp evidence đã commit và các hình Kafka, Neo4j,
MongoDB cùng Spark checkpoint hiển thị đúng trong từng chapter.
