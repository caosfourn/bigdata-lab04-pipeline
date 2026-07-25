# Walkthrough — Người 4: Task 6 + Architecture Diagram + Jupyter Book

## Files Created

### 1. [`src/replay_verifier.py`](file:///d:/GitHub/bigdata-lab04-pipeline/src/replay_verifier.py)

Script idempotent replay verifier với 2 phase:

```powershell
# Phase before (ghi baseline):
python -m src.replay_verifier lerobot/lerobot/__init__.py lerobot \
  --phase before --dry-run --output runtime/replay-before.json

# Sửa file, rồi chạy phase after (so sánh):
python -m src.replay_verifier lerobot/lerobot/__init__.py lerobot \
  --phase after --baseline runtime/replay-before.json --dry-run
```

**Kiến trúc:**

- `run_parser_dryrun()` — dùng `CollectingProducer` (offline, không cần Kafka)
- `query_neo4j()` — đếm `CPGNode` + `CPG_EDGE` theo `file_id`, detect duplicates
- `query_mongodb()` — lấy document `_id = file_id`, kiểm tra `document_count = 1`
- `read_checkpoint()` — đọc `offsets/N` files trong Spark checkpoint dir
- `compare_snapshots()` — đối chiếu before/after, trả về `PASS/FAIL` verdict
- Graceful degradation khi DB offline (`--dry-run` flag)

### 2. [`notebooks/task6_member4.ipynb`](file:///d:/GitHub/bigdata-lab04-pipeline/notebooks/task6_member4.ipynb)

13 cells bao gồm:

- Import + config (với fallback nếu lerobot chưa clone)
- Schema constants từ `src/schemas.py`
- Parse BEFORE + AFTER modification (dry-run)
- ID stability analysis (node_ids preserved vs new)
- Before/after comparison table
- Neo4j Cypher queries + live query nếu DB available
- MongoDB verification + live query nếu DB available
- Spark checkpoint reader
- `replay_verifier.py` subprocess call
- Final verdict table + reflection

### 3. [`docs/architecture_diagram.png`](file:///d:/GitHub/bigdata-lab04-pipeline/docs/architecture_diagram.png)

PNG diagram (454 KB) minh họa đầy đủ pipeline.

### 4. [`.github/workflows/deploy.yml`](file:///d:/GitHub/bigdata-lab04-pipeline/.github/workflows/deploy.yml)

GitHub Actions workflow tự động deploy Jupyter Book lên GitHub Pages khi push lên `main`:

- Build: `jupyter-book build .` → artifact `_build/html`
- Deploy: `actions/deploy-pages@v4`
- URL sẽ là: `https://caosfourn.github.io/bigdata-lab04-pipeline/`

## Files Updated

### 5. [`_toc.yml`](file:///d:/GitHub/bigdata-lab04-pipeline/_toc.yml)

Thêm `notebooks/task6_member4` dưới `book/task6`

### 6. [`book/task6.md`](file:///d:/GitHub/bigdata-lab04-pipeline/book/task6.md)

Nâng cấp với: approach table, script usage, file modification details, evidence table, reflection

### 7. [`book/architecture.md`](file:///d:/GitHub/bigdata-lab04-pipeline/book/architecture.md)

Thêm: component breakdown table, Kafka topic table, idempotency chain, Mermaid diagram

### 8. [`book/conclusion.md`](file:///d:/GitHub/bigdata-lab04-pipeline/book/conclusion.md)

Rewrite với: tóm tắt 4 thành viên, key design decisions, submission URL

---

## Validation Results

| Check                                       | Result                                     |
| ------------------------------------------- | ------------------------------------------ |
| `python -m compileall src/`                 | ✅ No errors                               |
| `replay_verifier.py` import                 | ✅ OK                                      |
| `replay_verifier --phase before --dry-run`  | ✅ Runs, saves JSON                        |
| `replay_verifier --phase after` (same file) | ✅ Correctly returns FAIL (hash unchanged) |
| All TOC files present                       | ✅ 13/13 files found                       |

---

## Những bước còn lại (chạy thực tế với DB)

> [!IMPORTANT]
> Các bước sau cần chạy khi **pipeline đang hoạt động** (Docker Compose lên):

1. **Clone lerobot:** `git clone --depth=1 https://github.com/huggingface/lerobot.git lerobot`
2. **Khởi động pipeline:** `docker compose up -d --build`
3. **Publish files lên Kafka:** `python -m src.kafka_publisher lerobot --repo-id huggingface/lerobot`
4. **Chạy verifier before:**
   ```powershell
   python -m src.replay_verifier lerobot/lerobot/__init__.py lerobot \
     --phase before --output runtime/replay-before.json \
     --neo4j-password cpg-password
   ```
5. **Sửa file** (notebook Cell 4 tự động làm điều này)
6. **Publish lại 1 file:** `python -m src.kafka_publisher lerobot lerobot/lerobot/__init__.py --repo-id huggingface/lerobot`
7. **Chạy verifier after** → điền kết quả thực vào evidence table trong `book/task6.md`
8. **Chụp screenshot** Neo4j Browser + MongoDB Compass → lưu vào `docs/evidence/`
9. **Enable GitHub Pages** trong repo Settings → Pages → Source: GitHub Actions
10. **Push lên main** → workflow `deploy.yml` tự build + deploy
