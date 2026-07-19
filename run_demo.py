"""
Script tạo ra output thực tế của Task 1 & Task 2.
Kết quả được dùng để điền vào notebook (nếu Jupyter chưa cài).
"""
import sys, os, json

os.chdir('d:\\HuynhHan\\Bigdata\\bigdata-lab04-pipeline')
sys.path.insert(0, 'src')

from discovery import discover_python_files, EXCLUDE_DIRS, EXCLUDE_FILES
from parser_service import CPGParser

REPO_ROOT = os.path.abspath('lerobot')

# ─── TASK 1 ───────────────────────────────────────────────────────────────────
print("=" * 65)
print("TASK 1: Repository Cloning & File Discovery")
print("=" * 65)
print(f"Repo: huggingface/lerobot")
print(f"Clone command: git clone --depth=1 https://github.com/huggingface/lerobot.git lerobot")
print()
print(f"Cấu hình lọc:")
print(f"  Thư mục loại trừ: {sorted(EXCLUDE_DIRS)}")
print(f"  File loại trừ   : {sorted(EXCLUDE_FILES)}")
print()

py_files = discover_python_files(REPO_ROOT)
print(f"✅ Tổng số file Python cốt lõi: {len(py_files)}")
print()

import collections
dir_counts = collections.Counter()
for f in py_files:
    parts = f['relative_path'].replace('\\', '/').split('/')
    dir_counts[parts[0]] += 1

print("Phân bổ theo thư mục top-level:")
for d, c in dir_counts.most_common():
    bar = '█' * (c // 5)
    print(f"  {d:<30} {c:>4} files  {bar}")

total_size = sum(f['file_size_bytes'] for f in py_files)
print(f"\nTổng kích thước: {total_size / 1024:.1f} KB")

# ─── TASK 2 ───────────────────────────────────────────────────────────────────
print()
print("=" * 65)
print("TASK 2: CPG Parser Service — Demo trên 5 file")
print("=" * 65)
print(f"{'File':<50} {'Nodes':>6} {'AST':>5} {'CFG':>5} {'DFG':>5} {'CALL':>5} {'ms':>7}")
print('-' * 90)

interesting_files = [f for f in py_files if f['file_size_bytes'] > 1000][:5]
totals = {'nodes': 0, 'ast': 0, 'cfg': 0, 'dfg': 0, 'call': 0}

for fi in interesting_files:
    parser = CPGParser(fi['absolute_path'], REPO_ROOT)
    nodes, edges, meta, err = parser.parse()
    if err:
        print(f"{fi['relative_path']:<50}  ERROR: {err['error_type']}")
        continue
    e = meta['total_edges']
    rel = fi['relative_path'].replace('\\', '/')
    print(f"{rel:<50} {meta['total_nodes']:>6} {e['ast']:>5} {e['cfg']:>5} {e['dfg']:>5} {e['call']:>5} {meta['parse_duration_ms']:>7.1f}")
    totals['nodes'] += meta['total_nodes']
    for k in ['ast','cfg','dfg','call']:
        totals[k] += e[k]

print('-' * 90)
print(f"{'TOTAL':<50} {totals['nodes']:>6} {totals['ast']:>5} {totals['cfg']:>5} {totals['dfg']:>5} {totals['call']:>5}")

# ─── STABLE ID PROOF ──────────────────────────────────────────────────────────
print()
print("=" * 65)
print("STABLE ID IDEMPOTENCY TEST")
print("=" * 65)

test_file = interesting_files[0]['absolute_path']
all_node_ids, all_edge_ids = [], []

for i in range(3):
    p = CPGParser(test_file, REPO_ROOT)
    n, e, m, _ = p.parse()
    all_node_ids.append(sorted([x['node_id'] for x in n]))
    all_edge_ids.append(sorted([x['edge_id'] for x in e]))
    print(f"  Run {i+1}: {len(n)} nodes, {len(e)} edges")

nodes_ok = all(all_node_ids[0] == r for r in all_node_ids)
edges_ok = all(all_edge_ids[0] == r for r in all_edge_ids)
print()
print(f"  Node IDs stable: {'✅ PASS' if nodes_ok else '❌ FAIL'}")
print(f"  Edge IDs stable: {'✅ PASS' if edges_ok else '❌ FAIL'}")
print(f"  Idempotency    : {'✅ PASS — Neo4j MERGE không tạo duplicate!' if nodes_ok and edges_ok else '❌ FAIL'}")

# ─── SAMPLE EVENTS ────────────────────────────────────────────────────────────
print()
print("=" * 65)
print("SAMPLE KAFKA EVENTS")
print("=" * 65)
parser = CPGParser(test_file, REPO_ROOT)
nodes, edges, metadata, _ = parser.parse()

func_nodes = [n for n in nodes if n['properties']['type'] == 'FunctionDef']
print("\n[cpg.nodes] Sample FunctionDef node:")
sample = func_nodes[0] if func_nodes else nodes[0]
print(json.dumps(sample, indent=2))

cfg_edges = [e for e in edges if e['type'].startswith('CFG')]
if cfg_edges:
    print("\n[cpg.edges] Sample CFG edge:")
    print(json.dumps(cfg_edges[0], indent=2))

dfg_edges = [e for e in edges if e['type'] == 'DFG_USE']
if dfg_edges:
    print("\n[cpg.edges] Sample DFG edge:")
    print(json.dumps(dfg_edges[0], indent=2))

call_edges = [e for e in edges if 'CALL' in e['type']]
if call_edges:
    print("\n[cpg.edges] Sample CALL edge:")
    print(json.dumps(call_edges[0], indent=2))

print("\n[cpg.metadata] Metadata event:")
print(json.dumps(metadata, indent=2))
