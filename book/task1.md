# Task 1 — Repository cloning and discovery

The assigned repository is cloned shallowly to reduce transfer and storage:

```bash
git clone --depth=1 https://github.com/huggingface/lerobot.git lerobot
python src/discovery.py lerobot
```

`src/discovery.py` excludes tests, examples, generated files, virtual
environments and VCS metadata. It records relative paths, sizes and SHA-256
hashes in `discovered_files.json`.

For the checked-out repository revision used during development, discovery
found 489 core Python files. Re-run the command immediately before submission
and retain its output because the count depends on the exact commit.

## Evidence to capture

- The shallow clone command and selected Git commit.
- Discovery command with the final file count.
- A sample of discovered relative paths and hashes.

## Reflection

Excluding tests and examples keeps the graph focused on production code.
Content hashes allow unchanged files to be skipped by an incremental runner.
