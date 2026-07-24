# Task 1 — Repository cloning and discovery

The assigned repository is cloned shallowly to reduce transfer and storage:

```bash
git clone --depth=1 https://github.com/huggingface/lerobot.git lerobot
python src/discovery.py lerobot
```

`src/discovery.py` excludes configured test/example directories, setup files,
generated suffixes, virtual environments and VCS metadata. It records relative
paths, sizes and SHA-256 hashes in `discovered_files.json`.

Re-run the command against the final Moodle-selected commit and record the
actual count here; it depends on both the commit and the agreed exclusion
rules. Do not reuse an output captured from another machine/revision.

## Evidence to capture

- The shallow clone command and selected Git commit.
- Discovery command with the final file count.
- A sample of discovered relative paths and hashes.

## Reflection

Excluding tests and examples keeps the graph focused on production code.
Content hashes allow unchanged files to be skipped by an incremental runner.
