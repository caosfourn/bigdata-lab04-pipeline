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

> ⬜ **Pending:** this chapter still needs the real discovery output (file
> count, sample paths/hashes, selected commit SHA) from the final
> Moodle-selected repository/commit, supplied by Member 1. Paste the
> executed command output below this note before submission.

## Reflection

**Approach and reasoning:** excluding tests, examples, and generated
directories keeps the resulting graph focused on production code, which is
what the CPG parser and downstream idempotency checks care about. Content
hashes (SHA-256 per file) give the incremental runner a cheap way to decide
"unchanged" without re-parsing, which matters once the pipeline scales past
a single file.

**What worked:** a shallow clone (`--depth=1`) is sufficient because the
pipeline only ever needs the current file tree, not history — this avoided
downloading the full `lerobot` history for a repository with a large commit
log.

**What to watch for:** discovery output is commit-specific. Reusing a count
or hash sample captured on a different commit (or a different exclusion
list) than the one graded would make this chapter's numbers unverifiable —
hence the pending-evidence note above rather than an invented figure.
