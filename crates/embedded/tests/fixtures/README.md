# v0.15 storage migration fixture

`v015_populated.redb.zst.b64` is a zstd-compressed Redb store generated with
DefraDB.rs v0.15.3 (`42d99dc1`). It contains this schema:

```graphql
type Task {
  task_id: String @index(unique: true)
  status: String @index
  note: String
}
```

The generator inserted three tasks and then updated `task-1`, producing a
multi-height commit graph. The integration test verifies document and index
reads, unique-index enforcement, writes, updates, commit-history lookup through
the old public DocID alias, reopen idempotence, and transactional rollback when
a legacy block is missing.

The base64 wrapper keeps this small historical database fixture reviewable and
portable through source-only packaging. Decode it with:

```sh
base64 --decode v015_populated.redb.zst.b64 | zstd --decompress > fixture.redb
```
