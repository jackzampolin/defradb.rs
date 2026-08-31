# v0.15 storage migration fixture

`v015_populated.regolith.tar.zst.b64` is a zstd-compressed tar of a regolith
store holding a database generated with DefraDB.rs v0.15.3 (`42d99dc1`). It
contains this schema:

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

## Provenance

v0.15.3 wrote to Redb, and this fixture began as `v015_populated.redb.zst.b64`.
Redb is no longer a backend, so the 66 key-value pairs were read out of the
original Redb file once and written into a regolith store, unchanged. The
migration under test is the document-layout one in
`crates/db/src/definition/migration/shortid.rs`, which is about key layout and
not about the storage engine, so it exercises the same code on the same bytes.

The Redb original is in git history at `0c8597b4^`, along with the backend that
could read it. Reading a v0.15 Redb file is a capability the tree no longer has:
a database still in that format has to be exported and reimported rather than
opened in place.

A regolith store is a directory, so this fixture is a tar rather than the single
file the Redb one was. The base64 wrapper keeps a small historical fixture
reviewable and portable through source-only packaging. Decode it with:

```sh
base64 --decode v015_populated.regolith.tar.zst.b64 | zstd --decompress | tar -x
```
