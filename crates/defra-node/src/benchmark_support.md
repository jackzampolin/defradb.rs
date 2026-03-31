# Dense Search V1 Reference

This file exists to sit next to the canonical executable reference for dense
search v1:

- `benchmark_support.rs`
- `benchmark_data_gen.rs`
- `search_chunks.rs`

Those files are the source of truth. This note is only a guide for consumers
such as `coding-data` that need to mirror the same shape.

## What DefraDB Owns

DefraDB owns the store and retrieval side:

- normalized document collections
- `@embedding` generation on write
- query-time embedding through an OpenAI-compatible `/embeddings` endpoint
- BM25 + dense similarity + reciprocal-rank fusion
- optional derived search chunks for oversized text fields

The generic dense-search implementation lives in:

- `crates/db/src/dense_search.rs`
- `crates/db/src/embedding.rs`

## What Stays Outside DefraDB

Source-specific parsing does not belong in DefraDB.

For the coding-data use case, external code should:

1. parse Codex / Claude / Gemini exports
2. normalize them into project / session / message / action documents
3. insert those normalized documents into DefraDB

DefraDB should not learn how to parse raw source logs.

## Canonical Coding Shape

The canonical reference schema is the fixture SDL in `benchmark_support.rs`.
It mirrors the production coding-data model:

- `CodingProject`
- `CodingSession`
- `CodingMessage`
- `CodingAction`
- `CodingSearchChunk`

`CodingMessage.content` and `CodingAction.command` are the natural search units.
That means most coding-session data should be indexed directly at the document
level.

`CodingSearchChunk` is the hidden chunking pattern for large text fields. Use it
when one logical source document is too large or mixed to rank well as a single
retrieval unit.

## Chunking Guidance

Do not force users to model chunk documents manually.

Instead:

- keep the normalized source documents as the primary data model
- derive chunk documents under the hood for oversized fields
- search over the derived chunks
- return enough parent metadata that results still feel like "a message in a
  session" or "an action in a session"

The generic chunk helper is in `search_chunks.rs`.

## Embedding V1 Contract

Dense search v1 is intentionally narrow:

- dense single-vector only
- one embedding vector per configured vector field
- OpenAI-compatible `/embeddings` API
- query and document vectors must come from the same compatible embedding family
- dot-product similarity
- BM25 + dense candidate fusion via RRF

Model-specific behavior such as pooling, query instructions, normalization, and
other asymmetric preprocessing is expected to be handled by the embedding
service, not by DefraDB v1.

## Current Non-Goals

These are not part of dense v1:

- sparse retrieval
- multi-vector / late-interaction retrieval
- ANN / vector indexes
- reranking
- source-specific raw export parsers inside DefraDB

## How To Read The Harness

Start here:

- schema and fixture shape: `benchmark_support.rs`
- normalized fixture generation: `benchmark_data_gen.rs`
- hidden chunk derivation: `search_chunks.rs`
- generic dense-search implementation: `crates/db/src/dense_search.rs`

If a future corpus wants search, the pattern should be:

1. normalize the source data into explicit DefraDB documents
2. decide which fields are natural search units
3. add derived chunk documents only for oversized fields
4. attach `@embedding` to the vector fields that should be generated on write
5. query through dense-search v1 using the same embedding family at read time
