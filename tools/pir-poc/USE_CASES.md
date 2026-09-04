# Use cases

What the application needs to retrieve and why the request is sensitive.
Protocol choices, scale limits and costs live in [DECISIONS.md](DECISIONS.md).

## Mizu routing-tag alert and retrieval

A wallet discovers encrypted actions addressed to its routing tag without
downloading everyone's actions or telling a provider which tag it follows.

- **Request:** routing tag within a public block or catch-up window.
- **Result:** an online presence hint, followed by matching encrypted action
  pages; an offline wallet requests catch-up pages directly.
- **Privacy:** hide the routing tag and subsequent selected records. A bucket
  hint can include collisions; the wallet checks/decrypts retrieved actions.
- **Time/block filter:** yes for new blocks or a known activity range. Full
  recovery must still cover all required history; splitting it saves no work by itself.

## Mizu nullifier witness

Before spending, a wallet needs evidence that its nullifier has not already
been used. The active tree changes as new nullifiers are inserted.

- **Request:** nullifier and required committed tree root/checkpoint.
- **Result:** the predecessor leaf and Merkle siblings needed for an indexed-tree
  non-membership witness, verified against that root.
- **Privacy:** hide which nullifier the wallet is preparing to spend. An old
  witness is useful only while its root remains acceptable to the spend protocol.
- **Time/block filter:** no generic recent-block filter for a current-root
  witness. Older leaves and nodes can still be required.

## Shinzo historical logs

A wallet or researcher retrieves past contract events without disclosing the
address or event topic it is investigating.

- **Request:** address/topic filter within a public block range.
- **Result:** fixed pages of matching log projections, with continuation when needed.
- **Privacy:** hide the filter and selected records; the declared range remains public.
- **Time/block filter:** yes when only that range is wanted; not when the
  application needs the complete history.

## Shinzo receipt

An application checks a transaction's outcome and associated provenance without
revealing which transaction matters to it.

- **Request:** transaction hash, scoped to its known inclusion block/range.
- **Result:** receipt/status and the configured provenance or attestation fields.
- **Privacy:** hide transaction interest. Learning an unknown inclusion block is
  a separate lookup and must not silently reveal the transaction.
- **Time/block filter:** yes if the inclusion block/range is known; otherwise
  locating it remains part of the private-query problem.

## Shinzo contract-event alert

An application follows new events from a contract or topic without publicly
registering that selector.

- **Request:** register an address/topic selector and follow committed epochs.
- **Result:** a presence hint; retrieve the relevant log page separately.
- **Privacy:** hide the subscription predicate and matching retrieval. The hint
  is not the event payload.
- **Time/block filter:** naturally limited to new events per epoch. Historical
  catch-up is a separate log query.

## DefraDB document by ID

An authorized reader retrieves a document projection without telling the serving
provider which document it selected.

- **Request:** document ID within a public collection/generation.
- **Result:** configured fields, encrypted where required; larger documents need
  a separately private, padded retrieval step.
- **Privacy:** hide document interest without changing access permissions.
- **Time/block filter:** only when it safely identifies the requested version.
  A known collection/tenant can bound the lookup even without a time filter.

## DefraDB secondary index

An application privately looks up an exact field value, such as a routing tag or
status, which may match multiple documents.

- **Request:** field/value and page within a public collection/partition.
- **Result:** a fixed page of document locators and useful fields.
- **Privacy:** hide the value and selected documents. Publicly fetching a returned
  locator would undo the privacy of the first lookup.
- **Time/block filter:** yes for a range-scoped query; no if all matches across
  the collection's required history are wanted.

## DefraDB private change feed

An application learns that a collection update matches its equality filter
without sending that filter to the provider in plaintext.

- **Request:** register a collection/field/value selector.
- **Result:** a per-epoch hint, followed by private retrieval of changed projections.
- **Privacy:** hide the predicate; registration, polling and follow-up timing
  need separate protection.
- **Time/block filter:** naturally limited to new commits/epochs; recovering
  missed changes still requires covering the missed interval.

These are exact lookups, result pages and alerts—not arbitrary private GraphQL
queries, joins or writes. Mechanisms: [PROTOCOLS.md](PROTOCOLS.md).
