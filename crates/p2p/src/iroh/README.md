# Iroh Transport Notes

DefraDB's Iroh transport uses the same `P2PTransport` surface as libp2p, but the
wire mechanics are intentionally different where Iroh has different primitives.

## Direct streams instead of `pubsub_rpc`

DocSync and BranchableSync use Iroh QUIC bidirectional streams rather than the
`pubsub_rpc` layer introduced for libp2p in #828. The `pubsub_rpc` topic model is
tied to libp2p peer IDs and gossipsub topic meshes. Iroh peers are addressed by
`EndpointId`, so reusing the libp2p response-topic convention would require a
peer-id translation layer that does not exist on the Go-compatible wire path.

Iroh therefore maps request/response traffic onto dedicated ALPNs:

- `/defra-iroh/docsync/0.1` and `/defra-iroh/docsync/0.1/resp`
- `/defra-iroh/branchable/0.1` and `/defra-iroh/branchable/0.1/resp`
- `/defra-iroh/se-query/0.1/req` and `/defra-iroh/se-query/0.1/resp`

The CBOR message structs stay shared with libp2p; only the transport envelope is
Iroh-specific.

## Libp2p-only transport methods

These `P2PTransport` methods are libp2p-only by design:

- `publish_raw`
- `subscribe_raw`
- `register_pubsub_rpc_topic`

They exist for libp2p's `pubsub_rpc` dispatcher and are left as the default
not-supported/no-op behavior on Iroh.

`topic_peers()` is also not a full libp2p equivalent on Iroh. libp2p can ask
gossipsub for all peers known for a topic. `iroh-gossip` exposes the direct
neighbors of a joined topic, so Iroh returns those topic-scoped neighbors rather
than all connected peers. This keeps the result topic-specific, but callers must
not treat it as complete topic membership.

## Audit follow-up references

This file documents the Iroh transport decisions behind #965, #966, #967, and
#968, and the surrounding P2P audit work tracked by #962 through #968.
