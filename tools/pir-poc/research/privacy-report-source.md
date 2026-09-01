# End-to-end privacy beyond PIR: source report

**Audience:** DefraDB, Shinzo, and Shieldd engineers

**Date:** 2026-08-25

**Status:** Research synthesis; not a protocol specification

**Primary objective:** Maximize end-to-end privacy while continuing to measure aggregate server work as the principal performance cost.

## Scope

This report asks what must surround the selected PIR protocols so that a wallet's private read is not undone by its IP address, timing, result size, authentication, live-delivery path, or later transaction broadcast. It also considers alternatives that avoid a private query entirely and Ethereum ecosystem work that can be reused.

The intended user may be on a phone, but the design may favor desktop efficiency when the improvement is material. A full node and routine download of every generation are not assumed. At the time of this research report, the baseline was replicated Dense XOR for snapshots, Compact DPF for live subscriptions, and separate OHTTP paths for origin separation. The later GPU/epoch pass in `../USE_CASES.md` supersedes the live default with packed-presence Dense whenever fixed epoch latency is acceptable; immediate Compact DPF remains the fallback.

The source hierarchy was: standards and specifications, protocol papers, official project documentation, then implementation documentation. Current implementation status is time-sensitive and is stated as of the report date.

## Assumptions

- PIR replicas, network relays, gateways, cloud providers, and the public transaction observer may collude unless the selected topology states otherwise.
- A local ISP or mobile carrier can observe destinations, byte counts, and timing. A global passive observer can observe both ends of a route.
- The client device, application, and transport implementation are honest. A compromised wallet can read the query before any privacy protocol runs.
- Public generation, collection, result-size class, and coarse time-window leakage may be accepted by explicit policy. Query target and client identity should not be leaked.
- Correctness and privacy are separate. A response that is private but forged is not useful.
- More infrastructure is useful only when it adds an independent trust or mixing boundary. Three processes in one account or cloud are one privacy domain.

## Executive answer

PIR is necessary for strict target privacy, but insufficient for wallet privacy. The strongest practical architecture has six independent properties:

1. **Content privacy:** keep Dense XOR and Compact DPF for strict reads.
2. **Origin privacy:** use an anonymous, pluggable transport. OHTTP is the low-latency baseline; Tor/onion routing is the strong deployable tier.
3. **Traffic-analysis resistance:** the maximum-privacy tier is an epoch-batched, fixed-packet, three-provider anytrust mix with anonymous replies. If any one mix provider behaves correctly, the other providers and a global observer cannot trivially map ingress to egress. This costs latency, cover bandwidth, and operational complexity.
4. **Anonymous admission:** replace API keys, cookies, device IDs, and payment accounts with Privacy Pass tokens or a Semaphore/RLN-style anonymous membership proof.
5. **Verifiable data:** bind queries to one public generation and verify witnesses/projections against public commitments or a light-client-verified root.
6. **Unlinkable writes:** do not broadcast a later spend through the same provider, session, or timing pattern. Use an anonymous broadcast path, gas sponsorship, and, where applicable, an encrypted mempool.

The most useful outside-the-box principle is: **the most private query is a query that never occurs.** Public, identical epoch packs; locally maintained witnesses; one-time discovery tags; and broad encrypted event broadcast can replace some targeted reads. They trade bandwidth, semantics, or strict privacy and therefore complement rather than replace PIR.

## Threat and leakage matrix

| Observer | Current risk | Required defense |
|---|---|---|
| PIR replica | Sees a random share, generation, route, size, and arrival time | PIR plus fixed public request/result classes |
| OHTTP relay | Sees client IP, gateway, byte counts, timing | Independent operators; Tor before relay for the strong tier |
| OHTTP gateway | Sees one plaintext PIR share, relay, class, timing | No stable client fields; batching; separate gateway per share |
| Colluding relay and gateway | Reconstructs client-to-share mapping | Tor or an anytrust mix; OHTTP alone cannot solve this |
| Colluding PIR replicas | Dense/DPF target privacy fails | Independent replica domains; add a third Dense replica if its trust domain is genuinely independent |
| ISP/mobile carrier | Sees relay/Tor use and timing/volume | Tor pluggable transport or mixnet; fixed cadence and packets for global-observer resistance |
| Global observer | Correlates ingress and egress timing/volume | Delays, shuffling, cover, and anonymous replies; low-latency routing alone is insufficient |
| Push provider | Links a device token to match timing | No direct per-match push; use a fixed-cadence anonymous mailbox or broad common wakeups |
| Rate limiter/auth service | Links requests through API key, cookie, account, or payment | Unlinkable one-use tokens or anonymous membership/rate-limit proofs |
| Public chain/spend observer | Correlates a read with a later spend | Query-ahead, delay, separate transport, sponsored fees, fresh acting identity, and shielded application semantics |
| Compromised dapp/client | Reads target, keys, and plaintext | Isolated audited client, no analytics, uniform builds, local secret hygiene; network cryptography cannot repair compromise |

## Finding 1: OHTTP is a good baseline, not the maximum

[RFC 9458](https://www.rfc-editor.org/rfc/rfc9458.html) gives a clean two-role separation. The relay sees the client connection and opaque message; the gateway sees the plaintext request and the relay. It requires HTTPS on both hops, different relay/gateway operators, fresh HPKE context for each request, authenticated gateway key configuration, and removal of identifying headers. The RFC explicitly leaves traffic analysis out of scope and provides no privacy if relay and gateway collude. Key rotation also matters because OHTTP does not provide forward secrecy during a gateway key configuration's lifetime.

The POC's independent relay/gateway path per replica is therefore the correct minimum topology. Production must also ensure that the paths do not converge in the same CDN, cloud account, logging pipeline, or administrative operator. One relay for both Dense shares becomes a stable cross-share correlation point even though it cannot decrypt either share.

OHTTP should remain as a request envelope even inside stronger transports: it authenticates the gateway, prevents intermediate modification, strips HTTP metadata, and makes transport substitution easier. It should not be described as protection against a global observer.

### Deployable hardening

- Use one gateway and relay trust domain per replica.
- Fetch one common signed gateway/config manifest through a cache or bundle; never issue per-client key configurations.
- Use fresh HPKE contexts and short, overlapping key epochs, then destroy retired private keys.
- Strip all unknown headers; prohibit cookies, stable authorization fields, trace IDs, and per-client retry state.
- Return identical encrypted success and application-error envelopes within a public class.
- Use only a small set of manifest-defined request/response classes. Power-of-two padding is too wasteful for the POC's 19.4 MB tag response.
- Add randomized delay within a common public epoch. Random jitter by itself is not a global-observer defense, but it removes trivial same-millisecond cross-share matching.

## Finding 2: Tor is the strongest near-term mobile/desktop transport

Tor onion services avoid an exit and hide the service location through introduction and rendezvous circuits. Tor remains a low-latency anonymity network and therefore does not defeat a sufficiently capable end-to-end timing observer. It is still a major improvement over OHTTP alone because a relay/gateway coalition does not directly learn the wallet IP.

The Ethereum Foundation's Reads workstream is directly relevant. Its [Abstract Access Layer roadmap](https://reads.ethereum.foundation/roadmap/) defines a pluggable interface so wallets can swap Tor, mixnets, or future transports without changing application logic. [Power to the Edges](https://reads.ethereum.foundation/feed/anon-rpc/) proposes an anonymous `fetch`-like interface, isolated client execution, content-hash validation, and browser-friendly WebRTC transport. Defra should copy the interface boundary, not its still-evolving wire format.

[TorJS](https://reads.ethereum.foundation/docs/torjs/) is Arti, the Tor Project's Rust client, compiled to WASM. It is a real and relevant browser-wallet prototype, not yet a hardened production dependency. The project's own engineering report lists an experimental TLS crypto backend, reduced browser timer precision, lack of process isolation from a hostile dapp, fast-bootstrap IP exposure, and WASM fingerprinting. Native mobile or desktop Arti should be evaluated before embedding TorJS. If TorJS is tested, put it in a dedicated worker/process boundary and skip fast bootstrap in the maximum mode.

Recommended strong tier:

```text
wallet -> native Arti/Tor -> OHTTP relay -> OHTTP gateway/PIR replica
                       or -> replica onion gateway
```

OHTTP inside Tor is not redundant. Tor hides origin from the OHTTP relay; OHTTP limits the application metadata available to the exit/relay path and keeps the gateway interface identical across transports.

## Finding 3: maximum metadata privacy needs mixing, delay, and cover

Low-latency relaying preserves enough timing for end-to-end correlation. Stronger systems deliberately make latency and bandwidth less informative:

- [Loopix](https://www.usenix.org/system/files/conference/usenixsecurity17/sec17-piotrowska.pdf) uses layered Sphinx packets, Poisson mixing delays, loop traffic, and cover traffic. Its evaluation reports seconds of end-to-end latency and more than 300 messages/second per mix node.
- [Vuvuzela](https://people.csail.mit.edu/dtl/pdf/lazar-vuvuzela.pdf) adds differential-privacy noise and repeated shuffling through an anytrust server chain. Its paper reports 68,000 messages/second for one million users with about 37 seconds latency, while protecting metadata when every server except one is controlled.
- [Stadium](https://people.eecs.berkeley.edu/~matei/papers/2017/sosp_stadium.pdf) horizontally distributes verifiable mix chains. It reports support for more than 20 million simultaneous users with 100 providers at roughly two minutes latency, with one honest provider required in each mix chain.
- [Nym's Sphinx SURBs](https://www.nym.com/nym-whitepaper.pdf) show how a service can answer without learning the sender's network address. A Single-Use Reply Block encodes a one-time reverse mix route and reply key.

This is the concrete way to realize the previously attractive “three servers, one honest” idea for origin and timing metadata. Dense XOR already has an anytrust content-privacy property: target privacy survives if one replica does not collude. A three-provider batch mix can give a comparable anytrust statement for ingress-to-egress mapping. These are separate server sets and separate claims.

### Target maximum-privacy path

```text
common epoch E
    wallet creates one fixed packet per replica share
    packet includes a one-time reply route/mailbox capability
        -> mix provider 1: unwrap, add cover, shuffle
        -> mix provider 2: unwrap, add cover, shuffle
        -> mix provider 3: unwrap, add cover, shuffle
        -> gateway i: decrypt OHTTP, batch by generation/protocol
        -> PIR replica i
        -> fixed chunks through SURB/mailbox on later epochs
```

Security requires at least one honest mix provider, uniform packet formats, a sufficiently populated epoch, active-attack defenses, and no identifier that survives the shuffle. Merely chaining three HTTP proxies preserves timing and does not provide this property.

The maximum tier is unsuitable for every interactive query. It should be explicit and latency-tolerant. A practical first experiment should test 1, 5, 15, and 30 second epochs and batch sizes from 32 to 4,096 before attempting continuous Vuvuzela-style cover.

### Avoiding a cover-traffic server-work explosion

Dummy Dense queries would impose a full scan on every PIR replica and conflict with the project's primary objective. Instead:

- Generate cover as indistinguishable fixed packets at clients and mix servers.
- Reveal a no-op only after the honest shuffle boundary, at the gateway, and return a fixed response without evaluating PIR.
- Batch real requests across clients at each gateway and evaluate them with shared-row traversal to amortize memory traffic.
- Keep the no-op/success response schedule identical at the mix boundary.

The gateway can distinguish a no-op after decryption, but after one honest shuffle it cannot map that item to a particular ingress. This optimization is not safe in plain OHTTP when the relay and gateway are allowed to collude.

## Finding 4: request and response shape are first-class secrets

The selected POC has naturally distinct shapes: Compact DPF is hundreds of bytes, active nullifier retrieval is tens or hundreds of kilobytes, and the billion-document tag result is about 19.4 MB per share. Making every operation look like the largest class would be counterproductive.

Use a small, public privacy-class taxonomy:

| Class | Example | Envelope policy |
|---|---|---|
| S | Compact DPF registration/poll | Fixed 1 KiB request and response |
| M | Active-nullifier witness | Fixed generation-specific request and fixed 64 KiB response class |
| L | Large tag projection | Fixed-size chunks on a constant class cadence; fixed maximum stripe/result class |

The class, generation, and optional public time window remain explicit leakage. Inside a class, hide success, miss, result cardinality, errors, retries, and exact completion time. A large result should be chunked so memory use and packet boundaries are uniform. A class-specific connection should not be reused as a long-lived wallet identifier.

Independent replica shares must not be sent simultaneously over visibly paired paths. Schedule them independently within the same common epoch and use different relay/gateway domains.

## Finding 5: rate limiting must not re-identify the wallet

Ordinary API keys, login cookies, payment accounts, stable device attestations, and push tokens undo origin privacy.

[Privacy Pass, RFC 9576](https://datatracker.ietf.org/doc/rfc9576/), separates token issuance from redemption. The origin verifies an unlinkable token without learning the issuance interaction. It is the best near-term admission mechanism for public PIR infrastructure. Clients should acquire a batch of common-denomination, common-epoch tokens well before use; one token is redeemed inside each encrypted request. Issuer and gateway should be separate organizations, and issuance metadata must be deliberately coarse because metadata can partition an anonymity set.

Ethereum provides a stronger option when access depends on anonymous group membership. [Semaphore](https://docs.semaphore.pse.dev/) proves that a client belongs to a Merkle-committed group and uses a nullifier to prevent double signaling without revealing the member. Waku's RLN applies this pattern to anonymous rate-limited messaging. For Defra, a proof can bind `service || epoch || quota-class` and produce one rate-limit nullifier per slot. This is heavier than Privacy Pass but useful for authorized cohorts where an issuer must not mint transferable bearer tokens.

Recommendation:

- Public service and ordinary abuse control: Privacy Pass.
- Cohort-authorized private collections: Semaphore/RLN-style membership proof with an epoch-scoped nullifier, carried inside OHTTP.
- Never expose the membership proof to the relay; never bind it to a wallet address, transport circuit, or long-lived device key.

## Finding 6: live delivery needs an anonymous mailbox, not device push

Compact DPF hides a registered predicate from two non-colluding servers, but delivery timing can reveal exactly when that predicate matched. Sending the result to APNs/FCM or a persistent socket associates a device or session with the event.

The strict design is:

1. Evaluate DPF registrations into fixed encrypted event capsules.
2. Deposit answer shares into epoch-scoped, fixed-capacity mailboxes.
3. Let clients poll mailboxes on a common cadence through Tor or the mix path.
4. Return a fixed number of capsules, including cover, per epoch.
5. Use a one-time reply/mailbox capability, not a device token.

[Waku](https://docs.waku.org/learn/concepts/protocols/) is useful only with care. Relay can distribute broad encrypted common-topic capsules and RLN can provide anonymous spam resistance. Waku's own security documentation says direct Store and Filter connections expose a PeerID and selected content topic. Therefore, do not use a wallet-specific Filter/Store topic as the privacy mechanism. Use one broad topic, coarse public buckets, or reach Store through the anonymous transport.

[Fuzzy Message Detection](https://protocol.penumbra.zone/main/crypto/fmd.html) offers tunable false positives and no false negatives, but should not replace Compact DPF for strict live privacy. The [private signaling analysis](https://www.usenix.org/system/files/sec22-madathil.pdf) describes FMD as k-anonymous rather than fully private and cites recipient-unlinkability/intersection attacks. Higher privacy also forces each recipient to process a fraction of all messages. FMD remains interesting as an optional noisy broadcast layer, not as the core claim.

The same private-signaling paper presents a two-server garbled-circuit design with strong privacy and constant recipient work. It is worth a later benchmark only if Compact DPF's two-party semi-honest model or mailbox semantics become inadequate; current DPF event evaluation is already far below transport overhead.

## Finding 7: sometimes eliminate the targeted query

### Public epoch packs and local trial decryption

[Zcash ZIP 307](https://zips.z.cash/zip-0307) uses compact blocks so light wallets can download the same public stream and trial-decrypt locally. The privacy lesson is to broadcast a compact common object, then privately retrieve only a large payload if needed. For Shinzo, a generation can publish compact fixed-size announcement pages, encrypted event capsules, or commitment deltas through a cache/CDN. Everyone in a privacy cohort receives the same bytes.

This is not viable for every document or all generations, but it can remove many small discovery queries and make query timing less coupled to wallet activity.

### Maintain the active nullifier witness from public updates

The ideal active-nullifier read is no read: a wallet caches its witness and applies the same public per-block update digest as every other wallet. [Utreexo](https://www.dci.mit.edu/projects/utreexo) demonstrates cached proof updates for a dynamic hash accumulator, while [vector commitments with efficient updates](https://eprint.iacr.org/2023/1830.pdf) study compact proof maintenance.

There is no free asymptotic escape at high update rate. The vector-commitment paper proves a lower bound proportional to the number of changed entries and their identifying information for public proof updates. Utreexo is also an inclusion accumulator for a UTXO-style set, not a drop-in non-membership witness for Shieldd's linked indexed nullifier tree. The useful experiment is therefore concrete: encode Shieldd's actual changed predecessor and sibling nodes per block, compress them, and measure identical CDN bytes/day plus phone witness-update CPU. If acceptable, this removes target leakage and PIR server work for continuously synchronized clients. Cold clients still need PIR or a larger catch-up pack.

### One-time pseudorandom discovery tags

[Aztec note discovery](https://docs.aztec.network/developers/docs/foundational-topics/advanced/storage/note_discovery) uses contract-siloed, one-time tags derived from a shared secret and counter. The node can index a pseudorandom tag for O(1) retrieval, while unknown senders require a handshake tradeoff. The documentation explicitly notes that ordinary tag queries still reveal IP and exact transaction selection.

For Shinzo, ratcheted per-relationship/per-contract tags plus Tor/OHTTP create a very low-server-work “semantic obscurity” mode. The provider sees the random token and selected record but may not know its wallet meaning. It is not strict query privacy and is vulnerable to timing, counter desynchronization, relationship compromise, and server knowledge of the tag-record link. Keep it as an opt-in fast path, not the protocol marketed as PIR.

[ERC-5564](https://eips.ethereum.org/EIPS/eip-5564) view tags provide a related design pattern: reveal a few public filter bits to reduce local trial-decryption work. Every extra bit partitions the anonymity set, so use only a globally fixed small width and measure the resulting bucket population.

## Finding 8: verifiability prevents privacy-preserving lies and tagging

A malicious replica can omit, corrupt, replay, or selectively delay an answer. Correctness checks reduce both integrity failures and active tagging opportunities.

- Authenticate a common generation manifest and reject replica divergence before query generation.
- Verify the returned nullifier path against a public Shieldd root.
- Bind projection AEAD to generation, collection, ordinal/stripe, and public result class.
- Obtain roots through a public consensus/light-client path rather than the queried provider. [Ethereum light clients](https://ethereum.org/developers/docs/nodes-and-clients/light-clients/) illustrate the separation: a small client verifies provider data against sync-committee consensus while still relying on a remote source for availability.
- Make failures and retries fixed-shape and delayed to the next epoch. Immediate retries can reveal client distance and link requests; RFC 9458 calls out this timing issue.
- Publish one signed, globally consistent operator/config manifest. A transparency log or Ethereum contract can commit gateway keys, mix operators, privacy classes, generation roots, and client binary hashes, preventing a provider from serving a unique fingerprinting configuration to one wallet.

The Ethereum Foundation's Unified Binary Tree work is conceptually aligned: serve a PIR-friendly representation while proving it equivalent to canonical chain state. It is currently a roadmap item, not a component to import.

## Finding 9: private reads are lost if the later write is linkable

The [Ethereum privacy roadmap](https://ethereum.org/roadmap/privacy/) explicitly separates private reads, private writes, and private proving. A wallet that privately retrieves a nullifier witness and immediately broadcasts a spend through the same RPC, IP, or session creates a high-confidence timing link.

Minimum write-path policy:

- Query ahead on a common schedule and add a randomized, policy-bounded delay before spending.
- Use a transport/circuit/provider distinct from every read-share path.
- Submit through Tor/mix transport, not the PIR gateway.
- Use a fresh acting identity where application semantics allow it.
- Avoid funding that identity from a known wallet immediately before use.
- Use an [ERC-4337](https://eips.ethereum.org/EIPS/eip-4337) bundler and paymaster, or an equivalent sponsored relayer, so gas funding does not directly link the fresh actor. The bundler must itself be reached anonymously and still sees the operation.
- Use [Shutter threshold encryption](https://docs.shutter.network/docs/shutter) where supported to hide transaction contents during the mempool phase. Shutter mitigates early disclosure/front-running; it does not hide network origin or final onchain data.

[Dandelion++](https://arxiv.org/abs/1805.11060) is a lightweight stem-then-fluff P2P origin defense, but subsequent [NDSS analysis](https://www.ndss-symposium.org/ndss-paper/on-the-anonymity-of-peer-to-peer-network-anonymity-schemes-used-by-cryptocurrencies/) finds weak anonymity under strategically colluding peers. It is defense in depth if the target network implements it, not a substitute for Tor or a mixnet.

## Finding 10: client and operational hygiene are part of the protocol

- Put query construction, transport, and local decryption in an audited native component or isolated worker. A dapp sharing the same JavaScript context can inspect memory.
- Do not send secrets, tags, nullifiers, queried CIDs, or timing to analytics, crash reporting, URLs, logs, support traces, or clipboard history.
- Use uniform released client builds and common configuration epochs. Exotic per-user privacy settings can fingerprint their users.
- Encrypt local caches and witnesses; erase expired HPKE contexts, reply capabilities, and decrypted projections.
- Do not expose a stable device attestation unless it is blinded into an anonymous token.
- Publish aggregate, delayed operational metrics only. Never log raw IP, selector/share digest, exact query time, request correlation ID, or mailbox identifier.
- Put replicas, relays, mix providers, token issuers, config signers, and write relayers in independent legal and administrative domains. Prefer different clouds, autonomous systems, and jurisdictions where practical.
- Document the actual collusion statement and audit the organization graph. “Three servers” is not a privacy claim without independence.

## Recommended architecture

### Tier 1: private by default, low latency

- Existing two-replica Dense/DPF protocols.
- Independent OHTTP relay/gateway path per share.
- Manifest-defined fixed classes and fixed application errors.
- Privacy Pass admission.
- Public signed configuration and generation roots.
- No analytics or stable client identifiers.
- Separate anonymous transaction broadcaster.

This hides the target from one non-colluding replica and the IP from one non-colluding relay/gateway pair. It does not resist global timing analysis or relay/gateway collusion.

### Tier 2: strong deployable privacy

- Everything in Tier 1.
- Native Arti/Tor for mobile/desktop; onion gateways where possible.
- Common epoch scheduling, independent per-share jitter, route/circuit isolation.
- Fixed-cadence mailbox for live answers.
- Query-ahead and write-path separation.

This is the recommended production “maximum” until a mix service is operated and measured.

### Tier 3: maximum metadata privacy target

- Three-provider anytrust batch mix for each share path.
- Fixed Sphinx-like packets, shuffle verification, cover, active-attack handling.
- One-time anonymous replies/SURBs and fixed-cadence chunk delivery.
- Gateway-side cover termination and cross-client batch evaluation.
- Formal differential-privacy/accounting parameters for observable counts.

This can target one-honest-provider metadata privacy and global-observer resistance. It accepts seconds-to-minutes latency, added bandwidth, and much more operational complexity.

## Prioritized engineering program

| Priority | Experiment | Decision metric |
|---:|---|---|
| P0 | Add an observer/leakage test harness around the OHTTP demo | No stable headers/IDs; fixed success/error sizes; trace contains only declared class leakage |
| P0 | Define S/M/L privacy classes and fixed chunk/error behavior | Byte/timing classifier cannot distinguish hit/miss inside a class above chance in a controlled trace set |
| P0 | Privacy Pass proof of concept | Issuance cannot be linked to redemption; gateway admits one-use common-epoch tokens without client identity |
| P0 | Native Arti/Tor benchmark plus onion gateway | Cold/warm startup, RAM, battery, latency, and failure behavior on desktop plus one phone |
| P1 | Epoch batch gateway for real Dense shares | Aggregate server work/query, queue delay, and linkability at 32–4,096 clients; test shared-row traversal |
| P1 | Three-node re-encrypting shuffle prototype with one honest node | Ingress/egress matching accuracy under 0/1/2 corrupt mixes, active drops/delays, and varying batch size |
| P1 | SURB or anonymous mailbox result path | Gateway answers without device/network identity; fixed response cadence and replay safety |
| P1 | Live DPF mailbox; remove direct push | Match and miss traces indistinguishable inside one class; server work and mobile polling cost |
| P1 | Public active-nullifier update packs | Compressed bytes/day, cold catch-up bytes, phone CPU/RAM, and percentage of PIR queries eliminated |
| P2 | Semaphore/RLN cohort admission | Proof generation on phone, verification work, quota semantics, and anonymity-set partitioning |
| P2 | Aztec-style ratcheted tag fast path | Server work, recovery/desync behavior, tag unlinkability, and exact declared leakage vs strict PIR |
| P2 | Broad Waku encrypted event feed | Bandwidth and availability without wallet-specific Store/Filter topics |
| P2 | Separate write broadcaster with sponsored fees and optional Shutter | Read-to-spend timing classifier and funding-link analysis |
| Research | Vuvuzela/Stadium-class continuous cover service | Formal privacy parameter, per-user bytes/day, latency, provider cost, and adoption threshold |

Every benchmark should report aggregate server CPU/work, gateway/mix work, client CPU/RAM, upload, download, latency distribution, anonymity assumptions, and observable leakage. Privacy experiments should retain packet traces and train a simple linkability classifier; “we added jitter” is not evidence.

## Rejected as primary guarantees

| Option | Why it is not the primary guarantee |
|---|---|
| HTTPS proxy, VPN, CDN | Moves trust to one operator; collusion and timing reconstruct the link |
| OHTTP alone | Strong role separation but explicitly no global traffic-analysis protection and no collusion resistance |
| Tor alone | Strong practical origin privacy, weak against a global end-to-end timing observer |
| Three ordinary proxies | No shuffle, uniform packets, or cover; timing survives every hop |
| Waku Filter/Store on wallet-specific topics | PeerID/topic linkability is documented by Waku |
| FMD as strict live privacy | Tunable k-anonymity and intersection risk; client work grows with false positives |
| Dandelion++ as write privacy | Useful lightweight P2P defense but weak under strategic collusion |
| Shutter as origin privacy | Hides mempool content until release, not client IP or final public state |
| TEE as the main privacy boundary | Hardware/vendor trust, attestation linkability, side channels, and catastrophic single-domain compromise |
| Continuous dummy PIR | Converts privacy cover directly into full database scans and violates the server-work objective |
| Per-client padding/configuration | Creates a stable fingerprint even if it reduces one local leak |

## Limitations and open questions

- This pass is a design and source review, not an anonymity measurement of the current code.
- TorJS and the Ethereum Abstract Access Layer are moving projects. Use their interface ideas now, but gate production dependencies on external audits and stable releases.
- Anytrust mixes need real independent operators and enough simultaneous traffic. A private deployment with one active wallet cannot obtain a large crowd without costly cover.
- Differential privacy degrades under repeated observations and requires explicit composition/accounting. No parameter is selected here.
- The active-nullifier public-update idea may be bandwidth-prohibitive at Shieldd's target rate. The lower bound means the real data structure and update distribution must be measured.
- Anonymous credentials prevent stable authentication, not traffic correlation. Issuance cadence and metadata still matter.
- No network protocol protects a compromised wallet, malicious OS, screen capture, or subsequent voluntary identity disclosure.
- Some applications inherently reveal result-size class or public time window. These must be explicit product choices, not accidental leaks.

## Claim-to-source ledger

| Claim | Primary source | Confidence / caveat |
|---|---|---|
| OHTTP splits client IP and plaintext between relay/gateway; traffic analysis and collusion remain | [RFC 9458](https://www.rfc-editor.org/rfc/rfc9458.html) | High; IETF standard |
| Privacy Pass provides unlinkable issuance/redemption for authorization | [RFC 9576](https://datatracker.ietf.org/doc/rfc9576/) | High; architecture RFC, metadata still partitions |
| TorJS runs Arti in WASM but remains a functional, unaudited prototype with stated caveats | [TorJS docs](https://reads.ethereum.foundation/docs/torjs/), [engineering report](https://reads.ethereum.foundation/feed/embedding-arti-in-the-browser/) | High as of report date; status can change |
| Ethereum is standardizing a pluggable anonymous access layer and exploring query-mixing RPC aggregation | [EF Reads roadmap](https://reads.ethereum.foundation/roadmap/), [Power to the Edges](https://reads.ethereum.foundation/feed/anon-rpc/) | High for roadmap intent; not a stable standard |
| Loopix uses delay/cover for global-observer resistance with seconds latency | [Loopix paper](https://www.usenix.org/system/files/conference/usenixsecurity17/sec17-piotrowska.pdf) | High; research system, not this POC |
| Vuvuzela protects metadata with one honest server and reports 68k msg/s, 1M users, ~37 s latency | [Vuvuzela paper](https://people.csail.mit.edu/dtl/pdf/lazar-vuvuzela.pdf) | High; 2015 evaluation |
| Stadium horizontally scales an anytrust mix with minutes latency | [Stadium paper](https://people.eecs.berkeley.edu/~matei/papers/2017/sosp_stadium.pdf) | High; 2017 evaluation |
| SURBs provide one-time anonymous reply routes | [Nym whitepaper](https://www.nym.com/nym-whitepaper.pdf) | Medium-high; implementation/deployment details require validation |
| Waku direct Store/Filter reveals PeerID/topic; RLN offers anonymous rate limits | [Waku security](https://docs.waku.org/learn/security-features/), [protocols](https://docs.waku.org/learn/concepts/protocols/) | High; official docs |
| Semaphore proves anonymous group membership with a nullifier | [Semaphore docs](https://docs.semaphore.pse.dev/) | High; audited components, integration still needs review |
| Aztec uses one-time secret-derived discovery tags but tag queries still expose IP and selection | [Aztec note discovery](https://docs.aztec.network/developers/docs/foundational-topics/advanced/storage/note_discovery) | High; application assumptions differ |
| ERC-5564 view tags reduce trial decryption by leaking a small filter | [ERC-5564](https://eips.ethereum.org/EIPS/eip-5564) | High for construction; anonymity effect is a design inference |
| FMD is tunable false-positive filtering with weaker repeated-use privacy | [Penumbra FMD](https://protocol.penumbra.zone/main/crypto/fmd.html), [private signaling paper](https://www.usenix.org/system/files/sec22-madathil.pdf) | Medium-high; attack applicability is parameter/workload dependent |
| Public compact streams plus local trial decryption are a deployed wallet-sync pattern | [ZIP 307](https://zips.z.cash/zip-0307) | High; bandwidth differs from Shieldd |
| Cached accumulator proofs can be publicly updated; public update information has a changed-entry lower bound | [Utreexo](https://www.dci.mit.edu/projects/utreexo), [efficient VC updates](https://eprint.iacr.org/2023/1830.pdf) | High conceptually; not a drop-in Shieldd construction |
| ERC-4337 bundlers/paymasters can separate acting identity from gas funding | [ERC-4337](https://eips.ethereum.org/EIPS/eip-4337), [Ethereum privacy app guide](https://ethereum.org/latest/privacy-apps-on-ethereum/) | High; bundler/network/onchain metadata remains |
| Shutter hides transaction content during the mempool phase via threshold encryption | [Shutter docs](https://docs.shutter.network/docs/shutter) | High for intent; deployment support varies |
| Dandelion++ is lightweight origin obfuscation but later analysis finds limited anonymity under collusion | [Dandelion++](https://arxiv.org/abs/1805.11060), [NDSS 2023 analysis](https://www.ndss-symposium.org/ndss-paper/on-the-anonymity-of-peer-to-peer-network-anonymity-schemes-used-by-cryptocurrencies/) | High; models and deployments differ |
| Ethereum's end-to-end roadmap separates private reads, writes, and proving | [Ethereum privacy roadmap](https://ethereum.org/roadmap/privacy/) | High; roadmap proposals are not all deployed |
