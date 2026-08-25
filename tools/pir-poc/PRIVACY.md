# Privacy architecture beyond PIR

**Decision date:** 2026-08-25

**Scope:** DefraDB PIR sidecar, Shinzo discovery, and Shieldd wallet reads

**Objective:** Maximize end-to-end privacy while treating aggregate server work as the primary performance cost.

## POC scope decision

PIR should remain the strict content-privacy layer, but it is only one layer. A private selector is not useful if the same provider learns the wallet IP or the reconstructed answer is not verified.

The POC intentionally ships only two origin paths behind one minimal transport interface:

| Mode | Path | Protects against | Main cost |
|---|---|---|---|
| Default | Independent OHTTP relay/gateway path per PIR share | A non-colluding relay/gateway pair and a non-colluding PIR replica | Low latency; no global timing protection |
| Strong experiment | Tor-compatible `socks5h`, then OHTTP | Relay/gateway collusion and ordinary origin observation | Tor startup, latency and some fingerprint risk |

It also uses one fixed OHTTP envelope in the demo, verifies Shieldd nullifier paths, authenticates encrypted tag projections and authenticates one consistent generation manifest. This is consistent with the Ethereum Foundation's private-read direction: content privacy, pluggable origin transport and locally verifiable results remain separate layers.

Mixnets, anonymous mailboxes, Privacy Pass, Semaphore/RLN, Waku, FMD, continuous cover and a separate private write path remain research notes below. They are not current POC dependencies or implied production requirements. Revisit one only after a measured threat requires it.

## Target architecture

```text
                                   public common state
                         generation/config/root transparency log
                                      |          |
                                      v          v
wallet private core -> scheduler -> anonymous admission -> one request per replica
      |                   |                   |
      |                   |                   +-- Privacy Pass, or Semaphore/RLN
      |                   +-- public epochs, fixed classes, query-ahead
      |
      +-- Dense XOR snapshot shares / Compact-DPF registration shares
                    |                              |
             path A, independent            path B, independent
                    |                              |
          OHTTP / Tor / mix A             OHTTP / Tor / mix B
                    |                              |
               gateway A                     gateway B
                    |                              |
               replica A                     replica B
                    \                              /
                     fixed anonymous reply/mailbox
                                  |
                         verify, combine, decrypt
                                  |
                   delayed separate write broadcaster
```

For three-replica Dense XOR, add a third independent path. Dense target privacy survives if at least one replica does not collude, but all configured answers are still required. The current Compact DPF remains exactly two-party.

Independence is an organizational property, not a process count. Replicas and paths should not share the same operator, cloud account, CDN logs, observability pipeline, or administrative control. Different clouds, autonomous systems, and jurisdictions are useful where practical.

## What each layer fixes

| Layer | Defense | Residual leakage |
|---|---|---|
| Query content | Dense XOR / Compact DPF | Protocol, generation, request class, timing |
| Client origin | OHTTP | Relay/gateway collusion and global timing |
| Strong origin | Tor/Arti, preferably onion gateway | End-to-end timing and traffic volume |
| Global metadata | Anytrust batch mix, cover, anonymous replies | Participation in the service; configured privacy class |
| Shape | Fixed request/result classes and chunk cadence | The public class itself |
| Admission | Privacy Pass or Semaphore/RLN | Coarse issuer/cohort/epoch metadata |
| Correctness | Public manifest/root, path and AEAD verification | Availability; a server can still refuse service |
| Live delivery | Fixed-cadence anonymous mailbox | Mailbox class and polling epochs |
| Later spend | Separate anonymous broadcaster, delay, sponsored gas | Whatever the application deliberately publishes onchain |
| Client | Isolated audited core, no analytics or stable IDs | Compromised OS/device remains fatal |

## Network origin and timing

### OHTTP baseline

[RFC 9458](https://www.rfc-editor.org/rfc/rfc9458.html) cleanly separates roles: the relay sees the client and encrypted request; the gateway sees the request and relay. It requires independent operators, HTTPS on both hops, fresh HPKE state per request, authenticated gateway keys, and strict header hygiene. The RFC explicitly does not solve traffic analysis, and relay/gateway collusion reconstructs the client-to-query link.

Keep the POC's independent path per share, then add:

- one globally consistent signed gateway/config manifest;
- short overlapping gateway-key epochs and deletion of retired keys;
- no cookies, API keys, trace IDs, client certificates, device IDs, or unknown forwarded headers;
- fixed encrypted success and application-error responses inside each class;
- independent scheduling of replica shares within a common public epoch;
- aggregate delayed metrics only—never raw IP, selector digest, mailbox ID, or exact request timestamp.

OHTTP should stay inside stronger modes. Tor hides the client from the OHTTP relay; OHTTP keeps application metadata away from intermediary transport nodes and preserves one serving interface.

### Tor/Arti strong mode

The Ethereum Foundation is building almost exactly the transport boundary needed here. Its [Abstract Access Layer](https://reads.ethereum.foundation/roadmap/) aims to let wallets swap Tor, mixnets, and future networks without application changes. [Power to the Edges](https://reads.ethereum.foundation/feed/anon-rpc/) proposes an anonymous `fetch` abstraction with isolated, hash-validated client modules.

Adopt that interface idea now: the PIR client should depend on an `anonymous request` trait, not directly on OHTTP sockets. Do not wait for or depend on the still-evolving Ethereum wire standard.

[TorJS](https://reads.ethereum.foundation/docs/torjs/) is a valuable browser prototype: it compiles the Tor Project's Rust Arti client to WASM and exposes a `fetch`-like API. Its own [security review](https://reads.ethereum.foundation/feed/embedding-arti-in-the-browser/) calls it functional rather than hardened and identifies an experimental TLS crypto backend, same-context dapp access, fast-bootstrap IP exposure, timing limitations, and WASM fingerprinting. Evaluate native Arti first for desktop/mobile. If TorJS is used, isolate it in a worker/process and make direct Tor bootstrap the maximum-mode default.

### Three-provider anytrust mix

A mix mode should use common epochs and fixed packets. Each provider removes one encryption layer, adds cover according to a public mechanism, and shuffles. The gateway receives a batch with no surviving ingress identifier. The request carries a one-time anonymous reply capability, such as a Sphinx [Single-Use Reply Block](https://www.nym.com/nym-whitepaper.pdf), so the response can return without a socket or device token.

Privacy depends on at least one honest mix provider, adequate batch population, uniform packets, active-attack defenses, and explicit differential-privacy accounting for observable counts. More hops without re-encryption, shuffling, and cover do not provide this property.

Do not send dummy Dense selectors to the database. They each cause a full scan. In the mix design, dummy packets can become distinguishable no-ops only after the honest shuffle boundary at the gateway; the gateway returns a fixed response without PIR evaluation. Real shares can be grouped by generation and evaluated in batches to amortize memory traversal.

## Traffic shape and timing

The current protocols are too different to hide in one universal envelope. Use a small set of public classes instead:

| Class | Current example | Policy |
|---|---|---|
| Small | Compact DPF | Fixed 1 KiB request/response envelope |
| Medium | Active-nullifier path | Generation-specific fixed request and fixed response class |
| Large | Billion-document tag projection | Fixed chunks, fixed maximum result/stripe class, constant chunk cadence |

Within one class, hits, misses, errors, retries, and result cardinality should be indistinguishable. The class, generation, and an explicitly supported coarse time window may remain public.

Random jitter alone does not stop a global observer. It is still useful to remove trivial cross-share matching. The strong policy is common global epochs plus independent random placement inside an epoch. Query ahead instead of immediately before a spend.

The 19.4 MB tag-share response deserves special treatment because its traffic trace is distinctive. Stream it in fixed chunks and bound peak client memory. Power-of-two padding is a poor default here: the existing benchmark inflated it to 33.6 MB.

## Anonymous authorization and abuse control

Stable authentication silently destroys origin privacy.

[Privacy Pass](https://datatracker.ietf.org/doc/rfc9576/) is the preferred public-service mechanism. A client obtains unlinkable tokens from an issuer and later redeems one at a gateway. Acquire common-denomination tokens in batches, well before queries; use one token per request; keep issuer and gateway independent; and use coarse common issuance epochs. Blindness does not hide unusual issuance timing or metadata.

For private collections or cohort access, reuse Ethereum's anonymous-membership pattern. [Semaphore](https://docs.semaphore.pse.dev/) proves membership in a Merkle-committed group without identifying the member and uses a nullifier to prevent duplicate signaling. Waku's RLN applies the same pattern to anonymous rate limiting. Bind a proof to `service || cohort || epoch || quota class`, carry it inside OHTTP, and reveal only an epoch-scoped rate-limit nullifier to the gateway.

Use Privacy Pass for simple bearer admission; use Semaphore/RLN only when anonymous cohort membership is part of authorization. Neither should be exposed to the relay.

## Live subscriptions and delivery

Compact DPF already makes event evaluation cheap and hides the subscription predicate from two non-colluding replicas. The remaining privacy risk is delivery metadata.

Do not push a match directly to APNs/FCM or a persistent wallet connection. Instead:

1. produce fixed encrypted answer capsules for every event epoch;
2. deposit shares in a fixed-capacity, epoch-scoped mailbox;
3. poll through Tor/mix on a common cadence;
4. return a fixed number of capsules, including cover;
5. rotate the one-time mailbox/reply capability.

[Waku](https://docs.waku.org/learn/concepts/protocols/) can distribute a broad encrypted common-topic feed and RLN can protect it from anonymous spam. Do not use wallet-specific Waku Filter or Store topics as the privacy layer: [Waku's security documentation](https://docs.waku.org/learn/security-features/) states that direct Store/Filter peers can link PeerID to queried topic.

[Fuzzy Message Detection](https://protocol.penumbra.zone/main/crypto/fmd.html) is interesting for noisy broadcast, but not as a strict replacement for DPF. False positives trade client work for a k-anonymity-style set, and repeated-use intersection attacks remain a concern. The [private signaling comparison](https://www.usenix.org/system/files/sec22-madathil.pdf) also offers a two-server garbled-circuit construction with stronger privacy and constant recipient work; benchmark it only if DPF's two-party/semi-honest model becomes a blocker.

## Privacy by not querying

### Common compact feeds

[Zcash compact blocks](https://zips.z.cash/zip-0307) demonstrate the basic pattern: every light wallet downloads the same compact stream and trial-decrypts locally. Shinzo can publish fixed-size announcement pages, encrypted event capsules, or commitment deltas through ordinary caches. PIR is then reserved for a large payload or a cold catch-up.

This is particularly valuable for small live events. It is not realistic for every historical document or an unbounded result projection.

### Public active-nullifier witness updates

A synchronized wallet may be able to eliminate active-nullifier PIR entirely by caching its non-membership witness and applying the same public block update digest as every other wallet. [Utreexo](https://www.dci.mit.edu/projects/utreexo) is evidence that cached accumulator proofs can be updated from public state changes. It is not a direct solution: Utreexo focuses on inclusion in a UTXO-style accumulator, whereas Shieldd needs non-membership in its linked indexed tree.

There is also a real bandwidth lower bound. Research on [vector commitments with efficient updates](https://eprint.iacr.org/2023/1830.pdf) shows that public proof-update information must grow with the number and identity of changed entries. At Shieldd's update rate, this may be too much for a phone.

The next step is empirical, not another paper design: encode the actual changed predecessor and sibling nodes for a maximum Shieldd block, compress the public pack, and measure bytes/day, cold catch-up, and phone witness-update CPU. Warm synchronized clients could need no targeted read; cold clients retain Dense PIR.

### One-time discovery tags

[Aztec's note discovery](https://docs.aztec.network/developers/docs/foundational-topics/advanced/storage/note_discovery) uses contract-siloed, secret-derived, one-time tags and a counter. A node can index the pseudorandom tag for an O(1) lookup. Combined with anonymous transport, this is a very efficient “semantic obscurity” mode for established relationships.

It is not strict privacy: the server still sees the random tag and selected record, and timing or later compromise may explain the tag. Unknown-sender handshakes, counter recovery, and desynchronization are additional costs. Keep strict PIR as the guarantee; offer ratcheted tags only as an explicit fast path.

[ERC-5564 view tags](https://eips.ethereum.org/EIPS/eip-5564) give a smaller reusable idea: reveal a few common filter bits so clients reject most irrelevant ciphertexts cheaply. More bits reduce client work but partition the anonymity set, so the width must be globally fixed and conservative.

## Correctness, configuration, and active attacks

- Verify nullifier witnesses against the public Shieldd root.
- The POC binds encrypted projection AEAD to generation height/root, tag and result slot. A production multi-collection envelope should additionally bind its public collection and result-class identifiers.
- Authenticate one common manifest across replicas before generating shares.
- Obtain roots/configuration from a public consensus or light-client path, not only the queried provider. [Ethereum light clients](https://ethereum.org/developers/docs/nodes-and-clients/light-clients/) are the relevant separation of remote availability from local verification.
- Make invalid, stale, miss, overload, and success responses identical inside a class.
- Delay retries to a later epoch; immediate retry timing can re-link a client and reveal its network distance.
- Publish gateway keys, generation roots, operator identities, privacy classes, and client binary hashes in a transparency log. An Ethereum contract is one possible common commitment, but clients should receive the same cached state rather than make a unique RPC lookup.

This also prevents a provider from giving one wallet a unique gateway key or transport configuration as a fingerprint.

## The write path must be private too

The [Ethereum privacy roadmap](https://ethereum.org/roadmap/privacy/) correctly treats private reads, writes, and proving as separate workstreams. A private read followed immediately by a public spend through the same provider can still identify the wallet.

Use a distinct transport, circuit, and operator for transaction submission; add query-ahead and bounded randomized delay; and avoid funding a fresh actor directly from a known wallet. An [ERC-4337 bundler/paymaster](https://eips.ethereum.org/EIPS/eip-4337) can sponsor gas so a fresh actor does not first need a linkable funding transfer. The bundler still sees the operation and must itself be reached anonymously.

[Shutter](https://docs.shutter.network/docs/shutter) threshold encryption can hide transaction contents during the mempool phase where it is supported, reducing front-running and premature disclosure. It does not hide the sender IP or the final onchain transaction.

[Dandelion++](https://arxiv.org/abs/1805.11060) is useful P2P defense in depth, but later [NDSS analysis](https://www.ndss-symposium.org/ndss-paper/on-the-anonymity-of-peer-to-peer-network-anonymity-schemes-used-by-cryptocurrencies/) found limited anonymity under strategically colluding peers. It should not replace Tor or a mix path.

## Client and operations rules

- Keep query construction, transport, local verification, and decryption in an audited native core or isolated worker.
- Never put note secrets, nullifiers, tags, queried CIDs, selectors, or plaintext in analytics, crash reports, URLs, logs, clipboard history, or support bundles.
- Release uniform clients and rotate everyone on common configuration epochs. Per-user privacy tweaks can become fingerprints.
- Encrypt local caches/witnesses and erase expired HPKE state and reply capabilities.
- Do not use direct device attestation unless it is converted into an unlinkable token.
- Publish only aggregate, delayed operational telemetry.
- Audit the organization graph behind every “independent” role.

No transport protects a compromised wallet or OS. TorJS running in the same JavaScript context as an untrusted dapp is specifically the wrong boundary for the highest tier.

## Small engineering program

| State | Build or measure | Success criterion |
|---:|---|---|
| Implemented | Pluggable direct and Tor-compatible OHTTP transport | PIR/OHTTP code is unchanged when transport changes; `socks5h` prevents local DNS leakage |
| Implemented | Fixed demo framing | Hit and application-error responses have identical encrypted sizes |
| Implemented | Client result verification | Shieldd Poseidon path, projection AEAD and generation manifest all fail closed on tampering |
| Implemented | End-to-end comparison | Visible HTTP, PIR HTTP and PIR OHTTP report setup and verified-query time; Tor is never simulated |
| Implemented | Native Tor plus two v3 onion routes on desktop | Fresh/cached startup, first/p50/p95 latency, Tor RAM/CPU and encrypted OHTTP bytes are measured; separate SOCKS-auth contexts isolate replica paths |
| Next | Independent remote operators plus native mobile Arti | One phone reports startup, latency, peak client RAM, battery and total on-wire bytes; role separation is real rather than co-located |
| Next if needed | Small/medium/large fixed classes with streaming | Only declared class/generation leakage and bounded phone peak memory |

Every future result should report aggregate server work, gateway work, client CPU/RAM, upload, download, latency, trust assumption and exact observable leakage. Additional systems do not enter the POC merely because they might improve a stronger threat model.

## What not to rely on

| Option | Assessment |
|---|---|
| VPN, proxy, or CDN alone | Useful fallback; merely moves the complete link to one operator |
| OHTTP alone | Excellent baseline role split; no relay/gateway-collusion or global-timing protection |
| Tor alone | Strong practical origin privacy; not a global traffic-analysis guarantee |
| Three normal proxies | Timing-preserving hops, not an anytrust mix |
| Direct APNs/FCM per match | Stable device token plus exact event timing |
| Wallet-specific Waku Filter/Store | PeerID and topic linkability |
| FMD as strict privacy | Tunable false-positive anonymity, intersection risk, and growing client work |
| Shutter as network privacy | Protects mempool content, not origin or final state |
| Dandelion++ as the only write defense | Lightweight but not robust to strategic collusion |
| TEE as the main boundary | Hardware/vendor trust, side channels, attestation linkage, catastrophic compromise |
| Continuous dummy PIR | Converts cover directly into expensive full scans |

## Bottom line

The highest-value sequence is now short:

1. deploy the two OHTTP/onion paths under independent operators and establish the co-located-versus-remote baseline;
2. run the same client through native Arti on one phone, including battery and peak client memory;
3. add a second fixed traffic class only if measured payloads do not fit the current class;
4. revisit one deferred mechanism only when a concrete observer or abuse problem remains.

This improves privacy outside PIR without changing DefraDB storage or query execution. Privacy infrastructure stays in the sidecar and transport boundary until measurements justify production integration.
