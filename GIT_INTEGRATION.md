# Git Integration Architecture

DefraDB.rs acts as the **Git signing and encryption backend**, mediating all
interactions between Git, the Orbis DKG ring, and Source Hub policy engine.
The result is end-to-end encrypted, identity-aware version control where
every commit is cryptographically tied to a real identity and every file is
encrypted to authorized recipients — with no single point of key custody.

## System Architecture

```
┌──────────────────────────────────────────────────┐
│                 Orbis (DKG Ring)                  │
│                                                  │
│  Distributed Key Generation for real identities  │
│  Threshold signatures (no single key holder)     │
│  Uses Source Hub as its bulletin board            │
└────────────────────────┬─────────────────────────┘
                         │
┌────────────────────────▼─────────────────────────┐
│                   Source Hub                      │
│                                                  │
│  Source of truth for access control policies     │
│  Bulletin board for Orbis DKG ceremonies         │
│  Determines who can sign, encrypt, decrypt       │
└────────────────────────┬─────────────────────────┘
                         │
┌────────────────────────▼─────────────────────────┐
│               DefraDB.rs Instance                │
│                                                  │
│  Hardware-rooted trust (TPM / secure enclave)    │
│  Git signing backend (replaces gpg.program)      │
│  Git encryption backend (clean/smudge filters)   │
│  CRDT document store with P2P replication        │
│  All data encrypted to Orbis identities          │
└──────────────────────────────────────────────────┘
```

## DefraDB.rs as Git Signing Backend

Git supports pluggable signing via `gpg.program`. DefraDB.rs replaces this
with a binary that routes signing requests through the Orbis DKG ring.

### How Signing Works

1. Developer runs `git commit`.
2. Git invokes defra.rs as the signing program.
3. DefraDB.rs identifies the committer via their hardware-rooted local identity.
4. DefraDB.rs initiates a threshold signing request to the Orbis DKG ring.
5. The DKG ring produces a signature from the developer's **real identity** —
   no single machine ever holds the complete private key.
6. The signature is returned to Git and stored with the commit object.

```
git commit
    │
    ▼
gpg.program = defra-sign
    │
    ▼
defra.rs local identity (hardware root of trust)
    │
    ▼
Orbis DKG ring (threshold signature)
    │
    ▼
Signed commit (real identity, distributed custody)
```

### What This Replaces

| Traditional Git Signing        | DefraDB.rs Signing                     |
|-------------------------------|----------------------------------------|
| Local GPG/SSH private key      | DKG threshold key (no single holder)   |
| Key lives on one machine       | Key shares distributed across ring     |
| Lost key = lost identity       | Ring recovers from node loss           |
| Self-asserted identity         | Hardware-rooted real identity          |
| Manual key distribution        | Orbis manages key lifecycle            |

### Verification

Verification uses the corresponding Orbis public identity. Any node with
access to the Orbis public key set can verify a commit signature without
needing the signer's local machine. This means CI systems, code review
tools, and other nodes can all verify signatures by querying Orbis.

## Git Encryption

All repository content is encrypted to Orbis identities. DefraDB.rs
implements this via Git's clean/smudge filter mechanism.

### How Encryption Works

```
Working Directory          Git Index/Objects         Remote
(plaintext)                (ciphertext)              (ciphertext)

    file.rs ──clean──▶  encrypted blob  ──push──▶  encrypted blob
                            │
    file.rs ◀──smudge──    │
                            │
                      encrypted to Orbis
                      identity public keys
```

1. **Clean filter (staging)**: When `git add` stages a file, defra.rs encrypts
   it to the set of authorized Orbis identities (determined by Source Hub policy).
2. **Smudge filter (checkout)**: When `git checkout` materializes a file,
   defra.rs decrypts it using the local identity's DKG key share via Orbis.
3. **At rest**: Repository content on disk (in `.git/objects/`) and on any
   remote is always ciphertext. Only authorized identities can read it.

### Policy-Driven Encryption Recipients

Source Hub policies determine **who** can decrypt each file or path pattern.
This is configured via `.gitattributes` patterns mapped to Source Hub policy
resources:

```gitattributes
# All files encrypted by default
*                   filter=defra-crypt diff=defra-crypt

# Specific paths can have different access policies
contracts/**        filter=defra-crypt-legal
infrastructure/**   filter=defra-crypt-ops
```

Each filter name maps to a Source Hub policy that defines which identities
(individuals, teams, roles) are encryption recipients. When policy changes
in Source Hub (e.g., a team member is removed), the next commit re-encrypts
affected files to the updated recipient set.

### Deterministic Ciphertext

Standard encryption (AES-GCM, age, GPG) produces different ciphertext for
identical plaintext due to random nonces. This causes Git to see every file
as "changed" on every checkout.

DefraDB.rs solves this by:
- Caching a content hash (blake3) of each file's plaintext.
- On `git status` / `git diff`, comparing the current plaintext hash against
  the cached hash.
- Only re-encrypting when the plaintext has actually changed.
- Storing cached hashes in `.git/defra/content-hashes` (never committed).

## Identity Model

### Three Layers of Identity

```
Orbis Identity (real, persistent)
    │
    ├── Verified via DKG ring membership
    │   No single key to steal
    │
    ▼
DefraDB.rs Node Identity (hardware-rooted)
    │
    ├── Bound to TPM / secure enclave
    │   Proves physical machine identity
    │
    ▼
Git Author Identity (derived)
    │
    └── Commit author/committer fields
        Backed by Orbis signature
```

Human identities live in Orbis. Machine identities are rooted in hardware.
Git identities are derived from and cryptographically backed by both.
An AI agent gets the same treatment: its Orbis identity ties it to a real
principal, its node identity ties it to specific hardware, and its Git
commits are signed by both.

## Access Control Integration

Source Hub policies govern three dimensions of Git access:

| Dimension     | Controlled By              | Enforcement Point           |
|---------------|----------------------------|-----------------------------|
| **Read**      | Encryption recipients      | Smudge filter (decryption)  |
| **Write**     | Commit signing authority   | Push hooks / CI verification|
| **Verify**    | Orbis public key set       | Any node with Orbis access  |

This maps directly to the existing ACP infrastructure in defra.rs
(`crates/acp/`), which already supports both local and Source Hub policy
engines.

## Value Proposition

### For Companies

- **E2E encrypted repos**: Source code is ciphertext at rest, in transit,
  and on remotes. Only authorized identities decrypt.
- **No key management overhead**: Orbis handles key lifecycle. No GPG key
  ceremonies, no "who has access to the deploy key" questions.
- **Policy-driven access**: Source Hub policies are the single source of
  truth. Revoke access in one place, enforcement is immediate.
- **Hardware-rooted trust**: Every machine that touches code is
  authenticated via its secure enclave. No credential theft via malware.
- **Full audit trail**: Every commit is signed by a real identity via DKG.
  Provenance is cryptographically verifiable, not just a `user.email` string.

### For AI Agent Platforms

- **Agent identity**: Every AI agent has an Orbis identity. Its actions
  (commits, data generation, API calls) are cryptographically attributable.
- **Scoped access**: Agents only decrypt what their Source Hub policy allows.
  An agent working on frontend code cannot read infrastructure secrets.
- **Provenance**: Human and agent contributions are distinguishable and
  verifiable. Code review can verify "this was written by agent X under
  the authority of human Y."
- **Secure data generation**: All data an agent produces in DefraDB is
  encrypted to its identity and the identities of authorized consumers.

## Use Case: Multi-Tenant ML Training on Sensitive Data

A concrete application of this architecture: a customer support platform
where multiple companies' voice data must be segmented during storage,
model training, and inference serving.

### The Problem

A platform processes customer support calls for many companies. Each
company's voice data is sensitive. The platform wants to:

1. Store each company's data with strict tenant isolation.
2. Train small models (QLoRA adapters) on each company's data without
   cross-tenant leakage.
3. Serve the resulting adapters back to each company's infrastructure.
4. Provide cryptographic proof that data never left its tenant boundary.

### How This Architecture Solves It

```
Company A's voice data              Company B's voice data
        │                                   │
        ▼                                   ▼
DefraDB (encrypted to A's            DefraDB (encrypted to B's
 Orbis identity, ACP policy)          Orbis identity, ACP policy)
        │                                   │
        ▼                                   ▼
Training agent (identity              Training agent (identity
 authorized by A's policy)             authorized by B's policy)
        │                                   │
        ▼                                   ▼
QLoRA adapter committed to            QLoRA adapter committed to
 encrypted Git repo (A's keys)        encrypted Git repo (B's keys)
        │                                   │
        ▼                                   ▼
Serving node (A's identity)           Serving node (B's identity)
 decrypts + loads adapter              decrypts + loads adapter
```

**Data ingestion**: Each company's voice data is stored in DefraDB, encrypted
to that company's Orbis identity. Source Hub ACP policies enforce that only
identities authorized by Company A can read Company A's data. The data is
physically co-located but cryptographically isolated — even a database
administrator without the right Orbis identity cannot decrypt it.

**Model training**: A training agent receives an Orbis identity authorized
by Company A's Source Hub policy. It pulls Company A's data from DefraDB
(decrypted via its authorized identity), trains a QLoRA adapter, and commits
the resulting weights to an encrypted Git repo. The adapter repo is encrypted
to Company A's identities. The training agent's identity cannot access
Company B's data — the smudge filter will refuse to decrypt.

**Adapter as artifact**: The QLoRA adapter weights are sensitive because they
contain a compressed representation of the training data. Storing them in an
encrypted Git repo means:
- The weights are ciphertext at rest and in transit.
- Only Company A's authorized identities can decrypt and load them.
- The commit history provides full provenance: which agent trained it,
  on which data version, at what time, authorized by which policy.

**Model serving**: Company A's serving infrastructure has an Orbis identity
authorized to read the adapter repo. It clones, the smudge filter decrypts
the weights, and the adapter is loaded for inference. Company B's serving
nodes cannot decrypt Company A's adapter — the encryption enforces what the
policy declares.

### Auditability

Every step in the pipeline is a signed Git commit:

| Step              | Committed By               | Contains                      |
|-------------------|----------------------------|-------------------------------|
| Data ingestion    | Ingestion agent (signed)   | Data manifest, schema version |
| Preprocessing     | Preprocessing agent        | Feature extraction config     |
| Training run      | Training agent             | Hyperparameters, metrics      |
| Adapter output    | Training agent             | QLoRA weights, eval results   |
| Deployment        | Serving agent              | Deployment config, model hash |

Each commit is signed by a real Orbis identity via DKG. The full provenance
chain is cryptographically verifiable: this adapter was trained by this
agent, on this data version, authorized by this policy, and deployed to
this serving node. This is the level of auditability that regulated
industries (healthcare, finance, customer voice data) require for
compliance.

## Existing Crate Mapping

| Concern               | Existing Crate     | Extension Needed                          |
|-----------------------|--------------------|-------------------------------------------|
| Identity              | `crates/identity/` | Orbis identity type, DKG key share        |
| Signing               | `crates/crypto/`   | Threshold signature protocol              |
| Key storage           | `crates/keyring/`  | Hardware root of trust binding            |
| Access control policy | `crates/acp/`      | Git path-based policy resources           |
| P2P replication       | `crates/p2p/`      | Encrypted payload transport (already done)|
| Block storage         | `crates/blockstore/`| Encrypted blocks (already done)          |

## Implementation Phases

### Phase 1: Local Git Signing Backend

DefraDB.rs acts as `gpg.program`, signing commits with the node's local
ed25519 key. No Orbis integration yet — this validates the Git plumbing.

### Phase 2: Clean/Smudge Encryption

Implement the filter driver for file-level encryption using the node's local
keys. Deterministic ciphertext caching. Policy-driven recipient sets from
local ACP engine.

### Phase 3: Orbis DKG Integration

Replace local signing keys with DKG threshold signatures from Orbis.
Encryption recipients resolve to Orbis identities. Source Hub becomes the
policy authority for Git access control.

### Phase 4: Hardware Root of Trust

Bind node identity to TPM / secure enclave. Attestation flows prove that
a signing request originates from authorized hardware.
