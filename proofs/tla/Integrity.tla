---- MODULE Integrity ----
\* Block-signature integrity beneath replication.
\*
\* Crypto is intentionally abstracted: a valid signature record means the
\* signer produced that signature over exactly one content value. Signature
\* scheme unforgeability is assumed outside the model.
EXTENDS FiniteSets

CONSTANTS
  Nodes,
  HonestNodes,
  Identities,
  HonestIdentities,
  Adversary,
  Contents,
  Blocks,
  InitialNetwork,
  NetworkConnected,
  MergePolicy       \* "NoCheck" | "SigOnly" | "VerifyThenMerge"

SigStatuses == {"absent", "invalid", "valid"}
MergePolicies == {"NoCheck", "SigOnly", "VerifyThenMerge"}

AllSignatures ==
  [status: SigStatuses, signer: Identities, signedContent: Contents]

AllBlocks ==
  [author: Identities, content: Contents, sig: AllSignatures]

VARIABLES
  network,
  merged

vars == <<network, merged>>

TypeOK ==
  /\ Nodes # {}
  /\ HonestNodes \subseteq Nodes
  /\ HonestNodes # {}
  /\ Identities # {}
  /\ HonestIdentities \subseteq Identities \ {Adversary}
  /\ HonestIdentities # {}
  /\ Adversary \in Identities
  /\ Contents # {}
  /\ Blocks \subseteq AllBlocks
  /\ InitialNetwork \subseteq Blocks
  /\ NetworkConnected \in BOOLEAN
  /\ MergePolicy \in MergePolicies
  /\ network \subseteq Blocks
  /\ merged \in [Nodes -> SUBSET Blocks]

Init ==
  /\ network = InitialNetwork
  /\ merged = [n \in Nodes |-> {}]

HasSignature(b) ==
  b.sig.status # "absent"

\* Abstract verification boundary: the signature is cryptographically valid for
\* its signer and for this exact content. It does not by itself say the signer
\* is the author claimed by P2P metadata or the block payload.
ValidSig(b) ==
  /\ b.sig.status = "valid"
  /\ b.sig.signedContent = b.content

Authentic(b) ==
  /\ ValidSig(b)
  /\ b.sig.signer = b.author

ReplayOnDifferentContent(b) ==
  /\ HasSignature(b)
  /\ b.sig.status = "valid"
  /\ b.sig.signedContent # b.content

HonestBlock(b) ==
  /\ b.author \in HonestIdentities
  /\ Authentic(b)

MayMerge(b) ==
  CASE MergePolicy = "NoCheck" ->
        TRUE
    [] MergePolicy = "SigOnly" ->
        ValidSig(b)
    [] MergePolicy = "VerifyThenMerge" ->
        Authentic(b)
    [] OTHER ->
        FALSE

Advertise(b) ==
  /\ NetworkConnected
  /\ b \in Blocks
  /\ b \notin network
  /\ network' = network \cup {b}
  /\ UNCHANGED merged

Merge(n, b) ==
  /\ NetworkConnected
  /\ n \in HonestNodes
  /\ b \in network
  /\ b \notin merged[n]
  /\ MayMerge(b)
  /\ merged' = [merged EXCEPT ![n] = @ \cup {b}]
  /\ UNCHANGED network

Next ==
  \/ \E b \in Blocks : Advertise(b)
  \/ \E n \in HonestNodes, b \in Blocks : Merge(n, b)

Fairness ==
  /\ \A b \in Blocks : WF_vars(Advertise(b))
  /\ \A n \in HonestNodes, b \in Blocks : WF_vars(Merge(n, b))

Spec == Init /\ [][Next]_vars /\ Fairness

\* Safety: no unsigned, invalid, replayed, or wrong-identity block reaches an
\* honest node's merged set.
INV_NoForgedMerge ==
  \A n \in HonestNodes :
    \A b \in merged[n] : Authentic(b)

\* Safety: every merged block is signed by the identity it claims as author.
INV_AuthorBinding ==
  \A n \in HonestNodes :
    \A b \in merged[n] :
      /\ HasSignature(b)
      /\ b.sig.signer = b.author

\* Safety: replaying a valid signature onto different content does not pass.
INV_NoReplayForge ==
  \A n \in HonestNodes :
    \A b \in merged[n] : ~ReplayOnDifferentContent(b)

\* Liveness sanity check: the integrity gate does not block honest signed
\* blocks under connected, fair delivery.
HonestBlocksEventuallyMerged ==
  <>[](\A n \in HonestNodes :
        \A b \in Blocks : HonestBlock(b) => b \in merged[n])
====
