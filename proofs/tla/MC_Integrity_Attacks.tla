---- MODULE MC_Integrity_Attacks ----
EXTENDS Integrity
\* One honest node receives an honest block plus adversarial variants:
\* unsigned, invalid, replayed over different content, and wrong-identity.

mcNodes == {"nodeA"}
mcHonestNodes == mcNodes

mcIdentities == {"did:alice", "did:eve"}
mcHonestIdentities == {"did:alice"}
mcAdversary == "did:eve"

mcContents == {"alice-v1", "evil-v1", "evil-v2"}

Sig(status, signer, signedContent) ==
  [status |-> status, signer |-> signer, signedContent |-> signedContent]

Block(author, content, sig) ==
  [author |-> author, content |-> content, sig |-> sig]

mcHonestBlock ==
  Block("did:alice", "alice-v1", Sig("valid", "did:alice", "alice-v1"))

mcUnsignedSpoof ==
  Block("did:alice", "evil-v1", Sig("absent", "did:eve", "evil-v1"))

mcInvalidSpoof ==
  Block("did:alice", "evil-v1", Sig("invalid", "did:alice", "evil-v1"))

mcReplayDifferentContent ==
  Block("did:alice", "evil-v2", Sig("valid", "did:alice", "alice-v1"))

mcWrongIdentity ==
  Block("did:alice", "evil-v1", Sig("valid", "did:eve", "evil-v1"))

mcBlocks ==
  {mcHonestBlock,
   mcUnsignedSpoof,
   mcInvalidSpoof,
   mcReplayDifferentContent,
   mcWrongIdentity}

mcInitialNetwork == mcBlocks

mcReplayBlocks == {mcReplayDifferentContent}
mcReplayNetwork == mcReplayBlocks
====
