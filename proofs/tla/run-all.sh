#!/usr/bin/env bash
# Run every TLA+ model and check its verdict against the expected red/green oracle.
# Each run gets a unique -metadir (TLC's default scratch dir is per-second; a tight
# loop would otherwise collide). Exits non-zero if any verdict does not match.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

# rows: <cfg> <module> <GREEN|RED>
RUNS=(
  "M1Convergence.cfg            M1Convergence.tla            GREEN"  # B3 control (S1')
  "M1Naive.cfg                  M1Convergence.tla            RED"    # B3 #2721 (S1)
  "MC_S2.cfg                    MC_S2.tla                    GREEN"  # B3 ideal (S2)
  "MC_S3.cfg                    MC_S3.tla                    RED"    # B3 split ownership (S3)
  "MC_S3_Fixed.cfg              MC_S3.tla                    GREEN"  # B3 immutable key (S3)
  "MC_S4_Naive.cfg              MC_S4.tla                    RED"    # B3 field-grain #2721 (S4)
  "MC_S4_FullWalkA.cfg          MC_S4.tla                    RED"    # B3 Model A over-fetch (S4)
  "MC_S4_ModelB.cfg             MC_S4.tla                    GREEN"  # B3 Model B (S4)
  "MC_Conv_Eventual.cfg         MC_Conv_Eventual.tla         GREEN"  # convergence, eventual connectivity
  "MC_Conv_RestartEviction.cfg  MC_Conv_RestartEviction.tla  GREEN"  # convergence, eviction+restart
  "MC_Conv_NoHeadRediscovery.cfg MC_Conv_NoHeadRediscovery.tla RED"  # convergence fails w/o rediscovery
  "MC_Claim_Unfiltered_Eventual.cfg  MC_Claim_Common.tla     GREEN"  # claim eventual-unique
  "MC_Claim_Filtered_Eventual.cfg    MC_Claim_Common.tla     GREEN"  # claim eventual-unique (filtered)
  "MC_Claim_Unfiltered_Execution.cfg MC_Claim_Common.tla     RED"    # exec-unique fails (CAS race)
  "MC_Claim_Filtered_Execution.cfg   MC_Claim_Common.tla     RED"    # exec-unique fails (filtered)
  "MC_Claim_Split_Eventual.cfg       MC_Claim_Common.tla     RED"    # split same-DID breaks convergence
  "MC_Kms_Green.cfg             MC_Kms_Gossip.tla            GREEN"  # KMS policy-gated
  "MC_Kms_NoPolicy_Red.cfg      MC_Kms_Gossip.tla            RED"    # KMS unauthorized obtains key
  "MC_Kms_BroadcastCiphertext_Red.cfg MC_Kms_Gossip.tla     RED"    # KMS ciphertext to non-recipient
  "MC_Kms_RevokeReplay_Green.cfg MC_Kms_Replay.tla          GREEN"  # KMS revoke/replay gated
  "MC_Kms_Revoke_Red.cfg        MC_Kms_Replay.tla            RED"    # KMS revoked obtains key
  "MC_Kms_Replay_Red.cfg        MC_Kms_Replay.tla            RED"    # KMS replay grants key
  "MC_Auth_Green.cfg            MC_Auth_Green.tla            GREEN"  # auth gated
  "MC_Auth_Red_PeerOnly.cfg     MC_Auth_Red_PeerOnly.tla     RED"   # auth: PeerID-only executes
  "MC_Auth_Red_Stale.cfg        MC_Auth_Red_Stale.tla        RED"   # auth: stale token authorizes
  "MC_Auth_Red_WrongScope.cfg   MC_Auth_Red_WrongScope.tla   RED"   # auth: wrong-permission executes
)

fails=0; n=0
for row in "${RUNS[@]}"; do
  read -r cfg mod want <<<"$row"
  n=$((n+1))
  out=$(./tools/tlc -metadir "states/run$n" -config "$cfg" "$mod" 2>&1)
  if echo "$out" | grep -q "No error has been found"; then got=GREEN
  elif echo "$out" | grep -qE "is violated|properties were violated"; then got=RED
  else got=ERROR; fi
  rm -rf "states/run$n"
  if [ "$got" = "$want" ]; then printf "  ok   %-6s %-34s %s\n" "$got" "$cfg" "$mod"
  else printf "  FAIL want=%s got=%s  %-30s %s\n" "$want" "$got" "$cfg" "$mod"; fails=$((fails+1)); fi
done

echo "----"
if [ "$fails" -eq 0 ]; then echo "all $n TLA+ runs matched their expected verdict"; else echo "$fails/$n MISMATCHED"; fi
exit "$fails"
