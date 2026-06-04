# ACP / Zanzibar Permission Soundness

This slice deliberately models a tractable ACP fragment rather than all of
Zanzibar.  It has two independent halves:

- Lean: algebraic soundness of the check evaluator against rewrite-closure
  semantics.
- TLA+: replicated tuple revocation and stale positive decision caches.

## Source Anchors

The Lean model is grounded in the Zanzibar expression and evaluator code:

- `crates/zanzibar/src/expression/mod.rs:17-36`: expression constructors:
  `This`, `ComputedUserset`, `TupleToUserset`, `Union`, `Intersection`, and
  `Difference`.
- `crates/zanzibar/src/engine/mod.rs:104-126`: `check` loads a relation's rule
  and evaluates it with a fresh trail/cache.
- `crates/zanzibar/src/engine/evaluate.rs:66-69`: `This` delegates to direct
  tuple lookup.
- `crates/zanzibar/src/engine/evaluate.rs:72-96`: computed userset checks another
  relation on the same object.
- `crates/zanzibar/src/engine/evaluate.rs:98-191`: tuple-to-userset follows
  relation targets/entity sets, with wildcard subjects granting immediately.
- `crates/zanzibar/src/engine/evaluate.rs:194-261`: union, intersection, and
  difference evaluate as OR, AND, and base AND NOT subtract.
- `crates/zanzibar/src/store/traits.rs:34-57`: store interface for relationship
  presence and direct permission checks.

The TLA model is grounded in ACP tuple storage and SourceHub cache behavior:

- `crates/acp/src/store.rs:42-130`: ACP tuple store operations, including put,
  delete, relation scans, and document registration.
- `crates/acp/src/persistent.rs:163-215`: persistent tuple put/delete/has are
  transactional store operations.
- `crates/acp/src/local.rs:50-103`: local ACP checks owner/direct/wildcard tuples
  and read-implying relations.
- `crates/acp/src/local.rs:244-369`: local add/delete relationship operations
  write and delete tuples.
- `crates/acp/src/local.rs:390-439`: local P2P export/replace reconciles
  document-scoped relationships while preserving owner.
- `crates/sourcehub/src/access_cache.rs:10-16`: positive access decisions are
  cached by actor/policy/resource/doc/permission.
- `crates/sourcehub/src/access_cache.rs:46-61`: cache hits return unexpired
  allowed decisions.
- `crates/sourcehub/src/access_cache.rs:85-95`: object invalidation clears cached
  decisions for a document.
- `crates/sourcehub/src/cosmos/dac.rs:186-223`: SourceHub ACP checks read from
  cache first, then calls `verify_access`, and caches only successful results.
- `crates/sourcehub/src/cosmos/dac.rs:250-307`: SourceHub relationship grant and
  revoke invalidate cached access for the object.

## Brainstorming Outcome

Chosen fragment:

- Abstract `Obj`, `Rel`, and `Subject` identifiers as natural numbers in Lean.
- Model direct tuples as `Obj -> Rel -> Subject -> Bool`.
- Model tuple-to-userset as finite target-object lists.  Wildcard and typed
  wildcard matching are not modeled in Lean; they are direct-store matching
  details for this slice.
- Use binary `Union` and `Intersection`; nested binary expressions encode the
  n-ary `Vec<RelationExpression>` shape.
- Bound recursive rule expansion with an explicit natural-number budget.  This
  mirrors the production trail/cycle guard without modeling every trail node.
- For TLA+, model one tuple identity as the permission unit and focus on whether
  a node can still grant it after revoke propagation.

Out of scope:

- Complete Zanzibar conformance, subject restrictions, wildcard storage-hash
  details, parser behavior, policy YAML validation, and all SourceHub chain
  mechanics.
- Key rotation or undoing access already exercised before revocation.  The TLA
  property is post-propagation enforcement, not retroactive secrecy.
- A global "removing any tuple never grants" theorem for expressions containing
  `Difference`.  That law is false: removing a tuple from the subtract side of
  `base - subtract` can intentionally grant.  Lean proves the removal monotonicity
  law for the positive fragment without `Difference`, and exact no-false-grant
  closure for the full fragment.

## Lean Split

New files:

- `proofs/lean/Acp.lean`: barrel module.
- `proofs/lean/Acp/Soundness.lean`: focused model and proofs.

Main definitions:

- `Acp.Expr`: full rewrite syntax.
- `Acp.eval`: executable budgeted checker.
- `Acp.derives`: budgeted rewrite-closure semantics.
- `Acp.check`: top-level relation check.
- `Acp.closure`: accepted subject/relation pair in the semantic closure.
- `Acp.Positive.PosExpr`: positive fragment without `Difference`.

Main theorems:

- `Acp.INV_CheckSound`: `check = true` iff the subject is in the rule closure.
- `Acp.INV_NoEscalation`: no accepted permission exists outside that closure.
- `Acp.Positive.INV_PositiveRemovalNoGrant`: if an after-revocation positive
  instance has fewer direct tuples but the same rules and targets, anything it
  grants was already granted before removal.
- `Acp.buggyDifferenceOverGrants`: red witness showing a checker that ignores
  the subtract side of `Difference` grants outside the correct closure.
- `Acp.check_deterministic` and `Acp.eval_terminates`: executable checks are
  deterministic and return a finite Boolean result for every budget.

The existing lakefile only declares `lean_lib DefraConvergence`; it was not
edited.  Integrator action: add `lean_lib Acp` if ACP should be included in
`lake build`.

Lean command run:

```bash
cd proofs/lean
lake env lean --root=. Acp/Soundness.lean
```

`#print axioms` status from Lean 4.18:

- `Acp.INV_CheckSound`: `[propext, Quot.sound]`
- `Acp.INV_NoEscalation`: `[propext, Quot.sound]`
- `Acp.Positive.INV_PositiveRemovalNoGrant`: `[propext, Quot.sound]`
- `Acp.buggyDifferenceOverGrants`: `[propext, Quot.sound]`

No theorem uses `sorry` or custom axioms.

## TLA+ Split

New files:

- `proofs/tla/Acp.tla`: base tuple-replication and revocation model.
- `proofs/tla/MC_Acp_Green.tla` + `.cfg`: cache invalidated on revoke
  propagation.
- `proofs/tla/MC_Acp_StaleCache_Red.tla` + `.cfg`: stale positive cache remains
  after revoke propagation.

State variables:

- `authority`: authoritative live tuple set.
- `known`: each node's replicated local tuple view.
- `cache`: each node's cached positive access decisions.
- `revoked`: tuples revoked by the authority.
- `seenRevoke`: per-node revoke propagation marker.
- `checked`: allowed checks recorded by the model.

Properties:

- `INV_RevocationConsistent`: if a revoked tuple has propagated to a node, that
  node's `CheckAllowed` predicate must be false.
- `INV_RevokedNotAuthoritative`: revoked tuples are not still authoritative.
- `PROP_RevocationEventuallyEnforced`: under connectivity and weak fairness,
  every scenario-selected revocation eventually propagates and remains denied
  everywhere.

TLA+ commands run:

```bash
cd proofs/tla
./tools/tlc -metadir states/acp_green -config MC_Acp_Green.cfg MC_Acp_Green.tla
./tools/tlc -metadir states/acp_red -config MC_Acp_StaleCache_Red.cfg MC_Acp_StaleCache_Red.tla
rm -rf states/acp_green states/acp_red
```

Results:

- GREEN: `MC_Acp_Green.cfg` completed with no errors.  TLC explored 20 distinct
  states and checked the liveness property.
- RED: `MC_Acp_StaleCache_Red.cfg` violated `INV_RevocationConsistent`.  The
  counterexample is: initial cached allow on `replica`; authority revokes
  `doc1#reader@alice`; revocation propagates to `replica`; local tuple is removed
  but the stale cache still grants.

## Interpretation

Lean proves the local checker does not invent permissions: for the modeled
rewrite fragment, Boolean acceptance is exactly membership in the rewrite
closure.  TLA+ proves the replicated tuple/cached-decision model has the desired
post-propagation revocation behavior only when revoke propagation invalidates
positive decision caches.

The combined result is source-anchored but not an automated conformance proof.
The Rust code must keep cache invalidation coupled to relationship mutation and
replication paths, and ACP policy changes that use `Difference` must not be
summarized as globally monotone under tuple deletion.
