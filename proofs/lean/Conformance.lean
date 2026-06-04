/-!
# Conformance contract generator

Emits a JSON contract — vocabularies derived from the Lean models — between two
sentinel lines, for the Rust `conformance` crate to extract and assert against
the live Rust types (anti-drift). Run by `lake env lean --run Conformance.lean`.

The pattern: an `inductive` mirrors a Rust enum, a `toDefraDB` function fixes the
wire names, and a list enumerates the domain. Adding a domain here = one more
vocabulary the Rust side must keep matching. New families append to
`vocabularies` (and grow the corresponding Rust assertion in
`tests/lean_conformance.rs`).
-/

namespace Conformance

/-- Mirrors `crates/crdt/src/traits.rs` `enum MergeResult`. -/
inductive MergeOutcome
  | applied
  | rejectedLowerPriority
  | rejectedTieBreak

namespace MergeOutcome

def toDefraDB : MergeOutcome → String
  | .applied               => "Applied"
  | .rejectedLowerPriority => "RejectedLowerPriority"
  | .rejectedTieBreak      => "RejectedTieBreak"

end MergeOutcome

def mergeOutcomes : List MergeOutcome :=
  [.applied, .rejectedLowerPriority, .rejectedTieBreak]

/-- Mirrors `crates/zanzibar/src/expression/mod.rs` `enum RelationExpression` —
    the domain of `Acp/Soundness.lean`'s `Expr` (the `eval_iff_derives`
    soundness theorem inducts over these constructors). -/
inductive RelExpr
  | this
  | computedUserset
  | tupleToUserset
  | union
  | intersection
  | difference

namespace RelExpr

def toDefraDB : RelExpr → String
  | .this            => "This"
  | .computedUserset => "ComputedUserset"
  | .tupleToUserset  => "TupleToUserset"
  | .union           => "Union"
  | .intersection    => "Intersection"
  | .difference      => "Difference"

end RelExpr

def relExprs : List RelExpr :=
  [.this, .computedUserset, .tupleToUserset, .union, .intersection, .difference]

structure Vocabulary where
  domain : String
  values : List String

def vocabularies : List Vocabulary :=
  [ { domain := "MergeResult",        values := mergeOutcomes.map MergeOutcome.toDefraDB }
  , { domain := "RelationExpression", values := relExprs.map RelExpr.toDefraDB } ]

/-! ## Minimal JSON encoding (no mathlib) -/

def jsonString (s : String) : String := "\"" ++ s ++ "\""

def jsonArray (xs : List String) : String :=
  "[" ++ String.intercalate "," xs ++ "]"

def Vocabulary.toJson (v : Vocabulary) : String :=
  "{" ++ jsonString "domain" ++ ":" ++ jsonString v.domain ++ ","
      ++ jsonString "values" ++ ":" ++ jsonArray (v.values.map jsonString) ++ "}"

def snapshotJson : String :=
  "{" ++ jsonString "generated_by" ++ ":" ++ jsonString "proofs/lean/Conformance.lean" ++ ","
      ++ jsonString "vocabularies" ++ ":"
      ++ jsonArray (vocabularies.map Vocabulary.toJson) ++ "}"

def beginMarker : String := "---BEGIN DEFRA LEAN CONTRACT JSON---"
def endMarker   : String := "---END DEFRA LEAN CONTRACT JSON---"

def emit : IO Unit := do
  IO.println beginMarker
  IO.println snapshotJson
  IO.println endMarker

end Conformance

def main : IO Unit := Conformance.emit
