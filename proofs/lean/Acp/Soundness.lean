import Std

/-!
ACP/Zanzibar permission soundness model.

This file intentionally keeps the universe small and mathlib-free.  `Obj`,
`Rel`, and `Subject` are abstract identifiers, direct relation tuples are a
boolean oracle, and tuple-to-userset edges are finite lists of target objects.

The full fragment proves exact agreement between the executable checker and the
rewrite-closure semantics for:

* direct tuples (`This`);
* computed usersets;
* tuple-to-userset traversal;
* union, intersection, and difference.

The separate positive fragment omits `Difference` and proves the monotonic
revocation/removal law.  That law is false for arbitrary expressions containing
difference: removing a tuple from the subtract side can intentionally grant.
-/

namespace Acp

abbrev Obj := Nat
abbrev Rel := Nat
abbrev Subject := Nat

inductive Expr where
  | this
  | computed (relation : Rel)
  | tupleToUserset (tupleRelation computedRelation : Rel)
  | union (left right : Expr)
  | intersection (left right : Expr)
  | difference (base subtract : Expr)
deriving Repr, DecidableEq

structure Instance where
  direct : Obj -> Rel -> Subject -> Bool
  targets : Obj -> Rel -> List Obj
  rule : Obj -> Rel -> Expr

def eval (i : Instance) (budget : Nat)
    (obj : Obj) (rel : Rel) (subject : Subject) (expr : Expr) : Bool :=
  match budget with
  | 0 => false
  | Nat.succ fuel =>
      match expr with
      | Expr.this => i.direct obj rel subject
      | Expr.computed computedRel =>
          eval i fuel obj computedRel subject (i.rule obj computedRel)
      | Expr.tupleToUserset tupleRel computedRel =>
          (i.targets obj tupleRel).any
            (fun target =>
              eval i fuel target computedRel subject (i.rule target computedRel))
      | Expr.union left right =>
          eval i fuel obj rel subject left || eval i fuel obj rel subject right
      | Expr.intersection left right =>
          eval i fuel obj rel subject left && eval i fuel obj rel subject right
      | Expr.difference base subtract =>
          eval i fuel obj rel subject base && !eval i fuel obj rel subject subtract

def derives (i : Instance) (budget : Nat)
    (obj : Obj) (rel : Rel) (subject : Subject) (expr : Expr) : Prop :=
  match budget with
  | 0 => False
  | Nat.succ fuel =>
      match expr with
      | Expr.this => i.direct obj rel subject = true
      | Expr.computed computedRel =>
          derives i fuel obj computedRel subject (i.rule obj computedRel)
      | Expr.tupleToUserset tupleRel computedRel =>
          Exists
            (fun target =>
              target ∈ i.targets obj tupleRel /\
                derives i fuel target computedRel subject (i.rule target computedRel))
      | Expr.union left right =>
          derives i fuel obj rel subject left \/
            derives i fuel obj rel subject right
      | Expr.intersection left right =>
          derives i fuel obj rel subject left /\
            derives i fuel obj rel subject right
      | Expr.difference base subtract =>
          derives i fuel obj rel subject base /\
            Not (derives i fuel obj rel subject subtract)

theorem eval_iff_derives (i : Instance) (budget : Nat) :
    forall obj rel subject expr,
      eval i budget obj rel subject expr = true <->
        derives i budget obj rel subject expr := by
  induction budget with
  | zero =>
      intro obj rel subject expr
      simp [eval, derives]
  | succ fuel ih =>
      intro obj rel subject expr
      cases expr with
      | this =>
          simp [eval, derives]
      | computed computedRel =>
          simp [eval, derives, ih]
      | tupleToUserset tupleRel computedRel =>
          simp [eval, derives]
          induction (i.targets obj tupleRel) with
          | nil =>
              simp
          | cons target rest ihList =>
              simp [List.any, ih target computedRel subject
                (i.rule target computedRel), ihList]
      | union left right =>
          simp [eval, derives, ih obj rel subject left, ih obj rel subject right]
      | intersection left right =>
          simp [eval, derives, ih obj rel subject left, ih obj rel subject right]
      | difference base subtract =>
          have hBase := ih obj rel subject base
          have hSub := ih obj rel subject subtract
          have hSubFalse :
              eval i fuel obj rel subject subtract = false <->
                Not (derives i fuel obj rel subject subtract) := by
            constructor
            · intro hFalse hDerives
              have hTrue := hSub.mpr hDerives
              rw [hFalse] at hTrue
              cases hTrue
            · intro hNotDerives
              cases hEval : eval i fuel obj rel subject subtract with
              | false => rfl
              | true =>
                  have hDerives := hSub.mp hEval
                  exact False.elim (hNotDerives hDerives)
          simp [eval, derives, hBase, hSubFalse]

def check (i : Instance) (budget : Nat)
    (obj : Obj) (rel : Rel) (subject : Subject) : Bool :=
  eval i budget obj rel subject (i.rule obj rel)

def closure (i : Instance) (budget : Nat)
    (obj : Obj) (rel : Rel) (subject : Subject) : Prop :=
  derives i budget obj rel subject (i.rule obj rel)

theorem check_sound (i : Instance) (budget : Nat)
    (obj : Obj) (rel : Rel) (subject : Subject) :
    check i budget obj rel subject = true ->
      closure i budget obj rel subject := by
  intro h
  exact (eval_iff_derives i budget obj rel subject (i.rule obj rel)).mp h

theorem check_complete (i : Instance) (budget : Nat)
    (obj : Obj) (rel : Rel) (subject : Subject) :
    closure i budget obj rel subject ->
      check i budget obj rel subject = true := by
  intro h
  exact (eval_iff_derives i budget obj rel subject (i.rule obj rel)).mpr h

theorem INV_CheckSound (i : Instance) (budget : Nat)
    (obj : Obj) (rel : Rel) (subject : Subject) :
    check i budget obj rel subject = true <->
      closure i budget obj rel subject :=
  eval_iff_derives i budget obj rel subject (i.rule obj rel)

theorem INV_NoEscalation (i : Instance) (budget : Nat)
    (obj : Obj) (rel : Rel) (subject : Subject) :
    check i budget obj rel subject = true <->
      closure i budget obj rel subject :=
  INV_CheckSound i budget obj rel subject

theorem check_deterministic (i : Instance) (budget : Nat)
    (obj : Obj) (rel : Rel) (subject : Subject) :
    check i budget obj rel subject = check i budget obj rel subject :=
  rfl

theorem eval_terminates (i : Instance) (budget : Nat)
    (obj : Obj) (rel : Rel) (subject : Subject) (expr : Expr) :
    Exists (fun result => eval i budget obj rel subject expr = result) :=
  Exists.intro (eval i budget obj rel subject expr) rfl

namespace Positive

inductive PosExpr where
  | this
  | computed (relation : Rel)
  | tupleToUserset (tupleRelation computedRelation : Rel)
  | union (left right : PosExpr)
  | intersection (left right : PosExpr)
deriving Repr, DecidableEq

structure PosInstance where
  direct : Obj -> Rel -> Subject -> Bool
  targets : Obj -> Rel -> List Obj
  rule : Obj -> Rel -> PosExpr

def eval (i : PosInstance) (budget : Nat)
    (obj : Obj) (rel : Rel) (subject : Subject) (expr : PosExpr) : Bool :=
  match budget with
  | 0 => false
  | Nat.succ fuel =>
      match expr with
      | PosExpr.this => i.direct obj rel subject
      | PosExpr.computed computedRel =>
          eval i fuel obj computedRel subject (i.rule obj computedRel)
      | PosExpr.tupleToUserset tupleRel computedRel =>
          (i.targets obj tupleRel).any
            (fun target =>
              eval i fuel target computedRel subject (i.rule target computedRel))
      | PosExpr.union left right =>
          eval i fuel obj rel subject left || eval i fuel obj rel subject right
      | PosExpr.intersection left right =>
          eval i fuel obj rel subject left && eval i fuel obj rel subject right

def derives (i : PosInstance) (budget : Nat)
    (obj : Obj) (rel : Rel) (subject : Subject) (expr : PosExpr) : Prop :=
  match budget with
  | 0 => False
  | Nat.succ fuel =>
      match expr with
      | PosExpr.this => i.direct obj rel subject = true
      | PosExpr.computed computedRel =>
          derives i fuel obj computedRel subject (i.rule obj computedRel)
      | PosExpr.tupleToUserset tupleRel computedRel =>
          Exists
            (fun target =>
              target ∈ i.targets obj tupleRel /\
                derives i fuel target computedRel subject (i.rule target computedRel))
      | PosExpr.union left right =>
          derives i fuel obj rel subject left \/
            derives i fuel obj rel subject right
      | PosExpr.intersection left right =>
          derives i fuel obj rel subject left /\
            derives i fuel obj rel subject right

theorem eval_iff_derives (i : PosInstance) (budget : Nat) :
    forall obj rel subject expr,
      eval i budget obj rel subject expr = true <->
        derives i budget obj rel subject expr := by
  induction budget with
  | zero =>
      intro obj rel subject expr
      simp [eval, derives]
  | succ fuel ih =>
      intro obj rel subject expr
      cases expr with
      | this =>
          simp [eval, derives]
      | computed computedRel =>
          simp [eval, derives, ih]
      | tupleToUserset tupleRel computedRel =>
          simp [eval, derives]
          induction (i.targets obj tupleRel) with
          | nil =>
              simp
          | cons target rest ihList =>
              simp [List.any, ih target computedRel subject
                (i.rule target computedRel), ihList]
      | union left right =>
          simp [eval, derives, ih obj rel subject left, ih obj rel subject right]
      | intersection left right =>
          simp [eval, derives, ih obj rel subject left, ih obj rel subject right]

def check (i : PosInstance) (budget : Nat)
    (obj : Obj) (rel : Rel) (subject : Subject) : Bool :=
  eval i budget obj rel subject (i.rule obj rel)

def closure (i : PosInstance) (budget : Nat)
    (obj : Obj) (rel : Rel) (subject : Subject) : Prop :=
  derives i budget obj rel subject (i.rule obj rel)

theorem check_iff_derives (i : PosInstance) (budget : Nat)
    (obj : Obj) (rel : Rel) (subject : Subject) :
    check i budget obj rel subject = true <->
      closure i budget obj rel subject :=
  eval_iff_derives i budget obj rel subject (i.rule obj rel)

theorem derives_monotone
    {after before : PosInstance}
    (hDirect :
      forall obj rel subject,
        after.direct obj rel subject = true ->
          before.direct obj rel subject = true)
    (hTargets :
      forall obj rel, after.targets obj rel = before.targets obj rel)
    (hRules :
      forall obj rel, after.rule obj rel = before.rule obj rel) :
    forall fuel obj rel subject expr,
      derives after fuel obj rel subject expr ->
        derives before fuel obj rel subject expr := by
  intro fuel
  induction fuel with
  | zero =>
      intro obj rel subject expr h
      cases h
  | succ fuel ih =>
      intro obj rel subject expr h
      cases expr with
      | this =>
          simp [derives] at h
          simp [derives]
          exact hDirect obj rel subject h
      | computed computedRel =>
          simp [derives] at h
          simp [derives]
          have hBefore :=
            ih obj computedRel subject (after.rule obj computedRel) h
          simpa [hRules obj computedRel] using hBefore
      | tupleToUserset tupleRel computedRel =>
          simp [derives] at h
          simp [derives]
          rcases h with ⟨target, hMem, hDerives⟩
          refine Exists.intro target ?_
          constructor
          · simpa [hTargets obj tupleRel] using hMem
          · have hBefore :=
              ih target computedRel subject
                (after.rule target computedRel) hDerives
            simpa [hRules target computedRel] using hBefore
      | union left right =>
          simp [derives] at h
          simp [derives]
          cases h with
          | inl hLeft =>
              exact Or.inl (ih obj rel subject left hLeft)
          | inr hRight =>
              exact Or.inr (ih obj rel subject right hRight)
      | intersection left right =>
          simp [derives] at h
          simp [derives]
          exact And.intro
            (ih obj rel subject left h.left)
            (ih obj rel subject right h.right)

theorem INV_PositiveRemovalNoGrant
    {after before : PosInstance}
    (hDirect :
      forall obj rel subject,
        after.direct obj rel subject = true ->
          before.direct obj rel subject = true)
    (hTargets :
      forall obj rel, after.targets obj rel = before.targets obj rel)
    (hRules :
      forall obj rel, after.rule obj rel = before.rule obj rel)
    (fuel : Nat) (obj : Obj) (rel : Rel) (subject : Subject) :
    check after fuel obj rel subject = true ->
      check before fuel obj rel subject = true := by
  intro hCheck
  have hAfterClosure :=
    (check_iff_derives after fuel obj rel subject).mp hCheck
  have hBeforeClosureOnAfterRule :=
    derives_monotone hDirect hTargets hRules fuel obj rel subject
      (after.rule obj rel) hAfterClosure
  have hBeforeClosure : closure before fuel obj rel subject := by
    simpa [closure, hRules obj rel] using hBeforeClosureOnAfterRule
  exact (check_iff_derives before fuel obj rel subject).mpr hBeforeClosure

end Positive

/-! A small red-shape witness: ignoring the subtract side of difference over-grants. -/

def object0 : Obj := 0
def readRel : Rel := 0
def denyRel : Rel := 1
def actor0 : Subject := 0

def differenceCounterexample : Instance :=
  { direct :=
      fun _ rel _ =>
        if rel = readRel then true
        else if rel = denyRel then true
        else false,
    targets := fun _ _ => [],
    rule :=
      fun _ rel =>
        if rel = readRel then
          Expr.difference Expr.this (Expr.computed denyRel)
        else
          Expr.this }

def evalBuggyDifference (i : Instance) (budget : Nat)
    (obj : Obj) (rel : Rel) (subject : Subject) (expr : Expr) : Bool :=
  match budget with
  | 0 => false
  | Nat.succ fuel =>
      match expr with
      | Expr.this => i.direct obj rel subject
      | Expr.computed computedRel =>
          evalBuggyDifference i fuel obj computedRel subject
            (i.rule obj computedRel)
      | Expr.tupleToUserset tupleRel computedRel =>
          (i.targets obj tupleRel).any
            (fun target =>
              evalBuggyDifference i fuel target computedRel subject
                (i.rule target computedRel))
      | Expr.union left right =>
          evalBuggyDifference i fuel obj rel subject left ||
            evalBuggyDifference i fuel obj rel subject right
      | Expr.intersection left right =>
          evalBuggyDifference i fuel obj rel subject left &&
            evalBuggyDifference i fuel obj rel subject right
      | Expr.difference base _subtract =>
          evalBuggyDifference i fuel obj rel subject base

def checkBuggyDifference (i : Instance) (budget : Nat)
    (obj : Obj) (rel : Rel) (subject : Subject) : Bool :=
  evalBuggyDifference i budget obj rel subject (i.rule obj rel)

theorem correctDifferenceDenies :
    check differenceCounterexample 3 object0 readRel actor0 = false := by
  rfl

theorem buggyDifferenceOverGrants :
    checkBuggyDifference differenceCounterexample 3 object0 readRel actor0 = true /\
      Not (closure differenceCounterexample 3 object0 readRel actor0) := by
  constructor
  · rfl
  · intro hClosure
    have hCheck :=
      (INV_CheckSound differenceCounterexample 3 object0 readRel actor0).mpr
        hClosure
    rw [correctDifferenceDenies] at hCheck
    cases hCheck

#print axioms INV_CheckSound
#print axioms INV_NoEscalation
#print axioms Positive.INV_PositiveRemovalNoGrant
#print axioms buggyDifferenceOverGrants

end Acp
