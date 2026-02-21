# Finding: Policy Expressions Support Intersection and Difference Despite "Union-Only" Documentation

**Stream**: 02 - Access Control Policy
**Severity**: LOW
**Category**: Documentation Mismatch / Correctness
**Status**: CONFIRMED
**Session**: S2 - NAC and Zanzibar Evaluation

## Summary

The Zanzibar expression parser and evaluator fully support `&` (intersection), `-` (difference), and `->` (tuple-to-userset) operators in addition to `+` (union). The policy YAML validator accepts all three operators without restriction. This contradicts the audit plan note "support only unions (not intersections/negations)" and may surprise policy authors who expect union-only semantics.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/zanzibar/src/expression/parser.rs` | 31-38 | Parser handles `+`, `&`, and `-` operators |
| `crates/zanzibar/src/engine/evaluate.rs` | 215-262 | Evaluator implements `Intersection` and `Difference` |
| `crates/acp/src/policy_yaml/validate.rs` | 28-52 | Validator tokenizes `+`, `-`, `&` as operators, no rejection |
| `crates/zanzibar/src/expression/mod.rs` | 14-36 | `RelationExpression` enum has `Intersection` and `Difference` variants |

## Details

### What the Parser Accepts

```rust
// crates/zanzibar/src/expression/parser.rs:77-85
'+' | '&' if depth == 0 => {
    rightmost = Some((i, chars[i]));
}
'-' if depth == 0 => {
    if i + 1 < chars.len() && chars[i + 1] == '>' {
        continue;  // "->" is tuple-to-userset, not difference
    }
    rightmost = Some((i, '-'));
}
```

### What the Evaluator Supports

```rust
// crates/zanzibar/src/engine/evaluate.rs:215-262
RelationExpression::Intersection(exprs) => {
    for expr in exprs {
        if !self.evaluate_expr_inner(...).await? {
            return Ok(false);  // Short-circuit: all must be true
        }
    }
    Ok(true)
}

RelationExpression::Difference { base, subtract } => {
    let base_result = self.evaluate_expr_inner(..., base, ...).await?;
    if !base_result { return Ok(false); }
    let subtract_result = self.evaluate_expr_inner(..., subtract, ...).await?;
    Ok(!subtract_result)  // base AND NOT subtract
}
```

### What the Validator Accepts

```rust
// crates/acp/src/policy_yaml/validate.rs:66-97
fn tokenize_expression(expr: &str) -> Result<Vec<ExprToken>, String> {
    // ...
    '+' | '-' => { tokens.push(ExprToken::Operator); }
    '&' => { tokens.push(ExprToken::Operator); }
    // All three are treated as generic "Operator" — no rejection
}
```

### Example: Dangerous Difference Expression

A policy author could write:

```yaml
permissions:
  - name: read
    expr: reader - blocked
```

This grants `read` to anyone with the `reader` relation EXCEPT those with the `blocked` relation. While this is semantically valid, it creates a revocation mechanism that interacts subtly with the system's "owner always has access" guarantee (since `build_policy()` prepends `owner +` to every expression).

### The Auto-Injected Owner

```rust
// crates/acp/src/policy_yaml/mod.rs:147-152
let user_expr = RelationExpression::parse(&perm.expr)?;
let expression = RelationExpression::Union(vec![
    RelationExpression::computed_userset("owner"),
    user_expr,  // <- this can be intersection or difference
]);
```

Because owner is prepended as a union, owner access is preserved regardless of the user expression. But the interaction with intersection/difference in user expressions may surprise policy authors.

### Severity Rationale

LOW because:
1. The implementation is actually correct — intersection and difference work as expected
2. Owner access is always preserved via the auto-injected union
3. This is more of a documentation/expectation gap than a security vulnerability
4. However, complex expressions increase the surface for logic errors in policies

## Remediation

### Option A: Document the full expression language

Update documentation to describe all supported operators and their semantics.

### Option B: Reject non-union operators in validation

If union-only semantics are intended, add a check in `validate_policy_expressions()`:

```rust
ExprToken::Operator => {
    // Check the actual operator character
    if op_char == '&' || op_char == '-' {
        return Err("only union (+) operator is supported in expressions".into());
    }
}
```
