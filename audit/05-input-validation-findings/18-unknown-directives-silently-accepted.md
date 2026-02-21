# Unknown SDL Directives Silently Accepted

**Severity**: LOW
**Category**: Input Validation — Schema Parsing
**Status**: Confirmed

## Summary

The SDL parser silently accepts unknown directives on both fields and types, emitting warnings but not errors. While this is a deliberate forward-compatibility mechanism, it means an attacker can submit schemas with arbitrary directives (e.g., `@malicious(code: "exec")`) that will be silently ignored. Unknown directive *arguments* on known directives are similarly accepted with warnings rather than errors.

## Affected Files

- `crates/query/src/sdl_parse/fields.rs` — lines 105-113 (field-level), lines 305-313 (type-level)
- `crates/query/src/sdl_parse/directives.rs` — lines 11-44 (known directive/argument whitelist)

## Details

### Known Directives (Whitelist)

**Field-level**: `@primary`, `@crdt`, `@index`, `@relation`, `@default`, `@constraints`, `@embedding`, `@encryptedIndex`, `@policy`

**Type-level**: `@index`, `@materialized`, `@branchable`, `@policy`

### Behavior with Unknown Directives

When an unrecognized directive is encountered:

```rust
// fields.rs — field-level unknown directive handling
_ => {
    self.warnings.push(ParseWarning::UnknownDirective {
        directive_name: directive.name.clone(),
        location: DirectiveLocation::Field,
        type_name: type_name.clone(),
        field_name: Some(field_name.to_string()),
    });
}
```

The directive is **logged as a warning and skipped** — no error is raised, parsing continues, and the schema is accepted. The same pattern applies to unknown arguments on known directives (`ParseWarning::UnknownDirectiveArgument`).

### Attack Scenario

An attacker submits:

```graphql
type User {
  name: String @inject(sql: "DROP TABLE users")
  age: Int @admin(bypass: true)
}
```

This schema is **accepted without error**. The unknown directives and their arguments are discarded, but the schema is created. While the injected content is never evaluated (Rust doesn't execute arbitrary strings), this creates confusion about what the schema actually enforces.

### Security Assessment

**Risk is LOW** because:
1. Rust does not evaluate string directive arguments as code
2. The directive arguments are discarded, not stored or passed downstream
3. The warnings are logged (not completely silent)
4. This matches Go DefraDB behavior (forward compatibility)

**Risk would increase if**:
- Directive argument strings were stored in metadata accessible to other systems
- Any processing pipeline later evaluated directive content
- Custom directives were added without updating the whitelist check

## Remediation

Consider adding a strict mode that rejects unknown directives:

```rust
// Option: strict validation mode (default off for Go compat)
if self.strict_mode {
    return Err(ParseError::UnknownDirective { ... });
}
```

## Test Gap

No integration test verifies that unknown directives are rejected or produce expected warnings. Add a test that submits a schema with `@unknown_directive` and verifies the warning output.
