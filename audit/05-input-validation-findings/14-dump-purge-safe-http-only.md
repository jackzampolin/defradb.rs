# 14: Dump and Purge Commands Are HTTP-Only (GREEN)

| Field    | Value |
|----------|-------|
| Severity | INFO |
| Category | Path Traversal |
| Status   | Not Vulnerable |

## Summary

The `dump` and `purge` CLI commands do not perform direct filesystem operations. They are HTTP client commands that send requests to the running node's HTTP API. The dump output goes to stdout (not a file), and purge operates on the database store (not arbitrary filesystem paths). Neither command introduces path traversal risks.

## Analysis

### Dump Command (`crates/cli/src/commands/client/dump.rs`)

```rust
impl DumpArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?;
        let result = client.dump().await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}
```

- No `PathBuf` parameters
- No filesystem writes — output goes to stdout
- Communicates via HTTP to the running node

### Server-Dump Command (`crates/cli/src/commands/server_dump.rs`)

```rust
impl ServerDumpArgs {
    pub async fn execute(&self, config: &Config) -> Result<()> {
        // Opens database directly using config.data_path()
        let lines = database.print_dump().await?;
        for line in &lines {
            println!("{}", line);
        }
    }
}
```

- Uses `config.data_path()` which is derived from the validated root directory
- Output goes to stdout (no file write)
- No user-controlled path beyond the config rootdir

### Purge Command (`crates/cli/src/commands/client/purge.rs`)

```rust
impl PurgeArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        if !self.force { return Err(...); }
        let client = HttpClient::new(&ctx.url)?;
        client.purge().await?;
        println!("Database purged successfully");
    }
}
```

- Requires `--force` flag (defense against accidental invocation)
- Communicates via HTTP — the actual purge is performed by the running node on its own data
- No filesystem path parameters
- Cannot be tricked into purging outside the database's data directory

## Conclusion

Both dump and purge are safe from path traversal. Dump outputs to stdout, purge operates through the HTTP API on the node's own data store. The `--force` flag on purge is a good safety measure.
