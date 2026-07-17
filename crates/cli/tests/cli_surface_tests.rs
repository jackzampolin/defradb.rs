use clap::Parser;
use cli::cli::Cli;

fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
    let mut argv = vec!["defra"];
    argv.extend_from_slice(args);
    Cli::try_parse_from(argv)
}

#[test]
fn new_cli_rename_surface_parses() {
    assert!(parse(&["client", "action", "list"]).is_ok());
    assert!(parse(&["client", "collection", "add", "type User { name: String }"]).is_ok());
    assert!(parse(&[
        "client",
        "document",
        "add",
        "--collection-name",
        "User",
        r#"{"name":"Alice"}"#,
    ])
    .is_ok());
    assert!(parse(&["client", "index", "new", "-c", "User", "-f", "name"]).is_ok());
    assert!(parse(&[
        "client",
        "encrypted-index",
        "new",
        "-c",
        "User",
        "--field",
        "name",
    ])
    .is_ok());
    assert!(parse(&["client", "tx", "new"]).is_ok());
    assert!(parse(&["keyring", "new"]).is_ok());
    assert!(parse(&["--development", "keyring", "add", "test-key", "deadbeef"]).is_ok());
    assert!(parse(&["--development", "keyring", "get", "test-key"]).is_ok());
}

#[test]
fn old_cli_rename_aliases_are_rejected() {
    assert!(parse(&["client", "schema", "add", "type User { name: String }"]).is_err());
    assert!(parse(&["client", "collection", "schema"]).is_err());
    assert!(parse(&["client", "collection", "docIDs"]).is_err());
    assert!(parse(&["client", "index", "create", "-c", "User", "-f", "name"]).is_err());
    assert!(parse(&[
        "client",
        "encrypted-index",
        "add",
        "-c",
        "User",
        "--field",
        "name"
    ])
    .is_err());
    assert!(parse(&["client", "tx", "create"]).is_err());
    assert!(parse(&["keyring", "generate"]).is_err());
    assert!(parse(&["keyring", "import", "test-key", "deadbeef"]).is_err());
    assert!(parse(&["keyring", "export", "test-key"]).is_err());
    assert!(parse(&[
        "client",
        "document",
        "add",
        "--name",
        "User",
        r#"{"name":"Alice"}"#,
    ])
    .is_err());
}
