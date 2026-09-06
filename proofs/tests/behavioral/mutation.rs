use anyhow::{bail, ensure, Context, Result};
use defra_harness::TestCluster;
use serde_json::Value;
use std::process::Command;
use std::time::Duration;

/// Go can exit successfully with both mutation data and a commit error.
/// Retry only explicit transaction aborts, never a failed convergence assertion.
pub async fn execute(cluster: &TestCluster, node: usize, query: &str) -> Result<Value> {
    let address = cluster.api_url(node).trim_start_matches("http://");
    for attempt in 0..5 {
        let output = Command::new(cluster.client(node).binary_path())
            .args(["--url", address, "client", "query", query])
            .output()
            .context("execute mutation")?;
        ensure!(
            output.status.success(),
            "mutation command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout)?;
        if let Some(data) = mutation_result(&stdout)? {
            return Ok(data);
        }
        if attempt < 4 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    bail!("mutation aborted after five transaction conflicts: {query}")
}

fn mutation_result(stdout: &str) -> Result<Option<Value>> {
    let start = stdout.find('{').context("mutation returned no JSON")?;
    let response: Value = serde_json::from_str(&stdout[start..])?;
    if let Some(errors) = response.get("errors").filter(|value| !value.is_null()) {
        let errors = errors.as_array().context("invalid GraphQL errors")?;
        if !errors.is_empty() {
            if errors.iter().all(|error| {
                error["message"].as_str() == Some("Transaction Conflict. Please retry")
            }) {
                return Ok(None);
            }
            bail!("mutation returned GraphQL errors: {response}");
        }
    }
    let data = response
        .get("data")
        .filter(|data| !data.is_null())
        .context("mutation returned no data")?;
    Ok(Some(data.clone()))
}

#[test]
fn commit_conflict_is_not_success_even_with_mutation_data() {
    let response = r#"{"data":{"update_User":[{"_docID":"doc"}]},"errors":[{"message":"Transaction Conflict. Please retry"}]}"#;
    assert!(mutation_result(response).unwrap().is_none());
}

#[test]
fn unrelated_errors_are_not_retried_or_hidden_by_data() {
    for errors in [
        serde_json::json!([{"message": "invalid field"}]),
        serde_json::json!([
            {"message": "Transaction Conflict. Please retry"},
            {"message": "invalid field"}
        ]),
    ] {
        let response = serde_json::json!({"data": {"update_User": []}, "errors": errors});
        assert!(mutation_result(&response.to_string()).is_err());
    }
}

#[test]
fn successful_response_requires_data_and_allows_cli_preamble() {
    let result = mutation_result(
        "------ Request Results ------\n{\"data\": {\"update_User\": []}, \"errors\": []}",
    )
    .unwrap();
    assert_eq!(result, Some(serde_json::json!({"update_User": []})));
    for invalid in ["", "not JSON", "{}", "{\"data\":null}", "{\"errors\":{}}"] {
        assert!(mutation_result(invalid).is_err());
    }
}
