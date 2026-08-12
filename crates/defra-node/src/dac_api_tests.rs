use super::EmbeddedNode;

const POLICY_YAML: &str = r#"
name: Sensitive Rows
resources:
  - name: users
    relations:
      - name: reader
    permissions:
      - name: read
        expr: reader
      - name: update
      - name: delete
"#;
const DID_A: &str = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";

#[tokio::test]
async fn add_dac_policy_rejects_empty_identity_and_policy() {
    let node = EmbeddedNode::builder().build().await.unwrap();
    let err = node.add_dac_policy("", POLICY_YAML).await.unwrap_err();
    assert_eq!(err.to_string(), "policy creator can not be empty");
    let err = node.add_dac_policy(DID_A, "").await.unwrap_err();
    assert_eq!(err.to_string(), "policy data can not be empty");
    node.shutdown().await;
}

#[tokio::test]
async fn add_dac_policy_returns_id_and_validates_policy() {
    let node = EmbeddedNode::builder().build().await.unwrap();
    let policy_id = node.add_dac_policy(DID_A, POLICY_YAML).await.unwrap();
    assert!(!policy_id.is_empty());
    let bad = "name: Bad\nresources:\n  - name: users\n    relations:\n      - name: reader\n    permissions:\n      - name: read\n        expr: undeclared\n";
    assert!(node.add_dac_policy(DID_A, bad).await.is_err());
    node.shutdown().await;
}
