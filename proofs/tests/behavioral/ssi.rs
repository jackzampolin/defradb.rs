//! Storage SSI serializability — write-skew rejection (point-read antidependency).
//!
//! Model: storage layer `ConflictTracker` (crates/storage/src/backends/shared.rs).
//! At commit, a transaction is rejected if a transaction that committed after its
//! snapshot either wrote a key it read, or read a key/range it wrote. Two txns
//! whose read/write sets cross (tx1: read B, write A; tx2: read A, write B) form a
//! read-write dependency cycle — committing both would be non-serializable, so the
//! second committer MUST abort.
//!
//! The carve-out: full document-collection prefix SCANS (`d/d/...`) are deliberately
//! NOT treated as conflicting reads (matching Go). Point reads (`get` -> record_key)
//! ARE. So the witnessing scenario reads the *other* document by its exact `_docID`
//! (a point read) rather than a bare collection scan, otherwise the antidependency
//! would be (correctly) ignored and no conflict would fire.
//!
//! Anti-tautology: a clean read-write-commit transaction is run FIRST and asserted to
//! SUCCEED, proving the tx path works end to end — so the later commit *failure* is a
//! genuine SSI rejection, not setup breakage or a node that simply can't commit.

use crate::support;
use defra_harness::TestCluster;

#[tokio::test]
async fn ssi_write_skew_prevented() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("build single-node cluster");
    let node = cluster.client(0);

    // Schema with an indexed-free scalar pair; two registers A and B.
    node.schema_add("type Reg { name: String  val: Int }")
        .expect("add Reg schema");

    let created_a = node
        .query(r#"mutation { add_Reg(input: {name: "A", val: 0}) { _docID } }"#)
        .expect("create A");
    let id_a = created_a["add_Reg"][0]["_docID"]
        .as_str()
        .expect("A _docID")
        .to_string();

    let created_b = node
        .query(r#"mutation { add_Reg(input: {name: "B", val: 0}) { _docID } }"#)
        .expect("create B");
    let id_b = created_b["add_Reg"][0]["_docID"]
        .as_str()
        .expect("B _docID")
        .to_string();

    // ---- Anti-tautology: a clean read-then-write transaction MUST commit. ----
    // If this fails, the harness/binary cannot commit transactions at all and any
    // later "rejection" would be meaningless.
    let warmup = node.tx_create().expect("tx_create warmup");
    let warm_read = node
        .query_with_tx(
            &format!(r#"query {{ Reg(docID: "{id_a}") {{ _docID val }} }}"#),
            &warmup,
        )
        .expect("warmup read A in tx");
    assert_eq!(
        warm_read["Reg"].as_array().map(|a| a.len()).unwrap_or(0),
        1,
        "warmup: point read of A by docID must see exactly one doc \
         (read set must be populated for SSI to be exercised)"
    );
    node.query_with_tx(
        &format!(
            r#"mutation {{ update_Reg(docID: "{id_a}", input: {{val: 1}}) {{ _docID val }} }}"#
        ),
        &warmup,
    )
    .expect("warmup write A in tx");
    node.tx_commit(&warmup)
        .expect("warmup transaction MUST commit cleanly (anti-tautology)");
    let after_warm = node
        .query(&format!(r#"query {{ Reg(docID: "{id_a}") {{ val }} }}"#))
        .expect("query A after warmup commit");
    assert_eq!(
        after_warm["Reg"][0]["val"], 1,
        "warmup commit must have persisted A.val=1"
    );

    // ---- Write-skew: open BOTH transactions before either writes. ----
    // Both snapshot the same version, so neither sees the other's pending write.
    let tx1 = node.tx_create().expect("tx_create tx1");
    let tx2 = node.tx_create().expect("tx_create tx2");

    // tx1 reads B (point read), then writes A.
    let tx1_read_b = node
        .query_with_tx(
            &format!(r#"query {{ Reg(docID: "{id_b}") {{ _docID val }} }}"#),
            &tx1,
        )
        .expect("tx1 read B");
    assert_eq!(
        tx1_read_b["Reg"].as_array().map(|a| a.len()).unwrap_or(0),
        1,
        "tx1 must point-read B (records B's key into tx1's read set)"
    );
    node.query_with_tx(
        &format!(r#"mutation {{ update_Reg(docID: "{id_a}", input: {{val: 100}}) {{ _docID }} }}"#),
        &tx1,
    )
    .expect("tx1 write A");

    // tx2 reads A (point read), then writes B — the crossing dependency.
    let tx2_read_a = node
        .query_with_tx(
            &format!(r#"query {{ Reg(docID: "{id_a}") {{ _docID val }} }}"#),
            &tx2,
        )
        .expect("tx2 read A");
    assert_eq!(
        tx2_read_a["Reg"].as_array().map(|a| a.len()).unwrap_or(0),
        1,
        "tx2 must point-read A (records A's key into tx2's read set)"
    );
    // tx2 must observe A's *pre-tx1* value (snapshot isolation): tx1's write is
    // uncommitted, so A.val is still the warmup value 1, not 100.
    assert_eq!(
        tx2_read_a["Reg"][0]["val"], 1,
        "tx2 must see A's snapshot value (1), not tx1's uncommitted write (100)"
    );
    node.query_with_tx(
        &format!(r#"mutation {{ update_Reg(docID: "{id_b}", input: {{val: 200}}) {{ _docID }} }}"#),
        &tx2,
    )
    .expect("tx2 write B");

    // ---- Commit both. Exactly one MUST abort. ----
    let c1 = node.tx_commit(&tx1);
    let c2 = node.tx_commit(&tx2);

    // tx1 commits against an empty post-snapshot history -> succeeds.
    let tx1_ok = c1.is_ok();
    // tx2 read A, which tx1 just wrote -> read-write antidependency -> rejected.
    let tx2_ok = c2.is_ok();

    assert!(
        !(tx1_ok && tx2_ok),
        "INV_Serializable violated: BOTH write-skew transactions committed \
         (tx1={c1:?}, tx2={c2:?}). One MUST abort."
    );
    assert!(
        tx1_ok != tx2_ok,
        "exactly one transaction must commit and the other abort \
         (tx1_ok={tx1_ok}, tx2_ok={tx2_ok}); tx1={c1:?}, tx2={c2:?}"
    );
    // First committer is the survivor; the loser's error must be a serialization
    // conflict, not an unrelated failure.
    assert!(
        tx1_ok,
        "tx1 (first committer) should be the survivor: {c1:?}"
    );
    let loser = c2.expect_err("tx2 must be rejected with a serialization conflict");
    let msg = loser.to_string().to_lowercase();
    assert!(
        msg.contains("conflict") || msg.contains("retry") || msg.contains("serializ"),
        "tx2 abort must be a serialization/conflict error, got: {loser:?}"
    );

    // Durable state must reflect exactly the winner: A=100 (tx1), B unchanged at 0
    // (tx2 aborted, so its B.val=200 write never landed).
    let final_a = node
        .query(&format!(r#"query {{ Reg(docID: "{id_a}") {{ val }} }}"#))
        .expect("final read A");
    let final_b = node
        .query(&format!(r#"query {{ Reg(docID: "{id_b}") {{ val }} }}"#))
        .expect("final read B");
    assert_eq!(
        final_a["Reg"][0]["val"], 100,
        "winner tx1's write to A must be durable"
    );
    assert_eq!(
        final_b["Reg"][0]["val"], 0,
        "aborted tx2's write to B must NOT be durable"
    );
}
