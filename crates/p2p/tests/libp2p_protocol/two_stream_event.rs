use p2p::TwoStreamEvent;

#[test]
fn manage_variants_exist() {
    fn _a(e: TwoStreamEvent) -> bool {
        matches!(
            e,
            TwoStreamEvent::ManageRequest { .. }
                | TwoStreamEvent::ManageReply { .. }
                | TwoStreamEvent::ManageQueryRequest { .. }
                | TwoStreamEvent::ManageQueryReply { .. }
        )
    }
    let _ = _a;
}
