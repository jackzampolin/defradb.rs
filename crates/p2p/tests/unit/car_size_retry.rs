use super::*;

#[tokio::test(start_paused = true)]
async fn size_limited_provider_is_not_retried_but_alternate_can_finish() {
    for alternate in [false, true] {
        let store = Arc::new(RegolithStore::in_memory().unwrap());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let child_data = encode_ipld(ipld!({ "value": "child" }));
        let child = make_cid(&child_data);
        let root_data = encode_ipld(ipld!({ "child": child }));
        let root = make_cid(&root_data);
        blockstore.put(&root, &root_data).await.unwrap();
        let transport = TestTransport::new(
            blockstore.clone(),
            root,
            root_data,
            HashMap::new(),
            HashMap::from([(child, child_data)]),
        );
        let completions = crate::sync::manager::BlockSyncCompletionTracker::default();
        transport
            .size_limited_providers
            .lock()
            .unwrap()
            .insert("remote-peer".to_owned(), (child, completions.clone()));
        let mut context = DagFetchContext::new(
            "doc".to_owned(),
            "collection".to_owned(),
            String::new(),
            PeerId::new("remote-peer".to_owned()),
        )
        .with_block_sync_completions(completions);
        if alternate {
            context = context.with_alternate_providers(vec![PeerId::new("alt-peer".to_owned())]);
        }
        let (tx, mut rx) = mpsc::channel(4);
        let start = tokio::time::Instant::now();
        poll_fetch_dag(
            transport.clone(),
            blockstore.clone(),
            tx,
            root,
            context,
            DagFetchLimiter::new(1),
            diagnostics(),
        )
        .await;
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "must not wait for another retry attempt"
        );
        assert_eq!(
            transport.sync_providers(),
            if alternate {
                vec!["remote-peer", "alt-peer"]
            } else {
                vec!["remote-peer"]
            }
        );
        assert_eq!(
            transport.cancelled_queries().len(),
            if alternate { 2 } else { 1 }
        );
        assert_eq!(blockstore.has(&child).await.unwrap(), alternate);
        assert_eq!(rx.try_recv().is_ok(), alternate);
    }
}
