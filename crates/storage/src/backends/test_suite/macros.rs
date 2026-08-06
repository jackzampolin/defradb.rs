/// Macro to generate test functions for a specific store type
#[macro_export]
macro_rules! generate_backend_tests {
    ($store_fn:expr) => {
        use $crate::backends::test_suite;

        #[tokio::test]
        async fn shared_test_basic_set_get() {
            let store = $store_fn().await;
            test_suite::test_basic_set_get(&store).await;
        }

        #[tokio::test]
        async fn shared_test_delete() {
            let store = $store_fn().await;
            test_suite::test_delete(&store).await;
        }

        #[tokio::test]
        async fn shared_test_has() {
            let store = $store_fn().await;
            test_suite::test_has(&store).await;
        }

        #[tokio::test]
        async fn shared_test_read_your_writes() {
            let store = $store_fn().await;
            test_suite::test_read_your_writes(&store).await;
        }

        #[tokio::test]
        async fn shared_test_empty_key_rejected() {
            let store = $store_fn().await;
            test_suite::test_empty_key_rejected(&store).await;
        }

        #[tokio::test]
        async fn shared_test_readonly_transaction() {
            let store = $store_fn().await;
            test_suite::test_readonly_transaction(&store).await;
        }

        #[tokio::test]
        async fn shared_test_closed_store_rejected() {
            let store = $store_fn().await;
            test_suite::test_closed_store_rejected(&store).await;
        }

        #[tokio::test]
        async fn shared_test_discard_prevents_persistence() {
            let store = $store_fn().await;
            test_suite::test_discard_prevents_persistence(&store).await;
        }

        #[tokio::test]
        async fn shared_test_success_callback() {
            let store = $store_fn().await;
            test_suite::test_success_callback(&store).await;
        }

        #[tokio::test]
        async fn shared_test_discard_callback() {
            let store = $store_fn().await;
            test_suite::test_discard_callback(&store).await;
        }

        #[tokio::test]
        async fn shared_test_async_success_callback() {
            let store = $store_fn().await;
            test_suite::test_async_success_callback(&store).await;
        }

        #[tokio::test]
        async fn shared_test_callback_panic_propagates() {
            let store = $store_fn().await;
            test_suite::test_callback_panic_propagates(&store).await;
        }

        #[tokio::test]
        async fn shared_test_async_callback_panic_propagates() {
            let store = $store_fn().await;
            test_suite::test_async_callback_panic_propagates(&store).await;
        }

        #[tokio::test]
        async fn shared_test_discard_callback_panic_propagates() {
            let store = $store_fn().await;
            test_suite::test_discard_callback_panic_propagates(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_basic() {
            let store = $store_fn().await;
            test_suite::test_iterator_basic(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_prefix() {
            let store = $store_fn().await;
            test_suite::test_iterator_prefix(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_reverse() {
            let store = $store_fn().await;
            test_suite::test_iterator_reverse(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_range() {
            let store = $store_fn().await;
            test_suite::test_iterator_range(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_keys_only() {
            let store = $store_fn().await;
            test_suite::test_iterator_keys_only(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_closed() {
            let store = $store_fn().await;
            test_suite::test_iterator_closed(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_empty_store() {
            let store = $store_fn().await;
            test_suite::test_iterator_empty_store(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_start_equals_end() {
            let store = $store_fn().await;
            test_suite::test_iterator_start_equals_end(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_prefix_no_match() {
            let store = $store_fn().await;
            test_suite::test_iterator_prefix_no_match(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_sees_pending_writes() {
            let store = $store_fn().await;
            test_suite::test_iterator_sees_pending_writes(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_sees_pending_deletes() {
            let store = $store_fn().await;
            test_suite::test_iterator_sees_pending_deletes(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_pending_writes_at_chunk_boundary() {
            let store = $store_fn().await;
            test_suite::test_iterator_pending_writes_at_chunk_boundary(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_reverse_with_prefix() {
            let store = $store_fn().await;
            test_suite::test_iterator_reverse_with_prefix(&store).await;
        }

        #[tokio::test]
        async fn shared_test_binary_data() {
            let store = $store_fn().await;
            test_suite::test_binary_data(&store).await;
        }

        #[tokio::test]
        async fn shared_test_binary_key_ordering() {
            let store = $store_fn().await;
            test_suite::test_binary_key_ordering(&store).await;
        }

        #[tokio::test]
        async fn shared_test_get_size() {
            let store = $store_fn().await;
            test_suite::test_get_size(&store).await;
        }

        #[tokio::test]
        async fn shared_test_get_size_with_deletes() {
            let store = $store_fn().await;
            test_suite::test_get_size_with_deletes(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_reverse_with_bounds() {
            let store = $store_fn().await;
            test_suite::test_iterator_reverse_with_bounds(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_single_item_at_start() {
            let store = $store_fn().await;
            test_suite::test_iterator_single_item_at_start(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_item_at_end_excluded() {
            let store = $store_fn().await;
            test_suite::test_iterator_item_at_end_excluded(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_prefix_between_keys() {
            let store = $store_fn().await;
            test_suite::test_iterator_prefix_between_keys(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_empty_prefix() {
            let store = $store_fn().await;
            test_suite::test_iterator_empty_prefix(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_prefix_with_start() {
            let store = $store_fn().await;
            test_suite::test_iterator_prefix_with_start(&store).await;
        }

        #[tokio::test]
        async fn shared_test_multiple_iterators() {
            let store = $store_fn().await;
            test_suite::test_multiple_iterators(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_seek() {
            let store = $store_fn().await;
            test_suite::test_iterator_seek(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_seek_between_keys() {
            let store = $store_fn().await;
            test_suite::test_iterator_seek_between_keys(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_seek_past_end() {
            let store = $store_fn().await;
            test_suite::test_iterator_seek_past_end(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_reset() {
            let store = $store_fn().await;
            test_suite::test_iterator_reset(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_seek_after_iteration() {
            let store = $store_fn().await;
            test_suite::test_iterator_seek_after_iteration(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_seek_reset_on_closed() {
            let store = $store_fn().await;
            test_suite::test_iterator_seek_reset_on_closed(&store).await;
        }

        // Reverse iterator edge case tests (from Go corekv)
        #[tokio::test]
        async fn shared_test_iterator_reverse_start_only() {
            let store = $store_fn().await;
            test_suite::test_iterator_reverse_start_only(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_reverse_end_only() {
            let store = $store_fn().await;
            test_suite::test_iterator_reverse_end_only(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_reverse_end_single_item_out_of_bounds() {
            let store = $store_fn().await;
            test_suite::test_iterator_reverse_end_single_item_out_of_bounds(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_reverse_start_end_no_items_in_range() {
            let store = $store_fn().await;
            test_suite::test_iterator_reverse_start_end_no_items_in_range(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_reverse_seek() {
            let store = $store_fn().await;
            test_suite::test_iterator_reverse_seek(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_reverse_seek_next() {
            let store = $store_fn().await;
            test_suite::test_iterator_reverse_seek_next(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_reverse_end_seek() {
            let store = $store_fn().await;
            test_suite::test_iterator_reverse_end_seek(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_reverse_prefix_next() {
            let store = $store_fn().await;
            test_suite::test_iterator_reverse_prefix_next(&store).await;
        }

        // Iterator state transition tests (from Go corekv)
        #[tokio::test]
        async fn shared_test_iterator_reset_partial_iteration() {
            let store = $store_fn().await;
            test_suite::test_iterator_reset_partial_iteration(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_reset_after_exhaustion() {
            let store = $store_fn().await;
            test_suite::test_iterator_reset_after_exhaustion(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_reset_then_seek() {
            let store = $store_fn().await;
            test_suite::test_iterator_reset_then_seek(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_seek_respects_start_bound() {
            let store = $store_fn().await;
            test_suite::test_iterator_seek_respects_start_bound(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_multiple_resets() {
            let store = $store_fn().await;
            test_suite::test_iterator_multiple_resets(&store).await;
        }

        // Empty value handling tests
        #[tokio::test]
        async fn shared_test_empty_value_handling() {
            let store = $store_fn().await;
            test_suite::test_empty_value_handling(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_empty_values() {
            let store = $store_fn().await;
            test_suite::test_iterator_empty_values(&store).await;
        }
    };
}

/// Macro for concurrency tests that need Arc<Store>
/// NOTE: This macro assumes generate_backend_tests! was already called, which imports test_suite
#[macro_export]
macro_rules! generate_backend_concurrency_tests {
    ($arc_store_fn:expr) => {
        #[tokio::test]
        async fn shared_test_concurrent_writes_different_keys() {
            let store = $arc_store_fn().await;
            test_suite::test_concurrent_writes_different_keys(store).await;
        }

        #[tokio::test]
        async fn shared_test_commutative_set_transitions() {
            let store = $arc_store_fn().await;
            test_suite::test_commutative_set_transitions(store).await;
        }

        #[tokio::test]
        async fn shared_test_concurrent_writes_same_key() {
            let store = $arc_store_fn().await;
            test_suite::test_concurrent_writes_same_key(store).await;
        }

        #[tokio::test]
        async fn shared_test_last_writer_wins() {
            let store = $arc_store_fn().await;
            test_suite::test_last_writer_wins(store).await;
        }

        #[tokio::test]
        async fn shared_test_last_writer_wins_reverse() {
            let store = $arc_store_fn().await;
            test_suite::test_last_writer_wins_reverse(store).await;
        }

        #[tokio::test]
        async fn shared_test_parallel_stress() {
            let store = $arc_store_fn().await;
            test_suite::test_parallel_stress(store).await;
        }

        #[tokio::test]
        async fn shared_test_snapshot_isolation_concurrent() {
            let store = $arc_store_fn().await;
            test_suite::test_snapshot_isolation_concurrent(store).await;
        }

        #[tokio::test]
        async fn shared_test_snapshot_isolation_long_running_reader() {
            let store = $arc_store_fn().await;
            test_suite::test_snapshot_isolation_long_running_reader(store).await;
        }

        #[tokio::test]
        async fn shared_test_snapshot_isolation_iterator() {
            let store = $arc_store_fn().await;
            test_suite::test_snapshot_isolation_iterator(store).await;
        }

        #[tokio::test]
        async fn shared_test_write_write_isolation() {
            let store = $arc_store_fn().await;
            test_suite::test_write_write_isolation(store).await;
        }
    };
}

/// Macro for Dropable tests (for stores that implement the Dropable trait)
/// NOTE: This macro assumes generate_backend_tests! was already called, which imports test_suite
#[macro_export]
macro_rules! generate_backend_dropable_tests {
    ($store_fn:expr) => {
        #[tokio::test]
        async fn shared_test_drop_all() {
            let store = $store_fn().await;
            test_suite::test_drop_all(&store).await;
        }

        #[tokio::test]
        async fn shared_test_drop_all_then_write() {
            let store = $store_fn().await;
            test_suite::test_drop_all_then_write(&store).await;
        }

        #[tokio::test]
        async fn shared_test_drop_all_empty_store() {
            let store = $store_fn().await;
            test_suite::test_drop_all_empty_store(&store).await;
        }
    };
}

pub use generate_backend_concurrency_tests;
pub use generate_backend_dropable_tests;
pub use generate_backend_tests;
